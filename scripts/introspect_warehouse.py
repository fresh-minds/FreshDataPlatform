#!/usr/bin/env python3
"""
Introspect PostgreSQL warehouse and generate DBML schema file.

Connects to the warehouse database and generates a DBML file representing
all tables, columns, types, and inferred relationships.
"""

import os
import sys
import logging
import argparse
from typing import Dict, List, Optional
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Warehouse configuration
WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = int(os.getenv("WAREHOUSE_PORT", "5433"))
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "admin")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")

# Output path
SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema")
OUTPUT_FILE = os.path.join(SCHEMA_DIR, "warehouse.dbml")


@dataclass
class Column:
    name: str
    data_type: str
    nullable: bool
    is_pk: bool
    description: Optional[str] = None


@dataclass
class Table:
    schema: str
    name: str
    columns: List[Column]
    description: Optional[str] = None


def pg_type_to_dbml(pg_type: str) -> str:
    """Convert PostgreSQL type to DBML type."""
    type_mapping = {
        'integer': 'int',
        'bigint': 'bigint',
        'smallint': 'smallint',
        'numeric': 'decimal',
        'real': 'float',
        'double precision': 'double',
        'boolean': 'boolean',
        'text': 'text',
        'varchar': 'varchar',
        'character varying': 'varchar',
        'char': 'char',
        'date': 'date',
        'timestamp': 'timestamp',
        'timestamp without time zone': 'timestamp',
        'timestamp with time zone': 'timestamptz',
        'time': 'time',
        'json': 'json',
        'jsonb': 'jsonb',
        'uuid': 'uuid',
    }
    return type_mapping.get(pg_type.lower(), pg_type)


def get_primary_keys(conn, schema: str, table: str) -> List[str]:
    """Get primary key columns for a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary
            AND n.nspname = %s
            AND c.relname = %s
        """, (schema, table))
        return [row[0] for row in cur.fetchall()]


def get_tables(conn, schemas: List[str] = None) -> List[Table]:
    """Get all tables from the warehouse."""
    if schemas is None:
        schemas = ['public']
    
    tables = []
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Get tables
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            AND table_schema = ANY(%s)
            ORDER BY table_schema, table_name
        """, (schemas,))
        table_rows = cur.fetchall()
        
        for row in table_rows:
            schema = row['table_schema']
            table_name = row['table_name']
            
            # Get primary keys for this table
            pk_columns = get_primary_keys(conn, schema, table_name)
            
            # Get columns
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            
            columns = []
            for col in cur.fetchall():
                columns.append(Column(
                    name=col['column_name'],
                    data_type=pg_type_to_dbml(col['data_type']),
                    nullable=col['is_nullable'] == 'YES',
                    is_pk=col['column_name'] in pk_columns,
                ))
            
            tables.append(Table(
                schema=schema,
                name=table_name,
                columns=columns,
            ))
    
    return tables


def infer_relationships(tables: List[Table]) -> List[str]:
    """Infer relationships based on naming conventions (e.g., *_id columns)."""
    relationships = []
    table_names = {t.name for t in tables}
    
    for table in tables:
        for col in table.columns:
            # Check for foreign key patterns
            if col.name.endswith('_id') and not col.is_pk:
                # Try to find matching table
                ref_table_name = col.name[:-3]  # Remove '_id'
                
                # Try various naming conventions
                potential_refs = [
                    ref_table_name,
                    f"dim_{ref_table_name}",
                    f"dim_{ref_table_name}s",
                    ref_table_name + 's',
                ]
                
                for ref in potential_refs:
                    if ref in table_names:
                        relationships.append(
                            f"Ref: {table.name}.{col.name} > {ref}.id"
                        )
                        break
    
    return relationships


def generate_dbml(
    tables: List[Table], relationships: List[str], include_generated_at: bool = True
) -> str:
    """Generate DBML content from tables and relationships."""
    lines = [
        "// Open Data Platform Warehouse Schema",
        "// Auto-generated from PostgreSQL introspection",
        "Project open_data_platform_dw {",
        "  database_type: 'PostgreSQL'",
        "  Note: 'Open Data Platform Warehouse - Gold Layer Tables'",
        "}",
        "",
    ]
    if include_generated_at:
        lines.insert(2, f"// Generated at: {__import__('datetime').datetime.now().isoformat()}")
        lines.insert(3, "")
    else:
        lines.insert(2, "")
    
    # Group tables by domain (inferred from name prefixes)
    domain_tables: Dict[str, List[Table]] = {
        'job_market_nl': [],
        'other': [],
    }
    
    for table in tables:
        name_lower = table.name.lower()
        if 'job_market' in name_lower or 'vacancy' in name_lower or 'skill' in name_lower:
            domain_tables['job_market_nl'].append(table)
        else:
            domain_tables['other'].append(table)
    
    # Generate table definitions grouped by domain
    for domain, domain_table_list in domain_tables.items():
        if not domain_table_list:
            continue
            
        lines.append(f"// ===========================================")
        lines.append(f"// {domain.upper()} Domain")
        lines.append(f"// ===========================================")
        lines.append("")
        
        for table in domain_table_list:
            lines.append(f"Table {table.name} {{")
            
            for col in table.columns:
                attrs = []
                if col.is_pk:
                    attrs.append("pk")
                if not col.nullable:
                    attrs.append("not null")
                
                attr_str = f" [{', '.join(attrs)}]" if attrs else ""
                lines.append(f"  {col.name} {col.data_type}{attr_str}")
            
            lines.append("}")
            lines.append("")
    
    # Add relationships
    if relationships:
        lines.append("// ===========================================")
        lines.append("// Relationships")
        lines.append("// ===========================================")
        lines.append("")
        for rel in relationships:
            lines.append(rel)
        lines.append("")
    
    return "\n".join(lines)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Introspect PostgreSQL warehouse and generate DBML schema"
    )
    parser.add_argument(
        "--output-file",
        default=OUTPUT_FILE,
        help=f"Output DBML file path (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--stable",
        action="store_true",
        help="Generate stable output without dynamic timestamp header",
    )
    parser.add_argument(
        "--schemas",
        default="public",
        help="Comma-separated list of schemas to introspect (default: public)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None):
    """Main function to introspect warehouse and generate DBML."""
    args = parse_args(argv)
    output_file = os.path.abspath(args.output_file)
    schemas = [schema.strip() for schema in args.schemas.split(",") if schema.strip()]

    logger.info("=" * 60)
    logger.info("Warehouse Schema Introspection")
    logger.info("=" * 60)
    
    # Connect to warehouse
    logger.info(f"Connecting to {WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}")
    try:
        conn = psycopg2.connect(
            host=WAREHOUSE_HOST,
            port=WAREHOUSE_PORT,
            dbname=WAREHOUSE_DB,
            user=WAREHOUSE_USER,
            password=WAREHOUSE_PASSWORD
        )
        logger.info("Connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)
    
    try:
        # Get tables
        logger.info("Introspecting tables...")
        tables = get_tables(conn, schemas=schemas)
        logger.info(f"Found {len(tables)} tables")
        
        if not tables:
            logger.warning("No tables found in warehouse. DBML will be empty.")
        
        # Infer relationships
        logger.info("Inferring relationships...")
        relationships = infer_relationships(tables)
        logger.info(f"Inferred {len(relationships)} relationships")
        
        # Generate DBML
        logger.info("Generating DBML...")
        dbml_content = generate_dbml(
            tables, relationships, include_generated_at=not args.stable
        )
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Write to file
        with open(output_file, 'w') as f:
            f.write(dbml_content)

        logger.info(f"✓ DBML written to: {output_file}")
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Summary")
        logger.info("=" * 60)
        for table in tables:
            logger.info(f"  - {table.name} ({len(table.columns)} columns)")
        
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
