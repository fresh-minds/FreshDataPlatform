#!/usr/bin/env python3
"""
Register all warehouse datasets in DataHub catalog.
This script discovers tables from the PostgreSQL warehouse and registers them in DataHub.
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Add repo root directory to path for imports when executed directly.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, REPO_ROOT)

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    TimeTypeClass,
    OtherSchemaClass,
    TagAssociationClass,
    GlobalTagsClass,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment from .env when running from the repo root.
load_dotenv()

# DataHub configuration
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://localhost:8081")

# Warehouse configuration
WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = int(os.getenv("WAREHOUSE_PORT", "5433"))
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "admin")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")

# Domain/Layer metadata for tags
LAYER_TAGS = {
    "gold": "urn:li:tag:Gold",
    "silver": "urn:li:tag:Silver", 
    "bronze": "urn:li:tag:Bronze"
}

DOMAIN_TAGS = {
    "job_market_nl": "urn:li:tag:JobMarket",
}


def pg_type_to_datahub_type(pg_type):
    """Convert PostgreSQL type to DataHub field type."""
    type_mapping = {
        'integer': NumberTypeClass(),
        'bigint': NumberTypeClass(),
        'smallint': NumberTypeClass(),
        'numeric': NumberTypeClass(),
        'real': NumberTypeClass(),
        'double precision': NumberTypeClass(),
        'boolean': BooleanTypeClass(),
        'text': StringTypeClass(),
        'varchar': StringTypeClass(),
        'character varying': StringTypeClass(),
        'char': StringTypeClass(),
        'date': DateTypeClass(),
        'timestamp': TimeTypeClass(),
        'timestamp without time zone': TimeTypeClass(),
        'timestamp with time zone': TimeTypeClass(),
        'time': TimeTypeClass(),
        'json': StringTypeClass(),
        'jsonb': StringTypeClass(),
        'uuid': StringTypeClass(),
    }
    return type_mapping.get(pg_type.lower(), StringTypeClass())


def get_warehouse_tables(conn):
    """Get all tables and their columns from the warehouse."""
    tables = {}
    
    # Get all tables in non-system schemas.
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_schema, table_name
        """)
        table_rows = cur.fetchall()
        
        for row in table_rows:
            table_schema = row['table_schema']
            table_name = row['table_name']
            
            # Get columns for this table
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (table_schema, table_name))
            columns = cur.fetchall()
            
            tables[(table_schema, table_name)] = {
                'schema': table_schema,
                'table_type': row['table_type'],
                'columns': columns,
                'description': f"Warehouse {row['table_type'].lower()}: {table_schema}.{table_name}"
            }
    
    return tables


def create_dataset_urn(
    schema_name: str, table_name: str, platform: str = "postgres", instance: str = "open_data_platform_dw"
) -> str:
    """Create a DataHub dataset URN."""
    # Dataset naming: <database>.<schema>.<table>
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{instance}.{schema_name}.{table_name},PROD)"


def determine_layer_and_domain(schema_name: str, table_name: str):
    """Determine the data layer and domain from schema/table name."""
    layer = "gold"
    domain = None

    schema_lower = schema_name.lower()
    # dbt schemas are often prefixed by the target schema (e.g., dbt_parallel_job_market_nl_dbt)
    if schema_lower.startswith("dbt_parallel_"):
        schema_lower = schema_lower[len("dbt_parallel_") :]

    if schema_lower.startswith("raw_"):
        layer = "bronze"
        domain = schema_lower[len("raw_") :]
    elif schema_lower.endswith("_silver"):
        layer = "silver"
        domain = schema_lower[: -len("_silver")]
    elif schema_lower in DOMAIN_TAGS:
        layer = "gold"
        domain = schema_lower
    else:
        # Fall back to heuristics on table name when schema doesn't map cleanly.
        table_lower = table_name.lower()
        if 'job_market' in table_lower or 'vacancy' in table_lower or 'skill' in table_lower:
            domain = "job_market_nl"

    return layer, domain


def register_table_in_datahub(emitter, schema_name: str, table_name: str, table_info):
    """Register a single table in DataHub."""
    dataset_urn = create_dataset_urn(schema_name, table_name)
    layer, domain = determine_layer_and_domain(schema_name, table_name)
    
    logger.info(f"Registering table: {schema_name}.{table_name} (layer={layer}, domain={domain})")
    
    # 1. Emit dataset properties
    properties = DatasetPropertiesClass(
        name=f"{schema_name}.{table_name}",
        description=table_info.get('description', f"Table {table_name} from Open Data Platform Warehouse"),
        customProperties={
            "source": "open_data_platform_warehouse",
            "layer": layer,
            "domain": domain or "unknown"
        }
    )
    
    mcp = MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=properties
    )
    emitter.emit(mcp)
    
    # 2. Emit schema metadata
    columns = table_info.get('columns', [])
    if columns:
        fields = []
        for col in columns:
            field = SchemaFieldClass(
                fieldPath=col['column_name'],
                type=SchemaFieldDataTypeClass(type=pg_type_to_datahub_type(col['data_type'])),
                nativeDataType=col['data_type'],
                description=f"Column {col['column_name']} ({col['data_type']})",
                nullable=col['is_nullable'] == 'YES'
            )
            fields.append(field)
        
        schema_metadata = SchemaMetadataClass(
            schemaName=f"{schema_name}.{table_name}",
            platform="urn:li:dataPlatform:postgres",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields
        )
        
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata
        )
        emitter.emit(mcp)
    
    # 3. Emit tags
    tags = []
    if layer and layer in LAYER_TAGS:
        tags.append(TagAssociationClass(tag=LAYER_TAGS[layer]))
    if domain and domain in DOMAIN_TAGS:
        tags.append(TagAssociationClass(tag=DOMAIN_TAGS[domain]))
    
    if tags:
        global_tags = GlobalTagsClass(tags=tags)
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=global_tags
        )
        emitter.emit(mcp)
    
    logger.info(f"Successfully registered: {table_name}")


def create_tags(emitter):
    """Create the required tags in DataHub."""
    from datahub.metadata.schema_classes import TagPropertiesClass
    
    all_tags = {
        **LAYER_TAGS,
        **DOMAIN_TAGS
    }
    
    for tag_name, tag_urn in all_tags.items():
        try:
            tag_properties = TagPropertiesClass(
                name=tag_name.capitalize(),
                description=f"Tag for {tag_name} categorization"
            )
            
            mcp = MetadataChangeProposalWrapper(
                entityUrn=tag_urn,
                aspect=tag_properties
            )
            emitter.emit(mcp)
            logger.info(f"Created tag: {tag_name}")
        except Exception as e:
            logger.warning(f"Could not create tag {tag_name}: {e}")


def main():
    """Main function to register all warehouse data in DataHub."""
    logger.info("=" * 60)
    logger.info("Starting DataHub Catalog Registration")
    logger.info("=" * 60)
    
    # Connect to DataHub
    logger.info(f"Connecting to DataHub at {DATAHUB_GMS_URL}")
    try:
        emitter = DatahubRestEmitter(DATAHUB_GMS_URL)
        emitter.test_connection()
        logger.info("Successfully connected to DataHub GMS")
    except Exception as e:
        logger.error(f"Failed to connect to DataHub: {e}")
        sys.exit(1)
    
    # Create tags first
    logger.info("Creating tags in DataHub...")
    create_tags(emitter)
    
    # Connect to warehouse
    logger.info(f"Connecting to warehouse at {WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}")
    try:
        conn = psycopg2.connect(
            host=WAREHOUSE_HOST,
            port=WAREHOUSE_PORT,
            dbname=WAREHOUSE_DB,
            user=WAREHOUSE_USER,
            password=WAREHOUSE_PASSWORD
        )
        logger.info("Successfully connected to warehouse")
    except Exception as e:
        logger.error(f"Failed to connect to warehouse: {e}")
        sys.exit(1)
    
    # Get all tables
    logger.info("Discovering warehouse tables...")
    tables = get_warehouse_tables(conn)
    logger.info(f"Found {len(tables)} tables in warehouse")
    
    # Register each table in DataHub
    success_count = 0
    error_count = 0
    
    for (schema_name, table_name), table_info in tables.items():
        try:
            register_table_in_datahub(emitter, schema_name, table_name, table_info)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to register {schema_name}.{table_name}: {e}")
            error_count += 1
    
    # Close connections
    conn.close()
    
    # Summary
    logger.info("=" * 60)
    logger.info("Registration Complete!")
    logger.info(f"  Successfully registered: {success_count} tables")
    logger.info(f"  Failed: {error_count} tables")
    logger.info("=" * 60)
    logger.info(f"View your data catalog at: http://localhost:9002")


if __name__ == "__main__":
    main()
