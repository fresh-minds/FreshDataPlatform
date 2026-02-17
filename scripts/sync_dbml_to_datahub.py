#!/usr/bin/env python3
"""
Sync DBML schema definitions to DataHub.

Parses DBML files and emits corresponding metadata to DataHub including:
- Dataset definitions
- Schema metadata with fields
- Tags for domains and layers
- Glossary term associations
"""

import os
import sys
import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import yaml
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    DomainsClass,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://localhost:8081")
SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema")


@dataclass
class DBMLColumn:
    name: str
    data_type: str
    is_pk: bool = False
    nullable: bool = True
    note: Optional[str] = None


@dataclass
class DBMLTable:
    name: str
    columns: List[DBMLColumn] = field(default_factory=list)
    note: Optional[str] = None


@dataclass
class GlossaryTerm:
    term: str
    definition: str
    domain: str
    synonyms: List[str] = field(default_factory=list)
    related_datasets: List[str] = field(default_factory=list)


def parse_dbml(content: str) -> List[DBMLTable]:
    """Parse DBML content and extract tables."""
    tables = []
    
    # Match table definitions
    table_pattern = r'Table\s+(\w+)\s*\{([^}]+)\}'
    table_matches = re.finditer(table_pattern, content, re.MULTILINE | re.DOTALL)
    
    for match in table_matches:
        table_name = match.group(1)
        table_body = match.group(2)
        
        columns = []
        
        # Parse columns
        column_pattern = r'^\s*(\w+)\s+(\w+)(?:\s*\[([^\]]+)\])?'
        for line in table_body.strip().split('\n'):
            line = line.strip()
            if not line or line.startswith('//') or line.startswith('Note:'):
                continue
            
            col_match = re.match(column_pattern, line)
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                attrs = col_match.group(3) or ""
                
                is_pk = 'pk' in attrs.lower()
                nullable = 'not null' not in attrs.lower()
                
                # Extract note
                note = None
                note_match = re.search(r"note:\s*['\"]([^'\"]+)['\"]", attrs, re.IGNORECASE)
                if note_match:
                    note = note_match.group(1)
                
                columns.append(DBMLColumn(
                    name=col_name,
                    data_type=col_type,
                    is_pk=is_pk,
                    nullable=nullable,
                    note=note,
                ))
        
        # Extract table note
        note = None
        note_match = re.search(r"Note:\s*['\"]([^'\"]+)['\"]", table_body)
        if note_match:
            note = note_match.group(1)
        
        tables.append(DBMLTable(name=table_name, columns=columns, note=note))
    
    return tables


def parse_glossary(glossary_path: str) -> Tuple[List[dict], List[GlossaryTerm]]:
    """Parse glossary YAML file."""
    with open(glossary_path, 'r') as f:
        data = yaml.safe_load(f)
    
    domains = data.get('domains', [])
    terms = []
    
    for term_data in data.get('terms', []):
        terms.append(GlossaryTerm(
            term=term_data.get('term', ''),
            definition=term_data.get('definition', ''),
            domain=term_data.get('domain', ''),
            synonyms=term_data.get('synonyms', []),
            related_datasets=term_data.get('related_datasets', []),
        ))
    
    return domains, terms


def dbml_type_to_datahub(dbml_type: str):
    """Convert DBML type to DataHub field type."""
    type_mapping = {
        'int': NumberTypeClass(),
        'integer': NumberTypeClass(),
        'bigint': NumberTypeClass(),
        'smallint': NumberTypeClass(),
        'decimal': NumberTypeClass(),
        'float': NumberTypeClass(),
        'double': NumberTypeClass(),
        'boolean': BooleanTypeClass(),
        'text': StringTypeClass(),
        'varchar': StringTypeClass(),
        'char': StringTypeClass(),
        'date': DateTypeClass(),
        'timestamp': TimeTypeClass(),
        'timestamptz': TimeTypeClass(),
        'time': TimeTypeClass(),
        'json': StringTypeClass(),
        'jsonb': StringTypeClass(),
        'uuid': StringTypeClass(),
    }
    return type_mapping.get(dbml_type.lower(), StringTypeClass())


def infer_domain(table_name: str) -> Optional[str]:
    """Infer domain from table name."""
    name_lower = table_name.lower()
    if 'transaction' in name_lower or 'grootboek' in name_lower or 'finance' in name_lower:
        return 'finance'
    elif 'job_market' in name_lower or 'vacancy' in name_lower or 'skill' in name_lower:
        return 'job_market_nl'
    return None


def create_dataset_urn(table_name: str, platform: str = "postgres", instance: str = "open_data_platform_dw") -> str:
    """Create DataHub dataset URN."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{instance}.public.{table_name},PROD)"


def emit_table_to_datahub(emitter: DatahubRestEmitter, table: DBMLTable):
    """Emit a single table to DataHub."""
    dataset_urn = create_dataset_urn(table.name)
    domain = infer_domain(table.name)
    
    logger.info(f"Emitting table: {table.name} (domain={domain})")
    
    # 1. Dataset properties
    properties = DatasetPropertiesClass(
        name=table.name,
        description=table.note or f"Table {table.name} from DBML schema",
        customProperties={
            "source": "dbml_schema",
            "layer": "gold",
            "domain": domain or "unknown",
        }
    )
    
    mcp = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=properties)
    emitter.emit(mcp)
    
    # 2. Schema metadata
    if table.columns:
        fields = []
        for col in table.columns:
            field_obj = SchemaFieldClass(
                fieldPath=col.name,
                type=SchemaFieldDataTypeClass(type=dbml_type_to_datahub(col.data_type)),
                nativeDataType=col.data_type,
                description=col.note or f"Column {col.name}",
                nullable=col.nullable,
            )
            fields.append(field_obj)
        
        schema_metadata = SchemaMetadataClass(
            schemaName=table.name,
            platform="urn:li:dataPlatform:postgres",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )
        
        mcp = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=schema_metadata)
        emitter.emit(mcp)
    
    # 3. Tags
    tags = [
        TagAssociationClass(tag="urn:li:tag:Gold"),
    ]
    if domain:
        tags.append(TagAssociationClass(tag=f"urn:li:tag:{domain.capitalize()}"))
    
    global_tags = GlobalTagsClass(tags=tags)
    mcp = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=global_tags)
    emitter.emit(mcp)
    
    logger.info(f"  ✓ Emitted {table.name} with {len(table.columns)} columns")


def emit_glossary_term(emitter: DatahubRestEmitter, term: GlossaryTerm):
    """Emit a glossary term to DataHub."""
    from datahub.metadata.schema_classes import GlossaryTermInfoClass
    
    term_urn = f"urn:li:glossaryTerm:{term.term.replace(' ', '_')}"
    
    term_info = GlossaryTermInfoClass(
        definition=term.definition,
        termSource="INTERNAL",
        name=term.term,
        customProperties={
            "domain": term.domain,
            "synonyms": ", ".join(term.synonyms),
        }
    )
    
    mcp = MetadataChangeProposalWrapper(entityUrn=term_urn, aspect=term_info)
    
    try:
        emitter.emit(mcp)
        logger.info(f"  ✓ Emitted glossary term: {term.term}")
    except Exception as e:
        logger.warning(f"  ⚠ Failed to emit term {term.term}: {e}")


def main():
    """Main function to sync DBML to DataHub."""
    logger.info("=" * 60)
    logger.info("DBML to DataHub Sync")
    logger.info("=" * 60)
    
    # Connect to DataHub
    logger.info(f"Connecting to DataHub at {DATAHUB_GMS_URL}")
    try:
        emitter = DatahubRestEmitter(DATAHUB_GMS_URL)
        emitter.test_connection()
        logger.info("Connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect to DataHub: {e}")
        sys.exit(1)
    
    # Find DBML files
    dbml_files = []
    for root, dirs, files in os.walk(SCHEMA_DIR):
        for f in files:
            if f.endswith('.dbml'):
                dbml_files.append(os.path.join(root, f))
    
    if not dbml_files:
        logger.warning(f"No DBML files found in {SCHEMA_DIR}")
        logger.info("Run 'python scripts/introspect_warehouse.py' first to generate DBML")
    else:
        logger.info(f"Found {len(dbml_files)} DBML file(s)")
    
    # Process DBML files
    all_tables = []
    for dbml_file in dbml_files:
        logger.info(f"Processing: {dbml_file}")
        with open(dbml_file, 'r') as f:
            content = f.read()
        
        tables = parse_dbml(content)
        all_tables.extend(tables)
        logger.info(f"  Parsed {len(tables)} tables")
    
    # Emit tables to DataHub
    success_count = 0
    error_count = 0
    
    for table in all_tables:
        try:
            emit_table_to_datahub(emitter, table)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to emit {table.name}: {e}")
            error_count += 1
    
    # Process glossary
    glossary_path = os.path.join(SCHEMA_DIR, "glossary.yaml")
    if os.path.exists(glossary_path):
        logger.info("Processing glossary...")
        domains, terms = parse_glossary(glossary_path)
        
        for term in terms:
            try:
                emit_glossary_term(emitter, term)
            except Exception as e:
                logger.warning(f"Failed to emit term {term.term}: {e}")
    else:
        logger.info("No glossary.yaml found, skipping glossary sync")
    
    # Summary
    logger.info("=" * 60)
    logger.info("Sync Complete!")
    logger.info(f"  Tables synced: {success_count}")
    logger.info(f"  Errors: {error_count}")
    logger.info("=" * 60)
    logger.info("View catalog at: http://localhost:9002")


if __name__ == "__main__":
    main()
