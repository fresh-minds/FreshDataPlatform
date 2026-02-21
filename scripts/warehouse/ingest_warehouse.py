import os
import psycopg2
from atlas_client import AtlasService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Warehouse Config
DB_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
DB_PORT = os.getenv("WAREHOUSE_PORT", "5433") # Default to external port for local run
DB_NAME = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
DB_USER = os.getenv("WAREHOUSE_USER", "admin")
DB_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def ingest_warehouse_metadata():
    atlas = AtlasService()
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Register Database Entity
        db_qn = f"{DB_NAME}@warehouse"
        db_entity = {
            "typeName": "rdbms_db",
            "attributes": {
                "qualifiedName": db_qn,
                "name": DB_NAME,
                "clusterName": "open-data-platform-cluster",
                "description": "Main Data Warehouse"
            }
        }
        atlas.create_entity(db_entity)
        
        # 2. Get Tables
        cur.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """)
        tables = cur.fetchall()
        
        for schema, table in tables:
            logger.info(f"Processing Table: {schema}.{table}")
            
            table_qn = f"{DB_NAME}.{schema}.{table}@warehouse"
            
            # Create Table Entity
            table_entity = {
                "typeName": "rdbms_table",
                "attributes": {
                    "qualifiedName": table_qn,
                    "name": table,
                    "db": {"typeName": "rdbms_db", "uniqueAttributes": {"qualifiedName": db_qn}},
                    "description": f"Table {schema}.{table}"
                }
            }
            atlas.create_entity(table_entity)
            
            # 3. Get Columns
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table))
            columns = cur.fetchall()
            
            for col_name, data_type in columns:
                col_qn = f"{table_qn}.{col_name}"
                col_entity = {
                    "typeName": "rdbms_column",
                    "attributes": {
                        "qualifiedName": col_qn,
                        "name": col_name,
                        "dataType": data_type,
                        "table": {"typeName": "rdbms_table", "uniqueAttributes": {"qualifiedName": table_qn}}
                    }
                }
                atlas.create_entity(col_entity)

        conn.close()
            
    except Exception as e:
        logger.error(f"Error ingesting warehouse metadata: {e}")

if __name__ == "__main__":
    ingest_warehouse_metadata()
