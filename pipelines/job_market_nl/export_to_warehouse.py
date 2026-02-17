"""
Export job market gold layer tables to PostgreSQL warehouse for BI.
"""
from __future__ import annotations

import os
from typing import Any, Optional

from pyspark.sql import SparkSession

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path
from shared.config.settings import get_settings
from shared.utils.export_helper import read_lakehouse_table

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5433")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "admin")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")
WAREHOUSE_SCHEMA = "job_market_nl"

JDBC_URL = f"jdbc:postgresql://{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"

TABLES_TO_EXPORT = [
    "it_market_snapshot",
    "it_market_top_skills",
]


def create_schema_if_not_exists() -> None:
    import psycopg2

    conn = psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        database=WAREHOUSE_DB,
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA}")
            conn.commit()
            print(f"[Job Market Export] ✓ Schema '{WAREHOUSE_SCHEMA}' ready")
    finally:
        conn.close()


def export_table_to_warehouse(
    spark: SparkSession,
    table_name: str,
    workspace_id: str,
) -> bool:
    source_path = get_lakehouse_table_path(
        table_name=table_name,
        layer=LakehouseLayer.GOLD,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    try:
        df = read_lakehouse_table(spark, source_path)
        count = df.count()
        if count == 0:
            print(f"[Job Market Export] Skipping {table_name} - empty table")
            return False

        target_table = f"{WAREHOUSE_SCHEMA}.{table_name}"

        df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", target_table) \
            .option("user", WAREHOUSE_USER) \
            .option("password", WAREHOUSE_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        print(f"[Job Market Export] ✓ Exported {table_name} ({count} rows) -> {target_table}")
        return True
    except Exception as e:
        print(f"[Job Market Export] ✗ Error exporting {table_name}: {e}")
        return False


def run_export_job_market_to_warehouse(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    workspace_id: Optional[str] = None,
    tables: Optional[list[str]] = None,
) -> dict[str, bool]:
    settings = get_settings()
    print("[Job Market Export] Starting warehouse export...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    if tables is None:
        tables = TABLES_TO_EXPORT

    try:
        create_schema_if_not_exists()
    except Exception as e:
        print(f"[Job Market Export] Warning: Could not create schema: {e}")

    results = {}
    for table_name in tables:
        results[table_name] = export_table_to_warehouse(
            spark=spark,
            table_name=table_name,
            workspace_id=workspace_id,
        )

    successful = sum(1 for v in results.values() if v)
    print(f"[Job Market Export] ✓ Completed: {successful}/{len(tables)} tables exported")

    return results
