"""
Silver layer transformation for UWV Open Match vacancies.
"""
from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path, ensure_local_path_exists
from shared.config.settings import get_settings


def run_silver_uwv_open_match(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "uwv_open_match_raw",
    silver_table_name: str = "uwv_open_match",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[UWV Silver] Starting UWV silver transform...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    raw_path = get_lakehouse_table_path(
        table_name=bronze_table_name,
        layer=LakehouseLayer.BRONZE,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )
    silver_path = get_lakehouse_table_path(
        table_name=silver_table_name,
        layer=LakehouseLayer.SILVER,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    df_raw = spark.read.format("delta").load(raw_path)

    df = (
        df_raw.select(
            col("vacancy_id").cast("string").alias("vacancy_id"),
            col("occupation").alias("occupation"),
            col("region").alias("region"),
            col("posted_date").alias("posted_date"),
            col("employment_type").alias("employment_type"),
            col("work_time").alias("work_time"),
            col("ingestion_timestamp"),
            lit("UWV").alias("source"),
        )
    )

    if settings.is_local:
        ensure_local_path_exists(silver_path)

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_path)
    print(f"[UWV Silver] ✓ Wrote silver table -> {silver_path}")
