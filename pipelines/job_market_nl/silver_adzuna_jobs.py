"""
Silver layer transformation for Adzuna job ads.
"""
from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path, ensure_local_path_exists
from shared.config.settings import get_settings


def run_silver_adzuna_jobs(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "adzuna_job_ads_raw",
    silver_table_name: str = "adzuna_job_ads",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[Adzuna Silver] Starting Adzuna silver transform...")

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
            col("id").cast("string").alias("job_id"),
            col("title").alias("title"),
            col("company_display_name").alias("company"),
            col("location_display_name").alias("location"),
            col("created").alias("posted_at"),
            col("description").alias("description"),
            col("contract_time").alias("contract_time"),
            col("contract_type").alias("contract_type"),
            col("salary_min").cast("double").alias("salary_min"),
            col("salary_max").cast("double").alias("salary_max"),
            col("salary_is_predicted").alias("salary_is_predicted"),
            col("ingestion_timestamp"),
            lit("ADZUNA").alias("source"),
        )
    )

    if settings.is_local:
        ensure_local_path_exists(silver_path)

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_path)
    print(f"[Adzuna Silver] ✓ Wrote silver table -> {silver_path}")
