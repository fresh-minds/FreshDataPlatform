"""
Silver layer transformation for Harvey Nash NL job postings.
"""

from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, trim

from shared.config.paths import LakehouseLayer, ensure_local_path_exists, get_lakehouse_table_path
from shared.config.settings import get_settings


def run_silver_harvey_nash_vacatures(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "harvey_nash_vacatures_raw",
    silver_table_name: str = "harvey_nash_vacatures",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[HarveyNash Silver] Starting Harvey Nash silver transform...")

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

    df = df_raw.select(
        col("id").cast("string").alias("job_id"),
        trim(col("title")).cast("string").alias("title"),
        trim(col("company")).cast("string").alias("company"),
        trim(col("location")).cast("string").alias("location"),
        trim(col("contract_type")).cast("string").alias("contract_type"),
        col("description").cast("string").alias("description"),
        col("salary_min").cast("double").alias("salary_min"),
        col("salary_max").cast("double").alias("salary_max"),
        col("url").cast("string").alias("url"),
        to_timestamp(col("posted_date")).alias("posted_date"),
        col("ingestion_timestamp"),
        lit("HARVEY_NASH").alias("source"),
    ).where(col("job_id").isNotNull() & (col("job_id") != ""))

    if settings.is_local:
        ensure_local_path_exists(silver_path)

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_path)
    print(f"[HarveyNash Silver] ✓ Wrote silver table -> {silver_path}")
