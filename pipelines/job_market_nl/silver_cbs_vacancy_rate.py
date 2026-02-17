"""
Silver layer transformation for CBS vacancy rate data.
"""
from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path, ensure_local_path_exists
from shared.config.settings import get_settings


def _find_column(columns: list[str], candidates: list[str]) -> Optional[str]:
    for candidate in candidates:
        for col_name in columns:
            if col_name.lower().startswith(candidate.lower()):
                return col_name
    return None


def run_silver_cbs_vacancy_rate(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "cbs_vacancy_rate_raw",
    bronze_dim_sic_table: str = "cbs_vacancy_rate_dim_sic2008",
    bronze_dim_periods_table: str = "cbs_vacancy_rate_dim_periods",
    silver_table_name: str = "cbs_vacancy_rate",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[CBS Silver] Starting CBS vacancy rate silver transform...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    raw_path = get_lakehouse_table_path(
        table_name=bronze_table_name,
        layer=LakehouseLayer.BRONZE,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )
    dim_sic_path = get_lakehouse_table_path(
        table_name=bronze_dim_sic_table,
        layer=LakehouseLayer.BRONZE,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )
    dim_periods_path = get_lakehouse_table_path(
        table_name=bronze_dim_periods_table,
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
    cols = df_raw.columns

    vacancies_col = _find_column(cols, ["Vacancies"])
    vacancy_rate_col = _find_column(cols, ["VacancyRate", "Vacancy_rate"]) 

    if not vacancies_col or not vacancy_rate_col:
        raise ValueError("[CBS Silver] Could not locate vacancy columns in raw dataset.")

    df_sic = spark.read.format("delta").load(dim_sic_path).withColumnRenamed("Title", "sector_label")
    df_periods = spark.read.format("delta").load(dim_periods_path).withColumnRenamed("Title", "period_label")

    df = (
        df_raw
        .join(df_sic, df_raw["SIC2008"] == df_sic["Key"], "left")
        .join(df_periods, df_raw["Periods"] == df_periods["Key"], "left")
        .select(
            col("SIC2008").alias("sector_code"),
            col("sector_label").alias("sector_name"),
            col("Periods").alias("period_key"),
            col("period_label").alias("period_label"),
            col(vacancies_col).cast("double").alias("vacancies"),
            col(vacancy_rate_col).cast("double").alias("vacancy_rate"),
            col("ingestion_timestamp"),
            lit("CBS").alias("source"),
        )
    )

    if settings.is_local:
        ensure_local_path_exists(silver_path)

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_path)
    print(f"[CBS Silver] ✓ Wrote silver table -> {silver_path}")
