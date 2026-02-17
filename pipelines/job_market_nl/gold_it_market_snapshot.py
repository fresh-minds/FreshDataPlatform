"""
Gold layer aggregations for IT job market in the Netherlands.
"""
from __future__ import annotations

from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, lit, lower, udf
from pyspark.sql.types import ArrayType, StringType

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path, ensure_local_path_exists
from shared.config.settings import get_settings
from pipelines.job_market_nl.utils import extract_skills_from_text


def run_gold_it_market_snapshot(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    silver_cbs_table: str = "cbs_vacancy_rate",
    silver_adzuna_table: str = "adzuna_job_ads",
    gold_snapshot_table: str = "it_market_snapshot",
    gold_skills_table: str = "it_market_top_skills",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[Job Market Gold] Starting gold aggregations...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    cbs_path = get_lakehouse_table_path(
        table_name=silver_cbs_table,
        layer=LakehouseLayer.SILVER,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    adzuna_path = get_lakehouse_table_path(
        table_name=silver_adzuna_table,
        layer=LakehouseLayer.SILVER,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    snapshot_path = get_lakehouse_table_path(
        table_name=gold_snapshot_table,
        layer=LakehouseLayer.GOLD,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    skills_path = get_lakehouse_table_path(
        table_name=gold_skills_table,
        layer=LakehouseLayer.GOLD,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    df_cbs = spark.read.format("delta").load(cbs_path)

    latest_period = df_cbs.select("period_key").distinct().orderBy(desc("period_key")).limit(1)
    df_latest = df_cbs.join(latest_period, on="period_key", how="inner")

    df_it = df_latest.filter(lower(col("sector_name")).contains("information"))

    df_snapshot = df_it.select(
        col("period_key"),
        col("period_label"),
        col("sector_name"),
        col("vacancies"),
        col("vacancy_rate"),
    )

    # Job ads summary
    try:
        df_ads = spark.read.format("delta").load(adzuna_path)
        df_ads_count = df_ads.count()
    except Exception:
        df_ads = None
        df_ads_count = 0

    if df_ads is not None and df_ads_count > 0:
        df_snapshot = df_snapshot.withColumn("job_ads_count", lit(df_ads_count))
    else:
        df_snapshot = df_snapshot.withColumn("job_ads_count", lit(0))

    if settings.is_local:
        ensure_local_path_exists(snapshot_path)

    df_snapshot.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(snapshot_path)
    print(f"[Job Market Gold] ✓ Wrote snapshot -> {snapshot_path}")

    if df_ads is not None and df_ads_count > 0:
        skill_udf = udf(lambda text: extract_skills_from_text(text), ArrayType(StringType()))
        df_skills = (
            df_ads
            .withColumn("skills", skill_udf(col("description")))
            .withColumn("skill", explode(col("skills")))
            .groupBy("skill")
            .count()
            .orderBy(desc("count"))
        )

        if settings.is_local:
            ensure_local_path_exists(skills_path)

        df_skills.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(skills_path)
        print(f"[Job Market Gold] ✓ Wrote top skills -> {skills_path}")
    else:
        print("[Job Market Gold] No job ads available for skills extraction.")
