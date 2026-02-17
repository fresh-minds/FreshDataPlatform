"""
Bronze ingestion for CBS vacancy rate data (Netherlands).
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path, ensure_local_path_exists
from shared.config.settings import get_settings
from shared.utils.spark_helpers import clean_df_for_spark

CBS_BASE_URL = "https://opendata.cbs.nl/ODataApi/OData"
CBS_TABLE_ID = os.getenv("CBS_VACANCY_RATE_TABLE", "80567ENG")


def _fetch_odata(url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    next_url = url
    while next_url:
        response = requests.get(next_url, params=params)
        response.raise_for_status()
        payload = response.json()
        all_rows.extend(payload.get("value", []))
        next_url = payload.get("@odata.nextLink")
        params = None
    return all_rows


def fetch_cbs_typed_dataset(table_id: str) -> pd.DataFrame:
    url = f"{CBS_BASE_URL}/{table_id}/TypedDataSet"
    rows = _fetch_odata(url)
    return pd.DataFrame(rows)


def fetch_cbs_dimension(table_id: str, dimension: str) -> pd.DataFrame:
    url = f"{CBS_BASE_URL}/{table_id}/{dimension}"
    rows = _fetch_odata(url)
    return pd.DataFrame(rows)


def run_bronze_cbs_vacancy_rate(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "cbs_vacancy_rate_raw",
    bronze_dim_sic_table: str = "cbs_vacancy_rate_dim_sic2008",
    bronze_dim_periods_table: str = "cbs_vacancy_rate_dim_periods",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[CBS Bronze] Starting CBS vacancy rate ingestion...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    table_path = get_lakehouse_table_path(
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

    use_mock = settings.is_local and os.getenv("LOCAL_MOCK_EXTERNAL", "true").lower() == "true"

    if use_mock:
        print("[CBS Bronze] Using mock CBS vacancy data.")
        data = [
            {
                "SIC2008": "J",
                "Periods": "2023Q4",
                "Vacancies_1": 12000,
                "VacancyRate_2": 3.2,
            },
            {
                "SIC2008": "J",
                "Periods": "2024Q1",
                "Vacancies_1": 11000,
                "VacancyRate_2": 2.9,
            },
        ]
        dim_sic = [
            {"Key": "J", "Title": "Information and communication"},
        ]
        dim_periods = [
            {"Key": "2023Q4", "Title": "2023 Q4"},
            {"Key": "2024Q1", "Title": "2024 Q1"},
        ]
    else:
        data_df = fetch_cbs_typed_dataset(CBS_TABLE_ID)
        data = data_df.to_dict(orient="records")
        dim_sic_df = fetch_cbs_dimension(CBS_TABLE_ID, "SIC2008")
        dim_periods_df = fetch_cbs_dimension(CBS_TABLE_ID, "Periods")
        dim_sic = dim_sic_df.to_dict(orient="records")
        dim_periods = dim_periods_df.to_dict(orient="records")

    if not data:
        print("[CBS Bronze] No data returned from CBS.")
        return

    pdf = clean_df_for_spark(pd.DataFrame(data))
    df_spark = spark.createDataFrame(pdf).withColumn("ingestion_timestamp", current_timestamp())

    if settings.is_local:
        ensure_local_path_exists(table_path)

    df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)
    print(f"[CBS Bronze] ✓ Ingested {len(data)} rows -> {table_path}")

    if dim_sic:
        dim_sic_pdf = clean_df_for_spark(pd.DataFrame(dim_sic))
        dim_sic_df = spark.createDataFrame(dim_sic_pdf)
        dim_sic_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dim_sic_path)
        print(f"[CBS Bronze] ✓ Ingested SIC dimension -> {dim_sic_path}")

    if dim_periods:
        dim_periods_pdf = clean_df_for_spark(pd.DataFrame(dim_periods))
        dim_periods_df = spark.createDataFrame(dim_periods_pdf)
        dim_periods_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dim_periods_path)
        print(f"[CBS Bronze] ✓ Ingested Period dimension -> {dim_periods_path}")
