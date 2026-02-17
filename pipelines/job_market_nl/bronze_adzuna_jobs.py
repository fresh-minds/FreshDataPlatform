"""
Bronze ingestion for Adzuna job ads (Netherlands).
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

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"


def fetch_adzuna_jobs(app_id: str, app_key: str, country: str, query: str, results_per_page: int = 50) -> List[Dict[str, Any]]:
    url = f"{ADZUNA_BASE_URL}/{country}/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": query,
        "results_per_page": results_per_page,
        "content-type": "application/json",
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("results", [])


def run_bronze_adzuna_jobs(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "adzuna_job_ads_raw",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[Adzuna Bronze] Starting Adzuna job ads ingestion...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    table_path = get_lakehouse_table_path(
        table_name=bronze_table_name,
        layer=LakehouseLayer.BRONZE,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    country = os.getenv("ADZUNA_COUNTRY", "nl")
    query = os.getenv("ADZUNA_QUERY", "software engineer OR data engineer OR devops")
    use_mock = settings.is_local and os.getenv("LOCAL_MOCK_EXTERNAL", "true").lower() == "true"

    if not app_id or not app_key:
        if use_mock:
            print("[Adzuna Bronze] Missing Adzuna credentials, using mock data.")
            jobs = [
                {
                    "id": "mock-adzuna-1",
                    "title": "Data Engineer",
                    "company": {"display_name": "MockCo"},
                    "location": {"display_name": "Amsterdam"},
                    "created": "2024-01-15T10:00:00Z",
                    "description": "Data engineer working with Python, SQL, and AWS.",
                    "salary_min": 55000,
                    "salary_max": 70000,
                    "salary_is_predicted": "0",
                    "contract_time": "full_time",
                    "contract_type": "permanent",
                }
            ]
        else:
            print("[Adzuna Bronze] Missing Adzuna credentials. Skipping ingestion.")
            return
    else:
        jobs = fetch_adzuna_jobs(app_id, app_key, country, query)

    if not jobs:
        print("[Adzuna Bronze] No job ads returned.")
        return

    # Flatten nested fields (company/location) before Spark conversion
    pdf = pd.json_normalize(jobs, sep="_")
    pdf = clean_df_for_spark(pdf)
    df_spark = spark.createDataFrame(pdf).withColumn("ingestion_timestamp", current_timestamp())

    if settings.is_local:
        ensure_local_path_exists(table_path)

    df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)
    print(f"[Adzuna Bronze] ✓ Ingested {len(jobs)} rows -> {table_path}")
