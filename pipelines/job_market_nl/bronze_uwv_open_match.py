"""
Bronze ingestion for UWV Open Match vacancy data (URL-based).
"""
from __future__ import annotations

import io
import os
import zipfile
from typing import Any, Optional

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path, ensure_local_path_exists
from shared.config.settings import get_settings
from shared.utils.spark_helpers import clean_df_for_spark


def _read_csv_from_bytes(content: bytes) -> pd.DataFrame:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_names = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                raise ValueError("No CSV found in zip archive")
            with zf.open(csv_names[0]) as csv_file:
                return pd.read_csv(csv_file)
    except zipfile.BadZipFile:
        return pd.read_csv(io.BytesIO(content))


def run_bronze_uwv_open_match(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "uwv_open_match_raw",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[UWV Bronze] Starting UWV Open Match ingestion...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    table_path = get_lakehouse_table_path(
        table_name=bronze_table_name,
        layer=LakehouseLayer.BRONZE,
        domain="job_market_nl",
        workspace_id=workspace_id,
    )

    url = os.getenv("UWV_OPEN_MATCH_URL")
    use_mock = settings.is_local and os.getenv("LOCAL_MOCK_EXTERNAL", "true").lower() == "true"

    if not url:
        if use_mock:
            print("[UWV Bronze] Missing UWV URL, using mock data.")
            data = pd.DataFrame(
                [
                    {
                        "vacancy_id": "uwv-1",
                        "occupation": "Software developer",
                        "region": "Noord-Holland",
                        "posted_date": "2024-01-10",
                        "employment_type": "permanent",
                        "work_time": "full_time",
                    }
                ]
            )
        else:
            print("[UWV Bronze] UWV_OPEN_MATCH_URL not set. Skipping ingestion.")
            return
    else:
        response = requests.get(url)
        response.raise_for_status()
        data = _read_csv_from_bytes(response.content)

    if data.empty:
        print("[UWV Bronze] No rows available.")
        return

    pdf = clean_df_for_spark(data)
    df_spark = spark.createDataFrame(pdf).withColumn("ingestion_timestamp", current_timestamp())

    if settings.is_local:
        ensure_local_path_exists(table_path)

    df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)
    print(f"[UWV Bronze] ✓ Ingested {len(data)} rows -> {table_path}")
