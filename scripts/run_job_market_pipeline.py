#!/usr/bin/env python3
"""
Run the NL job market pipeline end-to-end (bronze -> silver -> gold -> export).
Uses mock external data by default if credentials are missing.
"""
from __future__ import annotations

import os
import sys
import subprocess

# Ensure project root is on path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Force local execution
os.environ.setdefault("IS_LOCAL", "true")
# Ensure pipelines are not skipped
os.environ.setdefault("LOCAL_MOCK_PIPELINES", "false")
# Use mock external data when credentials are missing
os.environ.setdefault("LOCAL_MOCK_EXTERNAL", "true")

from dotenv import load_dotenv
load_dotenv()

from shared.fabric.runtime import get_fabric_context, get_spark_session

from pipelines.job_market_nl.bronze_cbs_vacancy_rate import run_bronze_cbs_vacancy_rate
from pipelines.job_market_nl.bronze_adzuna_jobs import run_bronze_adzuna_jobs
from pipelines.job_market_nl.bronze_uwv_open_match import run_bronze_uwv_open_match
from pipelines.job_market_nl.silver_cbs_vacancy_rate import run_silver_cbs_vacancy_rate
from pipelines.job_market_nl.silver_adzuna_jobs import run_silver_adzuna_jobs
from pipelines.job_market_nl.silver_uwv_open_match import run_silver_uwv_open_match
from pipelines.job_market_nl.gold_it_market_snapshot import run_gold_it_market_snapshot
from pipelines.job_market_nl.export_to_warehouse import run_export_job_market_to_warehouse
from pipelines.job_market_nl.postgres_pipeline import run_job_market_nl_postgres_pipeline


def main() -> None:
    # Default to the Java/Spark-free path. Enable Spark explicitly when needed.
    if os.getenv("JOB_MARKET_USE_SPARK", "false").lower() != "true":
        print("[Job Market Runner] JOB_MARKET_USE_SPARK!=true. Running Postgres-only pipeline.")
        run_job_market_nl_postgres_pipeline()
        return

    # Avoid noisy PySpark gateway failures when Java isn't installed locally.
    try:
        java_ok = subprocess.run(["java", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    except FileNotFoundError:
        java_ok = False

    if not java_ok:
        print("[Job Market Runner] Java not found. Running Postgres-only pipeline.")
        run_job_market_nl_postgres_pipeline()
        return

    try:
        spark = get_spark_session("JobMarketNL_Local")
        notebookutils, fabric = get_fabric_context()
    except Exception as exc:
        # Common local failure: missing Java runtime for Spark.
        print(f"[Job Market Runner] Spark unavailable ({exc}). Falling back to Postgres-only pipeline.")
        run_job_market_nl_postgres_pipeline()
        return

    run_bronze_cbs_vacancy_rate(spark, notebookutils, fabric)
    run_bronze_adzuna_jobs(spark, notebookutils, fabric)
    run_bronze_uwv_open_match(spark, notebookutils, fabric)

    run_silver_cbs_vacancy_rate(spark, notebookutils, fabric)
    run_silver_adzuna_jobs(spark, notebookutils, fabric)
    run_silver_uwv_open_match(spark, notebookutils, fabric)

    run_gold_it_market_snapshot(spark, notebookutils, fabric)
    run_export_job_market_to_warehouse(spark, notebookutils, fabric)

    print("[Job Market Runner] ✓ Pipeline completed.")


if __name__ == "__main__":
    main()
