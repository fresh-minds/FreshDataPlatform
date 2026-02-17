from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipelines.job_market_nl.postgres_pipeline import run_job_market_nl_postgres_pipeline


default_args = {
    "owner": "Open Data Platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "job_market_nl_pipeline",
    default_args=default_args,
    description="NL IT job market pipeline (CBS + job ads) -> Postgres for Superset",
    schedule_interval="@daily",
    catchup=False,
    tags=["job_market", "nl", "warehouse", "superset"],
) as dag:
    refresh_job_market = PythonOperator(
        task_id="refresh_job_market_nl",
        python_callable=run_job_market_nl_postgres_pipeline,
    )
