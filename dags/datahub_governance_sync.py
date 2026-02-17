from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from scripts.datahub_client import DataHubService

# This DAG serves as an example of how to manually emit metadata to DataHub
# Note: Automatic lineage should be handled by the acryl-datahub-airflow-plugin

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def emit_custom_lineage():
    dh = DataHubService(
        host=os.getenv("DATAHUB_GMS_HOST"),
        port=os.getenv("DATAHUB_GMS_PORT"),
    )
    # Example: Manual lineage from bronze to silver
    dh.emit_lineage(
        upstream_urns=["urn:li:dataset:(urn:li:dataPlatform:s3,bronze.finance,PROD)"],
        downstream_urn="urn:li:dataset:(urn:li:dataPlatform:postgres,silver.finance,PROD)"
    )

with DAG(
    'datahub_governance_sync',
    default_args=default_args,
    description='Sync governance metadata to DataHub',
    schedule_interval=None,
    catchup=False
) as dag:

    sync_task = PythonOperator(
        task_id='emit_manual_lineage',
        python_callable=emit_custom_lineage
    )
