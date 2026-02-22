# TEMPLATE — copy and customise for your new source.
# See docs/INGESTION_GUIDE.md § Step 5 for details.
#
# Rename this file to:
#   dags/<source_name>_<dataset>_ingestion.py
"""<SOURCE_NAME> — <DATASET> Ingestion DAG

Orchestrates the full medallion-architecture ingestion:

  Task groups
  ──────────
  preflight         Validate connections; ensure MinIO bucket + Postgres DDL.
  extract           Extract raw data → write bronze to MinIO.
  parse_load        Read bronze artifacts → parse → upsert Postgres silver table.
  dbt_run           dbt deps + run + test on staging → intermediate → marts.
  observability     Emit StatsD metrics and log a run summary.

Configuration (Airflow Connections + Variables / env vars):
  AIRFLOW_CONN_<SOURCE>                source credentials
  AIRFLOW_CONN_MINIO                   host=http://minio:9000, login=<key>, password=<secret>
  AIRFLOW_CONN_POSTGRES_WAREHOUSE      standard Postgres connection

  Env vars (fallback):
    <SOURCE>_USERNAME / _PASSWORD / _BASE_URL
    MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY
    WAREHOUSE_HOST / WAREHOUSE_PORT / WAREHOUSE_DB / WAREHOUSE_USER / WAREHOUSE_PASSWORD
    MINIO_BUCKET                        (default: lakehouse)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from src.ingestion.common.dag_helpers import (
    make_default_args,
    make_dbt_run_callable,
    make_emit_metrics_callable,
    make_ensure_bucket_callable,
    make_ensure_ddl_callable,
    make_validate_connections_callable,
    mime_to_ext,
    try_get_conn,
)

log = logging.getLogger(__name__)

# ── Source constants ─────────────────────────────────────────────────────────
# TODO: Replace with your source's identifiers.
_SOURCE_NAME = "my_source"
_DATASET = "my_dataset"
_DEFAULT_BUCKET = "lakehouse"


# ===========================================================================
# Import your source-specific config and modules
# ===========================================================================
# from src.ingestion.my_source.config import MY_SOURCE_CONFIG


# ===========================================================================
# Callables — heavy imports deferred inside functions
# ===========================================================================

def _extract_to_bronze(**kwargs):
    """Extract raw data → write bronze artifacts to MinIO.

    XCom pushes:
      run_id            str
      run_dt            str  (YYYY-MM-DD)
      extracted_count   int
      bronze_bucket     str
      artifact_summaries list[dict]
    """
    from datetime import datetime, timezone

    from src.ingestion.common.minio import write_bronze_artifact
    from src.ingestion.common.provenance import build_meta
    # TODO: Import your source's extract module
    # from src.ingestion.my_source.extract_my_dataset import extract_all

    run_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = kwargs.get("run_id") or (
        f"{_SOURCE_NAME}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    bucket = os.environ.get("MINIO_BUCKET", _DEFAULT_BUCKET)

    minio_conn = try_get_conn("minio")

    artifact_summaries: list[dict] = []
    extracted_count = 0

    # TODO: Call your extractor.
    # If browser-based, wrap in sync_playwright() context (see existing DAGs).
    # If API-based, call extract_all() directly.
    #
    # raw_artifacts = extract_all(run_id=run_id, run_dt=run_dt)
    raw_artifacts = []

    for art in raw_artifacts:
        ext = mime_to_ext(art.content_type)
        meta = build_meta(
            source_name=_SOURCE_NAME,
            dataset=_DATASET,
            run_id=run_id,
            artifact_id=art.artifact_id,
            url=art.url,
            canonical_url=art.url,
            http_status=art.http_status,
            content_type=art.content_type,
            response_ms=art.response_ms,
            raw_bytes=art.raw_bytes,
            extraction_method=art.extraction_method,
            entity_keys=art.entity_keys,
        )
        raw_key, meta_key = write_bronze_artifact(
            bucket=bucket,
            run_dt=run_dt,
            raw_bytes=art.raw_bytes,
            meta=meta,
            extension=ext,
            conn=minio_conn,
        )
        artifact_summaries.append({
            "artifact_id": art.artifact_id,
            "raw_key": raw_key,
            "meta_key": meta_key,
            "extraction_method": art.extraction_method,
            "byte_size": len(art.raw_bytes),
            "content_type": art.content_type,
            "url": art.url,
        })
        extracted_count += 1

    ti = kwargs["ti"]
    ti.xcom_push(key="run_id", value=run_id)
    ti.xcom_push(key="run_dt", value=run_dt)
    ti.xcom_push(key="extracted_count", value=extracted_count)
    ti.xcom_push(key="bronze_bucket", value=bucket)
    ti.xcom_push(key="artifact_summaries", value=artifact_summaries)

    log.info("[extract] Done — %d artifacts for run_id=%s", extracted_count, run_id)


def _parse_and_load(**kwargs):
    """Read bronze artifacts → parse → upsert Postgres silver table.

    XCom pushes:
      parsed_count    int
      upserted_count  int
    """
    from datetime import datetime, timezone

    from src.ingestion.common.minio import read_bronze_raw
    from src.ingestion.common.postgres import update_ingestion_state, upsert_records
    # TODO: Import your source's config and parser
    # from src.ingestion.my_source.config import MY_SOURCE_CONFIG
    # from src.ingestion.my_source.parse_my_dataset import parse_artifacts

    ti = kwargs["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="extract.extract_to_bronze")
    run_dt = ti.xcom_pull(key="run_dt", task_ids="extract.extract_to_bronze")
    bucket = ti.xcom_pull(key="bronze_bucket", task_ids="extract.extract_to_bronze")
    summaries: list[dict] = ti.xcom_pull(
        key="artifact_summaries", task_ids="extract.extract_to_bronze"
    ) or []

    minio_conn = try_get_conn("minio")
    pg_conn = try_get_conn("postgres_warehouse")

    # Read raw bytes back from MinIO
    raw_artifacts: list[dict] = []
    for s in summaries:
        raw_key = s.get("raw_key", "")
        if not raw_key:
            continue
        try:
            raw_bytes = read_bronze_raw(bucket=bucket, key=raw_key, conn=minio_conn)
            raw_artifacts.append({
                "artifact_id": s["artifact_id"],
                "raw_bytes": raw_bytes,
                "content_type": s.get("content_type", ""),
                "extraction_method": s.get("extraction_method", ""),
                "url": s.get("url", ""),
                "bronze_object_path": raw_key,
            })
        except Exception as exc:
            log.error("Could not read bronze artifact %s: %s", raw_key, exc)

    if not raw_artifacts:
        raise RuntimeError(
            f"No bronze artifacts readable from bucket '{bucket}' for run_dt={run_dt}."
        )

    # TODO: Uncomment and wire in your parser + config
    # records = parse_artifacts(raw_artifacts)
    records = []
    parsed_count = len(records)

    # TODO: Replace MY_SOURCE_CONFIG with your config
    # upserted_count = upsert_records(records, MY_SOURCE_CONFIG, conn=pg_conn)
    upserted_count = 0

    update_ingestion_state(
        source_name=_SOURCE_NAME,
        dataset=_DATASET,
        last_success_utc=datetime.now(timezone.utc),
        cursor_json={"run_id": run_id, "run_dt": run_dt},
        extracted_count=parsed_count,
        upserted_count=upserted_count,
        conn=pg_conn,
    )

    ti.xcom_push(key="parsed_count", value=parsed_count)
    ti.xcom_push(key="upserted_count", value=upserted_count)
    log.info("[parse_load] Parsed %d records, upserted %d.", parsed_count, upserted_count)


# ===========================================================================
# DAG definition
# ===========================================================================

# TODO: Replace MY_SOURCE_CONFIG with your config import
# _ddl_callable = make_ensure_ddl_callable(MY_SOURCE_CONFIG)
_ddl_callable = None  # placeholder — replace with line above

with DAG(
    dag_id=f"{_SOURCE_NAME}_{_DATASET}_ingestion",
    default_args=make_default_args(),
    description=f"Ingest {_SOURCE_NAME} {_DATASET} → MinIO bronze → Postgres silver → dbt gold",
    schedule_interval="@daily",
    catchup=False,
    tags=[_SOURCE_NAME, _DATASET, "ingestion", "bronze", "silver", "gold"],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
) as dag:

    # ── Preflight ─────────────────────────────────────────────────────────
    with TaskGroup("preflight", tooltip="Validate connections and infrastructure") as preflight:
        t_validate = PythonOperator(
            task_id="validate_connections",
            python_callable=make_validate_connections_callable([
                # TODO: Add your source-specific connection
                # {"conn_id": "my_source", "env_fallback": "MY_SOURCE_USERNAME", "label": "My Source"},
                {"conn_id": "minio", "env_fallback": "MINIO_ENDPOINT", "label": "MinIO"},
                {"conn_id": "postgres_warehouse", "env_fallback": "WAREHOUSE_HOST", "label": "Postgres"},
            ]),
            retries=0,
        )
        t_bucket = PythonOperator(
            task_id="ensure_minio_bucket",
            python_callable=make_ensure_bucket_callable(),
            retries=2,
            retry_delay=timedelta(seconds=30),
        )
        # TODO: Uncomment when _ddl_callable is set
        # t_ddl = PythonOperator(
        #     task_id="ensure_postgres_ddl",
        #     python_callable=_ddl_callable,
        #     retries=2,
        #     retry_delay=timedelta(seconds=30),
        # )
        # t_validate >> [t_bucket, t_ddl]
        t_validate >> t_bucket

    # ── Extract ───────────────────────────────────────────────────────────
    with TaskGroup("extract", tooltip="Extract raw data → bronze MinIO") as extract:
        t_extract = PythonOperator(
            task_id="extract_to_bronze",
            python_callable=_extract_to_bronze,
            retries=2,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=True,
            sla=timedelta(minutes=45),
            execution_timeout=timedelta(hours=1),
        )

    # ── Parse and Load ────────────────────────────────────────────────────
    with TaskGroup("parse_load", tooltip="Parse bronze → upsert Postgres silver") as parse_load:
        t_parse = PythonOperator(
            task_id="parse_and_load",
            python_callable=_parse_and_load,
            retries=2,
            retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=15),
        )

    # ── dbt Run ───────────────────────────────────────────────────────────
    # TODO: Replace selector with your dbt model name + "+"
    with TaskGroup("dbt_run", tooltip="dbt run + test: staging → intermediate → marts") as dbt_run_group:
        t_dbt = PythonOperator(
            task_id="dbt_run_and_test",
            python_callable=make_dbt_run_callable(
                select=f"stg_{_SOURCE_NAME}__{_DATASET}+",
                source_name=_SOURCE_NAME,
            ),
            retries=1,
            retry_delay=timedelta(minutes=5),
            sla=timedelta(minutes=20),
        )

    # ── Observability ─────────────────────────────────────────────────────
    with TaskGroup("observability", tooltip="Metrics and run summary") as observability:
        t_metrics = PythonOperator(
            task_id="emit_metrics",
            python_callable=make_emit_metrics_callable(source_name=_SOURCE_NAME),
            trigger_rule="all_done",
        )

    t_success_gate = EmptyOperator(task_id="pipeline_success_gate")

    # ── Dependency chain ──────────────────────────────────────────────────
    preflight >> extract >> parse_load >> dbt_run_group
    dbt_run_group >> observability
    dbt_run_group >> t_success_gate
