# TEMPLATE — copy and customise for your new source.
# See docs/INGESTION_GUIDE.md § Step 5 for details.
#
# Rename this file to:
#   dags/<source_name>_<dataset>_ingest_to_silver.py
"""<SOURCE_NAME> — <DATASET> Ingest-to-Silver DAG

Orchestrates source ingestion up to silver:

  Task groups
  ----------
  preflight         Validate connections; ensure MinIO bucket + Postgres DDL.
  bronze            Extract raw data -> write bronze artifacts to MinIO.
  silver            Read bronze artifacts -> parse -> upsert Postgres silver table.
  observability     Emit StatsD metrics and log a run summary.

Use a separate gold-curation DAG to combine/deduplicate multiple silver datasets.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from src.ingestion.common.dag_helpers import (
    make_default_args,
    make_emit_metrics_callable,
    make_ensure_bucket_callable,
    make_validate_connections_callable,
    mime_to_ext,
    try_get_conn,
)

log = logging.getLogger(__name__)

# -- Source constants --------------------------------------------------------
# TODO: Replace with your source's identifiers.
_SOURCE_NAME = "my_source"
_DATASET = "my_dataset"
_DEFAULT_BUCKET = "lakehouse"
_DAG_ID = "template_my_source_my_dataset_ingestion"


# ===========================================================================
# Callables — heavy imports deferred inside functions
# ===========================================================================


def _extract_to_bronze(**kwargs):
    """Extract raw data -> write bronze artifacts to MinIO."""
    from src.ingestion.common.metadata_store import (
        ensure_metadata_tables,
        insert_artifact_inventory,
        insert_pipeline_task_run,
        now_utc,
        upsert_pipeline_run,
    )
    from src.ingestion.common.minio import write_bronze_artifact
    from src.ingestion.common.provenance import build_meta

    # TODO: Import your source extractor
    # from src.ingestion.my_source.extract_my_dataset import extract_all

    task_start = now_utc()
    run_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = kwargs.get("run_id") or f"{_SOURCE_NAME}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    bucket = os.environ.get("MINIO_BUCKET", _DEFAULT_BUCKET)

    minio_conn = try_get_conn("minio")
    pg_conn = try_get_conn("postgres_warehouse")
    ensure_metadata_tables(conn=pg_conn)

    artifact_summaries: list[dict] = []
    extracted_count = 0

    # TODO: Call your extractor.
    # raw_artifacts = extract_all(run_id=run_id, run_dt=run_dt)
    raw_artifacts = []

    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name=_SOURCE_NAME,
        dataset=_DATASET,
        status="RUNNING",
        triggered_by=os.environ.get("USER", "airflow"),
        code_version=os.environ.get("GITHUB_SHA", "unknown"),
        started_at_utc=task_start,
        metadata={"phase": "bronze_extract"},
        conn=pg_conn,
    )

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
        artifact_summaries.append(
            {
                "artifact_id": art.artifact_id,
                "raw_key": raw_key,
                "meta_key": meta_key,
                "extraction_method": art.extraction_method,
                "byte_size": len(art.raw_bytes),
                "content_type": art.content_type,
                "url": art.url,
            }
        )
        insert_artifact_inventory(
            run_id=run_id,
            source_name=_SOURCE_NAME,
            dataset=_DATASET,
            artifact_id=art.artifact_id,
            layer="bronze",
            bucket=bucket,
            object_key=raw_key,
            meta_key=meta_key,
            run_dt=run_dt,
            fetched_at_utc=meta.fetched_at_utc,
            url=meta.url,
            canonical_url=meta.canonical_url,
            http_status=meta.http_status,
            content_type=meta.content_type,
            response_ms=meta.response_ms,
            checksum_sha256=meta.checksum_sha256,
            byte_size=meta.byte_size,
            extraction_method=meta.extraction_method,
            entity_keys=meta.entity_keys,
            conn=pg_conn,
        )
        extracted_count += 1

    ti = kwargs["ti"]
    ti.xcom_push(key="run_id", value=run_id)
    ti.xcom_push(key="run_dt", value=run_dt)
    ti.xcom_push(key="extracted_count", value=extracted_count)
    ti.xcom_push(key="bronze_bucket", value=bucket)
    ti.xcom_push(key="artifact_summaries", value=artifact_summaries)

    insert_pipeline_task_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        task_id="extract_to_bronze",
        task_group="bronze",
        status="SUCCESS",
        started_at_utc=task_start,
        finished_at_utc=now_utc(),
        metadata={"extracted_count": extracted_count},
        conn=pg_conn,
    )

    log.info("[bronze] Done - %d artifacts for run_id=%s", extracted_count, run_id)


def _parse_and_load(**kwargs):
    """Read bronze artifacts -> parse -> upsert Postgres silver table."""
    from src.ingestion.common.metadata_store import (
        ensure_metadata_tables,
        insert_dataset_version,
        insert_pipeline_task_run,
        now_utc,
        upsert_dataset_registry,
        upsert_pipeline_run,
    )
    from src.ingestion.common.minio import read_bronze_raw
    from src.ingestion.common.postgres import update_ingestion_state

    # TODO: Import your parser + source config
    # from src.ingestion.my_source.config import MY_SOURCE_CONFIG
    # from src.ingestion.my_source.parse_my_dataset import parse_artifacts
    # from src.ingestion.common.postgres import upsert_records

    ti = kwargs["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="bronze.extract_to_bronze")
    run_dt = ti.xcom_pull(key="run_dt", task_ids="bronze.extract_to_bronze")
    bucket = ti.xcom_pull(key="bronze_bucket", task_ids="bronze.extract_to_bronze")
    summaries: list[dict] = ti.xcom_pull(key="artifact_summaries", task_ids="bronze.extract_to_bronze") or []

    minio_conn = try_get_conn("minio")
    pg_conn = try_get_conn("postgres_warehouse")
    task_start = now_utc()
    ensure_metadata_tables(conn=pg_conn)

    raw_artifacts: list[dict] = []
    for s in summaries:
        raw_key = s.get("raw_key", "")
        if not raw_key:
            continue
        try:
            raw_bytes = read_bronze_raw(bucket=bucket, key=raw_key, conn=minio_conn)
            raw_artifacts.append(
                {
                    "artifact_id": s["artifact_id"],
                    "raw_bytes": raw_bytes,
                    "content_type": s.get("content_type", ""),
                    "extraction_method": s.get("extraction_method", ""),
                    "url": s.get("url", ""),
                    "bronze_object_path": raw_key,
                }
            )
        except Exception as exc:
            log.error("Could not read bronze artifact %s: %s", raw_key, exc)

    if not raw_artifacts:
        raise RuntimeError(f"No bronze artifacts readable from bucket '{bucket}' for run_dt={run_dt}.")

    # TODO: records = parse_artifacts(raw_artifacts)
    records = []
    parsed_count = len(records)

    # TODO: upserted_count = upsert_records(records, MY_SOURCE_CONFIG, conn=pg_conn)
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

    silver_dataset_id = f"{_SOURCE_NAME}.{_DATASET}"
    upsert_dataset_registry(
        dataset_id=silver_dataset_id,
        layer="silver",
        domain=_SOURCE_NAME,
        schema_name=_SOURCE_NAME,
        table_name=_DATASET,
        metadata={"pipeline": _DAG_ID},
        conn=pg_conn,
    )
    insert_dataset_version(
        dataset_id=silver_dataset_id,
        version_label=run_id,
        schema_hash=None,
        column_schema=[],
        row_count=parsed_count,
        byte_size=None,
        run_id=run_id,
        metadata={"upserted_count": upserted_count, "cursor": {"run_dt": run_dt}},
        conn=pg_conn,
    )

    ti.xcom_push(key="parsed_count", value=parsed_count)
    ti.xcom_push(key="upserted_count", value=upserted_count)

    insert_pipeline_task_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        task_id="parse_and_load",
        task_group="silver",
        status="SUCCESS",
        started_at_utc=task_start,
        finished_at_utc=now_utc(),
        metadata={"parsed_count": parsed_count, "upserted_count": upserted_count},
        conn=pg_conn,
    )
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name=_SOURCE_NAME,
        dataset=_DATASET,
        status="SUCCESS",
        finished_at_utc=now_utc(),
        metadata={"phase": "silver_complete"},
        conn=pg_conn,
    )

    log.info("[silver] Parsed %d records, upserted %d.", parsed_count, upserted_count)


# ===========================================================================
# DAG definition
# ===========================================================================

# TODO: Replace MY_SOURCE_CONFIG with your config import.
# from src.ingestion.common.dag_helpers import make_ensure_ddl_callable
# from src.ingestion.my_source.config import MY_SOURCE_CONFIG
# _ddl_callable = make_ensure_ddl_callable(MY_SOURCE_CONFIG)
_ddl_callable = None  # placeholder - replace with line above

with DAG(
    dag_id=_DAG_ID,
    default_args=make_default_args(),
    description=f"Ingest {_SOURCE_NAME} {_DATASET} -> MinIO bronze -> Postgres silver",
    schedule_interval="@daily",
    catchup=False,
    tags=[_SOURCE_NAME, _DATASET, "ingestion", "bronze", "silver"],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
) as dag:
    with TaskGroup("preflight", tooltip="Validate connections and infrastructure") as preflight:
        t_validate = PythonOperator(
            task_id="validate_connections",
            python_callable=make_validate_connections_callable(
                [
                    # TODO: Add your source-specific connection
                    # {"conn_id": "my_source", "env_fallback": "MY_SOURCE_USERNAME", "label": "My Source"},
                    {"conn_id": "minio", "env_fallback": "MINIO_ENDPOINT", "label": "MinIO"},
                    {"conn_id": "postgres_warehouse", "env_fallback": "WAREHOUSE_HOST", "label": "Postgres"},
                ]
            ),
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

    with TaskGroup("bronze", tooltip="Extract raw data -> bronze MinIO") as bronze:
        t_extract = PythonOperator(
            task_id="extract_to_bronze",
            python_callable=_extract_to_bronze,
            retries=2,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=True,
            sla=timedelta(minutes=45),
            execution_timeout=timedelta(hours=1),
        )

    with TaskGroup("silver", tooltip="Parse bronze -> upsert Postgres silver") as silver:
        t_parse = PythonOperator(
            task_id="parse_and_load",
            python_callable=_parse_and_load,
            retries=2,
            retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=15),
        )

    with TaskGroup("observability", tooltip="Metrics and run summary") as observability:
        t_metrics = PythonOperator(
            task_id="emit_metrics",
            python_callable=make_emit_metrics_callable(source_name=_SOURCE_NAME),
            trigger_rule="all_done",
        )

    preflight >> bronze >> silver >> observability
