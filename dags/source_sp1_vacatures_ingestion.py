"""Source SP1 — Vacatures Ingestion DAG

Orchestrates the full medallion-architecture ingestion:

  Task groups
  ──────────
  preflight         Validate connections; ensure MinIO bucket + Postgres DDL.
  extract           Playwright login → discover vacatures → write bronze to MinIO.
  parse_load        Read bronze artifacts → parse → upsert Postgres staging table.
  dbt_run           dbt deps + run + test on staging → intermediate → marts (dims + fact).
  observability     Emit StatsD metrics and log a run summary.

  Failure callback  Logs actionable troubleshooting guidance for any task failure.

Configuration (Airflow Connections + Variables / env vars):
  AIRFLOW_CONN_SOURCE_SP1             login=<email>, password=<secret>
                                      extra={"base_url": "<portal_url>"}
  AIRFLOW_CONN_MINIO                  host=http://minio:9000, login=<key>, password=<secret>
  AIRFLOW_CONN_POSTGRES_WAREHOUSE     standard Postgres connection

  Env vars (fallback):
    SP1_USERNAME / SP1_PASSWORD / SP1_BASE_URL
    MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY
    WAREHOUSE_HOST / WAREHOUSE_PORT / WAREHOUSE_DB / WAREHOUSE_USER / WAREHOUSE_PASSWORD
    MINIO_BUCKET                        (default: lakehouse)
    INCREMENTAL_LOOKBACK_DAYS           (default: 7)
    RATE_LIMIT_RPS_PER_DOMAIN           (default: 0.2)
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

_SOURCE_NAME = "source_sp1"
_DATASET = "vacatures"
_DEFAULT_BUCKET = "lakehouse"


# ===========================================================================
# Callables — all heavy imports deferred inside functions to avoid DAG-parse
# import errors (same pattern as job_market_nl_dag.py).
# ===========================================================================


# ---------------------------------------------------------------------------
# Extract to Bronze (source-specific — cannot be genericised)
# ---------------------------------------------------------------------------

def _extract_to_bronze(**kwargs):
    """Login → navigate → extract → write bronze artifacts.

    XCom pushes:
      run_id            str
      run_dt            str  (YYYY-MM-DD)
      extracted_count   int
      bronze_bucket     str
      artifact_summaries list[dict]
    """
    import os
    from datetime import datetime, timezone

    from playwright.sync_api import sync_playwright

    from src.ingestion.common.minio import write_bronze_artifact
    from src.ingestion.common.provenance import build_meta
    from src.ingestion.source_sp1.auth import get_credentials, login
    from src.ingestion.source_sp1.extract_vacatures import extract_all

    run_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = kwargs.get("run_id") or (
        f"{_SOURCE_NAME}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    bucket = os.environ.get("MINIO_BUCKET", _DEFAULT_BUCKET)
    lookback_days = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", "7"))

    minio_conn = try_get_conn("minio")
    portal_conn = try_get_conn("source_sp1")
    creds = get_credentials(portal_conn)

    artifact_summaries: list[dict] = []
    extracted_count = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (compatible; FreshMindsDataBot/1.0; "
                "+https://www.fresh-minds.nl)"
            ),
        )
        page = context.new_page()
        try:
            ok = login(page, creds)
            if not ok:
                raise RuntimeError(
                    "Authentication failed on Source SP1 portal. "
                    "Verify AIRFLOW_CONN_SOURCE_SP1 credentials. "
                    "If MFA is required, see README_source_sp1.md § MFA fallback."
                )

            raw_artifacts = extract_all(
                page=page,
                base_url=creds.base_url,
                run_id=run_id,
                run_dt=run_dt,
                lookback_days=lookback_days,
            )

            if not raw_artifacts:
                raise RuntimeError(
                    "No artifacts extracted. Portal may be empty, "
                    "selector changes may be needed, or the session is unauthenticated."
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
        finally:
            page.close()
            context.close()
            browser.close()

    ti = kwargs["ti"]
    ti.xcom_push(key="run_id", value=run_id)
    ti.xcom_push(key="run_dt", value=run_dt)
    ti.xcom_push(key="extracted_count", value=extracted_count)
    ti.xcom_push(key="bronze_bucket", value=bucket)
    ti.xcom_push(key="artifact_summaries", value=artifact_summaries)

    log.info("[extract] Done — %d artifacts for run_id=%s", extracted_count, run_id)


# ---------------------------------------------------------------------------
# Parse and Load (source-specific parsing, but uses generic upsert)
# ---------------------------------------------------------------------------

def _parse_and_load(**kwargs):
    """Read bronze artifacts → parse → upsert Postgres staging table.

    XCom pushes:
      parsed_count    int
      upserted_count  int
    """
    from datetime import datetime, timezone

    from src.ingestion.common.minio import read_bronze_raw
    from src.ingestion.common.postgres import update_ingestion_state, upsert_vacatures
    from src.ingestion.source_sp1.parse_vacatures import parse_artifacts

    ti = kwargs["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="extract.extract_to_bronze")
    run_dt = ti.xcom_pull(key="run_dt", task_ids="extract.extract_to_bronze")
    bucket = ti.xcom_pull(key="bronze_bucket", task_ids="extract.extract_to_bronze")
    summaries: list[dict] = ti.xcom_pull(
        key="artifact_summaries", task_ids="extract.extract_to_bronze"
    ) or []

    minio_conn = try_get_conn("minio")
    pg_conn = try_get_conn("postgres_warehouse")

    # Read raw bytes back from MinIO for each artifact
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
            f"No bronze artifacts readable from bucket '{bucket}' for run_dt={run_dt}. "
            "Check MinIO connectivity and artifact keys."
        )

    # Parse structured artifacts first.
    # "dom_html_snapshot" is a traceability-only artifact written by the extractor
    # before pagination; it is intentionally excluded from parsing to prevent
    # duplicate records from two HTML captures of the same page.
    structured = [
        a for a in raw_artifacts
        if a["extraction_method"] in ("export_download", "xhr_json")
    ]
    to_parse = structured or [
        a for a in raw_artifacts if a["extraction_method"] == "dom_html"
    ]

    records = parse_artifacts(to_parse)
    parsed_count = len(records)

    upserted_count = upsert_vacatures(records, conn=pg_conn)

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
# DAG definition — uses common helpers for preflight, dbt, observability
# ===========================================================================

# Import config at module level for the DDL callable builder
from src.ingestion.source_sp1.config import SP1_VACATURES_CONFIG

with DAG(
    dag_id="source_sp1_vacatures_ingestion",
    default_args=make_default_args(),
    description=(
        "Ingest Source SP1 vacatures → MinIO bronze "
        "→ Postgres silver → dbt gold"
    ),
    schedule_interval="@daily",
    catchup=False,
    tags=["sp1", "vacatures", "ingestion", "bronze", "silver", "gold"],
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
) as dag:

    # ── Preflight ──────────────────────────────────────────────────────────
    with TaskGroup("preflight", tooltip="Validate connections and infrastructure") as preflight:
        t_validate = PythonOperator(
            task_id="validate_connections",
            python_callable=make_validate_connections_callable([
                {
                    "conn_id": "source_sp1",
                    "env_fallback": "SP1_USERNAME",
                    "label": "Source SP1 portal",
                },
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
        t_ddl = PythonOperator(
            task_id="ensure_postgres_ddl",
            python_callable=make_ensure_ddl_callable(SP1_VACATURES_CONFIG),
            retries=2,
            retry_delay=timedelta(seconds=30),
        )
        t_validate >> [t_bucket, t_ddl]

    # ── Extract ────────────────────────────────────────────────────────────
    with TaskGroup("extract", tooltip="Playwright extract → bronze MinIO") as extract:
        t_extract = PythonOperator(
            task_id="extract_to_bronze",
            python_callable=_extract_to_bronze,
            retries=2,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=True,
            sla=timedelta(minutes=45),
            execution_timeout=timedelta(hours=1),
        )

    # ── Parse and Load ─────────────────────────────────────────────────────
    with TaskGroup("parse_load", tooltip="Parse bronze → upsert Postgres staging") as parse_load:
        t_parse = PythonOperator(
            task_id="parse_and_load",
            python_callable=_parse_and_load,
            retries=2,
            retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=15),
        )

    # ── dbt Run ────────────────────────────────────────────────────────────
    with TaskGroup("dbt_run", tooltip="dbt run + test: staging → intermediate → marts (dims + fact)") as dbt_run_group:
        t_dbt = PythonOperator(
            task_id="dbt_run_and_test",
            python_callable=make_dbt_run_callable(
                select="stg_source_sp1__vacatures+",
                source_name=_SOURCE_NAME,
            ),
            retries=1,
            retry_delay=timedelta(minutes=5),
            sla=timedelta(minutes=20),
        )

    # ── Observability ──────────────────────────────────────────────────────
    with TaskGroup("observability", tooltip="Metrics and run summary") as observability:
        t_metrics = PythonOperator(
            task_id="emit_metrics",
            python_callable=make_emit_metrics_callable(
                source_name=f"{_SOURCE_NAME}_{_DATASET}",
            ),
            trigger_rule="all_done",  # run even if dbt fails, for partial metrics
        )

    # Ensure the DAG run is marked failed when the pipeline path fails.
    t_success_gate = EmptyOperator(
        task_id="pipeline_success_gate",
    )

    # ── Dependency chain ───────────────────────────────────────────────────
    preflight >> extract >> parse_load >> dbt_run_group
    dbt_run_group >> observability
    dbt_run_group >> t_success_gate
