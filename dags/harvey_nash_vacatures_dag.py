"""
Harvey Nash NL Vacatures Pipeline DAG

Orchestrates the bronze → silver ingestion of job postings from
https://www.harveynash.nl/vacatures into the Postgres warehouse.

  1. Bronze — scrape the Harvey Nash vacatures page with Playwright and
              push raw job records to XCom.
  2. Silver — clean, normalize, and load the records into the warehouse
              table `odp_staffing_demand.harvey_nash_vacatures`, which is the
              source table for the dbt bronze/silver/gold models.

The Playwright Chromium browser is pre-installed in the Airflow image
(see airflow/Dockerfile).  Set `LOCAL_MOCK_EXTERNAL=true` to bypass
the real scrape and use stub data during local development.

Schedule: daily at midnight UTC.
"""

from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

_DAG_ID = "harvey_nash_vacatures_pipeline"


# ---------------------------------------------------------------------------
# Shared metadata helpers (mirrors odp_staffing_demand_dag.py pattern)
# ---------------------------------------------------------------------------

def _metadata_context(**kwargs):
    """Resolve common metadata logging context for this DAG run."""
    import os
    from datetime import datetime, timezone

    from src.ingestion.common.dag_helpers import resolve_code_version

    ti = kwargs["ti"]
    dag_run = kwargs.get("dag_run")
    run_id = (
        getattr(dag_run, "run_id", None)
        or f"{_DAG_ID}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    triggered_by = os.environ.get("USER", "airflow")
    code_version = resolve_code_version()
    return ti, run_id, triggered_by, code_version


def _log_task_success(
    *, kwargs: dict, task_group: str, task_id: str, metadata: Optional[dict] = None
) -> None:
    """Best-effort metadata logging for a successful task execution."""
    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        ensure_metadata_tables,
        insert_pipeline_task_run,
        now_utc,
        upsert_pipeline_run,
    )

    ti, run_id, triggered_by, code_version = _metadata_context(**kwargs)
    conn = try_get_conn("postgres_warehouse")
    started = now_utc()
    ensure_metadata_tables(conn=conn)
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name="harvey_nash_nl",
        dataset="harvey_nash_vacatures",
        status="RUNNING",
        triggered_by=triggered_by,
        code_version=code_version,
        started_at_utc=started,
        metadata={"task_group": task_group},
        conn=conn,
    )
    insert_pipeline_task_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        task_id=task_id,
        task_group=task_group,
        try_number=getattr(ti, "try_number", None),
        status="SUCCESS",
        started_at_utc=started,
        finished_at_utc=now_utc(),
        metadata=metadata or {},
        conn=conn,
    )


# ---------------------------------------------------------------------------
# Callable wrappers — imports deferred so DAG parsing stays fast and safe
# ---------------------------------------------------------------------------

def _scrape_vacatures(**kwargs):
    """
    Bronze: render the Harvey Nash vacatures page with Playwright, paginate
    through all listings, and push the raw list of job dicts to XCom.

    Uses mock data when LOCAL_MOCK_EXTERNAL=true (local dev / CI).
    """
    import os

    from pipelines.odp_staffing_demand.bronze_harvey_nash_vacatures import (
        HARVEY_NASH_URL,
        _MOCK_JOBS,
        _scrape_with_playwright,
    )

    use_mock = os.getenv("LOCAL_MOCK_EXTERNAL", "false").lower() == "true"

    if use_mock:
        vacatures = list(_MOCK_JOBS)
        print(f"[Harvey Nash Bronze] Using mock data ({len(vacatures)} jobs)")
    else:
        vacatures = _scrape_with_playwright(HARVEY_NASH_URL)
        print(f"[Harvey Nash Bronze] ✓ Scraped {len(vacatures)} vacatures from {HARVEY_NASH_URL}")

    kwargs["ti"].xcom_push(key="vacatures", value=vacatures)
    _log_task_success(
        kwargs=kwargs,
        task_group="bronze",
        task_id="scrape_vacatures",
        metadata={"vacatures_count": len(vacatures)},
    )


def _load_to_warehouse(**kwargs):
    """
    Silver: pull the raw vacatures from XCom, clean/normalize each record,
    and TRUNCATE + INSERT into `odp_staffing_demand.harvey_nash_vacatures` within a
    single Postgres transaction.  Also registers lineage and dataset metadata.
    """
    from datetime import datetime, timezone

    from psycopg2.extras import execute_values

    from pipelines.odp_staffing_demand.postgres_pipeline import _connect_warehouse, ensure_tables
    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        insert_dataset_version,
        insert_lineage_edge,
        now_utc,
        upsert_dataset_registry,
        upsert_pipeline_run,
    )

    ti = kwargs["ti"]
    vacatures = ti.xcom_pull(key="vacatures", task_ids="bronze.scrape_vacatures")

    if not vacatures:
        print("[Harvey Nash Silver] No vacatures received from XCom. Skipping load.")
        return

    conn = _connect_warehouse()
    ingested_at = datetime.now(timezone.utc)
    rows_loaded = 0

    try:
        ensure_tables(conn)

        rows = []
        for v in vacatures:
            job_id = str(v.get("id") or "").strip()
            if not job_id:
                continue  # skip records without a usable ID

            rows.append((
                job_id,
                (v.get("title") or "").strip() or None,
                (v.get("company") or "").strip() or None,
                (v.get("location") or "").strip() or None,
                (v.get("province") or "").strip() or None,
                (v.get("contract_type") or "").strip() or None,
                (v.get("description") or "").strip() or None,
                v.get("salary_min"),
                v.get("salary_max"),
                (v.get("salary_raw") or "").strip() or None,
                (v.get("url") or "").strip() or None,
                None,           # posted_date — not available at card level
                ingested_at,
            ))

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE odp_staffing_demand.harvey_nash_vacatures")
            if rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO odp_staffing_demand.harvey_nash_vacatures
                      (id, title, company, location, province, contract_type,
                       description, salary_min, salary_max, salary_raw,
                       url, posted_date, ingestion_timestamp)
                    VALUES %s
                    """,
                    rows,
                )
        conn.commit()
        rows_loaded = len(rows)
        print(f"[Harvey Nash Silver] ✓ Loaded {rows_loaded} vacatures to warehouse")

    finally:
        conn.close()

    # ── Metadata & lineage registration ──────────────────────────────────────
    _, run_id, triggered_by, code_version = _metadata_context(**kwargs)
    meta_conn = try_get_conn("postgres_warehouse")

    upsert_dataset_registry(
        dataset_id="odp_staffing_demand.harvey_nash_vacatures",
        layer="silver",
        domain="odp_staffing_demand",
        schema_name="odp_staffing_demand",
        table_name="harvey_nash_vacatures",
        owner="data-platform",
        classification="internal",
        sensitivity="public",
        retention_days=90,
        metadata={"pipeline": _DAG_ID, "source_url": "https://www.harveynash.nl/vacatures"},
        conn=meta_conn,
    )
    insert_dataset_version(
        dataset_id="odp_staffing_demand.harvey_nash_vacatures",
        version_label=run_id,
        schema_hash=None,
        column_schema=[],
        row_count=rows_loaded,
        byte_size=None,
        run_id=run_id,
        metadata={"loaded_at": now_utc().isoformat()},
        conn=meta_conn,
    )
    insert_lineage_edge(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        upstream_dataset="bronze.harvey_nash_nl_scrape",
        downstream_dataset="odp_staffing_demand.harvey_nash_vacatures",
        transformation_type="CLEANED",
        metadata={"task": "load_to_warehouse", "rows_loaded": rows_loaded},
        conn=meta_conn,
    )
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name="harvey_nash_nl",
        dataset="harvey_nash_vacatures",
        status="SUCCESS",
        triggered_by=triggered_by,
        code_version=code_version,
        finished_at_utc=now_utc(),
        metadata={"rows_loaded": rows_loaded},
        conn=meta_conn,
    )

    _log_task_success(
        kwargs=kwargs,
        task_group="silver",
        task_id="load_to_warehouse",
        metadata={"rows_loaded": rows_loaded},
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "Open Data Platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    _DAG_ID,
    default_args=default_args,
    description=(
        "Harvey Nash NL vacatures pipeline: Playwright scrape → Postgres warehouse "
        "(bronze → silver). Source for dbt brz_odp_staffing_demand__harvey_nash_vacatures "
        "and downstream silver/gold models."
    ),
    schedule_interval="@daily",
    catchup=False,
    tags=["harvey_nash", "job_market", "nl", "bronze", "silver", "web_scraping"],
    sla_miss_callback=None,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag:

    # ── Bronze Task Group ──────────────────────────────────────────────────
    with TaskGroup("bronze", tooltip="Scrape job listings from harveynash.nl") as bronze:
        scrape = PythonOperator(
            task_id="scrape_vacatures",
            python_callable=_scrape_vacatures,
            # Extra retries for the external scrape — the site may be briefly slow
            retries=3,
            retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=20),
            execution_timeout=timedelta(minutes=30),
        )

    # ── Silver Task Group ──────────────────────────────────────────────────
    with TaskGroup(
        "silver", tooltip="Clean, normalize and load vacatures to the warehouse"
    ) as silver:
        load = PythonOperator(
            task_id="load_to_warehouse",
            python_callable=_load_to_warehouse,
            retries=2,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=10),
        )

    # ── Dependency chain ───────────────────────────────────────────────────
    bronze >> silver
