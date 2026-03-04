"""
Adzuna Job Ads Pipeline DAG

Orchestrates the bronze → silver ingestion of IT job ads from the Adzuna API
(Netherlands) into the Postgres warehouse.

  1. Bronze — fetch job ads from the Adzuna API (or mock data when credentials
              are absent / LOCAL_MOCK_EXTERNAL=true).
  2. Silver — build aggregated outputs from the raw ads and load them into:
              - `job_market_nl.it_market_top_skills`
              - `job_market_nl.it_market_region_distribution`
              - `job_market_nl.it_market_job_ads_geo`

The three silver tables are consumed by `job_market_nl_dag.py` (gold layer) to
build the `it_market_snapshot` fact table and to populate geo/skills dashboards.

Schedule: daily at 01:30 UTC (runs before the gold DAG).
"""

from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

_DAG_ID = "adzuna_job_ads_pipeline"


# ---------------------------------------------------------------------------
# Shared metadata helpers
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
        source_name="adzuna_nl",
        dataset="adzuna_job_ads",
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

def _fetch_job_ads(**kwargs):
    """
    Bronze: fetch IT job ads from the Adzuna API for the Netherlands.

    Falls back to a small set of mock records when ADZUNA_APP_ID / ADZUNA_APP_KEY
    are not set, or when LOCAL_MOCK_EXTERNAL=true.

    Pushes the raw list of job-ad dicts to XCom under key ``job_ads``.
    """
    from pipelines.job_market_nl.postgres_pipeline import _fetch_job_ads_mock_or_adzuna

    job_ads = _fetch_job_ads_mock_or_adzuna()
    kwargs["ti"].xcom_push(key="job_ads", value=job_ads)
    _log_task_success(
        kwargs=kwargs,
        task_group="bronze",
        task_id="fetch_job_ads",
        metadata={"job_ads_count": len(job_ads)},
    )
    print(f"[Adzuna Bronze] ✓ Fetched {len(job_ads)} job ads")


def _build_and_load_silver(**kwargs):
    """
    Silver: pull raw job ads from XCom, build aggregated outputs
    (top skills, region distribution, geo points), then TRUNCATE + INSERT
    into the three silver Postgres tables.

    Tables written:
      - job_market_nl.it_market_top_skills
      - job_market_nl.it_market_region_distribution
      - job_market_nl.it_market_job_ads_geo
    """
    from psycopg2.extras import execute_values

    from pipelines.job_market_nl.postgres_pipeline import (
        _connect_warehouse,
        build_job_ads_geo,
        build_region_distribution,
        build_top_skills,
        ensure_tables,
    )
    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        insert_dataset_version,
        insert_lineage_edge,
        now_utc,
        upsert_dataset_registry,
        upsert_pipeline_run,
    )

    ti = kwargs["ti"]
    job_ads = ti.xcom_pull(key="job_ads", task_ids="bronze.fetch_job_ads")

    if not job_ads:
        print("[Adzuna Silver] No job ads received from XCom. Skipping load.")
        return

    top_skills = build_top_skills(job_ads)
    region_distribution = build_region_distribution(job_ads)
    job_ads_geo = build_job_ads_geo(job_ads)

    # Convert to tuples for psycopg2 execute_values
    top_skills_tuples = [tuple(s) for s in (top_skills or [])]
    region_tuples = [tuple(r) for r in (region_distribution or [])]
    geo_tuples = [tuple(g) for g in (job_ads_geo or [])]

    conn = _connect_warehouse()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_top_skills")
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_region_distribution")
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_job_ads_geo")

            if top_skills_tuples:
                execute_values(
                    cur,
                    "INSERT INTO job_market_nl.it_market_top_skills (skill, count) VALUES %s",
                    top_skills_tuples,
                )

            if region_tuples:
                execute_values(
                    cur,
                    """
                    INSERT INTO job_market_nl.it_market_region_distribution
                      (region, job_ads_count, share_pct, latitude, longitude)
                    VALUES %s
                    """,
                    region_tuples,
                )

            if geo_tuples:
                execute_values(
                    cur,
                    """
                    INSERT INTO job_market_nl.it_market_job_ads_geo
                      (job_id, region, latitude, longitude, location_label)
                    VALUES %s
                    """,
                    geo_tuples,
                )
        conn.commit()
        print(
            f"[Adzuna Silver] ✓ Loaded skills={len(top_skills_tuples)}, "
            f"regions={len(region_tuples)}, geo={len(geo_tuples)}"
        )
    finally:
        conn.close()

    # ── Metadata & lineage ────────────────────────────────────────────────────
    _, run_id, triggered_by, code_version = _metadata_context(**kwargs)
    meta_conn = try_get_conn("postgres_warehouse")

    datasets = [
        ("job_market_nl.it_market_top_skills", len(top_skills_tuples)),
        ("job_market_nl.it_market_region_distribution", len(region_tuples)),
        ("job_market_nl.it_market_job_ads_geo", len(geo_tuples)),
    ]
    for dataset_id, row_count in datasets:
        schema_name, table_name = dataset_id.split(".", 1)
        upsert_dataset_registry(
            dataset_id=dataset_id,
            layer="silver",
            domain="job_market_nl",
            schema_name=schema_name,
            table_name=table_name,
            owner="data-platform",
            classification="internal",
            sensitivity="internal",
            retention_days=90,
            metadata={"pipeline": _DAG_ID},
            conn=meta_conn,
        )
        insert_dataset_version(
            dataset_id=dataset_id,
            version_label=run_id,
            schema_hash=None,
            column_schema=[],
            row_count=row_count,
            byte_size=None,
            run_id=run_id,
            metadata={"loaded_at": now_utc().isoformat()},
            conn=meta_conn,
        )
        insert_lineage_edge(
            run_id=run_id,
            pipeline_name=_DAG_ID,
            upstream_dataset="bronze.adzuna_api",
            downstream_dataset=dataset_id,
            transformation_type="TRANSFORMED",
            metadata={"task": "build_and_load_silver"},
            conn=meta_conn,
        )

    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name="adzuna_nl",
        dataset="adzuna_job_ads",
        status="SUCCESS",
        triggered_by=triggered_by,
        code_version=code_version,
        finished_at_utc=now_utc(),
        metadata={
            "skills_rows": len(top_skills_tuples),
            "region_rows": len(region_tuples),
            "geo_rows": len(geo_tuples),
        },
        conn=meta_conn,
    )

    _log_task_success(
        kwargs=kwargs,
        task_group="silver",
        task_id="build_and_load_silver",
        metadata={
            "skills_count": len(top_skills_tuples),
            "regions_count": len(region_tuples),
            "geo_count": len(geo_tuples),
        },
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
        "Adzuna NL job ads pipeline: API fetch → Postgres warehouse "
        "(bronze → silver). Populates top_skills, region_distribution, and "
        "job_ads_geo tables consumed by the gold job_market_nl_dag."
    ),
    schedule_interval="30 1 * * *",  # 01:30 UTC daily — runs before gold DAG
    catchup=False,
    tags=["adzuna", "job_market", "nl", "bronze", "silver", "job_ads"],
    sla_miss_callback=None,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag:

    # ── Bronze Task Group ──────────────────────────────────────────────────
    with TaskGroup("bronze", tooltip="Fetch job ads from Adzuna API") as bronze:
        fetch_ads = PythonOperator(
            task_id="fetch_job_ads",
            python_callable=_fetch_job_ads,
            retries=3,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=15),
        )

    # ── Silver Task Group ──────────────────────────────────────────────────
    with TaskGroup(
        "silver", tooltip="Build aggregated outputs and load to warehouse"
    ) as silver:
        build_silver = PythonOperator(
            task_id="build_and_load_silver",
            python_callable=_build_and_load_silver,
            retries=2,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=10),
        )

    # ── Dependency chain ───────────────────────────────────────────────────
    bronze >> silver
