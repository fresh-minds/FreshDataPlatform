"""
NL IT Job Market Pipeline DAG — decomposed with task groups, SLA, and quality checkpoints.

This DAG orchestrates the full job market data pipeline:
  1. Bronze — fetch source data
  2. Silver — build cleaned/aggregated intermediate outputs
  3. Gold — publish curated warehouse tables
  4. Quality — run data quality checks
"""

from datetime import datetime, timedelta
from typing import Optional

from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

_DAG_ID = "job_market_nl_pipeline"


def _metadata_context(**kwargs):
    """Resolve common metadata logging context for this DAG run."""
    import os
    import subprocess
    from datetime import datetime, timezone

    ti = kwargs["ti"]
    dag_run = kwargs.get("dag_run")
    run_id = getattr(dag_run, "run_id", None) or f"{_DAG_ID}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    triggered_by = os.environ.get("USER", "airflow")
    try:
        git_sha = subprocess.run(["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=False)
        code_version = git_sha.stdout.strip() if git_sha.returncode == 0 and git_sha.stdout.strip() else "unknown"
    except Exception:
        code_version = "unknown"
    return ti, run_id, triggered_by, code_version


def _log_task_success(
    *, kwargs: dict, task_group: str, task_id: str, metadata: Optional[dict] = None
) -> None:
    """Best-effort metadata logging for successful task execution."""
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
        source_name="job_market_nl",
        dataset="it_market",
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
# Callable wrappers — thin functions that call into pipeline code.
# Keeping imports inside the callables avoids import errors at DAG parse time.
# ---------------------------------------------------------------------------

def _fetch_cbs_data(**kwargs):
    """Fetch CBS vacancy rate and vacancies data."""
    from pipelines.job_market_nl.postgres_pipeline import (
        CBS_IT_SECTOR_KEY,
        CBS_VACANCIES_TABLE,
        CBS_VACANCY_RATE_TABLE,
        _fetch_cbs_vacancies_latest,
        _fetch_cbs_vacancy_rate_latest,
        _get_period_label,
        _get_sector_label_for_vacancies,
    )

    period_rate, vacancy_rate = _fetch_cbs_vacancy_rate_latest(CBS_VACANCY_RATE_TABLE, CBS_IT_SECTOR_KEY)
    period_vac, vacancies = _fetch_cbs_vacancies_latest(CBS_VACANCIES_TABLE, CBS_IT_SECTOR_KEY)

    period_key = max([p for p in [period_rate, period_vac] if p], default=period_rate or period_vac)
    period_label = _get_period_label(CBS_VACANCIES_TABLE, period_key)
    sector_name = _get_sector_label_for_vacancies(CBS_VACANCIES_TABLE, CBS_IT_SECTOR_KEY)

    kwargs["ti"].xcom_push(key="cbs_data", value={
        "period_key": period_key,
        "period_label": period_label,
        "sector_name": sector_name,
        "vacancies": vacancies,
        "vacancy_rate": vacancy_rate,
    })
    _log_task_success(
        kwargs=kwargs,
        task_group="bronze",
        task_id="fetch_cbs_data",
        metadata={"period_key": period_key, "vacancies": vacancies, "vacancy_rate": vacancy_rate},
    )
    print(f"[CBS Ingest] ✓ period={period_key}, vacancies={vacancies}, rate={vacancy_rate}")


def _fetch_job_ads(**kwargs):
    """Fetch Adzuna job ads (or mock data in local environments)."""
    from pipelines.job_market_nl.postgres_pipeline import _fetch_job_ads_mock_or_adzuna

    job_ads = _fetch_job_ads_mock_or_adzuna()
    kwargs["ti"].xcom_push(key="job_ads", value=job_ads)
    _log_task_success(
        kwargs=kwargs,
        task_group="bronze",
        task_id="fetch_job_ads",
        metadata={"job_ads_count": len(job_ads)},
    )
    print(f"[Adzuna Ingest] ✓ fetched {len(job_ads)} job ads")


def _build_all_outputs(**kwargs):
    """Build snapshot row, top skills, region distribution, and geo data."""
    from pipelines.job_market_nl.postgres_pipeline import (
        SnapshotRow,
        build_job_ads_geo,
        build_region_distribution,
        build_top_skills,
    )

    ti = kwargs["ti"]
    cbs_data = ti.xcom_pull(key="cbs_data", task_ids="bronze.fetch_cbs_data")
    job_ads = ti.xcom_pull(key="job_ads", task_ids="bronze.fetch_job_ads")

    snapshot = SnapshotRow(
        period_key=cbs_data["period_key"],
        period_label=cbs_data["period_label"],
        sector_name=cbs_data["sector_name"],
        vacancies=cbs_data["vacancies"],
        vacancy_rate=cbs_data["vacancy_rate"],
        job_ads_count=len(job_ads),
    )

    top_skills = build_top_skills(job_ads)
    region_distribution = build_region_distribution(job_ads)
    job_ads_geo = build_job_ads_geo(job_ads)

    ti.xcom_push(key="snapshot", value={
        "period_key": snapshot.period_key,
        "period_label": snapshot.period_label,
        "sector_name": snapshot.sector_name,
        "vacancies": snapshot.vacancies,
        "vacancy_rate": snapshot.vacancy_rate,
        "job_ads_count": snapshot.job_ads_count,
    })
    ti.xcom_push(key="top_skills", value=top_skills)
    ti.xcom_push(key="region_distribution", value=region_distribution)
    ti.xcom_push(key="job_ads_geo", value=job_ads_geo)
    _log_task_success(
        kwargs=kwargs,
        task_group="silver",
        task_id="build_outputs",
        metadata={
            "period_key": snapshot.period_key,
            "skills_count": len(top_skills),
            "regions_count": len(region_distribution),
            "geo_count": len(job_ads_geo),
        },
    )

    print(
        f"[Transform] ✓ snapshot period={snapshot.period_key}, "
        f"skills={len(top_skills)}, regions={len(region_distribution)}, geo={len(job_ads_geo)}"
    )


def _load_to_warehouse(**kwargs):
    """Load all transformed data into Postgres warehouse within a single transaction."""
    from pipelines.job_market_nl.postgres_pipeline import (
        SnapshotRow,
        refresh_tables,
    )
    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        insert_dataset_version,
        insert_lineage_edge,
        now_utc,
        upsert_dataset_registry,
    )

    ti = kwargs["ti"]
    snap_data = ti.xcom_pull(key="snapshot", task_ids="silver.build_outputs")
    top_skills = ti.xcom_pull(key="top_skills", task_ids="silver.build_outputs")
    region_distribution = ti.xcom_pull(key="region_distribution", task_ids="silver.build_outputs")
    job_ads_geo = ti.xcom_pull(key="job_ads_geo", task_ids="silver.build_outputs")

    snapshot = SnapshotRow(**snap_data)

    # Convert lists back to tuples for psycopg2
    top_skills_tuples = [tuple(s) for s in (top_skills or [])]
    region_tuples = [tuple(r) for r in (region_distribution or [])]
    geo_tuples = [tuple(g) for g in (job_ads_geo or [])]

    refresh_tables(snapshot, top_skills_tuples, region_tuples, geo_tuples)
    conn = try_get_conn("postgres_warehouse")
    _, run_id, _, _ = _metadata_context(**kwargs)

    datasets = [
        ("job_market_nl.it_market_snapshot", snapshot.job_ads_count),
        ("job_market_nl.it_market_top_skills", len(top_skills_tuples)),
        ("job_market_nl.it_market_region_distribution", len(region_tuples)),
        ("job_market_nl.it_market_job_ads_geo", len(geo_tuples)),
    ]
    for dataset_id, row_count in datasets:
        schema_name, table_name = dataset_id.split(".", 1)
        upsert_dataset_registry(
            dataset_id=dataset_id,
            layer="gold",
            domain="job_market_nl",
            schema_name=schema_name,
            table_name=table_name,
            owner="data-platform",
            classification="internal",
            sensitivity="internal",
            retention_days=365,
            metadata={"pipeline": _DAG_ID},
            conn=conn,
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
            conn=conn,
        )

    lineage = {
        "job_market_nl.it_market_snapshot": ["silver.cbs_vacancy_rate", "silver.adzuna_job_ads"],
        "job_market_nl.it_market_top_skills": ["silver.adzuna_job_ads"],
        "job_market_nl.it_market_region_distribution": ["silver.adzuna_job_ads"],
        "job_market_nl.it_market_job_ads_geo": ["silver.adzuna_job_ads"],
    }
    for downstream, upstreams in lineage.items():
        for upstream in upstreams:
            insert_lineage_edge(
                run_id=run_id,
                pipeline_name=_DAG_ID,
                upstream_dataset=upstream,
                downstream_dataset=downstream,
                transformation_type="TRANSFORMED",
                metadata={"task": "load_to_warehouse"},
                conn=conn,
            )

    _log_task_success(
        kwargs=kwargs,
        task_group="gold",
        task_id="load_to_warehouse",
        metadata={
            "snapshot_rows": 1,
            "skills_rows": len(top_skills_tuples),
            "region_rows": len(region_tuples),
            "geo_rows": len(geo_tuples),
        },
    )
    print("[Export] ✓ Loaded all tables to Postgres warehouse")


def _run_quality_checks(**kwargs):
    """Run data quality checks against the freshly loaded warehouse tables."""
    import subprocess
    import sys

    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        insert_data_quality_result,
        now_utc,
        upsert_pipeline_run,
    )

    _, run_id, triggered_by, code_version = _metadata_context(**kwargs)
    conn = try_get_conn("postgres_warehouse")
    result = subprocess.run(
        [sys.executable, "-m", "scripts.quality.run_data_quality", "--all"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    status = "SUCCESS" if result.returncode == 0 else "FAILED"
    insert_data_quality_result(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dataset_id="job_market_nl.it_market_snapshot",
        assertion_id="scripts.quality.run_data_quality.all",
        assertion_type="framework_run",
        severity="medium",
        status="PASS" if result.returncode == 0 else "FAIL",
        observed_value=str(result.returncode),
        expected_value="0",
        details={"stderr": result.stderr[-4000:] if result.stderr else "", "stdout_tail": result.stdout[-4000:]},
        conn=conn,
    )
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name="job_market_nl",
        dataset="it_market",
        status=status,
        triggered_by=triggered_by,
        code_version=code_version,
        finished_at_utc=now_utc(),
        metadata={"quality_return_code": result.returncode},
        conn=conn,
    )
    _log_task_success(
        kwargs=kwargs,
        task_group="quality",
        task_id="run_quality_checks",
        metadata={"quality_return_code": result.returncode},
    )
    if result.returncode != 0:
        print(f"[Quality] ⚠ Quality checks returned non-zero: {result.stderr}")
        # Log but don't fail the DAG — quality issues are warnings for now
    else:
        print("[Quality] ✓ All data quality checks passed")


# ---------------------------------------------------------------------------
# DAG Definition
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
    description="NL IT job market pipeline (bronze -> silver -> gold -> Postgres) with metadata tracking",
    schedule_interval="@daily",
    catchup=False,
    tags=["job_market", "nl", "bronze", "silver", "gold", "metadata"],
    sla_miss_callback=None,  # TODO: wire up to alerting (e.g. scripts/send_alert.py)
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
) as dag:

    # ── Bronze Task Group ──────────────────────────────────────────────
    with TaskGroup("bronze", tooltip="Fetch raw source data from external systems") as bronze:
        fetch_cbs = PythonOperator(
            task_id="fetch_cbs_data",
            python_callable=_fetch_cbs_data,
            retries=3,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=15),
        )
        fetch_ads = PythonOperator(
            task_id="fetch_job_ads",
            python_callable=_fetch_job_ads,
            retries=3,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=15),
        )
        # CBS and Adzuna can be fetched in parallel
        [fetch_cbs, fetch_ads]

    # ── Silver Task Group ──────────────────────────────────────────────
    with TaskGroup("silver", tooltip="Build cleaned and aggregated intermediate outputs") as silver:
        build_outputs = PythonOperator(
            task_id="build_outputs",
            python_callable=_build_all_outputs,
            sla=timedelta(minutes=10),
        )

    # ── Gold Task Group ────────────────────────────────────────────────
    with TaskGroup("gold", tooltip="Publish curated warehouse tables") as gold:
        load_warehouse = PythonOperator(
            task_id="load_to_warehouse",
            python_callable=_load_to_warehouse,
            retries=2,
            retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=10),
        )

    # ── Quality Task Group ─────────────────────────────────────────────
    with TaskGroup("quality", tooltip="Run data quality checks") as quality:
        run_dq = PythonOperator(
            task_id="run_quality_checks",
            python_callable=_run_quality_checks,
            retries=1,
            retry_delay=timedelta(minutes=1),
            sla=timedelta(minutes=15),
            trigger_rule="all_success",
        )

    # ── Dependency Chain ───────────────────────────────────────────────
    bronze >> silver >> gold >> quality
