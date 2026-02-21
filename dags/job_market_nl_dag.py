"""
NL IT Job Market Pipeline DAG — decomposed with task groups, SLA, and quality checkpoints.

This DAG orchestrates the full job market data pipeline:
  1. Ingest — fetch CBS, Adzuna, and UWV data
  2. Transform — build snapshot, top skills, region distribution, geo data
  3. Export — load into Postgres warehouse
  4. Quality — run data quality checks
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# ---------------------------------------------------------------------------
# Callable wrappers — thin functions that call into pipeline code.
# Keeping imports inside the callables avoids import errors at DAG parse time.
# ---------------------------------------------------------------------------

def _fetch_cbs_data(**kwargs):
    """Fetch CBS vacancy rate and vacancies data."""
    from pipelines.job_market_nl.postgres_pipeline import (
        _fetch_cbs_vacancy_rate_latest,
        _fetch_cbs_vacancies_latest,
        _get_period_label,
        _get_sector_label_for_vacancies,
        CBS_VACANCY_RATE_TABLE,
        CBS_VACANCIES_TABLE,
        CBS_IT_SECTOR_KEY,
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
    print(f"[CBS Ingest] ✓ period={period_key}, vacancies={vacancies}, rate={vacancy_rate}")


def _fetch_job_ads(**kwargs):
    """Fetch Adzuna job ads (or mock data in local environments)."""
    from pipelines.job_market_nl.postgres_pipeline import _fetch_job_ads_mock_or_adzuna

    job_ads = _fetch_job_ads_mock_or_adzuna()
    kwargs["ti"].xcom_push(key="job_ads", value=job_ads)
    print(f"[Adzuna Ingest] ✓ fetched {len(job_ads)} job ads")


def _build_all_outputs(**kwargs):
    """Build snapshot row, top skills, region distribution, and geo data."""
    from pipelines.job_market_nl.postgres_pipeline import (
        SnapshotRow,
        build_top_skills,
        build_region_distribution,
        build_job_ads_geo,
    )

    ti = kwargs["ti"]
    cbs_data = ti.xcom_pull(key="cbs_data", task_ids="ingest.fetch_cbs_data")
    job_ads = ti.xcom_pull(key="job_ads", task_ids="ingest.fetch_job_ads")

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

    ti = kwargs["ti"]
    snap_data = ti.xcom_pull(key="snapshot", task_ids="transform.build_outputs")
    top_skills = ti.xcom_pull(key="top_skills", task_ids="transform.build_outputs")
    region_distribution = ti.xcom_pull(key="region_distribution", task_ids="transform.build_outputs")
    job_ads_geo = ti.xcom_pull(key="job_ads_geo", task_ids="transform.build_outputs")

    snapshot = SnapshotRow(**snap_data)

    # Convert lists back to tuples for psycopg2
    top_skills_tuples = [tuple(s) for s in (top_skills or [])]
    region_tuples = [tuple(r) for r in (region_distribution or [])]
    geo_tuples = [tuple(g) for g in (job_ads_geo or [])]

    refresh_tables(snapshot, top_skills_tuples, region_tuples, geo_tuples)
    print("[Export] ✓ Loaded all tables to Postgres warehouse")


def _run_quality_checks(**kwargs):
    """Run data quality checks against the freshly loaded warehouse tables."""
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "scripts/quality/run_data_quality.py", "--all"],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
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
    "job_market_nl_pipeline",
    default_args=default_args,
    description="NL IT job market pipeline (CBS + job ads) -> Postgres for Superset",
    schedule_interval="@daily",
    catchup=False,
    tags=["job_market", "nl", "warehouse", "superset"],
    sla_miss_callback=None,  # TODO: wire up to alerting (e.g. scripts/send_alert.py)
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
) as dag:

    # ── Ingest Task Group ──────────────────────────────────────────────
    with TaskGroup("ingest", tooltip="Fetch data from external sources") as ingest:
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

    # ── Transform Task Group ───────────────────────────────────────────
    with TaskGroup("transform", tooltip="Build snapshot, skills, regions, geo") as transform:
        build_outputs = PythonOperator(
            task_id="build_outputs",
            python_callable=_build_all_outputs,
            sla=timedelta(minutes=10),
        )

    # ── Export Task Group ──────────────────────────────────────────────
    with TaskGroup("export", tooltip="Load data into Postgres warehouse") as export:
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
    ingest >> transform >> export >> quality
