"""
CBS Vacancy Rate Pipeline DAG

Orchestrates the bronze → silver ingestion of CBS (Statistics Netherlands)
vacancy rate and unfilled vacancies for the IT sector.

  1. Bronze — fetch the latest period from CBS OData API tables:
              - 80567ENG  (vacancy rate)
              - 80472ENG  (unfilled vacancies)
  2. Silver — clean and load the data into the Postgres warehouse table
              `job_market_nl.cbs_vacancy_rate`.

The silver table is consumed by `job_market_nl_dag.py` (gold layer) to build
the `it_market_snapshot` fact table.

Schedule: daily at 01:00 UTC (runs before the gold DAG).
"""

from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

_DAG_ID = "cbs_vacancy_rate_pipeline"


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
        source_name="cbs_nl",
        dataset="cbs_vacancy_rate",
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

def _fetch_cbs_data(**kwargs):
    """
    Bronze: call CBS OData API for the latest vacancy rate and unfilled
    vacancies for the IT / Information & Communication sector.

    Pushes a dict to XCom under key ``cbs_data``.
    """
    from pipelines.job_market_nl.postgres_pipeline import (
        CBS_IT_SECTOR_KEY,
        CBS_VACANCIES_TABLE,
        CBS_VACANCY_RATE_TABLE,
        _fetch_cbs_vacancies_latest,
        _fetch_cbs_vacancy_rate_latest,
        _get_period_label,
        _get_sector_label_for_vacancies,
    )

    period_rate, vacancy_rate = _fetch_cbs_vacancy_rate_latest(
        CBS_VACANCY_RATE_TABLE, CBS_IT_SECTOR_KEY
    )
    period_vac, vacancies = _fetch_cbs_vacancies_latest(
        CBS_VACANCIES_TABLE, CBS_IT_SECTOR_KEY
    )

    # Use the most recent period available across both tables.
    period_key = max(
        [p for p in [period_rate, period_vac] if p],
        default=period_rate or period_vac,
    )
    period_label = _get_period_label(CBS_VACANCIES_TABLE, period_key)
    sector_name = _get_sector_label_for_vacancies(CBS_VACANCIES_TABLE, CBS_IT_SECTOR_KEY)

    cbs_data = {
        "period_key": period_key,
        "period_label": period_label,
        "sector_name": sector_name,
        "vacancies": vacancies,
        "vacancy_rate": vacancy_rate,
    }
    kwargs["ti"].xcom_push(key="cbs_data", value=cbs_data)
    _log_task_success(
        kwargs=kwargs,
        task_group="bronze",
        task_id="fetch_cbs_data",
        metadata={
            "period_key": period_key,
            "vacancies": vacancies,
            "vacancy_rate": vacancy_rate,
        },
    )
    print(
        f"[CBS Bronze] ✓ period={period_key!r}, "
        f"vacancies={vacancies}, rate={vacancy_rate}"
    )


def _load_cbs_to_warehouse(**kwargs):
    """
    Silver: pull CBS data from XCom and INSERT a new row into
    `job_market_nl.cbs_vacancy_rate` (append — keeps history).
    Registers lineage and dataset metadata.
    """
    from datetime import datetime, timezone

    from pipelines.job_market_nl.postgres_pipeline import _connect_warehouse, ensure_tables
    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        insert_dataset_version,
        insert_lineage_edge,
        now_utc,
        upsert_dataset_registry,
        upsert_pipeline_run,
    )

    ti = kwargs["ti"]
    cbs_data = ti.xcom_pull(key="cbs_data", task_ids="bronze.fetch_cbs_data")

    if not cbs_data:
        raise ValueError("[CBS Silver] No CBS data received from XCom — aborting.")

    ingested_at = datetime.now(timezone.utc)
    conn = _connect_warehouse()

    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO job_market_nl.cbs_vacancy_rate
                  (period_key, period_label, sector_name, vacancies, vacancy_rate,
                   ingestion_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    cbs_data["period_key"],
                    cbs_data.get("period_label"),
                    cbs_data.get("sector_name"),
                    cbs_data.get("vacancies"),
                    cbs_data.get("vacancy_rate"),
                    ingested_at,
                ),
            )
        conn.commit()
        print(
            f"[CBS Silver] ✓ Inserted period={cbs_data['period_key']!r} "
            f"into job_market_nl.cbs_vacancy_rate"
        )
    finally:
        conn.close()

    # ── Metadata & lineage ────────────────────────────────────────────────────
    _, run_id, triggered_by, code_version = _metadata_context(**kwargs)
    meta_conn = try_get_conn("postgres_warehouse")

    upsert_dataset_registry(
        dataset_id="job_market_nl.cbs_vacancy_rate",
        layer="silver",
        domain="job_market_nl",
        schema_name="job_market_nl",
        table_name="cbs_vacancy_rate",
        owner="data-platform",
        classification="internal",
        sensitivity="public",
        retention_days=365,
        metadata={"pipeline": _DAG_ID, "source": "CBS OData API"},
        conn=meta_conn,
    )
    insert_dataset_version(
        dataset_id="job_market_nl.cbs_vacancy_rate",
        version_label=run_id,
        schema_hash=None,
        column_schema=[],
        row_count=1,
        byte_size=None,
        run_id=run_id,
        metadata={"loaded_at": now_utc().isoformat()},
        conn=meta_conn,
    )
    insert_lineage_edge(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        upstream_dataset="bronze.cbs_odata_api",
        downstream_dataset="job_market_nl.cbs_vacancy_rate",
        transformation_type="CLEANED",
        metadata={"task": "load_cbs_to_warehouse", "period_key": cbs_data["period_key"]},
        conn=meta_conn,
    )
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name="cbs_nl",
        dataset="cbs_vacancy_rate",
        status="SUCCESS",
        triggered_by=triggered_by,
        code_version=code_version,
        finished_at_utc=now_utc(),
        metadata={"period_key": cbs_data["period_key"]},
        conn=meta_conn,
    )

    _log_task_success(
        kwargs=kwargs,
        task_group="silver",
        task_id="load_cbs_to_warehouse",
        metadata={"period_key": cbs_data["period_key"]},
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
        "CBS NL vacancy rate pipeline: OData API → Postgres warehouse "
        "(bronze → silver). Silver table feeds the gold job_market_nl_dag."
    ),
    schedule_interval="0 1 * * *",  # 01:00 UTC daily — runs before gold DAG
    catchup=False,
    tags=["cbs", "job_market", "nl", "bronze", "silver", "statistics"],
    sla_miss_callback=None,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag:

    # ── Bronze Task Group ──────────────────────────────────────────────────
    with TaskGroup("bronze", tooltip="Fetch latest CBS vacancy data from OData API") as bronze:
        fetch_cbs = PythonOperator(
            task_id="fetch_cbs_data",
            python_callable=_fetch_cbs_data,
            retries=3,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=15),
        )

    # ── Silver Task Group ──────────────────────────────────────────────────
    with TaskGroup(
        "silver", tooltip="Load CBS vacancy data into the warehouse"
    ) as silver:
        load_cbs = PythonOperator(
            task_id="load_cbs_to_warehouse",
            python_callable=_load_cbs_to_warehouse,
            retries=2,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=10),
        )

    # ── Dependency chain ───────────────────────────────────────────────────
    bronze >> silver
