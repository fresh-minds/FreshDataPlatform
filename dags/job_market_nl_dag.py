"""
NL IT Job Market Gold Pipeline DAG

Publishes the curated gold-layer warehouse tables by reading from the silver
Postgres tables that are populated by the dedicated source DAGs:

  - ``cbs_vacancy_rate_dag``   → job_market_nl.cbs_vacancy_rate
  - ``adzuna_job_ads_dag``     → job_market_nl.it_market_{top_skills,region_distribution,job_ads_geo}
  - ``uwv_open_match_dag``     → job_market_nl.uwv_vacancies
  - ``harvey_nash_vacatures_dag`` → job_market_nl.harvey_nash_vacatures

This DAG only runs the Gold and Quality task groups:

  1. Gold    — build the ``it_market_snapshot`` fact row from the latest CBS
               silver data and the Adzuna job-ad count, then write it to the
               warehouse.
  2. Quality — run data quality checks against all freshly loaded tables.

Scheduling note: schedule this DAG after the source DAGs have completed.
A simple approach is to set the schedule to 03:00 UTC so the silver DAGs
(01:00, 01:30, 02:00 UTC) have time to finish.  For a strict dependency,
wire up Airflow ExternalTaskSensors pointing at each source DAG.
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
    from datetime import datetime, timezone

    from src.ingestion.common.dag_helpers import resolve_code_version

    ti = kwargs["ti"]
    dag_run = kwargs.get("dag_run")
    run_id = getattr(dag_run, "run_id", None) or f"{_DAG_ID}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    triggered_by = os.environ.get("USER", "airflow")
    code_version = resolve_code_version()
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

def _load_to_warehouse(**kwargs):
    """
    Gold: build ``it_market_snapshot`` from pre-populated silver tables.

    Reads the latest CBS vacancy data from ``job_market_nl.cbs_vacancy_rate``
    and the current Adzuna job-ad count from ``job_market_nl.it_market_job_ads_geo``,
    then TRUNCATE + INSERTs a single snapshot row into
    ``job_market_nl.it_market_snapshot``.

    Prerequisite: ``cbs_vacancy_rate_dag`` and ``adzuna_job_ads_dag`` must have
    run and populated their respective silver tables before this task executes.
    """
    from pipelines.job_market_nl.postgres_pipeline import (
        SnapshotRow,
        _connect_warehouse,
        ensure_tables,
    )
    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        insert_dataset_version,
        insert_lineage_edge,
        now_utc,
        upsert_dataset_registry,
    )

    conn = _connect_warehouse()
    try:
        ensure_tables(conn)

        with conn.cursor() as cur:
            # ── Read latest CBS silver row ──────────────────────────────────
            cur.execute(
                """
                SELECT period_key, period_label, sector_name, vacancies, vacancy_rate
                FROM job_market_nl.cbs_vacancy_rate
                ORDER BY ingestion_timestamp DESC
                LIMIT 1
                """
            )
            cbs_row = cur.fetchone()

        if not cbs_row:
            raise RuntimeError(
                "[Gold] job_market_nl.cbs_vacancy_rate is empty. "
                "Ensure cbs_vacancy_rate_dag has run successfully before this DAG."
            )

        period_key, period_label, sector_name, vacancies, vacancy_rate = cbs_row

        with conn.cursor() as cur:
            # ── Count Adzuna geo rows as job_ads_count ──────────────────────
            cur.execute("SELECT COUNT(*) FROM job_market_nl.it_market_job_ads_geo")
            job_ads_count = cur.fetchone()[0] or 0

        snapshot = SnapshotRow(
            period_key=period_key,
            period_label=period_label,
            sector_name=sector_name,
            vacancies=vacancies,
            vacancy_rate=vacancy_rate,
            job_ads_count=job_ads_count,
        )

        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_snapshot")
            cur.execute(
                """
                INSERT INTO job_market_nl.it_market_snapshot
                  (period_key, period_label, sector_name, vacancies, vacancy_rate, job_ads_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    snapshot.period_key,
                    snapshot.period_label,
                    snapshot.sector_name,
                    snapshot.vacancies,
                    snapshot.vacancy_rate,
                    snapshot.job_ads_count,
                ),
            )
        conn.commit()
        print(
            f"[Gold] ✓ Snapshot: period={snapshot.period_key}, "
            f"vacancies={snapshot.vacancies}, rate={snapshot.vacancy_rate}, "
            f"job_ads={snapshot.job_ads_count}"
        )
    finally:
        conn.close()

    # ── Metadata & lineage ────────────────────────────────────────────────────
    _, run_id, _, _ = _metadata_context(**kwargs)
    meta_conn = try_get_conn("postgres_warehouse")

    upsert_dataset_registry(
        dataset_id="job_market_nl.it_market_snapshot",
        layer="gold",
        domain="job_market_nl",
        schema_name="job_market_nl",
        table_name="it_market_snapshot",
        owner="data-platform",
        classification="internal",
        sensitivity="internal",
        retention_days=365,
        metadata={"pipeline": _DAG_ID},
        conn=meta_conn,
    )
    insert_dataset_version(
        dataset_id="job_market_nl.it_market_snapshot",
        version_label=run_id,
        schema_hash=None,
        column_schema=[],
        row_count=1,
        byte_size=None,
        run_id=run_id,
        metadata={"loaded_at": now_utc().isoformat()},
        conn=meta_conn,
    )

    lineage = {
        "job_market_nl.it_market_snapshot": [
            "job_market_nl.cbs_vacancy_rate",
            "job_market_nl.it_market_job_ads_geo",
        ],
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
                conn=meta_conn,
            )

    _log_task_success(
        kwargs=kwargs,
        task_group="gold",
        task_id="load_to_warehouse",
        metadata={
            "snapshot_rows": 1,
            "period_key": period_key,
            "job_ads_count": job_ads_count,
        },
    )
    print("[Export] ✓ Loaded it_market_snapshot to Postgres warehouse")


def _run_dbt_gold(**kwargs):
    """
    Gold: materialise all gold-layer tables in freshminds_dw via dbt.

    Runs after ``_load_to_warehouse`` has populated the silver source tables
    (it_market_snapshot, harvey_nash_vacatures, etc.) that dbt reads from.
    Uses subprocess — same pattern as ``_run_quality_checks`` — so that
    stdout/stderr are forwarded to Airflow task logs and a non-zero exit code
    marks the task FAILED with retries.
    """
    import os
    import shutil
    import subprocess

    from src.ingestion.common.dag_helpers import try_get_conn
    from src.ingestion.common.metadata_store import (
        insert_lineage_edge,
        upsert_dataset_registry,
    )

    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    candidate_dbt_dirs = [
        os.path.join(airflow_home, "project", "dbt"),
        os.path.join(airflow_home, "dbt"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt"),
    ]
    dbt_project_dir = next(
        (
            path
            for path in candidate_dbt_dirs
            if os.path.exists(os.path.join(path, "dbt_project.yml"))
        ),
        None,
    )
    if not dbt_project_dir:
        raise RuntimeError(
            "[dbt] Could not locate dbt project directory. Checked: "
            + ", ".join(candidate_dbt_dirs)
        )
    dbt_profiles_dir = dbt_project_dir
    dbt_bin = shutil.which("dbt")
    if not dbt_bin:
        raise RuntimeError(
            "[dbt] dbt executable not found on PATH in Airflow runtime. "
            "Install dbt in the Airflow image or expose it via PATH."
        )
    dbt_packages_dir = os.path.join(dbt_project_dir, "dbt_packages")

    # Forward warehouse credentials; fall back to local dev defaults.
    env = os.environ.copy()
    env.setdefault("WAREHOUSE_DB",       "freshminds_dw")
    env.setdefault("WAREHOUSE_HOST",     "warehouse")
    env.setdefault("WAREHOUSE_PORT",     "5432")
    env.setdefault("WAREHOUSE_USER",     "admin")
    env.setdefault("WAREHOUSE_PASSWORD", "admin")

    # Install dbt packages if the packages directory is absent or empty.
    if not os.path.isdir(dbt_packages_dir) or not os.listdir(dbt_packages_dir):
        print("[dbt] Running dbt deps to install packages...")
        deps = subprocess.run(
            [
                dbt_bin,
                "deps",
                "--project-dir", dbt_project_dir,
                "--profiles-dir", dbt_profiles_dir,
            ],
            capture_output=True,
            text=True,
            env=env,
        )
        print(deps.stdout)
        if deps.returncode != 0:
            print(deps.stderr)
            raise RuntimeError(
                f"[dbt] dbt deps failed (exit {deps.returncode}):\n{deps.stderr[-4000:]}"
            )

    # Run all gold models.  dbt resolves upstream bronze/silver refs automatically,
    # creating or refreshing those views in the same run.
    print("[dbt] Running: dbt run --select +gold ...")
    run = subprocess.run(
        [
            dbt_bin,
            "run",
            "--project-dir", dbt_project_dir,
            "--profiles-dir", dbt_profiles_dir,
            "--threads", "4",
            "--select", "+gold",
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    print(run.stdout)
    if run.returncode != 0:
        print(run.stderr)
        raise RuntimeError(
            f"[dbt] dbt run failed (exit {run.returncode}):\n{run.stderr[-4000:]}"
        )

    print("[dbt] ✓ Gold materialisation complete.")

    # ── Metadata & lineage ─────────────────────────────────────────────────
    _, run_id, _, _ = _metadata_context(**kwargs)
    meta_conn = try_get_conn("postgres_warehouse")

    # Register every gold table in the dataset registry.
    for dataset_id, table_name in [
        ("gold.dim_date",                  "dim_date"),
        ("gold.dim_region",                "dim_region"),
        ("gold.dim_company",               "dim_company"),
        ("gold.dim_period",                "dim_period"),
        ("gold.dim_sector",                "dim_sector"),
        ("gold.fact_it_market_snapshot",   "fact_it_market_snapshot"),
        ("gold.fact_it_market_top_skills", "fact_it_market_top_skills"),
        ("gold.fact_job_postings",         "fact_job_postings"),
    ]:
        upsert_dataset_registry(
            dataset_id=dataset_id,
            layer="gold",
            domain="job_market_nl",
            schema_name="gold",
            table_name=table_name,
            owner="data-platform",
            classification="internal",
            sensitivity="internal",
            retention_days=365,
            metadata={"pipeline": _DAG_ID, "built_by": "dbt"},
            conn=meta_conn,
        )

    # Record silver → gold lineage for the key fact tables.
    gold_lineage = {
        "gold.fact_it_market_snapshot": [
            "job_market_nl.it_market_snapshot",
            "job_market_nl.it_market_region_distribution",
            "job_market_nl.it_market_top_skills",
        ],
        "gold.fact_it_market_top_skills": [
            "job_market_nl.it_market_top_skills",
        ],
        "gold.fact_job_postings": [
            "job_market_nl.harvey_nash_vacatures",
        ],
    }
    for downstream, upstreams in gold_lineage.items():
        for upstream in upstreams:
            insert_lineage_edge(
                run_id=run_id,
                pipeline_name=_DAG_ID,
                upstream_dataset=upstream,
                downstream_dataset=downstream,
                transformation_type="TRANSFORMED",
                metadata={"task": "run_dbt_gold", "tool": "dbt"},
                conn=meta_conn,
            )

    _log_task_success(
        kwargs=kwargs,
        task_group="gold",
        task_id="run_dbt_gold",
        metadata={"dbt_return_code": run.returncode},
    )
    print("[dbt] ✓ Gold tables registered in metadata store.")


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
    description=(
        "NL IT job market gold pipeline — reads from silver tables populated by "
        "cbs_vacancy_rate_dag, adzuna_job_ads_dag, and uwv_open_match_dag, "
        "then publishes it_market_snapshot and runs quality checks."
    ),
    schedule_interval="0 3 * * *",  # 03:00 UTC daily — after all silver DAGs finish
    catchup=False,
    tags=["job_market", "nl", "gold", "metadata"],
    sla_miss_callback=None,  # TODO: wire up to alerting (e.g. scripts/send_alert.py)
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
) as dag:

    # ── Gold Task Group ────────────────────────────────────────────────────
    with TaskGroup("gold", tooltip="Publish curated warehouse tables from silver sources") as gold:
        load_warehouse = PythonOperator(
            task_id="load_to_warehouse",
            python_callable=_load_to_warehouse,
            retries=2,
            retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=10),
        )

        run_dbt_gold = PythonOperator(
            task_id="run_dbt_gold",
            python_callable=_run_dbt_gold,
            retries=1,
            retry_delay=timedelta(minutes=5),
            sla=timedelta(minutes=20),
        )

        # load_to_warehouse (silver) must complete before dbt builds gold tables.
        load_warehouse >> run_dbt_gold

    # ── Quality Task Group ─────────────────────────────────────────────────
    with TaskGroup("quality", tooltip="Run data quality checks") as quality:
        run_dq = PythonOperator(
            task_id="run_quality_checks",
            python_callable=_run_quality_checks,
            retries=1,
            retry_delay=timedelta(minutes=1),
            sla=timedelta(minutes=15),
            trigger_rule="all_success",
        )

    # ── Dependency Chain ───────────────────────────────────────────────────
    gold >> quality
