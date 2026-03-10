"""
UWV Open Match Pipeline DAG

Orchestrates the bronze → silver ingestion of UWV Open Match vacancy data
into the Postgres warehouse.

UWV (Uitvoeringsinstituut Werknemersverzekeringen) publishes open vacancy
data at a URL configured via the environment variable ``UWV_OPEN_MATCH_URL``.
The file is a CSV or ZIP-wrapped CSV.

  1. Bronze — download the CSV/ZIP from UWV_OPEN_MATCH_URL, parse the rows,
              and push them to XCom.  Falls back to a small mock dataset when
              the URL is not set or LOCAL_MOCK_EXTERNAL=true.
  2. Silver — clean, deduplicate, and TRUNCATE + INSERT all rows into the
              Postgres warehouse table `odp_staffing_demand.uwv_vacancies`.

Schedule: daily at 02:00 UTC.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

_DAG_ID = "uwv_open_match_pipeline"

# ---------------------------------------------------------------------------
# Mock fallback data — used when UWV_OPEN_MATCH_URL is unset or LOCAL_MOCK_EXTERNAL=true
# ---------------------------------------------------------------------------

_MOCK_UWV_ROWS: List[Dict[str, Any]] = [
    {
        "vacancy_id": "uwv-mock-1",
        "occupation": "Software Developer",
        "region": "Noord-Holland",
        "posted_date": "2024-01-10",
        "employment_type": "permanent",
        "work_time": "full_time",
    },
    {
        "vacancy_id": "uwv-mock-2",
        "occupation": "Data Engineer",
        "region": "Zuid-Holland",
        "posted_date": "2024-01-11",
        "employment_type": "permanent",
        "work_time": "full_time",
    },
    {
        "vacancy_id": "uwv-mock-3",
        "occupation": "DevOps Engineer",
        "region": "Utrecht",
        "posted_date": "2024-01-12",
        "employment_type": "temporary",
        "work_time": "full_time",
    },
]


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
        source_name="uwv_nl",
        dataset="uwv_vacancies",
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

def _fetch_uwv_data(**kwargs):
    """
    Bronze: download the UWV Open Match CSV/ZIP from ``UWV_OPEN_MATCH_URL``
    and push a list of row dicts to XCom under key ``uwv_rows``.

    Falls back to mock data when the URL env var is absent or when
    ``LOCAL_MOCK_EXTERNAL=true``.
    """
    import io
    import os
    import zipfile

    import requests

    url = os.getenv("UWV_OPEN_MATCH_URL")
    use_mock = os.getenv("LOCAL_MOCK_EXTERNAL", "false").lower() == "true"

    if not url or use_mock:
        reason = "LOCAL_MOCK_EXTERNAL=true" if use_mock else "UWV_OPEN_MATCH_URL not set"
        print(f"[UWV Bronze] {reason} — using mock data ({len(_MOCK_UWV_ROWS)} rows)")
        kwargs["ti"].xcom_push(key="uwv_rows", value=_MOCK_UWV_ROWS)
        _log_task_success(
            kwargs=kwargs,
            task_group="bronze",
            task_id="fetch_uwv_data",
            metadata={"rows_fetched": len(_MOCK_UWV_ROWS), "source": "mock"},
        )
        return

    print(f"[UWV Bronze] Downloading from {url} ...")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    content = response.content

    # Parse CSV — supports plain CSV or ZIP-wrapped CSV
    try:
        import pandas as pd

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_names:
                    raise ValueError("No CSV found in ZIP archive")
                with zf.open(csv_names[0]) as csv_file:
                    df = pd.read_csv(csv_file)
        except zipfile.BadZipFile:
            df = pd.read_csv(io.BytesIO(content))

        if df.empty:
            print("[UWV Bronze] Downloaded CSV is empty — using mock data")
            rows = list(_MOCK_UWV_ROWS)
        else:
            # Normalise column names to snake_case
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            rows = df.to_dict(orient="records")
            print(f"[UWV Bronze] ✓ Parsed {len(rows)} rows from UWV CSV")

    except Exception as exc:
        print(f"[UWV Bronze] CSV parse error: {exc} — using mock data")
        rows = list(_MOCK_UWV_ROWS)

    kwargs["ti"].xcom_push(key="uwv_rows", value=rows)
    _log_task_success(
        kwargs=kwargs,
        task_group="bronze",
        task_id="fetch_uwv_data",
        metadata={"rows_fetched": len(rows), "source": url},
    )


def _load_uwv_to_warehouse(**kwargs):
    """
    Silver: pull the UWV rows from XCom, normalise the required columns,
    and TRUNCATE + INSERT into `odp_staffing_demand.uwv_vacancies`.
    Registers lineage and dataset metadata.
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
    uwv_rows = ti.xcom_pull(key="uwv_rows", task_ids="bronze.fetch_uwv_data")

    if not uwv_rows:
        print("[UWV Silver] No rows received from XCom — skipping load.")
        return

    ingested_at = datetime.now(timezone.utc)

    # Build insert tuples — map common column-name variants
    rows = []
    seen_ids: set = set()
    for r in uwv_rows:
        # Column name normalisation: UWV CSVs may use different casing
        vacancy_id = str(
            r.get("vacancy_id") or r.get("id") or r.get("vacatureid") or ""
        ).strip()
        if not vacancy_id or vacancy_id in seen_ids:
            continue
        seen_ids.add(vacancy_id)
        rows.append((
            vacancy_id,
            str(r.get("occupation") or r.get("beroep") or r.get("functie") or "").strip() or None,
            str(r.get("region") or r.get("regio") or r.get("provincie") or "").strip() or None,
            str(r.get("posted_date") or r.get("plaatsingsdatum") or r.get("datum") or "").strip() or None,
            str(r.get("employment_type") or r.get("dienstverband") or "").strip() or None,
            str(r.get("work_time") or r.get("werktijd") or r.get("uren") or "").strip() or None,
            ingested_at,
        ))

    if not rows:
        print("[UWV Silver] All rows were empty or duplicate — nothing to load.")
        return

    conn = _connect_warehouse()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE odp_staffing_demand.uwv_vacancies")
            execute_values(
                cur,
                """
                INSERT INTO odp_staffing_demand.uwv_vacancies
                  (vacancy_id, occupation, region, posted_date, employment_type,
                   work_time, ingestion_timestamp)
                VALUES %s
                """,
                rows,
            )
        conn.commit()
        print(f"[UWV Silver] ✓ Loaded {len(rows)} rows into odp_staffing_demand.uwv_vacancies")
    finally:
        conn.close()

    # ── Metadata & lineage ────────────────────────────────────────────────────
    _, run_id, triggered_by, code_version = _metadata_context(**kwargs)
    meta_conn = try_get_conn("postgres_warehouse")

    upsert_dataset_registry(
        dataset_id="odp_staffing_demand.uwv_vacancies",
        layer="silver",
        domain="odp_staffing_demand",
        schema_name="odp_staffing_demand",
        table_name="uwv_vacancies",
        owner="data-platform",
        classification="internal",
        sensitivity="public",
        retention_days=365,
        metadata={"pipeline": _DAG_ID, "source": "UWV Open Match"},
        conn=meta_conn,
    )
    insert_dataset_version(
        dataset_id="odp_staffing_demand.uwv_vacancies",
        version_label=run_id,
        schema_hash=None,
        column_schema=[],
        row_count=len(rows),
        byte_size=None,
        run_id=run_id,
        metadata={"loaded_at": now_utc().isoformat()},
        conn=meta_conn,
    )
    insert_lineage_edge(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        upstream_dataset="bronze.uwv_open_match_csv",
        downstream_dataset="odp_staffing_demand.uwv_vacancies",
        transformation_type="CLEANED",
        metadata={"task": "load_uwv_to_warehouse", "rows_loaded": len(rows)},
        conn=meta_conn,
    )
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name=_DAG_ID,
        dag_id=_DAG_ID,
        source_name="uwv_nl",
        dataset="uwv_vacancies",
        status="SUCCESS",
        triggered_by=triggered_by,
        code_version=code_version,
        finished_at_utc=now_utc(),
        metadata={"rows_loaded": len(rows)},
        conn=meta_conn,
    )

    _log_task_success(
        kwargs=kwargs,
        task_group="silver",
        task_id="load_uwv_to_warehouse",
        metadata={"rows_loaded": len(rows)},
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
        "UWV Open Match NL pipeline: CSV download → Postgres warehouse "
        "(bronze → silver).  Requires UWV_OPEN_MATCH_URL env var; "
        "falls back to mock data when absent."
    ),
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    catchup=False,
    tags=["uwv", "job_market", "nl", "bronze", "silver", "open_data"],
    sla_miss_callback=None,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag:

    # ── Bronze Task Group ──────────────────────────────────────────────────
    with TaskGroup(
        "bronze", tooltip="Download UWV Open Match vacancy CSV"
    ) as bronze:
        fetch_uwv = PythonOperator(
            task_id="fetch_uwv_data",
            python_callable=_fetch_uwv_data,
            retries=3,
            retry_delay=timedelta(minutes=3),
            sla=timedelta(minutes=20),
        )

    # ── Silver Task Group ──────────────────────────────────────────────────
    with TaskGroup(
        "silver", tooltip="Clean and load UWV vacancies into the warehouse"
    ) as silver:
        load_uwv = PythonOperator(
            task_id="load_uwv_to_warehouse",
            python_callable=_load_uwv_to_warehouse,
            retries=2,
            retry_delay=timedelta(minutes=2),
            sla=timedelta(minutes=10),
        )

    # ── Dependency chain ───────────────────────────────────────────────────
    bronze >> silver
