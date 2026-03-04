#!/usr/bin/env python3
"""Run the Dutch IT job market pipeline and persist metadata into platform_metadata."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.ingestion.common.metadata_store import (  # noqa: E402
    ensure_metadata_tables,
    insert_data_quality_result,
    insert_dataset_version,
    insert_lineage_edge,
    insert_pipeline_task_run,
    now_utc,
    upsert_dataset_registry,
    upsert_pipeline_run,
)
from src.ingestion.common.dag_helpers import resolve_code_version  # noqa: E402


class _Conn:
    def __init__(self) -> None:
        self.host = os.getenv("WAREHOUSE_HOST", "localhost")
        self.port = int(os.getenv("WAREHOUSE_PORT", "5433"))
        self.schema = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
        self.login = os.getenv("WAREHOUSE_USER", "admin")
        self.password = os.getenv("WAREHOUSE_PASSWORD", "admin")


GOLD_DATASETS = [
    ("job_market_nl.it_market_snapshot", "it_market_snapshot"),
    ("job_market_nl.it_market_top_skills", "it_market_top_skills"),
    ("job_market_nl.it_market_region_distribution", "it_market_region_distribution"),
    ("job_market_nl.it_market_job_ads_geo", "it_market_job_ads_geo"),
]

UPSTREAMS = {
    "job_market_nl.it_market_snapshot": ["silver.cbs_vacancy_rate", "silver.adzuna_job_ads"],
    "job_market_nl.it_market_top_skills": ["silver.adzuna_job_ads"],
    "job_market_nl.it_market_region_distribution": ["silver.adzuna_job_ads"],
    "job_market_nl.it_market_job_ads_geo": ["silver.adzuna_job_ads"],
}


def _connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "open_data_platform_dw"),
        user=os.getenv("WAREHOUSE_USER", "admin"),
        password=os.getenv("WAREHOUSE_PASSWORD", "admin"),
    )


def _table_columns(conn: psycopg2.extensions.connection, schema_name: str, table_name: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        rows = cur.fetchall()
    return [
        {
            "name": row[0],
            "data_type": row[1],
            "nullable": row[2] == "YES",
        }
        for row in rows
    ]


def _table_row_count(conn: psycopg2.extensions.connection, fqn: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {fqn}")
        row = cur.fetchone()
    return int(row[0] if row else 0)


def main() -> int:
    conn_adapter = _Conn()
    run_id = f"job_market_nl_metadata_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    started_at = now_utc()

    ensure_metadata_tables(conn=conn_adapter)
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name="job_market_nl.metadata_pipeline",
        dag_id="job_market_nl.metadata_pipeline",
        source_name="job_market_nl",
        dataset="it_market",
        status="RUNNING",
        triggered_by=os.getenv("USER", "local"),
        code_version=resolve_code_version(),
        started_at_utc=started_at,
        metadata={"mode": "parallel_metadata_pipeline"},
        conn=conn_adapter,
    )

    exit_code = 0
    error_message = None
    try:
        from scripts.pipeline.run_job_market_pipeline import main as run_job_market_pipeline_main

        task_start = now_utc()
        run_job_market_pipeline_main()
        insert_pipeline_task_run(
            run_id=run_id,
            pipeline_name="job_market_nl.metadata_pipeline",
            task_id="run_job_market_pipeline",
            task_group="pipeline",
            status="SUCCESS",
            started_at_utc=task_start,
            finished_at_utc=now_utc(),
            conn=conn_adapter,
        )

        with _connect() as warehouse_conn:
            for dataset_id, table_name in GOLD_DATASETS:
                schema_name = dataset_id.split(".", 1)[0]
                columns = _table_columns(warehouse_conn, schema_name, table_name)
                row_count = _table_row_count(warehouse_conn, dataset_id)

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
                    metadata={"generated_by": "run_job_market_metadata_pipeline.py"},
                    conn=conn_adapter,
                )

                insert_dataset_version(
                    dataset_id=dataset_id,
                    version_label=run_id,
                    schema_hash=None,
                    column_schema=columns,
                    row_count=row_count,
                    byte_size=None,
                    run_id=run_id,
                    metadata={"refresh_ts": now_utc().isoformat()},
                    conn=conn_adapter,
                )

                insert_data_quality_result(
                    run_id=run_id,
                    pipeline_name="job_market_nl.metadata_pipeline",
                    dataset_id=dataset_id,
                    assertion_id=f"row_count_positive:{dataset_id}",
                    assertion_type="row_count",
                    severity="medium",
                    status="PASS" if row_count > 0 else "FAIL",
                    observed_value=str(row_count),
                    expected_value=">0",
                    details={"table": dataset_id},
                    conn=conn_adapter,
                )

                for upstream in UPSTREAMS.get(dataset_id, []):
                    insert_lineage_edge(
                        run_id=run_id,
                        pipeline_name="job_market_nl.metadata_pipeline",
                        upstream_dataset=upstream,
                        downstream_dataset=dataset_id,
                        transformation_type="TRANSFORMED",
                        metadata={"source": "job_market_nl rules"},
                        conn=conn_adapter,
                    )

    except Exception as exc:
        exit_code = 1
        error_message = str(exc)

    finished_at = now_utc()
    upsert_pipeline_run(
        run_id=run_id,
        pipeline_name="job_market_nl.metadata_pipeline",
        dag_id="job_market_nl.metadata_pipeline",
        source_name="job_market_nl",
        dataset="it_market",
        status="SUCCESS" if exit_code == 0 else "FAILED",
        triggered_by=os.getenv("USER", "local"),
        code_version=resolve_code_version(),
        started_at_utc=started_at,
        finished_at_utc=finished_at,
        metadata={"error": error_message} if error_message else {"mode": "parallel_metadata_pipeline"},
        conn=conn_adapter,
    )

    if exit_code == 0:
        print(f"[Job Market Metadata Pipeline] SUCCESS run_id={run_id}")
    else:
        print(f"[Job Market Metadata Pipeline] FAILED run_id={run_id} error={error_message}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
