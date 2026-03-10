#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

import psycopg2

from src.ingestion.common.dag_helpers import resolve_code_version
from src.ingestion.common.metadata_store import ensure_metadata_tables, upsert_pipeline_run


class _WarehouseConnAdapter:
    def __init__(self) -> None:
        self.host = os.getenv("WAREHOUSE_HOST", "localhost")
        self.port = int(os.getenv("WAREHOUSE_PORT", "5433"))
        self.schema = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
        self.login = os.getenv("WAREHOUSE_USER", "admin")
        self.password = os.getenv("WAREHOUSE_PASSWORD", "admin")


def _connect():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "open_data_platform_dw"),
        user=os.getenv("WAREHOUSE_USER", "admin"),
        password=os.getenv("WAREHOUSE_PASSWORD", "admin"),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Log pipeline run metadata into platform_audit.pipeline_runs")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--status", required=True, choices=["RUNNING", "SUCCESS", "FAILED"])
    parser.add_argument("--started-at-utc", required=True)
    parser.add_argument("--finished-at-utc", required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    triggered_by = os.getenv("GITHUB_ACTOR") or os.getenv("USER") or os.getenv("USERNAME") or "unknown"
    code_version = resolve_code_version()

    # Validate timestamp inputs eagerly.
    datetime.fromisoformat(args.started_at_utc.replace("Z", "+00:00"))
    datetime.fromisoformat(args.finished_at_utc.replace("Z", "+00:00"))

    conn = _connect()
    conn_adapter = _WarehouseConnAdapter()
    try:
        ensure_metadata_tables(conn=conn_adapter)
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS platform_audit")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_audit.pipeline_runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    triggered_by TEXT NOT NULL,
                    code_version TEXT NOT NULL,
                    started_at_utc TIMESTAMPTZ NOT NULL,
                    finished_at_utc TIMESTAMPTZ NOT NULL,
                    logged_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO platform_audit.pipeline_runs (
                    run_id,
                    pipeline_name,
                    status,
                    triggered_by,
                    code_version,
                    started_at_utc,
                    finished_at_utc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE
                SET
                    pipeline_name = EXCLUDED.pipeline_name,
                    status = EXCLUDED.status,
                    triggered_by = EXCLUDED.triggered_by,
                    code_version = EXCLUDED.code_version,
                    started_at_utc = EXCLUDED.started_at_utc,
                    finished_at_utc = EXCLUDED.finished_at_utc,
                    logged_at_utc = now()
                """,
                (
                    args.run_id,
                    args.pipeline_name,
                    args.status,
                    triggered_by,
                    code_version,
                    args.started_at_utc,
                    args.finished_at_utc,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    upsert_pipeline_run(
        run_id=args.run_id,
        pipeline_name=args.pipeline_name,
        dag_id=args.pipeline_name,
        source_name="qa",
        dataset="governance",
        status=args.status,
        triggered_by=triggered_by,
        code_version=code_version,
        started_at_utc=datetime.fromisoformat(args.started_at_utc.replace("Z", "+00:00")),
        finished_at_utc=datetime.fromisoformat(args.finished_at_utc.replace("Z", "+00:00")),
        metadata={"origin": "tests/e2e/scripts/log_pipeline_run.py"},
        conn=conn_adapter,
    )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(
        f"Logged pipeline run metadata at {now}: "
        f"run_id={args.run_id}, pipeline={args.pipeline_name}, status={args.status}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
