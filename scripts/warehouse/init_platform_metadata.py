#!/usr/bin/env python3
"""Initialize platform metadata schema/tables in the warehouse."""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.ingestion.common.metadata_store import ensure_metadata_tables, now_utc, upsert_pipeline_run  # noqa: E402


class _Conn:
    def __init__(self) -> None:
        self.host = os.getenv("WAREHOUSE_HOST", "localhost")
        self.port = int(os.getenv("WAREHOUSE_PORT", "5433"))
        self.schema = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
        self.login = os.getenv("WAREHOUSE_USER", "admin")
        self.password = os.getenv("WAREHOUSE_PASSWORD", "admin")


def main() -> int:
    conn = _Conn()
    ensure_metadata_tables(conn=conn)
    upsert_pipeline_run(
        run_id="platform_metadata_bootstrap",
        pipeline_name="platform_metadata.bootstrap",
        dag_id="platform_metadata.bootstrap",
        source_name="platform",
        dataset="metadata",
        status="SUCCESS",
        triggered_by=os.getenv("USER", "bootstrap"),
        code_version=os.getenv("GITHUB_SHA", "local"),
        started_at_utc=now_utc(),
        finished_at_utc=now_utc(),
        metadata={"action": "ensure_metadata_tables"},
        conn=conn,
    )

    print("Initialized warehouse metadata schema: platform_metadata")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
