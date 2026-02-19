#!/usr/bin/env python3
"""Ensure Job Market source tables exist before dbt models reference them."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.job_market_nl.postgres_pipeline import _connect_warehouse, ensure_tables  # noqa: E402


def main() -> int:
    conn = _connect_warehouse()
    try:
        ensure_tables(conn)
    finally:
        conn.close()

    print(
        "[bootstrap] Ensured job market source tables exist "
        "(job_market_nl.it_market_snapshot, job_market_nl.it_market_top_skills, "
        "job_market_nl.it_market_region_distribution, job_market_nl.it_market_job_ads_geo).",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
