#!/usr/bin/env python3
"""Ensure Job Market source tables exist before dbt models reference them."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.odp_staffing_demand.postgres_pipeline import _connect_warehouse, ensure_tables  # noqa: E402


def main() -> int:
    conn = _connect_warehouse()
    try:
        ensure_tables(conn)
    finally:
        conn.close()

    print(
        "[bootstrap] Ensured job market source tables exist "
        "(odp_staffing_demand.it_market_snapshot, odp_staffing_demand.it_market_top_skills, "
        "odp_staffing_demand.it_market_region_distribution, odp_staffing_demand.it_market_job_ads_geo, "
        "odp_staffing_demand.harvey_nash_vacatures, odp_staffing_demand.cbs_vacancy_rate, "
        "odp_staffing_demand.uwv_vacancies).",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
