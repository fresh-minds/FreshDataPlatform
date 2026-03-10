#!/usr/bin/env python3
"""Bootstrap the ODP Staffing Demand Superset assets.

Phase 1 routes the new canonical script name to the existing gold dashboard
bootstrap implementation.
"""

from scripts.superset.superset_bootstrap_gold_dashboards import main


if __name__ == "__main__":
    main()
