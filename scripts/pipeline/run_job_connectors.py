#!/usr/bin/env python3
from __future__ import annotations

# ruff: noqa: E402
import argparse
import logging
import os
import sys

# Ensure project root is in path for local execution.
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from shared.job_connectors.registry import available_connectors, build_enabled_connectors, enabled_connector_names
from shared.job_connectors.env import bool_env, parse_csv
from shared.job_connectors.runner import build_http_client_from_env, build_raw_store_from_env, run_connectors
from shared.job_connectors.sinks import JsonlSink, StdoutSink


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Open Data Platform job scraping connectors (RSS + Sitemap).")
    parser.add_argument("--list", action="store_true", help="List available connectors and exit.")
    parser.add_argument("--connector", help="Run a single connector by name (e.g. rss, sitemap).")
    parser.add_argument("--dry-run", action="store_true", help="Print normalized jobs to stdout (no writes).")
    parser.add_argument(
        "--output", help="Output JSONL path (default: JOB_CONNECTORS_OUTPUT_PATH or ./data/job_connectors/jobs.jsonl)."
    )
    parser.add_argument(
        "--allow-all-hosts",
        action="store_true",
        help="Disable host allowlist requirement for this run (NOT recommended; ensure ToS/robots compliance yourself).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

    registry = available_connectors()
    if args.list:
        print("Available connectors:", ", ".join(sorted(registry.keys())))
        print("Enabled connectors:", ", ".join(enabled_connector_names()))
        return 0

    if args.allow_all_hosts:
        os.environ["JOB_CONNECTORS_REQUIRE_ALLOWLIST"] = "false"
    else:
        require_allowlist = bool_env("JOB_CONNECTORS_REQUIRE_ALLOWLIST", True)
        allowlist_hosts = parse_csv(os.getenv("JOB_CONNECTORS_ALLOWLIST_HOSTS"))
        if require_allowlist and not allowlist_hosts:
            print("JOB_CONNECTORS_ALLOWLIST_HOSTS is empty but JOB_CONNECTORS_REQUIRE_ALLOWLIST=true.")
            print("Set JOB_CONNECTORS_ALLOWLIST_HOSTS or pass --allow-all-hosts (NOT recommended).")
            return 2

    if args.connector:
        connector = registry.get(args.connector.strip().lower())
        if connector is None:
            print(f"Unknown connector: {args.connector}")
            print("Available connectors:", ", ".join(sorted(registry.keys())))
            return 2
        connectors = [connector]
    else:
        connectors = build_enabled_connectors()

    if args.dry_run:
        sink = StdoutSink()
    else:
        out_path = args.output or os.getenv("JOB_CONNECTORS_OUTPUT_PATH", "./data/job_connectors/jobs.jsonl")
        sink = JsonlSink(out_path)

    raw_store = build_raw_store_from_env()

    with build_http_client_from_env() as http:
        summary = run_connectors(connectors, http=http, sink=sink, raw_store=raw_store)

    total_inserted = sum(s.inserted for s in summary.connectors)
    total_updated = sum(s.updated for s in summary.connectors)
    total_skipped = sum(s.skipped for s in summary.connectors)
    total_failed = sum(s.failed for s in summary.connectors)

    print(
        f"Run summary run_id={summary.run_id} inserted={total_inserted} updated={total_updated} "
        f"skipped={total_skipped} failed={total_failed}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
