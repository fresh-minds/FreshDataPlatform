#!/usr/bin/env python3
"""
Local pipeline runner for Open Data Platform.

Provides a unified interface for running data pipelines locally
with full support for the same code that runs in Microsoft Fabric.

Usage:
    python scripts/pipeline/run_local.py --pipeline job_market_nl.bronze_adzuna_jobs
    python scripts/pipeline/run_local.py --list
"""

import argparse
import os
import sys
import time
import uuid
from contextlib import nullcontext
from typing import Callable, Optional, Dict

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

# Force local mode
os.environ["IS_LOCAL"] = "true"

from dotenv import load_dotenv

load_dotenv()

from shared.fabric.runtime import get_fabric_context, get_spark_session
from shared.observability import get_observability


# Registry of available pipelines
PIPELINE_REGISTRY: Dict[str, Callable] = {}
OBSERVABILITY = get_observability("open-data-platform-pipeline-runner")


def register_pipeline(name: str, func: Callable) -> None:
    """Register a pipeline function."""
    PIPELINE_REGISTRY[name] = func


def load_pipelines(only_domain: Optional[str] = None, only_pipeline: Optional[str] = None) -> None:
    """Load pipeline modules and register them, optionally scoped to a domain or pipeline."""
    def _want(name: str) -> bool:
        return only_pipeline is None or only_pipeline == name
    if only_domain in (None, "job_market_nl"):
        if _want("job_market_nl.bronze_cbs_vacancy_rate"):
            from pipelines.job_market_nl.bronze_cbs_vacancy_rate import run_bronze_cbs_vacancy_rate
            register_pipeline("job_market_nl.bronze_cbs_vacancy_rate", run_bronze_cbs_vacancy_rate)

        if _want("job_market_nl.bronze_adzuna_jobs"):
            from pipelines.job_market_nl.bronze_adzuna_jobs import run_bronze_adzuna_jobs
            register_pipeline("job_market_nl.bronze_adzuna_jobs", run_bronze_adzuna_jobs)

        if _want("job_market_nl.bronze_uwv_open_match"):
            from pipelines.job_market_nl.bronze_uwv_open_match import run_bronze_uwv_open_match
            register_pipeline("job_market_nl.bronze_uwv_open_match", run_bronze_uwv_open_match)

        if _want("job_market_nl.silver_cbs_vacancy_rate"):
            from pipelines.job_market_nl.silver_cbs_vacancy_rate import run_silver_cbs_vacancy_rate
            register_pipeline("job_market_nl.silver_cbs_vacancy_rate", run_silver_cbs_vacancy_rate)

        if _want("job_market_nl.silver_adzuna_jobs"):
            from pipelines.job_market_nl.silver_adzuna_jobs import run_silver_adzuna_jobs
            register_pipeline("job_market_nl.silver_adzuna_jobs", run_silver_adzuna_jobs)

        if _want("job_market_nl.silver_uwv_open_match"):
            from pipelines.job_market_nl.silver_uwv_open_match import run_silver_uwv_open_match
            register_pipeline("job_market_nl.silver_uwv_open_match", run_silver_uwv_open_match)

        if _want("job_market_nl.gold_it_market_snapshot"):
            from pipelines.job_market_nl.gold_it_market_snapshot import run_gold_it_market_snapshot
            register_pipeline("job_market_nl.gold_it_market_snapshot", run_gold_it_market_snapshot)

        if _want("job_market_nl.export_to_warehouse"):
            from pipelines.job_market_nl.export_to_warehouse import run_export_job_market_to_warehouse
            register_pipeline("job_market_nl.export_to_warehouse", run_export_job_market_to_warehouse)


def run_pipeline(name: str, dry_run: bool = False) -> None:
    """
    Run a specific pipeline by name.

    Args:
        name: Pipeline name (e.g., "job_market_nl.bronze_adzuna_jobs")
        dry_run: If True, only verify setup without running
    """
    if name not in PIPELINE_REGISTRY:
        print(f"Error: Unknown pipeline '{name}'")
        print(f"Available pipelines: {list(PIPELINE_REGISTRY.keys())}")
        sys.exit(1)

    pipeline_func = PIPELINE_REGISTRY[name]

    print(f"\n{'='*60}")
    print(f"Running pipeline: {name}")
    print(f"{'='*60}\n")

    if os.getenv("LOCAL_MOCK_PIPELINES", "true").lower() == "true":
        print(f"[Mock Mode] Skipping execution for {name}")
        return

    # Initialize runtime context
    spark = get_spark_session(f"Local_{name.replace('.', '_')}")
    notebookutils, fabric = get_fabric_context()

    run_id = str(uuid.uuid4())
    os.environ["PIPELINE_RUN_ID"] = run_id
    start_time = time.perf_counter()

    tracer = OBSERVABILITY.tracer
    pipeline_metrics = OBSERVABILITY.pipeline_metrics
    span_attrs = {
        "pipeline.name": name,
        "pipeline.run_id": run_id,
        "pipeline.domain": name.split(".")[0] if "." in name else name,
        "pipeline.layer": name.split(".")[1].split("_")[0] if "." in name else "unknown",
        "execution.mode": "local",
    }

    if dry_run:
        print("[Dry Run] Setup verified successfully:")
        print(f"  - Spark version: {spark.version}")
        print(f"  - Workspace ID: {fabric.get_workspace_id()}")
        print(f"  - IS_LOCAL: {os.getenv('IS_LOCAL')}")
        return

    span_context = (
        tracer.start_as_current_span("pipeline.run", attributes=span_attrs) if tracer else nullcontext()
    )

    with span_context as span:
        try:
            result = pipeline_func(
                spark=spark,
                notebookutils=notebookutils,
                fabric=fabric,
            )
            duration = time.perf_counter() - start_time
            rows_processed: Optional[int] = None
            print(f"\n{'='*60}")
            print(f"✓ Pipeline '{name}' completed successfully")
            if result is not None:
                if hasattr(result, "count"):
                    rows_processed = result.count()
                    print(f"  Records processed: {rows_processed}")
                else:
                    print(f"  Result: {result}")
            print(f"{'='*60}\n")
            if pipeline_metrics:
                pipeline_metrics.record_success(name, duration, rows_processed)
        except Exception as e:
            duration = time.perf_counter() - start_time
            if span is not None:
                span.record_exception(e)
            if pipeline_metrics:
                pipeline_metrics.record_failure(name, duration)
            print(f"\n{'='*60}")
            print(f"✗ Pipeline '{name}' failed: {e}")
            print(f"{'='*60}\n")
            import traceback

            traceback.print_exc()
            sys.exit(1)


def run_pipelines_by_filter(
    domain: Optional[str] = None,
    layer: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Run multiple pipelines matching filter criteria.

    Args:
        domain: Filter by domain (e.g., "job_market_nl")
        layer: Filter by layer (e.g., "bronze")
        dry_run: If True, only verify setup
    """
    matching = []

    for name in PIPELINE_REGISTRY.keys():
        parts = name.split(".")
        if len(parts) >= 2:
            pipeline_domain = parts[0]
            pipeline_layer = parts[1].split("_")[0]  # e.g., "bronze" from "bronze_sales_orders"

            if domain and pipeline_domain != domain:
                continue
            if layer and pipeline_layer != layer:
                continue

            matching.append(name)

    if not matching:
        print(f"No pipelines match domain='{domain}', layer='{layer}'")
        sys.exit(1)

    print(f"Running {len(matching)} pipeline(s): {matching}")

    for name in matching:
        run_pipeline(name, dry_run=dry_run)


def list_pipelines() -> None:
    """List all available pipelines."""
    print("\nAvailable pipelines:")
    print("-" * 40)
    for name in sorted(PIPELINE_REGISTRY.keys()):
        print(f"  - {name}")
    print()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run data pipelines locally",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/pipeline/run_local.py --pipeline job_market_nl.bronze_adzuna_jobs
  python scripts/pipeline/run_local.py --domain job_market_nl --layer bronze
  python scripts/pipeline/run_local.py --list
  python scripts/pipeline/run_local.py --pipeline job_market_nl.bronze_adzuna_jobs --dry-run
        """,
    )
    parser.add_argument(
        "--pipeline",
        "-p",
        help="Specific pipeline to run (e.g., job_market_nl.bronze_adzuna_jobs)",
    )
    parser.add_argument(
        "--domain",
        "-d",
        help="Run all pipelines for a domain (e.g., job_market_nl)",
    )
    parser.add_argument(
        "--layer",
        "-l",
        help="Run all pipelines for a layer (e.g., bronze, silver, gold)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available pipelines",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Verify setup without running pipelines",
    )

    args = parser.parse_args()

    # Load pipeline registry (scoped to requested domain when possible)
    only_domain: Optional[str] = None
    if args.pipeline and "." in args.pipeline:
        only_domain = args.pipeline.split(".", 1)[0]
    elif args.domain:
        only_domain = args.domain

    load_pipelines(only_domain=only_domain, only_pipeline=args.pipeline)

    if args.list:
        list_pipelines()
    elif args.pipeline:
        run_pipeline(args.pipeline, dry_run=args.dry_run)
    elif args.domain or args.layer:
        run_pipelines_by_filter(
            domain=args.domain,
            layer=args.layer,
            dry_run=args.dry_run,
        )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
