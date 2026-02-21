"""Reusable Airflow DAG utilities for ingestion pipelines.

These helpers extract common patterns from existing DAGs so new sources
can build their DAGs with minimal boilerplate.  Every function is
side-effect-free at import time — heavy imports are deferred into the
callables that Airflow invokes at runtime.

Usage example (inside a DAG file)::

    from src.ingestion.common.dag_helpers import (
        make_default_args,
        on_failure_callback,
        try_get_conn,
        mime_to_ext,
    )
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def try_get_conn(conn_id: str):
    """Return an Airflow connection or ``None`` (callers fall back to env vars).

    This is safe to call outside Airflow (e.g. in unit tests) — it returns
    ``None`` when the Airflow runtime is not available.
    """
    try:
        from airflow.hooks.base import BaseHook
        return BaseHook.get_connection(conn_id)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# MIME / extension mapping
# ---------------------------------------------------------------------------

_MIME_MAP = [
    ("json", "json"),
    ("csv", "csv"),
    ("html", "html"),
    ("xlsx", "xlsx"),
    ("vnd.ms-excel", "xls"),
    ("pdf", "pdf"),
    ("xml", "xml"),
    ("zip", "zip"),
]


def mime_to_ext(mime: str) -> str:
    """Map a MIME type (or substring) to a file extension; defaults to ``bin``."""
    for mime_substr, ext in _MIME_MAP:
        if mime_substr in mime:
            return ext
    return "bin"


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------

def on_failure_callback(context: dict) -> None:
    """Generic failure callback that logs actionable troubleshooting guidance.

    Assign this to ``default_args["on_failure_callback"]`` in your DAG.
    """
    task_id = getattr(context.get("task_instance"), "task_id", "unknown")
    exception = context.get("exception", "")
    dag_id = getattr(context.get("dag"), "dag_id", "unknown")
    log.error(
        "[notify] Task '%s' in DAG '%s' failed.\n"
        "Exception: %s\n\n"
        "Generic troubleshooting checklist:\n"
        "  1. Check source-specific credentials (AIRFLOW_CONN_<source> or env vars).\n"
        "  2. Verify source reachability.\n"
        "  3. Check AIRFLOW_CONN_MINIO (endpoint, access key, secret key).\n"
        "  4. Check AIRFLOW_CONN_POSTGRES_WAREHOUSE (host, db, user, password).\n"
        "  5. Review Airflow task logs for HTTP 403/429 errors.\n"
        "  6. If selectors have changed, update extraction code.",
        task_id,
        dag_id,
        exception,
    )


# ---------------------------------------------------------------------------
# Default args builder
# ---------------------------------------------------------------------------

def make_default_args(
    *,
    owner: str = "data-engineering",
    start_date: Optional[datetime] = None,
    retries: int = 2,
    retry_delay_minutes: int = 5,
    email: Optional[list[str]] = None,
    **overrides,
) -> dict:
    """Build standard ``default_args`` for an ingestion DAG.

    All parameters can be overridden via keyword arguments.  The failure
    callback is always ``on_failure_callback`` unless explicitly replaced.
    """
    args = {
        "owner": owner,
        "depends_on_past": False,
        "start_date": start_date or datetime(2024, 1, 1),
        "email_on_failure": True,
        "email": email or ["alerts@example.com"],
        "email_on_retry": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "on_failure_callback": on_failure_callback,
    }
    args.update(overrides)
    return args


# ---------------------------------------------------------------------------
# Preflight task builders
# ---------------------------------------------------------------------------

def make_validate_connections_callable(
    required_conns: list[dict],
):
    """Return a callable that validates a list of Airflow connections / env vars.

    ``required_conns`` is a list of dicts with keys:

    - ``conn_id``: Airflow connection ID to try
    - ``env_fallback``: env var that must be set if the connection is missing
    - ``label``: human-readable label for error messages

    Example::

        make_validate_connections_callable([
            {"conn_id": "minio", "env_fallback": "MINIO_ENDPOINT", "label": "MinIO"},
            {"conn_id": "postgres_warehouse", "env_fallback": "WAREHOUSE_HOST", "label": "Postgres"},
        ])
    """

    def _validate(**kwargs):
        from airflow.hooks.base import BaseHook

        errors: list[str] = []
        for spec in required_conns:
            try:
                BaseHook.get_connection(spec["conn_id"])
            except Exception:
                if not os.environ.get(spec["env_fallback"]):
                    errors.append(
                        f"{spec['label']}: set AIRFLOW_CONN_{spec['conn_id'].upper()} "
                        f"or {spec['env_fallback']} env var"
                    )
        if errors:
            raise EnvironmentError(
                "Preflight failed — missing connections:\n"
                + "\n".join(f"  • {e}" for e in errors)
            )
        log.info("[preflight] All connections validated.")

    return _validate


def make_ensure_ddl_callable(config):
    """Return a callable that runs ``ensure_source_ddl`` + ``ensure_state_table``."""

    def _ensure_ddl(**kwargs):
        from src.ingestion.common.postgres import ensure_source_ddl, ensure_state_table

        conn = try_get_conn("postgres_warehouse")
        ensure_source_ddl(config, conn=conn)
        ensure_state_table(conn=conn)
        log.info("[preflight] DDL ensured for %s", config.fqn)

    return _ensure_ddl


def make_ensure_bucket_callable(bucket_env: str = "MINIO_BUCKET", default: str = "lakehouse"):
    """Return a callable that ensures the MinIO bucket exists."""

    def _ensure_bucket(**kwargs):
        from src.ingestion.common.minio import ensure_bucket

        bucket = os.environ.get(bucket_env, default)
        conn = try_get_conn("minio")
        ensure_bucket(bucket=bucket, conn=conn)
        log.info("[preflight] MinIO bucket '%s' ready.", bucket)

    return _ensure_bucket


# ---------------------------------------------------------------------------
# dbt task builder
# ---------------------------------------------------------------------------

def make_dbt_run_callable(
    select: str,
    source_name: str,
    extract_task_path: str = "extract.extract_to_bronze",
):
    """Return a callable that runs ``dbt deps`` + ``dbt run`` + ``dbt test``.

    Parameters
    ----------
    select : str
        dbt selector expression, e.g. ``"stg_my_source__entity+"``.
    source_name : str
        Used for artifact naming in MinIO.
    extract_task_path : str
        Dotted task path for XCom pull of ``run_id``, ``run_dt``, ``bronze_bucket``.
    """

    def _dbt_run(**kwargs):
        import shutil
        import subprocess

        from src.ingestion.common.minio import write_bronze_artifact
        from src.ingestion.common.provenance import build_meta

        ti = kwargs["ti"]
        run_id = ti.xcom_pull(key="run_id", task_ids=extract_task_path) or "unknown"
        run_dt = ti.xcom_pull(key="run_dt", task_ids=extract_task_path)
        bucket = ti.xcom_pull(key="bronze_bucket", task_ids=extract_task_path) or "lakehouse"
        minio_conn = try_get_conn("minio")

        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        project_root = os.path.join(repo_root, "project")
        if not os.path.isdir(project_root):
            project_root = repo_root
        dbt_project_dir = os.path.join(project_root, "dbt_parallel")
        dbt_profiles_dir = dbt_project_dir

        dbt_bin = shutil.which("dbt")
        if not dbt_bin:
            raise RuntimeError("dbt executable not found on PATH.")

        def _run(*args: str, fatal: bool = True) -> bool:
            cmd = [
                dbt_bin, args[0],
                "--project-dir", dbt_project_dir,
                "--profiles-dir", dbt_profiles_dir,
                *args[1:],
            ]
            log.info("dbt: %s", " ".join(cmd))
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.stdout:
                log.info(result.stdout[-4_000:])
            if result.returncode != 0:
                msg = f"dbt command failed (rc={result.returncode}): {' '.join(args)}"
                if result.stderr:
                    log.error(result.stderr[-2_000:])
                if fatal:
                    raise RuntimeError(msg)
                log.warning("%s (non-fatal)", msg)
                return False
            return True

        _run("deps", fatal=False)
        _run("run", "--select", select)
        _run("test", "--select", select)

        # Archive dbt artifacts
        for fname, aname in [
            (f"{dbt_project_dir}/target/manifest.json", "manifest"),
            (f"{dbt_project_dir}/target/run_results.json", "run_results"),
        ]:
            try:
                with open(fname, "rb") as fh:
                    raw_bytes = fh.read()
                artifact_id = f"dbt_{aname}_{run_id}"
                meta = build_meta(
                    source_name=source_name,
                    dataset="dbt_artifacts",
                    run_id=run_id,
                    artifact_id=artifact_id,
                    url=f"local://{fname}",
                    canonical_url=f"local://{fname}",
                    http_status=200,
                    content_type="application/json",
                    response_ms=0,
                    raw_bytes=raw_bytes,
                    extraction_method="dbt_artifact",
                )
                write_bronze_artifact(
                    bucket=bucket,
                    run_dt=run_dt,
                    raw_bytes=raw_bytes,
                    meta=meta,
                    extension="json",
                    conn=minio_conn,
                )
                log.info("[dbt] Stored artifact: %s", aname)
            except FileNotFoundError:
                log.warning("[dbt] Artifact not found: %s (skipped)", fname)
            except Exception as exc:
                log.warning("[dbt] Could not store artifact %s: %s (non-fatal)", aname, exc)

        log.info("[dbt] ✓ run + test completed for select=%s", select)

    return _dbt_run


# ---------------------------------------------------------------------------
# Observability task builder
# ---------------------------------------------------------------------------

def make_emit_metrics_callable(
    source_name: str,
    extract_task_path: str = "extract.extract_to_bronze",
    parse_task_path: str = "parse_load.parse_and_load",
):
    """Return a callable that logs metrics and pushes to StatsD."""

    def _emit_metrics(**kwargs):
        import socket

        ti = kwargs["ti"]
        extracted = ti.xcom_pull(key="extracted_count", task_ids=extract_task_path) or 0
        parsed = ti.xcom_pull(key="parsed_count", task_ids=parse_task_path) or 0
        upserted = ti.xcom_pull(key="upserted_count", task_ids=parse_task_path) or 0
        run_id = ti.xcom_pull(key="run_id", task_ids=extract_task_path) or "unknown"

        if extracted == 0:
            log.warning(
                "[observability] extracted_count=0 for run_id=%s — "
                "source may be empty or extraction selectors need updating.",
                run_id,
            )

        log.info(
            "[observability] run_id=%s | artifacts=%d | parsed=%d | upserted=%d",
            run_id, extracted, parsed, upserted,
        )

        try:
            host, port = "statsd-exporter", 9125
            prefix = f"airflow.{source_name}"
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                for name, value in [
                    (f"{prefix}.extracted_count", extracted),
                    (f"{prefix}.parsed_count", parsed),
                    (f"{prefix}.upserted_count", upserted),
                ]:
                    sock.sendto(f"{name}:{value}|g".encode(), (host, port))
        except Exception as exc:
            log.debug("StatsD emission skipped: %s", exc)

    return _emit_metrics
