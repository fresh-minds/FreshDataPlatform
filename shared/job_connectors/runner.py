from __future__ import annotations

import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

from shared.job_connectors.connectors.base import BaseConnector
from shared.job_connectors.env import bool_env, float_env, int_env, parse_csv
from shared.job_connectors.http import (
    FetchError,
    HostNotAllowedError,
    PoliteHttpClient,
    RobotsDisallowedError,
)
from shared.job_connectors.logging_utils import log_event
from shared.job_connectors.raw_store import RawPayloadStore
from shared.job_connectors.sinks import JobSink

LOGGER = logging.getLogger("open_data_platform.job_connectors")


@dataclass
class ConnectorRunStats:
    connector_name: str
    fetched_refs: int = 0
    fetched_details: int = 0
    parsed_ok: int = 0
    normalized_ok: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    duration_s: float = 0.0


@dataclass
class RunSummary:
    run_id: str
    connectors: List[ConnectorRunStats]


def build_http_client_from_env(
    *, transport: Optional[object] = None, sleep: Callable[[float], None] = time.sleep
) -> PoliteHttpClient:
    user_agent = os.getenv(
        "JOB_CONNECTORS_USER_AGENT", "OpenDataPlatformJobAggregator/0.1 (contact: set JOB_CONNECTORS_USER_AGENT)"
    )
    allowlist_hosts = [host.lower() for host in parse_csv(os.getenv("JOB_CONNECTORS_ALLOWLIST_HOSTS"))]

    return PoliteHttpClient(
        user_agent=user_agent,
        timeout_connect_s=float_env("JOB_CONNECTORS_TIMEOUT_CONNECT_S", 10.0),
        timeout_read_s=float_env("JOB_CONNECTORS_TIMEOUT_READ_S", 20.0),
        rate_limit_per_host_s=float_env("JOB_CONNECTORS_RATE_LIMIT_PER_HOST_S", 1.0),
        max_retries=int_env("JOB_CONNECTORS_MAX_RETRIES", 3),
        backoff_base_s=float_env("JOB_CONNECTORS_BACKOFF_BASE_S", 0.5),
        backoff_max_s=float_env("JOB_CONNECTORS_BACKOFF_MAX_S", 30.0),
        robots_strict=bool_env("JOB_CONNECTORS_ROBOTS_STRICT", True),
        require_allowlist=bool_env("JOB_CONNECTORS_REQUIRE_ALLOWLIST", True),
        allowlist_hosts=allowlist_hosts,
        transport=transport,
        sleep=sleep,
    )


def build_raw_store_from_env() -> RawPayloadStore:
    enabled = bool_env("JOB_CONNECTORS_PERSIST_RAW", False)
    base_dir = os.getenv("JOB_CONNECTORS_RAW_DIR", "./data/job_connectors/raw")
    retention_days = int_env("JOB_CONNECTORS_RAW_RETENTION_DAYS", 7)
    return RawPayloadStore(base_dir=base_dir, enabled=enabled, retention_days=retention_days)


def run_connectors(
    connectors: Iterable[BaseConnector],
    *,
    http: PoliteHttpClient,
    sink: JobSink,
    raw_store: Optional[RawPayloadStore] = None,
    run_id: Optional[str] = None,
) -> RunSummary:
    run_id = run_id or str(uuid.uuid4())
    max_consecutive_failures = int_env("JOB_CONNECTORS_MAX_CONSECUTIVE_FAILURES", 10)

    summaries: List[ConnectorRunStats] = []
    raw_store = raw_store or build_raw_store_from_env()

    for connector in connectors:
        stats = ConnectorRunStats(connector_name=connector.name)
        start = time.perf_counter()
        consecutive_failures = 0

        log_event(
            LOGGER,
            logging.INFO,
            "connector_start",
            connector_name=connector.name,
            run_id=run_id,
        )

        try:
            refs = connector.list_jobs(http, run_id=run_id)
        except Exception as e:
            stats.failed += 1
            stats.duration_s = time.perf_counter() - start
            log_event(
                LOGGER,
                logging.ERROR,
                "connector_list_failed",
                connector_name=connector.name,
                run_id=run_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            summaries.append(stats)
            continue

        stats.fetched_refs = len(refs)
        log_event(
            LOGGER,
            logging.INFO,
            "connector_list_ok",
            connector_name=connector.name,
            run_id=run_id,
            fetched_refs=stats.fetched_refs,
        )

        for ref in refs:
            if consecutive_failures >= max_consecutive_failures:
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "connector_circuit_open",
                    connector_name=connector.name,
                    run_id=run_id,
                    consecutive_failures=consecutive_failures,
                )
                break

            try:
                raw = connector.fetch_job_details(http, run_id=run_id, job_ref=ref)
                if raw is None:
                    stats.failed += 1
                    consecutive_failures += 1
                    continue
                stats.fetched_details += 1
                consecutive_failures = 0
            except (HostNotAllowedError, RobotsDisallowedError) as e:
                stats.skipped += 1
                log_event(
                    LOGGER,
                    logging.INFO,
                    "job_fetch_skipped",
                    connector_name=connector.name,
                    run_id=run_id,
                    url=ref.url,
                    error_type=type(e).__name__,
                )
                continue
            except FetchError as e:
                stats.failed += 1
                consecutive_failures += 1
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "job_fetch_failed",
                    connector_name=connector.name,
                    run_id=run_id,
                    url=e.url,
                    status_code=e.status_code,
                    error_type=type(e).__name__,
                    error=str(e),
                )
                continue
            except Exception as e:
                stats.failed += 1
                consecutive_failures += 1
                log_event(
                    LOGGER,
                    logging.WARNING,
                    "job_fetch_failed",
                    connector_name=connector.name,
                    run_id=run_id,
                    url=ref.url,
                    error_type=type(e).__name__,
                    error=str(e),
                )
                continue

            if raw_store is not None:
                try:
                    raw_path = raw_store.save(raw)
                    if raw_path:
                        raw.metadata["raw_path"] = raw_path
                except Exception:
                    # Raw persistence must not break ingestion.
                    pass

            stats.parsed_ok += 1
            job = connector.normalize(raw, run_id=run_id)
            if job is None:
                stats.skipped += 1
                continue
            stats.normalized_ok += 1

            action = sink.upsert(job)
            if action == "inserted":
                stats.inserted += 1
            elif action == "updated":
                stats.updated += 1
            else:
                stats.skipped += 1

        stats.duration_s = time.perf_counter() - start
        summaries.append(stats)

        log_event(
            LOGGER,
            logging.INFO,
            "connector_done",
            connector_name=connector.name,
            run_id=run_id,
            fetched_refs=stats.fetched_refs,
            fetched_details=stats.fetched_details,
            parsed_ok=stats.parsed_ok,
            normalized_ok=stats.normalized_ok,
            inserted=stats.inserted,
            updated=stats.updated,
            skipped=stats.skipped,
            failed=stats.failed,
            duration_s=round(stats.duration_s, 3),
        )

    sink.close()
    return RunSummary(run_id=run_id, connectors=summaries)
