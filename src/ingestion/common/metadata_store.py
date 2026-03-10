"""Warehouse metadata registry and event logging helpers.

This module provides a normalized metadata schema for pipeline operations,
lineage, governance, and catalog publication events.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import psycopg2.extras

from src.ingestion.common.postgres import get_connection

log = logging.getLogger(__name__)

_METADATA_SCHEMA = "platform_metadata"


def ensure_metadata_tables(conn=None) -> None:
    """Create metadata tables used across ingestion, dbt, and governance flows."""
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {_METADATA_SCHEMA};

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.pipeline_runs (
        run_id               text PRIMARY KEY,
        pipeline_name        text NOT NULL,
        dag_id               text,
        source_name          text,
        dataset              text,
        status               text NOT NULL,
        triggered_by         text,
        code_version         text,
        started_at_utc       timestamptz,
        finished_at_utc      timestamptz,
        duration_ms          bigint,
        metadata_json        jsonb,
        updated_at_utc       timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.pipeline_task_runs (
        id                   bigserial PRIMARY KEY,
        run_id               text NOT NULL,
        pipeline_name        text NOT NULL,
        task_id              text NOT NULL,
        task_group           text,
        try_number           integer,
        status               text NOT NULL,
        started_at_utc       timestamptz,
        finished_at_utc      timestamptz,
        duration_ms          bigint,
        error_message        text,
        metadata_json        jsonb,
        logged_at_utc        timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.artifact_inventory (
        artifact_uid         text PRIMARY KEY,
        run_id               text NOT NULL,
        source_name          text NOT NULL,
        dataset              text NOT NULL,
        artifact_id          text NOT NULL,
        layer                text NOT NULL,
        bucket               text,
        object_key           text NOT NULL,
        meta_key             text,
        run_dt               date,
        fetched_at_utc       timestamptz,
        url                  text,
        canonical_url        text,
        http_status          integer,
        content_type         text,
        response_ms          integer,
        checksum_sha256      text,
        byte_size            bigint,
        extraction_method    text,
        entity_keys_json     jsonb,
        metadata_json        jsonb,
        logged_at_utc        timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.dataset_registry (
        dataset_id           text PRIMARY KEY,
        platform             text NOT NULL DEFAULT 'warehouse',
        layer                text,
        domain               text,
        schema_name          text,
        table_name           text,
        owner                text,
        steward              text,
        classification       text,
        sensitivity          text,
        retention_days       integer,
        sla_minutes          integer,
        status               text NOT NULL DEFAULT 'active',
        metadata_json        jsonb,
        updated_at_utc       timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.dataset_versions (
        id                   bigserial PRIMARY KEY,
        dataset_id           text NOT NULL,
        version_label        text,
        schema_hash          text,
        column_schema_json   jsonb,
        row_count            bigint,
        byte_size            bigint,
        valid_from_utc       timestamptz NOT NULL DEFAULT now(),
        run_id               text,
        metadata_json        jsonb
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.dataset_lineage_edges (
        id                   bigserial PRIMARY KEY,
        run_id               text,
        pipeline_name        text,
        upstream_dataset     text NOT NULL,
        downstream_dataset   text NOT NULL,
        transformation_type  text,
        column_lineage_json  jsonb,
        metadata_json        jsonb,
        discovered_at_utc    timestamptz NOT NULL DEFAULT now(),
        UNIQUE (run_id, upstream_dataset, downstream_dataset)
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.data_quality_results (
        id                   bigserial PRIMARY KEY,
        run_id               text,
        pipeline_name        text,
        dataset_id           text,
        assertion_id         text NOT NULL,
        assertion_type       text,
        severity             text,
        status               text NOT NULL,
        observed_value       text,
        expected_value       text,
        threshold_value      text,
        row_count_checked    bigint,
        details_json         jsonb,
        evaluated_at_utc     timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.policy_evaluation_results (
        id                   bigserial PRIMARY KEY,
        run_id               text,
        policy_name          text NOT NULL,
        dataset_id           text,
        requirement          text,
        status               text NOT NULL,
        severity             text,
        details_json         jsonb,
        evaluated_at_utc     timestamptz NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS {_METADATA_SCHEMA}.catalog_publication_events (
        id                   bigserial PRIMARY KEY,
        run_id               text,
        dataset_id           text,
        target_system        text NOT NULL,
        target_urn           text,
        action               text NOT NULL,
        status               text NOT NULL,
        details_json         jsonb,
        published_at_utc     timestamptz NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_pm_pipeline_runs_pipeline_name
        ON {_METADATA_SCHEMA}.pipeline_runs (pipeline_name);
    CREATE INDEX IF NOT EXISTS idx_pm_pipeline_task_runs_run_id
        ON {_METADATA_SCHEMA}.pipeline_task_runs (run_id);
    CREATE INDEX IF NOT EXISTS idx_pm_artifact_inventory_run_id
        ON {_METADATA_SCHEMA}.artifact_inventory (run_id);
    CREATE INDEX IF NOT EXISTS idx_pm_artifact_inventory_source_dataset
        ON {_METADATA_SCHEMA}.artifact_inventory (source_name, dataset);
    CREATE INDEX IF NOT EXISTS idx_pm_dataset_versions_dataset_id
        ON {_METADATA_SCHEMA}.dataset_versions (dataset_id);
    CREATE INDEX IF NOT EXISTS idx_pm_lineage_downstream
        ON {_METADATA_SCHEMA}.dataset_lineage_edges (downstream_dataset);
    CREATE INDEX IF NOT EXISTS idx_pm_quality_results_run_id
        ON {_METADATA_SCHEMA}.data_quality_results (run_id);
    CREATE INDEX IF NOT EXISTS idx_pm_policy_results_run_id
        ON {_METADATA_SCHEMA}.policy_evaluation_results (run_id);
    CREATE INDEX IF NOT EXISTS idx_pm_catalog_events_run_id
        ON {_METADATA_SCHEMA}.catalog_publication_events (run_id);
    """

    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(ddl)

    log.info("DDL ensured: %s.*", _METADATA_SCHEMA)


def _duration_ms(started_at_utc: Optional[datetime], finished_at_utc: Optional[datetime]) -> Optional[int]:
    if not started_at_utc or not finished_at_utc:
        return None
    return int((finished_at_utc - started_at_utc).total_seconds() * 1000)


def upsert_pipeline_run(
    *,
    run_id: str,
    pipeline_name: str,
    status: str,
    dag_id: Optional[str] = None,
    source_name: Optional[str] = None,
    dataset: Optional[str] = None,
    triggered_by: Optional[str] = None,
    code_version: Optional[str] = None,
    started_at_utc: Optional[datetime] = None,
    finished_at_utc: Optional[datetime] = None,
    metadata: Optional[dict] = None,
    conn=None,
) -> None:
    """Insert or update a pipeline run envelope record."""
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.pipeline_runs (
        run_id, pipeline_name, dag_id, source_name, dataset, status,
        triggered_by, code_version, started_at_utc, finished_at_utc,
        duration_ms, metadata_json
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id) DO UPDATE SET
        pipeline_name   = EXCLUDED.pipeline_name,
        dag_id          = EXCLUDED.dag_id,
        source_name     = EXCLUDED.source_name,
        dataset         = EXCLUDED.dataset,
        status          = EXCLUDED.status,
        triggered_by    = EXCLUDED.triggered_by,
        code_version    = EXCLUDED.code_version,
        started_at_utc  = COALESCE(EXCLUDED.started_at_utc, {_METADATA_SCHEMA}.pipeline_runs.started_at_utc),
        finished_at_utc = EXCLUDED.finished_at_utc,
        duration_ms     = EXCLUDED.duration_ms,
        metadata_json   = COALESCE(EXCLUDED.metadata_json, {_METADATA_SCHEMA}.pipeline_runs.metadata_json),
        updated_at_utc  = now()
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                run_id,
                pipeline_name,
                dag_id,
                source_name,
                dataset,
                status,
                triggered_by,
                code_version,
                started_at_utc,
                finished_at_utc,
                _duration_ms(started_at_utc, finished_at_utc),
                psycopg2.extras.Json(metadata) if metadata else None,
            ),
        )


def insert_pipeline_task_run(
    *,
    run_id: str,
    pipeline_name: str,
    task_id: str,
    status: str,
    task_group: Optional[str] = None,
    try_number: Optional[int] = None,
    started_at_utc: Optional[datetime] = None,
    finished_at_utc: Optional[datetime] = None,
    error_message: Optional[str] = None,
    metadata: Optional[dict] = None,
    conn=None,
) -> None:
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.pipeline_task_runs (
        run_id, pipeline_name, task_id, task_group, try_number, status,
        started_at_utc, finished_at_utc, duration_ms, error_message, metadata_json
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                run_id,
                pipeline_name,
                task_id,
                task_group,
                try_number,
                status,
                started_at_utc,
                finished_at_utc,
                _duration_ms(started_at_utc, finished_at_utc),
                error_message,
                psycopg2.extras.Json(metadata) if metadata else None,
            ),
        )


def insert_artifact_inventory(
    *,
    run_id: str,
    source_name: str,
    dataset: str,
    artifact_id: str,
    layer: str,
    object_key: str,
    bucket: Optional[str] = None,
    meta_key: Optional[str] = None,
    run_dt: Optional[str] = None,
    fetched_at_utc: Optional[str] = None,
    url: Optional[str] = None,
    canonical_url: Optional[str] = None,
    http_status: Optional[int] = None,
    content_type: Optional[str] = None,
    response_ms: Optional[int] = None,
    checksum_sha256: Optional[str] = None,
    byte_size: Optional[int] = None,
    extraction_method: Optional[str] = None,
    entity_keys: Optional[dict] = None,
    metadata: Optional[dict] = None,
    conn=None,
) -> None:
    artifact_uid = f"{run_id}:{artifact_id}:{object_key}"

    parsed_run_dt = None
    if run_dt:
        parsed_run_dt = datetime.strptime(run_dt, "%Y-%m-%d").date()

    fetched_ts = None
    if fetched_at_utc:
        fetched_ts = datetime.fromisoformat(fetched_at_utc.replace("Z", "+00:00"))

    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.artifact_inventory (
        artifact_uid, run_id, source_name, dataset, artifact_id, layer,
        bucket, object_key, meta_key, run_dt,
        fetched_at_utc, url, canonical_url, http_status, content_type, response_ms,
        checksum_sha256, byte_size, extraction_method, entity_keys_json, metadata_json
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (artifact_uid) DO UPDATE SET
        meta_key = EXCLUDED.meta_key,
        metadata_json = COALESCE(EXCLUDED.metadata_json, {_METADATA_SCHEMA}.artifact_inventory.metadata_json),
        logged_at_utc = now()
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                artifact_uid,
                run_id,
                source_name,
                dataset,
                artifact_id,
                layer,
                bucket,
                object_key,
                meta_key,
                parsed_run_dt,
                fetched_ts,
                url,
                canonical_url,
                http_status,
                content_type,
                response_ms,
                checksum_sha256,
                byte_size,
                extraction_method,
                psycopg2.extras.Json(entity_keys) if entity_keys else None,
                psycopg2.extras.Json(metadata) if metadata else None,
            ),
        )


def upsert_dataset_registry(
    *,
    dataset_id: str,
    layer: Optional[str],
    domain: Optional[str],
    schema_name: Optional[str],
    table_name: Optional[str],
    owner: Optional[str] = None,
    steward: Optional[str] = None,
    classification: Optional[str] = None,
    sensitivity: Optional[str] = None,
    retention_days: Optional[int] = None,
    sla_minutes: Optional[int] = None,
    status: str = "active",
    metadata: Optional[dict] = None,
    conn=None,
) -> None:
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.dataset_registry (
        dataset_id, layer, domain, schema_name, table_name,
        owner, steward, classification, sensitivity, retention_days, sla_minutes,
        status, metadata_json
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (dataset_id) DO UPDATE SET
        layer = EXCLUDED.layer,
        domain = EXCLUDED.domain,
        schema_name = EXCLUDED.schema_name,
        table_name = EXCLUDED.table_name,
        owner = COALESCE(EXCLUDED.owner, {_METADATA_SCHEMA}.dataset_registry.owner),
        steward = COALESCE(EXCLUDED.steward, {_METADATA_SCHEMA}.dataset_registry.steward),
        classification = COALESCE(EXCLUDED.classification, {_METADATA_SCHEMA}.dataset_registry.classification),
        sensitivity = COALESCE(EXCLUDED.sensitivity, {_METADATA_SCHEMA}.dataset_registry.sensitivity),
        retention_days = COALESCE(EXCLUDED.retention_days, {_METADATA_SCHEMA}.dataset_registry.retention_days),
        sla_minutes = COALESCE(EXCLUDED.sla_minutes, {_METADATA_SCHEMA}.dataset_registry.sla_minutes),
        status = EXCLUDED.status,
        metadata_json = COALESCE(EXCLUDED.metadata_json, {_METADATA_SCHEMA}.dataset_registry.metadata_json),
        updated_at_utc = now()
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                dataset_id,
                layer,
                domain,
                schema_name,
                table_name,
                owner,
                steward,
                classification,
                sensitivity,
                retention_days,
                sla_minutes,
                status,
                psycopg2.extras.Json(metadata) if metadata else None,
            ),
        )


def insert_dataset_version(
    *,
    dataset_id: str,
    version_label: Optional[str],
    schema_hash: Optional[str],
    column_schema: Optional[list[dict]],
    row_count: Optional[int],
    byte_size: Optional[int],
    run_id: Optional[str],
    metadata: Optional[dict] = None,
    conn=None,
) -> None:
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.dataset_versions (
        dataset_id, version_label, schema_hash, column_schema_json,
        row_count, byte_size, run_id, metadata_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                dataset_id,
                version_label,
                schema_hash,
                psycopg2.extras.Json(column_schema) if column_schema else None,
                row_count,
                byte_size,
                run_id,
                psycopg2.extras.Json(metadata) if metadata else None,
            ),
        )


def insert_lineage_edge(
    *,
    run_id: Optional[str],
    pipeline_name: Optional[str],
    upstream_dataset: str,
    downstream_dataset: str,
    transformation_type: Optional[str] = None,
    column_lineage: Optional[dict] = None,
    metadata: Optional[dict] = None,
    conn=None,
) -> None:
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.dataset_lineage_edges (
        run_id, pipeline_name, upstream_dataset, downstream_dataset,
        transformation_type, column_lineage_json, metadata_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id, upstream_dataset, downstream_dataset) DO UPDATE SET
        pipeline_name = EXCLUDED.pipeline_name,
        transformation_type = EXCLUDED.transformation_type,
        column_lineage_json = COALESCE(EXCLUDED.column_lineage_json, {_METADATA_SCHEMA}.dataset_lineage_edges.column_lineage_json),
        metadata_json = COALESCE(EXCLUDED.metadata_json, {_METADATA_SCHEMA}.dataset_lineage_edges.metadata_json),
        discovered_at_utc = now()
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                run_id,
                pipeline_name,
                upstream_dataset,
                downstream_dataset,
                transformation_type,
                psycopg2.extras.Json(column_lineage) if column_lineage else None,
                psycopg2.extras.Json(metadata) if metadata else None,
            ),
        )


def insert_data_quality_result(
    *,
    run_id: Optional[str],
    pipeline_name: Optional[str],
    dataset_id: Optional[str],
    assertion_id: str,
    status: str,
    assertion_type: Optional[str] = None,
    severity: Optional[str] = None,
    observed_value: Optional[str] = None,
    expected_value: Optional[str] = None,
    threshold_value: Optional[str] = None,
    row_count_checked: Optional[int] = None,
    details: Optional[dict] = None,
    conn=None,
) -> None:
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.data_quality_results (
        run_id, pipeline_name, dataset_id, assertion_id, assertion_type,
        severity, status, observed_value, expected_value, threshold_value,
        row_count_checked, details_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                run_id,
                pipeline_name,
                dataset_id,
                assertion_id,
                assertion_type,
                severity,
                status,
                observed_value,
                expected_value,
                threshold_value,
                row_count_checked,
                psycopg2.extras.Json(details) if details else None,
            ),
        )


def insert_policy_evaluation_result(
    *,
    run_id: Optional[str],
    policy_name: str,
    status: str,
    dataset_id: Optional[str] = None,
    requirement: Optional[str] = None,
    severity: Optional[str] = None,
    details: Optional[dict] = None,
    conn=None,
) -> None:
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.policy_evaluation_results (
        run_id, policy_name, dataset_id, requirement, status, severity, details_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                run_id,
                policy_name,
                dataset_id,
                requirement,
                status,
                severity,
                psycopg2.extras.Json(details) if details else None,
            ),
        )


def insert_catalog_publication_event(
    *,
    run_id: Optional[str],
    target_system: str,
    action: str,
    status: str,
    dataset_id: Optional[str] = None,
    target_urn: Optional[str] = None,
    details: Optional[dict] = None,
    conn=None,
) -> None:
    sql = f"""
    INSERT INTO {_METADATA_SCHEMA}.catalog_publication_events (
        run_id, dataset_id, target_system, target_urn, action, status, details_json
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                run_id,
                dataset_id,
                target_system,
                target_urn,
                action,
                status,
                psycopg2.extras.Json(details) if details else None,
            ),
        )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def json_hash(payload: object) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return str(hash(canonical))
