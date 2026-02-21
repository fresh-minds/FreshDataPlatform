"""Postgres warehouse utilities: DDL management, upserts, and ingestion state.

Generic functions (``ensure_source_ddl``, ``upsert_records``) accept a
``SourceTableConfig`` so any source can reuse this module.  Backward-compatible
wrappers (``ensure_ddl``, ``upsert_vacatures``) are kept for the existing
Source SP1 DAG.

The ingestion state table lives in schema ``staging``.

Credentials resolved in priority order:
  1. Airflow connection object passed as ``conn``
  2. Environment variables: WAREHOUSE_HOST, WAREHOUSE_PORT, WAREHOUSE_DB,
     WAREHOUSE_USER, WAREHOUSE_PASSWORD

Credentials without a matching env var default to ``""`` (empty string) so
that psycopg2 raises a clear authentication error rather than silently
connecting to a misconfigured stack with insecure defaults.
"""
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras

from src.ingestion.common.source_config import SourceTableConfig

log = logging.getLogger(__name__)

# -- Staging schema constants (shared across all sources) --------------------
_STAGING_SCHEMA = "staging"
_STATE_TABLE = "ingestion_state"


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _build_dsn(conn=None) -> str:
    if conn is not None:
        host = conn.host or os.environ.get("WAREHOUSE_HOST", "warehouse")
        port = conn.port or int(os.environ.get("WAREHOUSE_PORT", "5432"))
        dbname = conn.schema or os.environ.get("WAREHOUSE_DB", "open_data_platform_dw")
        user = conn.login or os.environ.get("WAREHOUSE_USER", "")
        password = conn.password or os.environ.get("WAREHOUSE_PASSWORD", "")
    else:
        host = os.environ.get("WAREHOUSE_HOST", "warehouse")
        port = int(os.environ.get("WAREHOUSE_PORT", "5432"))
        dbname = os.environ.get("WAREHOUSE_DB", "open_data_platform_dw")
        user = os.environ.get("WAREHOUSE_USER", "")
        password = os.environ.get("WAREHOUSE_PASSWORD", "")

    if not user:
        raise EnvironmentError(
            "Postgres user not set. Configure AIRFLOW_CONN_POSTGRES_WAREHOUSE "
            "or WAREHOUSE_USER env var."
        )
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


@contextmanager
def get_connection(conn=None):
    """Context manager yielding a psycopg2 connection; commits on exit."""
    dsn = _build_dsn(conn)
    pg = psycopg2.connect(dsn)
    try:
        yield pg
        pg.commit()
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()


# ---------------------------------------------------------------------------
# Generic DDL — works with any SourceTableConfig
# ---------------------------------------------------------------------------

def _generate_create_table_sql(config: SourceTableConfig) -> str:
    """Generate CREATE TABLE IF NOT EXISTS from a SourceTableConfig."""
    lines: list[str] = []
    pk_col: Optional[str] = None

    for col in config.columns:
        parts = [f"    {col.name:<24s}{col.pg_type}"]
        if col.primary_key:
            parts.append("NOT NULL")
            pk_col = col.name
        elif col.not_null:
            parts.append("NOT NULL")
        if col.name == config.ingested_at_column:
            parts.append("DEFAULT now()")
        lines.append(" ".join(parts))

    if pk_col:
        lines.append(f"    CONSTRAINT {config.table}_pkey PRIMARY KEY ({pk_col})")

    col_defs = ",\n".join(lines)
    return f"CREATE TABLE IF NOT EXISTS {config.fqn} (\n{col_defs}\n);"


def _generate_index_sql(config: SourceTableConfig) -> str:
    """Generate CREATE INDEX IF NOT EXISTS statements for indexed columns."""
    stmts: list[str] = []
    for col in config.indexed_columns:
        idx_name = f"{config.table}_{col.name}_idx"
        stmts.append(
            f"CREATE INDEX IF NOT EXISTS {idx_name}\n"
            f"    ON {config.fqn} ({col.name});"
        )
    return "\n".join(stmts)


def ensure_source_ddl(config: SourceTableConfig, conn=None) -> None:
    """Idempotently create schema, table, and indexes for a source.

    This is the generic entry point — any source calls this with its own
    ``SourceTableConfig``.
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {config.schema};")
        cur.execute(_generate_create_table_sql(config))
        idx_sql = _generate_index_sql(config)
        if idx_sql:
            cur.execute(idx_sql)
    log.info("DDL ensured: %s", config.fqn)


def ensure_state_table(conn=None) -> None:
    """Idempotently create the shared ``staging.ingestion_state`` table."""
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {_STAGING_SCHEMA};
    CREATE TABLE IF NOT EXISTS {_STAGING_SCHEMA}.{_STATE_TABLE} (
        source_name         text        NOT NULL,
        dataset             text        NOT NULL,
        last_success_utc    timestamp without time zone,
        cursor_json         jsonb,
        extracted_count     integer,
        upserted_count      integer,
        PRIMARY KEY (source_name, dataset)
    );
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(ddl)
    log.info("DDL ensured: %s.%s", _STAGING_SCHEMA, _STATE_TABLE)


# ---------------------------------------------------------------------------
# Generic upsert — works with any SourceTableConfig
# ---------------------------------------------------------------------------

def _generate_upsert_sql(config: SourceTableConfig) -> str:
    """Build INSERT … ON CONFLICT … DO UPDATE … RETURNING SQL for a config.

    The generated SQL:
    - Inserts all columns except ``ingested_at_column`` (which has DEFAULT now())
    - On conflict on the PK, updates all data columns
    - Skips the update when the checksum has not changed
    - Returns the PK column for accurate row counting
    """
    insert_cols = config.insert_columns
    pk = config.pk_column
    data_cols = config.data_columns
    checksum = config.checksum_column

    col_list = ", ".join(insert_cols)
    set_clause = ",\n    ".join(
        f"{c.name} = EXCLUDED.{c.name}" for c in data_cols
    )
    where_clause = (
        f"{config.fqn}.{checksum} IS DISTINCT FROM EXCLUDED.{checksum}\n"
        f"    OR {config.fqn}.{checksum} IS NULL"
    )

    return (
        f"INSERT INTO {config.fqn} (\n"
        f"    {col_list}\n"
        f") VALUES %s\n"
        f"ON CONFLICT ({pk}) DO UPDATE SET\n"
        f"    {set_clause}\n"
        f"WHERE\n"
        f"    {where_clause}\n"
        f"RETURNING {pk}"
    )


def upsert_records(
    records: list[dict],
    config: SourceTableConfig,
    conn=None,
) -> int:
    """Upsert *records* into the source table described by *config*.

    Uses ``execute_values`` with ``RETURNING pk`` to get an accurate count of
    rows that were actually inserted or updated (rows skipped by the checksum
    WHERE clause are NOT counted).

    Returns the number of rows inserted or updated.
    """
    if not records:
        log.info("upsert_records(%s): no records to write.", config.fqn)
        return 0

    insert_cols = config.insert_columns
    sql = _generate_upsert_sql(config)

    values = [
        tuple(rec.get(col) for col in insert_cols)
        for rec in records
    ]

    with get_connection(conn) as pg:
        cur = pg.cursor()
        returned = psycopg2.extras.execute_values(
            cur, sql, values, page_size=200, fetch=True
        )
        affected = len(returned)

    log.info(
        "Upserted %d / %d records into %s "
        "(unchanged rows skipped by checksum guard).",
        affected, len(records), config.fqn,
    )
    return affected


# ---------------------------------------------------------------------------
# Backward-compatible wrappers — used by the existing SP1 DAG
# ---------------------------------------------------------------------------

def ensure_ddl(conn=None) -> None:
    """Idempotently create SP1 vacatures table + ingestion state table.

    This is the backward-compatible entry point used by the existing DAG.
    New sources should call ``ensure_source_ddl(config)`` +
    ``ensure_state_table()`` directly.
    """
    from src.ingestion.source_sp1.config import SP1_VACATURES_CONFIG

    ensure_source_ddl(SP1_VACATURES_CONFIG, conn=conn)
    ensure_state_table(conn=conn)
    log.info(
        "DDL ensured (compat): %s  |  %s.%s",
        SP1_VACATURES_CONFIG.fqn,
        _STAGING_SCHEMA, _STATE_TABLE,
    )


def upsert_vacatures(records: list[dict], conn=None) -> int:
    """Upsert SP1 vacatures — backward-compatible wrapper.

    New sources should call ``upsert_records(records, config)`` directly.
    """
    from src.ingestion.source_sp1.config import SP1_VACATURES_CONFIG

    return upsert_records(records, SP1_VACATURES_CONFIG, conn=conn)


# ---------------------------------------------------------------------------
# Ingestion state — read path uses autocommit to avoid unnecessary transaction
# ---------------------------------------------------------------------------

def get_ingestion_state(source_name: str, dataset: str, conn=None) -> Optional[dict]:
    """Return the current ingestion state row, or None if it doesn't exist yet."""
    dsn = _build_dsn(conn)
    pg = psycopg2.connect(dsn)
    try:
        pg.autocommit = True
        cur = pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            f"SELECT * FROM {_STAGING_SCHEMA}.{_STATE_TABLE} "
            "WHERE source_name = %s AND dataset = %s",
            (source_name, dataset),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        pg.close()


def update_ingestion_state(
    *,
    source_name: str,
    dataset: str,
    last_success_utc: datetime,
    cursor_json: Optional[dict] = None,
    extracted_count: int = 0,
    upserted_count: int = 0,
    conn=None,
) -> None:
    """Insert or update the ingestion state for a source/dataset pair."""
    sql = f"""
    INSERT INTO {_STAGING_SCHEMA}.{_STATE_TABLE}
        (source_name, dataset, last_success_utc, cursor_json, extracted_count, upserted_count)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (source_name, dataset) DO UPDATE SET
        last_success_utc = EXCLUDED.last_success_utc,
        cursor_json      = EXCLUDED.cursor_json,
        extracted_count  = EXCLUDED.extracted_count,
        upserted_count   = EXCLUDED.upserted_count
    """
    with get_connection(conn) as pg:
        cur = pg.cursor()
        cur.execute(
            sql,
            (
                source_name,
                dataset,
                last_success_utc,
                json.dumps(cursor_json) if cursor_json else None,
                extracted_count,
                upserted_count,
            ),
        )
    log.info("Ingestion state updated for %s / %s.", source_name, dataset)
