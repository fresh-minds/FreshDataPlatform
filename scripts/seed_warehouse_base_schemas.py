#!/usr/bin/env python3
"""Seed base warehouse schemas from dbt_parallel outputs for local E2E runs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, List, Tuple

import psycopg2
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class SeedSpec:
    target_schema: str
    source_schema: str
    tables: List[str]


SEED_SPECS = [
    SeedSpec(
        target_schema="finance",
        source_schema="dbt_parallel_finance",
        tables=[
            "fact_grootboekmutaties",
            "fact_crediteuren",
            "fact_debiteuren",
            "dim_grootboek",
            "dim_datum",
            "dim_administraties",
            "dim_kostendrager",
            "dim_kostendragers",
            "dim_kostenplaatsen",
            "dim_relaties",
        ],
    ),
]


def connect():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "open_data_platform_dw"),
        user=os.getenv("WAREHOUSE_USER", "admin"),
        password=os.getenv("WAREHOUSE_PASSWORD", "admin"),
    )


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def ensure_schema(cur, schema: str) -> None:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def seed_table(cur, target_schema: str, source_schema: str, table: str) -> None:
    target = f"{target_schema}.{table}"
    source = f"{source_schema}.{table}"

    cur.execute(f"CREATE TABLE IF NOT EXISTS {target} (LIKE {source} INCLUDING ALL)")
    cur.execute(f"TRUNCATE TABLE {target}")
    cur.execute(f"INSERT INTO {target} SELECT * FROM {source}")


def seed_schema(cur, spec: SeedSpec) -> Tuple[List[str], List[str]]:
    created: List[str] = []
    skipped: List[str] = []

    ensure_schema(cur, spec.target_schema)
    for table in spec.tables:
        if not table_exists(cur, spec.source_schema, table):
            skipped.append(table)
            continue
        seed_table(cur, spec.target_schema, spec.source_schema, table)
        created.append(table)

    return created, skipped


def main() -> int:
    conn = connect()
    conn.autocommit = True
    created_total: List[str] = []
    skipped_total: List[str] = []

    try:
        with conn.cursor() as cur:
            for spec in SEED_SPECS:
                created, skipped = seed_schema(cur, spec)
                created_total.extend([f"{spec.target_schema}.{name}" for name in created])
                skipped_total.extend([f"{spec.source_schema}.{name}" for name in skipped])
    finally:
        conn.close()

    print("Seeded base schemas from dbt_parallel outputs.")
    print(f"Created/updated tables: {len(created_total)}")
    if created_total:
        print("  " + ", ".join(created_total))
    if skipped_total:
        print(f"Skipped missing source tables: {len(skipped_total)}")
        print("  " + ", ".join(skipped_total))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
