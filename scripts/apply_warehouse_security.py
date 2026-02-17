#!/usr/bin/env python3
"""Apply warehouse security baseline (roles, grants, RLS, masking views)."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2


def main() -> int:
    sql_path = Path(__file__).resolve().parent / "init_warehouse_security.sql"
    sql = sql_path.read_text(encoding="utf-8")

    conn = psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5433")),
        dbname=os.getenv("WAREHOUSE_DB", "open_data_platform_dw"),
        user=os.getenv("WAREHOUSE_USER", "admin"),
        password=os.getenv("WAREHOUSE_PASSWORD", "admin"),
    )

    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                """
                SELECT rolname
                FROM pg_roles
                WHERE rolname IN ('odp_etl_writer', 'odp_analyst_reader')
                ORDER BY rolname
                """
            )
            roles = [row[0] for row in cur.fetchall()]
        print("Applied warehouse security baseline")
        print(f"Roles present: {roles}")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
