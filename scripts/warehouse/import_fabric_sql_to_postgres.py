#!/usr/bin/env python3
"""
Import tables from a Microsoft Fabric SQL analytics endpoint into PostgreSQL.

Source: Fabric SQL endpoint (TDS / SQL Server compatible via pyodbc)
Target: PostgreSQL warehouse (psycopg2)

Examples:
    python scripts/warehouse/import_fabric_sql_to_postgres.py \
        --fabric-endpoint f7x5z7ufbtoubd2v4iyuhxftm4-etsqr7ku3xyubmm3cotmbfcvvm.datawarehouse.fabric.microsoft.com \
        --fabric-database MyWarehouse \
        --fabric-user my.user@contoso.com \
        --fabric-password "$FABRIC_SQL_PASSWORD" \
        --all-schemas \
        --target-schema fabric_landing \
        --mode truncate

    python scripts/warehouse/import_fabric_sql_to_postgres.py \
        --table dbo.job_applications --table dbo.candidates --mode replace
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple

from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

try:
    import pyodbc
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency 'pyodbc'. Install it with:\n"
        "  .venv/bin/pip install pyodbc\n"
        "Also ensure ODBC Driver 18 for SQL Server is installed on your machine."
    ) from exc


load_dotenv()

logger = logging.getLogger("fabric_to_postgres")


@dataclass(frozen=True)
class SourceColumn:
    name: str
    source_type: str
    char_len: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    datetime_precision: int | None
    nullable: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Microsoft Fabric SQL endpoint tables into PostgreSQL",
    )

    parser.add_argument(
        "--fabric-endpoint",
        default=os.getenv("FABRIC_SQL_ENDPOINT"),
        help="Fabric SQL endpoint hostname",
    )
    parser.add_argument(
        "--fabric-port",
        type=int,
        default=int(os.getenv("FABRIC_SQL_PORT", "1433")),
        help="Fabric SQL endpoint port (default: 1433)",
    )
    parser.add_argument(
        "--fabric-database",
        default=os.getenv("FABRIC_SQL_DATABASE"),
        help="Fabric SQL database/warehouse name",
    )
    parser.add_argument(
        "--fabric-user",
        default=os.getenv("FABRIC_SQL_USER"),
        help="Fabric SQL username",
    )
    parser.add_argument(
        "--fabric-password",
        default=os.getenv("FABRIC_SQL_PASSWORD"),
        help="Fabric SQL password or service principal client secret",
    )
    parser.add_argument(
        "--fabric-driver",
        default=os.getenv("FABRIC_SQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        help="ODBC driver name",
    )
    parser.add_argument(
        "--fabric-authentication",
        default=os.getenv("FABRIC_SQL_AUTHENTICATION", "ActiveDirectoryPassword"),
        help=(
            "ODBC Authentication value "
            "(e.g. ActiveDirectoryPassword, ActiveDirectoryServicePrincipal, SqlPassword)"
        ),
    )
    parser.add_argument(
        "--fabric-tenant-id",
        default=os.getenv("FABRIC_SQL_TENANT_ID", ""),
        help="Optional Microsoft Entra tenant ID (recommended for service principal auth)",
    )

    parser.add_argument(
        "--warehouse-host",
        default=os.getenv("WAREHOUSE_HOST", "localhost"),
        help="PostgreSQL host",
    )
    parser.add_argument(
        "--warehouse-port",
        type=int,
        default=int(os.getenv("WAREHOUSE_PORT", "5433")),
        help="PostgreSQL port",
    )
    parser.add_argument(
        "--warehouse-db",
        default=os.getenv("WAREHOUSE_DB", "open_data_platform_dw"),
        help="PostgreSQL database name",
    )
    parser.add_argument(
        "--warehouse-user",
        default=os.getenv("WAREHOUSE_USER", "admin"),
        help="PostgreSQL username",
    )
    parser.add_argument(
        "--warehouse-password",
        default=os.getenv("WAREHOUSE_PASSWORD", "admin"),
        help="PostgreSQL password",
    )

    parser.add_argument(
        "--target-schema",
        default=os.getenv("FABRIC_TARGET_SCHEMA", "fabric_landing"),
        help="Target PostgreSQL schema",
    )
    parser.add_argument(
        "--table",
        action="append",
        default=[],
        help="Specific source table(s) as schema.table (repeatable)",
    )
    parser.add_argument(
        "--include-schema",
        action="append",
        default=[],
        help="Source schema(s) to include when auto-discovering tables (repeatable)",
    )
    parser.add_argument(
        "--all-schemas",
        action="store_true",
        help="Discover tables in all schemas (except system schemas)",
    )
    parser.add_argument(
        "--mode",
        choices=["append", "truncate", "replace"],
        default="truncate",
        help="append: add rows; truncate: clear table first; replace: drop/recreate",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("FABRIC_IMPORT_BATCH_SIZE", "5000")),
        help="Number of source rows per fetch/insert batch",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    args = parser.parse_args()

    missing = []
    for required in ["fabric_endpoint", "fabric_database"]:
        if not getattr(args, required):
            missing.append(required.replace("_", "-"))

    auth_mode = (args.fabric_authentication or "").strip().lower()
    creds_required = {
        "activedirectorypassword",
        "activedirectoryserviceprincipal",
        "sqlpassword",
    }
    if auth_mode in creds_required:
        for required in ["fabric_user", "fabric_password"]:
            if not getattr(args, required):
                missing.append(required.replace("_", "-"))

    if missing:
        raise SystemExit(f"Missing required arguments/env: {', '.join(missing)}")

    if args.batch_size < 1:
        raise SystemExit("--batch-size must be >= 1")

    return args


def setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s %(levelname)s %(message)s")


def sqlserver_ident(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def source_table_expr(schema_name: str, table_name: str) -> str:
    return f"{sqlserver_ident(schema_name)}.{sqlserver_ident(table_name)}"


def parse_table_ref(value: str) -> Tuple[str, str]:
    parts = value.split(".", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid --table value '{value}'. Expected schema.table")
    return parts[0], parts[1]


def connect_fabric(args: argparse.Namespace) -> "pyodbc.Connection":
    connection_parts = [
        f"Driver={{{args.fabric_driver}}};"
        f"Server=tcp:{args.fabric_endpoint},{args.fabric_port};"
        f"Database={args.fabric_database};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
        f"Authentication={args.fabric_authentication};"
    ]

    if args.fabric_user:
        connection_parts.append(f"Uid={args.fabric_user};")
    if args.fabric_password:
        connection_parts.append(f"Pwd={args.fabric_password};")
    if args.fabric_tenant_id:
        connection_parts.append(f"Authority Id={args.fabric_tenant_id};")

    conn_str = "".join(connection_parts)
    logger.info(
        "Connecting to Fabric endpoint %s using auth mode %s",
        args.fabric_endpoint,
        args.fabric_authentication,
    )
    return pyodbc.connect(conn_str)


def connect_postgres(args: argparse.Namespace):
    logger.info("Connecting to PostgreSQL %s:%s/%s", args.warehouse_host, args.warehouse_port, args.warehouse_db)
    return psycopg2.connect(
        host=args.warehouse_host,
        port=args.warehouse_port,
        dbname=args.warehouse_db,
        user=args.warehouse_user,
        password=args.warehouse_password,
    )


def discover_tables(src_conn: "pyodbc.Connection", args: argparse.Namespace) -> List[Tuple[str, str]]:
    if args.table:
        return [parse_table_ref(item) for item in args.table]

    include_schemas = args.include_schema or (["dbo"] if not args.all_schemas else [])

    query = [
        "SELECT TABLE_SCHEMA, TABLE_NAME",
        "FROM INFORMATION_SCHEMA.TABLES",
        "WHERE TABLE_TYPE = 'BASE TABLE'",
        "  AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys')",
    ]

    params: List[object] = []
    if include_schemas:
        placeholders = ", ".join(["?"] * len(include_schemas))
        query.append(f"  AND TABLE_SCHEMA IN ({placeholders})")
        params.extend(include_schemas)

    query.append("ORDER BY TABLE_SCHEMA, TABLE_NAME")

    with src_conn.cursor() as cur:
        cur.execute("\n".join(query), params)
        rows = cur.fetchall()

    tables = [(str(row[0]), str(row[1])) for row in rows]
    if not tables:
        raise RuntimeError("No source tables found with current selection")

    logger.info("Discovered %d source table(s)", len(tables))
    return tables


def get_source_columns(src_conn: "pyodbc.Connection", schema_name: str, table_name: str) -> List[SourceColumn]:
    query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            DATETIME_PRECISION,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    with src_conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        rows = cur.fetchall()

    if not rows:
        raise RuntimeError(f"No columns returned for source table {schema_name}.{table_name}")

    return [
        SourceColumn(
            name=str(row[0]),
            source_type=str(row[1]).lower(),
            char_len=None if row[2] is None else int(row[2]),
            numeric_precision=None if row[3] is None else int(row[3]),
            numeric_scale=None if row[4] is None else int(row[4]),
            datetime_precision=None if row[5] is None else int(row[5]),
            nullable=str(row[6]).upper() == "YES",
        )
        for row in rows
    ]


def map_sqlserver_to_postgres(col: SourceColumn) -> str:
    data_type = col.source_type

    if data_type in {"bigint", "int", "smallint", "tinyint", "bit", "real", "float", "date", "time"}:
        return {
            "bigint": "bigint",
            "int": "integer",
            "smallint": "smallint",
            "tinyint": "smallint",
            "bit": "boolean",
            "real": "real",
            "float": "double precision",
            "date": "date",
            "time": "time",
        }[data_type]

    if data_type in {"decimal", "numeric", "money", "smallmoney"}:
        precision = col.numeric_precision or 38
        scale = col.numeric_scale or 0
        return f"numeric({precision},{scale})"

    if data_type in {"datetime", "datetime2", "smalldatetime", "datetimeoffset"}:
        return "timestamp"

    if data_type == "uniqueidentifier":
        return "uuid"

    if data_type in {"char", "nchar", "varchar", "nvarchar"}:
        if col.char_len and col.char_len > 0 and col.char_len <= 10485760:
            return f"varchar({col.char_len})"
        return "text"

    if data_type in {"text", "ntext", "xml", "json", "sql_variant", "hierarchyid"}:
        return "text"

    if data_type in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return "bytea"

    return "text"


def ensure_target_schema(pg_conn, schema_name: str) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
    pg_conn.commit()


def table_exists(pg_conn, schema_name: str, table_name: str) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
    """
    with pg_conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        return bool(cur.fetchone()[0])


def create_target_table(pg_conn, schema_name: str, table_name: str, columns: Sequence[SourceColumn]) -> None:
    column_defs = []
    for col in columns:
        type_sql = map_sqlserver_to_postgres(col)
        null_sql = sql.SQL("NULL") if col.nullable else sql.SQL("NOT NULL")
        column_defs.append(
            sql.SQL("{} {} {}").format(sql.Identifier(col.name), sql.SQL(type_sql), null_sql)
        )

    create_stmt = sql.SQL("CREATE TABLE {} ({})").format(
        sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name)),
        sql.SQL(", ").join(column_defs),
    )
    with pg_conn.cursor() as cur:
        cur.execute(create_stmt)
    pg_conn.commit()


def prepare_target_table(
    pg_conn,
    target_schema: str,
    target_table: str,
    columns: Sequence[SourceColumn],
    mode: str,
) -> None:
    exists = table_exists(pg_conn, target_schema, target_table)

    fq_table = sql.SQL("{}.{}").format(sql.Identifier(target_schema), sql.Identifier(target_table))

    with pg_conn.cursor() as cur:
        if mode == "replace":
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(fq_table))
            pg_conn.commit()
            create_target_table(pg_conn, target_schema, target_table, columns)
            return

        if not exists:
            create_target_table(pg_conn, target_schema, target_table, columns)
            return

        if mode == "truncate":
            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(fq_table))

    pg_conn.commit()


def insert_rows(
    pg_conn,
    target_schema: str,
    target_table: str,
    column_names: Sequence[str],
    rows: Sequence[Tuple[object, ...]],
) -> None:
    if not rows:
        return

    insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
        sql.SQL("{}.{}").format(sql.Identifier(target_schema), sql.Identifier(target_table)),
        sql.SQL(", ").join(sql.Identifier(col) for col in column_names),
    )

    with pg_conn.cursor() as cur:
        execute_values(cur, insert_stmt.as_string(pg_conn), rows, page_size=min(1000, len(rows)))


def iter_source_batches(
    src_conn: "pyodbc.Connection",
    schema_name: str,
    table_name: str,
    batch_size: int,
) -> Iterable[List[Tuple[object, ...]]]:
    query = f"SELECT * FROM {source_table_expr(schema_name, table_name)}"
    with src_conn.cursor() as cur:
        cur.execute(query)
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                break
            yield [tuple(row) for row in batch]


def load_table(
    src_conn: "pyodbc.Connection",
    pg_conn,
    source_schema: str,
    source_table: str,
    target_schema: str,
    mode: str,
    batch_size: int,
) -> int:
    columns = get_source_columns(src_conn, source_schema, source_table)
    prepare_target_table(
        pg_conn,
        target_schema=target_schema,
        target_table=source_table,
        columns=columns,
        mode=mode,
    )

    inserted = 0
    column_names = [col.name for col in columns]

    for batch in iter_source_batches(src_conn, source_schema, source_table, batch_size=batch_size):
        insert_rows(
            pg_conn,
            target_schema=target_schema,
            target_table=source_table,
            column_names=column_names,
            rows=batch,
        )
        inserted += len(batch)
        pg_conn.commit()

    logger.info(
        "Loaded %s.%s -> %s.%s (%d rows)",
        source_schema,
        source_table,
        target_schema,
        source_table,
        inserted,
    )
    return inserted


def main() -> int:
    args = parse_args()
    setup_logging(args.log_level)

    src_conn = None
    pg_conn = None

    try:
        src_conn = connect_fabric(args)
        pg_conn = connect_postgres(args)

        ensure_target_schema(pg_conn, args.target_schema)
        tables = discover_tables(src_conn, args)

        total_rows = 0
        for source_schema, source_table in tables:
            total_rows += load_table(
                src_conn=src_conn,
                pg_conn=pg_conn,
                source_schema=source_schema,
                source_table=source_table,
                target_schema=args.target_schema,
                mode=args.mode,
                batch_size=args.batch_size,
            )

        logger.info("Completed load for %d table(s), %d total rows", len(tables), total_rows)
        return 0
    finally:
        if src_conn is not None:
            src_conn.close()
        if pg_conn is not None:
            pg_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
