from __future__ import annotations

from typing import Iterable

from tests.helpers.connectors import SQLWarehouseConnector, split_dataset_name


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _qualified_table(dataset: str) -> str:
    schema_name, table_name = split_dataset_name(dataset)
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


def timestamp_expression(column: str, fmt: str) -> str:
    col = _quote_ident(column)
    normalized = fmt.strip().lower()

    if normalized == "timestamp":
        return f"{col}::timestamptz"
    if normalized == "epoch_millis":
        return f"to_timestamp(({col})::numeric / 1000.0)"
    if normalized == "epoch_millis_wrapped":
        return f"to_timestamp((regexp_replace({col}::text, '[^0-9]', '', 'g'))::numeric / 1000.0)"

    raise ValueError(f"Unsupported timestamp format: {fmt}")


def assert_table_exists(warehouse: SQLWarehouseConnector, dataset: str) -> None:
    assert warehouse.table_exists(dataset), f"Expected table/view `{dataset}` to exist"


def fetch_max_timestamp(
    warehouse: SQLWarehouseConnector,
    dataset: str,
    column: str,
    fmt: str,
):
    expr = timestamp_expression(column=column, fmt=fmt)
    query = f"SELECT MAX({expr}) AS max_ts FROM {_qualified_table(dataset)}"
    return warehouse.query_scalar(query)


def fetch_min_timestamp(
    warehouse: SQLWarehouseConnector,
    dataset: str,
    column: str,
    fmt: str,
):
    expr = timestamp_expression(column=column, fmt=fmt)
    query = f"SELECT MIN({expr}) AS min_ts FROM {_qualified_table(dataset)}"
    return warehouse.query_scalar(query)


def missing_columns(
    warehouse: SQLWarehouseConnector,
    dataset: str,
    required_columns: Iterable[str],
) -> list[str]:
    existing = set(warehouse.column_names(dataset))
    return [column for column in required_columns if column not in existing]


def duplicate_count(
    warehouse: SQLWarehouseConnector,
    dataset: str,
    key_columns: Iterable[str],
) -> int:
    cols = [c.strip() for c in key_columns if c and c.strip()]
    if not cols:
        raise ValueError("duplicate_count requires at least one key column")

    select_cols = ", ".join(_quote_ident(col) for col in cols)
    query = f"""
        SELECT COALESCE(SUM(cnt - 1), 0) AS duplicate_rows
        FROM (
            SELECT {select_cols}, COUNT(*) AS cnt
            FROM {_qualified_table(dataset)}
            GROUP BY {select_cols}
            HAVING COUNT(*) > 1
        ) d
    """
    value = warehouse.query_scalar(query)
    return int(value or 0)


def null_count(
    warehouse: SQLWarehouseConnector,
    dataset: str,
    column: str,
) -> int:
    query = (
        f"SELECT COUNT(*) AS null_rows FROM {_qualified_table(dataset)} "
        f"WHERE {_quote_ident(column)} IS NULL"
    )
    value = warehouse.query_scalar(query)
    return int(value or 0)


def invalid_accepted_values_count(
    warehouse: SQLWarehouseConnector,
    dataset: str,
    column: str,
    accepted_values: Iterable[str],
) -> int:
    accepted = [value for value in accepted_values]
    if not accepted:
        return 0

    quoted = ", ".join("'" + str(value).replace("'", "''") + "'" for value in accepted)
    query = f"""
        SELECT COUNT(*) AS invalid_rows
        FROM {_qualified_table(dataset)}
        WHERE {_quote_ident(column)} IS NOT NULL
          AND {_quote_ident(column)}::text NOT IN ({quoted})
    """
    value = warehouse.query_scalar(query)
    return int(value or 0)


def out_of_range_count(
    warehouse: SQLWarehouseConnector,
    dataset: str,
    column: str,
    min_value: float | int | None,
    max_value: float | int | None,
) -> int:
    predicates: list[str] = []
    col = _quote_ident(column)
    if min_value is not None:
        predicates.append(f"({col})::numeric < {float(min_value)}")
    if max_value is not None:
        predicates.append(f"({col})::numeric > {float(max_value)}")
    if not predicates:
        return 0

    query = f"""
        SELECT COUNT(*) AS invalid_rows
        FROM {_qualified_table(dataset)}
        WHERE {' OR '.join(predicates)}
    """
    value = warehouse.query_scalar(query)
    return int(value or 0)
