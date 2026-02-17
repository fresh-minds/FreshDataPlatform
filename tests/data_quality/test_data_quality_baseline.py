from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.helpers.dataset_config import DatasetConfig
from tests.helpers.sql_checks import (
    assert_table_exists,
    duplicate_count,
    fetch_max_timestamp,
    invalid_accepted_values_count,
    missing_columns,
    null_count,
    out_of_range_count,
)


def _all_datasets(dataset_configs: dict[str, DatasetConfig]) -> list[DatasetConfig]:
    return [dataset_configs[key] for key in sorted(dataset_configs.keys())]


@pytest.mark.data_quality
@pytest.mark.qa_critical
def test_schema_required_columns_and_types(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    for dataset in _all_datasets(dataset_configs):
        assert_table_exists(warehouse, dataset.dataset)

        required_columns = (dataset.tests.get("schema") or {}).get("required_columns", [])
        missing = missing_columns(warehouse, dataset.dataset, required_columns)
        assert not missing, f"{dataset.dataset} is missing required columns: {missing}"

        expected_types = (dataset.tests.get("schema") or {}).get("column_types", {})
        if not isinstance(expected_types, dict):
            continue

        actual_type_map = {
            str(column["column_name"]): str(column["data_type"]).lower()
            for column in warehouse.get_columns(dataset.dataset)
        }
        for column_name, expected_type in expected_types.items():
            assert column_name in actual_type_map, (
                f"{dataset.dataset} expected type for `{column_name}` but the column is missing"
            )
            actual_type = actual_type_map[column_name]
            assert str(expected_type).lower() in actual_type, (
                f"{dataset.dataset}.{column_name} expected type containing `{expected_type}` "
                f"but found `{actual_type}`"
            )


@pytest.mark.data_quality
@pytest.mark.qa_critical
def test_not_null_and_unique_constraints(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    for dataset in _all_datasets(dataset_configs):
        constraints = dataset.tests.get("constraints") or {}

        unique_cols = constraints.get("unique", [])
        for unique_col in unique_cols:
            dupes = duplicate_count(warehouse, dataset.dataset, [str(unique_col)])
            assert dupes == 0, (
                f"{dataset.dataset}.{unique_col} must be unique but found {dupes} duplicate row(s)"
            )

        not_null_cols = constraints.get("not_null", [])
        for column_name in not_null_cols:
            nulls = null_count(warehouse, dataset.dataset, str(column_name))
            assert nulls == 0, (
                f"{dataset.dataset}.{column_name} must be not null but found {nulls} null row(s)"
            )


@pytest.mark.data_quality
def test_accepted_values_and_numeric_ranges(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    for dataset in _all_datasets(dataset_configs):
        constraints = dataset.tests.get("constraints") or {}

        accepted_values = constraints.get("accepted_values", {}) or {}
        for column_name, allowed_values in accepted_values.items():
            invalid_count = invalid_accepted_values_count(
                warehouse=warehouse,
                dataset=dataset.dataset,
                column=str(column_name),
                accepted_values=list(allowed_values),
            )
            assert invalid_count == 0, (
                f"{dataset.dataset}.{column_name} has {invalid_count} value(s) outside accepted set {allowed_values}"
            )

        ranges = constraints.get("ranges", {}) or {}
        for column_name, bounds in ranges.items():
            if not isinstance(bounds, dict):
                continue
            invalid_count = out_of_range_count(
                warehouse=warehouse,
                dataset=dataset.dataset,
                column=str(column_name),
                min_value=bounds.get("min"),
                max_value=bounds.get("max"),
            )
            assert invalid_count == 0, (
                f"{dataset.dataset}.{column_name} has {invalid_count} out-of-range value(s)"
            )


@pytest.mark.data_quality
@pytest.mark.qa_critical
def test_freshness_sla(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    now_utc = datetime.now(timezone.utc)

    for dataset in _all_datasets(dataset_configs):
        freshness = dataset.tests.get("freshness") or {}
        if not freshness:
            continue

        column_name = str(freshness.get("column", dataset.timestamp_column or "")).strip()
        max_age_hours = float(freshness.get("max_age_hours", 24))
        ts_format = str(freshness.get("format", "timestamp"))

        assert column_name, f"{dataset.dataset} freshness check requires a timestamp column"
        max_ts = fetch_max_timestamp(
            warehouse=warehouse,
            dataset=dataset.dataset,
            column=column_name,
            fmt=ts_format,
        )
        assert max_ts is not None, (
            f"{dataset.dataset} freshness check could not derive a max timestamp from `{column_name}`"
        )

        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)

        age_hours = (now_utc - max_ts).total_seconds() / 3600.0
        assert age_hours <= max_age_hours, (
            f"{dataset.dataset} freshness breach: age={age_hours:.2f}h exceeds SLA {max_age_hours:.2f}h"
        )
