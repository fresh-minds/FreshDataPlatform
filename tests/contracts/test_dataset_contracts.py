from __future__ import annotations

import re

import pytest

from tests.helpers.dataset_config import DatasetConfig
from tests.helpers.sql_checks import duplicate_count, missing_columns

SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


def _all_datasets(dataset_configs: dict[str, DatasetConfig]) -> list[DatasetConfig]:
    return [dataset_configs[key] for key in sorted(dataset_configs.keys())]


@pytest.mark.contracts
@pytest.mark.qa_critical
def test_contract_required_columns_exist(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    for dataset in _all_datasets(dataset_configs):
        required_columns = (dataset.tests.get("schema") or {}).get("required_columns", [])
        missing = missing_columns(warehouse, dataset.dataset, required_columns)
        assert not missing, (
            f"Contract break in {dataset.dataset}: required columns missing or renamed: {missing}"
        )


@pytest.mark.contracts
@pytest.mark.qa_critical
def test_contract_primary_keys_present_and_unique(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    for dataset in _all_datasets(dataset_configs):
        assert dataset.primary_key, f"Contract for {dataset.dataset} must declare at least one primary key column"

        missing_pk = missing_columns(warehouse, dataset.dataset, dataset.primary_key)
        assert not missing_pk, (
            f"Contract break in {dataset.dataset}: primary key columns missing: {missing_pk}"
        )

        dupes = duplicate_count(warehouse, dataset.dataset, dataset.primary_key)
        assert dupes == 0, (
            f"Contract break in {dataset.dataset}: primary key {list(dataset.primary_key)} has {dupes} duplicate rows"
        )


@pytest.mark.contracts
def test_contract_naming_conventions(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    for dataset in _all_datasets(dataset_configs):
        assert SNAKE_CASE.match(dataset.schema_name), (
            f"Schema name `{dataset.schema_name}` in {dataset.dataset} must be snake_case"
        )
        assert SNAKE_CASE.match(dataset.table_name), (
            f"Table name `{dataset.table_name}` in {dataset.dataset} must be snake_case"
        )

        for column_name in warehouse.column_names(dataset.dataset):
            assert SNAKE_CASE.match(column_name), (
                f"Column `{column_name}` in {dataset.dataset} violates snake_case contract"
            )
