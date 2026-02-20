from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.helpers.dataset_config import DatasetConfig
from tests.helpers.sql_checks import fetch_min_timestamp

RETENTION_GRACE_DAYS = 30


def _all_datasets(dataset_configs: dict[str, DatasetConfig]) -> list[DatasetConfig]:
    return [dataset_configs[key] for key in sorted(dataset_configs.keys())]


@pytest.mark.governance
@pytest.mark.qa_critical
def test_metadata_completeness_and_policy_compliance(
    dataset_configs: dict[str, DatasetConfig],
    policy_engine,
) -> None:
    violations: list[str] = []

    for dataset in _all_datasets(dataset_configs):
        results = policy_engine.evaluate(dataset)
        for result in results:
            if result.passed:
                continue
            for violation in result.violations:
                violations.append(
                    f"{violation.dataset} [{violation.policy}] {violation.requirement}: {violation.message}"
                )

    assert not violations, "Governance policy violations found:\n" + "\n".join(violations)


@pytest.mark.governance
@pytest.mark.qa_critical
def test_gold_datasets_have_lineage(
    dataset_configs: dict[str, DatasetConfig],
    metadata_store,
) -> None:
    for dataset in _all_datasets(dataset_configs):
        if dataset.layer != "gold":
            continue

        has_lineage = len(dataset.upstreams) > 0 or metadata_store.has_lineage_for_dataset(dataset.dataset)
        assert has_lineage, (
            f"Gold dataset {dataset.dataset} must define upstream lineage in dataset config or metadata registry"
        )


@pytest.mark.governance
@pytest.mark.qa_critical
def test_pii_classification_and_export_leak_controls(
    dataset_configs: dict[str, DatasetConfig],
    repo_root: Path,
) -> None:
    for dataset in _all_datasets(dataset_configs):
        if not dataset.pii_columns:
            continue

        missing = [column for column in dataset.pii_columns if column not in dataset.pii_classifications]
        assert not missing, (
            f"Dataset {dataset.dataset} has unclassified PII columns: {missing}"
        )

        for relative_path in dataset.governance.get("non_authorized_export_queries", []):
            query_path = (repo_root / str(relative_path)).resolve()
            assert query_path.exists(), (
                f"Configured export query file for {dataset.dataset} does not exist: {relative_path}"
            )
            sql_text = query_path.read_text(encoding="utf-8").lower()
            for pii_column in dataset.pii_columns:
                assert pii_column.lower() not in sql_text, (
                    f"PII column `{pii_column}` from {dataset.dataset} appears in non-authorized export query {relative_path}"
                )


@pytest.mark.governance
@pytest.mark.qa_critical
def test_restricted_datasets_rbac_controls(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    for dataset in _all_datasets(dataset_configs):
        if dataset.sensitivity != "restricted":
            continue
        if not bool(dataset.governance.get("require_rbac", False)):
            continue

        allowed_roles = dataset.governance.get("allowed_roles_read", []) or []
        assert allowed_roles, (
            f"Restricted dataset {dataset.dataset} requires non-empty allowed_roles_read"
        )

        for role_name in allowed_roles:
            allowed = warehouse.has_select_privilege(str(role_name), dataset.dataset)
            assert allowed, (
                f"RBAC violation: role `{role_name}` is expected to have SELECT on {dataset.dataset}"
            )

        public_allowed = warehouse.has_select_privilege("public", dataset.dataset)
        assert not public_allowed, (
            f"RBAC violation: PUBLIC should not have SELECT on restricted dataset {dataset.dataset}"
        )


@pytest.mark.governance
def test_retention_window_enforced(
    warehouse,
    dataset_configs: dict[str, DatasetConfig],
) -> None:
    now_utc = datetime.now(timezone.utc)

    for dataset in _all_datasets(dataset_configs):
        if not dataset.timestamp_column:
            continue

        freshness = dataset.tests.get("freshness") or {}
        ts_format = str(freshness.get("format", "timestamp"))

        min_ts = fetch_min_timestamp(
            warehouse=warehouse,
            dataset=dataset.dataset,
            column=dataset.timestamp_column,
            fmt=ts_format,
        )
        assert min_ts is not None, (
            f"Could not evaluate retention policy for {dataset.dataset}: no parseable timestamp values"
        )

        if min_ts.tzinfo is None:
            min_ts = min_ts.replace(tzinfo=timezone.utc)

        age_days = (now_utc - min_ts).total_seconds() / 86400.0
        allowed_days = dataset.retention_days + RETENTION_GRACE_DAYS
        assert age_days <= allowed_days, (
            f"Retention violation for {dataset.dataset}: oldest row age={age_days:.1f}d exceeds "
            f"policy={dataset.retention_days}d (+{RETENTION_GRACE_DAYS}d grace)"
        )


@pytest.mark.governance
@pytest.mark.qa_critical
def test_pipeline_run_auditability_recorded(warehouse) -> None:
    table_exists = warehouse.table_exists("platform_audit.pipeline_runs")
    assert table_exists, (
        "Auditability check failed: expected table platform_audit.pipeline_runs to exist. "
        "Run scripts/testing/run_e2e_tests.sh to register pipeline run metadata."
    )

    rows = warehouse.query_rows(
        """
        SELECT
          run_id,
          pipeline_name,
          triggered_by,
          code_version,
          status,
          started_at_utc,
          finished_at_utc
        FROM platform_audit.pipeline_runs
        ORDER BY started_at_utc DESC
        LIMIT 1
        """
    )
    assert rows, "Auditability check failed: platform_audit.pipeline_runs has no rows"

    row = rows[0]
    assert row["run_id"], "Auditability check failed: run_id is empty"
    assert row["pipeline_name"], "Auditability check failed: pipeline_name is empty"
    assert row["triggered_by"], "Auditability check failed: triggered_by is empty"
    assert row["code_version"], "Auditability check failed: code_version is empty"
    assert row["status"] in {"RUNNING", "SUCCESS", "FAILED"}, (
        f"Auditability check failed: unexpected status `{row['status']}`"
    )
    assert row["started_at_utc"] is not None, "Auditability check failed: started_at_utc is null"
    assert row["finished_at_utc"] is not None, "Auditability check failed: finished_at_utc is null"
