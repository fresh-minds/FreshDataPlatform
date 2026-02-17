from __future__ import annotations

import os
from pathlib import Path
from typing import Generator

import pytest

from tests.helpers.connectors import MetadataStoreConnector, SQLWarehouseConnector
from tests.helpers.dataset_config import DatasetConfig, load_dataset_configs
from tests.helpers.policy_engine import GovernancePolicyEngine, load_policies
from tests.helpers.qa_config import QAEnvironment, load_environment_config
from tests.helpers.reporting_plugin import QAReportingPlugin

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    markers = [
        "data_correctness: Data reconciliation and rule correctness tests.",
        "data_quality: Baseline data quality tests.",
        "contracts: Schema/contract stability tests.",
        "governance: Metadata, lineage, RBAC, retention, and privacy tests.",
        "pipeline_e2e: End-to-end pipeline execution tests.",
        "qa_critical: Must-pass QA checks for platform readiness.",
        "qa_mutation: Tests that intentionally mutate test data and then clean up.",
    ]
    for marker in markers:
        config.addinivalue_line("markers", marker)

    if os.getenv("QA_ENABLE_REPORTING", "false").strip().lower() in {"1", "true", "yes", "on"}:
        artifact_dir = Path(
            os.getenv(
                "QA_ARTIFACT_DIR",
                str(REPO_ROOT / "tests" / "e2e" / "evidence" / "latest" / "results"),
            )
        ).resolve()
        plugin = QAReportingPlugin(artifact_dir=artifact_dir)
        config.pluginmanager.register(plugin, "qa-reporting-plugin")


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def qa_environment(repo_root: Path) -> QAEnvironment:
    return load_environment_config(repo_root)


@pytest.fixture(scope="session")
def warehouse(qa_environment: QAEnvironment) -> Generator[SQLWarehouseConnector, None, None]:
    connector = SQLWarehouseConnector(qa_environment.warehouse)
    try:
        connector.ping()
    except Exception as exc:  # noqa: BLE001
        if qa_environment.require_services:
            pytest.fail(
                "Warehouse connection is required for QA tests but failed: "
                f"{exc}"
            )
        pytest.skip(f"Warehouse unavailable, skipping QA dataset tests: {exc}")
    yield connector


@pytest.fixture(scope="session")
def metadata_store(repo_root: Path, qa_environment: QAEnvironment) -> MetadataStoreConnector:
    return MetadataStoreConnector(repo_root=repo_root, cfg=qa_environment.metadata)


@pytest.fixture(scope="session")
def dataset_configs(repo_root: Path) -> dict[str, DatasetConfig]:
    return load_dataset_configs(repo_root)


@pytest.fixture(scope="session")
def governance_policies(repo_root: Path) -> list[dict]:
    return load_policies(repo_root)


@pytest.fixture(scope="session")
def policy_engine(
    governance_policies: list[dict],
    metadata_store: MetadataStoreConnector,
) -> GovernancePolicyEngine:
    return GovernancePolicyEngine(
        policies=governance_policies,
        has_lineage_fn=metadata_store.has_lineage_for_dataset,
    )
