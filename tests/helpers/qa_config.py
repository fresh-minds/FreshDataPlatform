from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing QA config file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"QA config file must be a mapping: {path}")
    return payload


def _as_mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"Expected mapping for {path}")
    return value


def _resolve_str(
    qa_env_var: str,
    fallback_env_var: str,
    default: str,
) -> str:
    return (
        os.getenv(qa_env_var)
        or os.getenv(fallback_env_var)
        or default
    )


def _resolve_int(
    qa_env_var: str,
    fallback_env_var: str,
    default: int,
) -> int:
    raw = os.getenv(qa_env_var) or os.getenv(fallback_env_var)
    if raw is None:
        return default
    return int(raw)


@dataclass(frozen=True)
class WarehouseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password=***"
        )


@dataclass(frozen=True)
class AirflowConfig:
    base_url: str
    username: str
    password: str
    dag_id: str


@dataclass(frozen=True)
class MetadataConfig:
    datahub_gms_url: str


@dataclass(frozen=True)
class QAEnvironment:
    env_name: str
    warehouse: WarehouseConfig
    airflow: AirflowConfig
    metadata: MetadataConfig
    allow_mutating_checks: bool
    allow_pipeline_trigger: bool
    require_services: bool
    artifacts_dir: Path
    repo_root: Path

    @property
    def is_production(self) -> bool:
        return self.env_name.lower() == "prod"


def load_environment_config(repo_root: Path) -> QAEnvironment:
    config_path = repo_root / "tests" / "configs" / "environments.yml"
    payload = _read_yaml(config_path)

    environments = _as_mapping(payload.get("environments", {}), "environments")
    env_name = os.getenv("QA_ENV", "test").strip().lower()
    env_payload = _as_mapping(environments.get(env_name), f"environments.{env_name}")

    warehouse_payload = _as_mapping(env_payload.get("warehouse", {}), f"environments.{env_name}.warehouse")
    airflow_payload = _as_mapping(env_payload.get("airflow", {}), f"environments.{env_name}.airflow")
    metadata_payload = _as_mapping(env_payload.get("metadata", {}), f"environments.{env_name}.metadata")

    warehouse = WarehouseConfig(
        host=_resolve_str("QA_WAREHOUSE_HOST", "WAREHOUSE_HOST", str(warehouse_payload.get("host", "localhost"))),
        port=_resolve_int("QA_WAREHOUSE_PORT", "WAREHOUSE_PORT", int(warehouse_payload.get("port", 5433))),
        dbname=_resolve_str("QA_WAREHOUSE_DB", "WAREHOUSE_DB", str(warehouse_payload.get("dbname", "open_data_platform_dw"))),
        user=_resolve_str("QA_WAREHOUSE_USER", "WAREHOUSE_USER", str(warehouse_payload.get("user", "admin"))),
        password=_resolve_str("QA_WAREHOUSE_PASSWORD", "WAREHOUSE_PASSWORD", str(warehouse_payload.get("password", "admin"))),
    )

    airflow = AirflowConfig(
        base_url=_resolve_str("QA_AIRFLOW_BASE_URL", "AIRFLOW_BASE_URL", str(airflow_payload.get("base_url", "http://localhost:8080"))).rstrip("/"),
        username=_resolve_str("QA_AIRFLOW_USERNAME", "AIRFLOW_ADMIN_USER", str(airflow_payload.get("username", "admin"))),
        password=_resolve_str("QA_AIRFLOW_PASSWORD", "AIRFLOW_ADMIN_PASSWORD", str(airflow_payload.get("password", "admin"))),
        dag_id=str(airflow_payload.get("dag_id", "job_market_nl_pipeline")),
    )

    metadata = MetadataConfig(
        datahub_gms_url=_resolve_str(
            "QA_DATAHUB_GMS_URL",
            "DATAHUB_GMS_URL",
            str(metadata_payload.get("datahub_gms_url", "http://localhost:8081")),
        ).rstrip("/"),
    )

    allow_mutations_default = bool(env_payload.get("allow_mutating_checks", env_name != "prod"))
    allow_pipeline_trigger_default = bool(env_payload.get("allow_pipeline_trigger", env_name != "prod"))
    require_services_default = bool(env_payload.get("require_services", env_name != "prod"))

    allow_mutating_checks = _bool_env("QA_ALLOW_MUTATIONS", allow_mutations_default)
    allow_pipeline_trigger = _bool_env("QA_ALLOW_PIPELINE_TRIGGER", allow_pipeline_trigger_default)
    require_services = _bool_env("QA_REQUIRE_SERVICES", require_services_default)

    if env_name == "prod" and allow_mutating_checks and not _bool_env("QA_ALLOW_PROD_MUTATIONS", False):
        # Hard safety stop for accidental production mutation checks.
        allow_mutating_checks = False

    artifacts_dir = Path(
        os.getenv(
            "QA_ARTIFACT_DIR",
            str(repo_root / "tests" / "e2e" / "evidence" / "latest" / "results"),
        )
    ).resolve()

    return QAEnvironment(
        env_name=env_name,
        warehouse=warehouse,
        airflow=airflow,
        metadata=metadata,
        allow_mutating_checks=allow_mutating_checks,
        allow_pipeline_trigger=allow_pipeline_trigger,
        require_services=require_services,
        artifacts_dir=artifacts_dir,
        repo_root=repo_root,
    )
