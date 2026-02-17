from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from tests.helpers.connectors import normalize_dataset_name, split_dataset_name


@dataclass(frozen=True)
class DatasetConfig:
    dataset: str
    owner: str
    description: str
    domain: str
    layer: str
    classification: str
    sensitivity: str
    product_tag: str
    pii_columns: tuple[str, ...]
    pii_classifications: dict[str, str]
    retention_days: int
    timestamp_column: str | None
    primary_key: tuple[str, ...]
    upstreams: tuple[str, ...]
    tests: dict[str, Any]
    governance: dict[str, Any]

    @property
    def normalized_name(self) -> str:
        return normalize_dataset_name(self.dataset)

    @property
    def schema_name(self) -> str:
        return split_dataset_name(self.dataset)[0]

    @property
    def table_name(self) -> str:
        return split_dataset_name(self.dataset)[1]


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Dataset config must be a mapping: {path}")
    return payload


def _ensure_str(payload: dict[str, Any], key: str, path: Path) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing required string `{key}` in {path}")
    return value.strip()


def _ensure_int(payload: dict[str, Any], key: str, path: Path) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"Missing/invalid required positive integer `{key}` in {path}")
    return value


def _ensure_list_of_str(payload: dict[str, Any], key: str, path: Path) -> tuple[str, ...]:
    value = payload.get(key, [])
    if not isinstance(value, list):
        raise ValueError(f"`{key}` must be a list in {path}")

    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"`{key}` must only contain non-empty strings in {path}")
        out.append(item.strip())
    return tuple(out)


def load_dataset_configs(repo_root: Path) -> dict[str, DatasetConfig]:
    config_dir = repo_root / "tests" / "configs" / "datasets"
    if not config_dir.exists():
        raise FileNotFoundError(f"Dataset config directory not found: {config_dir}")

    configs: dict[str, DatasetConfig] = {}

    for path in sorted(config_dir.glob("*.yml")):
        payload = _read_yaml(path)

        dataset = _ensure_str(payload, "dataset", path)
        _ = split_dataset_name(dataset)

        owner = _ensure_str(payload, "owner", path)
        description = _ensure_str(payload, "description", path)
        domain = _ensure_str(payload, "domain", path)
        layer = _ensure_str(payload, "layer", path)
        classification = _ensure_str(payload, "classification", path)
        sensitivity = _ensure_str(payload, "sensitivity", path)
        product_tag = _ensure_str(payload, "product_tag", path)
        retention_days = _ensure_int(payload, "retention_days", path)

        pii_columns = _ensure_list_of_str(payload, "pii_columns", path)

        raw_pii_classifications = payload.get("pii_classifications", {})
        if raw_pii_classifications is None:
            raw_pii_classifications = {}
        if not isinstance(raw_pii_classifications, dict):
            raise ValueError(f"`pii_classifications` must be a mapping in {path}")
        pii_classifications = {
            str(k): str(v)
            for k, v in raw_pii_classifications.items()
            if isinstance(k, str) and isinstance(v, str)
        }

        timestamp_column = payload.get("timestamp_column")
        if timestamp_column is not None and not isinstance(timestamp_column, str):
            raise ValueError(f"`timestamp_column` must be a string if provided: {path}")

        primary_key = _ensure_list_of_str(payload, "primary_key", path)
        upstreams = _ensure_list_of_str(payload, "upstreams", path)

        tests = payload.get("tests", {})
        if not isinstance(tests, dict):
            raise ValueError(f"`tests` must be a mapping in {path}")

        governance = payload.get("governance", {})
        if not isinstance(governance, dict):
            raise ValueError(f"`governance` must be a mapping in {path}")

        cfg = DatasetConfig(
            dataset=dataset,
            owner=owner,
            description=description,
            domain=domain,
            layer=layer,
            classification=classification,
            sensitivity=sensitivity,
            product_tag=product_tag,
            pii_columns=pii_columns,
            pii_classifications=pii_classifications,
            retention_days=retention_days,
            timestamp_column=timestamp_column,
            primary_key=primary_key,
            upstreams=upstreams,
            tests=tests,
            governance=governance,
        )

        key = cfg.normalized_name
        if key in configs:
            raise ValueError(f"Duplicate dataset definition `{dataset}` in {path}")
        configs[key] = cfg

    if not configs:
        raise ValueError(f"No dataset config files found in {config_dir}")

    return configs


def get_dataset(configs: dict[str, DatasetConfig], dataset_name: str) -> DatasetConfig:
    key = normalize_dataset_name(dataset_name)
    if key not in configs:
        known = ", ".join(sorted(configs.keys()))
        raise KeyError(f"Dataset `{dataset_name}` not configured. Known datasets: {known}")
    return configs[key]
