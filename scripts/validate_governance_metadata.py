#!/usr/bin/env python3
"""
Validate governance metadata definitions.

Checks:
- each domain DBML file has a matching `<domain>_product.yaml`
- required governance fields are present:
  - owner
  - classification
  - SLA fields
- metrics in schema/metrics.yaml have required ownership metadata
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Sequence

import yaml


ALLOWED_CLASSIFICATIONS = {"public", "internal", "confidential", "restricted"}
ALLOWED_CRITICALITY = {"low", "medium", "high"}
ALLOWED_CERTIFICATION = {"DRAFT", "CERTIFIED"}
ALLOWED_STATUS = {"ACTIVE", "DEPRECATED", "INACTIVE"}


@dataclass
class ValidationIssue:
    path: Path
    message: str

    def format(self, repo_root: Path) -> str:
        try:
            rel = self.path.relative_to(repo_root)
        except ValueError:
            rel = self.path
        return f"{rel}: {self.message}"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return True
        if stripped.startswith("<") and stripped.endswith(">"):
            return True
    return False


def parse_dataset_name(raw: str) -> tuple[str, str] | None:
    parts = raw.split(".")
    if len(parts) != 2:
        return None
    if not parts[0] or not parts[1]:
        return None
    return parts[0], parts[1]


def ensure_number(value: Any) -> bool:
    return isinstance(value, (int, float))


def validate_product_file(path: Path) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []

    try:
        data = load_yaml(path) or {}
    except Exception as exc:
        return [ValidationIssue(path, f"invalid YAML: {exc}")]

    product = data.get("data_product", {})
    governance = data.get("governance", {})
    classification = governance.get("classification", {})
    service_levels = data.get("service_levels", {})

    owner = product.get("owner")
    if is_missing(owner):
        issues.append(ValidationIssue(path, "missing required field `data_product.owner`"))

    criticality = product.get("criticality")
    if criticality not in ALLOWED_CRITICALITY:
        issues.append(
            ValidationIssue(
                path,
                f"`data_product.criticality` must be one of {sorted(ALLOWED_CRITICALITY)}",
            )
        )

    confidentiality = classification.get("confidentiality")
    if confidentiality not in ALLOWED_CLASSIFICATIONS:
        issues.append(
            ValidationIssue(
                path,
                "missing/invalid `governance.classification.confidentiality` "
                f"(allowed: {sorted(ALLOWED_CLASSIFICATIONS)})",
            )
        )

    freshness = service_levels.get("freshness_sla_hours")
    if not ensure_number(freshness) or freshness <= 0:
        issues.append(
            ValidationIssue(
                path,
                "missing/invalid `service_levels.freshness_sla_hours` "
                "(must be a positive number)",
            )
        )

    availability = service_levels.get("availability_sla_percent")
    if not ensure_number(availability) or availability <= 0 or availability > 100:
        issues.append(
            ValidationIssue(
                path,
                "missing/invalid `service_levels.availability_sla_percent` "
                "(must be > 0 and <= 100)",
            )
        )

    outputs = data.get("interfaces", {}).get("output_datasets", [])
    if not isinstance(outputs, list) or not outputs:
        issues.append(
            ValidationIssue(
                path,
                "missing `interfaces.output_datasets` (must contain at least one dataset)",
            )
        )
    else:
        for dataset in outputs:
            if not isinstance(dataset, str) or not parse_dataset_name(dataset):
                issues.append(
                    ValidationIssue(
                        path,
                        f"invalid dataset name in `interfaces.output_datasets`: `{dataset}` "
                        "(expected `schema.table`)",
                    )
                )

    return issues


def validate_domain_products(schema_root: Path) -> tuple[List[ValidationIssue], dict[str, set[str]]]:
    issues: List[ValidationIssue] = []
    outputs_by_domain: dict[str, set[str]] = {}

    domains_dir = schema_root / "domains"
    dbml_files = sorted(domains_dir.glob("*.dbml"))
    product_files = sorted(
        p for p in domains_dir.glob("*_product.yaml") if p.name != "template_product.yaml"
    )

    expected = {f"{p.stem}_product.yaml" for p in dbml_files}
    actual = {p.name for p in product_files}

    for missing in sorted(expected - actual):
        issues.append(
            ValidationIssue(
                domains_dir / missing,
                "missing product metadata file for domain model",
            )
        )

    for product_file in product_files:
        file_issues = validate_product_file(product_file)
        issues.extend(file_issues)

        # Keep index of output datasets for metrics-to-product validation.
        try:
            payload = load_yaml(product_file) or {}
        except Exception:
            continue
        domain = (payload.get("data_product", {}) or {}).get("domain")
        outputs = (payload.get("interfaces", {}) or {}).get("output_datasets", []) or []
        if isinstance(domain, str) and domain:
            outputs_by_domain.setdefault(domain, set()).update(
                d for d in outputs if isinstance(d, str)
            )

    return issues, outputs_by_domain


def validate_metrics_file(path: Path, outputs_by_domain: dict[str, set[str]]) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []

    if not path.exists():
        return [ValidationIssue(path, "missing metrics registry file")]

    try:
        data = load_yaml(path) or {}
    except Exception as exc:
        return [ValidationIssue(path, f"invalid YAML: {exc}")]

    metrics = data.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        return [ValidationIssue(path, "missing `metrics` entries")]

    for idx, metric in enumerate(metrics, start=1):
        prefix = f"metric[{idx}]"

        metric_id = metric.get("id")
        if is_missing(metric_id):
            issues.append(ValidationIssue(path, f"{prefix}: missing `id`"))

        owner = metric.get("owner")
        if is_missing(owner):
            issues.append(ValidationIssue(path, f"{prefix}: missing `owner`"))

        status = metric.get("status")
        if status not in ALLOWED_STATUS:
            issues.append(
                ValidationIssue(
                    path,
                    f"{prefix}: invalid `status` (allowed: {sorted(ALLOWED_STATUS)})",
                )
            )

        certification = metric.get("certification")
        if certification not in ALLOWED_CERTIFICATION:
            issues.append(
                ValidationIssue(
                    path,
                    f"{prefix}: invalid `certification` "
                    f"(allowed: {sorted(ALLOWED_CERTIFICATION)})",
                )
            )

        source_dataset = ((metric.get("source") or {}).get("dataset"))
        if not isinstance(source_dataset, str):
            issues.append(ValidationIssue(path, f"{prefix}: missing `source.dataset`"))
            continue

        parsed = parse_dataset_name(source_dataset)
        if parsed is None:
            issues.append(
                ValidationIssue(
                    path,
                    f"{prefix}: invalid `source.dataset` value `{source_dataset}` "
                    "(expected `schema.table`)",
                )
            )
            continue

        domain, _ = parsed
        allowed_outputs = outputs_by_domain.get(domain, set())
        if source_dataset not in allowed_outputs:
            issues.append(
                ValidationIssue(
                    path,
                    f"{prefix}: source dataset `{source_dataset}` is not declared in "
                    f"`{domain}_product.yaml` output_datasets",
                )
            )

    return issues


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate governance metadata files")
    parser.add_argument(
        "--schema-root",
        default="schema",
        help="Path to schema directory (default: schema)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    root = repo_root()
    schema_root = (root / args.schema_root).resolve()

    issues: List[ValidationIssue] = []

    product_issues, outputs_by_domain = validate_domain_products(schema_root)
    issues.extend(product_issues)

    metrics_path = schema_root / "metrics.yaml"
    issues.extend(validate_metrics_file(metrics_path, outputs_by_domain))

    if issues:
        print(f"Governance metadata validation failed with {len(issues)} issue(s):")
        for issue in issues:
            print(f"  - {issue.format(root)}")
        return 1

    domain_count = len(outputs_by_domain)
    metric_count = len((load_yaml(metrics_path) or {}).get("metrics", []))
    print(
        "Governance metadata validation passed "
        f"({domain_count} domain product file(s), {metric_count} metric(s))."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
