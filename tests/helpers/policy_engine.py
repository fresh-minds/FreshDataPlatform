from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import yaml

from tests.helpers.dataset_config import DatasetConfig


@dataclass(frozen=True)
class PolicyViolation:
    policy: str
    dataset: str
    requirement: str
    message: str


@dataclass(frozen=True)
class PolicyResult:
    policy: str
    dataset: str
    passed: bool
    violations: tuple[PolicyViolation, ...]


RequirementFn = Callable[[DatasetConfig], tuple[bool, str]]


class GovernancePolicyEngine:
    def __init__(
        self,
        policies: list[dict[str, Any]],
        has_lineage_fn: Callable[[str], bool],
    ) -> None:
        self._policies = policies
        self._has_lineage_fn = has_lineage_fn
        self._requirement_checks: dict[str, RequirementFn] = {
            "owner_present": self._owner_present,
            "description_present": self._description_present,
            "domain_present": self._domain_present,
            "product_tag_present": self._product_tag_present,
            "classification_present": self._classification_present,
            "rbac_present": self._rbac_present,
            "allowed_roles_defined": self._allowed_roles_defined,
            "retention_days_present": self._retention_days_present,
            "lineage_present": self._lineage_present,
            "pii_classified": self._pii_classified,
            "pii_masking_configured": self._pii_masking_configured,
        }

    def evaluate(self, dataset: DatasetConfig) -> list[PolicyResult]:
        results: list[PolicyResult] = []

        for policy in self._policies:
            policy_name = str(policy.get("policy", "unnamed_policy"))
            when_clause = policy.get("when", {}) or {}
            if not isinstance(when_clause, dict):
                raise ValueError(f"Policy `{policy_name}` has invalid `when` clause")

            if not self._matches_when(dataset, when_clause):
                continue

            required = policy.get("require", []) or []
            if not isinstance(required, list):
                raise ValueError(f"Policy `{policy_name}` has invalid `require` clause")

            violations: list[PolicyViolation] = []
            for requirement in required:
                requirement_name = str(requirement)
                checker = self._requirement_checks.get(requirement_name)
                if checker is None:
                    raise ValueError(
                        f"Policy `{policy_name}` references unsupported requirement `{requirement_name}`"
                    )

                passed, detail = checker(dataset)
                if not passed:
                    violations.append(
                        PolicyViolation(
                            policy=policy_name,
                            dataset=dataset.dataset,
                            requirement=requirement_name,
                            message=detail,
                        )
                    )

            results.append(
                PolicyResult(
                    policy=policy_name,
                    dataset=dataset.dataset,
                    passed=len(violations) == 0,
                    violations=tuple(violations),
                )
            )

        return results

    def _matches_when(self, dataset: DatasetConfig, when_clause: dict[str, Any]) -> bool:
        for key, expected in when_clause.items():
            actual = self._resolve_condition_value(dataset, str(key))
            if actual != expected:
                return False
        return True

    def _resolve_condition_value(self, dataset: DatasetConfig, key: str) -> Any:
        if key == "pii_columns_present":
            return len(dataset.pii_columns) > 0
        if key.startswith("governance."):
            nested_key = key.split(".", 1)[1]
            return dataset.governance.get(nested_key)

        if hasattr(dataset, key):
            return getattr(dataset, key)

        return dataset.governance.get(key)

    def _owner_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        passed = bool(dataset.owner.strip())
        return passed, "owner is missing" if not passed else "owner is present"

    def _description_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        passed = bool(dataset.description.strip())
        return passed, "description is missing" if not passed else "description is present"

    def _domain_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        passed = bool(dataset.domain.strip())
        return passed, "domain is missing" if not passed else "domain is present"

    def _product_tag_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        passed = bool(dataset.product_tag.strip())
        return passed, "product_tag is missing" if not passed else "product_tag is present"

    def _classification_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        passed = bool(dataset.classification.strip())
        return passed, "classification is missing" if not passed else "classification is present"

    def _rbac_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        require_rbac = bool(dataset.governance.get("require_rbac", False))
        if not require_rbac:
            return True, "rbac not required"
        roles = dataset.governance.get("allowed_roles_read", [])
        passed = isinstance(roles, list) and len(roles) > 0
        return passed, "allowed_roles_read is required when require_rbac=true" if not passed else "rbac roles configured"

    def _allowed_roles_defined(self, dataset: DatasetConfig) -> tuple[bool, str]:
        roles = dataset.governance.get("allowed_roles_read", [])
        passed = isinstance(roles, list) and len([r for r in roles if isinstance(r, str) and r.strip()]) > 0
        return passed, "allowed_roles_read has no usable role entries" if not passed else "allowed roles configured"

    def _retention_days_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        passed = dataset.retention_days > 0
        return passed, "retention_days must be > 0" if not passed else "retention_days configured"

    def _lineage_present(self, dataset: DatasetConfig) -> tuple[bool, str]:
        if len(dataset.upstreams) > 0:
            return True, "lineage configured via dataset upstreams"
        has_registry_lineage = self._has_lineage_fn(dataset.dataset)
        if has_registry_lineage:
            return True, "lineage present in metadata registry"
        return False, "no upstream lineage found in dataset config or metadata registry"

    def _pii_classified(self, dataset: DatasetConfig) -> tuple[bool, str]:
        if len(dataset.pii_columns) == 0:
            return True, "dataset has no declared PII columns"

        missing = [col for col in dataset.pii_columns if col not in dataset.pii_classifications]
        passed = len(missing) == 0
        if not passed:
            return False, f"missing PII classifications for columns: {missing}"
        return True, "all PII columns have classifications"

    def _pii_masking_configured(self, dataset: DatasetConfig) -> tuple[bool, str]:
        required = bool(dataset.governance.get("pii_masking_required", False))
        if not required:
            return True, "pii masking not required by policy"
        masking_view = dataset.governance.get("masking_view")
        passed = isinstance(masking_view, str) and bool(masking_view.strip())
        return passed, "masking_view is required when pii_masking_required=true" if not passed else "masking_view configured"


def load_policies(repo_root: Path) -> list[dict[str, Any]]:
    path = repo_root / "tests" / "configs" / "policies" / "governance_policies.yml"
    if not path.exists():
        raise FileNotFoundError(f"Missing governance policy file: {path}")

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError("Governance policy file must contain a top-level mapping")

    policies = payload.get("policies", [])
    if not isinstance(policies, list):
        raise ValueError("`policies` must be a list")

    normalized: list[dict[str, Any]] = []
    for policy in policies:
        if not isinstance(policy, dict):
            raise ValueError("Each policy entry must be a mapping")
        normalized.append(policy)

    if not normalized:
        raise ValueError("No governance policies configured")

    return normalized
