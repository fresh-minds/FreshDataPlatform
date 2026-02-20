"""Unit tests for GovernancePolicyEngine — no file I/O or warehouse required."""
from __future__ import annotations

import pytest

from tests.helpers.dataset_config import DatasetConfig
from tests.helpers.policy_engine import GovernancePolicyEngine


def _dataset(**overrides) -> DatasetConfig:
    """Return a minimal valid DatasetConfig with sensible defaults."""
    defaults = dict(
        dataset="gold.job_market",
        owner="team-data",
        description="Job market gold dataset",
        domain="job_market",
        layer="gold",
        classification="internal",
        sensitivity="internal",
        product_tag="job-market",
        pii_columns=(),
        pii_classifications={},
        retention_days=365,
        timestamp_column=None,
        primary_key=("id",),
        upstreams=("silver.jobs",),
        tests={},
        governance={},
    )
    defaults.update(overrides)
    return DatasetConfig(**defaults)


def _engine(policies, has_lineage=False):
    return GovernancePolicyEngine(
        policies=policies,
        has_lineage_fn=lambda _dataset_name: has_lineage,
    )


class TestWhenClause:
    def test_policy_applies_when_layer_matches(self):
        policies = [{"policy": "gold_owner", "when": {"layer": "gold"}, "require": ["owner_present"]}]
        engine = _engine(policies)
        results = engine.evaluate(_dataset(layer="gold", owner=""))
        assert len(results) == 1
        assert not results[0].passed

    def test_policy_skipped_when_layer_does_not_match(self):
        policies = [{"policy": "gold_owner", "when": {"layer": "gold"}, "require": ["owner_present"]}]
        engine = _engine(policies)
        results = engine.evaluate(_dataset(layer="silver", owner=""))
        assert results == []

    def test_policy_applies_when_pii_columns_present(self):
        policies = [
            {"policy": "pii_check", "when": {"pii_columns_present": True}, "require": ["pii_classified"]}
        ]
        engine = _engine(policies)
        dataset = _dataset(pii_columns=("email",), pii_classifications={})
        results = engine.evaluate(dataset)
        assert len(results) == 1
        assert not results[0].passed

    def test_policy_skipped_when_pii_columns_absent(self):
        policies = [
            {"policy": "pii_check", "when": {"pii_columns_present": True}, "require": ["pii_classified"]}
        ]
        engine = _engine(policies)
        results = engine.evaluate(_dataset(pii_columns=()))
        assert results == []

    def test_empty_when_clause_always_applies(self):
        policies = [{"policy": "always", "when": {}, "require": ["owner_present"]}]
        engine = _engine(policies)
        results = engine.evaluate(_dataset(owner=""))
        assert len(results) == 1
        assert not results[0].passed


class TestRequirements:
    def test_owner_present_passes(self):
        policies = [{"policy": "p", "when": {}, "require": ["owner_present"]}]
        results = _engine(policies).evaluate(_dataset(owner="alice"))
        assert results[0].passed

    def test_owner_present_fails_on_blank(self):
        policies = [{"policy": "p", "when": {}, "require": ["owner_present"]}]
        results = _engine(policies).evaluate(_dataset(owner="   "))
        assert not results[0].passed

    def test_description_present_passes(self):
        policies = [{"policy": "p", "when": {}, "require": ["description_present"]}]
        results = _engine(policies).evaluate(_dataset(description="A dataset"))
        assert results[0].passed

    def test_description_present_fails_on_empty(self):
        policies = [{"policy": "p", "when": {}, "require": ["description_present"]}]
        results = _engine(policies).evaluate(_dataset(description=""))
        assert not results[0].passed

    def test_retention_days_present_passes(self):
        policies = [{"policy": "p", "when": {}, "require": ["retention_days_present"]}]
        results = _engine(policies).evaluate(_dataset(retention_days=90))
        assert results[0].passed

    def test_lineage_present_via_upstreams(self):
        policies = [{"policy": "p", "when": {}, "require": ["lineage_present"]}]
        results = _engine(policies).evaluate(_dataset(upstreams=("silver.jobs",)))
        assert results[0].passed

    def test_lineage_present_via_metadata_registry(self):
        policies = [{"policy": "p", "when": {}, "require": ["lineage_present"]}]
        engine = _engine(policies, has_lineage=True)
        results = engine.evaluate(_dataset(upstreams=()))
        assert results[0].passed

    def test_lineage_fails_when_absent_everywhere(self):
        policies = [{"policy": "p", "when": {}, "require": ["lineage_present"]}]
        engine = _engine(policies, has_lineage=False)
        results = engine.evaluate(_dataset(upstreams=()))
        assert not results[0].passed

    def test_pii_classified_passes_when_all_columns_classified(self):
        policies = [{"policy": "p", "when": {}, "require": ["pii_classified"]}]
        dataset = _dataset(pii_columns=("email",), pii_classifications={"email": "contact"})
        results = _engine(policies).evaluate(dataset)
        assert results[0].passed

    def test_pii_classified_fails_when_column_missing_classification(self):
        policies = [{"policy": "p", "when": {}, "require": ["pii_classified"]}]
        dataset = _dataset(pii_columns=("email", "phone"), pii_classifications={"email": "contact"})
        results = _engine(policies).evaluate(dataset)
        assert not results[0].passed
        assert "phone" in results[0].violations[0].message

    def test_rbac_present_passes_when_roles_configured(self):
        policies = [{"policy": "p", "when": {}, "require": ["rbac_present"]}]
        dataset = _dataset(governance={"require_rbac": True, "allowed_roles_read": ["analyst"]})
        results = _engine(policies).evaluate(dataset)
        assert results[0].passed

    def test_rbac_present_fails_when_require_rbac_true_but_no_roles(self):
        policies = [{"policy": "p", "when": {}, "require": ["rbac_present"]}]
        dataset = _dataset(governance={"require_rbac": True, "allowed_roles_read": []})
        results = _engine(policies).evaluate(dataset)
        assert not results[0].passed

    def test_pii_masking_configured_skipped_when_not_required(self):
        policies = [{"policy": "p", "when": {}, "require": ["pii_masking_configured"]}]
        dataset = _dataset(governance={"pii_masking_required": False})
        results = _engine(policies).evaluate(dataset)
        assert results[0].passed

    def test_pii_masking_configured_fails_when_view_missing(self):
        policies = [{"policy": "p", "when": {}, "require": ["pii_masking_configured"]}]
        dataset = _dataset(governance={"pii_masking_required": True, "masking_view": ""})
        results = _engine(policies).evaluate(dataset)
        assert not results[0].passed


class TestViolationAccumulation:
    def test_multiple_violations_all_reported(self):
        policies = [
            {
                "policy": "completeness",
                "when": {},
                "require": ["owner_present", "description_present", "domain_present"],
            }
        ]
        dataset = _dataset(owner="", description="", domain="")
        results = _engine(policies).evaluate(dataset)
        assert len(results[0].violations) == 3

    def test_no_violations_when_all_pass(self):
        policies = [{"policy": "p", "when": {}, "require": ["owner_present", "description_present"]}]
        dataset = _dataset(owner="alice", description="desc")
        results = _engine(policies).evaluate(dataset)
        assert results[0].passed
        assert results[0].violations == ()


class TestInvalidPolicy:
    def test_unknown_requirement_raises_value_error(self):
        policies = [{"policy": "p", "when": {}, "require": ["nonexistent_requirement"]}]
        with pytest.raises(ValueError, match="unsupported requirement"):
            _engine(policies).evaluate(_dataset())

    def test_invalid_when_clause_type_raises_value_error(self):
        policies = [{"policy": "p", "when": "not-a-dict", "require": ["owner_present"]}]
        with pytest.raises(ValueError, match="invalid `when` clause"):
            _engine(policies).evaluate(_dataset())

    def test_invalid_require_clause_type_raises_value_error(self):
        policies = [{"policy": "p", "when": {}, "require": "not-a-list"}]
        with pytest.raises(ValueError, match="invalid `require` clause"):
            _engine(policies).evaluate(_dataset())
