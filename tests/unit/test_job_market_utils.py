from __future__ import annotations

import pytest

from pipelines.job_market_nl.utils import (
    coalesce_float,
    coalesce_str,
    extract_skills_from_text,
    normalize_region,
    normalize_salary_to_year,
    parse_period_label,
)

# Hours/days/weeks per year used in salary normalisation.
_HOURS_PER_YEAR = 40 * 52
_DAYS_PER_YEAR = 5 * 52
_WEEKS_PER_YEAR = 52
_MONTHS_PER_YEAR = 12


class TestExtractSkillsFromText:
    def test_basic_skills_detected(self):
        text = "Looking for a Python data engineer with AWS and SQL."
        skills = extract_skills_from_text(text)
        assert "python" in skills
        assert "aws" in skills
        assert "sql" in skills

    def test_returns_empty_for_none_input(self):
        assert extract_skills_from_text(None) == []

    def test_returns_empty_for_empty_string(self):
        assert extract_skills_from_text("") == []

    def test_returns_empty_when_no_skills_match(self):
        assert extract_skills_from_text("We sell widgets and sprockets.") == []

    def test_case_insensitive_matching(self):
        skills = extract_skills_from_text("PYTHON and KUBERNETES are required.")
        assert "python" in skills
        assert "kubernetes" in skills

    def test_k8s_alias_matches_kubernetes(self):
        skills = extract_skills_from_text("Experience with k8s is a plus.")
        assert "kubernetes" in skills

    def test_result_is_sorted_and_deduplicated(self):
        # "node.js" and "nodejs" both map to "javascript"
        skills = extract_skills_from_text("We use node.js and nodejs")
        assert skills.count("javascript") == 1
        assert skills == sorted(skills)

    def test_custom_skill_map_overrides_default(self):
        custom_map = {"rust": [r"\brust\b"]}
        skills = extract_skills_from_text("Looking for a Rust developer", skill_map=custom_map)
        assert skills == ["rust"]


class TestNormalizeSalaryToYear:
    @pytest.mark.parametrize(
        "interval,expected_multiplier",
        [
            ("hour", _HOURS_PER_YEAR),
            ("hourly", _HOURS_PER_YEAR),
            ("per hour", _HOURS_PER_YEAR),
            ("day", _DAYS_PER_YEAR),
            ("daily", _DAYS_PER_YEAR),
            ("per day", _DAYS_PER_YEAR),
            ("week", _WEEKS_PER_YEAR),
            ("weekly", _WEEKS_PER_YEAR),
            ("per week", _WEEKS_PER_YEAR),
            ("month", _MONTHS_PER_YEAR),
            ("monthly", _MONTHS_PER_YEAR),
            ("per month", _MONTHS_PER_YEAR),
            ("year", 1),
            ("yearly", 1),
            ("per year", 1),
            ("annum", 1),
        ],
    )
    def test_interval_aliases(self, interval: str, expected_multiplier: int):
        result = normalize_salary_to_year(100, 200, interval)
        assert result.min_eur_year == pytest.approx(100 * expected_multiplier)
        assert result.max_eur_year == pytest.approx(200 * expected_multiplier)

    def test_unknown_interval_treats_as_already_annual(self):
        result = normalize_salary_to_year(50_000, 80_000, "fortnight")
        assert result.min_eur_year == pytest.approx(50_000)
        assert result.max_eur_year == pytest.approx(80_000)

    def test_none_min_and_max_returns_none_range(self):
        result = normalize_salary_to_year(None, None, "hour")
        assert result.min_eur_year is None
        assert result.max_eur_year is None

    def test_only_min_provided(self):
        result = normalize_salary_to_year(50, None, "hour")
        assert result.min_eur_year == pytest.approx(50 * _HOURS_PER_YEAR)
        assert result.max_eur_year is None

    def test_only_max_provided(self):
        result = normalize_salary_to_year(None, 80, "hour")
        assert result.min_eur_year is None
        assert result.max_eur_year == pytest.approx(80 * _HOURS_PER_YEAR)

    def test_none_interval_treats_as_annual(self):
        result = normalize_salary_to_year(60_000, 90_000, None)
        assert result.min_eur_year == pytest.approx(60_000)
        assert result.max_eur_year == pytest.approx(90_000)

    def test_empty_interval_treats_as_annual(self):
        result = normalize_salary_to_year(60_000, 90_000, "")
        assert result.min_eur_year == pytest.approx(60_000)
        assert result.max_eur_year == pytest.approx(90_000)

    def test_hourly_is_case_insensitive(self):
        upper = normalize_salary_to_year(50, 80, "HOUR")
        lower = normalize_salary_to_year(50, 80, "hour")
        assert upper.min_eur_year == lower.min_eur_year
        assert upper.max_eur_year == lower.max_eur_year


class TestCoalesceStr:
    def test_returns_first_truthy(self):
        assert coalesce_str(None, "", "hello", "world") == "hello"

    def test_returns_none_when_all_none_or_empty(self):
        assert coalesce_str(None, "") is None

    def test_single_value(self):
        assert coalesce_str("only") == "only"


class TestCoalesceFloat:
    def test_returns_first_non_none(self):
        assert coalesce_float(None, 0.0, 1.5) == 0.0

    def test_returns_none_when_all_none(self):
        assert coalesce_float(None, None) is None


class TestNormalizeRegion:
    def test_strips_whitespace(self):
        assert normalize_region("  Amsterdam  ") == "Amsterdam"

    def test_collapses_internal_spaces(self):
        assert normalize_region("North  Holland") == "North Holland"

    def test_returns_none_for_empty(self):
        assert normalize_region("") is None

    def test_returns_none_for_none(self):
        assert normalize_region(None) is None


class TestParsePeriodLabel:
    def test_removes_spaces(self):
        assert parse_period_label("2024 Q1") == "2024Q1"

    def test_returns_none_for_none(self):
        assert parse_period_label(None) is None

    def test_returns_none_for_empty(self):
        assert parse_period_label("") is None
