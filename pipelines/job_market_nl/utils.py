"""
Utilities for Netherlands job market ingestion and enrichment.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
import re


@dataclass(frozen=True)
class SalaryRange:
    min_eur_year: Optional[float]
    max_eur_year: Optional[float]


DEFAULT_SKILL_MAP: Dict[str, List[str]] = {
    "python": [r"\bpython\b"],
    "java": [r"\bjava\b"],
    "javascript": [r"\bjavascript\b", r"\bnode\.js\b", r"\bnodejs\b"],
    "typescript": [r"\btypescript\b"],
    "sql": [r"\bsql\b"],
    "aws": [r"\baws\b", r"amazon web services"],
    "azure": [r"\bazure\b"],
    "gcp": [r"\bgcp\b", r"google cloud"],
    "kubernetes": [r"\bkubernetes\b", r"\bk8s\b"],
    "docker": [r"\bdocker\b"],
    "devops": [r"\bdevops\b"],
    "data engineering": [r"data engineer", r"data engineering"],
    "data science": [r"data scientist", r"data science"],
    "machine learning": [r"machine learning", r"\bml\b"],
    "security": [r"\bsecurity\b", r"\bcyber\b"],
}


def extract_skills_from_text(text: Optional[str], skill_map: Optional[Dict[str, Iterable[str]]] = None) -> List[str]:
    if not text:
        return []

    skill_map = skill_map or DEFAULT_SKILL_MAP
    text_lower = text.lower()
    matches = []
    for skill, patterns in skill_map.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                matches.append(skill)
                break
    return sorted(set(matches))


def normalize_salary_to_year(
    min_salary: Optional[float],
    max_salary: Optional[float],
    interval: Optional[str],
) -> SalaryRange:
    if min_salary is None and max_salary is None:
        return SalaryRange(None, None)

    interval = (interval or "").lower().strip()

    multiplier = 1.0
    if interval in {"hour", "hourly", "per hour"}:
        multiplier = 40 * 52
    elif interval in {"day", "daily", "per day"}:
        multiplier = 5 * 52
    elif interval in {"week", "weekly", "per week"}:
        multiplier = 52
    elif interval in {"month", "monthly", "per month"}:
        multiplier = 12
    elif interval in {"year", "yearly", "per year", "annum"}:
        multiplier = 1.0

    min_year = min_salary * multiplier if min_salary is not None else None
    max_year = max_salary * multiplier if max_salary is not None else None

    return SalaryRange(min_year, max_year)


def coalesce_str(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value:
            return value
    return None


def coalesce_float(*values: Optional[float]) -> Optional[float]:
    for value in values:
        if value is not None:
            return value
    return None


def normalize_region(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def parse_period_label(period: Optional[str]) -> Optional[str]:
    if not period:
        return None
    return period.replace(" ", "")
