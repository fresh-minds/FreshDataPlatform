"""Job market NL pipeline that writes directly to the Postgres warehouse.

This is a Java/Spark-free fallback so the Superset dashboard can be populated
in environments where PySpark is unavailable.

It fetches:
- CBS vacancy rate (table 80567ENG)
- CBS unfilled vacancies (table 80472ENG)

And creates/refreshes:
- job_market_nl.it_market_snapshot
- job_market_nl.it_market_top_skills
- job_market_nl.it_market_region_distribution
- job_market_nl.it_market_job_ads_geo
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import requests

from pipelines.job_market_nl.utils import extract_skills_from_text


CBS_ODATA_BASE = "https://opendata.cbs.nl/ODataApi/OData"
CBS_VACANCY_RATE_TABLE = os.getenv("CBS_VACANCY_RATE_TABLE", "80567ENG")
CBS_VACANCIES_TABLE = os.getenv("CBS_VACANCIES_TABLE", "80472ENG")

# CBS "Information and communication" industry key in both tables
CBS_IT_SECTOR_KEY = os.getenv("CBS_IT_SECTOR_KEY", "391600")

NL_PROVINCE_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "Drenthe": (52.9476, 6.6231),
    "Flevoland": (52.5279, 5.5954),
    "Friesland": (53.1642, 5.7818),
    "Gelderland": (52.0617, 5.9397),
    "Groningen": (53.2194, 6.5665),
    "Limburg": (51.2093, 5.9330),
    "Noord-Brabant": (51.5719, 5.3790),
    "Noord-Holland": (52.5200, 4.7885),
    "Overijssel": (52.4388, 6.5016),
    "Utrecht": (52.0907, 5.1214),
    "Zeeland": (51.4940, 3.8497),
    "Zuid-Holland": (52.0705, 4.3007),
}

CITY_TO_PROVINCE: Dict[str, str] = {
    "almere": "Flevoland",
    "amersfoort": "Utrecht",
    "amsterdam": "Noord-Holland",
    "arnhem": "Gelderland",
    "breda": "Noord-Brabant",
    "den bosch": "Noord-Brabant",
    "den haag": "Zuid-Holland",
    "delft": "Zuid-Holland",
    "dordrecht": "Zuid-Holland",
    "eindhoven": "Noord-Brabant",
    "enschede": "Overijssel",
    "groningen": "Groningen",
    "haarlem": "Noord-Holland",
    "leiden": "Zuid-Holland",
    "leeuwarden": "Friesland",
    "maastricht": "Limburg",
    "nijmegen": "Gelderland",
    "rotterdam": "Zuid-Holland",
    "tilburg": "Noord-Brabant",
    "utrecht": "Utrecht",
    "zwolle": "Overijssel",
}

PROVINCE_ALIASES: Dict[str, str] = {
    "drenthe": "Drenthe",
    "flevoland": "Flevoland",
    "friesland": "Friesland",
    "gelderland": "Gelderland",
    "groningen": "Groningen",
    "limburg": "Limburg",
    "noord brabant": "Noord-Brabant",
    "north brabant": "Noord-Brabant",
    "noord holland": "Noord-Holland",
    "north holland": "Noord-Holland",
    "overijssel": "Overijssel",
    "utrecht": "Utrecht",
    "zeeland": "Zeeland",
    "zuid holland": "Zuid-Holland",
    "south holland": "Zuid-Holland",
}


@dataclass(frozen=True)
class SnapshotRow:
    period_key: str
    period_label: str
    sector_name: str
    vacancies: Optional[float]
    vacancy_rate: Optional[float]
    job_ads_count: int


def _fetch_all_odata(url: str, params: Optional[Dict[str, Any]] = None, timeout_s: int = 30) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    next_url: Optional[str] = url
    next_params = params

    while next_url:
        resp = requests.get(next_url, params=next_params, timeout=timeout_s)
        resp.raise_for_status()
        payload = resp.json()
        rows.extend(payload.get("value", []))
        next_url = payload.get("@odata.nextLink")
        next_params = None

    return rows


def _fetch_single_value(url: str, params: Dict[str, Any], timeout_s: int = 30) -> Optional[Dict[str, Any]]:
    rows = _fetch_all_odata(url, params=params, timeout_s=timeout_s)
    return rows[0] if rows else None


def _odata_eq_any(field: str, values: Iterable[str]) -> str:
    # CBS dimensions frequently include trailing spaces in keys.
    parts = [f"({field} eq '{value}')" for value in values if value is not None]
    return " or ".join(parts)


def _key_variants(key: str) -> List[str]:
    stripped = key.rstrip()
    variants = [key]
    if stripped != key:
        variants.append(stripped)
    if not key.endswith(" "):
        variants.append(f"{key} ")
    if stripped and not stripped.endswith(" "):
        variants.append(f"{stripped} ")

    # Preserve order but dedupe
    seen = set()
    out: List[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _get_period_label(table_id: str, period_key: str) -> str:
    url = f"{CBS_ODATA_BASE}/{table_id}/Periods"
    row = _fetch_single_value(url, {"$filter": f"Key eq '{period_key}'"})
    return (row or {}).get("Title") or period_key


def _get_sector_label_for_rate(table_id: str, sector_key: str) -> str:
    url = f"{CBS_ODATA_BASE}/{table_id}/SIC2008"
    key_filter = _odata_eq_any("Key", _key_variants(sector_key))
    row = _fetch_single_value(url, {"$filter": key_filter}) if key_filter else None
    return (row or {}).get("Title") or sector_key


def _get_sector_label_for_vacancies(table_id: str, sector_key: str) -> str:
    url = f"{CBS_ODATA_BASE}/{table_id}/SIC2008SizeClasses"
    key_filter = _odata_eq_any("Key", _key_variants(sector_key))
    row = _fetch_single_value(url, {"$filter": key_filter}) if key_filter else None
    return (row or {}).get("Title") or sector_key


def _fetch_cbs_vacancy_rate_latest(table_id: str, sector_key: str) -> Tuple[str, Optional[float]]:
    url = f"{CBS_ODATA_BASE}/{table_id}/TypedDataSet"
    sector_filter = _odata_eq_any("SIC2008", _key_variants(sector_key))
    rows = _fetch_all_odata(url, params={"$filter": sector_filter, "$select": "Periods,VacancyRate_1"})
    if not rows:
        raise RuntimeError(f"CBS vacancy rate returned no rows for sector {sector_key}")

    # Period keys are like 2025KW04; lexical max works for chronological order.
    latest = max(rows, key=lambda r: r.get("Periods") or "")
    period_key = latest.get("Periods")
    vacancy_rate = latest.get("VacancyRate_1")
    return period_key, float(vacancy_rate) if vacancy_rate is not None else None


def _fetch_cbs_vacancies_latest(table_id: str, sector_key: str) -> Tuple[str, Optional[float]]:
    url = f"{CBS_ODATA_BASE}/{table_id}/TypedDataSet"
    sector_filter = _odata_eq_any("SIC2008SizeClasses", _key_variants(sector_key))
    rows = _fetch_all_odata(
        url,
        params={
            "$filter": sector_filter,
            "$select": "Periods,UnfilledVacancies_1",
        },
    )
    if not rows:
        raise RuntimeError(f"CBS vacancies returned no rows for sector {sector_key}")

    latest = max(rows, key=lambda r: r.get("Periods") or "")
    period_key = latest.get("Periods")

    unfilled_thousands = latest.get("UnfilledVacancies_1")
    if unfilled_thousands is None:
        return period_key, None

    # Unit is x 1000
    return period_key, float(unfilled_thousands) * 1000.0


def _fetch_job_ads_mock_or_adzuna() -> List[Dict[str, Any]]:
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")

    if not app_id or not app_key:
        return [
            {
                "id": "mock-adzuna-1",
                "title": "Data Engineer",
                "company": "MockCo",
                "location": "Amsterdam",
                "description": "Data engineer working with Python, SQL, and AWS. Kubernetes and Docker are a plus.",
            },
            {
                "id": "mock-adzuna-2",
                "title": "DevOps Engineer",
                "company": "MockCo",
                "location": "Utrecht",
                "description": "DevOps role: Azure, Kubernetes, Docker, CI/CD. Some Python scripting.",
            },
        ]

    country = os.getenv("ADZUNA_COUNTRY", "nl")
    query = os.getenv("ADZUNA_QUERY", "software engineer OR data engineer OR devops")
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": query,
        "results_per_page": 50,
        "content-type": "application/json",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("results", [])


def _normalize_geo_token(value: str) -> str:
    cleaned = (
        value.strip()
        .lower()
        .replace("-", " ")
        .replace("_", " ")
        .replace(",", " ")
    )
    return " ".join(cleaned.split())


def _extract_location_tokens(ad: Dict[str, Any]) -> List[str]:
    values: List[str] = []
    location = ad.get("location")

    if isinstance(location, str):
        values.append(location)
    elif isinstance(location, dict):
        display_name = location.get("display_name")
        if display_name:
            values.append(str(display_name))
        area = location.get("area")
        if isinstance(area, list):
            for area_item in area:
                if area_item:
                    values.append(str(area_item))

    # Support normalized Adzuna payload variants.
    for field in ("location_display_name", "location_area"):
        field_value = ad.get(field)
        if isinstance(field_value, str):
            values.append(field_value)
        elif isinstance(field_value, list):
            for item in field_value:
                if item:
                    values.append(str(item))

    deduped: List[str] = []
    seen = set()
    for value in values:
        normalized = _normalize_geo_token(str(value))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(str(value))
    return deduped


def _infer_province(ad: Dict[str, Any]) -> Optional[str]:
    tokens = _extract_location_tokens(ad)
    for token in tokens:
        normalized = _normalize_geo_token(token)
        if not normalized:
            continue

        for alias, province in PROVINCE_ALIASES.items():
            if alias in normalized:
                return province

        for city, province in CITY_TO_PROVINCE.items():
            if city in normalized:
                return province
    return None


def build_region_distribution(job_ads: List[Dict[str, Any]]) -> List[Tuple[str, int, float, float, float]]:
    counts: Dict[str, int] = {}
    for ad in job_ads:
        province = _infer_province(ad)
        if province:
            counts[province] = counts.get(province, 0) + 1

    if not counts:
        return []

    total = float(sum(counts.values()))
    rows: List[Tuple[str, int, float, float, float]] = []
    for province, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lat, lon = NL_PROVINCE_CENTROIDS[province]
        share_pct = (count / total) * 100.0
        rows.append((province, count, share_pct, lat, lon))
    return rows


def build_job_ads_geo(job_ads: List[Dict[str, Any]]) -> List[Tuple[str, str, float, float, Optional[str]]]:
    rows: List[Tuple[str, str, float, float, Optional[str]]] = []
    for idx, ad in enumerate(job_ads, start=1):
        province = _infer_province(ad)
        if not province:
            continue
        lat, lon = NL_PROVINCE_CENTROIDS[province]
        job_id = str(ad.get("id") or ad.get("job_id") or f"job-{idx}")
        location_tokens = _extract_location_tokens(ad)
        location_label = location_tokens[0] if location_tokens else None
        rows.append((job_id, province, lat, lon, location_label))
    return rows


def build_snapshot_row() -> SnapshotRow:
    period_rate, vacancy_rate = _fetch_cbs_vacancy_rate_latest(CBS_VACANCY_RATE_TABLE, CBS_IT_SECTOR_KEY)
    period_vac, vacancies = _fetch_cbs_vacancies_latest(CBS_VACANCIES_TABLE, CBS_IT_SECTOR_KEY)

    # Use the most recent period available across both tables.
    period_key = max([p for p in [period_rate, period_vac] if p], default=period_rate or period_vac)

    period_label = _get_period_label(CBS_VACANCIES_TABLE, period_key)
    sector_name = _get_sector_label_for_vacancies(CBS_VACANCIES_TABLE, CBS_IT_SECTOR_KEY)

    job_ads = _fetch_job_ads_mock_or_adzuna()
    job_ads_count = len(job_ads)

    return SnapshotRow(
        period_key=period_key,
        period_label=period_label,
        sector_name=sector_name,
        vacancies=vacancies,
        vacancy_rate=vacancy_rate,
        job_ads_count=job_ads_count,
    )


def build_top_skills(job_ads: List[Dict[str, Any]], top_n: int = 25) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {}
    for ad in job_ads:
        desc = ad.get("description") or ad.get("description", "")
        for skill in extract_skills_from_text(desc):
            counts[skill] = counts.get(skill, 0) + 1

    items = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return items[:top_n]


def _connect_warehouse():
    host = os.getenv("WAREHOUSE_HOST", "localhost")
    port = int(os.getenv("WAREHOUSE_PORT", "5433"))
    db = os.getenv("WAREHOUSE_DB", "freshminds_dw")
    user = os.getenv("WAREHOUSE_USER", "admin")
    password = os.getenv("WAREHOUSE_PASSWORD", "")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)


def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS job_market_nl")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_market_nl.it_market_snapshot (
              period_key TEXT NOT NULL,
              period_label TEXT,
              sector_name TEXT,
              vacancies DOUBLE PRECISION,
              vacancy_rate DOUBLE PRECISION,
              job_ads_count INTEGER NOT NULL,
              loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_market_nl.it_market_top_skills (
              skill TEXT NOT NULL,
              count INTEGER NOT NULL,
              loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_market_nl.it_market_region_distribution (
              region TEXT NOT NULL,
              job_ads_count INTEGER NOT NULL,
              share_pct DOUBLE PRECISION NOT NULL,
              latitude DOUBLE PRECISION NOT NULL,
              longitude DOUBLE PRECISION NOT NULL,
              loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_market_nl.it_market_job_ads_geo (
              job_id TEXT NOT NULL,
              region TEXT NOT NULL,
              latitude DOUBLE PRECISION NOT NULL,
              longitude DOUBLE PRECISION NOT NULL,
              location_label TEXT,
              loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def refresh_tables(
    snapshot: SnapshotRow,
    top_skills: List[Tuple[str, int]],
    region_distribution: List[Tuple[str, int, float, float, float]],
    job_ads_geo: List[Tuple[str, str, float, float, Optional[str]]],
) -> None:
    conn = _connect_warehouse()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_snapshot")
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_top_skills")
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_region_distribution")
            cur.execute("TRUNCATE TABLE job_market_nl.it_market_job_ads_geo")

            cur.execute(
                """
                INSERT INTO job_market_nl.it_market_snapshot
                  (period_key, period_label, sector_name, vacancies, vacancy_rate, job_ads_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    snapshot.period_key,
                    snapshot.period_label,
                    snapshot.sector_name,
                    snapshot.vacancies,
                    snapshot.vacancy_rate,
                    snapshot.job_ads_count,
                ),
            )

            if top_skills:
                args = [(skill, count) for skill, count in top_skills]
                # Use execute_values for efficiency
                from psycopg2.extras import execute_values

                execute_values(
                    cur,
                    "INSERT INTO job_market_nl.it_market_top_skills (skill, count) VALUES %s",
                    args,
                )

            if region_distribution:
                from psycopg2.extras import execute_values

                execute_values(
                    cur,
                    """
                    INSERT INTO job_market_nl.it_market_region_distribution
                      (region, job_ads_count, share_pct, latitude, longitude)
                    VALUES %s
                    """,
                    region_distribution,
                )

            if job_ads_geo:
                from psycopg2.extras import execute_values

                execute_values(
                    cur,
                    """
                    INSERT INTO job_market_nl.it_market_job_ads_geo
                      (job_id, region, latitude, longitude, location_label)
                    VALUES %s
                    """,
                    job_ads_geo,
                )

        conn.commit()
    finally:
        conn.close()


def run_job_market_nl_postgres_pipeline() -> None:
    snapshot = build_snapshot_row()
    job_ads = _fetch_job_ads_mock_or_adzuna()
    top_skills = build_top_skills(job_ads)
    region_distribution = build_region_distribution(job_ads)
    job_ads_geo = build_job_ads_geo(job_ads)

    refresh_tables(snapshot, top_skills, region_distribution, job_ads_geo)

    print(
        "[Job Market Postgres] ✓ Refreshed job_market_nl.it_market_snapshot, "
        "job_market_nl.it_market_top_skills, job_market_nl.it_market_region_distribution, and "
        "job_market_nl.it_market_job_ads_geo "
        f"(period={snapshot.period_key}, ads={snapshot.job_ads_count}, skills={len(top_skills)}, regions={len(region_distribution)}, geo_points={len(job_ads_geo)})"
    )
