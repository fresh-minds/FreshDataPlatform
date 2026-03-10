"""
Bronze ingestion for Harvey Nash NL job postings (vacatures).

Scrapes https://www.harveynash.nl/vacatures using Playwright DOM extraction.
The page is JavaScript-rendered (Next.js + herefish.com) so static requests
do not work; Playwright renders the full page and extracts data from job cards.

Field notes from reverse engineering:
  - Location card text is 'City, Province' (e.g. 'Amsterdam, Noord-Holland')
  - Salary is exposed as text: '€7035 - €9607 per maand' or 'Richttarief 80 EUR'
  - 15 cards per page; pagination via a 'next page' link
  - Job IDs are numeric, embedded in the URL slug
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from shared.config.paths import LakehouseLayer, ensure_local_path_exists, get_lakehouse_table_path
from shared.config.settings import get_settings
from shared.utils.spark_helpers import clean_df_for_spark

HARVEY_NASH_URL = "https://www.harveynash.nl/vacatures"

_MOCK_JOBS: List[Dict[str, Any]] = [
    {
        "id": "mock-hn-1",
        "title": "Senior Data Engineer",
        "company": "Harvey Nash",
        "location": "Amsterdam",
        "province": "Noord-Holland",
        "contract_type": "",
        "description": "",
        "salary_min": None,
        "salary_max": None,
        "salary_raw": "Tarief in overleg",
        "url": "https://www.harveynash.nl/vacatures/100001-Senior-Data-Engineer",
        "posted_date": "",
    },
    {
        "id": "mock-hn-2",
        "title": "Cloud Architect",
        "company": "Harvey Nash",
        "location": "Rotterdam",
        "province": "Zuid-Holland",
        "contract_type": "",
        "description": "",
        "salary_min": 90.0,
        "salary_max": 120.0,
        "salary_raw": "Richttarief 90 EUR all-in ex BTW",
        "url": "https://www.harveynash.nl/vacatures/100002-Cloud-Architect",
        "posted_date": "",
    },
]

# JS snippet injected into the page to extract all visible job cards at once.
_CARD_EXTRACTION_JS = """
    () => {
        const cards = [];
        const items = document.querySelectorAll('[class*="job-item"]');
        items.forEach(item => {
            const link = item.querySelector('a[href*="/vacatures/"]');
            const locationEl = item.querySelector('[class*="location"], [class*="Location"]');
            const titleEl = item.querySelector('h2, h3, h4, [class*="title"], [class*="Title"]');
            const salaryEl = item.querySelector(
                '[class*="salary"], [class*="Salary"], [class*="rate"], [class*="Rate"], ' +
                '[class*="tarief"], [class*="loon"]'
            );
            cards.push({
                url: link ? link.href : '',
                location_raw: locationEl ? locationEl.innerText.trim() : '',
                title: titleEl ? titleEl.innerText.trim() : (link ? link.innerText.trim() : ''),
                salary_raw: salaryEl ? salaryEl.innerText.trim() : '',
                raw_text: item.innerText.trim(),
            });
        });
        return cards;
    }
"""

# Selector for the 'next page' link in the pagination bar.
_NEXT_PAGE_SEL = (
    'a[aria-label="next page"], '
    'a[aria-label="volgende"], '
    '[class*="pagination"] a[rel="next"]:not([aria-disabled="true"]), '
    '[class*="pagination__next"]:not([disabled]):not([aria-disabled="true"])'
)


def _extract_id_from_url(url: str) -> str:
    match = re.search(r"/vacatures/(\d+)", url)
    return match.group(1) if match else ""


def _parse_location(raw: str) -> Tuple[str, str]:
    """Split 'Amsterdam, Noord-Holland' → ('Amsterdam', 'Noord-Holland')."""
    parts = [p.strip() for p in raw.split(",")]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return raw.strip(), ""


def _parse_salary(text: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract (salary_min, salary_max) from card text.
    Handles:
      '€7035 - €9607 per maand'
      'Richttarief 80 EUR all-in ex BTW'
    Returns (None, None) when no amount is found.
    """
    match = re.search(r"€\s*([\d.,]+)\s*[-–]\s*€?\s*([\d.,]+)", text)
    if match:
        def _f(s: str) -> float:
            return float(s.replace(".", "").replace(",", "."))
        return _f(match.group(1)), _f(match.group(2))
    match = re.search(r"(?:richttarief|tarief)\s+([\d.,]+)\s*(?:eur|€)", text, re.IGNORECASE)
    if match:
        return float(match.group(1).replace(",", ".")), None
    return None, None


def _extract_cards(page: Any, seen: Dict[str, Any]) -> int:
    """Extract all visible job cards on the current page; returns count of new entries."""
    try:
        cards = page.evaluate(_CARD_EXTRACTION_JS)
    except Exception as e:
        print(f"[HarveyNash Bronze] Card extraction error: {e}")
        return 0

    new = 0
    for card in cards:
        job_url = card.get("url", "")
        job_id = _extract_id_from_url(job_url)
        if not job_id or job_id in seen:
            continue

        location_raw = card.get("location_raw", "")
        city, province = _parse_location(location_raw)

        salary_text = card.get("salary_raw") or card.get("raw_text", "")
        sal_min, sal_max = _parse_salary(salary_text)

        title = card.get("title", "")
        if title.startswith(location_raw):
            title = title[len(location_raw):].strip()

        seen[job_id] = {
            "id": job_id,
            "title": title,
            "company": "Harvey Nash",
            "location": city,
            "province": province,
            "contract_type": "",
            "description": "",
            "salary_min": sal_min,
            "salary_max": sal_max,
            "salary_raw": (salary_text or "")[:300],
            "url": job_url,
            "posted_date": "",
        }
        new += 1

    return new


def _scrape_with_playwright(url: str, timeout_ms: int = 60_000) -> List[Dict[str, Any]]:
    """Render the Harvey Nash vacatures page with Playwright and extract all job cards."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ImportError(
            "playwright is required. Install with: pip install playwright && playwright install chromium"
        ) from exc

    seen: Dict[str, Any] = {}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="nl-NL",
        )
        page = context.new_page()

        print(f"[HarveyNash Bronze] Loading {url} ...")
        try:
            page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        except Exception as exc:
            print(f"[HarveyNash Bronze] Navigation warning (continuing): {exc}")
        page.wait_for_timeout(3000)

        # Page 1
        n = _extract_cards(page, seen)
        print(f"[HarveyNash Bronze] Page 1: {n} cards (total: {len(seen)})")

        # Paginate
        page_num = 2
        while page_num <= 50:  # safety cap
            try:
                next_btn = page.locator(_NEXT_PAGE_SEL).first
                if not next_btn.is_visible(timeout=3000):
                    break
                if next_btn.get_attribute("aria-disabled") == "true":
                    break
                next_btn.click()
                page.wait_for_timeout(2500)
                try:
                    page.wait_for_load_state("networkidle", timeout=8_000)
                except Exception:
                    pass
                n = _extract_cards(page, seen)
                print(f"[HarveyNash Bronze] Page {page_num}: {n} new cards (total: {len(seen)})")
                if n == 0:
                    break
                page_num += 1
            except Exception as e:
                if "not enabled" in str(e) or "aria-disabled" in str(e):
                    print("[HarveyNash Bronze] Last page reached.")
                else:
                    print(f"[HarveyNash Bronze] Pagination stopped: {e}")
                break

        browser.close()

    return list(seen.values())


def run_bronze_harvey_nash_vacatures(
    spark: SparkSession,
    notebookutils: Any,
    fabric: Any,
    bronze_table_name: str = "harvey_nash_vacatures_raw",
    workspace_id: Optional[str] = None,
) -> None:
    settings = get_settings()
    print("[HarveyNash Bronze] Starting Harvey Nash vacatures ingestion...")

    if workspace_id is None:
        workspace_id = fabric.get_workspace_id()

    table_path = get_lakehouse_table_path(
        table_name=bronze_table_name,
        layer=LakehouseLayer.BRONZE,
        domain="odp_staffing_demand",
        workspace_id=workspace_id,
    )

    use_mock = settings.is_local and __import__("os").getenv("LOCAL_MOCK_EXTERNAL", "true").lower() == "true"

    if use_mock:
        print("[HarveyNash Bronze] Using mock data (LOCAL_MOCK_EXTERNAL=true).")
        jobs = _MOCK_JOBS
    else:
        jobs = _scrape_with_playwright(HARVEY_NASH_URL)

    if not jobs:
        print("[HarveyNash Bronze] No job postings found. Skipping write.")
        return

    pdf = pd.DataFrame(jobs)
    pdf = clean_df_for_spark(pdf)
    df_spark = spark.createDataFrame(pdf).withColumn("ingestion_timestamp", current_timestamp())

    if settings.is_local:
        ensure_local_path_exists(table_path)

    df_spark.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)
    print(f"[HarveyNash Bronze] ✓ Ingested {len(jobs)} rows -> {table_path}")
