"""
Final Harvey Nash NL scraper test.
- Full DOM card extraction with pagination
- Salary / rate parsing from card text
- Province split from location string
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

HARVEY_NASH_URL = "https://www.harveynash.nl/vacatures"


def _extract_id_from_url(url: str) -> str:
    match = re.search(r"/vacatures/(\d+)", url)
    return match.group(1) if match else ""


def _parse_location(location_raw: str) -> Tuple[str, str]:
    """
    Split 'Amsterdam, Noord-Holland' into ('Amsterdam', 'Noord-Holland').
    Returns (city, province).
    """
    parts = [p.strip() for p in location_raw.split(",")]
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return location_raw.strip(), ""


def _parse_salary(text: str) -> Tuple[Optional[float], Optional[float], str]:
    """
    Extract salary_min, salary_max, and salary_type from card text.
    Handles patterns like:
      '€7035 - €9607 per maand'
      'Richttarief 80 EUR all-in ex BTW'
      'Tarief in overleg'
    Returns (salary_min, salary_max, salary_raw_text).
    """
    # Try '€X - €Y per ...' pattern
    match = re.search(r"€\s*([\d.,]+)\s*[-–]\s*€?\s*([\d.,]+)", text)
    if match:
        def to_float(s: str) -> float:
            return float(s.replace(".", "").replace(",", "."))
        return to_float(match.group(1)), to_float(match.group(2)), text
    # Try 'Richttarief X EUR' hourly rate
    match = re.search(r"(?:richttarief|tarief)\s+([\d.,]+)\s*(?:eur|€)", text, re.IGNORECASE)
    if match:
        rate = float(match.group(1).replace(",", "."))
        return rate, None, text
    return None, None, text


def scrape(url: str = HARVEY_NASH_URL, timeout_ms: int = 60_000) -> List[Dict[str, Any]]:
    from playwright.sync_api import sync_playwright

    all_jobs: Dict[str, Dict[str, Any]] = {}  # keyed by job_id for dedup

    _CARD_JS = """
        () => {
            const cards = [];
            const items = document.querySelectorAll('[class*="job-item"]');
            items.forEach(item => {
                const link = item.querySelector('a[href*="/vacatures/"]');
                const locationEl = item.querySelector('[class*="location"], [class*="Location"]');
                const titleEl = item.querySelector('h2, h3, h4, [class*="title"], [class*="Title"]');
                const salaryEl = item.querySelector('[class*="salary"], [class*="Salary"], [class*="rate"], [class*="Rate"], [class*="tarief"], [class*="loon"]');
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

    def _extract_cards_from_page(page: Any) -> int:
        try:
            cards = page.evaluate(_CARD_JS)
        except Exception as e:
            print(f"  [DOM] Card eval error: {e}")
            return 0

        new = 0
        for card in cards:
            job_url = card.get("url", "")
            job_id = _extract_id_from_url(job_url)
            if not job_id or job_id in all_jobs:
                continue

            location_raw = card.get("location_raw", "")
            city, province = _parse_location(location_raw)

            # Salary: prefer dedicated salary element, fall back to raw_text
            salary_text = card.get("salary_raw") or card.get("raw_text", "")
            sal_min, sal_max, sal_raw = _parse_salary(salary_text)

            title = card.get("title", "")
            # Strip location from title if it leaked in (some cards include it)
            if title.startswith(location_raw):
                title = title[len(location_raw):].strip()

            all_jobs[job_id] = {
                "id": job_id,
                "title": title,
                "company": "Harvey Nash",
                "location": city,
                "province": province,
                "contract_type": "",   # not exposed in card; available on detail page
                "description": "",     # available on detail page
                "salary_min": sal_min,
                "salary_max": sal_max,
                "salary_raw": sal_raw[:200] if sal_raw else "",
                "url": job_url,
                "posted_date": "",     # not on card
            }
            new += 1

        return new

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

        print(f"[Scraper] Loading {url} ...")
        try:
            page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        except Exception as exc:
            print(f"[Scraper] Navigation warning: {exc}")
        page.wait_for_timeout(3000)

        # ── Page 1 ────────────────────────────────────────────────────────
        n = _extract_cards_from_page(page)
        print(f"[Scraper] Page 1: extracted {n} new cards (total so far: {len(all_jobs)})")

        # ── Total count hint ──────────────────────────────────────────────
        try:
            count_text = page.evaluate("""
                () => {
                    const els = document.querySelectorAll('[class*="count"], [class*="total"], [class*="results-summary"], [class*="ResultCount"]');
                    return Array.from(els).map(e => e.innerText.trim()).filter(t => /\\d/.test(t) && t.length < 120);
                }
            """)
            print(f"[Scraper] Result count hints: {count_text[:3]}")
        except Exception:
            pass

        # ── Pagination: click 'next' until exhausted ──────────────────────
        page_num = 2
        while True:
            try:
                # Look for enabled next/volgende button
                next_sel = 'a[aria-label="next page"], a[aria-label="volgende"], [class*="pagination"] a[rel="next"], nav a:has(svg[aria-label*="next"]), [class*="pagination__next"]:not([disabled])'
                next_btn = page.locator(next_sel).first
                if next_btn.count() == 0:
                    print(f"[Scraper] No more 'next' button found. Done.")
                    break
                # Check if it's truly clickable
                if not next_btn.is_visible():
                    print(f"[Scraper] Next button not visible. Done.")
                    break
                print(f"[Scraper] Clicking next -> page {page_num} ...")
                next_btn.click()
                page.wait_for_timeout(3000)
                try:
                    page.wait_for_load_state("networkidle", timeout=10_000)
                except Exception:
                    pass
                n = _extract_cards_from_page(page)
                print(f"[Scraper] Page {page_num}: extracted {n} new cards (total: {len(all_jobs)})")
                if n == 0:
                    print("[Scraper] No new cards on this page. Stopping pagination.")
                    break
                page_num += 1
                if page_num > 20:  # safety cap
                    break
            except Exception as e:
                print(f"[Scraper] Pagination error: {e}. Stopping.")
                break

        browser.close()

    jobs = list(all_jobs.values())

    print("\n" + "=" * 70)
    print(f"Total vacatures scraped: {len(jobs)}")
    print("\nFirst 5 vacatures:")
    for i, j in enumerate(jobs[:5], 1):
        print(f"\n  [{i}] id={j['id']}")
        print(f"       title         : {j['title']}")
        print(f"       location/city : {j['location']}")
        print(f"       province      : {j['province']}")
        print(f"       salary_min    : {j['salary_min']}")
        print(f"       salary_max    : {j['salary_max']}")
        print(f"       salary_raw    : {j['salary_raw'][:80]}")
        print(f"       url           : {j['url'][:90]}")
    print("=" * 70)
    print("\nSchema:", list(jobs[0].keys()) if jobs else "N/A")
    return jobs


if __name__ == "__main__":
    jobs = scrape()
    print(f"\n✓ Done: {len(jobs)} vacatures from Harvey Nash NL")
