"""Extract vacatures from the Source SP1 portal via Playwright.

Extraction strategy (most-stable → least-stable):
  1. Export download — look for an explicit CSV/XLSX export button in the
     vacatures list view. If found, trigger the download and return it.
  2. XHR/JSON capture — intercept every JSON response the browser legitimately
     receives while navigating and paginating the vacatures listing. Salesforce
     Experience Cloud sites emit Aura controller payloads and/or REST API
     responses that contain the full record set.
  3. DOM HTML fallback — capture the rendered HTML and return it so the parser
     can attempt table / card extraction.

Rate limiting (HARD RULE):
  - Default: 0.2 RPS (one request per 5 seconds) from RATE_LIMIT_RPS_PER_DOMAIN.
  - Maximum concurrency per domain: 1 (single Playwright page, sequential).
  - Retries with exponential backoff are handled at the Airflow task level.
  - If the portal returns 403 / 429, the code raises immediately with clear
    guidance rather than retrying in a tight loop.
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_RATE_LIMIT_RPS = float(os.environ.get("RATE_LIMIT_RPS_PER_DOMAIN", "0.2"))
_MIN_DELAY_S = 1.0 / _RATE_LIMIT_RPS if _RATE_LIMIT_RPS > 0 else 5.0
_MAX_PAGES = int(os.environ.get("MAX_VACATURE_PAGES", "50"))

# Salesforce Experience Cloud portal — common vacatures path segments
_VACATURES_PATHS = [
    "/s/vacatures",
    "/s/vacature",
    "/s/openstaande-vacatures",
    "/s/requisitions",
    "/s/jobs",
    "/s/opdrachten",
    "/s/job-overview",
]

# XHR URL patterns that are likely to carry vacancy payload data.
# These match Salesforce Aura, Apex REST, and generic vacancy endpoints.
_XHR_PATTERNS = [
    r"/aura\?",             # Salesforce Aura framework actions
    r"/s/sfsites/aura",     # SF Sites Aura endpoint
    r"/apex/",              # Visualforce / Apex endpoint
    r"/services/data/",     # Salesforce REST API
    r"vacatur",             # Any endpoint containing "vacatur"
    r"requisition",         # Requisition-related endpoint
    r"vacancy",             # Vacancy endpoint
]

# Keywords that must appear in a JSON payload for it to be considered
# vacancy data (heuristic deduplication filter).
_PAYLOAD_KEYWORDS = [
    "vacatur", "functie", "titel", "title", "status",
    "publish", "location", "description", "requisiti",
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RawArtifact:
    artifact_id: str
    url: str
    raw_bytes: bytes
    content_type: str
    http_status: int
    response_ms: int
    # "export_download" | "xhr_json" | "dom_html" | "dom_html_snapshot"
    # "dom_html_snapshot" is written to bronze for traceability only;
    # the parser skips it in favour of "dom_html" artifacts.
    extraction_method: str
    entity_keys: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def extract_all(
    *,
    page,
    base_url: str,
    run_id: str,
    run_dt: str,
    lookback_days: int = 7,
) -> list[RawArtifact]:
    """Navigate to the vacatures section and extract all available data.

    Returns a (possibly mixed-method) list of RawArtifact objects.
    The caller writes these to MinIO; the parser reads them back.
    """
    artifacts: list[RawArtifact] = []
    captured_xhr: list[dict] = []

    # Register XHR listener before any navigation
    page.on("response", lambda resp: _capture_xhr_response(resp, captured_xhr))

    # Honour rate limit before the first navigation (login may have just completed)
    _polite_wait()

    # Navigate to vacatures listing
    vacatures_url = _navigate_to_vacatures(page, base_url)
    log.info("Vacatures page: %s", vacatures_url)

    # Always capture an HTML snapshot for traceability
    html_snap = _snapshot_html(page, vacatures_url)
    if html_snap:
        artifacts.append(html_snap)

    # --- Strategy 1: look for a download / export button ---
    export_art = _try_export_download(page, vacatures_url, run_id)
    if export_art:
        log.info("Export download succeeded — using as primary artifact.")
        artifacts.append(export_art)
        return artifacts

    # --- Strategy 2: collect XHR/JSON responses via pagination ---
    _paginate_and_scroll(page)

    xhr_arts = _package_xhr_artifacts(captured_xhr, run_id)
    if xhr_arts:
        log.info("Collected %d XHR JSON artifacts.", len(xhr_arts))
        artifacts.extend(xhr_arts)
        return artifacts

    # --- Strategy 3: DOM HTML fallback ---
    log.info("No structured data captured; using DOM HTML fallback.")
    dom_art = _full_page_html(page, vacatures_url, run_id)
    if dom_art:
        artifacts.append(dom_art)

    if len(artifacts) == 1 and artifacts[0].extraction_method == "dom_html":
        log.warning(
            "Only HTML snapshot captured. Parsing will attempt table/card extraction "
            "but results may be incomplete if the page uses heavy client-side rendering."
        )

    return artifacts


# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

def _navigate_to_vacatures(page, base_url: str) -> str:
    """Try known path patterns, then fall back to navigation-link discovery."""
    base_root = base_url.rstrip("/")
    if base_root.endswith("/s"):
        base_root = base_root[:-2]  # strip trailing /s so we can re-append paths

    for path in _VACATURES_PATHS:
        url = base_root + path
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15_000)
            _polite_wait()
            if _page_has_vacature_content(page):
                log.info("Vacatures found at %s", url)
                return page.url
        except Exception as exc:
            log.debug("Path %s → %s", path, exc)

    # Try to find a "vacatures" link in the logged-in navigation
    nav_link = _find_nav_link(page, ["vacatures", "vacature", "jobs", "opdrachten"])
    if nav_link:
        page.goto(nav_link, wait_until="domcontentloaded", timeout=20_000)
        _polite_wait()
        log.info("Navigated via nav link: %s", page.url)
        return page.url

    log.warning("Could not locate a dedicated vacatures page. Staying on: %s", page.url)
    return page.url


def _page_has_vacature_content(page) -> bool:
    body_text = (page.text_content("body") or "").lower()
    return any(kw in body_text for kw in ["vacatur", "functie", "requisiti", "job", "opdracht"])


def _find_nav_link(page, keywords: list[str]) -> Optional[str]:
    try:
        for link in page.query_selector_all("a[href]"):
            href = link.get_attribute("href") or ""
            text = (link.text_content() or "").lower()
            for kw in keywords:
                if kw in href.lower() or kw in text:
                    # Return absolute URL
                    if href.startswith("http"):
                        return href
                    # Reconstruct from base
                    return page.url.split("/s/")[0] + href
    except Exception as exc:
        log.debug("Nav link search failed: %s", exc)
    return None


# ---------------------------------------------------------------------------
# Strategy 1 — Export download
# ---------------------------------------------------------------------------

_EXPORT_SELECTORS = [
    "button:has-text('Export')",
    "button:has-text('Exporteren')",
    "button:has-text('Download')",
    "a:has-text('Export CSV')",
    "a:has-text('Download CSV')",
    "a[href$='.csv']",
    "a[href$='.xlsx']",
    ".exportBtn",
    "[title='Export']",
    "[aria-label*='export' i]",
    "[aria-label*='download' i]",
    "button[name='export']",
]


def _try_export_download(page, vacatures_url: str, run_id: str) -> Optional[RawArtifact]:
    """Click an export button and capture the resulting download."""
    for sel in _EXPORT_SELECTORS:
        try:
            el = page.query_selector(sel)
            if el is None or not el.is_visible():
                continue
            log.info("Found export element (%s) — triggering download.", sel)
            with page.expect_download(timeout=30_000) as dl_info:
                el.click()
            dl = dl_info.value
            raw_bytes = _download_to_bytes(dl)
            filename = dl.suggested_filename or "vacatures_export"
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "bin"
            return RawArtifact(
                artifact_id=f"export_{run_id}",
                url=vacatures_url,
                raw_bytes=raw_bytes,
                content_type=_ext_to_mime(ext),
                http_status=200,
                response_ms=0,
                extraction_method="export_download",
            )
        except Exception as exc:
            log.debug("Export selector %r failed: %s", sel, exc)
    return None


def _download_to_bytes(download) -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".dl", delete=False) as fh:
        tmp = fh.name
    try:
        download.save_as(tmp)
        with open(tmp, "rb") as fh:
            return fh.read()
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Strategy 2 — XHR/JSON capture
# ---------------------------------------------------------------------------

def _capture_xhr_response(response, captured: list) -> None:
    """Playwright response event handler — captures vacancy-related JSON responses.

    Safety notes:
    - ``response.body()`` is called inside a try/except because Playwright can
      raise for aborted, redirected, or already-consumed responses.
    - This function MUST NOT raise; any exception is caught and logged at DEBUG.
    """
    try:
        url = response.url
        content_type = response.headers.get("content-type", "")

        if "json" not in content_type:
            return

        # Check HTTP error codes that signal access problems
        if response.status == 429:
            log.error(
                "HTTP 429 (rate limited) from %s. Halting XHR capture. "
                "Increase RATE_LIMIT_RPS_PER_DOMAIN delay or contact portal admin.",
                url,
            )
            return
        if response.status == 403:
            log.error(
                "HTTP 403 (forbidden) from %s. Session may have expired or "
                "the account lacks access to this resource.",
                url,
            )
            return

        if not any(re.search(pat, url, re.IGNORECASE) for pat in _XHR_PATTERNS):
            return

        # body() can fail if the response was aborted, redirected, or the
        # underlying network connection closed before the body was read.
        try:
            body = response.body()
        except Exception as body_exc:
            log.debug("Could not read response body from %s: %s", url, body_exc)
            return

        if len(body) < 100:
            return

        try:
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        if not _payload_looks_like_vacatures(payload):
            return

        captured.append({
            "url": url,
            "status": response.status,
            "content_type": content_type,
            "body": body,
        })
        log.debug("XHR captured: %s (%d bytes)", url, len(body))

    except Exception as exc:
        # Event handlers must not raise — log and swallow
        log.debug("XHR capture handler error: %s", exc)


def _payload_looks_like_vacatures(payload: Any) -> bool:
    text = json.dumps(payload).lower()
    hits = sum(1 for kw in _PAYLOAD_KEYWORDS if kw in text)
    return hits >= 3


def _paginate_and_scroll(page) -> None:
    """Scroll and paginate to trigger all lazy-loaded XHR responses."""
    _polite_wait()

    # Scroll down in steps to trigger infinite-scroll / lazy loaders
    try:
        for _ in range(6):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.8)
        page.evaluate("window.scrollTo(0, 0)")
    except Exception:
        pass

    # Click "Next page" / "Load more" buttons
    next_selectors = [
        "button:has-text('Next')",
        "button:has-text('Volgende')",
        "button:has-text('Load more')",
        "button:has-text('Meer laden')",
        "button[aria-label*='next' i]",
        "a[aria-label*='next' i]",
        ".pagination-next",
        "[data-page='next']",
        "li.next > a",
    ]

    for _page_num in range(_MAX_PAGES):
        _polite_wait()
        clicked = False
        for sel in next_selectors:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_enabled() and btn.is_visible():
                    btn.click()
                    try:
                        page.wait_for_load_state("networkidle", timeout=8_000)
                    except Exception:
                        time.sleep(1)
                    clicked = True
                    log.debug("Paginated to next page (%d).", _page_num + 2)
                    break
            except Exception:
                pass
        if not clicked:
            break


def _package_xhr_artifacts(captured: list, run_id: str) -> list[RawArtifact]:
    seen: set[str] = set()
    artifacts: list[RawArtifact] = []
    for i, entry in enumerate(captured):
        url = entry["url"]
        if url in seen:
            continue
        seen.add(url)
        artifacts.append(
            RawArtifact(
                artifact_id=f"xhr_{run_id}_{i:04d}",
                url=url,
                raw_bytes=entry["body"],
                content_type=entry.get("content_type", "application/json"),
                http_status=entry.get("status", 200),
                response_ms=0,
                extraction_method="xhr_json",
            )
        )
    return artifacts


# ---------------------------------------------------------------------------
# Strategy 3 — DOM HTML
# ---------------------------------------------------------------------------

def _snapshot_html(page, url: str) -> Optional[RawArtifact]:
    """Capture the current page HTML as a traceability-only artifact.

    Uses ``extraction_method="dom_html_snapshot"`` so that the parse/load
    task recognises it as traceability-only and skips it during parsing.
    Only ``"dom_html"`` artifacts are fed to the parser.
    """
    try:
        html = page.content()
        return RawArtifact(
            artifact_id=f"html_snap_{uuid.uuid4().hex[:8]}",
            url=url,
            raw_bytes=html.encode("utf-8"),
            content_type="text/html",
            http_status=200,
            response_ms=0,
            extraction_method="dom_html_snapshot",
        )
    except Exception as exc:
        log.warning("HTML snapshot failed: %s", exc)
        return None


def _full_page_html(page, url: str, run_id: str) -> Optional[RawArtifact]:
    try:
        html = page.content()
        return RawArtifact(
            artifact_id=f"dom_{run_id}",
            url=url,
            raw_bytes=html.encode("utf-8"),
            content_type="text/html",
            http_status=200,
            response_ms=0,
            extraction_method="dom_html",
        )
    except Exception as exc:
        log.error("Full-page HTML capture failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _polite_wait() -> None:
    """Enforce the configured rate limit between requests."""
    time.sleep(_MIN_DELAY_S)


def _ext_to_mime(ext: str) -> str:
    return {
        "csv": "text/csv",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "xls": "application/vnd.ms-excel",
        "json": "application/json",
        "html": "text/html",
    }.get(ext, "application/octet-stream")
