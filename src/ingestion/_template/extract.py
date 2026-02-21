# TEMPLATE — copy and customise for your new source.
# See docs/INGESTION_GUIDE.md § Step 2 for details.
#
# Rename this file to:
#   src/ingestion/<source_name>/extract_<dataset>.py
"""Extractor for <SOURCE_NAME> <DATASET>.

The extractor is responsible for:
  1. Authenticating with the source (if needed)
  2. Discovering / listing entities
  3. Downloading raw data (JSON, HTML, CSV, Excel, etc.)
  4. Returning a list of ``RawArtifact`` objects ready for bronze storage

The DAG calls ``extract_all()`` inside a Playwright browser context.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class RawArtifact:
    """One raw artifact to be stored in MinIO bronze layer."""
    artifact_id: str
    raw_bytes: bytes
    url: str
    content_type: str = "application/json"
    http_status: int = 200
    response_ms: int = 0
    extraction_method: str = "api_json"       # e.g. api_json, xhr_json, dom_html, export_download
    entity_keys: dict = field(default_factory=dict)


def extract_all(
    *,
    page=None,              # Playwright page (if browser-based extraction)
    base_url: str = "",
    run_id: str = "",
    run_dt: str = "",
    lookback_days: int = 7,
    # Add source-specific params here (API key, session, etc.)
) -> list[RawArtifact]:
    """Extract all raw artifacts from the source.

    Returns a list of ``RawArtifact`` objects.  Each artifact will be written
    to MinIO bronze by the DAG.

    TODO: Implement source-specific extraction logic.
    """
    artifacts: list[RawArtifact] = []

    # ── Example: API-based extraction ──────────────────────────────────
    # import requests
    # resp = requests.get(f"{base_url}/api/entities", timeout=30)
    # resp.raise_for_status()
    # artifacts.append(RawArtifact(
    #     artifact_id=f"api_{run_id}_0000",
    #     raw_bytes=resp.content,
    #     url=resp.url,
    #     content_type=resp.headers.get("content-type", "application/json"),
    #     http_status=resp.status_code,
    #     extraction_method="api_json",
    # ))

    # ── Example: Playwright-based extraction ───────────────────────────
    # page.goto(f"{base_url}/data")
    # page.wait_for_selector("table.results")
    # html = page.content()
    # artifacts.append(RawArtifact(
    #     artifact_id=f"dom_html_{run_id}_0000",
    #     raw_bytes=html.encode("utf-8"),
    #     url=page.url,
    #     content_type="text/html",
    #     extraction_method="dom_html",
    # ))

    log.info("[extract] Extracted %d artifacts for run_id=%s", len(artifacts), run_id)
    return artifacts
