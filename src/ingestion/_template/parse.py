# TEMPLATE — copy and customise for your new source.
# See docs/INGESTION_GUIDE.md § Step 3 for details.
#
# Rename this file to:
#   src/ingestion/<source_name>/parse_<dataset>.py
"""Parser for <SOURCE_NAME> <DATASET>.

The parser transforms raw bronze artifacts into structured records that
match the ``SourceTableConfig`` column definitions.

The DAG calls ``parse_artifacts()`` with a list of raw artifact dicts
(containing ``raw_bytes``, ``content_type``, ``bronze_object_path``, etc.)
and expects back a list of flat dictionaries ready for Postgres upsert.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger(__name__)


def parse_artifacts(artifacts: list[dict]) -> list[dict]:
    """Parse a list of raw bronze artifacts into structured records.

    Each artifact dict has at minimum:
      - ``raw_bytes``: bytes
      - ``content_type``: str
      - ``bronze_object_path``: str (MinIO key)
      - ``extraction_method``: str

    Returns a list of flat dicts whose keys match the column names in your
    ``SourceTableConfig``.
    """
    all_records: list[dict] = []
    for art in artifacts:
        raw_bytes = art["raw_bytes"]
        content_type = art.get("content_type", "")
        bronze_path = art.get("bronze_object_path", "")

        try:
            records = _parse_single(raw_bytes, content_type, bronze_path)
            all_records.extend(records)
        except Exception as exc:
            log.error("Failed to parse artifact %s: %s", bronze_path, exc)

    log.info("[parse] Parsed %d total records from %d artifacts.", len(all_records), len(artifacts))
    return all_records


def _parse_single(raw_bytes: bytes, content_type: str, bronze_path: str) -> list[dict]:
    """Parse a single artifact into records.

    TODO: Implement format-specific parsing logic.
    """
    records: list[dict] = []

    if "json" in content_type:
        data = json.loads(raw_bytes)
        # TODO: Navigate to the actual records in your JSON structure
        # items = data.get("results", [])
        # for item in items:
        #     records.append(_map_record(item, bronze_path))
    elif "html" in content_type:
        # from bs4 import BeautifulSoup
        # soup = BeautifulSoup(raw_bytes, "html.parser")
        # TODO: Parse HTML table or elements into records
        pass
    elif "csv" in content_type:
        # import csv, io
        # reader = csv.DictReader(io.StringIO(raw_bytes.decode("utf-8")))
        # for row in reader:
        #     records.append(_map_record(row, bronze_path))
        pass
    elif "xlsx" in content_type or "spreadsheet" in content_type:
        # import openpyxl, io
        # wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), read_only=True)
        # TODO: Parse Excel workbook into records
        pass
    else:
        log.warning("Unsupported content type: %s", content_type)

    return records


def _map_record(raw: dict, bronze_path: str) -> dict:
    """Map a single raw record to the target schema.

    TODO: Map source fields to your SourceTableConfig column names.
    """
    record = {
        "entity_id": raw.get("id", ""),
        "title": raw.get("title", ""),
        "status": raw.get("status", ""),
        "created_date": raw.get("created_date"),
        "description": raw.get("description", ""),
        "source_url": raw.get("url", ""),
        "bronze_object_path": bronze_path,
    }

    # Compute checksum for change detection
    record["checksum_sha256"] = _compute_checksum(record)

    return record


def _compute_checksum(record: dict) -> str:
    """SHA-256 checksum over all data fields (excludes ingested_at, bronze_object_path)."""
    excluded = {"ingested_at", "bronze_object_path", "checksum_sha256"}
    payload = {k: v for k, v in sorted(record.items()) if k not in excluded and v is not None}
    return hashlib.sha256(json.dumps(payload, default=str).encode()).hexdigest()
