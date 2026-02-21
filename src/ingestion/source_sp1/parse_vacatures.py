"""Parse bronze artifacts into normalised VacatureRecord dicts.

Handles three artifact types produced by the extractor:
  - export_download   CSV / XLSX file from a portal export button.
  - xhr_json          Salesforce Aura, REST API, or generic JSON payloads
                      captured via Playwright network intercept.
  - dom_html          Rendered HTML; last-resort table / card extraction via
                      BeautifulSoup.

All parsers produce the same dict shape, ready for postgres.upsert_vacatures().
The vacature_id falls back to a deterministic SHA-256 hash when no stable ID
can be found in the source data.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Optional

log = logging.getLogger(__name__)

# Statuses that indicate an inactive / closed vacancy
_INACTIVE_STATUSES = frozenset({
    "closed", "gesloten", "inactive", "inactief",
    "expired", "verlopen", "cancelled", "geannuleerd",
    "filled", "vervuld",
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_artifacts(
    raw_artifacts: list[dict],
) -> list[dict]:
    """Parse a list of raw-artifact dicts and return normalised record dicts.

    Each element of *raw_artifacts* must have:
      raw_bytes         bytes
      content_type      str
      extraction_method str
      url               str
      artifact_id       str
      bronze_object_path str  (optional)
    """
    all_records: list[dict] = []
    for art in raw_artifacts:
        method = art.get("extraction_method", "")
        try:
            if method == "export_download":
                records = _parse_export(art)
            elif method == "xhr_json":
                records = _parse_xhr_json(art)
            elif method == "dom_html":
                records = _parse_dom_html(art)
            else:
                log.warning("Unknown extraction_method=%r; skipping artifact %s.", method, art.get("artifact_id"))
                continue

            # Post-processing: fill computed fields
            for rec in records:
                if not rec.checksum_sha256:
                    rec.checksum_sha256 = rec.compute_checksum()
                if not rec.bronze_object_path:
                    rec.bronze_object_path = art.get("bronze_object_path", "")

            all_records.extend(r.to_dict() for r in records)

        except Exception as exc:
            log.error(
                "Failed to parse artifact %s (%s): %s",
                art.get("artifact_id"), method, exc, exc_info=True,
            )

    log.info("Parsed %d vacature records from %d artifacts.", len(all_records), len(raw_artifacts))
    return all_records


# ---------------------------------------------------------------------------
# VacatureRecord
# ---------------------------------------------------------------------------

@dataclass
class VacatureRecord:
    vacature_id: str
    title: Optional[str] = None
    status: Optional[str] = None
    client_name: Optional[str] = None
    location: Optional[str] = None
    publish_date: Optional[str] = None      # ISO date string: YYYY-MM-DD
    closing_date: Optional[str] = None      # ISO date string: YYYY-MM-DD
    hours: Optional[float] = None
    hours_text: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    updated_at_source: Optional[str] = None  # ISO datetime string
    source_url: Optional[str] = None
    bronze_object_path: Optional[str] = None
    checksum_sha256: Optional[str] = None
    is_active: Optional[bool] = None

    def to_dict(self) -> dict:
        return asdict(self)

    def compute_checksum(self) -> str:
        parts = [
            self.title, self.status, self.client_name, self.location,
            self.publish_date, self.closing_date, self.hours_text,
            self.description, self.category, self.updated_at_source,
        ]
        content = "|".join("" if p is None else str(p) for p in parts)
        return hashlib.sha256(content.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Export (CSV / XLSX)
# ---------------------------------------------------------------------------

def _parse_export(art: dict) -> list[VacatureRecord]:
    raw_bytes = art["raw_bytes"]
    content_type = art.get("content_type", "")

    # Dispatch on content_type only — artifact_id is a run-id string, not a filename.
    if "csv" in content_type:
        return _parse_csv_bytes(raw_bytes, art)
    if "excel" in content_type or "xlsx" in content_type or "spreadsheet" in content_type:
        return _parse_xlsx_bytes(raw_bytes, art)

    # Unknown content type (e.g. application/octet-stream) — try CSV first, then XLSX
    log.debug(
        "Export content_type=%r is ambiguous; trying CSV then XLSX.", content_type
    )
    try:
        return _parse_csv_bytes(raw_bytes, art)
    except Exception:
        return _parse_xlsx_bytes(raw_bytes, art)


def _parse_csv_bytes(raw_bytes: bytes, art: dict) -> list[VacatureRecord]:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("Cannot decode CSV with utf-8, utf-8-sig, or latin-1.")

    reader = csv.DictReader(io.StringIO(text))
    return [r for row in reader for r in [_map_flat_row(row, art)] if r]


def _parse_xlsx_bytes(raw_bytes: bytes, art: dict) -> list[VacatureRecord]:
    try:
        import openpyxl  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError("Install openpyxl: pip install openpyxl") from exc

    wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [str(h or "").strip() or f"col_{i}" for i, h in enumerate(rows[0])]
    return [r for row_vals in rows[1:] for r in [_map_flat_row(dict(zip(headers, row_vals)), art)] if r]


# ---------------------------------------------------------------------------
# XHR / JSON
# ---------------------------------------------------------------------------

def _parse_xhr_json(art: dict) -> list[VacatureRecord]:
    # Explicitly decode to str before parsing; json.loads(bytes) silently fails
    # for non-UTF-8 encodings that json.loads cannot auto-detect.
    raw_bytes: bytes = art["raw_bytes"]
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")
    payload = json.loads(text)
    return _records_from_value(payload, art)


def _records_from_value(value: Any, art: dict) -> list[VacatureRecord]:
    if not value:
        return []

    if isinstance(value, list):
        result = []
        for item in value:
            result.extend(_records_from_value(item, art))
        return result

    if isinstance(value, dict):
        # Salesforce Aura envelope: {"actions": [{"returnValue": ...}]}
        if "actions" in value:
            result = []
            for action in value["actions"]:
                rv = action.get("returnValue") if isinstance(action, dict) else None
                result.extend(_records_from_value(rv, art))
            # Salesforce Aura stores actual record data in
            # context.globalValueProviders[$Record], not in action returnValues.
            if not result:
                result.extend(_extract_aura_record_provider(value, art))
            return result

        # Salesforce REST API list: {"records": [...], "totalSize": N}
        if "records" in value and isinstance(value["records"], list):
            return [r for item in value["records"] for r in [_map_sf_record(item, art)] if r]

        # Single record heuristic
        if _is_vacature_dict(value):
            rec = _map_sf_record(value, art)
            return [rec] if rec else []

        # Nested — recurse into lists/dicts
        result = []
        for v in value.values():
            if isinstance(v, (list, dict)):
                result.extend(_records_from_value(v, art))
        return result

    return []


def _is_vacature_dict(d: dict) -> bool:
    """Heuristic: does this dict look like a single vacancy record?"""
    keys_lower = {k.lower().replace("__c", "").replace("_", "") for k in d}
    score = sum([
        bool(keys_lower & {"id", "vacatureid", "jobid", "requisitionid", "externalid"}),
        bool(keys_lower & {"title", "functietitel", "name", "jobtitle", "vacaturetitel"}),
        bool(keys_lower & {"status", "staat"}),
        bool(keys_lower & {"location", "locatie", "city", "stad"}),
        bool(keys_lower & {"description", "omschrijving", "jobdescription"}),
    ])
    return score >= 2


def _extract_aura_record_provider(payload: dict, art: dict) -> list[VacatureRecord]:
    """Extract records from a Salesforce Aura ``$Record`` globalValueProvider.

    Salesforce Lightning (Aura) list-view responses return record IDs in the
    action returnValues but store the actual field data in a separate section:
    ``context.globalValueProviders[type=$Record].values.records``.

    Each record is a dict keyed by object API name, wrapping a ``record``
    object that contains ``fields`` (each with ``{displayValue, value}``)
    and ``recordTypeInfo``.
    """
    ctx = payload.get("context")
    if not isinstance(ctx, dict):
        return []
    gvps = ctx.get("globalValueProviders")
    if not isinstance(gvps, list):
        return []

    for provider in gvps:
        if not isinstance(provider, dict):
            continue
        if provider.get("type") != "$Record":
            continue

        values = provider.get("values")
        if not isinstance(values, dict):
            continue
        records_dict = values.get("records")
        if not isinstance(records_dict, dict):
            continue

        result: list[VacatureRecord] = []
        for record_id, outer in records_dict.items():
            flat = _flatten_aura_record(record_id, outer)
            if flat:
                rec = _map_flat_row(flat, art)
                if rec:
                    result.append(rec)

        if result:
            log.info(
                "Extracted %d records from Aura $Record globalValueProvider.",
                len(result),
            )
            return result

    return []


def _flatten_aura_record(record_id: str, outer: dict) -> Optional[dict]:
    """Flatten a Salesforce Aura ``$Record`` entry to a simple key→value dict.

    The nesting is: ``{objectApiName: {record: {fields: {Name: {value: ...}}}}}``.
    We flatten ``{displayValue, value}`` pairs to just ``value`` and inject
    ``recordTypeInfo.name`` as the ``Status`` key so it feeds into the
    standard field mapping.
    """
    if not isinstance(outer, dict):
        return None

    for _obj_name, wrapper in outer.items():
        if not isinstance(wrapper, dict):
            continue
        record = wrapper.get("record")
        if not isinstance(record, dict):
            continue
        fields = record.get("fields")
        if not isinstance(fields, dict):
            continue

        # Flatten {field_name: {displayValue, value}} → {field_name: value}
        flat: dict[str, Any] = {}
        for fname, fdata in fields.items():
            if isinstance(fdata, dict):
                flat[fname] = fdata.get("value")
            else:
                flat[fname] = fdata

        # Inject recordTypeInfo.name as Status for the field mapper
        rti = record.get("recordTypeInfo")
        if isinstance(rti, dict) and rti.get("name"):
            flat["Status"] = rti["name"]

        return flat

    return None


# ---------------------------------------------------------------------------
# Field mapping
# ---------------------------------------------------------------------------

def _map_flat_row(row: dict, art: dict) -> Optional[VacatureRecord]:
    """Map a flat dict (CSV/XLSX row or SF record) to a VacatureRecord."""
    if not row:
        return None

    # Normalise keys: lowercase, strip Salesforce custom-field suffix __c, strip underscores
    norm = {
        re.sub(r"[_ ]+", "", k.lower().replace("__c", "")): v
        for k, v in row.items()
        if k is not None
    }

    def g(*keys: str) -> Any:
        for k in keys:
            v = norm.get(k)
            if v is not None and str(v).strip():
                return v
        return None

    vacature_id = g(
        "id", "vacatureid", "jobid", "requisitionid", "externalid",
        "referencenumber", "referentienummer", "nummer",
    )
    if not vacature_id:
        # Derive deterministic hash from stable identifying fields
        key_parts = [
            str(g("title", "functietitel", "name", "jobtitle") or ""),
            str(g("location", "locatie", "city") or ""),
            str(g("publishdate", "publicatiedatum", "startdate", "createddate") or ""),
        ]
        vacature_id = "hash_" + hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:20]

    title = g("title", "functietitel", "name", "jobtitle", "vacaturetitel")
    status = g("status", "staat", "state")
    client_name = g(
        "clientname", "klant", "client", "company", "companytext",
        "accountname", "opdrachtgever", "account",
    )
    location = g("location", "locatie", "city", "stad", "worksite", "plaats")
    publish_date = _parse_date(g("publishdate", "publicatiedatum", "startdate", "opendate", "createddate"))
    closing_date = _parse_date(g("closingdate", "sluitingsdatum", "enddate", "expirationdate", "duedate", "finalproposaldate"))
    hours_text = _to_str(g("hours", "uren", "hoursperweek", "workingtime", "contracthours", "fte"))
    hours = _parse_hours(hours_text)
    description = g("description", "omschrijving", "jobdescription", "functieomschrijving", "tekst")
    category = g("category", "categorie", "sector", "functiegroep", "discipline", "type")
    updated_at_source = _parse_datetime(
        g("lastmodifieddate", "systemmodstamp", "updatedat", "modifieddate", "modified")
    )

    status_lower = (status or "").lower().strip()
    is_active: Optional[bool] = None
    if status_lower:
        is_active = status_lower not in _INACTIVE_STATUSES
    if closing_date:
        try:
            cd = date.fromisoformat(closing_date)
            if cd < date.today():
                is_active = False
        except (ValueError, TypeError):
            pass

    return VacatureRecord(
        vacature_id=str(vacature_id),
        title=_to_str(title),
        status=_to_str(status),
        client_name=_to_str(client_name),
        location=_to_str(location),
        publish_date=publish_date,
        closing_date=closing_date,
        hours=hours,
        hours_text=hours_text,
        description=_to_str(description, max_len=10_000),
        category=_to_str(category),
        updated_at_source=updated_at_source,
        source_url=art.get("url", ""),
        is_active=is_active,
    )


def _map_sf_record(record: dict, art: dict) -> Optional[VacatureRecord]:
    """Map a Salesforce-style record (may have nested 'attributes' key)."""
    if not record:
        return None
    # Flatten: merge top-level and any first-level nested dicts
    flat: dict = {}
    for k, v in record.items():
        if k == "attributes":
            continue
        if isinstance(v, dict) and not _is_vacature_dict(v):
            # Flatten one level of nesting (e.g. Account.Name → accountname)
            for nk, nv in v.items():
                if nk != "attributes":
                    flat[f"{k}_{nk}"] = nv
        else:
            flat[k] = v
    return _map_flat_row(flat, art)


# ---------------------------------------------------------------------------
# DOM HTML fallback
# ---------------------------------------------------------------------------

def _parse_dom_html(art: dict) -> list[VacatureRecord]:
    try:
        from bs4 import BeautifulSoup  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError("beautifulsoup4 is required (already in requirements).") from exc

    html = art["raw_bytes"].decode("utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    # Try structured table first
    records = _parse_html_tables(soup, art)
    if records:
        return records

    # Try card / list-item patterns
    return _parse_html_cards(soup, art)


def _parse_html_tables(soup, art: dict) -> list[VacatureRecord]:
    records = []
    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [
            (th.get_text(strip=True) or f"col_{i}").lower()
            for i, th in enumerate(header_row.find_all(["th", "td"]))
        ]
        if not any(kw in " ".join(headers) for kw in ["vacatur", "titel", "title", "job", "functie"]):
            continue
        kept = 0
        skipped_no_id = 0
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if not cells:
                continue
            row_dict = dict(zip(headers, cells))
            rec = _map_flat_row(row_dict, art)
            if rec and not rec.vacature_id.startswith("hash_"):
                records.append(rec)
                kept += 1
            else:
                skipped_no_id += 1
        if skipped_no_id:
            log.warning(
                "HTML table parser: discarded %d row(s) with no stable ID from %s. "
                "The portal HTML may not expose an explicit vacancy ID column.",
                skipped_no_id, art.get("url", "unknown"),
            )
    return records


def _parse_html_cards(soup, art: dict) -> list[VacatureRecord]:
    card_selectors = [
        "[class*='vacature']", "[class*='job-card']",
        "[class*='listing-item']", "[class*='requisition']",
        "article", "li.slds-item",
    ]
    for selector in card_selectors:
        cards = soup.select(selector)
        if len(cards) < 2:
            continue
        records = []
        for card in cards:
            rec = _card_to_record(card, art)
            if rec:
                records.append(rec)
        if records:
            return records
    return []


def _card_to_record(card, art: dict) -> Optional[VacatureRecord]:
    # Try to extract a Salesforce record ID from the card's link
    link = card.find("a", href=True)
    href = (link.get("href") or "") if link else ""
    sf_id_match = re.search(r"/([A-Za-z0-9]{15,18})(?:/|$)", href)
    vacature_id = sf_id_match.group(1) if sf_id_match else None

    headings = card.find_all(re.compile(r"^h[1-6]$"))
    title = next((h.get_text(strip=True) for h in headings if h.get_text(strip=True)), None)

    if not vacature_id:
        if not title:
            return None
        vacature_id = "hash_" + hashlib.sha256(title.encode()).hexdigest()[:20]

    all_text = card.get_text(" ", strip=True)

    return VacatureRecord(
        vacature_id=vacature_id,
        title=title,
        description=all_text[:5_000] if all_text else None,
        source_url=art.get("url", ""),
    )


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------

def _to_str(v: Any, max_len: int = 0) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if max_len and len(s) > max_len:
        s = s[:max_len]
    return s


def _parse_date(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # Salesforce ISO datetime → date part
    if "T" in s:
        s = s[:10]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
    return None


def _parse_datetime(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # Salesforce: 2024-01-15T12:34:56.000+0000
    s_trimmed = re.sub(r"\.\d+", "", s).replace("Z", "")
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s_trimmed, fmt).replace(tzinfo=None).isoformat()
        except (ValueError, TypeError):
            pass
    return None


def _parse_hours(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    numbers = re.findall(r"\d+(?:\.\d+)?", s)
    if not numbers:
        return None
    if len(numbers) >= 2:
        return (float(numbers[0]) + float(numbers[1])) / 2
    return float(numbers[0])
