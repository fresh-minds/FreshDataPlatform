from __future__ import annotations

import hashlib
import html as html_lib
import json
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlsplit, urlunsplit

from shared.job_connectors.models import EmploymentType, NormalizedJob, RawJobPayload, RemotePolicy

try:  # Optional dependency; we fall back to regex parsing if unavailable.
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover
    BeautifulSoup = None


_EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
_PHONE_RE = re.compile(r"(?<!\w)(?:\+?\d[\d\s\-().]{7,}\d)(?!\w)")


def canonicalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    # Drop fragments for dedupe stability.
    parts = parts._replace(fragment="")
    return urlunsplit(parts)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def redact_pii(text: str) -> str:
    if not text:
        return text
    text = _EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    text = _PHONE_RE.sub("[REDACTED_PHONE]", text)
    return text


def html_to_text(html: str) -> str:
    if not html:
        return ""

    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(" ", strip=True)
        return normalize_whitespace(html_lib.unescape(text))

    # Minimal fallback.
    cleaned = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\\1>", " ", html)
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    return normalize_whitespace(html_lib.unescape(cleaned))


def truncate(text: Optional[str], max_len: int = 50_000) -> Optional[str]:
    if text is None:
        return None
    if len(text) <= max_len:
        return text
    return text[:max_len]


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None

    # RFC 822 / RSS <pubDate>
    try:
        dt = parsedate_to_datetime(value)
        if dt is not None:
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt
    except (TypeError, ValueError):
        pass

    # ISO-ish
    iso = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _iter_jsonld_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        graph = obj.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                yield from _iter_jsonld_dicts(item)
        for v in obj.values():
            yield from _iter_jsonld_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_jsonld_dicts(item)


def _is_jobposting(d: Dict[str, Any]) -> bool:
    t = d.get("@type")
    if isinstance(t, str):
        types = [t]
    elif isinstance(t, list):
        types = [x for x in t if isinstance(x, str)]
    else:
        types = []
    return any(x.lower() == "jobposting" for x in types)


def extract_jobposting_jsonld(html: str) -> Optional[Dict[str, Any]]:
    if not html:
        return None

    scripts = re.findall(
        r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
    )
    for raw in scripts:
        candidate = raw.strip()
        if not candidate:
            continue
        candidate = html_lib.unescape(candidate)
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue

        for d in _iter_jsonld_dicts(parsed):
            if _is_jobposting(d):
                return d
    return None


def _jsonld_get_str(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        name = value.get("name")
        if isinstance(name, str):
            return name
    return None


def _jsonld_extract_company(jobposting: Dict[str, Any]) -> Optional[str]:
    org = jobposting.get("hiringOrganization") or jobposting.get("hiringOrganization", {})
    if isinstance(org, dict):
        name = org.get("name")
        if isinstance(name, str):
            return normalize_whitespace(name)
    return None


def _jsonld_extract_location(jobposting: Dict[str, Any]) -> Optional[str]:
    loc = jobposting.get("jobLocation")
    locs: List[Any]
    if isinstance(loc, list):
        locs = loc
    elif loc is None:
        locs = []
    else:
        locs = [loc]

    parts: List[str] = []
    for item in locs:
        if not isinstance(item, dict):
            continue
        addr = item.get("address")
        if not isinstance(addr, dict):
            continue
        # Prefer locality/region/country; street tends to be overly specific.
        for key in ("addressLocality", "addressRegion", "addressCountry"):
            v = addr.get(key)
            if isinstance(v, str) and v.strip():
                parts.append(v.strip())

    cleaned = normalize_whitespace(", ".join(parts))
    return cleaned or None


def _extract_title_from_html(html: str) -> Optional[str]:
    if not html:
        return None

    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return normalize_whitespace(h1.get_text(" ", strip=True))
        title = soup.find("title")
        if title and title.get_text(strip=True):
            return normalize_whitespace(title.get_text(" ", strip=True))
        return None

    m = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", html)
    if m:
        return normalize_whitespace(html_to_text(m.group(1)))
    m = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
    if m:
        return normalize_whitespace(html_to_text(m.group(1)))
    return None


def _extract_description_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    text = html_to_text(html)
    return truncate(text) or None


def infer_employment_type(text: Optional[str], jsonld: Optional[Dict[str, Any]] = None) -> EmploymentType:
    tokens: List[str] = []
    if jsonld is not None:
        et = jsonld.get("employmentType")
        if isinstance(et, str):
            tokens.append(et)
        elif isinstance(et, list):
            tokens.extend([x for x in et if isinstance(x, str)])

    if text:
        tokens.append(text)

    blob = " ".join(tokens).lower()

    if any(k in blob for k in ("freelance", "zzp", "self-employed")):
        return "freelance"
    if any(k in blob for k in ("contract", "contractor", "temporary", "interim", "fixed-term")):
        return "contract"
    if any(
        k in blob
        for k in (
            "permanent",
            "full-time",
            "full time",
            "full_time",
            "part-time",
            "part time",
            "part_time",
        )
    ):
        return "perm"
    return "unknown"


def infer_remote_policy(text: Optional[str], jsonld: Optional[Dict[str, Any]] = None) -> RemotePolicy:
    if jsonld is not None:
        jlt = jsonld.get("jobLocationType")
        if isinstance(jlt, str) and jlt.upper() == "TELECOMMUTE":
            return "remote"

    blob = (text or "").lower()
    if "hybrid" in blob:
        return "hybrid"
    if any(k in blob for k in ("remote", "work from home", "home-based", "telecommute")):
        return "remote"
    if any(k in blob for k in ("on-site", "onsite", "in office", "on site")):
        return "onsite"
    return "unknown"


def _checksum_fields(
    *,
    source: str,
    source_job_id: str,
    url: str,
    title: str,
    company: Optional[str],
    location_text: Optional[str],
    description: Optional[str],
    published_at: Optional[datetime],
) -> str:
    parts = [
        source,
        source_job_id,
        canonicalize_url(url),
        title,
        company or "",
        location_text or "",
        description or "",
        published_at.isoformat() if published_at else "",
    ]
    blob = "\n".join(parts).encode("utf-8", errors="replace")
    return hashlib.sha256(blob).hexdigest()


def normalize_raw_payload(
    raw: RawJobPayload,
    *,
    fallback_title: Optional[str] = None,
    fallback_description: Optional[str] = None,
    fallback_published_at: Optional[datetime] = None,
    redact_pii_enabled: bool = True,
) -> Optional[NormalizedJob]:
    """
    Normalize a RawJobPayload into a NormalizedJob.

    - Prefers JobPosting JSON-LD when present.
    - Falls back to RSS item metadata or simple HTML heuristics.
    """

    body = raw.body or ""
    jsonld = extract_jobposting_jsonld(body)

    title = None
    company = None
    location_text = None
    description = None
    published_at = None

    if jsonld is not None:
        title = _jsonld_get_str(jsonld.get("title")) or _jsonld_get_str(jsonld.get("name"))
        if title:
            title = normalize_whitespace(title)

        company = _jsonld_extract_company(jsonld)
        location_text = _jsonld_extract_location(jsonld)

        desc = jsonld.get("description")
        if isinstance(desc, str) and desc.strip():
            description = truncate(html_to_text(desc))

        published_at = parse_datetime(_jsonld_get_str(jsonld.get("datePosted")))

    if not title:
        title = normalize_whitespace(fallback_title or "") or _extract_title_from_html(body)

    if description is None:
        description = truncate(html_to_text(fallback_description or "")) if fallback_description else None
        if description is None and body:
            description = _extract_description_from_html(body)

    if published_at is None:
        published_at = fallback_published_at

    if not title:
        return None

    if description:
        description = normalize_whitespace(description)
        if redact_pii_enabled:
            description = redact_pii(description)

    if company:
        company = normalize_whitespace(company)
    if location_text:
        location_text = normalize_whitespace(location_text)

    employment_type = infer_employment_type(description, jsonld=jsonld)
    remote_policy = infer_remote_policy(description, jsonld=jsonld)

    checksum = _checksum_fields(
        source=raw.source,
        source_job_id=raw.source_job_id,
        url=raw.url,
        title=title,
        company=company,
        location_text=location_text,
        description=description,
        published_at=published_at,
    )

    return NormalizedJob(
        source=raw.source,
        source_job_id=raw.source_job_id,
        url=canonicalize_url(raw.url),
        title=title,
        company=company,
        location_text=location_text,
        description=description,
        published_at=published_at,
        employment_type=employment_type,
        remote_policy=remote_policy,
        checksum=checksum,
    )
