from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree as ET

from shared.job_connectors.connectors.base import BaseConnector
from shared.job_connectors.http import FetchError, PoliteHttpClient, RobotsDisallowedError
from shared.job_connectors.models import JobRef, NormalizedJob, RawJobPayload
from shared.job_connectors.normalize import canonicalize_url, normalize_raw_payload, parse_datetime


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    value = elem.text.strip()
    return value or None


def _sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="replace")).hexdigest()


def _host_from_url(url: str) -> str:
    return (urlsplit(url).hostname or "unknown").lower()


def _parse_rss_items(feed_xml: str, *, feed_url: str) -> List[Dict[str, Any]]:
    root = ET.fromstring(feed_xml)
    root_name = _local(root.tag).lower()

    if root_name == "feed":  # Atom
        return _parse_atom_entries(root, feed_url=feed_url)

    # RSS 2.0 typically: <rss><channel><item>...</item></channel></rss>
    channel = None
    for child in root:
        if _local(child.tag).lower() == "channel":
            channel = child
            break
    if channel is None:
        return []

    items: List[Dict[str, Any]] = []
    for item in channel:
        if _local(item.tag).lower() != "item":
            continue
        title = _text(item.find("./title"))
        link = _text(item.find("./link"))
        guid = _text(item.find("./guid"))
        pub_date = _text(item.find("./pubDate"))

        # Some feeds provide description + content:encoded.
        description = _text(item.find("./description"))
        if not description:
            for child in item:
                if _local(child.tag).lower() == "encoded":  # content:encoded
                    description = _text(child)
                    break

        if link:
            link = urljoin(feed_url, link)
        items.append(
            {
                "title": title,
                "link": link,
                "guid": guid,
                "published": pub_date,
                "description": description,
            }
        )

    return items


def _parse_atom_entries(root: ET.Element, *, feed_url: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for entry in list(root):
        if _local(entry.tag).lower() != "entry":
            continue

        title = None
        entry_id = None
        link = None
        published = None
        summary = None
        content = None

        for child in list(entry):
            name = _local(child.tag).lower()
            if name == "title":
                title = _text(child)
            elif name == "id":
                entry_id = _text(child)
            elif name == "link":
                href = child.attrib.get("href")
                rel = (child.attrib.get("rel") or "").lower()
                if href and (rel in {"", "alternate"}):
                    link = href
            elif name in {"published", "updated"} and not published:
                published = _text(child)
            elif name == "summary":
                summary = _text(child)
            elif name == "content":
                content = _text(child)

        if link:
            link = urljoin(feed_url, link)

        entries.append(
            {
                "title": title,
                "link": link,
                "guid": entry_id,
                "published": published,
                "description": content or summary,
            }
        )
    return entries


class GenericRssConnector(BaseConnector):
    def __init__(
        self,
        *,
        feed_urls: List[str],
        fetch_details: bool = False,
        max_items_per_feed: int = 200,
        redact_pii_enabled: bool = True,
    ) -> None:
        self._feed_urls = [u.strip() for u in feed_urls if u and u.strip()]
        self._fetch_details = bool(fetch_details)
        self._max_items_per_feed = max(1, int(max_items_per_feed))
        self._redact_pii_enabled = bool(redact_pii_enabled)

    @property
    def name(self) -> str:
        return "rss"

    def _fallback_metadata(self, job_ref: JobRef) -> Dict[str, Any]:
        return {
            "fallback_title": job_ref.title,
            "fallback_description": job_ref.extra.get("item_description"),
            "fallback_published_at": job_ref.published_at.isoformat() if job_ref.published_at else None,
        }

    def _fallback_payload(
        self,
        job_ref: JobRef,
        *,
        fetched_at: datetime,
        status_code: Optional[int] = None,
        metadata_updates: Optional[Dict[str, Any]] = None,
    ) -> RawJobPayload:
        metadata = self._fallback_metadata(job_ref)
        if metadata_updates:
            metadata.update(metadata_updates)
        return RawJobPayload(
            source=job_ref.source,
            source_job_id=job_ref.source_job_id,
            url=job_ref.url,
            fetched_at=fetched_at,
            body=str(job_ref.extra.get("item_description") or ""),
            status_code=status_code,
            content_type="application/rss+xml;item",
            metadata=metadata,
        )

    def list_jobs(self, http: PoliteHttpClient, *, run_id: str) -> List[JobRef]:
        refs: List[JobRef] = []
        for feed_url in self._feed_urls:
            result = http.get_text(feed_url, check_robots=True)
            items = _parse_rss_items(result.text, feed_url=feed_url)
            for item in items[: self._max_items_per_feed]:
                url = item.get("link")
                if not url:
                    continue
                url = canonicalize_url(url)
                source = _host_from_url(url)
                source_job_id = item.get("guid") or _sha1(url)
                title = item.get("title")
                published_at = parse_datetime(item.get("published"))
                refs.append(
                    JobRef(
                        source=source,
                        source_job_id=str(source_job_id),
                        url=url,
                        title=title,
                        published_at=published_at,
                        extra={
                            "feed_url": feed_url,
                            "item_description": item.get("description"),
                        },
                    )
                )
        return refs

    def fetch_job_details(
        self,
        http: PoliteHttpClient,
        *,
        run_id: str,
        job_ref: JobRef,
    ) -> Optional[RawJobPayload]:
        fetched_at = datetime.now(timezone.utc)

        if not self._fetch_details:
            return self._fallback_payload(job_ref, fetched_at=fetched_at)

        try:
            result = http.get_text(job_ref.url, check_robots=True)
        except RobotsDisallowedError:
            # Fall back to RSS snippet if detail fetch is disallowed.
            return self._fallback_payload(
                job_ref,
                fetched_at=fetched_at,
                metadata_updates={"detail_fetch_blocked": "robots"},
            )
        except FetchError as e:
            # Fall back to RSS snippet for transient/HTTP failures as well.
            return self._fallback_payload(
                job_ref,
                fetched_at=fetched_at,
                status_code=e.status_code,
                metadata_updates={"detail_fetch_failed": True},
            )

        return RawJobPayload(
            source=job_ref.source,
            source_job_id=job_ref.source_job_id,
            url=result.url,
            fetched_at=fetched_at,
            body=result.text,
            status_code=result.status_code,
            content_type=result.content_type,
            metadata=self._fallback_metadata(job_ref),
        )

    def normalize(self, raw: RawJobPayload, *, run_id: str) -> Optional[NormalizedJob]:
        fallback_title = raw.metadata.get("fallback_title")
        fallback_desc = raw.metadata.get("fallback_description")
        fallback_pub = parse_datetime(raw.metadata.get("fallback_published_at"))
        return normalize_raw_payload(
            raw,
            fallback_title=str(fallback_title) if fallback_title else None,
            fallback_description=str(fallback_desc) if fallback_desc else None,
            fallback_published_at=fallback_pub,
            redact_pii_enabled=self._redact_pii_enabled,
        )
