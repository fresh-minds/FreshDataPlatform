from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

from shared.job_connectors.connectors.base import BaseConnector
from shared.job_connectors.http import FetchError, PoliteHttpClient, RobotsDisallowedError
from shared.job_connectors.models import JobRef, NormalizedJob, RawJobPayload
from shared.job_connectors.normalize import canonicalize_url, normalize_raw_payload, parse_datetime


def _local(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _sha1(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="replace")).hexdigest()


def _host_from_url(url: str) -> str:
    return (urlsplit(url).hostname or "unknown").lower()


def _iter_sitemap_locs(xml_text: str) -> Iterable[Dict[str, Any]]:
    root = ET.fromstring(xml_text)
    root_name = _local(root.tag).lower()

    if root_name == "sitemapindex":
        for sitemap in list(root):
            if _local(sitemap.tag).lower() != "sitemap":
                continue
            loc = None
            lastmod = None
            for child in list(sitemap):
                name = _local(child.tag).lower()
                if name == "loc" and child.text:
                    loc = child.text.strip()
                elif name == "lastmod" and child.text:
                    lastmod = child.text.strip()
            if loc:
                yield {"type": "sitemap", "loc": loc, "lastmod": lastmod}
        return

    if root_name == "urlset":
        for url_el in list(root):
            if _local(url_el.tag).lower() != "url":
                continue
            loc = None
            lastmod = None
            for child in list(url_el):
                name = _local(child.tag).lower()
                if name == "loc" and child.text:
                    loc = child.text.strip()
                elif name == "lastmod" and child.text:
                    lastmod = child.text.strip()
            if loc:
                yield {"type": "url", "loc": loc, "lastmod": lastmod}
        return

    # Unknown root
    return


class GenericSitemapConnector(BaseConnector):
    def __init__(
        self,
        *,
        sitemap_urls: List[str],
        include_url_patterns: Optional[List[str]] = None,
        exclude_url_patterns: Optional[List[str]] = None,
        fetch_details: bool = True,
        max_urls: int = 1000,
        max_sitemaps: int = 50,
        redact_pii_enabled: bool = True,
    ) -> None:
        self._sitemap_urls = [u.strip() for u in sitemap_urls if u and u.strip()]
        self._include_re = [re.compile(p, flags=re.IGNORECASE) for p in (include_url_patterns or []) if p]
        self._exclude_re = [re.compile(p, flags=re.IGNORECASE) for p in (exclude_url_patterns or []) if p]
        self._fetch_details = bool(fetch_details)
        self._max_urls = max(1, int(max_urls))
        self._max_sitemaps = max(1, int(max_sitemaps))
        self._redact_pii_enabled = bool(redact_pii_enabled)

    @property
    def name(self) -> str:
        return "sitemap"

    def _fallback_metadata(self, job_ref: JobRef) -> Dict[str, Any]:
        return {
            "fallback_title": job_ref.title,
            "fallback_description": None,
            "fallback_published_at": job_ref.published_at.isoformat() if job_ref.published_at else None,
        }

    def _url_allowed(self, url: str) -> bool:
        if self._exclude_re and any(r.search(url) for r in self._exclude_re):
            return False
        if not self._include_re:
            return True
        return any(r.search(url) for r in self._include_re)

    def list_jobs(self, http: PoliteHttpClient, *, run_id: str) -> List[JobRef]:
        refs: List[JobRef] = []
        seen_sitemaps: Set[str] = set()
        queue: List[str] = list(self._sitemap_urls)

        while queue and len(seen_sitemaps) < self._max_sitemaps and len(refs) < self._max_urls:
            sitemap_url = queue.pop(0)
            if sitemap_url in seen_sitemaps:
                continue
            seen_sitemaps.add(sitemap_url)

            try:
                result = http.get_text(sitemap_url, check_robots=True)
            except (FetchError, RobotsDisallowedError):
                continue

            for item in _iter_sitemap_locs(result.text):
                if item.get("type") == "sitemap":
                    loc = item.get("loc")
                    if isinstance(loc, str) and loc not in seen_sitemaps:
                        queue.append(loc)
                    continue

                loc = item.get("loc")
                if not isinstance(loc, str) or not loc:
                    continue
                url = canonicalize_url(loc)
                if not self._url_allowed(url):
                    continue

                source = _host_from_url(url)
                source_job_id = _sha1(url)
                published_at = parse_datetime(item.get("lastmod"))
                refs.append(
                    JobRef(
                        source=source,
                        source_job_id=source_job_id,
                        url=url,
                        title=None,
                        published_at=published_at,
                        extra={"sitemap_url": sitemap_url},
                    )
                )

                if len(refs) >= self._max_urls:
                    break

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
            metadata = self._fallback_metadata(job_ref)
            metadata["detail_fetch_skipped"] = True
            return RawJobPayload(
                source=job_ref.source,
                source_job_id=job_ref.source_job_id,
                url=job_ref.url,
                fetched_at=fetched_at,
                body="",
                status_code=None,
                content_type="text/html",
                metadata=metadata,
            )

        try:
            result = http.get_text(job_ref.url, check_robots=True)
        except (RobotsDisallowedError, FetchError):
            return None

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
        fallback_pub = parse_datetime(raw.metadata.get("fallback_published_at"))
        return normalize_raw_payload(
            raw,
            fallback_title=str(fallback_title) if fallback_title else None,
            fallback_description=None,
            fallback_published_at=fallback_pub,
            redact_pii_enabled=self._redact_pii_enabled,
        )
