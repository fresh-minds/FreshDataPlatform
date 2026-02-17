from __future__ import annotations

import os
from typing import Dict, List

from shared.job_connectors.connectors import BaseConnector, GenericRssConnector, GenericSitemapConnector
from shared.job_connectors.env import bool_env, int_env, parse_csv


def enabled_connector_names() -> List[str]:
    return [name.lower() for name in parse_csv(os.getenv("JOB_CONNECTORS_ENABLED", "rss,sitemap"))]


def available_connectors() -> Dict[str, BaseConnector]:
    redact_pii = bool_env("JOB_CONNECTORS_REDACT_PII", True)

    rss = GenericRssConnector(
        feed_urls=parse_csv(os.getenv("CONNECTOR_RSS_FEED_URLS")),
        fetch_details=bool_env("CONNECTOR_RSS_FETCH_DETAILS", False),
        max_items_per_feed=int_env("CONNECTOR_RSS_MAX_ITEMS_PER_FEED", 200),
        redact_pii_enabled=redact_pii,
    )

    include_patterns = parse_csv(os.getenv("CONNECTOR_SITEMAP_INCLUDE_PATTERNS")) or [
        r"jobs?",
        r"careers?",
        r"vacancies",
        r"vacature",
        r"werken-bij",
    ]

    sitemap = GenericSitemapConnector(
        sitemap_urls=parse_csv(os.getenv("CONNECTOR_SITEMAP_URLS")),
        include_url_patterns=include_patterns,
        exclude_url_patterns=parse_csv(os.getenv("CONNECTOR_SITEMAP_EXCLUDE_PATTERNS")),
        fetch_details=bool_env("CONNECTOR_SITEMAP_FETCH_DETAILS", True),
        max_urls=int_env("CONNECTOR_SITEMAP_MAX_URLS", 1000),
        max_sitemaps=int_env("CONNECTOR_SITEMAP_MAX_SITEMAPS", 50),
        redact_pii_enabled=redact_pii,
    )

    return {"rss": rss, "sitemap": sitemap}


def build_enabled_connectors() -> List[BaseConnector]:
    registry = available_connectors()
    enabled = enabled_connector_names()
    out: List[BaseConnector] = []
    for name in enabled:
        c = registry.get(name)
        if c is not None:
            out.append(c)
    return out
