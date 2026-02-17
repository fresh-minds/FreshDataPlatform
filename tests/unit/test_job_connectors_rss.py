from __future__ import annotations

from pathlib import Path

import httpx

from shared.job_connectors.connectors.rss import GenericRssConnector
from shared.job_connectors.http import PoliteHttpClient


def _fixture(path: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "job_connectors"
    return (base / path).read_text(encoding="utf-8")


def test_rss_connector_parses_and_normalizes_snippet_with_pii_redaction():
    feed_url = "https://example.com/jobs/rss.xml"
    feed_xml = _fixture("rss/sample_rss.xml")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == feed_url:
            return httpx.Response(200, text=feed_xml, headers={"Content-Type": "application/rss+xml"})
        return httpx.Response(404, text="not found")

    transport = httpx.MockTransport(handler)

    connector = GenericRssConnector(
        feed_urls=[feed_url],
        fetch_details=False,
        redact_pii_enabled=True,
    )

    http = PoliteHttpClient(
        user_agent="TestAgent/1.0",
        require_allowlist=True,
        allowlist_hosts=["example.com"],
        robots_strict=True,
        rate_limit_per_host_s=0.0,
        max_retries=0,
        transport=transport,
        sleep=lambda _s: None,
    )
    try:
        refs = connector.list_jobs(http, run_id="test")
        assert len(refs) == 1
        ref = refs[0]
        assert ref.source == "example.com"
        assert ref.source_job_id == "job-123"
        assert ref.url == "https://example.com/jobs/123"

        raw = connector.fetch_job_details(http, run_id="test", job_ref=ref)
        assert raw is not None

        job = connector.normalize(raw, run_id="test")
        assert job is not None
        assert job.title == "Data Engineer"
        assert job.source == "example.com"
        assert job.url == "https://example.com/jobs/123"
        assert job.remote_policy == "remote"
        assert job.description is not None
        assert "hr@example.com" not in job.description
        assert "[REDACTED_EMAIL]" in job.description
        assert job.checksum
    finally:
        http.close()


def test_rss_connector_falls_back_when_robots_disallows_detail_fetch():
    feed_url = "https://example.com/jobs/rss.xml"
    feed_xml = _fixture("rss/sample_rss.xml")
    robots_txt = _fixture("rss/robots_disallow_jobs.txt")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://example.com/robots.txt":
            return httpx.Response(200, text=robots_txt, headers={"Content-Type": "text/plain"})
        if url == feed_url:
            return httpx.Response(200, text=feed_xml, headers={"Content-Type": "application/rss+xml"})
        # Detail page will be requested but should be blocked by robots.
        if url == "https://example.com/jobs/123":
            return httpx.Response(
                200, text="<html><h1>Should not fetch</h1></html>", headers={"Content-Type": "text/html"}
            )
        return httpx.Response(404, text="not found")

    connector = GenericRssConnector(feed_urls=[feed_url], fetch_details=True, redact_pii_enabled=True)

    http = PoliteHttpClient(
        user_agent="TestAgent/1.0",
        require_allowlist=True,
        allowlist_hosts=["example.com"],
        robots_strict=True,
        rate_limit_per_host_s=0.0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
        sleep=lambda _s: None,
    )
    try:
        refs = connector.list_jobs(http, run_id="test")
        ref = refs[0]
        raw = connector.fetch_job_details(http, run_id="test", job_ref=ref)
        assert raw is not None
        # We should not have fetched HTML; content remains feed snippet.
        assert raw.metadata.get("detail_fetch_blocked") == "robots"
        job = connector.normalize(raw, run_id="test")
        assert job is not None
        assert job.title == "Data Engineer"
    finally:
        http.close()
