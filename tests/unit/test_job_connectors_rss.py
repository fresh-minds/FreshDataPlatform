from __future__ import annotations

from pathlib import Path
from xml.etree.ElementTree import ParseError

import httpx
import pytest

from shared.job_connectors.connectors.rss import GenericRssConnector
from shared.job_connectors.http import FetchError, PoliteHttpClient


def _fixture(path: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "job_connectors"
    return (base / path).read_text(encoding="utf-8")


def _make_http(handler, allowlist_hosts=("example.com",)):
    return PoliteHttpClient(
        user_agent="TestAgent/1.0",
        require_allowlist=True,
        allowlist_hosts=list(allowlist_hosts),
        robots_strict=True,
        rate_limit_per_host_s=0.0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
        sleep=lambda _s: None,
    )


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

    connector = GenericRssConnector(
        feed_urls=[feed_url],
        fetch_details=False,
        redact_pii_enabled=True,
    )

    http = _make_http(handler)
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
        if url == "https://example.com/jobs/123":
            return httpx.Response(
                200, text="<html><h1>Should not fetch</h1></html>", headers={"Content-Type": "text/html"}
            )
        return httpx.Response(404, text="not found")

    connector = GenericRssConnector(feed_urls=[feed_url], fetch_details=True, redact_pii_enabled=True)

    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        ref = refs[0]
        raw = connector.fetch_job_details(http, run_id="test", job_ref=ref)
        assert raw is not None
        assert raw.metadata.get("detail_fetch_blocked") == "robots"
        job = connector.normalize(raw, run_id="test")
        assert job is not None
        assert job.title == "Data Engineer"
    finally:
        http.close()


def test_rss_connector_returns_empty_list_for_empty_feed():
    """A valid RSS feed with no items should yield an empty job list."""
    feed_url = "https://example.com/jobs/rss.xml"
    empty_feed = '<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>'

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == feed_url:
            return httpx.Response(200, text=empty_feed, headers={"Content-Type": "application/rss+xml"})
        return httpx.Response(404, text="not found")

    connector = GenericRssConnector(feed_urls=[feed_url], fetch_details=False)
    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        assert refs == []
    finally:
        http.close()


def test_rss_connector_raises_fetch_error_on_http_500():
    """A 5xx response from the feed URL should raise FetchError from list_jobs."""
    feed_url = "https://example.com/jobs/rss.xml"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == feed_url:
            return httpx.Response(500, text="Internal Server Error")
        return httpx.Response(404, text="not found")

    connector = GenericRssConnector(feed_urls=[feed_url], fetch_details=False)
    http = _make_http(handler)
    try:
        with pytest.raises(FetchError) as exc_info:
            connector.list_jobs(http, run_id="test")
        assert exc_info.value.status_code == 500
    finally:
        http.close()


def test_rss_connector_raises_on_malformed_xml():
    """Malformed XML in the feed body should raise a ParseError from list_jobs."""
    feed_url = "https://example.com/jobs/rss.xml"
    bad_xml = "this is not xml <<< broken"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == feed_url:
            return httpx.Response(200, text=bad_xml, headers={"Content-Type": "application/rss+xml"})
        return httpx.Response(404, text="not found")

    connector = GenericRssConnector(feed_urls=[feed_url], fetch_details=False)
    http = _make_http(handler)
    try:
        with pytest.raises(ParseError):
            connector.list_jobs(http, run_id="test")
    finally:
        http.close()


def test_rss_connector_fetch_details_falls_back_on_http_error():
    """An HTTP error on the detail page should fall back to the RSS snippet."""
    feed_url = "https://example.com/jobs/rss.xml"
    feed_xml = _fixture("rss/sample_rss.xml")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == feed_url:
            return httpx.Response(200, text=feed_xml, headers={"Content-Type": "application/rss+xml"})
        if url == "https://example.com/jobs/123":
            return httpx.Response(404, text="Not Found")
        return httpx.Response(404, text="not found")

    connector = GenericRssConnector(feed_urls=[feed_url], fetch_details=True, redact_pii_enabled=False)
    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        ref = refs[0]
        raw = connector.fetch_job_details(http, run_id="test", job_ref=ref)
        assert raw is not None
        assert raw.metadata.get("detail_fetch_failed") is True
        job = connector.normalize(raw, run_id="test")
        assert job is not None
        assert job.title == "Data Engineer"
    finally:
        http.close()


def test_rss_connector_skips_items_without_link():
    """Items missing a <link> element should be silently skipped."""
    feed_url = "https://example.com/jobs/rss.xml"
    feed_xml = """<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <item><title>No Link Job</title><guid>no-link</guid></item>
    <item>
      <title>Has Link</title>
      <link>https://example.com/jobs/999</link>
      <guid>job-999</guid>
    </item>
  </channel>
</rss>"""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == feed_url:
            return httpx.Response(200, text=feed_xml, headers={"Content-Type": "application/rss+xml"})
        return httpx.Response(404, text="not found")

    connector = GenericRssConnector(feed_urls=[feed_url], fetch_details=False)
    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        assert len(refs) == 1
        assert refs[0].source_job_id == "job-999"
    finally:
        http.close()
