from __future__ import annotations

from pathlib import Path
from xml.etree.ElementTree import ParseError

import httpx
import pytest

from shared.job_connectors.connectors.sitemap import GenericSitemapConnector
from shared.job_connectors.http import FetchError, PoliteHttpClient


def _fixture(path: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "job_connectors"
    return (base / path).read_text(encoding="utf-8")


def _make_http(handler, allowlist_hosts=("careers.example.com",)):
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


def test_sitemap_connector_discovers_urls_from_index_and_normalizes_jsonld():
    index_url = "https://careers.example.com/sitemap.xml"
    jobs_sitemap_url = "https://careers.example.com/sitemap-jobs.xml"
    job_url = "https://careers.example.com/jobs/abc"

    index_xml = _fixture("sitemap/sitemap_index.xml")
    jobs_xml = _fixture("sitemap/sitemap_jobs.xml")
    job_html = _fixture("html/job_posting_jsonld.html")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://careers.example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == index_url:
            return httpx.Response(200, text=index_xml, headers={"Content-Type": "application/xml"})
        if url == jobs_sitemap_url:
            return httpx.Response(200, text=jobs_xml, headers={"Content-Type": "application/xml"})
        if url == job_url:
            return httpx.Response(200, text=job_html, headers={"Content-Type": "text/html"})
        return httpx.Response(404, text="not found")

    connector = GenericSitemapConnector(
        sitemap_urls=[index_url],
        include_url_patterns=[r"/jobs/"],
        fetch_details=True,
        redact_pii_enabled=True,
        max_urls=100,
        max_sitemaps=10,
    )

    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        assert len(refs) == 1
        ref = refs[0]
        assert ref.url == job_url

        raw = connector.fetch_job_details(http, run_id="test", job_ref=ref)
        assert raw is not None

        job = connector.normalize(raw, run_id="test")
        assert job is not None
        assert job.title == "Senior Data Engineer"
        assert job.company == "ExampleCorp"
        assert job.location_text == "Amsterdam, NH, NL"
        assert job.remote_policy == "hybrid"
        assert job.employment_type == "perm"
        assert job.description is not None
        assert "recruiting@careers.example.com" not in job.description
        assert "[REDACTED_EMAIL]" in job.description
        assert job.checksum
    finally:
        http.close()


def test_sitemap_connector_returns_empty_when_no_urls_match_pattern():
    """A sitemap with URLs that don't match the include_url_patterns should yield no jobs."""
    index_url = "https://careers.example.com/sitemap.xml"
    sitemap_xml = """<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://careers.example.com/about</loc></url>
  <url><loc>https://careers.example.com/contact</loc></url>
</urlset>"""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://careers.example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == index_url:
            return httpx.Response(200, text=sitemap_xml, headers={"Content-Type": "application/xml"})
        return httpx.Response(404, text="not found")

    connector = GenericSitemapConnector(
        sitemap_urls=[index_url],
        include_url_patterns=[r"/jobs/"],
        fetch_details=False,
        max_urls=100,
        max_sitemaps=10,
    )
    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        assert refs == []
    finally:
        http.close()


def test_sitemap_connector_tolerates_http_500_on_sitemap_url():
    """A 5xx from a sitemap URL is silently skipped; list_jobs returns empty rather than raising.

    The sitemap connector is resilient by design: individual sitemap fetch
    failures are swallowed so a single bad URL doesn't abort the whole crawl.
    """
    index_url = "https://careers.example.com/sitemap.xml"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://careers.example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == index_url:
            return httpx.Response(500, text="Internal Server Error")
        return httpx.Response(404, text="not found")

    connector = GenericSitemapConnector(
        sitemap_urls=[index_url],
        include_url_patterns=[r"/jobs/"],
        fetch_details=False,
        max_urls=100,
        max_sitemaps=10,
    )
    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        assert refs == [], "failed sitemap fetch should yield no refs, not raise"
    finally:
        http.close()


def test_sitemap_connector_raises_on_malformed_xml():
    """Malformed XML in the sitemap should raise a ParseError."""
    index_url = "https://careers.example.com/sitemap.xml"
    bad_xml = "<<not valid xml>>"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://careers.example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == index_url:
            return httpx.Response(200, text=bad_xml, headers={"Content-Type": "application/xml"})
        return httpx.Response(404, text="not found")

    connector = GenericSitemapConnector(
        sitemap_urls=[index_url],
        include_url_patterns=[r"/jobs/"],
        fetch_details=False,
        max_urls=100,
        max_sitemaps=10,
    )
    http = _make_http(handler)
    try:
        with pytest.raises(ParseError):
            connector.list_jobs(http, run_id="test")
    finally:
        http.close()


def test_sitemap_connector_respects_max_urls_limit():
    """The connector should not return more refs than max_urls."""
    index_url = "https://careers.example.com/sitemap.xml"
    urls = "\n".join(
        f"  <url><loc>https://careers.example.com/jobs/{i}</loc></url>" for i in range(20)
    )
    sitemap_xml = f"""<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{urls}
</urlset>"""

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://careers.example.com/robots.txt":
            return httpx.Response(404, text="")
        if url == index_url:
            return httpx.Response(200, text=sitemap_xml, headers={"Content-Type": "application/xml"})
        return httpx.Response(404, text="not found")

    connector = GenericSitemapConnector(
        sitemap_urls=[index_url],
        include_url_patterns=[r"/jobs/"],
        fetch_details=False,
        max_urls=5,
        max_sitemaps=10,
    )
    http = _make_http(handler)
    try:
        refs = connector.list_jobs(http, run_id="test")
        assert len(refs) <= 5
    finally:
        http.close()
