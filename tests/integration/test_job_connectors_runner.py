from __future__ import annotations

from pathlib import Path

import httpx

from shared.job_connectors.connectors.rss import GenericRssConnector
from shared.job_connectors.connectors.sitemap import GenericSitemapConnector
from shared.job_connectors.http import PoliteHttpClient
from shared.job_connectors.raw_store import RawPayloadStore
from shared.job_connectors.runner import run_connectors
from shared.job_connectors.sinks import InMemorySink


def _fixture(path: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "job_connectors"
    return (base / path).read_text(encoding="utf-8")


def test_runner_executes_multiple_connectors_against_fixtures_only(tmp_path):
    feed_url = "https://example.com/jobs/rss.xml"
    index_url = "https://careers.example.com/sitemap.xml"
    jobs_sitemap_url = "https://careers.example.com/sitemap-jobs.xml"
    job_url = "https://careers.example.com/jobs/abc"

    feed_xml = _fixture("rss/sample_rss.xml")
    index_xml = _fixture("sitemap/sitemap_index.xml")
    jobs_xml = _fixture("sitemap/sitemap_jobs.xml")
    job_html = _fixture("html/job_posting_jsonld.html")

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url in {"https://example.com/robots.txt", "https://careers.example.com/robots.txt"}:
            return httpx.Response(404, text="")
        if url == feed_url:
            return httpx.Response(200, text=feed_xml, headers={"Content-Type": "application/rss+xml"})
        if url == index_url:
            return httpx.Response(200, text=index_xml, headers={"Content-Type": "application/xml"})
        if url == jobs_sitemap_url:
            return httpx.Response(200, text=jobs_xml, headers={"Content-Type": "application/xml"})
        if url == job_url:
            return httpx.Response(200, text=job_html, headers={"Content-Type": "text/html"})
        return httpx.Response(404, text="not found")

    http = PoliteHttpClient(
        user_agent="TestAgent/1.0",
        require_allowlist=True,
        allowlist_hosts=["example.com", "careers.example.com"],
        robots_strict=True,
        rate_limit_per_host_s=0.0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
        sleep=lambda _s: None,
    )
    try:
        connectors = [
            GenericRssConnector(feed_urls=[feed_url], fetch_details=False, redact_pii_enabled=True),
            GenericSitemapConnector(
                sitemap_urls=[index_url],
                include_url_patterns=[r"/jobs/"],
                fetch_details=True,
                redact_pii_enabled=True,
                max_urls=100,
                max_sitemaps=10,
            ),
        ]
        sink = InMemorySink()
        raw_store = RawPayloadStore(base_dir=str(tmp_path / "raw"), enabled=False)
        summary = run_connectors(connectors, http=http, sink=sink, raw_store=raw_store, run_id="test-run")

        assert summary.run_id == "test-run"
        assert len(summary.connectors) == 2

        # One from RSS + one from Sitemap
        assert len(sink.items) == 2
        urls = {job.url for job in sink.items.values()}
        assert "https://example.com/jobs/123" in urls
        assert "https://careers.example.com/jobs/abc" in urls

        inserted = sum(s.inserted for s in summary.connectors)
        assert inserted == 2
    finally:
        http.close()
