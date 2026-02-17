from __future__ import annotations

from pathlib import Path

import httpx

from shared.job_connectors.connectors.sitemap import GenericSitemapConnector
from shared.job_connectors.http import PoliteHttpClient


def _fixture(path: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / "job_connectors"
    return (base / path).read_text(encoding="utf-8")


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

    http = PoliteHttpClient(
        user_agent="TestAgent/1.0",
        require_allowlist=True,
        allowlist_hosts=["careers.example.com"],
        robots_strict=True,
        rate_limit_per_host_s=0.0,
        max_retries=0,
        transport=httpx.MockTransport(handler),
        sleep=lambda _s: None,
    )
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
