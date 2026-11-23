import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from models import ScrapeInput
from scraper import GoogleJobsScraper, ScraperError, TrafficChallengeError
from trawl_source import TrawlJobsSource, parse_jobs_html, private_trawl_url

SAMPLE_HTML = """
<html><body>
  <div class="MQUd2b" data-job-id="trawl-job-1">
    <h3 class="PUpOsf">Backend Engineer</h3>
    <div class="vNEEBe">Acme Corp</div>
    <div class="Qk80Jf">Austin, TX</div>
    <div class="oNwCmf">via Acme Careers</div>
    <span class="LL4CDc">2 days ago</span>
    <span class="LL4CDc">Full-time</span>
    <span class="LL4CDc">Remote</span>
    <a href="https://www.google.com/search?q=jobs#vhid=docid/trawl-job-1">Share</a>
  </div>
  <div role="dialog" data-job-detail data-job-id="trawl-job-1">
    <h2>Backend Engineer</h2>
    <div data-job-company>Acme Corp</div>
    <div data-job-description>Build scalable APIs and data systems.</div>
    <a href="https://acme.example/apply/1">Apply on Acme Careers</a>
    <section data-job-highlight><h3>Qualifications</h3><ul><li>Python experience</li></ul></section>
  </div>
</body></html>
"""


def test_private_trawl_url_validation():
    assert private_trawl_url("http://trawl-serp-dev.railway.internal:8191") == "http://trawl-serp-dev.railway.internal:8191"
    assert private_trawl_url("http://localhost:8191/") == "http://localhost:8191"
    for invalid in [
        "",
        "https://public.example.com",
        "http://user:pass@localhost:8191",
        "http://trawl-serp-dev.railway.internal:8191/scrape",
        "http://169.254.169.254",
    ]:
        with pytest.raises(ScraperError) as error:
            private_trawl_url(invalid)
        assert error.value.status == 503


def test_parse_jobs_html_extracts_cards_details_and_empty_state():
    jobs = parse_jobs_html(SAMPLE_HTML)
    assert len(jobs) == 1
    assert jobs[0]["job_id"] == "trawl-job-1"
    assert jobs[0]["title"] == "Backend Engineer"
    assert jobs[0]["company_name"] == "Acme Corp"
    assert jobs[0]["location"] == "Austin, TX"
    assert jobs[0]["description"] == "Build scalable APIs and data systems."
    assert jobs[0]["apply_options"] == [{"title": "Apply on Acme Careers", "link": "https://acme.example/apply/1"}]
    assert jobs[0]["job_highlights"] == [{"title": "Qualifications", "items": ["Python experience"]}]

    assert parse_jobs_html("<html><body>No matching jobs found for your search.</body></html>") == []

    with pytest.raises(TrafficChallengeError):
        parse_jobs_html("<html><body>Our systems have detected unusual traffic from your network.</body></html>")

    with pytest.raises(ScraperError, match="supported jobs list"):
        parse_jobs_html("<html><body>Enable JavaScript</body></html>")


def test_trawl_source_end_to_end_proxy_regional_host_and_schema(monkeypatch):
    monkeypatch.setenv("GOOGLE_JOBS_SOURCE", "trawl")
    monkeypatch.setenv("GOOGLE_JOBS_TRAWL_BASE_URL", "http://trawl-serp-dev.railway.internal:8191")
    monkeypatch.setenv("GOOGLE_JOBS_PROXY_URL", "http://proxy-user:proxy-pass@p.webshare.io:80")
    requests = []

    async def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        return httpx.Response(
            200,
            json={
                "url": "https://www.google.co.uk/search?q=engineer+jobs&udm=8",
                "statusCode": 200,
                "html": SAMPLE_HTML,
            },
        )

    source = TrawlJobsSource(base_url="http://trawl-serp-dev.railway.internal:8191", transport=httpx.MockTransport(handler))
    scraper = GoogleJobsScraper(source=source)
    rows = asyncio.run(scraper.collect(ScrapeInput(query="engineer", country="gb", max_total_results=2, remote_only=True)))
    assert len(rows) == 1
    assert rows[0]["job_id"] == "trawl-job-1"
    assert rows[0]["work_from_home"] is True
    assert requests[0]["skipHttp"] is True
    assert requests[0]["maxTier"] == 3
    assert requests[0]["omitRawBody"] is True
    assert requests[0]["proxy"] == "http://proxy-user:proxy-pass@p.webshare.io:80"
    keys = {column["key"] for column in json.loads(Path("pulldata.output.schema.json").read_text())["columns"]}
    assert set(rows[0]) == keys
    assert source.client is None


def test_trawl_source_retries_transient_503_and_never_retries_challenge(monkeypatch):
    monkeypatch.delenv("GOOGLE_JOBS_PROXY_URL", raising=False)

    async def run():
        calls = []

        async def transient_handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(503, text="busy")
            return httpx.Response(200, json={"url": "https://www.google.com/search?udm=8", "statusCode": 200, "html": SAMPLE_HTML})

        sleep = AsyncMock()
        source = TrawlJobsSource(
            base_url="http://localhost:8191",
            transport=httpx.MockTransport(transient_handler),
            sleep=sleep,
        )
        try:
            page = await source.search(ScrapeInput(query="engineer"))
            assert len(page.jobs) == 1 and len(calls) == 2 and sleep.await_count == 1
        finally:
            await source.close()

        challenge_calls = []

        async def challenge_handler(request):
            challenge_calls.append(request)
            return httpx.Response(
                200,
                json={"url": "https://www.google.com/sorry/index", "statusCode": 200, "html": "<html>captcha</html>"},
            )

        sleep2 = AsyncMock()
        source2 = TrawlJobsSource(
            base_url="http://localhost:8191",
            transport=httpx.MockTransport(challenge_handler),
            sleep=sleep2,
        )
        try:
            with pytest.raises(TrafficChallengeError):
                await source2.search(ScrapeInput(query="engineer"))
            assert len(challenge_calls) == 1
            sleep2.assert_not_awaited()
        finally:
            await source2.close()

    asyncio.run(run())
