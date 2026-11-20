import asyncio
import copy
import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from models import ScrapeInput
from scraper import GoogleJobsScraper, ScraperError, TrafficChallengeError
from serp_source import JobPage, OwnedSerpSource
from test_contract import job


class FakeSource:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []
        self.closed = 0

    def ready(self):
        pass

    async def search(self, payload, page_token=None):
        self.calls.append(page_token)
        return self.pages[page_token]

    async def close(self):
        self.closed += 1


def test_api_pages_reuse_normalizer_filters_dedupe_and_output_contract():
    source = FakeSource(
        {
            None: JobPage([job(1, extensions=["2 days ago", "Part-time"]), job(2), job(2)], "second"),
            "second": JobPage([job(2), job(3)]),
        }
    )
    payload = ScrapeInput(query="engineer", max_total_results=2, job_type="full_time", remote_only=True)
    rows = asyncio.run(GoogleJobsScraper(source=source).collect(payload))
    assert [row["job_id"] for row in rows] == ["2", "3"]
    assert [row["position"] for row in rows] == [1, 2]
    keys = {column["key"] for column in json.loads(Path("pulldata.output.schema.json").read_text())["columns"]}
    assert all(set(row) == keys for row in rows)
    assert source.calls == [None, "second"] and source.closed == 1


def test_restart_inside_api_page_replays_only_that_page_without_losing_rows():
    source = FakeSource({None: JobPage([job(1), job(2)], "second"), "second": JobPage([job(3)])})
    checkpoints = []

    async def checkpoint(state):
        checkpoints.append(copy.deepcopy(state))
        raise asyncio.CancelledError()

    payload = ScrapeInput(query="engineer", max_total_results=3)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(GoogleJobsScraper(source=source).collect(payload, checkpoint=checkpoint))
    assert source.closed == 1 and checkpoints[0]["page_in_progress"]
    rows = asyncio.run(GoogleJobsScraper(source=source).collect(payload, state=checkpoints[0]))
    assert [row["job_id"] for row in rows] == ["1", "2", "3"]
    assert source.calls == [None, None, "second"] and source.closed == 2


def test_restart_after_completed_api_page_uses_next_token_directly():
    source = FakeSource({None: JobPage([job(1)], "second"), "second": JobPage([job(2)])})
    checkpoints = []

    async def checkpoint(state):
        if not state["page_in_progress"]:
            checkpoints.append(copy.deepcopy(state))
            raise asyncio.CancelledError()

    payload = ScrapeInput(query="engineer", max_total_results=2)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(GoogleJobsScraper(source=source).collect(payload, checkpoint=checkpoint))
    rows = asyncio.run(GoogleJobsScraper(source=source).collect(payload, state=checkpoints[0]))
    assert [row["job_id"] for row in rows] == ["1", "2"]
    assert source.calls == [None, "second"]


def test_api_empty_limits_and_repeated_page_token_stop():
    payload = ScrapeInput(query="engineer", max_pages=1)
    source = FakeSource({None: JobPage([job(1)], "next")})
    assert len(asyncio.run(GoogleJobsScraper(source=source).collect(payload))) == 1
    assert source.calls == [None]
    empty = FakeSource({None: JobPage([])})
    assert asyncio.run(GoogleJobsScraper(source=empty).collect(payload)) == []
    repeated = FakeSource({None: JobPage([job(1)], "next"), "next": JobPage([job(1)], "next")})
    assert len(asyncio.run(GoogleJobsScraper(source=repeated).collect(ScrapeInput(query="engineer")))) == 1
    assert repeated.calls == [None, "next"]


def test_api_challenge_opens_circuit_and_releases_resources():
    source = FakeSource({})
    source.search = AsyncMock(side_effect=TrafficChallengeError())
    scraper = GoogleJobsScraper(source=source)
    with pytest.raises(TrafficChallengeError):
        asyncio.run(scraper.collect(ScrapeInput(query="engineer")))
    with pytest.raises(ScraperError, match="cooling down"):
        asyncio.run(scraper.collect(ScrapeInput(query="engineer")))
    assert source.search.await_count == 1 and source.closed == 1


def test_source_switch_and_provider_exception_cannot_mix_or_expose_saved_data():
    source = FakeSource({None: JobPage([job(1)])})
    with pytest.raises(ScraperError, match="switch sources"):
        asyncio.run(GoogleJobsScraper(source=source).collect(ScrapeInput(query="engineer"), {"source_kind": "browser", "results": []}))
    source.search = AsyncMock(side_effect=RuntimeError("DO_NOT_ECHO_TOKEN"))
    with pytest.raises(ScraperError) as error:
        asyncio.run(GoogleJobsScraper(source=source).collect(ScrapeInput(query="engineer")))
    assert "DO_NOT_ECHO_TOKEN" not in str(error.value) and source.closed == 1


def test_owned_service_page_request_has_fixed_route_auth_and_no_browser(monkeypatch):
    async def handler(request):
        assert request.url.path == "/google-jobs/search"
        assert request.headers["x-internal-secret"] == "test-internal-secret"
        assert json.loads(request.content) == {"query": "engineer", "location": "Austin", "country": "us", "page_token": None}
        return httpx.Response(200, json={"source": "google_jobs", "jobs_results": [job(1)], "next_page_token": None})

    source = OwnedSerpSource(
        base_url="https://google-search-results-prod-production.up.railway.app",
        secret="test-internal-secret",
        transport=httpx.MockTransport(handler),
    )
    monkeypatch.delenv("GOOGLE_JOBS_PROXY_URL", raising=False)
    rows = asyncio.run(GoogleJobsScraper(source=source).collect(ScrapeInput(query="engineer", location="Austin")))
    assert len(rows) == 1 and source.client is None


@pytest.mark.parametrize("code", [401, 403, 404, 400, 302])
def test_permanent_errors_and_redirects_are_never_retried(code):
    calls = []

    async def handler(request):
        calls.append(request)
        return httpx.Response(code, json={"error": "DO_NOT_ECHO_TOKEN"}, headers={"location": "https://untrusted.example/"})

    source = OwnedSerpSource(
        base_url="http://google-search-results-dev.railway.internal:8008", secret="fixture", transport=httpx.MockTransport(handler)
    )
    with pytest.raises(ScraperError) as error:
        asyncio.run(GoogleJobsScraper(source=source).collect(ScrapeInput(query="engineer")))
    assert len(calls) == 1 and "DO_NOT_ECHO_TOKEN" not in str(error.value)
    assert source.client is None


def test_api_retries_only_transient_failures_and_never_challenges():
    async def run(challenge):
        calls = []

        async def handler(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(503, json={"code": "traffic_challenge" if challenge else "unavailable"})
            return httpx.Response(200, json={"source": "google_jobs", "jobs_results": []})

        sleep = AsyncMock()
        source = OwnedSerpSource(
            base_url="http://google-search-results-dev.railway.internal:8008",
            secret="fixture",
            transport=httpx.MockTransport(handler),
            sleep=sleep,
        )
        try:
            if challenge:
                with pytest.raises(TrafficChallengeError):
                    await source.search(ScrapeInput(query="engineer"))
                assert len(calls) == 1
                sleep.assert_not_awaited()
            else:
                assert (await source.search(ScrapeInput(query="engineer"))).jobs == []
                assert len(calls) == 2 and sleep.await_count == 1
        finally:
            await source.close()

    asyncio.run(run(False))
    asyncio.run(run(True))


def test_organic_serp_results_are_rejected_instead_of_being_invented_as_jobs():
    source = OwnedSerpSource(
        base_url="https://google-search-results-prod-production.up.railway.app",
        secret="fixture",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"status": "success", "data": [{"title": "Web result", "url": "https://example.com/"}]}
            )
        ),
    )
    with pytest.raises(ScraperError, match="organic web results"):
        asyncio.run(GoogleJobsScraper(source=source).collect(ScrapeInput(query="engineer")))


@pytest.mark.parametrize(
    "origin",
    [
        "https://127.0.0.1",
        "http://169.254.169.254",
        "https://google.com",
        "https://user:pass@service.up.railway.app",
        "https://service.up.railway.app/redirect?url=other",
    ],
)
def test_only_operator_configured_owned_railway_origins_are_allowed(origin):
    with pytest.raises(ScraperError, match="origin"):
        OwnedSerpSource(base_url=origin, secret="fixture").ready()
