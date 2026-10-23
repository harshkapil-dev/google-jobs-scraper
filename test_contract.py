import asyncio
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from playwright.async_api import async_playwright

import app as worker
import browser as browser_module
from browser import GoogleJobsBrowser, allowed_request, proxy_configuration
from job_store import JobStore
from models import ScrapeInput
from scraper import GoogleJobsScraper, ScraperError, matches_filters, normalize_job, public_url


def job(index=1, **overrides):
    return {
        "job_id": str(index),
        "title": f"React Developer {index}",
        "company_name": "Example Co",
        "location": "Austin, TX",
        "description": "Full public job description",
        "apply_options": [{"title": "Company", "link": f"https://example.com/careers/{index}"}],
        "extensions": ["2 days ago", "Full-time", "Remote", "$100K a year"],
        **overrides,
    }


class FakeBrowser:
    def __init__(self, batches=None):
        self.batches = batches if batches is not None else [[job(1), job(2)]]
        self.calls, self.closed = 0, 0

    @asynccontextmanager
    async def session(self, payload, session_id=None):
        self.calls += 1
        try:
            yield {"index": 0}
        finally:
            self.closed += 1

    async def cards(self, page):
        return self.batches[min(page["index"], len(self.batches) - 1)] if self.batches else []

    async def identifier(self, card):
        return card.get("job_id")

    async def extract(self, card, page):
        return card

    async def advance(self, page):
        page["index"] += 1


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv("GOOGLE_JOBS_ALLOW_MEMORY_STORE", "true")
    monkeypatch.setenv("GOOGLE_JOBS_PROXY_URL", "http://test-user:test-password@proxy.example.com:8080")
    for name in [
        "REDIS_URL",
        "SCRAPER_QUEUE_REDIS_URL",
        "PULLDATA_WORKER_TOKEN",
        "INTERNAL_API_SECRET",
        "NODE_ENV",
        "DATABASE_URL",
        "PULLDATA_RESULTS_DATABASE_URL",
        "GOOGLE_JOBS_PROXY_SESSION_MODE",
    ]:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SCRAPER_QUEUE_NAMESPACE", "google-jobs-local-tests")
    monkeypatch.setattr(worker, "store", JobStore())
    monkeypatch.setattr(worker, "slots", asyncio.Semaphore(2))
    monkeypatch.setattr(worker, "tasks", {})
    monkeypatch.setattr(worker, "collector", GoogleJobsScraper(FakeBrowser()))


def test_complete_worker_contract():
    with TestClient(worker.app) as client:
        assert client.get("/health").json() == {"ok": True}
        assert client.get("/ready").json() == {"ready": True}
        payload = {"query": "react developer", "max_total_results": 2}
        sync = client.post("/scrape-sync", json=payload)
        assert sync.status_code == 200
        rows = sync.json()["results"]
        assert sync.json()["status"] == "done" and len(rows) == 2
        columns = json.loads(Path("pulldata.output.schema.json").read_text())["columns"]
        for row in rows:
            assert set(row) == {column["key"] for column in columns}
            for column in columns:
                value = row[column["key"]]
                if value is None:
                    continue
                if column["type"] in {"string", "url", "datetime"}:
                    assert isinstance(value, str)
                if column["type"] == "integer":
                    assert type(value) is int
                if column["type"] == "boolean":
                    assert type(value) is bool
        started = client.post("/scrape", json=payload).json()
        assert started["status"] == "queued"
        for _ in range(100):
            status = client.get(f"/job/{started['job_id']}").json()
            if status["status"] == "done":
                break
            time.sleep(0.01)
        assert status["status"] == "done" and status["total"] == 2
        pages = [client.get(f"/job/{started['job_id']}/results?offset={offset}&limit=1").json() for offset in range(3)]
        assert [page["total"] for page in pages] == [2, 2, 2]
        assert [row["job_id"] for page in pages for row in page["results"]] == ["1", "2"]
        assert pages[2]["results"] == []
        for params in ["offset=-1", "offset=100001", "limit=0", "limit=1001", "limit=1.5"]:
            assert client.get(f"/job/{started['job_id']}/results?{params}").status_code == 422


def test_input_validation_and_safe_errors():
    with TestClient(worker.app) as client:
        for override in [
            {"query": ""},
            {"query": " "},
            {"max_total_results": 0},
            {"max_total_results": 1001},
            {"max_total_results": 1.5},
            {"max_total_results": True},
            {"max_total_results": "3"},
            {"max_pages": 101},
            {"timeout_seconds": 301},
            {"country": "USA"},
            {"date_posted": "yesterday"},
            {"job_type": "unbounded"},
            {"remote_only": "false"},
            {"proxy": "DO_NOT_ECHO_SECRET"},
        ]:
            response = client.post("/scrape", json={"query": "engineer", **override})
            assert response.status_code == 422
            assert "DO_NOT_ECHO_SECRET" not in response.text
        assert client.post("/scrape", json=["invalid"]).status_code == 422
        assert client.post("/scrape", content="invalid JSON").status_code == 422
        assert client.post("/scrape", content="x" * 65537).status_code == 413
        assert client.get("/job/not-a-uuid").status_code == 422
        assert client.get(f"/job/{uuid4()}").status_code == 404
    assert ScrapeInput(query="engineer", max_total_results=1000, max_pages=100, timeout_seconds=300)


def test_health_is_independent_and_auth_fails_closed(monkeypatch):
    monkeypatch.delenv("GOOGLE_JOBS_PROXY_URL")
    monkeypatch.setenv("PULLDATA_WORKER_TOKEN", "worker-test-token")
    monkeypatch.setenv("INTERNAL_API_SECRET", "internal-test-token")
    with TestClient(worker.app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/ready").status_code == 401
        assert client.get("/ready", headers={"x-internal-secret": "internal-test-token"}).status_code == 401
        assert client.get("/ready", headers={"Authorization": "Bearer worker-test-token"}).status_code == 503
        assert client.post("/scrape", json={"query": "engineer"}).status_code == 401
    monkeypatch.delenv("PULLDATA_WORKER_TOKEN")
    monkeypatch.setenv("GOOGLE_JOBS_PROXY_URL", "http://user:password@proxy.example.com:8080")
    monkeypatch.delenv("GOOGLE_JOBS_ALLOW_MEMORY_STORE")
    with TestClient(worker.app) as client:
        assert client.get("/ready", headers={"x-internal-secret": "internal-test-token"}).status_code == 503


def test_idempotent_request_identity_and_platform_envelope():
    with TestClient(worker.app) as client:
        payload = {
            "query": "engineer",
            "max_total_results": 2,
            "request_id": str(uuid4()),
            "callback_url": "http://169.254.169.254/",
            "wallet_balance_cents_at_start": "100",
            "max_leads_per_query": 0,
            "scrape_emails": "no",
        }
        first = client.post("/scrape-sync", json=payload).json()
        second = client.post("/scrape-sync", json=payload).json()
        assert first == second
        assert worker.collector.browser.calls == 1
        assert client.post("/scrape-sync", json={**payload, "query": "different"}).status_code == 409


def test_pagination_deduplication_partial_results_and_restart():
    async def run():
        checkpoints = []
        transport = FakeBrowser([[job(1), job(1)], [job(1), job(2), job(3)]])

        async def checkpoint(state):
            checkpoints.append(json.loads(json.dumps(state)))

        rows = await GoogleJobsScraper(transport).collect(ScrapeInput(query="engineer", max_total_results=2), checkpoint=checkpoint)
        assert [row["job_id"] for row in rows] == ["1", "2"]
        assert [row["job_id"] for row in checkpoints[0]["results"]] == ["1"]
        resumed = await GoogleJobsScraper(transport).collect(ScrapeInput(query="engineer", max_total_results=2), state=checkpoints[0])
        assert [row["job_id"] for row in resumed] == ["1", "2"]
        assert transport.closed == 2

    asyncio.run(run())


def test_filtered_rows_keep_contiguous_positions_and_unknown_values_are_excluded():
    async def run():
        transport = FakeBrowser(
            [
                [
                    job(1, extensions=["1 month ago", "Full-time"]),
                    job(2, extensions=["2 days ago", "Part-time", "Remote"]),
                    job(3, extensions=["1 day ago", "Full-time", "Remote"]),
                ]
            ]
        )
        rows = await GoogleJobsScraper(transport).collect(
            ScrapeInput(query="engineer", date_posted="past_3_days", job_type="full_time", remote_only=True)
        )
        assert [row["job_id"] for row in rows] == ["3"]
        assert rows[0]["position"] == 1
        unknown = normalize_job(job(extensions=[]), ScrapeInput(query="engineer"), 1, "2026-10-22T00:00:00Z")
        assert unknown["work_from_home"] is None and unknown["salary"] is None
        assert not matches_filters(unknown, ScrapeInput(query="engineer", remote_only=True))
        assert not matches_filters(unknown, ScrapeInput(query="engineer", date_posted="past_week"))

    asyncio.run(run())


def test_empty_results_page_bound_and_overall_timeout():
    async def run():
        assert await GoogleJobsScraper(FakeBrowser([])).collect(ScrapeInput(query="engineer")) == []
        rows = await GoogleJobsScraper(FakeBrowser([[job(1)], [job(2)]])).collect(ScrapeInput(query="engineer", max_pages=1))
        assert len(rows) == 1
        slow = FakeBrowser()

        async def cards(_page):
            await asyncio.sleep(1)

        slow.cards = cards
        payload = ScrapeInput(query="engineer").model_copy(update={"timeout_seconds": 0.01})
        with pytest.raises(ScraperError, match="overall timeout"):
            await GoogleJobsScraper(slow).collect(payload)
        assert slow.closed == 1

    asyncio.run(run())


@pytest.mark.parametrize("status,attempts", [(429, 3), (503, 3), (401, 1), (403, 1)])
def test_browser_navigation_retry_policy_and_cleanup(monkeypatch, status, attempts):
    page = MagicMock()
    page.goto = AsyncMock(return_value=MagicMock(status=status))
    context = MagicMock(new_page=AsyncMock(return_value=page), route=AsyncMock(), close=AsyncMock())
    browser = MagicMock(new_context=AsyncMock(return_value=context), close=AsyncMock())
    playwright = MagicMock()
    playwright.chromium.launch = AsyncMock(return_value=browser)
    manager = MagicMock(__aenter__=AsyncMock(return_value=playwright), __aexit__=AsyncMock(return_value=False))
    monkeypatch.setattr(browser_module, "async_playwright", lambda: manager)
    monkeypatch.setattr(browser_module.asyncio, "sleep", AsyncMock())

    async def run():
        with pytest.raises(ScraperError):
            async with GoogleJobsBrowser().session(ScrapeInput(query="engineer")):
                pass
        assert page.goto.await_count == attempts
        context.close.assert_awaited_once()
        browser.close.assert_awaited_once()

    asyncio.run(run())


def test_challenges_fail_safely_without_retrying_or_solving():
    page = MagicMock()
    page.url = "https://www.google.com/sorry/index"
    page.locator.return_value.inner_text = AsyncMock(return_value="Our systems have detected unusual traffic")
    with pytest.raises(ScraperError, match="traffic challenge"):
        asyncio.run(GoogleJobsBrowser().check_page(page))


def test_proxy_and_network_target_validation(monkeypatch):
    for scheme, port in [("http", 80), ("https", 443)]:
        monkeypatch.setenv("GOOGLE_JOBS_PROXY_URL", f"{scheme}://test-user:test-password@proxy.example.com")
        assert proxy_configuration()["server"] == f"{scheme}://proxy.example.com:{port}"
    for url in [
        "http://localhost/",
        "http://127.0.0.1/",
        "http://169.254.169.254/",
        "https://user:password@example.com/",
        "javascript:alert(1)",
        "https://host.internal/",
        "https://[::1]/",
    ]:
        assert public_url(url) is None
    for url in [
        "https://google.com.evil.test/",
        "http://www.google.com/",
        "https://169.254.169.254/",
        "https://www.google.com:444/",
        "https://jobs.example.com/",
    ]:
        assert not allowed_request(url)
    assert allowed_request("https://www.google.com/search?q=react")
    assert public_url("https://www.google.com/search#vhid=docid/job123").endswith("#vhid=docid/job123")
    for url in ["not-a-url", "socks5://proxy.example.com:80", "http://user:secret@proxy.example.com:80/path"]:
        monkeypatch.setenv("GOOGLE_JOBS_PROXY_URL", url)
        with pytest.raises(ScraperError) as error:
            proxy_configuration()
        assert "secret" not in str(error.value)


def test_store_leases_and_terminal_updates_are_atomic():
    async def run():
        store = JobStore()
        value = {"status": "queued", "results": []}
        assert await store.create("one", value)
        assert not await store.create("one", value)
        assert await store.claim("one", "first", 30)
        assert not await store.claim("one", "second", 30)
        assert not await store.save("one", "second", {"status": "done", "results": []})
        assert await store.save("one", "first", {"status": "done", "results": [job()]})
        assert not await store.save("one", "first", {"status": "running", "results": []})
        await store.release("one", "second")
        assert not await store.claim("one", "second", 30)
        await store.release("one", "first")
        assert await store.claim("one", "second", 30)
        assert (await store.get("one"))["status"] == "done"

    asyncio.run(run())


def test_rendered_browser_extraction_from_deterministic_fixture():
    # Authored markup tests parser mechanics. It is not live-source evidence.
    async def run():
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.set_content(
                    """<main><div class="MQUd2b" data-job-id="google-job-1"><h3 class="PUpOsf">React Developer</h3><div class="vNEEBe">Example Co</div><div class="Qk80Jf">Austin, TX</div><div class="oNwCmf">via Example Board</div><span class="LL4CDc">2 days ago</span><span class="LL4CDc">Full-time</span></div><div role="dialog" data-job-detail><div data-job-description>Full description with qualifications and responsibilities.</div><a href="https://example.com/apply">Apply on company site</a><a href="https://jobs.example.com/123">Apply on job board</a><section data-job-highlight><h3>Qualifications</h3><ul><li>React experience</li></ul></section></div></main>"""
                )
                transport = GoogleJobsBrowser()
                cards = await transport.cards(page)
                assert await transport.identifier(cards[0]) == "google-job-1"
                raw = await transport.extract(cards[0], page)
                assert raw["title"] == "React Developer" and raw["company_name"] == "Example Co"
                assert len(raw["apply_options"]) == 2
                assert raw["job_highlights"] == [{"title": "Qualifications", "items": ["React experience"]}]
                assert raw["description"].startswith("Full description")
            finally:
                await browser.close()

    asyncio.run(run())


def test_cancellation_preserves_partial_rows_and_cannot_reopen_terminal_jobs():
    with TestClient(worker.app) as client:
        identity = str(uuid4())

        async def setup():
            await worker.store.create(identity, {"status": "running", "results": [job()], "error": None})
            assert await worker.store.claim(identity, "replica-one", 30)

        client.portal.call(setup)
        response = client.post(f"/job/{identity}/cancel")
        assert response.json()["status"] == "cancelled"
        assert response.json()["total"] == 1
        assert client.post(f"/job/{identity}/stop").json() == response.json()
        assert client.get(f"/job/{identity}/results").json()["total"] == 1

        async def verify():
            assert not await worker.store.save(identity, "replica-one", {"status": "done", "results": []})

        client.portal.call(verify)


def test_webshare_browser_jobs_use_a_stable_session_without_mutating_other_gateways(monkeypatch):
    monkeypatch.setenv("GOOGLE_JOBS_PROXY_URL", "http://test-user-us-rotate:test-password@p.webshare.io:80")
    assert proxy_configuration("123456789")["username"] == "test-user-us-rotate"
    monkeypatch.setenv("GOOGLE_JOBS_PROXY_SESSION_MODE", "sticky")
    first = proxy_configuration("123456789")
    assert first["username"] == "test-user-us-123456789"
    assert proxy_configuration("123456789") == first
    assert proxy_configuration("987654321")["username"] != first["username"]
    monkeypatch.setenv("GOOGLE_JOBS_PROXY_URL", "http://test-user-rotate:test-password@proxy.example.com:80")
    assert proxy_configuration("123456789")["username"] == "test-user-rotate"


def test_configured_postgres_failure_never_falls_back_to_memory(monkeypatch):
    import psycopg
    import postgres_job_store

    monkeypatch.setenv("PULLDATA_RESULTS_DATABASE_URL", "postgresql://private-test-password@unreachable.invalid/test")
    monkeypatch.setattr(postgres_job_store.psycopg, "connect", MagicMock(side_effect=psycopg.OperationalError("private-test-password")))
    postgres_store = JobStore()
    for operation in [lambda: asyncio.run(postgres_store.create("one", {"status": "queued"})), postgres_store.ready]:
        with pytest.raises(ScraperError) as error:
            operation()
        assert error.value.status == 503
        assert "private-test-password" not in str(error.value)
    assert postgres_store.memory == {}
