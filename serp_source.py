"""Bounded page client for Pulldata's own Google Jobs SERP endpoint."""

import asyncio
import os
import random
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

from models import ScrapeInput
from scraper import ScraperError, TrafficChallengeError

MAX_RESPONSE_BYTES = 4 * 1024 * 1024
PAGE_PATH = "/google-jobs/search"


@dataclass(frozen=True)
class JobPage:
    jobs: list[dict]
    next_page_token: str | None = None


class OwnedSerpSource:
    def __init__(self, *, base_url=None, secret=None, transport=None, sleep=asyncio.sleep):
        self.base_url = (base_url if base_url is not None else os.getenv("GOOGLE_JOBS_SERP_BASE_URL", "")).rstrip("/")
        self.secret = secret if secret is not None else os.getenv("INTERNAL_API_SECRET", "")
        self.transport = transport
        self.sleep = sleep
        self.client = None

    def ready(self):
        try:
            parsed = urlsplit(self.base_url)
            host = parsed.hostname or ""
            # An operator chooses the service. Users cannot supply a fetch URL.
            private = parsed.scheme == "http" and host.endswith(".railway.internal")
            public = parsed.scheme == "https" and host.endswith(".up.railway.app")
            if (
                not (private or public)
                or parsed.username
                or parsed.password
                or parsed.path not in {"", "/"}
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError()
            if parsed.port is not None and not 1 <= parsed.port <= 65535:
                raise ValueError()
        except ValueError:
            raise ScraperError("Owned SERP service origin is not configured correctly", 503) from None
        if not self.secret:
            raise ScraperError("Owned SERP service authentication is not configured", 503)

    async def search(self, payload: ScrapeInput, page_token: str | None = None):
        self.ready()
        if page_token is not None and (not isinstance(page_token, str) or not page_token or len(page_token) > 4096):
            raise ScraperError("Owned SERP returned an invalid page token")
        if self.client is None:
            self.client = httpx.AsyncClient(
                transport=self.transport,
                trust_env=False,
                follow_redirects=False,
                timeout=httpx.Timeout(40, connect=5, pool=5),
            )
        body = {"query": payload.query, "location": payload.location, "country": payload.country, "page_token": page_token}
        for attempt in range(2):
            try:
                async with self.client.stream(
                    "POST",
                    self.base_url + PAGE_PATH,
                    json=body,
                    headers={"x-internal-secret": self.secret, "User-Agent": "PulldataGoogleJobs/1.0"},
                ) as response:
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > MAX_RESPONSE_BYTES:
                            raise ScraperError("Owned SERP response exceeds the page limit")
                    try:
                        document = __import__("json").loads(data)
                    except ValueError:
                        raise ScraperError("Owned SERP did not return a Google Jobs JSON page") from None
                    if isinstance(document, dict) and document.get("code") == "traffic_challenge":
                        raise TrafficChallengeError()
                    if response.status_code in {401, 403}:
                        raise ScraperError("Owned SERP service authentication was rejected", 503)
                    if response.status_code == 404:
                        raise ScraperError("Owned SERP does not expose the required Google Jobs endpoint", 503)
                    if response.status_code in {429, 502, 503, 504} and attempt == 0:
                        await self.sleep(0.5 + random.uniform(0, 0.25))
                        continue
                    if response.status_code != 200:
                        raise ScraperError("Owned SERP could not complete the Google Jobs page", 503)
                    if not isinstance(document, dict) or document.get("source") != "google_jobs":
                        raise ScraperError("Owned SERP returned incompatible results; organic web results are not Google Jobs")
                    jobs, token = document.get("jobs_results"), document.get("next_page_token")
                    if not isinstance(jobs, list) or len(jobs) > 100 or any(not isinstance(job, dict) for job in jobs):
                        raise ScraperError("Owned SERP returned an invalid Google Jobs page")
                    if token is not None and (not isinstance(token, str) or not token or len(token) > 4096):
                        raise ScraperError("Owned SERP returned an invalid page token")
                    return JobPage(jobs, token)
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if attempt == 0:
                    await self.sleep(0.5 + random.uniform(0, 0.25))
                    continue
                raise ScraperError("Owned SERP connection is temporarily unavailable", 503) from None
            except httpx.TimeoutException:
                # A read timeout may already have started work; do not duplicate it.
                raise ScraperError("Owned SERP Google Jobs request timed out", 504) from None
            except httpx.HTTPError:
                raise ScraperError("Owned SERP Google Jobs request failed", 503) from None
        raise ScraperError("Owned SERP Google Jobs request failed", 503)

    async def close(self):
        if self.client:
            await self.client.aclose()
            self.client = None
