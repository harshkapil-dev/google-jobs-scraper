"""Private Trawl (Camoufox) HTML source for public Google Jobs."""

import asyncio
from dataclasses import dataclass, field
from html.parser import HTMLParser
import json
import os
import random
import re
from urllib.parse import unquote, urlsplit

import httpx

from browser import ALLOWED_GOOGLE_HOSTS, google_search_url, proxy_configuration
from models import ScrapeInput
from scraper import ScraperError, TrafficChallengeError
from serp_source import JobPage

MAX_BROWSER_BYTES = 5 * 1024 * 1024
CARD_CLASSES = {"MQUd2b", "gws-plugins-horizon-jobs__li-ed", "iFjolb"}
TITLE_CLASSES = {"PUpOsf", "BjJfJf"}
COMPANY_CLASSES = {"vNEEBe", "nJlQNd", "a3jPc"}
LOCATION_CLASSES = {"Qk80Jf", "FqK3wc"}
VIA_CLASSES = {"oNwCmf"}
EXT_CLASSES = {"LL4CDc", "I2Cbhb"}
DESC_CLASSES = {"NgUYpe", "HBvzbc"}
DETAIL_CLASSES = {"gws-plugins-horizon-jobs__p-dtl"}
HIGHLIGHT_CLASSES = {"YgLbBe"}
VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}


@dataclass
class _Node:
    tag: str
    attrs: dict[str, str]
    children: list = field(default_factory=list)

    @property
    def classes(self) -> set[str]:
        return set((self.attrs.get("class") or "").split())

    def text(self) -> str:
        parts: list[str] = []
        for child in self.children:
            if isinstance(child, str):
                parts.append(child)
            elif child.tag not in {"script", "style", "noscript"}:
                parts.append(child.text())
        return re.sub(r"\s+", " ", " ".join(parts)).strip()

    def walk(self):
        yield self
        for child in self.children:
            if isinstance(child, _Node):
                yield from child.walk()


class _DomBuilder(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = _Node("document", {})
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = _Node(tag.lower(), {k.lower(): (v or "") for k, v in attrs})
        self.stack[-1].children.append(node)
        if node.tag not in VOID_TAGS:
            self.stack.append(node)

    def handle_endtag(self, tag):
        target = tag.lower()
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == target:
                del self.stack[index:]
                break

    def handle_data(self, data):
        if data:
            self.stack[-1].children.append(data)


def private_trawl_url(value: str | None = None) -> str:
    raw = (value if value is not None else os.getenv("GOOGLE_JOBS_TRAWL_BASE_URL", "")).strip()
    try:
        parsed = urlsplit(raw)
        host = (parsed.hostname or "").lower()
        allowed_host = host in {"localhost", "127.0.0.1", "host.docker.internal"} or host.endswith(".railway.internal")
        if (
            parsed.scheme not in {"http", "https"}
            or not allowed_host
            or parsed.username
            or parsed.password
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError()
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError()
        return raw.rstrip("/")
    except ValueError:
        raise ScraperError("Private Google Jobs Trawl origin is not configured correctly", 503) from None


def _is_challenge(url: str, html: str) -> bool:
    if "/sorry/" in (url or ""):
        return True
    return bool(re.search(r"unusual traffic|our systems have detected|verify you are human|not a robot", html or "", re.I))


def _is_card(node: _Node) -> bool:
    if "data-job-detail" in node.attrs or node.attrs.get("role") == "dialog" or (node.classes & DETAIL_CLASSES):
        return False
    if node.classes & CARD_CLASSES:
        return True
    return bool(node.attrs.get("data-job-id") or node.attrs.get("data-htidocid"))


def _is_detail_panel(node: _Node) -> bool:
    return "data-job-detail" in node.attrs or node.attrs.get("role") == "dialog" or bool(node.classes & DETAIL_CLASSES)


def _extract_id(node: _Node) -> str | None:
    for attr in ("data-job-id", "data-htidocid", "data-entityid"):
        if node.attrs.get(attr):
            return node.attrs[attr].strip()
    for descendant in node.walk():
        if descendant is node:
            continue
        if descendant.attrs.get("data-entityid"):
            return descendant.attrs["data-entityid"].strip()
        if descendant.tag == "a":
            href = descendant.attrs.get("href") or ""
            match = re.search(r"(?:docid[=/]|htidocid=)([^&#/]+)", unquote(href))
            if match:
                return match.group(1)
    return None


def _pick_text(node: _Node, classes: set[str], tags: tuple[str, ...] = (), attrs: tuple[str, ...] = ()) -> str | None:
    for child in node.walk():
        if child is node:
            continue
        if (
            (child.classes & classes)
            or (tags and child.tag in tags)
            or any(attr in child.attrs for attr in attrs)
            or ("role" in attrs and child.attrs.get("role") == "heading")
        ):
            value = child.text()
            if value:
                return value
    return None


def _extract_detail_map(root: _Node) -> dict[str, dict]:
    details: dict[str, dict] = {}
    for node in root.walk():
        if not _is_detail_panel(node):
            continue
        desc = _pick_text(node, DESC_CLASSES, attrs=("data-job-description",))
        if not desc:
            for child in node.walk():
                if child.attrs.get("data-attrid") == "description" and child.text():
                    desc = child.text()
                    break
        if not desc:
            continue
        apply_options = []
        for child in node.walk():
            if child.tag == "a" and child.attrs.get("href"):
                label = child.text() or child.attrs.get("aria-label") or ""
                if re.search(r"apply|application", label, re.I):
                    apply_options.append({"title": label or None, "link": child.attrs["href"]})
        highlights = []
        for child in node.walk():
            if "data-job-highlight" in child.attrs or (child.classes & HIGHLIGHT_CLASSES):
                heading = _pick_text(child, set(), tags=("h3", "h4"), attrs=("role",))
                items = [li.text() for li in child.walk() if li.tag == "li" and li.text()]
                if items:
                    highlights.append({"title": heading, "items": items})
        entry = {"description": desc, "apply_options": apply_options, "job_highlights": highlights}
        job_id = node.attrs.get("data-job-id") or node.attrs.get("data-htidocid")
        if job_id:
            details[f"id:{job_id.strip()}"] = entry
        title = _pick_text(node, TITLE_CLASSES, tags=("h1", "h2"), attrs=("data-job-title", "role"))
        company = _pick_text(node, COMPANY_CLASSES, attrs=("data-job-company",))
        if title and company:
            details[f"tc:{title.casefold()}|{company.casefold()}"] = entry
    return details


def parse_jobs_html(html: str) -> list[dict]:
    if not isinstance(html, str) or len(html.encode("utf-8", errors="ignore")) > MAX_BROWSER_BYTES:
        raise ScraperError("Google Jobs Trawl response is invalid", 502)
    if _is_challenge("", html):
        raise TrafficChallengeError()
    builder = _DomBuilder()
    builder.feed(html)
    root = builder.root
    details = _extract_detail_map(root)

    cards: list[_Node] = []

    def collect_cards(node: _Node):
        if _is_card(node):
            cards.append(node)
            return
        for child in node.children:
            if isinstance(child, _Node):
                collect_cards(child)

    collect_cards(root)

    jobs: list[dict] = []
    for card in cards:
        title = _pick_text(card, TITLE_CLASSES, tags=("h1", "h2", "h3"), attrs=("role",))
        company = _pick_text(card, COMPANY_CLASSES, attrs=("data-job-company",))
        if not title or not company:
            continue
        job_id = _extract_id(card)
        location = _pick_text(card, LOCATION_CLASSES, attrs=("data-job-location",))
        via = _pick_text(card, VIA_CLASSES, attrs=("data-job-via",))
        extensions = []
        share_link = None
        thumbnail = None
        for child in card.walk():
            if child is card:
                continue
            if (child.classes & EXT_CLASSES) or "data-job-extension" in child.attrs:
                val = child.text()
                if val and val not in extensions:
                    extensions.append(val)
            if child.tag == "a" and not share_link:
                href = child.attrs.get("href") or ""
                if "docid" in href or "htidocid" in href:
                    share_link = href
            if child.tag == "img" and not thumbnail:
                thumbnail = child.attrs.get("data-src") or child.attrs.get("src") or None

        detail = (details.get(f"id:{job_id}") if job_id else None) or details.get(f"tc:{title.casefold()}|{company.casefold()}") or {}
        jobs.append(
            {
                "job_id": job_id,
                "title": title,
                "company_name": company,
                "location": location,
                "via": via,
                "thumbnail": thumbnail,
                "share_link": share_link,
                "extensions": extensions,
                "description": detail.get("description"),
                "apply_options": detail.get("apply_options", []),
                "job_highlights": detail.get("job_highlights", []),
            }
        )

    if jobs:
        return jobs
    body_text = root.text()
    if re.search(r"no (?:matching )?jobs|did not match any jobs|no results found", body_text, re.I):
        return []
    raise ScraperError("Google Jobs did not expose a supported jobs list", 502)


def _proxy_url_string(proxy_dict: dict | None) -> str | None:
    if not proxy_dict:
        return None
    server = proxy_dict["server"]
    parsed = urlsplit(server)
    user = proxy_dict.get("username") or ""
    password = proxy_dict.get("password") or ""
    if user or password:
        from urllib.parse import quote

        creds = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    else:
        creds = ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{creds}{parsed.hostname}{port}"


class TrawlJobsSource:
    def __init__(self, *, base_url: str | None = None, transport=None, sleep=asyncio.sleep):
        self.base_url = base_url
        self.transport = transport
        self.sleep = sleep
        self.client = None

    def _optional_proxy(self, session_id: str | None = None) -> str | None:
        has_proxy = any(
            os.getenv(key)
            for key in (
                "GOOGLE_JOBS_PROXY_URL",
                "WEBSHARE_RESIDENTIAL_PROXY_URL",
                "WEBSHARE_RESIDENTIAL_PROXY_HOST",
                "WEBSHARE_PROXY_DOMAIN",
            )
        )
        if not has_proxy:
            return None
        return _proxy_url_string(proxy_configuration(session_id))

    def ready(self):
        private_trawl_url(self.base_url)
        self._optional_proxy()

    async def search(self, payload: ScrapeInput, page_token: str | None = None, session_id: str | None = None) -> JobPage:
        base = private_trawl_url(self.base_url)
        proxy_url = self._optional_proxy(session_id)
        target_url = google_search_url(payload)
        if self.client is None:
            self.client = httpx.AsyncClient(
                transport=self.transport,
                trust_env=False,
                follow_redirects=False,
                timeout=httpx.Timeout(55, connect=5, pool=5),
            )
        body = {
            "url": target_url,
            "skipHttp": True,
            "maxTier": 3,
            "omitRawBody": True,
            "settleTimeout": 2500,
            "maxTimeout": 45_000,
        }
        if proxy_url:
            body["proxy"] = proxy_url

        for attempt in range(2):
            try:
                async with self.client.stream("POST", f"{base}/scrape", json=body, headers={"Accept": "application/json"}) as response:
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > MAX_BROWSER_BYTES:
                            raise ScraperError("Google Jobs Trawl response exceeds the page limit", 502)
                    if response.status_code in {429, 502, 503, 504} and attempt == 0:
                        await self.sleep(0.5 + random.uniform(0, 0.25))
                        continue
                    if response.status_code != 200:
                        raise ScraperError(
                            "Google Jobs Trawl browser could not retrieve public results", 503 if response.status_code == 429 else 502
                        )
                    try:
                        document = json.loads(data)
                    except ValueError:
                        raise ScraperError("Google Jobs Trawl browser returned invalid JSON", 502) from None
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if attempt == 0:
                    await self.sleep(0.5 + random.uniform(0, 0.25))
                    continue
                raise ScraperError("Google Jobs Trawl browser is temporarily unavailable", 503) from None
            except httpx.TimeoutException:
                raise ScraperError("Google Jobs Trawl browser timed out", 504) from None
            except httpx.HTTPError:
                raise ScraperError("Google Jobs Trawl browser request failed", 503) from None

            landed_url = document.get("url") if isinstance(document, dict) else None
            status_code = document.get("statusCode") if isinstance(document, dict) else None
            html = document.get("html") if isinstance(document, dict) else None
            if not isinstance(landed_url, str) or not isinstance(html, str):
                raise ScraperError("Google Jobs Trawl browser returned invalid data", 502)
            if _is_challenge(landed_url, html):
                raise TrafficChallengeError()
            parsed = urlsplit(landed_url)
            if (parsed.hostname or "").lower() not in ALLOWED_GOOGLE_HOSTS or parsed.path != "/search" or status_code != 200:
                raise ScraperError("Google Jobs Trawl browser did not reach Google results", 502)
            return JobPage(parse_jobs_html(html), None)

        raise ScraperError("Google Jobs Trawl browser could not retrieve public results", 502)

    async def close(self):
        if self.client:
            await self.client.aclose()
            self.client = None
