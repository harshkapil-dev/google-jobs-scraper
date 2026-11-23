"""Collect public Google Jobs from a browser or the owned SERP page API."""

import asyncio
import hashlib
import ipaddress
import os
import time
from datetime import datetime, timezone
from secrets import randbelow
from urllib.parse import unquote, urlsplit, urlunsplit

import re

from models import ScrapeInput


class ScraperError(Exception):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


class TrafficChallengeError(ScraperError):
    def __init__(self):
        super().__init__("Google Jobs requires a traffic challenge; collection stopped without retrying or solving", 503)


class ChallengeCircuit:
    """One worker process, one bounded cooldown; recovery is armed by a real request."""

    def __init__(self, cooldown_seconds=300, clock=time.monotonic):
        self.cooldown_seconds = cooldown_seconds
        self.clock = clock
        self.open_until = 0.0
        self.probing = False

    def check(self):
        if self.clock() < self.open_until or self.probing:
            raise ScraperError("Google Jobs source is cooling down after a traffic challenge", 503)

    def acquire(self):
        self.check()
        # After cooldown, permit only one request to test source recovery.
        self.probing = bool(self.open_until)

    def challenged(self):
        self.open_until = self.clock() + self.cooldown_seconds

    def success(self):
        self.open_until = 0.0

    def release(self):
        self.probing = False


def text(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def public_url(value):
    value = text(value)
    if not value:
        return None
    try:
        url = urlsplit(value)
        host = (url.hostname or "").lower()
        if url.scheme not in {"https", "http"} or url.username or url.password or url.port not in {None, 80, 443}:
            return None
        if not host or "." not in host or host.endswith((".local", ".internal", ".localhost")) or host == "localhost":
            return None
        try:
            if not ipaddress.ip_address(host).is_global:
                return None
        except ValueError:
            pass
        return urlunsplit((url.scheme, url.netloc, url.path, url.query, url.fragment))
    except ValueError:
        return None


def normalize_job(job: dict, payload: ScrapeInput, position: int, fetched_at: str):
    title, company = text(job.get("title")), text(job.get("company_name"))
    if not title or not company:
        raise ScraperError("Google Jobs exposed an invalid job record")
    options = []
    for option in job.get("apply_options", []) if isinstance(job.get("apply_options"), list) else []:
        if isinstance(option, dict) and (link := public_url(option.get("link"))):
            entry = {"title": text(option.get("title")), "link": link}
            if entry not in options:
                options.append(entry)
    source_id = text(job.get("job_id"))
    if not source_id:
        link = unquote(text(job.get("share_link")) or "")
        match = re.search(r"(?:docid[=/]|htidocid=)([^&#/]+)", link)
        if match:
            source_id = match.group(1)
    fallback = "|".join([title.casefold(), company.casefold(), text(job.get("location")) or "", options[0]["link"] if options else ""])
    job_id = source_id or hashlib.sha256(fallback.encode()).hexdigest()
    detected = dict(job.get("detected_extensions") or {}) if isinstance(job.get("detected_extensions"), dict) else {}
    extensions = job.get("extensions") if isinstance(job.get("extensions"), list) else []
    for extension in extensions:
        if not isinstance(extension, str):
            continue
        if re.search(r"ago$|just posted|today|yesterday", extension, re.I):
            detected.setdefault("posted_at", extension)
        if re.search(r"full.?time|part.?time|contract|internship|intern", extension, re.I):
            detected.setdefault("schedule_type", extension)
        if re.search(r"work from home|fully remote|^remote$", extension, re.I):
            detected["work_from_home"] = True
        if re.search(r"^[\$£€₹]|\b(?:USD|INR|GBP|EUR)\b", extension, re.I):
            detected.setdefault("salary", extension)
    # A missing remote indicator is unknown, never a fabricated false value.
    remote = detected.get("work_from_home")
    remote = remote if isinstance(remote, bool) else None
    job_url = public_url(job.get("share_link")) or public_url(job.get("job_url"))
    return {
        "job_id": job_id,
        "title": title,
        "company_name": company,
        "location": text(job.get("location")),
        "via": text(job.get("via")),
        "description": text(job.get("description")),
        "job_url": job_url,
        "apply_url": options[0]["link"] if options else None,
        "apply_options": options,
        "posted_at": text(detected.get("posted_at")),
        "salary": text(detected.get("salary")),
        "job_type": text(detected.get("schedule_type")),
        "work_from_home": remote,
        "company_logo": public_url(job.get("thumbnail")),
        "job_highlights": job.get("job_highlights") if isinstance(job.get("job_highlights"), list) else [],
        "extensions": job.get("extensions") if isinstance(job.get("extensions"), list) else [],
        "detected_extensions": detected,
        "position": position,
        "search_query": payload.query,
        "search_location": payload.location or None,
        "fetched_at": fetched_at,
    }


def matches_filters(row, payload):
    if payload.remote_only and row["work_from_home"] is not True:
        return False
    types = {"full_time": r"full.?time", "part_time": r"part.?time", "contract": r"contract", "internship": r"intern"}
    if payload.job_type != "any" and not re.search(types[payload.job_type], row["job_type"] or "", re.I):
        return False
    if payload.date_posted != "any":
        posted = re.sub(r"^posted\s+", "", (row["posted_at"] or "").strip().lower())
        if posted in {"just posted", "today"}:
            hours = 0
        elif posted == "yesterday":
            hours = 24
        else:
            match = re.fullmatch(r"(\d+)\s+(minute|hour|day|week|month)s? ago", posted)
            if not match:
                return False
            hours = int(match[1]) * {"minute": 1 / 60, "hour": 1, "day": 24, "week": 168, "month": 720}[match[2]]
        if hours > {"past_24_hours": 24, "past_3_days": 72, "past_week": 168, "past_month": 720}[payload.date_posted]:
            return False
    return True


class GoogleJobsScraper:
    def __init__(self, browser=None, source=None, breaker=None):
        self.browser = browser
        self.source = source
        self.breaker = breaker or ChallengeCircuit()

    def source_kind(self):
        kind = "serp" if self.source is not None else os.getenv("GOOGLE_JOBS_SOURCE", "browser")
        if kind not in {"browser", "serp", "trawl"}:
            raise ScraperError("Google Jobs source must be browser, serp, or trawl", 503)
        return kind

    def ready(self):
        kind = self.source_kind()
        if kind == "serp":
            from serp_source import OwnedSerpSource

            (self.source or OwnedSerpSource()).ready()
        elif kind == "trawl":
            from trawl_source import TrawlJobsSource

            (self.source or TrawlJobsSource()).ready()
        else:
            from browser import proxy_configuration

            proxy_configuration()
        self.breaker.check()

    async def collect(self, payload: ScrapeInput, state=None, checkpoint=None):
        kind = self.source_kind()
        if kind in {"serp", "trawl"}:
            return await self.collect_api(payload, state, checkpoint, kind=kind)
        from browser import GoogleJobsBrowser

        transport = self.browser or GoogleJobsBrowser()
        state = state or {}
        rows = list(state.get("results", []))[: payload.max_total_results]
        seen = set(state.get("processed_ids", [])) | {row["job_id"] for row in rows}
        fetched_at = state.get("fetched_at") or datetime.now(timezone.utc).isoformat()
        if state.get("exhausted") or len(rows) >= payload.max_total_results:
            return rows
        if state.get("source_kind") not in {None, "browser"}:
            raise ScraperError("Google Jobs cannot switch sources while resuming a saved page", 503)
        replay_pages = state.get("page_count", 0)
        session_id = state.get("proxy_session") or str(randbelow(900000000) + 100000000)
        stalls = 0
        self.breaker.acquire()
        try:
            async with asyncio.timeout(payload.timeout_seconds):
                async with transport.session(payload, session_id=session_id) as page:
                    # Replays the loaded list after restart; durable IDs avoid duplicate rows.
                    for page_number in range(payload.max_pages):
                        cards = await transport.cards(page)
                        new_ids = 0
                        for card in cards:
                            identifier = await transport.identifier(card)
                            if identifier and identifier in seen:
                                continue
                            raw = await transport.extract(card, page)
                            row = normalize_job(raw, payload, len(rows) + 1, fetched_at)
                            if row["job_id"] in seen:
                                continue
                            seen.add(row["job_id"])
                            new_ids += 1
                            if matches_filters(row, payload):
                                rows.append(row)
                            snapshot = {
                                "results": rows,
                                "processed_ids": list(seen),
                                "page_count": max(replay_pages, page_number + 1),
                                "fetched_at": fetched_at,
                                "exhausted": False,
                                "source_kind": "browser",
                            }
                            if checkpoint:
                                await checkpoint(snapshot)
                            if len(rows) == payload.max_total_results:
                                break
                        stalls = stalls + 1 if not new_ids and page_number + 1 >= replay_pages else 0
                        exhausted = not cards or stalls >= 3
                        if checkpoint:
                            await checkpoint(
                                {
                                    "results": rows,
                                    "processed_ids": list(seen),
                                    "page_count": max(replay_pages, page_number + 1),
                                    "fetched_at": fetched_at,
                                    "exhausted": exhausted,
                                    "source_kind": "browser",
                                }
                            )
                        if len(rows) >= payload.max_total_results or exhausted:
                            break
                        await transport.advance(page)
            self.breaker.success()
        except TrafficChallengeError:
            self.breaker.challenged()
            raise
        except TimeoutError:
            raise ScraperError("Google Jobs search exceeded its overall timeout", 504) from None
        except ScraperError:
            raise
        except Exception:
            # Browser exceptions often contain authenticated proxy configuration.
            raise ScraperError("Google Jobs browser could not complete this search") from None
        finally:
            self.breaker.release()
        return rows

    async def collect_api(self, payload, state=None, checkpoint=None, *, kind="serp"):
        if self.source is not None:
            source = self.source
        elif kind == "trawl":
            from trawl_source import TrawlJobsSource

            source = TrawlJobsSource()
        else:
            from serp_source import OwnedSerpSource

            source = OwnedSerpSource()
        state = state or {}
        rows = list(state.get("results", []))[: payload.max_total_results]
        if state.get("exhausted") or len(rows) >= payload.max_total_results:
            return rows
        if (
            state.get("source_kind") not in {None, kind}
            or (state.get("results") or state.get("processed_ids") or state.get("page_count"))
            and state.get("source_kind") is None
        ):
            raise ScraperError("Google Jobs cannot switch sources while resuming a saved page", 503)
        seen = set(state.get("processed_ids", [])) | {row["job_id"] for row in rows}
        fetched_at = state.get("fetched_at") or datetime.now(timezone.utc).isoformat()
        page_number = max(0, state.get("page_count", 0) - int(bool(state.get("page_in_progress"))))
        token = state.get("page_token")
        token_hashes = set(state.get("page_token_hashes", []))
        stalls = 0
        self.breaker.acquire()
        try:
            async with asyncio.timeout(payload.timeout_seconds):
                while page_number < payload.max_pages:
                    page = await source.search(payload, token)
                    new_ids = 0

                    async def save(*, in_progress, exhausted, cursor):
                        if checkpoint:
                            await checkpoint(
                                {
                                    "results": list(rows),
                                    "processed_ids": sorted(seen),
                                    "fetched_at": fetched_at,
                                    "page_count": page_number + 1,
                                    "page_in_progress": in_progress,
                                    "page_token": cursor,
                                    "page_token_hashes": sorted(token_hashes),
                                    "source_kind": kind,
                                    "exhausted": exhausted,
                                }
                            )

                    for raw in page.jobs:
                        row = normalize_job(raw, payload, len(rows) + 1, fetched_at)
                        if row["job_id"] in seen:
                            continue
                        seen.add(row["job_id"])
                        new_ids += 1
                        if matches_filters(row, payload):
                            rows.append(row)
                        await save(in_progress=True, exhausted=False, cursor=token)
                        if len(rows) >= payload.max_total_results:
                            break
                    stalls = stalls + 1 if not new_ids else 0
                    next_token = page.next_page_token
                    digest = hashlib.sha256(next_token.encode()).hexdigest() if next_token else None
                    exhausted = not page.jobs or next_token is None or next_token == token or digest in token_hashes or stalls >= 3
                    limited = len(rows) >= payload.max_total_results
                    if digest:
                        token_hashes.add(digest)
                    await save(in_progress=limited, exhausted=exhausted and not limited, cursor=token if limited else next_token)
                    if limited or exhausted:
                        break
                    token = next_token
                    page_number += 1
            self.breaker.success()
        except TrafficChallengeError:
            self.breaker.challenged()
            raise
        except TimeoutError:
            raise ScraperError("Google Jobs search exceeded its overall timeout", 504) from None
        except ScraperError:
            raise
        except Exception:
            raise ScraperError("Owned SERP could not complete this Google Jobs search") from None
        finally:
            try:
                await source.close()
            finally:
                self.breaker.release()
        return rows
