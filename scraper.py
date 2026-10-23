"""Normalize and collect public Google Jobs with a self-hosted browser."""

import asyncio
import hashlib
import ipaddress
from datetime import datetime, timezone
from secrets import randbelow
from urllib.parse import unquote, urlsplit, urlunsplit

import re

from models import ScrapeInput


class ScraperError(Exception):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


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
    def __init__(self, browser=None):
        self.browser = browser

    async def collect(self, payload: ScrapeInput, state=None, checkpoint=None):
        from browser import GoogleJobsBrowser

        transport = self.browser or GoogleJobsBrowser()
        state = state or {}
        rows = list(state.get("results", []))[: payload.max_total_results]
        seen = set(state.get("processed_ids", [])) | {row["job_id"] for row in rows}
        fetched_at = state.get("fetched_at") or datetime.now(timezone.utc).isoformat()
        if state.get("exhausted") or len(rows) >= payload.max_total_results:
            return rows
        replay_pages = state.get("page_count", 0)
        session_id = state.get("proxy_session") or str(randbelow(900000000) + 100000000)
        stalls = 0
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
                                }
                            )
                        if len(rows) >= payload.max_total_results or exhausted:
                            break
                        await transport.advance(page)
        except TimeoutError:
            raise ScraperError("Google Jobs search exceeded its overall timeout", 504) from None
        except ScraperError:
            raise
        except Exception:
            # Browser exceptions often contain authenticated proxy configuration.
            raise ScraperError("Google Jobs browser could not complete this search") from None
        return rows
