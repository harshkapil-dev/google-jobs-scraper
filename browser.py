"""Browser transport for public Google Jobs. Credentials come from runtime only."""

import asyncio
import json
import os
import random
import re
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlencode, urlsplit

from playwright.async_api import Error as BrowserError, TimeoutError as BrowserTimeout, async_playwright

from scraper import ScraperError, TrafficChallengeError

CARD_SELECTOR = '.MQUd2b, .gws-plugins-horizon-jobs__li-ed, .iFjolb, [data-job-id]:not([data-job-detail]):not([role="dialog"]), [data-htidocid]:not([data-job-detail]):not([role="dialog"])'
DESCRIPTION_SELECTOR = '.NgUYpe, .HBvzbc, [data-attrid="description"], [data-job-description]'
ALLOWED_GOOGLE_HOSTS = {
    "www.google.com",
    "google.com",
    "consent.google.com",
    "www.google.co.uk",
    "www.google.ca",
    "www.google.com.au",
    "www.google.de",
    "www.google.fr",
    "www.google.co.in",
}
DETAIL_SNAPSHOT = r"""({expected, excluded}) => {
  const visible = node => !!(node.offsetWidth || node.offsetHeight || node.getClientRects().length);
  const norm = value => (value || '').replace(/\s+/g, ' ').trim().toLowerCase();
  const descriptions = Array.from(document.querySelectorAll('.NgUYpe, .HBvzbc, [data-attrid="description"], [data-job-description]')).filter(visible);
  for (const description of descriptions) {
    const panel = description.closest('[role="dialog"], .gws-plugins-horizon-jobs__p-dtl, [data-job-detail]');
    if (!panel || panel.matches('[aria-busy="true"]') || Array.from(panel.querySelectorAll('[aria-busy="true"], [role="progressbar"]')).some(visible)) continue;
    const id = panel.getAttribute('data-job-id') || panel.getAttribute('data-htidocid');
    if (id && expected.job_id && id !== expected.job_id) continue;
    const heading = Array.from(panel.querySelectorAll('h1, h2, [role="heading"], [data-job-title]')).filter(visible);
    const company = Array.from(panel.querySelectorAll('.vNEEBe, .nJlQNd, .a3jPc, [data-job-company]')).filter(visible);
    if (!heading.some(node => norm(node.textContent) === norm(expected.title))) continue;
    if (!company.some(node => norm(node.textContent) === norm(expected.company_name))) continue;
    const text = (description.innerText || description.textContent || '').trim();
    if (!text || excluded.includes(text)) continue;
    return {
      description: text,
      apply_options: Array.from(panel.querySelectorAll('a[href]')).filter(a => visible(a) && /apply|application/i.test(a.innerText || a.getAttribute('aria-label') || '')).map(a => ({title: a.innerText || a.getAttribute('aria-label'), link:a.href})),
      job_highlights: Array.from(panel.querySelectorAll('[data-job-highlight], .YgLbBe')).filter(visible).map(section => ({title: section.querySelector('h3, [role="heading"]')?.textContent?.trim() || null, items: Array.from(section.querySelectorAll('li')).map(li=>li.textContent.trim())}))
    };
  }
  return false;
}"""
DATE_LABELS = {
    "past_24_hours": ["Past 24 hours", "Past day", "Yesterday"],
    "past_3_days": ["Past 3 days", "Last 3 days", "Past week"],
    "past_week": ["Past week", "Last week"],
    "past_month": ["Past month", "Last month"],
}
TYPE_LABELS = {
    "full_time": ["Full-time", "Full time"],
    "part_time": ["Part-time", "Part time"],
    "contract": ["Contract", "Contractor"],
    "internship": ["Internship", "Intern"],
}


def sticky_proxy(proxy, session_id):
    host = urlsplit(proxy["server"]).hostname or ""
    username = proxy.get("username", "")
    # Webshare documents a numeric suffix for sticky sessions. Other gateways
    # and already-sticky credentials are passed through unchanged.
    mode = os.getenv("GOOGLE_JOBS_PROXY_SESSION_MODE", "rotating")
    if mode not in {"rotating", "sticky"}:
        raise ScraperError("Google Jobs proxy session mode is invalid", 503)
    if mode == "sticky" and session_id and (host == "webshare.io" or host.endswith(".webshare.io")) and username.endswith("-rotate"):
        proxy = {**proxy, "username": username[: -len("rotate")] + str(session_id)}
    return proxy


def proxy_configuration(session_id=None):
    raw = os.getenv("GOOGLE_JOBS_PROXY_URL") or os.getenv("WEBSHARE_RESIDENTIAL_PROXY_URL")
    if raw:
        try:
            url = urlsplit(raw)
            if url.scheme not in {"http", "https"} or not url.hostname or url.path not in {"", "/"} or url.query or url.fragment:
                raise ValueError()
            proxy = {
                "server": f"{url.scheme}://{url.hostname}:{url.port or (443 if url.scheme == 'https' else 80)}",
                "username": unquote(url.username or ""),
                "password": unquote(url.password or ""),
            }
            return sticky_proxy(proxy, session_id)
        except ValueError:
            raise ScraperError("Google Jobs proxy configuration is invalid", 503) from None
    host = os.getenv("WEBSHARE_RESIDENTIAL_PROXY_HOST") or os.getenv("WEBSHARE_PROXY_DOMAIN")
    port = os.getenv("WEBSHARE_RESIDENTIAL_PROXY_PORT") or os.getenv("WEBSHARE_PROXY_PORT")
    username = os.getenv("WEBSHARE_RESIDENTIAL_PROXY_USERNAME") or os.getenv("WEBSHARE_PROXY_USERNAME")
    password = os.getenv("WEBSHARE_RESIDENTIAL_PROXY_PASSWORD") or os.getenv("WEBSHARE_PROXY_PASSWORD")
    if not all([host, port, username, password]):
        raise ScraperError("Google Jobs residential proxy is not configured", 503)
    try:
        if not re.fullmatch(r"[a-zA-Z0-9.-]+", host) or not 1 <= int(port) <= 65535:
            raise ValueError()
    except ValueError:
        raise ScraperError("Google Jobs proxy configuration is invalid", 503) from None
    return sticky_proxy({"server": f"http://{host}:{port}", "username": username, "password": password}, session_id)


def google_search_url(payload):
    query = payload.query + (f" jobs in {payload.location}" if payload.location else " jobs")
    # Keep the browser controls in English. Job descriptions retain source text.
    return "https://www.google.com/search?" + urlencode({"q": query, "udm": "8", "hl": "en", "gl": payload.country})


def allowed_request(url):
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        return (
            parsed.scheme == "https"
            and parsed.port in {None, 443}
            and not parsed.username
            and not parsed.password
            and (host in ALLOWED_GOOGLE_HOSTS or host.endswith((".gstatic.com", ".googleusercontent.com")))
        )
    except ValueError:
        return False


class GoogleJobsBrowser:
    detail_timeout_ms = 10000

    async def pause(self):
        # Pace active browser interactions; correctness still depends on DOM identity.
        await asyncio.sleep(random.uniform(0.15, 0.4))

    @asynccontextmanager
    async def session(self, payload, session_id=None):
        async with async_playwright() as playwright:
            browser = None
            context = None
            try:
                browser = await playwright.chromium.launch(
                    headless=True,
                    proxy=proxy_configuration(session_id),
                    timeout=20000,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                context = await browser.new_context(locale="en-US", viewport={"width": 1440, "height": 1000}, service_workers="block")

                async def route(request_route):
                    request = request_route.request
                    if not allowed_request(request.url) or request.resource_type in {"image", "media", "font"}:
                        await request_route.abort()
                    else:
                        await request_route.continue_()

                await context.route("**/*", route)
                page = await context.new_page()
                page.set_default_timeout(8000)
                page.set_default_navigation_timeout(30000)
                for attempt in range(3):
                    try:
                        response = await page.goto(google_search_url(payload), wait_until="domcontentloaded")
                    except BrowserTimeout:
                        if attempt == 2:
                            raise ScraperError("Google Jobs navigation timed out", 504) from None
                    except BrowserError as error:
                        transient = any(code in str(error) for code in ["ERR_CONNECTION_RESET", "ERR_TIMED_OUT", "ERR_EMPTY_RESPONSE"])
                        if not transient or attempt == 2:
                            raise ScraperError("Google Jobs proxy or browser connection failed", 503) from None
                    else:
                        # A 429/5xx challenge is not a transient navigation failure.
                        # Detect it before the status-based retry branch.
                        await self.reject_challenge(page)
                        if response and response.status in {429, 500, 502, 503, 504}:
                            if attempt == 2:
                                raise ScraperError("Google Jobs is temporarily unavailable", 503)
                        elif response and response.status >= 400:
                            raise ScraperError("Google Jobs rejected this search")
                        else:
                            break
                    await asyncio.sleep(0.5 * 2**attempt + random.uniform(0, 0.25))
                await self.check_page(page)
                await self.apply_filters(page, payload)
                yield page
            finally:
                try:
                    if context:
                        await context.close()
                finally:
                    if browser:
                        await browser.close()

    async def reject_challenge(self, page):
        if "/sorry/" in page.url:
            raise TrafficChallengeError()
        body = await page.locator("body").inner_text(timeout=3000)
        if re.search(r"unusual traffic|verify you are human|not a robot", body, re.I):
            raise TrafficChallengeError()

    async def check_page(self, page):
        await self.reject_challenge(page)
        # Reject cookies through the normal page control when Google presents consent.
        reject = page.get_by_role("button", name=re.compile("Reject all", re.I))
        if await reject.count() and await reject.first.is_visible():
            await reject.first.click()
            await self.pause()
            await page.wait_for_load_state("domcontentloaded")

    async def select_filter(self, page, group_names, options):
        async def select_option():
            for label in options:
                for role in ["option", "menuitem", "menuitemradio", "checkbox", "button", "link"]:
                    choice = page.get_by_role(role, name=label, exact=True)
                    for item in await choice.all():
                        if await item.is_visible():
                            await item.click()
                            await self.pause()
                            return True
            return False

        # Some layouts expose chips immediately; others group them in a menu.
        if await select_option():
            return
        for name in group_names:
            button = page.get_by_role("button", name=re.compile(name, re.I))
            if await button.count() and await button.first.is_visible():
                await button.first.click()
                await self.pause()
                try:
                    await page.wait_for_function(
                        "labels => Array.from(document.querySelectorAll('[role=option], [role=menuitem], [role=menuitemradio], [role=checkbox], button, a')).some(node => node.getClientRects().length && labels.includes((node.textContent || '').trim()))",
                        arg=options,
                        timeout=3000,
                    )
                except BrowserTimeout:
                    continue
                if await select_option():
                    return
        raise ScraperError("Google Jobs could not apply the requested search filter")

    async def apply_filters(self, page, payload):
        if payload.date_posted != "any":
            await self.select_filter(page, ["Date posted", "Date"], DATE_LABELS[payload.date_posted])
        if payload.job_type != "any":
            await self.select_filter(page, ["Job type", "Type"], TYPE_LABELS[payload.job_type])
        if payload.remote_only:
            await self.select_filter(page, ["Work from home", "Location"], ["Remote", "Work from home"])
        await self.check_page(page)

    async def cards(self, page):
        cards = page.locator(CARD_SELECTOR)
        try:
            await cards.first.wait_for(state="visible", timeout=10000)
        except BrowserTimeout:
            await self.check_page(page)
            body = await page.locator("body").inner_text()
            if re.search(r"no (?:matching )?jobs|did not match any jobs|no results found", body, re.I):
                return []
            raise ScraperError("Google Jobs did not expose a supported jobs list") from None
        stable = []
        for card in await cards.all():
            key = await card.evaluate(
                "node => ['data-job-id','data-htidocid'].map(name => [name,node.getAttribute(name)]).find(item => item[1]) || null"
            )
            # nth() changes identity when Google reorders/virtualizes the list.
            stable.append(page.locator(f":is({CARD_SELECTOR})[{key[0]}={json.dumps(key[1])}]").first if key else card)
        return stable

    async def identifier(self, card):
        raw = await card.evaluate(
            "element => element.getAttribute('data-job-id') || element.getAttribute('data-htidocid') || element.querySelector('[data-entityid]')?.getAttribute('data-entityid') || element.querySelector('a[href*=docid], a[href*=htidocid]')?.href || null"
        )
        if raw and "://" in raw:
            match = re.search(r"(?:docid[=/]|htidocid=)([^&#/]+)", unquote(raw))
            return match[1] if match else None
        return raw

    async def extract(self, card, page):
        """Read only public rendered DOM, never private page application state."""
        raw = await card.evaluate("""element => {
          const pick = selectors => { for (const selector of selectors) { const node = element.querySelector(selector); if (node?.textContent?.trim()) return node.textContent.trim(); } return null; };
          const link = element.querySelector('a[href*="docid"], a[href*="htidocid"]');
          const tags = Array.from(element.querySelectorAll('.LL4CDc, .I2Cbhb, [data-job-extension]')).map(node => node.textContent.trim()).filter(Boolean);
          return {
            job_id: element.getAttribute('data-job-id') || element.getAttribute('data-htidocid') || element.querySelector('[data-entityid]')?.getAttribute('data-entityid'),
            title: pick(['.PUpOsf', '.BjJfJf', '[role="heading"]', 'h1', 'h2', 'h3']),
            company_name: pick(['.vNEEBe', '.nJlQNd', '.a3jPc']),
            location: pick(['.Qk80Jf', '.FqK3wc', '[data-job-location]']),
            via: pick(['.oNwCmf', '[data-job-via]']),
            thumbnail: element.querySelector('img')?.getAttribute('data-src') || element.querySelector('img')?.getAttribute('src'),
            share_link: link?.href || null, extensions: tags
          };
        }""")
        if not raw.get("title") or not raw.get("company_name"):
            raise ScraperError("Google Jobs layout has changed; job identity could not be verified")
        # An already visible description can belong to the previous card. Capture
        # it before clicking, and accept details only from the requested identity.
        current = await page.evaluate(DETAIL_SNAPSHOT, {"expected": raw, "excluded": []})
        excluded = [] if current else await page.locator(f":is({DESCRIPTION_SELECTOR}):visible").all_inner_texts()
        excluded = [value.strip() for value in excluded if value.strip()]
        await self.pause()
        # Open the Google-hosted detail panel. Never visit an application link.
        await card.click()
        await self.wait_detail(page, raw, excluded)
        expand = page.get_by_role("button", name=re.compile("Show full description", re.I))
        if await expand.count() and await expand.first.is_visible():
            await expand.first.click()
        more_apply = page.get_by_role("button", name=re.compile("More apply options|Show all apply options|See more apply options", re.I))
        if await more_apply.count() and await more_apply.first.is_visible():
            await more_apply.first.click()
        await self.pause()
        # Read identity and details in one DOM snapshot after expansions as well.
        raw.update(await self.wait_detail(page, raw, excluded))
        return raw

    async def wait_detail(self, page, raw, excluded):
        try:
            handle = await page.wait_for_function(
                DETAIL_SNAPSHOT, arg={"expected": raw, "excluded": excluded}, timeout=self.detail_timeout_ms, polling=100
            )
            try:
                return await handle.json_value()
            finally:
                await handle.dispose()
        except BrowserTimeout:
            await self.check_page(page)
            raise ScraperError("Google Jobs details did not finish loading for the selected job", 504) from None

    async def advance(self, page):
        cards = page.locator(CARD_SELECTOR)
        if not await cards.count():
            return
        previous = await cards.evaluate_all(
            "nodes => nodes.map(n => n.getAttribute('data-job-id') || n.getAttribute('data-htidocid') || n.textContent).join('|')"
        )
        await self.pause()
        await cards.last.scroll_into_view_if_needed()
        await cards.last.evaluate("""element => {
          let parent = element.parentElement;
          while (parent && parent !== document.body) {
            const style = getComputedStyle(parent);
            if (parent.scrollHeight > parent.clientHeight && /auto|scroll/.test(style.overflowY)) { parent.scrollTop = parent.scrollHeight; return; }
            parent = parent.parentElement;
          }
          document.scrollingElement.scrollTop = document.scrollingElement.scrollHeight;
        }""")
        # Wait for appended/replaced cards instead of assuming a fixed one-second load.
        try:
            await page.wait_for_function(
                "({selector, previous}) => Array.from(document.querySelectorAll(selector)).map(n => n.getAttribute('data-job-id') || n.getAttribute('data-htidocid') || n.textContent).join('|') !== previous",
                arg={"selector": CARD_SELECTOR, "previous": previous},
                timeout=5000,
                polling=100,
            )
        except BrowserTimeout:
            pass  # The collector bounds repeated unchanged pages and reports exhaustion.
        await self.check_page(page)
