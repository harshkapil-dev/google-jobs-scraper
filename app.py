import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5
from secrets import randbelow

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from job_store import JobStore, TERMINAL
from models import ScrapeInput
from scraper import GoogleJobsScraper, ScraperError

logger = logging.getLogger("google-jobs-scraper")
store = JobStore()
collector = GoogleJobsScraper()
tasks: dict[str, asyncio.Task] = {}
# Deliberately small: each job performs serial, bounded upstream pagination.
slots = asyncio.Semaphore(2)
MAX_PENDING = 8
PLATFORM_FIELDS = {
    "source",
    "run_id",
    "request_id",
    "user_id",
    "queue_namespace",
    "callback_url",
    "effective_max_total_results",
    "affordable_rows_at_start",
    "affordable_rows_at_dispatch",
    "available_credits_at_start",
    "wallet_balance_cents_at_start",
    "wallet_balance_cents_at_dispatch",
    "wallet_reserved_cents_at_dispatch",
    "wallet_unreserved_cents_at_dispatch",
    "wallet_concurrency_mode",
    "max_leads_per_query",
    "scrape_emails",
    "custom_fields",
    "task_name",
}


def log(event, job_id):
    logger.warning(json.dumps({"scraper_slug": "google-jobs-scraper", "job_id": job_id, "event": event}))


@asynccontextmanager
async def lifespan(_app):
    yield
    pending = list(tasks.values())
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


app = FastAPI(title="Google Jobs Scraper - Pulldata Worker", lifespan=lifespan)


def authenticate(authorization: str | None = Header(default=None), x_internal_secret: str | None = Header(default=None)):
    token = os.getenv("PULLDATA_WORKER_TOKEN", "")
    internal = os.getenv("INTERNAL_API_SECRET", "")
    if token:
        if not hmac.compare_digest(authorization or "", f"Bearer {token}"):
            raise HTTPException(401, "Unauthorized")
    elif internal and not hmac.compare_digest(x_internal_secret or "", internal):
        raise HTTPException(401, "Unauthorized")


@app.exception_handler(ScraperError)
async def scraper_error(_request, error):
    return JSONResponse(status_code=error.status, content={"error": str(error)})


@app.exception_handler(RequestValidationError)
async def validation_error(_request, _error):
    return JSONResponse(status_code=422, content={"error": "Invalid Google Jobs request parameters"})


@app.exception_handler(Exception)
async def unexpected_error(_request, _error):
    return JSONResponse(status_code=500, content={"error": "Google Jobs request could not be completed"})


async def parse_input(request: Request):
    # Do not echo validation inputs: a caller may have accidentally pasted credentials.
    raw = await request.body()
    if len(raw) > 65536:
        raise HTTPException(413, "Request is too large")
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError()
        payload = ScrapeInput.model_validate({k: v for k, v in data.items() if k not in PLATFORM_FIELDS})
        identity = data.get("request_id") or data.get("run_id")
        if identity is not None and (not isinstance(identity, str) or len(identity) > 128):
            raise ValueError()
        job_id = str(uuid5(NAMESPACE_URL, "google-jobs-scraper:" + identity)) if identity else str(uuid4())
        fingerprint = hashlib.sha256(json.dumps(payload.model_dump(), sort_keys=True).encode()).hexdigest()
        return payload, job_id, fingerprint
    except (ValueError, ValidationError):
        raise HTTPException(422, "Invalid Google Jobs request parameters") from None


async def execute(job_id):
    owner = str(uuid4())
    claimed = False
    try:
        async with slots:
            job = await store.get(job_id)
            if not job or job["status"] in TERMINAL:
                return
            payload = ScrapeInput.model_validate(job["input"])
            claimed = await store.claim(job_id, owner, payload.timeout_seconds + 30)
            if not claimed:
                return
            if time.time() > job["deadline"]:
                job.update(status="failed", error="Google Jobs search exceeded its recovery deadline")
                await store.save(job_id, owner, job)
                return
            job.update(status="running")
            if not await store.save(job_id, owner, job):
                return
            log("running", job_id)

            async def checkpoint(state):
                job.update(state)
                if not await store.save(job_id, owner, job):
                    raise ScraperError("Google Jobs job lease is no longer active", 503)

            try:
                rows = await collector.collect(payload, job, checkpoint)
                job.update(status="done", results=rows, error=None)
            except ScraperError as error:
                if error.status == 503 and "storage" in str(error).lower():
                    # Keep durable active state resumable if the storage connection drops.
                    return
                job.update(status="failed", error=str(error))
            except asyncio.CancelledError:
                # A rolling restart leaves persisted pages and a resumable active job.
                log("interrupted", job_id)
                raise
            except Exception:
                job.update(status="failed", error="Google Jobs search could not be completed")
            if await store.save(job_id, owner, job):
                log(job["status"], job_id)
    except ScraperError:
        log("storage_unavailable", job_id)
    finally:
        if claimed:
            with suppress(ScraperError):
                await store.release(job_id, owner)


def schedule(job_id):
    if job_id in tasks:
        return
    if len(tasks) >= MAX_PENDING:
        raise ScraperError("Google Jobs worker capacity is currently full", 429)
    task = asyncio.create_task(execute(job_id))
    tasks[job_id] = task
    task.add_done_callback(lambda done: tasks.pop(job_id, None))


async def start(request):
    payload, job_id, fingerprint = await parse_input(request)
    previous = await store.get(job_id)
    if previous and previous["fingerprint"] != fingerprint:
        raise HTTPException(409, "Request identity was already used with different parameters")
    if not previous:
        if len(tasks) >= MAX_PENDING:
            raise ScraperError("Google Jobs worker capacity is currently full", 429)
        await store.create(
            job_id,
            {
                "status": "queued",
                "input": payload.model_dump(),
                "fingerprint": fingerprint,
                "proxy_session": str(randbelow(900000000) + 100000000),
                "results": [],
                "deadline": time.time() + payload.timeout_seconds * 3,
                "error": None,
            },
        )
    job = await store.get(job_id)
    if job["fingerprint"] != fingerprint:
        raise HTTPException(409, "Request identity was already used with different parameters")
    if job["status"] not in TERMINAL:
        schedule(job_id)
    return payload, job_id, job


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/ready", dependencies=[Depends(authenticate)])
def ready():
    from browser import proxy_configuration

    proxy_configuration()
    store.ready()
    return {"ready": True}


@app.post("/scrape", dependencies=[Depends(authenticate)])
async def scrape(request: Request):
    _payload, job_id, job = await start(request)
    return {"job_id": job_id, "status": job["status"]}


@app.post("/scrape-sync", dependencies=[Depends(authenticate)])
async def scrape_sync(request: Request):
    payload, job_id, job = await start(request)
    try:
        async with asyncio.timeout(payload.timeout_seconds + 10):
            while job["status"] not in TERMINAL:
                await asyncio.sleep(0.1)
                job = await store.get(job_id)
    except TimeoutError:
        raise ScraperError("Google Jobs search is still running; use the job status endpoint", 504) from None
    if job["status"] != "done":
        raise ScraperError(job.get("error") or "Google Jobs search failed")
    return {"job_id": job_id, "status": "done", "total": len(job["results"]), "results": job["results"]}


async def find_job(job_id):
    job = await store.get(str(job_id))
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] not in TERMINAL:
        schedule(str(job_id))
    return job


@app.get("/job/{job_id}", dependencies=[Depends(authenticate)])
async def job_status(job_id: UUID):
    job = await find_job(job_id)
    return {"job_id": str(job_id), "status": job["status"], "total": len(job["results"]), "error": job.get("error")}


@app.get("/job/{job_id}/results", dependencies=[Depends(authenticate)])
async def job_results(job_id: UUID, offset: int = Query(default=0, ge=0, le=100000), limit: int = Query(default=25, ge=1, le=1000)):
    job = await find_job(job_id)
    return {"total": len(job["results"]), "results": job["results"][offset : offset + limit]}


@app.post("/job/{job_id}/cancel", dependencies=[Depends(authenticate)])
@app.post("/job/{job_id}/stop", dependencies=[Depends(authenticate)])
async def cancel_job(job_id: UUID):
    identity = str(job_id)
    job = await store.cancel(identity)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] == "cancelled":
        task = tasks.get(identity)
        if task:
            task.cancel()
    return {"job_id": identity, "status": job["status"], "total": len(job["results"])}
