"""Durable state with per-job leases. No background connections or idle polling."""

import asyncio
import json
import os
import time
from contextlib import contextmanager
from threading import Lock

import redis

from scraper import ScraperError
from postgres_job_store import PostgresJobStore

TERMINAL = {"done", "failed", "cancelled"}


class JobStore:
    def __init__(self):
        self.memory, self.leases, self.lock = {}, {}, Lock()
        url = os.getenv("PULLDATA_RESULTS_DATABASE_URL") or os.getenv("DATABASE_URL")
        self.postgres = PostgresJobStore(url) if url else None

    @contextmanager
    def connection(self):
        url = os.getenv("SCRAPER_QUEUE_REDIS_URL") or os.getenv("REDIS_URL")
        if not url:
            if os.getenv("GOOGLE_JOBS_ALLOW_MEMORY_STORE") != "true" or os.getenv("NODE_ENV") == "production":
                raise ScraperError("Durable Google Jobs storage is not configured", 503)
            yield None
            return
        client = redis.Redis.from_url(url, socket_connect_timeout=3, socket_timeout=3, health_check_interval=0, decode_responses=True)
        try:
            yield client
        except redis.RedisError:
            raise ScraperError("Google Jobs storage is temporarily unavailable", 503) from None
        finally:
            client.close()

    def key(self, job_id):
        namespace = os.getenv("SCRAPER_QUEUE_NAMESPACE", "").strip()
        if not namespace and os.getenv("GOOGLE_JOBS_ALLOW_MEMORY_STORE") != "true":
            raise ScraperError("Google Jobs queue namespace is not configured", 503)
        return f"marketplace:worker:{namespace or 'local'}:google-jobs-scraper:{job_id}"

    def _get(self, job_id):
        if self.postgres:
            return self.postgres.get(self.key(job_id))
        with self.connection() as client:
            key = self.key(job_id)
            if client:
                raw = client.get(key)
            else:
                with self.lock:
                    record = self.memory.get(key)
                    raw = record[0] if record and record[1] > time.time() else None
            return json.loads(raw) if raw else None

    def _create(self, job_id, value):
        if self.postgres:
            return self.postgres.create(self.key(job_id), value)
        raw = json.dumps(value, allow_nan=False)
        with self.connection() as client:
            key = self.key(job_id)
            if client:
                return bool(client.set(key, raw, ex=86400, nx=True))
            with self.lock:
                if key in self.memory and self.memory[key][1] > time.time():
                    return False
                self.memory[key] = (raw, time.time() + 86400)
                return True

    def _claim(self, job_id, owner, seconds):
        if self.postgres:
            return self.postgres.claim(self.key(job_id), owner, seconds)
        with self.connection() as client:
            key = self.key(job_id) + ":lease"
            if client:
                return bool(client.set(key, owner, ex=seconds, nx=True))
            with self.lock:
                if key in self.leases and self.leases[key][1] > time.time():
                    return False
                self.leases[key] = (owner, time.time() + seconds)
                return True

    def _save(self, job_id, owner, value):
        if self.postgres:
            return self.postgres.save(self.key(job_id), owner, value)
        raw = json.dumps(value, allow_nan=False)
        with self.connection() as client:
            key = self.key(job_id)
            if client:
                return bool(
                    client.eval(
                        """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
local old = redis.call('GET', KEYS[1])
if old then
 local status = cjson.decode(old).status
 if status == 'done' or status == 'failed' or status == 'cancelled' then return 0 end
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', 86400)
return 1
""",
                        2,
                        key,
                        key + ":lease",
                        owner,
                        raw,
                    )
                )
            with self.lock:
                lease = self.leases.get(key + ":lease")
                if not lease or lease[0] != owner or lease[1] <= time.time():
                    return False
                previous = self.memory.get(key)
                if previous and json.loads(previous[0])["status"] in TERMINAL:
                    return False
                self.memory[key] = (raw, time.time() + 86400)
                return True

    def _release(self, job_id, owner):
        if self.postgres:
            return self.postgres.release(self.key(job_id), owner)
        with self.connection() as client:
            key = self.key(job_id) + ":lease"
            if client:
                client.eval("if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0", 1, key, owner)
            else:
                with self.lock:
                    if self.leases.get(key, (None,))[0] == owner:
                        self.leases.pop(key, None)

    def _cancel(self, job_id):
        if self.postgres:
            return self.postgres.cancel(self.key(job_id))
        with self.connection() as client:
            key = self.key(job_id)
            if client:
                for _ in range(3):
                    with client.pipeline() as transaction:
                        try:
                            transaction.watch(key)
                            raw = transaction.get(key)
                            if not raw:
                                transaction.unwatch()
                                return None
                            value = json.loads(raw)
                            if value["status"] in TERMINAL:
                                transaction.unwatch()
                                return value
                            value.update(status="cancelled", error=None)
                            transaction.multi()
                            transaction.set(key, json.dumps(value), ex=86400)
                            transaction.delete(key + ":lease")
                            transaction.execute()
                            return value
                        except redis.WatchError:
                            continue
                raise ScraperError("Google Jobs cancellation conflicted with a progress update; retry the request", 503)
            else:
                with self.lock:
                    stored = self.memory.get(key)
                    if not stored or stored[1] <= time.time():
                        return None
                    value = json.loads(stored[0])
                    if value["status"] not in TERMINAL:
                        value.update(status="cancelled", error=None)
                        self.memory[key] = (json.dumps(value), stored[1])
                        self.leases.pop(key + ":lease", None)
                    raw = self.memory[key][0]
            return json.loads(raw) if raw else None

    def ready(self):
        self.key("readiness")
        if self.postgres:
            self.postgres.ready()
        else:
            with self.connection() as client:
                if client:
                    client.ping()

    async def cancel(self, job_id):
        return await asyncio.to_thread(self._cancel, job_id)

    async def get(self, job_id):
        return await asyncio.to_thread(self._get, job_id)

    async def create(self, job_id, value):
        return await asyncio.to_thread(self._create, job_id, value)

    async def claim(self, job_id, owner, seconds):
        return await asyncio.to_thread(self._claim, job_id, owner, seconds)

    async def save(self, job_id, owner, value):
        return await asyncio.to_thread(self._save, job_id, owner, value)

    async def release(self, job_id, owner):
        return await asyncio.to_thread(self._release, job_id, owner)
