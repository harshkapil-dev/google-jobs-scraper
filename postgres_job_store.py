"""Short-lived Postgres operations with atomic job leases and no idle polling."""

from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scraper import ScraperError


class PostgresJobStore:
    def __init__(self, url):
        self.url = url

    @contextmanager
    def connection(self):
        try:
            with psycopg.connect(self.url, connect_timeout=5, row_factory=dict_row) as connection:
                connection.execute("SET LOCAL statement_timeout = '5s'")
                yield connection
        except psycopg.Error:
            raise ScraperError("Google Jobs storage is temporarily unavailable", 503) from None

    def ready(self):
        with self.connection() as connection:
            connection.execute("SELECT 1 FROM google_jobs_worker_jobs LIMIT 1")

    def get(self, key):
        with self.connection() as connection:
            row = connection.execute("SELECT payload FROM google_jobs_worker_jobs WHERE key=%s AND expires_at>now()", (key,)).fetchone()
            return row["payload"] if row else None

    def create(self, key, value):
        with self.connection() as connection:
            return bool(
                connection.execute(
                    "INSERT INTO google_jobs_worker_jobs(key,payload,expires_at) VALUES(%s,%s,now()+interval '24 hours') "
                    "ON CONFLICT(key) DO UPDATE SET payload=EXCLUDED.payload,expires_at=EXCLUDED.expires_at,"
                    "lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE google_jobs_worker_jobs.expires_at<=now() RETURNING key",
                    (key, Jsonb(value)),
                ).fetchone()
            )

    def claim(self, key, owner, seconds):
        with self.connection() as connection:
            return bool(
                connection.execute(
                    "UPDATE google_jobs_worker_jobs SET lease_owner=%s,lease_until=now()+%s*interval '1 second' "
                    "WHERE key=%s AND expires_at>now() AND payload->>'status' NOT IN ('done','failed','cancelled') "
                    "AND (lease_until IS NULL OR lease_until<=now()) RETURNING key",
                    (owner, seconds, key),
                ).fetchone()
            )

    def save(self, key, owner, value):
        with self.connection() as connection:
            return bool(
                connection.execute(
                    "UPDATE google_jobs_worker_jobs SET payload=%s,expires_at=now()+interval '24 hours',updated_at=now() "
                    "WHERE key=%s AND lease_owner=%s AND lease_until>now() AND expires_at>now() "
                    "AND payload->>'status' NOT IN ('done','failed','cancelled') RETURNING key",
                    (Jsonb(value), key, owner),
                ).fetchone()
            )

    def release(self, key, owner):
        with self.connection() as connection:
            connection.execute(
                "UPDATE google_jobs_worker_jobs SET lease_owner=NULL,lease_until=NULL WHERE key=%s AND lease_owner=%s", (key, owner)
            )

    def cancel(self, key):
        with self.connection() as connection:
            row = connection.execute(
                "SELECT payload FROM google_jobs_worker_jobs WHERE key=%s AND expires_at>now() FOR UPDATE", (key,)
            ).fetchone()
            if not row:
                return None
            value = row["payload"]
            if value["status"] not in {"done", "failed", "cancelled"}:
                value.update(status="cancelled", error=None)
                connection.execute(
                    "UPDATE google_jobs_worker_jobs SET payload=%s,lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE key=%s",
                    (Jsonb(value), key),
                )
            return value
