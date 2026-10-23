# Google Jobs Scraper for Pulldata

Initial implementation of a self-hosted Chromium worker with residential proxies. Catalog category: **Jobs**. Slug: `google-jobs-scraper`. No search-data API provider or subscription is required.

**Release status: local development, disabled.** Live Google Jobs collection has not passed. A current server-proxy test on 23 October reached Google’s traffic-challenge page and returned no listings. Current network probes reached Google's traffic challenge or timed out through the configured proxy. Browser parser tests use authored deterministic markup; they do not establish compatibility with the current live Google layout. The development pipeline must require real rows before enabling the catalog entry; production publication remains gated.

## Input

| Field | Meaning | Default / bounds |
| --- | --- | --- |
| `query` | Job keyword, title, or employer | Required; 1–500 characters |
| `location` | City, state, and country | Optional; at most 500 characters |
| `country` | Two-letter Google country code | `us` |
| `date_posted` | `any`, `past_24_hours`, `past_3_days`, `past_week`, `past_month` | `any` |
| `job_type` | `any`, `full_time`, `part_time`, `contract`, `internship` | `any` |
| `remote_only` | Require an explicit remote/work-from-home indicator | `false` |
| `max_total_results` | Maximum unique matching rows | 100; 1–1,000 |
| `max_pages` | Safety cap on browser pagination batches | 100; 1–100 |
| `timeout_seconds` | Overall browser job timeout | 300; 30–300 seconds |

Google's visible filter controls are used before collection. The 3-day date filter can use Google's week filter followed by a stricter local check. Every returned row is checked against the requested date, job type, and remote filters. Unknown values are excluded when the associated filter is active. Unsupported filter controls fail clearly instead of silently ignoring the request. The interface language is English so those controls and relative posting labels can be interpreted consistently; descriptions retain source text.

Pagination scrolls the job list, collects new IDs, and stops at the requested maximum, source exhaustion, pagination cap, or timeout. A maximum is a ceiling, not a guarantee of that many available jobs. Source order is preserved. Unexposed job values are null; arrays are empty when the source exposes no entries.

## Results and exports

Each row includes the job ID, title, company, company logo, location, remote indicator, job type, source posting-time text, available salary, source/“via”, primary and all exposed application links, full exposed description, grouped highlights, original extensions, search context, position, and collection timestamp. The exact columns are in `pulldata.output.schema.json`.

Posting labels such as “2 days ago” remain source text. They are not converted into invented precise publication timestamps. IDs use Google's source identifier when exposed and a deterministic identity fallback otherwise. Application links are returned without visiting the application sites.

CSV, JSON, and Excel (`XLSX`) are configured in the existing Pulldata results interface. The backend Excel exporter reads every result page, preserves nested application links and highlights as JSON cells, and keeps strings beginning with `=` as literal text. Excel's 32,767-character cell limit produces a clear error directing the user to JSON rather than truncating content.

## Pulldata integration

- Dedicated endpoint: `POST /v1/google-jobs/search/results`.
- Existing asynchronous API: `POST /api/developer/v1/jobs` with `{ "scraperId": "google-jobs-scraper", "inputParams": { ... } }`.
- Existing job status, partial results, history, billing, cancellation, saved configurations, webhooks, and exports remain owned by Pulldata's shared services.
- Dashboard and public playground requests use the dedicated endpoint and the shared schema.
- No-code route: `/dashboard/no-code-tools/google-jobs-scraper` after development catalog staging.
- Documentation: `/docs/api/v1/google-jobs/search/results/post` and `/docs/api/google-jobs`.
- Authenticated exports: `/api/jobs/{runId}/export?format=csv|json|xlsx` or `/api/developer/v1/jobs/{runId}/export?format=csv|json|xlsx`.
- Proposed development pricing is $1 per 1,000 delivered rows (0.1 cents per row), subject to admin review. No production pricing or catalog entry has been changed.

`pulldata-backend/src/scripts/seed-google-jobs.ts` is a development-only, disabled-by-default catalog helper. The canonical marketplace pipeline should stage the final enabled development record after a passing run. The helper refuses a published slug and production execution.

## Worker contract

- `GET /health`: cheap liveness, no proxy, browser, DNS, Redis, or database access.
- `GET /ready`: validates proxy configuration and performs a bounded storage readiness check; never fetches Google.
- `POST /scrape-sync`: terminal `{ status, total, results }`.
- `POST /scrape`: prompt `{ job_id, status }`.
- `GET /job/{job_id}`: active/terminal state, total, safe error.
- `GET /job/{job_id}/results?offset=0&limit=25`: stable paginated rows, including persisted partial results.
- `POST /job/{job_id}/cancel` or `/stop`: idempotent cancellation preserving partial rows.

An optional worker bearer token protects readiness and work routes. If configured, a bearer token is required even if an internal secret is present. Otherwise, the platform internal secret protects work routes when supplied. Health remains unauthenticated for Railway.

## Runtime configuration

Declare names in `pulldata.env.schema.json`; supply actual scraper secrets only in the marketplace deployment request's `runtimeEnv`.

Use `GOOGLE_JOBS_PROXY_URL`, `WEBSHARE_RESIDENTIAL_PROXY_URL`, or the four `WEBSHARE_RESIDENTIAL_PROXY_*` gateway components. Proxying is required. Webshare rotating credentials receive a stable numeric session suffix per job so browser requests keep one exit IP. Existing sticky credentials and other gateway usernames are preserved. Proxy credentials are never inputs in API, playground, or no-code forms. The browser fetches only fixed public Google hosts and required Google static assets. External application sites, local/private user-supplied targets, media, and fonts are blocked.

The platform supplies a dedicated environment-specific Neon database and `SCRAPER_QUEUE_NAMESPACE`; infrastructure values must not be placed in scraper-owned `runtimeEnv`. PostgreSQL is preferred when `PULLDATA_RESULTS_DATABASE_URL` or `DATABASE_URL` is configured. Connections are opened for an operation, use a five-second transaction timeout, and close immediately. Atomic leases reject stale owners; completed and cancelled jobs cannot be overwritten. A configured PostgreSQL failure fails closed. Redis remains a fallback when no database URL is configured. There is no startup reconciliation, idle polling, cron, or heartbeat.

Per-job durable leases prevent two replicas from collecting the same run. Progress and IDs are persisted after every processed job. A restarted replica resumes on a real status/results request once the previous lease is available. Completed state is immutable and retained for 24 hours. Repeated platform `request_id`/`run_id` values reuse a job; conflicting input returns HTTP 409. Browser concurrency is two per process with at most eight pending jobs.

`GOOGLE_JOBS_ALLOW_MEMORY_STORE=true` is strictly for local tests and is rejected when `NODE_ENV=production`. For local configuration, use the non-secret `.env.example` as a list of required settings; the worker does not automatically load `.env` files.

## Local verification

```sh
python -m pip install -r requirements.txt
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
docker build -t pulldata-google-jobs:local .
docker run --rm pulldata-google-jobs:local python -m pytest -q
```

Tests mock unstable upstreams and also exercise extraction in real Chromium against an authored DOM fixture. Docker/Redis verification must additionally cover partial-result persistence through a process restart, result pagination, invalid bounds, idempotency, and cleanup. A genuine live run using `pulldata.input.json` remains mandatory before pushing or starting the deployment pipeline.

The worker is based on the live Pulldata Python marketplace starter and contract 1.0, read on 22 October 2026. There is no production publication authorization in this task.

## Research notes

- Google's [Indexing API](https://developers.google.com/search/apis/indexing-api/v3/quickstart) notifies Google of publishers' changed job URLs; it does not retrieve job-search listings.
- Google's [JobPosting documentation](https://developers.google.com/search/docs/appearance/structured-data/job-posting) describes source job fields and the Google Jobs search experience.
- Webshare documents [rotating and sticky session suffixes](https://www.webshare.io/blog/how-to-use-webshare-proxies-with-clash); browser jobs use a stable sticky suffix when the configured Webshare username ends in `-rotate`.
- [Playwright's network documentation](https://playwright.dev/python/docs/network#http-proxy) documents authenticated browser proxies and network interception.
- Provider documentation was researched to compare options, then excluded from the implementation after the owner selected self-hosted browser collection.

Before release, obtain a successful small real scrape, verify current live selectors/filter controls/scrolling, run a full immutable-source marketplace development deployment, verify both sync and async worker paths, then complete the separate admin review and production gates in the workspace AGENTS.md.


## Verification recorded on 22 October 2026

- Worker suite: 17 passing tests on the host and in the Docker image, with no skipped tests.
- Ruff lint and formatting checks: passed.
- Backend suite: all 272 tests passed, including the new Excel export and API contract tests.
- Shared frontend suite: 12 tests passed, including schema/docs/playground parity.
- Dashboard and website type checks and production builds: passed.
- Marketplace structural contract validation and the platform hard-coded-secret scan: passed locally. This does not establish repository availability or constitute a remote marketplace run.
- Container HTTP checks: synchronous and asynchronous two-row fixture runs, pagination, invalid input, and idempotency passed. One partial row survived a worker process restart in real Redis; the resumed job completed with two unique rows.
- Genuine Linux browser/proxy smoke: blocked by Google's traffic challenge or the bounded timeout, including a documented Webshare sticky-session attempt; zero returned rows. No Pulldata public API/customer-wallet run was started, so there is no Pulldata wallet charge or production run ID.
- Backend type check remains blocked by a pre-existing type error in `src/catalog/facebook-tools.test.ts:33`, outside the Google Jobs changes.

No catalog record has been enabled and no development or production deployment has been claimed as passed.
