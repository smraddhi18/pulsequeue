# PulseQueue Roadmap

PulseQueue is a distributed background job queue for Node.js services: producers enqueue jobs over an HTTP API, a pool of workers leases and runs them with retries and exponential backoff, failed jobs land in a dead-letter queue, and an operator dashboard API exposes queue health. Built in TypeScript with Fastify, PostgreSQL (source of truth) and Redis (fast lease/notify path).

Status legend: [ ] planned, [~] in progress, [x] done

## Milestone 0: Project foundation

- [x] Repo created, roadmap written (2026-10-17)
- [x] TypeScript + Fastify skeleton with layered structure (routes, services, repositories, domain)
- [x] Config from env vars validated with zod, fail fast on bad config
- [x] Structured JSON logging with pino and request IDs
- [x] Central error handling with typed domain errors and consistent error responses
- [x] ESLint + Prettier, Vitest test setup, health and readiness endpoints
- [x] Dockerfile (multi-stage), docker-compose with Postgres and Redis
- [x] GitHub Actions CI: lint, typecheck, test

## Milestone 1: Job model and enqueue API

- [x] Postgres schema and migrations for queues, jobs and job attempts
- [x] POST /queues/:name/jobs with payload validation, priority, delay and max attempts
- [x] Idempotency keys so producers can safely retry enqueue
- [x] GET job status, list jobs with cursor pagination
- [x] Postgres pool from env config, readiness check, graceful pool shutdown
- [x] Integration tests against real Postgres (CI postgres:16 service container)

## Milestone 2: Workers, leasing and retries

- [x] Worker process that leases jobs with SELECT ... FOR UPDATE SKIP LOCKED
- [x] Visibility timeout and heartbeats so crashed workers release jobs
- [x] Retries with exponential backoff and jitter
- [x] Graceful shutdown that drains in-flight jobs
- [x] Concurrency tests for leasing against real Postgres (many workers, no double-run)

## Milestone 3: Dead-letter queue and replay

- [x] Move jobs to DLQ after max attempts, keep full attempt history
- [x] DLQ inspect, replay and purge endpoints
- [x] Poison-message detection

## Milestone 4: Scheduling

- [x] Delayed jobs and cron-style recurring jobs
- [x] Leader election so only one scheduler fires each tick

## Milestone 5: Redis fast path and caching

- [x] Redis pub/sub wake-ups so idle workers stop polling
- [x] Cached queue stats with explicit invalidation

## Milestone 6: Auth, multi-tenancy and rate limiting

- [x] API keys per tenant, hashed at rest
- [x] Per-tenant and per-queue rate limits (token bucket in Redis)
- [x] Queue-level concurrency limits

## Milestone 7: Dashboard API and observability

- [x] Queue depth, throughput, latency and failure-rate endpoints
- [x] Prometheus metrics, OpenTelemetry tracing across enqueue and execution
- [x] Grafana dashboard in docker-compose

## Milestone 8: Load testing and hardening

- [ ] k6 load tests with documented throughput and latency numbers
- [ ] Chaos tests: kill workers mid-job, restart Postgres
- [ ] Scaling notes and capacity plan in README

## Progress log

- 2026-10-17: Project chosen, repo created, roadmap written.
- 2026-10-18: No commit — box infra stuck starting up; daily routine blocked.
- 2026-10-19: Milestone 0 foundation landed locally — TypeScript/Fastify skeleton, zod config, pino + request IDs, typed errors, /health + /ready, Vitest, ESLint/Prettier, multi-stage Dockerfile, docker-compose (Postgres 16 + Redis 7), GitHub Actions CI.
- 2026-10-20: Milestone 1 done — SQL migrations (queues, jobs, job_attempts) with a checksummed, advisory-locked runner; POST /queues/:name/jobs with zod validation and race-safe idempotency keys (unique index + ON CONFLICT, request fingerprint, 422 on reuse); GET /jobs/:id and keyset-paginated GET /queues/:name/jobs; pg pool wired into /ready and shutdown; unit + real-Postgres integration tests, CI Postgres service.
- 2026-10-21: Milestone 2 done — migration 0002 (lease columns, lease_token fencing, running-iff-leased CHECK, one-running-attempt unique index, partial lease-expiry index); worker process (`npm run worker`) with bounded concurrency leasing via a single `FOR UPDATE SKIP LOCKED` statement in priority/run_at order; queue-keyed handler registry with AbortSignal; heartbeats + per-worker reaper for crashed workers; per-attempt timeout; capped exponential backoff with full jitter; every attempt in job_attempts; exhausted jobs -> failed + onJobFailed hook for the M3 DLQ; SIGTERM drain with lease release (refunded attempt); zod worker config; compose worker service + CI SIGTERM smoke test; unit + real-Postgres tests incl. 8 workers x 400 jobs exactly-once.
- 2026-10-22: Milestone 3 done — migration 0003 (DLQ as `status = dead` + `dead_reason` / `dead_lettered_at` / `replay_count` / `lease_expiries`, `jobs_dead_has_metadata` invariant, legacy `failed` backfilled into the DLQ and forbidden, partial `jobs_dlq_idx`); dead-lettering inside the lease_token-fenced fail/reap statement so a stale worker cannot dead-letter; poison detection (`NonRetryableError` / `retryable: false`, `InvalidPayloadError`, N lease expiries via `WORKER_POISON_LEASE_EXPIRIES`); `GET /queues/:name/dlq` (keyset, reason filter), `GET /dlq/:jobId` (attempt history), single + bulk replay (guarded UPDATE, exactly-one-wins, `SKIP LOCKED` batches, 409 `JOB_NOT_DEAD_LETTERED`), single + `olderThan` bulk purge; `dlq.*` structured log events; unit + real-Postgres tests incl. 10 concurrent replays, replay-vs-purge races and an end-to-end fail -> DLQ -> replay -> succeed run.
- 2026-10-23: Milestone 4 done — delayed jobs via `delayMs` or absolute `runAt` (mutually exclusive, 30-day horizon, DB clock), due `scheduled` jobs promoted to `queued` in bounded `SKIP LOCKED` batches and leased from the new queued-only `jobs_ready_idx` (never early, never walking future jobs); migration 0004 (`schedules` table, `schedule_misfire_policy` enum, enabled-iff-next_run_at invariant, partial due index); schedules API (create, keyset list with `enabled` filter, get, idempotent pause/resume, delete, 409 on duplicate names) with cron-parser v5 + IANA timezones and field-level 400s; scheduler process (`npm run scheduler`) with advisory-lock leader election where every tick runs on the lock session (crash = lock and open tick released together), ticks that claim due schedules with `SKIP LOCKED`, enqueue with the key `schedule:<id>:<occurrence>` and advance `next_run_at` in one transaction, per-schedule savepoints, misfire policies `skip` / `fire_once` / `catch_up` with grace window and catch-up limit, step-down after repeated failures, SIGTERM release; `scheduler.*` / `schedule.*` log events; zod scheduler config; compose `scheduler` service (2 replicas) and a CI scheduler smoke test; unit + real-Postgres tests incl. two schedulers (one leader, failover on stop and on a killed session), concurrent ticks without duplicates, misfire policies after a 5 h outage and two real scheduler processes handing over on SIGTERM.
- 2026-10-24: Milestone 5 done — Redis as an optional, disposable accelerator (Postgres stays the source of truth; `REDIS_URL` unset = M4 behaviour) via ioredis with fail-fast commands (no offline queue, 500 ms command timeout, no per-request retries) and equal-jitter reconnect backoff; wake-ups published only after COMMIT on per-queue channels `<prefix>:wakeup:<queue>` with a leading + trailing-edge per-queue coalescer, from enqueue, replay, retries, releases, reaper requeues and scheduler fires; a dedicated subscriber connection per worker that resubscribes on every reconnect and triggers a catch-up poll, with workers sleeping up to `WORKER_WAKEUP_FALLBACK_POLL_MS` (30 s) while subscribed but never past the next due delayed job or retry, and falling back to normal polling while unsubscribed; `GET /queues/:name/stats` (counts per status, backlog ages, next scheduled, one aggregate statement) cached in Redis with a TTL, explicit coalesced dirty-flag invalidation from every state change, `HIT` / `STALE` / `MISS` / `BYPASS` via `x-cache`, two-level stampede protection (in-process single-flight + cluster-wide refresh claim) and a Lua compare-and-set so an older snapshot never overwrites a newer one; `/ready` reports `degraded` (200) when only Redis is down and `not_ready` (503) only for Postgres; `redis.*` / `wakeup.*` / `stats.cache.*` log events; zod Redis config; compose Redis as a soft dependency without persistence, CI `redis:7-alpine` service and a worker smoke test that must subscribe; unit + real-Redis tests incl. subscriber recovery through a connection-cutting TCP proxy, stampede protection across two API instances, `/ready` during an outage, pickup latency with a 20 s fallback poll, push vs poll query counts and the real worker process picking up a job via a wake-up.
- 2026-10-26: Milestone 6 run failed before upload — the auth, tenancy, rate-limit and concurrency-cap work was built locally but nothing was pushed; it was finished, re-verified and landed on 2026-10-27.
- 2026-10-27: Milestone 6 done — migrations 0005 (`tenants` with optional rate-limit override, `default` tenant seeded; `api_keys` with `api_key_role` admin / producer / reader, unique 12-char prefix, 32-byte SHA-256 `secret_hash`, expiry, revocation, `rotated_from`), 0006 (queues owned by tenants, catalog-only backfill to `default`, names unique per `(tenant_id, name)`) and 0007 (`max_concurrency` and queue rate-limit columns, partial `jobs_running_queue_idx`); API keys `pq_<prefix>_<secret>` (256-bit secret, only the digest stored, prefix lookup + `timingSafeEqual`, 5 s per-instance LRU cache, throttled `last_used_at`); `onRequest` auth hook with deny-by-default `config.access` on every route (`/health` and `/ready` public), typed `401` (+ `WWW-Authenticate`) / `403`; tenant scoping in every service and query with cross-tenant `404`s; `POST/GET/DELETE /api-keys`, `POST /api-keys/:id/rotate` (grace period), `GET /auth/whoami`, `GET/PUT /queues/:name/settings`; operator CLI `npm run api-key` (create, list, revoke, tenant-create, tenant-rate-limit); enqueue rate limiting with per-tenant and per-queue token buckets in one atomic Lua script on the Redis clock (all-or-nothing, hash-tagged keys that expire when full), `RateLimit-*` headers, `429 RATE_LIMITED` + `Retry-After`, fail-open default and fail-closed `503`; fleet-wide queue concurrency caps enforced in the lease transaction (capped queue rows locked `FOR NO KEY UPDATE` in id order, running count, `LATERAL ... LIMIT` budget with `SKIP LOCKED`; uncapped queues keep the single-statement lease); `auth.*` / `apikey.*` / `ratelimit.*` / `queue.settings_updated` log events; zod auth / rate-limit config; compose and CI API smoke test (CLI bootstrap, `401`, `201, 201, 429`); unit + real-Postgres + real-Redis tests incl. 200 concurrent bucket takes over 4 connections admitting exactly the burst, two API instances sharing buckets, fail-open / fail-closed outages, tenant isolation of jobs / DLQ / schedules, parallel leasing never exceeding a cap, no deadlocks across overlapping capped queues and three real workers respecting a cap. 730 tests (232 integration), all green.
- 2026-10-28: Milestone 7 done — migration 0008 (`jobs.traceparent` with a W3C format `CHECK`; `job_attempts.queue_id` backfilled and `ready_at` written at lease time so queue wait is measured on the DB clock; covering `job_attempts_queue_finished_idx (queue_id, finished_at) INCLUDE (status, started_at, ready_at)` for index-only dashboard scans); tenant-scoped dashboard API for any role: `GET /dashboard/queues` (depth per status, oldest queued age, concurrency cap, keyset by name), `/dashboard/queues/:name/throughput` (epoch-aligned, zero-filled `date_bin` buckets of enqueued / succeeded / failed / timed-out / dead-lettered), `/latency` (exact `percentile_cont` p50 / p95 / p99 / max of execution and queue wait, bounded by `DASHBOARD_LATENCY_MAX_SAMPLES` with a `truncated` flag) and `/failure-rate` (attempt vs terminal job failure rate), with zod `<int><s|m|h|d>` windows and buckets (7d / 1d latency caps, whole-bucket windows, at most 1440 buckets); prom-client metrics per process with a `role` label — HTTP histogram by route template (`__unmatched__` for 404s), enqueue and rate-limit counters, worker leased / completed / failed (outcome x disposition) / dead-lettered (reason x source) counters, execution and queue-wait histograms, lost-lease, release, lease-error, reaper and in-flight series, scheduler leadership / tick / occurrence series — counted only after the fenced write commits; a lazy `queue_depth` gauge read from Postgres at scrape time in the API only (cached, single-flight, READ ONLY with `statement_timeout`, limited to N queues, stale snapshot + error counter on failure) and a per-process tenant/queue series cap (`__other__`); a separate `/metrics` listener per process (`METRICS_PORT`, loopback by default, constant-time bearer token required in production on a non-loopback host, bind failure logged not fatal) so the deny-by-default API port is untouched (`/metrics` there is 404); OpenTelemetry tracing off by default (`TRACING_ENABLED`, OTLP/HTTP batch export, parent-based ratio sampler, explicit context, no global SDK): SERVER span per request continuing an inbound `traceparent`, PRODUCER `pulsequeue.enqueue` span stored in `jobs.traceparent`, CONSUMER `job.execute` span per attempt as its child with outcome / disposition / exception, `trace_id` / `span_id` in request and job logs; `metrics.*` / `tracing.*` log events; zod metrics / dashboard / tracing config; compose Prometheus (Docker DNS discovery of every replica, bearer auth), Grafana (provisioned Prometheus + Jaeger datasources and a PulseQueue overview dashboard) and Jaeger all-in-one; CI metrics smoke test (API and worker listeners: 401 without / with a wrong token, expected series, 404 on the API port); unit + real-Postgres + real-Redis tests incl. exact percentiles over seeded attempts, aligned zero-filled buckets, dashboard tenant isolation, the 0008 upgrade path and index-only plan, the depth collector under a table lock and with the database down, the full enqueue -> retry -> dead-letter metrics lifecycle through the real API and worker runtime, scheduler leadership metrics on two replicas and one trace from an HTTP caller through `jobs.traceparent` into the worker span with matching `trace_id` in logs. 809 tests (266 integration), all green.
