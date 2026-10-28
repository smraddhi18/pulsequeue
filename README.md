# PulseQueue

A production-style distributed job queue for Node.js services, built in TypeScript with Fastify, PostgreSQL and Redis. Producers enqueue jobs over an HTTP API, workers lease and execute them with retries and exponential backoff, failed jobs go to a dead-letter queue, and a dashboard API reports queue health.

This project is being built milestone by milestone. See [ROADMAP.md](ROADMAP.md) for the plan and progress. **Current state: Milestone 7 (tenant-scoped dashboard API for depth, throughput, latency percentiles and failure rate; Prometheus metrics from the API, workers and scheduler on a separate token-protected listener; OpenTelemetry traces from the HTTP request through `jobs.traceparent` into every execution attempt; Prometheus + Grafana + Jaeger in docker-compose) is done.**

## Architecture

```mermaid
flowchart LR
    P[Producer services] -->|"POST /queues/:name/jobs<br/>Authorization: Bearer pq_...<br/>Idempotency-Key"| API
    O[Operators / dashboards] -->|"GET /jobs/:id, /queues/:name/jobs<br/>GET /queues/:name/stats<br/>GET /dashboard/queues/... (M7)"| API
    OP[Operators / runbooks] -->|"DLQ inspect / replay / purge<br/>schedules CRUD, pause, resume"| API
    AD[Tenant admins] -->|"POST/GET/DELETE /api-keys, POST /api-keys/:id/rotate<br/>GET/PUT /queues/:name/settings, GET /auth/whoami"| API
    LB[Load balancer / k8s probes] -->|"/health, /ready (public)<br/>(redis down = degraded, still 200)"| API
    CLI[api-key CLI<br/>bootstrap tenants + first admin key] -->|"SQL via repositories"| PG

    subgraph API[Fastify API - stateless, N replicas]
        direction TB
        OBP[Observability hooks onRequest / onResponse<br/>SERVER span from inbound traceparent, trace_id in logs,<br/>HTTP histogram by route template] --> AU
        AU[Auth hook onRequest<br/>every route declares access: public / read / enqueue / manage<br/>Bearer pq_prefix_secret, lookup by prefix,<br/>SHA-256 + timingSafeEqual, LRU cache AUTH_CACHE_TTL_MS<br/>401 / 403] --> RT
        RT[Routes<br/>zod validation, thin<br/>jobs, dlq, schedules, stats, dashboard, api-keys, queue settings] --> RL
        RL[RateLimiter enqueue only<br/>tenant bucket + optional queue bucket<br/>429 + Retry-After + RateLimit-*<br/>fail open / closed] --> SV
        RT --> SV[Services<br/>JobService, DlqService, ScheduleService, StatsService,<br/>ApiKeyService, QueueService, DashboardService, HealthService<br/>every query scoped by principal.tenantId]
        SV --> RP[Repositories<br/>Tenant, ApiKey, Queue, Job, DeadLetter, Schedule, QueueStats, Dashboard<br/>all SQL lives here]
        SV --> SIG[QueueSignals<br/>after COMMIT: jobsRunnable / jobsChanged<br/>coalesced per queue]
    end

    subgraph PG["PostgreSQL 16 - source of truth"]
        direction TB
        TN[tenants<br/>slug id, optional rate limit override]
        AK[api_keys<br/>prefix UNIQUE, secret_hash = sha256 bytea,<br/>role admin / producer / reader,<br/>expires_at, revoked_at, rotated_from]
        QU[queues<br/>UNIQUE tenant_id, name<br/>max_concurrency, rate_limit_per_sec / burst]
        JT[jobs<br/>queued / scheduled / running / succeeded / dead<br/>jobs_ready_idx, jobs_running_queue_idx<br/>traceparent of the enqueue span]
        ST[schedules]
        AT[job_attempts<br/>queue_id, ready_at for queue wait<br/>job_attempts_queue_finished_idx]
        TN -.- AK
        TN -.- QU
        QU -.- JT
        QU -.- ST
        JT -.- AT
    end

    subgraph RD["Redis 7 - optional, disposable state"]
        direction TB
        CH["pub/sub channels<br/>pq:wakeup:NAME (by name: shared worker fleet)"]
        SC["stats cache + dirty flags<br/>pq:stats:v1:{TENANT:NAME}, pq:stats:dirty:{TENANT:NAME}"]
        TB["token buckets (hash tokens, ts)<br/>pq:rl:{t:TENANT}<br/>pq:rl:{t:TENANT}:q:NAME<br/>one Lua script, Redis TIME, all-or-nothing"]
    end

    RP -->|"pg Pool"| PG
    MG[migrate job<br/>0001-0008, advisory lock] -->|DDL| PG
    RL -->|"EVALSHA token bucket<br/>(one round trip per enqueue)"| TB
    SV -->|"stats read-through, Lua CAS"| SC
    SIG -.->|"PUBLISH wake-up"| CH
    SIG -.->|"SET dirty"| SC

    subgraph SP[Scheduler - 2 replicas, one leader]
        SL[Leader: advisory lock, ticks,<br/>misfire planner]
    end
    SL -->|"claim due schedules SKIP LOCKED,<br/>enqueue with schedule:id:time key"| PG
    SL -.->|"wake-ups"| CH

    subgraph WP[Worker pool - N processes x WORKER_CONCURRENCY]
        direction TB
        WS[Wake-up subscriber] -->|"wake()"| WL
        WL[Worker loop<br/>push / poll, promote due jobs,<br/>lease free slots] --> HR[Handler registry]
        WL --> HB[Heartbeats]
        RE[Reaper + poison detection]
        WL --> LR[LeaseRepository<br/>writes fenced on lease_token]
        HB --> LR
        RE --> LR
    end

    LR -->|"uncapped queues: one SKIP LOCKED statement<br/>capped queues: lock queue rows FOR NO KEY UPDATE (id order),<br/>count running, lease at most max_concurrency - running"| PG
    CH -.->|"hint only"| WS
    WP -.->|"wake-ups + dirty flags after COMMIT"| RD
```

Solid lines are the durable path through Postgres. Dotted lines from and to Redis are best-effort hints (wake-ups, dirty flags): losing any of them costs latency or one extra query, never a job. The token buckets are the one piece of Redis state that enforces something; it is still disposable (a lost bucket refills, i.e. a burst is allowed once) and with Redis down the limiter fails open by default (`RATE_LIMIT_FAIL_MODE=closed` refuses enqueues instead). Redis is optional; without `REDIS_URL` the dotted lines and rate limiting simply do not exist. Authentication and the queue concurrency limit live entirely in Postgres and work without Redis.

### Observability architecture (Milestone 7)

```mermaid
flowchart LR
    C[Caller<br/>traceparent header optional] -->|"POST /queues/:name/jobs"| API
    D[Dashboards / UI<br/>reader key] -->|"GET /dashboard/queues<br/>/dashboard/queues/:name/throughput, latency, failure-rate"| API

    subgraph API[API process]
        direction TB
        H[onRequest: SERVER span + trace_id in request log<br/>onResponse: http_request_duration_seconds by route template]
        E[enqueue route: PRODUCER span pulsequeue.enqueue<br/>jobs_enqueued_total created / duplicate]
        DS[DashboardService<br/>zod window / bucket, tenant-scoped SQL]
        MA["/metrics listener :9464, bearer token<br/>queue_depth gauge read lazily, cached, timed out"]
        H --> E
        H --> DS
    end

    subgraph PG[PostgreSQL]
        J[jobs.traceparent<br/>W3C context of the enqueue span]
        A[job_attempts.queue_id, ready_at<br/>job_attempts_queue_finished_idx<br/>INCLUDE status, started_at, ready_at]
    end

    subgraph W[Worker process]
        direction TB
        X[job.execute CONSUMER span<br/>parent = jobs.traceparent<br/>trace_id / span_id in job logs]
        MW["/metrics listener :9464<br/>leased, completed, failed, dead-lettered,<br/>execution + queue-wait histograms,<br/>lease lost, reaper, in-flight"]
    end

    subgraph S[Scheduler process]
        MS["/metrics listener :9464<br/>is_leader, ticks, occurrences"]
    end

    E -->|"INSERT ... traceparent"| J
    DS -->|"date_bin buckets, percentile_cont,<br/>index-only range scans"| A
    MA -->|"count per queue x status<br/>READ ONLY, statement_timeout"| PG
    J -->|"lease returns traceparent"| X
    X -->|"attempt row with queue_id, ready_at"| A

    subgraph OBS[Observability stack - docker-compose]
        PR[(Prometheus<br/>dns_sd per service, Bearer)]
        GF[Grafana<br/>provisioned datasources + PulseQueue dashboard]
        JG[(Jaeger all-in-one<br/>OTLP/HTTP :4318, UI :16686)]
    end

    PR -->|"scrape 15s"| MA
    PR -->|"scrape 15s"| MW
    PR -->|"scrape 15s"| MS
    GF -->|PromQL| PR
    GF -->|trace search| JG
    H -.->|"OTLP/HTTP batch export<br/>(TRACING_ENABLED, off by default)"| JG
    X -.->|"OTLP/HTTP batch export"| JG
```

Metrics and traces are best-effort side channels: a failed scrape, an unreachable collector or a metrics port that cannot bind is logged and never blocks an enqueue or a job. `/metrics` is deliberately **not** on the public API port (it is a `404` there), so the deny-by-default auth hook is untouched and no tenant key can read another tenant's series.

## Features

### Done

**Milestone 0: foundation**

- TypeScript + Fastify HTTP skeleton with a layered architecture
- Zod-validated configuration (fail-fast on bad env)
- Structured JSON logging with pino and per-request IDs
- Typed domain errors and consistent JSON error responses
- `GET /health` (liveness) and `GET /ready` (readiness)
- ESLint, Prettier, Vitest, multi-stage Dockerfile, docker-compose, GitHub Actions CI

**Milestone 1: job model and enqueue API**

- PostgreSQL schema for `queues`, `jobs` and `job_attempts`, applied by a small forward-only SQL migration runner (`schema_migrations`, checksums, advisory lock)
- `POST /queues/:name/jobs`: strict zod validation, payload byte budget, priority, delay, max attempts; queues are auto-created race-safely
- Idempotency keys (`Idempotency-Key` header): safe producer retries, replay detection, `422` on key reuse with a different body; race-safe through a unique index + `INSERT ... ON CONFLICT`
- `GET /jobs/:id` and `GET /queues/:name/jobs` with opaque keyset cursors over `(created_at, id)`, status filter and bounded `limit`
- Indexes designed for the Milestone 2 `FOR UPDATE SKIP LOCKED` lease query and for pagination, verified with `EXPLAIN` in tests
- `pg` connection pool configured from env with explicit timeouts; `/ready` probes Postgres; the pool drains on graceful shutdown
- Postgres errors mapped to typed domain errors (e.g. invalid data -> 400, outage or timeout -> 503)
- Unit tests + integration tests against a real Postgres (CI runs a `postgres:16` service container)

**Milestone 2: workers, leasing and retries**

- Worker process (`npm run worker`, `src/worker.ts`) with bounded concurrency that leases jobs in one short statement with `SELECT ... FOR UPDATE SKIP LOCKED`, in priority, then `run_at` order, honouring delays and the queues it has handlers for
- Pluggable handler registry keyed by queue name; handlers get the payload, attempt info, a bound child logger and an `AbortSignal`
- Visibility timeout + heartbeats: a running job's lease is extended every `WORKER_HEARTBEAT_INTERVAL_MS`; a reaper (in every worker) reclaims expired leases so a crashed worker's jobs run again
- Fencing tokens: every write after the lease is conditional on a per-lease `lease_token`, so a worker that lost its lease can never overwrite the new owner's outcome
- Retries with capped exponential backoff and full jitter (`RETRY_BACKOFF_BASE_MS`, `RETRY_BACKOFF_MAX_MS`); `NonRetryableError` skips retries; per-attempt timeout (`WORKER_JOB_TIMEOUT_MS`)
- Every attempt recorded in `job_attempts` (worker, start, finish, outcome `succeeded` / `failed` / `timed_out` / `released`, error)
- Exhausted or non-retryable jobs fire an `onJobFailed` hook (in M2 they ended `failed`; since Milestone 3 they are dead-lettered in the same fenced statement)
- Graceful shutdown on SIGTERM/SIGINT: stop leasing, drain in-flight jobs up to `WORKER_SHUTDOWN_TIMEOUT_MS`, release the leases of anything unfinished (without burning an attempt), close the pool
- All tuning via zod-validated env (e.g. heartbeat must fit twice into the lease); structured logs carry `workerId`, `jobId`, `attempt` and `attemptId`
- docker-compose `worker` service (same image, liveness-file healthcheck, `stop_grace_period` above the drain budget); CI boots the compiled worker and checks it exits 0 on SIGTERM
- Tests: backoff/jitter, retry policy and the worker loop against an in-memory store; real-Postgres tests for leasing order, fencing, reaping, 8 workers x 400 jobs (each runs exactly once, never concurrently), retry-then-succeed, exhaust-to-DLQ, timeouts, heartbeats outliving the lease, drain and drain-timeout release, and the real worker process handling SIGTERM

**Milestone 3: dead-letter queue and replay**

- Terminal failures move to the dead-letter queue (`status = dead` + `dead_reason`, `dead_lettered_at`) **inside the same fenced statement** that records the failed attempt, so the move is atomic and a worker that lost its lease cannot dead-letter (or touch) the job; the full `job_attempts` history is kept
- Dead-letter reasons: `attempts_exhausted`, `non_retryable` (`NonRetryableError` or any error with `retryable: false`), `invalid_payload` (`InvalidPayloadError`, e.g. failed payload schema), `poison_lease_expired`
- Poison-message detection: a job whose lease expires `WORKER_POISON_LEASE_EXPIRIES` times (default 3; it keeps crashing or hanging its worker) is dead-lettered by the reaper even with attempts left; non-retryable and invalid-payload errors are dead-lettered on the first attempt
- Migration `0003`: DLQ columns, `dead_letter_reason` enum, `jobs_dead_has_metadata` invariant, legacy `failed` forbidden (existing `failed` rows backfilled into the DLQ), partial `jobs_dlq_idx`
- `GET /queues/:name/dlq` (keyset-paginated, newest dead-lettered first, `reason` filter) and `GET /dlq/:jobId` (DLQ metadata + attempt history)
- `POST /queues/:name/dlq/:jobId/replay` (fresh retry budget, optional `delayMs`, same job id and idempotency key; concurrent replays: exactly one `200`, the rest `409 JOB_NOT_DEAD_LETTERED`) and `POST /queues/:name/dlq/replay` (bulk, bounded `limit`, `reason` filter, `SKIP LOCKED` so concurrent bulk replays never overlap)
- `DELETE /queues/:name/dlq/:jobId` and `DELETE /queues/:name/dlq?olderThan=...` (bulk purge needs an explicit cutoff, bounded batches; attempts cascade)
- Structured log events with a stable `event` field: `dlq.dead_lettered` (worker / reaper), `dlq.replayed`, `dlq.bulk_replayed`, `dlq.purged`, `dlq.bulk_purged` (API, with request id)
- Tests: DLQ service rules and request schemas against in-memory fakes; real-Postgres tests for the schema invariants, the `0003` backfill, stale-lease fencing, poison detection, 10 concurrent replays of one job (exactly one wins), replay vs purge races, concurrent bulk replays, keyset listing, purge cutoffs, and the HTTP API end to end with a real worker (fail -> DLQ -> inspect -> replay -> succeed, attempt numbering continues)

**Milestone 4: scheduling**

- Delayed jobs: `POST /queues/:name/jobs` takes either `delayMs` or an absolute `runAt` (ISO-8601 with an offset), mutually exclusive, at most 30 days ahead; a `runAt` in the past runs now. `run_at` is clamped against the database clock
- Workers never lease a job early: due `scheduled` jobs are first **promoted** to `queued` in a bounded `SKIP LOCKED` batch (via the new `jobs_scheduled_due_idx` range scan on `run_at`), then leased from `jobs_ready_idx`, which contains only `queued` rows, so the lease scan no longer walks future jobs that outrank due ones (migration `0004`)
- Recurring schedules (migration `0004`, `schedules` table): `POST /queues/:name/schedules`, `GET /queues/:name/schedules` (keyset-paginated, `enabled` filter), `GET /schedules/:id`, `POST /schedules/:id/pause`, `POST /schedules/:id/resume`, `DELETE /schedules/:id`; unique name per queue (`409`)
- Cron via the maintained [`cron-parser`](https://www.npmjs.com/package/cron-parser) v5 (Luxon): 5-field expressions (or 6 with seconds = 0) and `@daily`-style macros, any IANA timezone with correct DST handling, minute resolution; invalid expressions, zones or never-matching expressions are `400 VALIDATION_ERROR` with the same `details.fieldErrors` shape as every other validation error
- Scheduler process (`npm run scheduler`, `src/scheduler.ts`) with **leader election through a Postgres advisory lock** held on a dedicated session; only the leader ticks, and each tick runs on the lock-holding session so a dead leader's transaction can never commit
- Tick = one transaction: claim due schedules (`FOR UPDATE SKIP LOCKED`, batch), enqueue the job(s) with the deterministic idempotency key `schedule:<id>:<fire time>`, advance `next_run_at`; a full batch re-ticks immediately
- Misfire policies per schedule (`skip`, `fire_once` (default), `catch_up`) with a grace window (`SCHEDULER_MISFIRE_GRACE_MS`) and a catch-up limit (`SCHEDULER_MAX_CATCH_UP`)
- Fault isolation: a schedule that can no longer be evaluated (e.g. its queue's payload limit shrank) records `lastError` and backs off via a savepoint without blocking the rest of the batch; repeated tick failures make the leader step down so a healthy replica can take over
- Graceful shutdown on SIGTERM/SIGINT: finish the in-flight tick (bounded by `SCHEDULER_SHUTDOWN_TIMEOUT_MS`), unlock, close the session, exit 0; the standby takes over within one retry interval
- Structured log events: `scheduler.started`, `scheduler.leader_acquired`, `scheduler.leader_lost`, `scheduler.leader_released`, `scheduler.fired`, `scheduler.misfire_skipped`, `scheduler.duplicate_suppressed`, `scheduler.schedule_error`, `scheduler.tick_failed`, `scheduler.stopped`; API events `schedule.created`, `schedule.paused`, `schedule.resumed`, `schedule.deleted`
- Zod-validated scheduler config (e.g. the grace window must cover at least two ticks); docker-compose `scheduler` service with 2 replicas, a liveness-file healthcheck and `stop_grace_period`; CI boots the compiled scheduler and requires it to take and release leadership and exit 0
- Tests: cron parsing / next-run / DST walks, misfire planning, request schemas, config and the scheduler loop (fake lock) as unit tests; real-Postgres tests for promotion and "never leased early", the schedules API, two schedulers (only one leads, failover on stop and on a killed session, concurrent ticks never duplicate), misfire policies after an outage, and two real `src/scheduler.ts` processes handing over leadership on SIGTERM

**Milestone 5: Redis wake-ups and cached stats**

- Redis is optional and never the source of truth: with `REDIS_URL` unset every Redis feature is off and the system behaves exactly as in M4; with Redis down at runtime the API, workers and scheduler keep working (fallback poll, uncached stats), and nothing is lost
- Pub/sub **wake-ups**: after an enqueue, replay, bulk replay, retry, lease release, reaper reclaim or scheduler fire **commits**, the process publishes on the per-queue channel `<prefix>:wakeup:<queue>`; publishes are fire-and-forget and coalesced per queue (leading + trailing edge, `WAKEUP_COALESCE_MS`), so a burst of 10k enqueues costs about 2 publishes per window
- Workers subscribe on a **dedicated connection** to their own queues only; reconnects use jittered exponential backoff, the subscriber re-issues `SUBSCRIBE` itself on every reconnect and the worker polls once right after (pub/sub has no replay)
- **Push mode**: while subscribed an idle worker sleeps `WORKER_WAKEUP_FALLBACK_POLL_MS` (default 30s, jittered) instead of `WORKER_POLL_INTERVAL_MS`, cut short by a wake-up and capped by the next scheduled `run_at` (new `LeaseRepository.nextDueInMs`), so delayed jobs and retries still run on time; a wake during an in-flight lease query triggers an immediate re-poll. Poll mode returns automatically while Redis is unreachable (`wakeup.mode_changed`)
- `GET /queues/:name/stats`: counts by status, total, oldest queued job (`oldestQueuedAt`, `oldestQueuedAgeMs`), next scheduled run and longest-running job, from **one** aggregate statement (one MVCC snapshot, database clock), `404` for unknown queues
- **Stats cache** in Redis with explicit invalidation: every committed change marks the queue dirty (coalesced, `STATS_INVALIDATION_COALESCE_MS`); readers get `HIT`, `STALE` (dirty but younger than `STATS_CACHE_MIN_REFRESH_MS`, or someone else is refreshing), `MISS` (recomputed) or `BYPASS` (Redis off / down) in the `x-cache` header
- **Stampede protection** at two levels: a cluster-wide refresh claim (atomic `DEL` of the dirty flag, exactly one winner across all API instances) and an in-process single-flight, so N concurrent cold reads on one instance cost one query; writes are a Lua compare-and-set on the snapshot time so a slow reader can never roll the cache back; entries and dirty flags expire after `STATS_CACHE_TTL_MS`
- `/ready` includes a Redis check: Postgres failing = `not_ready` (503), only Redis failing = **`degraded` (200)**; without `REDIS_URL` there is no Redis check
- ioredis client tuned to fail fast (`enableOfflineQueue: false`, `maxRetriesPerRequest: 0`, `REDIS_COMMAND_TIMEOUT_MS` = 500ms), so a sick Redis costs at most one short timeout per request, never a hung request
- Structured log events: `redis.connected`, `redis.disconnected`, `redis.reconnecting`, `redis.error`, `redis.closed`, `redis.disabled`, `wakeup.published`, `wakeup.publish_failed`, `wakeup.subscribed`, `wakeup.unsubscribed`, `wakeup.subscribe_failed`, `wakeup.received`, `wakeup.mode_changed`, `stats.cache.hit`, `stats.cache.stale`, `stats.cache.miss`, `stats.cache.bypass`, `stats.cache.error`, `stats.cache.invalidated`, `stats.cache.invalidate_failed` (errors throttled to one line per 10s with a `suppressed` count)
- Graceful shutdown flushes pending coalesced signals before closing Redis (API, worker and scheduler)
- Redis config validated by zod (e.g. the fallback poll must be at least the normal poll interval, the min refresh below the TTL); CI runs a `redis:7-alpine` service and the worker smoke test requires `wakeup.subscribed`; compose runs Redis without persistence (it only holds derived state)
- Tests: coalescer, single-flight, log throttle, stats cache policy (incl. 20 concurrent cold reads = 1 query), readiness states, the worker wake loop (push / poll switching, lost wake-ups, nextDue cap) and every service's signals as unit tests; real-Redis + real-Postgres tests for channel isolation, dirty-flag TTLs, the claim and CAS, subscriber recovery after an outage (through a TCP proxy that cuts connections), the stats API (HIT / STALE / MISS / BYPASS, 20 concurrent requests = 1 query, two API instances sharing a cache), `/ready` degraded during an outage, pickup latency under 500ms with a 20s fallback poll, push vs poll query counts, delayed jobs and retries on time in push mode, and the real `src/worker.ts` process picking up a job via a wake-up

**Milestone 6: auth, multi-tenancy and rate limiting**

- **Tenants and API keys** (migration `0005`): `tenants` (slug ids such as `acme`; existing data belongs to `default`) and `api_keys` with roles `admin` (everything, incl. key management, queue settings, destructive DLQ operations and schedules), `producer` (enqueue + reads) and `reader` (reads only)
- Keys look like `pq_<prefix>_<secret>`: a random 12-char non-secret **prefix** (unique index, the lookup key and what listings and logs show) plus 256 random bits; **only `SHA-256(token)` is stored** (`bytea`, length-checked). Verification = one indexed lookup by prefix + `crypto.timingSafeEqual` on the digests, so lookup time does not depend on how many secret characters match; the plaintext is returned once (`201`, `cache-control: no-store`)
- **Auth hook on every non-health route**: an `onRequest` hook (before the body is read) authenticates `Authorization: Bearer ...`; every route must declare `config.access` (`public`, `read`, `enqueue` or `manage`) or the app refuses to start, so a new endpoint can never ship unauthenticated by accident. `401 UNAUTHORIZED` + `WWW-Authenticate: Bearer` for missing / malformed / unknown / revoked / expired keys, `403 FORBIDDEN` when the role lacks the permission; only `/health` and `/ready` are public
- Verified keys are cached per instance (LRU, `AUTH_CACHE_TTL_MS` = 5s, cache key = token digest); revocation is immediate on the serving instance and within the TTL elsewhere; `last_used_at` is written at most once a minute per key, off the request path
- Key management API for the caller's tenant: `POST /api-keys`, `GET /api-keys` (keyset, `includeRevoked`), `GET /api-keys/:id`, `DELETE /api-keys/:id` (idempotent revoke), `POST /api-keys/:id/rotate` (new key, old one revoked now or after `gracePeriodSeconds`), `GET /auth/whoami`
- Operator CLI (`npm run api-key -- ...`, `node dist/cli/api-key.js` in the image) to create tenants, bootstrap the first admin key, list / revoke keys and set a tenant's rate limit, without the API
- **Tenant isolation** (migration `0006`): queues belong to a tenant and names are unique per tenant (two tenants can both own `emails`); jobs, schedules and DLQ entries inherit the owner through their queue and every lookup is scoped by the caller's tenant, so another tenant's resources are `404`, never `403` (no existence oracle). Stats cache entries are per tenant + queue
- **Enqueue rate limiting**: token buckets in Redis per tenant (default `RATE_LIMIT_TENANT_PER_SEC` = 100/s, burst 200, overridable per tenant) and optionally per queue (`PUT /queues/:name/settings`). One **atomic Lua script** refills lazily from `redis.call('TIME')` (no clock skew between API instances) and takes a token from **all buckets or none**; keys expire once the bucket would be full. Success responses carry `RateLimit-Limit` / `RateLimit-Remaining` / `RateLimit-Reset` (IETF draft), refusals are `429 RATE_LIMITED` with `Retry-After` (whole seconds, >= 1). Validation runs first, so bad requests cost no quota
- Redis failure policy: `RATE_LIMIT_FAIL_MODE=open` (default; enqueues go through, throttled `ratelimit.unavailable` warning) or `closed` (`503` + `Retry-After: 1` for enqueues only; reads keep working)
- **Queue concurrency limits** (migration `0007`): `maxConcurrency` per queue caps running jobs **across the whole worker fleet**, enforced inside the lease transaction (lock the capped queue rows `FOR NO KEY UPDATE` in id order, count running jobs via the new partial `jobs_running_queue_idx`, lease at most the remaining budget with a `LATERAL ... LIMIT`). Uncapped queues keep the single-statement M5 lease; producers are never blocked by the lock
- `GET /queues/:name/settings` and `PUT /queues/:name/settings` (full replacement, `null` clears a limit; creates the queue so limits can be set before the first job)
- Structured log events: `auth.forbidden`, `apikey.created`, `apikey.revoked`, `apikey.rotated`, `apikey.touch_failed`, `ratelimit.limited`, `ratelimit.unavailable`, `ratelimit.disabled`, `queue.settings_updated`; request logs carry `tenantId` and `keyPrefix`
- Zod config for auth and rate limits (e.g. `closed` without `REDIS_URL` is rejected at startup); compose and CI updated: CI bootstraps an admin key with the compiled CLI, boots the compiled API and checks public probes, `401` without a key, and `201, 201, 429 + Retry-After` at burst 2
- Tests: token parsing / generation (no modulo bias), the auth plugin (401 / 403 / public routes / startup guard), `ApiKeyService` (cache, expiry, rotation), `RateLimiter` policy (binding bucket, fail open / closed) and header mapping as unit tests; real-Postgres + real-Redis tests for migrations `0005`-`0007` (backfill to `default`, constraints), the key repository and CLI, the key API (hash at rest, rotation with grace, revocation, cross-tenant `404`s), tenant isolation for jobs, DLQ and schedules, the Lua bucket (refill on the Redis clock, all-or-nothing, **no over-admission under 200 concurrent takes from several connections**), the rate-limited API (two API instances share buckets, tenant and queue limits, fail open / closed through an outage), and **concurrency caps under parallel leasing** (many connections leasing at once and three real workers never exceed the cap, no deadlocks with overlapping capped queues)

**Milestone 7: dashboard API and observability**

- **Dashboard API** (reader role, tenant-scoped like every other read; another tenant's queue is `404`): `GET /dashboard/queues` (depth per status, oldest queued job and its age, concurrency cap; keyset-paginated by name), `GET /dashboard/queues/:name/throughput?window=1h&bucket=1m` (zero-filled, epoch-aligned buckets of enqueued / succeeded / failed / timed-out / dead-lettered plus totals), `GET /dashboard/queues/:name/latency?window=1h` (p50 / p95 / p99 / max of execution time and queue wait, in ms) and `GET /dashboard/queues/:name/failure-rate?window=1h` (attempt failure rate and terminal job failure rate)
- Zod-validated `window` / `bucket` (`<int><s|m|h|d>`): buckets 10s-1d, windows up to 7d (latency up to 1d), the window must be a whole number of buckets and at most 1440 of them; latency percentiles use at most `DASHBOARD_LATENCY_MAX_SAMPLES` most recent attempts and say so (`truncated`)
- Migration `0008`: `job_attempts.queue_id` (backfilled) and `ready_at` (when the job became runnable, written at lease time), so queue wait is measured on the database clock; `job_attempts_queue_finished_idx (queue_id, finished_at) INCLUDE (status, started_at, ready_at)` makes every dashboard attempt query an index-only range scan; `jobs.traceparent` with a format `CHECK`
- **Prometheus metrics** (prom-client, one registry per process, `role` label): `http_request_duration_seconds` by method, **route template** and status (unmatched routes collapse to `__unmatched__`); `jobs_enqueued_total` (created / duplicate), `ratelimit_decisions_total`; worker `jobs_leased_total`, `jobs_completed_total`, `jobs_failed_total` (outcome x disposition), `jobs_dead_lettered_total` (reason x source worker / reaper), `job_execution_seconds`, `job_queue_wait_seconds`, `lease_lost_total` (stage), `jobs_released_total`, `lease_errors_total`, `reaper_reclaimed_total`, `worker_jobs_in_flight`; scheduler `scheduler_is_leader`, `scheduler_leadership_changes_total`, `scheduler_ticks_total`, `scheduler_tick_duration_seconds`, `schedule_occurrences_total`; process metrics outside tests
- `pulsequeue_queue_depth{tenant,queue,status}` is **read lazily from Postgres at scrape time** by the API: cached (`METRICS_QUEUE_DEPTH_CACHE_MS`), single-flight, `READ ONLY` with its own `statement_timeout`, limited to `METRICS_MAX_QUEUE_SERIES` queues; on failure the scrape still succeeds with the previous snapshot and `queue_depth_collect_errors_total` counts it
- Label cardinality is bounded: at most `METRICS_MAX_QUEUE_SERIES` tenant/queue pairs per process, the rest are recorded as `__other__`; job ids, raw URLs and error messages are never labels
- **Metrics listener per process** (`METRICS_PORT`, default host `127.0.0.1`): `GET`/`HEAD /metrics` only, optional bearer token compared in constant time (required in production on a non-loopback host), `401` + `WWW-Authenticate`, `404` / `405` otherwise; a listener that cannot bind is logged (`metrics.listen_failed`) and the process keeps working
- **OpenTelemetry tracing** (off by default; `TRACING_ENABLED` + `OTEL_EXPORTER_OTLP_ENDPOINT`, OTLP/HTTP, parent-based ratio sampling): a SERVER span per request continues an inbound W3C `traceparent`; the enqueue PRODUCER span's context is stored in `jobs.traceparent`; the worker's `job.execute` CONSUMER span is its child, so one trace covers request -> enqueue -> every attempt (retries included), with outcome, disposition and errors recorded on the span. `trace_id` / `span_id` are added to request and job log lines. Tracing off is a true no-op (nothing stored, nothing exported)
- docker-compose: **Prometheus** (scrapes app, worker and scheduler replicas through Docker DNS, with the bearer token), **Grafana** (provisioned Prometheus + Jaeger datasources and a PulseQueue dashboard: throughput, failure ratio, DLQ, depth, execution and queue-wait percentiles, HTTP p99 per route, in-flight, lost leases, rate limits, scheduler leadership) and **Jaeger** all-in-one (OTLP :4318, UI :16686)
- Structured log events: `metrics.listening`, `metrics.listen_failed`, `metrics.scrape_failed`, `metrics.queue_depth_failed`, `metrics.series_limit_reached`, `tracing.enabled`, `tracing.export_failed`; zod config for metrics and tracing (tracing without an endpoint, or an open production metrics port without a token, fail at startup)
- CI metrics smoke test: the compiled API and worker serve `/metrics` on their own ports, `401` without / with a wrong token, `200` with the HTTP histogram (route template), enqueue counter, queue depth and the worker's completion and queue-wait series, and `404` for `/metrics` on the API port
- Tests: duration parsing and query schemas, the metrics listener (token, methods, collect failures), series limiter, tracing (disabled no-op, traceparent round trip, invalid input, sampling) and every metrics class as unit tests; real-Postgres tests for the dashboard (exact percentiles over 100 seeded attempts, aligned zero-filled buckets, tenant isolation, validation, truncation), migration `0008` (backfill, `CHECK`, index-only plan), the queue-depth collector (cache, single-flight, queue limit, lock timeout, database down), the metrics lifecycle enqueue -> execute -> retry -> dead-letter through the real API and worker runtime, scheduler leadership metrics on two replicas, and **trace propagation from an HTTP caller through `jobs.traceparent` into the worker span** with matching `trace_id` in logs

### Planned

- Load / chaos testing and capacity notes (M8)

## Enqueue request flow

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant A as Auth hook (onRequest)
    participant K as ApiKeyService
    participant R as Route (jobs.ts)
    participant L as RateLimiter
    participant RD as Redis (token buckets)
    participant S as JobService
    participant J as Queue + JobRepository
    participant PG as PostgreSQL

    P->>A: POST /queues/emails/jobs<br/>Authorization: Bearer pq_ab12cd34ef56_...
    A->>K: authenticate(header)
    K->>K: parse pq_prefix_secret, digest = sha256(token)
    alt digest in LRU cache (AUTH_CACHE_TTL_MS)
        K-->>A: principal (tenant, role, tenant limit)
    else cache miss
        K->>PG: SELECT key + tenant WHERE prefix = $1 AND not revoked / expired
        K->>K: timingSafeEqual(stored hash, digest)
        K-->>A: principal, or 401 UNAUTHORIZED
    end
    A->>A: role allows "enqueue"? else 403 FORBIDDEN
    A->>R: request (body read + parsed only now)
    R->>R: zod: queue name, headers, body (400 costs no quota)
    R->>L: consume(tenant, "emails", tenant limit)
    L->>PG: queue rate limit (cached RATE_LIMIT_SETTINGS_CACHE_TTL_MS)
    L->>RD: EVALSHA token bucket [rl:{t:acme}, rl:{t:acme}:q:emails]
    RD->>RD: TIME, refill each bucket, take 1 from all or none
    alt allowed
        RD-->>L: allowed, remaining per bucket
        L-->>R: RateLimit-Limit / Remaining / Reset (tightest bucket)
    else bucket empty
        RD-->>L: refused, retry after ms
        R-->>P: 429 RATE_LIMITED + Retry-After + RateLimit-*
    else Redis unavailable
        L-->>R: fail open: skip (no headers) / fail closed: 503 + Retry-After
    end
    R->>S: enqueue(tenant, input)
    S->>J: ensure queue (tenant_id, name), insertIdempotent
    J->>PG: INSERT ... ON CONFLICT (queue_id, idempotency_key) DO NOTHING
    alt new key
        R-->>P: 201 Created + Location + RateLimit-*
    else same key, same body
        R-->>P: 200 OK + Idempotent-Replayed: true
    else same key, different body
        R-->>P: 422 IDEMPOTENCY_KEY_REUSED
    end
    S-->>RD: after COMMIT: wake-up + stats dirty flag (best effort)
```

Authentication runs in an `onRequest` hook, before Fastify reads the body, so an unauthenticated client cannot make the server buffer or parse anything. Rate limiting runs after validation and before any write; idempotent replays count like new enqueues (a retry storm is still load). Routes only parse input and shape output. `JobService` owns the business rules (defaults, payload budget, initial status, replay semantics, pagination). Repositories own every SQL statement and translate driver errors into domain errors. Domain types, limits and errors live under `src/domain/`.

## Scheduler tick flow

```mermaid
sequenceDiagram
    autonumber
    participant A as Scheduler A (leader)
    participant B as Scheduler B (standby)
    participant PG as PostgreSQL
    participant W as Workers

    A->>PG: pg_try_advisory_lock(key) on its dedicated session
    PG-->>A: true (scheduler.leader_acquired)
    B->>PG: pg_try_advisory_lock(key)
    PG-->>B: false, retry every SCHEDULER_LEADER_RETRY_INTERVAL_MS
    loop every SCHEDULER_TICK_INTERVAL_MS (same session as the lock)
        A->>PG: BEGIN, SELECT now()
        A->>PG: SELECT schedules WHERE enabled AND next_run_at <= now()<br/>ORDER BY next_run_at LIMIT batch FOR UPDATE SKIP LOCKED
        loop each claimed schedule (SAVEPOINT)
            A->>A: cron occurrences in [next_run_at, now], misfire policy
            A->>PG: INSERT job (run_at = occurrence, key schedule:id:occurrence)<br/>ON CONFLICT DO NOTHING
            A->>PG: UPDATE schedules SET next_run_at = next occurrence after now,<br/>last_run_at, last_job_id, fire_count
        end
        A->>PG: COMMIT, then log scheduler.fired / misfire_skipped
    end
    W->>PG: promote due scheduled jobs, lease queued jobs (unchanged worker path)
    Note over A,PG: SIGTERM: finish the tick, pg_advisory_unlock_all(), close (scheduler.leader_released).<br/>Crash or partition: the session ends, Postgres drops the lock and aborts the open tick.
    B->>PG: pg_try_advisory_lock(key)
    PG-->>B: true (scheduler.leader_acquired), B ticks from the committed next_run_at
```

Duplicates are prevented three times over: only the lock holder ticks; the claim takes row locks and advances `next_run_at` in the same transaction, so even two concurrent ticks cannot evaluate the same occurrence; and the job insert uses the deterministic idempotency key `schedule:<id>:<occurrence ISO time>` against the existing unique index, so a replayed occurrence is a no-op (logged as `scheduler.duplicate_suppressed`).

## Dead-letter and replay flow

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker / Reaper
    participant PG as PostgreSQL
    participant Op as Operator
    participant R as Route (dlq.ts)
    participant S as DlqService
    participant D as DeadLetterRepository

    W->>PG: fail(lease, retryDelayMs = null, reason) or reapExpired(poison threshold)
    Note over W,PG: ONE statement: UPDATE jobs SET status = dead, dead_reason, dead_lettered_at<br/>WHERE id AND status = running AND lease_token = mine,<br/>plus the job_attempts row. Stale lease: zero rows, nothing written.
    W->>W: log event dlq.dead_lettered, then onJobFailed hook (notification only)
    Op->>R: GET /queues/billing/dlq?reason=attempts_exhausted
    R->>S: list -> D: keyset page on jobs_dlq_idx
    Op->>R: GET /dlq/:jobId
    R->>S: get -> D: job + attempt history
    Op->>R: POST /queues/billing/dlq/:jobId/replay {"delayMs": 0}
    R->>S: replay(queue, jobId)
    S->>D: findById (fast 404 / 409, reason for the audit log)
    S->>D: replay: UPDATE ... SET status = queued, attempts = 0, DLQ fields cleared,<br/>replay_count + 1 WHERE id AND queue_id AND status = dead
    alt this request won
        D-->>S: row
        R-->>Op: 200 job (queued) + Location, log dlq.replayed
    else a concurrent replay / purge won (row lock, WHERE re-checked)
        D-->>S: no row
        R-->>Op: 409 JOB_NOT_DEAD_LETTERED (details.status)
    end
    W->>PG: lease picks it up again: attempt_number continues (history kept)
```

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue
    [*] --> scheduled: enqueue with delayMs / future runAt, or fired by a schedule ahead of time
    queued --> running: leased (SKIP LOCKED), attempts + 1
    scheduled --> queued: promoted once run_at <= now() (SKIP LOCKED batch)
    running --> running: heartbeat extends the lease
    running --> succeeded: handler resolved
    running --> scheduled: threw or timed out, attempts left (run_at = now + backoff)
    running --> dead: NonRetryableError / InvalidPayloadError, or last attempt failed
    running --> queued: reaper - lease expired (attempt counts)
    running --> dead: reaper - last attempt, or poison (N lease expiries)
    running --> queued: shutdown release (attempt refunded)
    dead --> queued: replay (attempts reset, history kept)
    dead --> scheduled: replay with delayMs
    dead --> [*]: purge (job + attempts deleted)
    succeeded --> [*]
```

`failed` exists in the enum from Milestone 2 but is no longer written: migration `0003` moved existing `failed` rows into the DLQ and a CHECK forbids it, so every terminal failure is inspectable and replayable.

## Worker execution flow

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker loop
    participant PG as PostgreSQL
    participant H as Handler
    participant R as Reaper (in every worker)

    W->>PG: promote due scheduled jobs to queued (bounded batch, SKIP LOCKED)
    W->>PG: lease(queues, limit = free slots) in ONE statement
    Note over W,PG: SELECT ... WHERE status = queued ORDER BY priority DESC, run_at, id<br/>LIMIT n FOR UPDATE SKIP LOCKED, then UPDATE to running<br/>(new lease_token, attempts + 1) and INSERT a job_attempts row
    PG-->>W: leased jobs with lease_token and attempt id
    W->>H: handler(ctx) with AbortSignal and job logger
    loop every WORKER_HEARTBEAT_INTERVAL_MS while running
        W->>PG: extend lease_expires_at WHERE lease_token matches
        alt zero rows (lease lost)
            W->>H: abort (LeaseLostError), write nothing
        end
    end
    alt handler resolved
        W->>PG: job + attempt succeeded (fenced on lease_token)
    else threw or timed out, attempts left
        W->>PG: attempt failed or timed_out, job scheduled at now + full-jitter backoff
    else NonRetryableError / InvalidPayloadError, or last attempt
        W->>PG: job dead-lettered with a reason in the same fenced statement, then the onJobFailed hook runs
    end
    R->>PG: every WORKER_REAPER_INTERVAL_MS reclaim expired leases (SKIP LOCKED)
    Note over R,PG: attempts left: queued again. Last attempt, or lease_expiries reaching<br/>WORKER_POISON_LEASE_EXPIRIES: dead-lettered. The dead worker's attempt is timed_out.
```

On SIGTERM/SIGINT the worker stops leasing (a lease that races the signal is released untouched), waits up to `WORKER_SHUTDOWN_TIMEOUT_MS` for in-flight jobs, then aborts the rest and **releases** their leases: they become `queued` immediately (no wait for the lease to expire), and the attempt is recorded as `released` and refunded so deploys do not eat retry budget. Then pending wake-ups and stats invalidations are flushed, Redis and the pool close, and the process exits 0.

### Writing a handler

```ts
import { InvalidPayloadError, NonRetryableError } from './worker/errors.js';
import type { HandlerRegistry } from './worker/registry.js';

export function registerEmailHandlers(registry: HandlerRegistry): void {
  registry.register<{ to: string; template: string }>('emails', async ({ job, signal, logger }) => {
    if (!job.payload.to) throw new InvalidPayloadError('missing recipient'); // DLQ: invalid_payload
    if (await isUnsubscribed(job.payload.to)) throw new NonRetryableError('unsubscribed'); // DLQ: non_retryable
    // Idempotent: the provider de-duplicates on our job id, so a re-run sends nothing twice.
    await mailer.send({ ...job.payload, idempotencyKey: job.id }, { signal });
    logger.info('email sent'); // already tagged with jobId, queue, attempt, workerId
  });
}
```

Register handlers in `src/worker/handlers/index.ts` (it ships `echo` and `demo`). Resolve = success; throw = failed attempt (retried with backoff); `NonRetryableError` (or any error with `retryable: false`, handy for errors from libraries) = dead-letter now with reason `non_retryable`; `InvalidPayloadError` = dead-letter now with reason `invalid_payload`. Pass `signal` to anything that can be cancelled: it fires on timeout, lost lease or shutdown. The `demo` queue exercises the failure paths from the API, e.g. `{"payload":{"durationMs":500,"failUntilAttempt":3},"maxAttempts":5}`.

## Wake-up flow (Milestone 5)

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant API as API (JobService)
    participant PG as PostgreSQL
    participant R as Redis
    participant S as Worker subscriber
    participant W as Worker loop

    S->>R: SUBSCRIBE pq:wakeup:emails (own queues only, on every 'ready')
    R-->>S: subscribed (wakeup.subscribed)
    S->>W: listening = true: push mode + one catch-up poll
    W->>PG: nextDueInMs(queues): earliest scheduled run_at
    Note over W: idle sleep = jitter(WORKER_WAKEUP_FALLBACK_POLL_MS),<br/>capped by the next due run_at (nothing publishes when time passes)
    P->>API: POST /queues/emails/jobs
    API->>PG: INSERT job (autocommit)
    PG-->>API: committed
    API-->>P: 201 Created
    API->>R: PUBLISH pq:wakeup:emails (after commit, fire-and-forget,<br/>leading + trailing edge per WAKEUP_COALESCE_MS)
    R-->>S: message
    S->>W: wake(): cut the idle sleep short
    W->>PG: promote due + lease (unchanged SKIP LOCKED path)
    alt message lost (Redis blip, publisher crashed after commit)
        W->>PG: fallback poll after at most the fallback interval
    end
    alt Redis connection drops
        S->>W: listening = false: poll mode (WORKER_POLL_INTERVAL_MS)
        S->>R: reconnect with jittered backoff, SUBSCRIBE again
        S->>W: listening = true + catch-up poll (messages in between are gone)
    end
```

A wake-up carries no job, only "queue X may have work"; the lease query against Postgres decides what runs, so a duplicate, late or spurious wake-up is one cheap empty lease call, and a lost one costs latency up to the fallback interval, never a job. Publishes happen only after the change committed, so a woken worker always sees the new row. Retries, released leases and reaper reclaims publish too; a retry's backoff `run_at` is in the future, which is why push-mode workers cap their sleep by the next due `run_at` instead of waiting for the fallback poll.

## Stats read flow (Milestone 5)

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as API (StatsCache)
    participant R as Redis
    participant PG as PostgreSQL
    participant Wr as Any writer (API / worker / scheduler)

    Wr->>PG: COMMIT a change to queue emails
    Wr->>R: SET pq:stats:dirty:{emails} 1 PX ttl (coalesced per queue)
    C->>A: GET /queues/emails/stats
    A->>R: pipeline HGET entry data + EXISTS dirty (one round trip)
    alt entry, not dirty
        A-->>C: 200 x-cache: HIT
    else entry, dirty, younger than STATS_CACHE_MIN_REFRESH_MS
        A-->>C: 200 x-cache: STALE (bounds recompute rate)
    else entry, dirty, older: claim = DEL dirty (exactly one caller wins cluster-wide)
        A->>PG: winner: ONE aggregate statement (in-process single-flight)
        A->>R: Lua CAS: write only if no newer snapshot (seq = computedAt)
        A-->>C: 200 x-cache: MISS (losers of the claim: STALE)
    else no entry (cold or expired)
        A->>PG: one statement per instance (single-flight joins concurrent readers)
        A->>R: CAS write with PEXPIRE STATS_CACHE_TTL_MS
        A-->>C: 200 x-cache: MISS
    else Redis disabled, down or slower than REDIS_COMMAND_TIMEOUT_MS
        A->>PG: compute directly (still single-flight)
        A-->>C: 200 x-cache: BYPASS
    end
```

Staleness is bounded twice: after a change a reader sees the old numbers for at most `STATS_CACHE_MIN_REFRESH_MS` (default 1s); if the invalidation itself was lost (Redis blip between commit and SET, crashed writer), the entry still expires after `STATS_CACHE_TTL_MS` (default 10s). `computedAt` in the body says when the numbers were true.

## Trace and metrics flow (Milestone 7)

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant A as API (observability hooks)
    participant R as Enqueue route
    participant PG as PostgreSQL
    participant W as Worker
    participant H as Handler
    participant J as Jaeger (OTLP)
    participant P as Prometheus

    C->>A: POST /queues/emails/jobs<br/>traceparent: 00-T-c1-01
    A->>A: SERVER span s1 (parent c1, trace T)<br/>request log gets trace_id T
    A->>R: auth, validation, rate limit
    R->>R: PRODUCER span e1 "pulsequeue.enqueue" (child of s1)
    R->>PG: INSERT job ... traceparent = 00-T-e1-01
    R-->>C: 201 Created
    A->>A: observe http_request_duration_seconds{route="/queues/:name/jobs"}<br/>jobs_enqueued_total{result="created"}
    A-->>J: batch export s1, e1 (async, best effort)
    W->>PG: lease (SKIP LOCKED) returns traceparent,<br/>attempt row gets queue_id + ready_at = run_at
    W->>W: jobs_leased_total, job_queue_wait_seconds (started_at - ready_at, DB clock)
    W->>W: CONSUMER span x1 "job.execute" (parent e1, trace T)
    W->>H: run(job), logs carry trace_id T, span_id x1
    alt success
        W->>PG: complete (fenced on lease_token)
        W->>W: jobs_completed_total, job_execution_seconds{outcome="succeeded"}
    else failure
        W->>PG: fail: retry (scheduled) or dead-letter (same statement)
        W->>W: jobs_failed_total{disposition}, jobs_dead_lettered_total{reason, source="worker"},<br/>span status ERROR + exception event
    end
    W-->>J: batch export x1 (one trace: request -> enqueue -> each attempt)
    P->>A: GET :9464/metrics (Bearer) -> HTTP + enqueue series, queue_depth from Postgres (cached)
    P->>W: GET :9464/metrics (Bearer) -> job series
```

The worker does not hold a span open across retries: each attempt is its own `job.execute` span with the enqueue span as parent, so a job retried three times shows three sibling attempt spans under one enqueue in Jaeger, with the backoff gaps visible between them. Jobs fired by the scheduler (or replayed from the DLQ) have no caller, so their attempts start a new trace.

## Data model

```mermaid
erDiagram
    tenants ||--o{ api_keys : "credentials"
    tenants ||--o{ queues : owns
    api_keys |o--o{ api_keys : "rotated_from"
    queues ||--o{ jobs : contains
    queues ||--o{ schedules : "recurring jobs"
    jobs ||--o{ job_attempts : "attempt history"

    tenants {
        text id PK "slug a-z0-9- max 63, 'default' seeded"
        text name
        int rate_limit_per_sec "NULL = RATE_LIMIT_TENANT_PER_SEC"
        int rate_limit_burst "set together with per_sec"
        timestamptz created_at
        timestamptz updated_at
    }
    api_keys {
        uuid id PK
        text tenant_id FK "ON DELETE CASCADE"
        text name
        text prefix UK "12 chars a-z0-9, not secret"
        bytea secret_hash "sha256(token), 32 bytes"
        api_key_role role "admin producer reader"
        timestamptz created_at
        timestamptz expires_at "NULL = never"
        timestamptz last_used_at "throttled, 1/min"
        timestamptz revoked_at
        uuid rotated_from FK "predecessor on rotate"
    }
    queues {
        bigint id PK
        text tenant_id FK "UK with name"
        text name "a-z0-9._- max 64, UK per tenant"
        int max_concurrency "NULL = uncapped"
        int rate_limit_per_sec "NULL = no queue bucket"
        int rate_limit_burst
        timestamptz created_at
        timestamptz updated_at
    }
    jobs {
        uuid id PK
        bigint queue_id FK
        job_status status "queued scheduled running succeeded failed dead"
        smallint priority "0-100, higher first"
        jsonb payload
        int attempts
        int max_attempts
        timestamptz run_at
        text idempotency_key "unique per queue"
        text request_fingerprint
        text last_error
        timestamptz finished_at
        text locked_by "worker id while running"
        uuid lease_token "fencing token, new per lease"
        timestamptz locked_at
        timestamptz lease_expires_at "visibility timeout"
        timestamptz heartbeat_at
        dead_letter_reason dead_reason "set iff status = dead"
        timestamptz dead_lettered_at "set iff status = dead"
        int lease_expiries "poison counter, reset on replay"
        int replay_count
        timestamptz last_replayed_at
        text traceparent "M7, W3C context of the enqueue span (tracing on)"
        timestamptz created_at
        timestamptz updated_at
    }
    schedules {
        uuid id PK
        bigint queue_id FK
        text name "unique per queue"
        text cron_expression
        text timezone "IANA, default UTC"
        jsonb payload "job template"
        smallint priority
        int max_attempts
        schedule_misfire_policy misfire_policy "skip fire_once catch_up"
        boolean enabled
        timestamptz next_run_at "NULL iff paused"
        timestamptz last_run_at
        uuid last_job_id
        bigint fire_count
        text last_error
        timestamptz created_at
        timestamptz updated_at
    }
    job_attempts {
        bigint id PK
        uuid job_id FK
        int attempt_number "unique per job"
        job_attempt_status status "running succeeded failed timed_out released"
        text worker_id
        text error
        timestamptz started_at
        timestamptz finished_at
        bigint queue_id FK "M7, denormalized for dashboard range scans"
        timestamptz ready_at "M7, jobs.run_at at lease time; NULL before 0008"
    }
```

`jobs.attempts` counts attempts that consumed retry budget (a released attempt is refunded); `job_attempts.attempt_number` numbers every execution, released ones included. Since Milestone 3 a terminal failure ends in `dead`: the job stays in `jobs` with `dead_reason`, `dead_lettered_at`, `last_error` and `attempts`, its queue is the original queue, and every attempt stays in `job_attempts`. A replay resets `attempts` (retry budget) and `lease_expiries`, clears the DLQ fields and bumps `replay_count`; `attempt_number` keeps counting, so the history reads "failed, failed, failed, replay, succeeded".

Schema invariants added in [`migrations/0002_worker_leasing.sql`](migrations/0002_worker_leasing.sql): a job is `running` **if and only if** it holds a complete lease (`jobs_running_has_lease`), at most one `running` attempt exists per job (`job_attempts_one_running_per_job_uq`, so a double lease would fail loudly), and an attempt has `finished_at` exactly when it is no longer running.

Invariants added in [`migrations/0003_dead_letter_queue.sql`](migrations/0003_dead_letter_queue.sql): a job is `dead` **if and only if** it has `dead_reason` and `dead_lettered_at` (`jobs_dead_has_metadata`), and `failed` can no longer be written (`jobs_no_legacy_failed`).

Invariants added in [`migrations/0004_scheduling.sql`](migrations/0004_scheduling.sql): a schedule is `enabled` **if and only if** it has a `next_run_at` (`schedules_enabled_has_next_run`), names are unique per queue and match the API's name rule, and priority / attempts / cron / timezone lengths are range-checked.

Added in Milestone 6: [`migrations/0005_tenants_api_keys.sql`](migrations/0005_tenants_api_keys.sql) creates `tenants` (seeding `default`) and `api_keys`: the prefix is unique and format-checked, `secret_hash` must be exactly 32 bytes (a SHA-256 digest; a plaintext token can never be stored by mistake), a tenant's rate limit is both-or-neither, and expiry must be after creation. [`0006_queue_tenancy.sql`](migrations/0006_queue_tenancy.sql) adds `queues.tenant_id` (catalog-only `ADD COLUMN ... DEFAULT 'default'`, so every existing queue belongs to `default` without a table rewrite) and replaces the global name unique constraint with `(tenant_id, name)`. Jobs, schedules and DLQ entries reach their tenant through `queue_id`, so they need no column of their own. Rollout: drain M5 API servers before applying `0006` (their `ON CONFLICT (name)` has no matching constraint afterwards); M5 workers and schedulers are unaffected. [`0007_queue_limits.sql`](migrations/0007_queue_limits.sql) adds the nullable, range-checked `max_concurrency` and queue rate limit columns.

Indexes (see [`0001`](migrations/0001_create_queues_jobs_attempts.sql), [`0002`](migrations/0002_worker_leasing.sql), [`0003`](migrations/0003_dead_letter_queue.sql), [`0004`](migrations/0004_scheduling.sql), [`0005`](migrations/0005_tenants_api_keys.sql), [`0006`](migrations/0006_queue_tenancy.sql) and [`0007`](migrations/0007_queue_limits.sql)):

| Index                                                                                      | Serves                                                                                                        |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `jobs_lease_idx (queue_id, priority DESC, run_at, id) WHERE status IN (queued, scheduled)` | M2/M3 lease query; kept only for workers still running during a rolling deploy, dropped in a later migration  |
| `jobs_ready_idx (queue_id, priority DESC, run_at, id) WHERE status = queued`               | M4 lease query: only runnable rows, so the scan never walks a future job                                      |
| `jobs_scheduled_due_idx (queue_id, run_at, id) WHERE status = scheduled`                   | promotion: range scan touching only the delayed / backoff rows that are due                                   |
| `schedules_due_idx (next_run_at) WHERE enabled`                                            | scheduler claim, in due order; paused schedules are not in it                                                 |
| `schedules_queue_created_idx (queue_id, created_at DESC, id DESC)`                         | `GET /queues/:name/schedules` keyset pages                                                                    |
| `jobs_queue_created_idx (queue_id, created_at DESC, id DESC)`                              | `GET /queues/:name/jobs` keyset pages; also covers the `queue_id` foreign key                                 |
| `jobs_queue_status_created_idx (queue_id, status, created_at DESC, id DESC)`               | the same listing filtered by `status`                                                                         |
| `jobs_queue_idempotency_key_uq (queue_id, idempotency_key) WHERE key IS NOT NULL`          | race-safe idempotency via `ON CONFLICT`; partial, so keyless jobs cost nothing                                |
| `job_attempts_job_attempt_uq (job_id, attempt_number)`                                     | one row per attempt; attempt history in order                                                                 |
| `jobs_running_lease_expiry_idx (lease_expires_at) WHERE status = running`                  | reaper scan for expired leases; holds only in-flight rows, so it is tiny                                      |
| `job_attempts_one_running_per_job_uq (job_id) WHERE status = running`                      | invariant: never two in-flight attempts of one job                                                            |
| `jobs_dlq_idx (queue_id, dead_lettered_at DESC, id DESC) WHERE status = dead`              | DLQ keyset listing, bulk replay (oldest first) and `olderThan` purges; partial, so it is as small as the DLQ  |
| `api_keys_prefix_uq (prefix)`                                                              | authentication: one index probe per (uncached) request                                                        |
| `api_keys_tenant_created_idx (tenant_id, created_at DESC, id DESC)`                        | `GET /api-keys` keyset pages                                                                                  |
| `queues_tenant_name_uq (tenant_id, name)`                                                  | API lookups and race-safe get-or-create, per tenant                                                           |
| `queues_name_idx (name)`                                                                   | workers and the scheduler: a shared fleet consumes a queue name across all tenants                            |
| `jobs_running_queue_idx (queue_id) WHERE status = running`                                 | concurrency caps: count running jobs per capped queue inside the lease transaction; holds only in-flight rows |

Added in Milestone 7 ([`migrations/0008_observability.sql`](migrations/0008_observability.sql)): `jobs.traceparent` (format `CHECK`, `NULL` when tracing is off), `job_attempts.queue_id` (backfilled from `jobs`) and `job_attempts.ready_at`, plus:

| Index                                                                                                                          | Serves                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `job_attempts_queue_finished_idx (queue_id, finished_at) INCLUDE (status, started_at, ready_at) WHERE finished_at IS NOT NULL` | dashboard throughput, latency and failure rate: index-only range scans of one queue's finished attempts in a window |

Dashboard depth counts reuse `jobs_queue_status_created_idx`, enqueue throughput `jobs_queue_created_idx` and dead-letter throughput `jobs_dlq_idx`; no other index was needed.

The integration suite runs `EXPLAIN` on the lease, reaper, pagination and dashboard queries and asserts that they use these indexes.

## Design decisions and trade-offs

### Milestone 7

| Decision                                                                          | Why                                                                                                                                                                                                                                                                                                                                                        | Trade-off                                                                                                                                                                                                                                                                                             |
| --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Dashboard numbers computed in SQL from `job_attempts`, not from Prometheus**    | The dashboard API must be tenant-scoped, exact and available without a metrics stack; Postgres already holds every attempt. `date_bin` + `generate_series` give aligned, zero-filled buckets in one statement, `percentile_cont` gives exact percentiles, and the new covering index makes each query an index-only range scan of one queue's window.      | Cost grows with attempts in the window, so windows are capped (7d, latency 1d) and percentiles use at most `DASHBOARD_LATENCY_MAX_SAMPLES` recent attempts (flagged `truncated`). No cache: windows move with `now()`. At very high volume, pre-aggregated rollup tables would replace the raw scans. |
| **Denormalize `queue_id` and `ready_at` onto `job_attempts`**                     | "Attempts of queue X finished in the last hour" without joining every attempt to `jobs`; attempts never change queue, so the copy cannot go stale. `ready_at` (the job's `run_at` at lease time) makes queue wait `started_at - ready_at` on the database clock, immune to worker clock skew and to retries' backoff.                                      | Two more columns per attempt; rows from before `0008` have no `ready_at` and are excluded from wait percentiles (they still count for throughput and failure rate). The backfill is one statement, fine at this scale; a large table would backfill in batches before the index.                      |
| **`/metrics` on a separate listener (`METRICS_PORT`), not on the API port**       | Keeps the M6 deny-by-default hook intact (no "public except /metrics" hole), never exposes cross-tenant series to tenant keys, and works the same for workers and schedulers, which have no HTTP server. Loopback by default; a non-loopback production listener refuses to start without `METRICS_TOKEN` (constant-time compare).                         | One more port to wire in deployment; Prometheus needs the bearer token. A metrics port that fails to bind is logged, not fatal, so a misconfiguration costs observability, not job processing.                                                                                                        |
| **Bounded label cardinality**                                                     | Route templates instead of URLs, `__unmatched__` for 404 scans, no job ids or error messages as labels, and a per-process cap on tenant/queue pairs (`__other__` beyond it), because one tenant creating thousands of queues must not be able to take down Prometheus.                                                                                     | Beyond the cap, per-queue detail is lost in metrics (the dashboard API still has it).                                                                                                                                                                                                                 |
| **Queue depth read lazily from Postgres at scrape time, in the API only**         | Depth is a property of the database, not of any process, so counting it in worker memory would be wrong across replicas. A collector runs one grouped count when scraped, cached for `METRICS_QUEUE_DEPTH_CACHE_MS`, single-flight, `READ ONLY` with a `statement_timeout`, and limited to N queues; on failure it serves the previous snapshot.           | Each API replica exports the same depth (use `max by (queue)` in PromQL, as the dashboard does), and the value is up to the cache TTL old. Counting `queued` rows is O(backlog); a huge backlog would need counter tables.                                                                            |
| **Worker counts outcomes where they are decided**                                 | Completed / failed / dead-lettered counters are incremented only after the fenced write succeeds, so a worker that lost its lease does not count work it did not commit (it increments `lease_lost_total{stage}` instead). Reaper reclaims are separate series (`source="reaper"`).                                                                        | Counters are per process and reset on restart; PromQL `rate()` handles resets, and exact historical totals come from the dashboard API.                                                                                                                                                               |
| **Trace context in a column (`jobs.traceparent`), one span per attempt**          | The queue is the async boundary: storing the enqueue span's W3C context on the row lets any worker, any time later, continue the same trace with no shared state. Each attempt is its own `job.execute` span (CONSUMER) under the enqueue span, so retries and backoff gaps are visible.                                                                   | Only the API and worker are traced; the scheduler and DLQ replays start new traces, and `pg` / `ioredis` calls are not auto-instrumented (explicit spans keep overhead and dependencies small; auto-instrumentation can be layered on later).                                                         |
| **Tracing off by default, explicit context instead of a global SDK**              | `createTracing` builds a provider per process and passes context explicitly (no global context manager, no monkey-patching), so tests run tracers side by side with in-memory exporters and "off" is a genuine no-op: no spans, no exporter, nothing written to `traceparent`. OTLP/HTTP with a batch processor works with Jaeger, Tempo or any collector. | Libraries that rely on the global OTel API do not see our spans; spans that are not yet exported are lost if a process is killed (flushed on graceful shutdown).                                                                                                                                      |
| **Prometheus, Grafana and Jaeger as compose services, provisioned from the repo** | `docker compose up` gives a working dashboard with no clicking; the dashboard JSON and scrape config are reviewed like code. Workers and schedulers are discovered through Docker DNS, so scaling needs no config change.                                                                                                                                  | Dev-grade settings (anonymous Viewer in Grafana, a default dev token, in-memory Jaeger storage); production would use real secrets, persistent trace storage and alerting rules.                                                                                                                      |

### Milestone 6

| Decision                                                                                        | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Trade-off                                                                                                                                                                                                                                                                           |
| ----------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Opaque bearer keys `pq_<prefix>_<secret>`, only `SHA-256(token)` stored, lookup by prefix**   | A DB leak or a log line never yields a usable credential. The non-secret prefix makes lookup one unique-index probe (no scan over hashes, no per-key salt search), gives operators a handle to show in listings and logs, and the `pq_` shape is greppable for secret scanners. The digest comparison uses `crypto.timingSafeEqual`, so response time does not reveal how much of a guessed secret matched.                                                                                                                                                                                                                            | Plain SHA-256, not bcrypt / argon2: correct for 256-bit random secrets (nothing to brute force), and a slow hash on every request would cost milliseconds of CPU per call. Not suitable if keys were ever user-chosen passwords. Lost keys cannot be recovered, only rotated.       |
| **API keys over JWT / OAuth**                                                                   | Server-to-server producers want a long-lived credential that can be revoked instantly and audited (`last_used_at`); stateful keys give that with one indexed read, and no signing-key infrastructure.                                                                                                                                                                                                                                                                                                                                                                                                                                  | A Postgres read per uncached request (and one more moving part than a self-contained JWT); mitigated by the cache. No user identities or fine-grained scopes yet, just three roles.                                                                                                 |
| **Per-instance verified-key cache (`AUTH_CACHE_TTL_MS` = 5s, LRU, keyed by token digest)**      | Keeps auth off the database for hot producers. Keyed by the digest, so a wrong secret with a valid prefix is never served from cache. Revoke / rotate evicts on the serving instance at once.                                                                                                                                                                                                                                                                                                                                                                                                                                          | A revoked key keeps working for up to the TTL on _other_ instances (bounded, documented, `0` disables the cache). Redis pub/sub invalidation would close the gap but would make auth depend on Redis.                                                                               |
| **Auth hook in `onRequest`, deny-by-default route declarations**                                | Runs before the body is read, so anonymous traffic cannot make the server buffer and parse payloads. Every route must declare `config.access`; a route without one fails startup, which turns "forgot to protect the new endpoint" into a boot error instead of a vulnerability.                                                                                                                                                                                                                                                                                                                                                       | Health probes are the only public routes; anything else that should be public needs an explicit `access: 'public'`.                                                                                                                                                                 |
| **Tenant scoping in services and SQL, cross-tenant = `404`**                                    | One choke point: services take the principal's `tenantId` and repositories filter on it, so a forgotten check fails closed (not found). `404` instead of `403` avoids an existence oracle for other tenants' job ids.                                                                                                                                                                                                                                                                                                                                                                                                                  | Shared tables (row-level tenancy), not schema- or database-per-tenant: cheap and simple, but a noisy tenant shares Postgres capacity (hence rate limits and concurrency caps). Postgres RLS would add defense in depth; not used yet.                                               |
| **Token bucket in Redis via one Lua script, on the Redis clock, all-or-nothing across buckets** | A token bucket allows short bursts (`burst`) while enforcing an average rate, which matches producer traffic better than fixed windows (no 2x burst at window edges) and needs only two fields per bucket. The script is atomic, so concurrent API instances can never over-admit, and `redis.call('TIME')` avoids clock skew between API hosts. Taking from the tenant and queue buckets together means a request refused by one bucket does not spend the other's tokens. Keys use a `{t:<tenant>}` hash tag so both buckets of a request live in one Redis Cluster slot, and they expire once full, so idle tenants cost no memory. | One Redis round trip (~0.2ms) per enqueue. The script is the only shared mutable state in Redis that enforces something; a Redis failover can lose bucket state (a refilled burst, at most once). A sliding-window log would be more exact but costs memory per request.            |
| **Fail open by default, `closed` as an option**                                                 | The limiter protects the system from overload; making every enqueue fail when Redis (an optional component) is down would turn a cache outage into a full producer outage. Teams that sell quotas can choose `closed` (enqueues `503` + `Retry-After: 1`, reads unaffected).                                                                                                                                                                                                                                                                                                                                                           | In `open` mode limits are not enforced during a Redis outage (logged, throttled, as `ratelimit.unavailable`).                                                                                                                                                                       |
| **Only enqueue is rate limited; validation first, replays count**                               | Enqueue is the write path that creates work and storage; reads are cheap and bounded by pagination. Invalid requests are rejected before spending quota (a broken client is not locked out by its own errors). Idempotent replays count, because a retry storm is still load.                                                                                                                                                                                                                                                                                                                                                          | Expensive reads (big list pages) are not limited yet; a separate read budget would be the next step.                                                                                                                                                                                |
| **Concurrency caps enforced in the lease transaction, not with a Redis semaphore**              | The cap must hold across the whole worker fleet and survive crashes. Counting `running` rows in the same transaction that leases means a crashed worker's slots free themselves when the reaper expires the lease, with no leaked permits. Capped queue rows are locked `FOR NO KEY UPDATE` in id order: concurrent leasers serialize per capped queue (no over-lease, no deadlock), while producers' FK checks (`KEY SHARE`) are not blocked. A `LATERAL ... LIMIT budget` with `SKIP LOCKED` leases exactly the remaining budget.                                                                                                    | Leasers of the same capped queue serialize for the length of one short transaction; uncapped queues keep the single-statement M5 path with no extra cost. The cap is per (tenant, queue) and counts leased jobs, so a job between lease and start still occupies a slot (intended). |
| **Queue settings via full replacement `PUT`; limits cached 5s per instance**                    | `PUT` is idempotent and makes "clear this limit" explicit (`null`). It creates the queue, so limits can be in place before the first job arrives. The queue rate limit is read on every enqueue, so it is cached (`RATE_LIMIT_SETTINGS_CACHE_TTL_MS`) and forgotten on the instance that served the `PUT`.                                                                                                                                                                                                                                                                                                                             | Other API instances apply a new queue rate limit within the TTL. Concurrency caps are read inside the lease transaction and apply on the next lease.                                                                                                                                |
| **Bootstrap with a CLI, no "auth off" switch**                                                  | There is no chicken-and-egg admin endpoint and no way to deploy an open API by misconfiguration; the first key is minted by an operator with DB access (`npm run api-key -- create ... --create-tenant`).                                                                                                                                                                                                                                                                                                                                                                                                                              | One extra step in local setup; tests use a helper that mints keys directly.                                                                                                                                                                                                         |

### Milestone 5

| Decision                                                                                                                                       | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Trade-off                                                                                                                                                                                                                                                                                                                            |
| ---------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Redis is an optional accelerator; Postgres stays the only source of truth**                                                                  | Wake-ups and cache entries are hints and derived data. Every path has a Postgres fallback (fallback poll, direct stats query), so a Redis outage, a failover that loses writes, or `FLUSHALL` costs latency and some extra queries, never a job or a wrong lease. That is also why Redis runs without persistence.                                                                                                                                                                                                                                                                    | Two systems to operate. The fallback paths must stay tested (they are: outage tests through a TCP proxy, `BYPASS` and poll-mode tests), or they rot.                                                                                                                                                                                 |
| **Pub/sub wake-ups, not a Redis list / stream of job ids**                                                                                     | A wake-up says only "queue X may have work"; the worker still leases from Postgres with `SKIP LOCKED`, so ordering, priority, fencing and exactly-one-owner semantics are unchanged and duplicates are harmless. A list or stream would be a second queue that has to agree with Postgres (lost or doubled ids on failover, two-phase hand-off). Postgres `LISTEN/NOTIFY` was the alternative with no new dependency, but it needs a dedicated session per worker (no PgBouncer transaction pooling) and NOTIFY takes a global lock at commit, which serializes busy enqueue traffic. | Pub/sub is at-most-once with no replay: a message published while a worker is disconnected is gone. Covered by the catch-up poll on (re)subscribe and the fallback poll; worst-case pickup latency while Redis misbehaves is `WORKER_WAKEUP_FALLBACK_POLL_MS` (30s default; tune it down if that matters more than idle query load). |
| **Publish after COMMIT, fire-and-forget**                                                                                                      | A worker woken before the commit would lease nothing and go back to sleep: a lost wake-up by construction. Fire-and-forget keeps Redis off the request's critical path (no added latency, no failure mode for producers).                                                                                                                                                                                                                                                                                                                                                             | A crash between commit and publish loses the wake-up (fallback poll catches it). A transactional outbox would make the signal durable, but the signal is not worth a write.                                                                                                                                                          |
| **Per-queue channels, coalesced with a leading + trailing edge** (`WAKEUP_COALESCE_MS` = 50ms)                                                 | Workers subscribe only to the queues they consume, so a hot queue does not wake unrelated workers. The leading edge keeps a single enqueue's latency at zero; the trailing edge guarantees the last change in a burst is always followed by a publish; together at most about 2 publishes per queue per window regardless of load.                                                                                                                                                                                                                                                    | Up to one window of extra latency for the jobs inside a burst (the first already woke the worker, which is busy anyway). Many tiny queues = many channels (cheap in Redis).                                                                                                                                                          |
| **Push mode sleep = jittered fallback, capped by the next due `run_at`**                                                                       | Time passing publishes nothing, so without the cap a delayed job or a retry backoff would wait for the fallback poll. One indexed `min(run_at)` query per idle cycle (only in push mode) replaces ~30 empty lease polls. A minimum sleep (25ms) prevents a spin if a due job is not leasable (e.g. another worker just took it).                                                                                                                                                                                                                                                      | One extra tiny query per idle cycle. Jobs made due by an external `UPDATE` (bypassing the API) wait for the fallback poll.                                                                                                                                                                                                           |
| **Dedicated subscriber connection with explicit SUBSCRIBE on every reconnect** (`autoResubscribe: false`) and equal-jitter exponential backoff | A connection in subscriber mode cannot run commands, so it must be separate. Re-subscribing ourselves gives an exact "subscribed again" moment for the catch-up poll and flips the worker back to push mode only after Redis confirmed the subscription. Jittered backoff keeps a fleet of workers from reconnecting in lockstep after a Redis restart.                                                                                                                                                                                                                               | Two Redis connections per worker (commands + subscriber).                                                                                                                                                                                                                                                                            |
| **Stats cache: invalidate by a dirty flag, not by deleting or rewriting the entry**                                                            | "Delete on write" races: a reader that computed before the commit writes its old result back after the delete and the cache stays wrong until TTL. Writers instead set a flag (O(1), idempotent, coalesced); the reader that later clears the flag (atomic `DEL`, one winner cluster-wide) snapshots _after_ clearing, so any change after that point dirties the new entry again. Writes are a Lua compare-and-set on the snapshot time (`computedAt`), so an older snapshot can never replace a newer one.                                                                          | Every committed change in every process costs one coalesced `SET PX` (about 2 per queue per 100ms when hot).                                                                                                                                                                                                                         |
| **Serve STALE inside `STATS_CACHE_MIN_REFRESH_MS`, hard TTL as the safety net**                                                                | On a busy queue every change dirties the entry; recomputing on each read would turn the cache into a pass-through exactly when the count query is most expensive. The min-refresh window caps recomputes at about 1 per queue per second across all API instances. The TTL bounds staleness even if invalidations are lost.                                                                                                                                                                                                                                                           | Stats can be up to 1s (min refresh) or, if invalidations were lost, 10s (TTL) behind. Fine for dashboards and autoscalers; anything that needs exact numbers reads jobs directly.                                                                                                                                                    |
| **Two-level stampede protection** (cluster-wide claim + in-process single-flight)                                                              | The claim ensures one instance recomputes a dirty entry; single-flight ensures N concurrent requests on a cold key cost one query per instance (tested: 20 concurrent requests = 1 query; a regression where a leftover dirty flag made every request force its own query was caught by that test).                                                                                                                                                                                                                                                                                   | A cold key hit by K instances at the same instant still costs K queries (one each); a distributed lock would fix that at the price of lock waits and timeouts on the read path.                                                                                                                                                      |
| **Stats from one aggregate statement** (correlated subqueries, one snapshot)                                                                   | All numbers are mutually consistent and timestamped with the database clock; each aggregate uses an existing index (no migration needed).                                                                                                                                                                                                                                                                                                                                                                                                                                             | The count is O(jobs in the queue) per recompute; that is the cost the cache amortizes. Very large queues will want counter tables or retention (M8).                                                                                                                                                                                 |
| **`/ready`: Redis down = `degraded` with HTTP 200, Postgres down = `not_ready` 503**                                                           | Redis is shared by every replica; failing readiness on it would pull the whole API out of the load balancer during a Redis blip, turning a latency problem into a full outage, even though every request still works. The check stays in the body (and in logs) so monitoring alerts on it.                                                                                                                                                                                                                                                                                           | A load balancer cannot route around a single replica with a broken Redis path; alerting has to look at the body or the `redis.*` logs, not just the status code.                                                                                                                                                                     |
| **ioredis** (v6) as the client                                                                                                                 | Mature, widely deployed, typed, first-class pub/sub, Lua `defineCommand`, pipelines, Cluster and Sentinel support, and fine-grained failure controls (`enableOfflineQueue`, `commandTimeout`, `maxRetriesPerRequest`, `retryStrategy`, `reconnectOnError`) that make "fail fast, fall back to Postgres" easy to express. `node-redis` v4+ is an equally valid choice (official, RESP3); ioredis' explicit offline-queue and per-command timeout semantics fit this design slightly better.                                                                                            | One more runtime dependency. Fail-fast settings mean commands issued during a reconnect fail immediately (by design: callers fall back).                                                                                                                                                                                             |
| **Keys and channels namespaced by `REDIS_KEY_PREFIX`, stats keys hash-tagged (`{queue}`) and versioned (`v1`)**                                | Several environments can share one Redis; the entry and its dirty flag share a Cluster slot so the read is one pipeline; a deploy that changes the cached shape starts from fresh keys instead of misreading old ones.                                                                                                                                                                                                                                                                                                                                                                | Key names are an API between versions; the dirty flag is deliberately unversioned so old and new writers invalidate the same entries during a rolling deploy.                                                                                                                                                                        |

### Milestone 4

| Decision                                                                                                                                                | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Trade-off                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Leader election with a session-level advisory lock** (`pg_try_advisory_lock`) on a dedicated connection, and **every tick runs on that same session** | The lock and the work share one session, which is a real fence: if the leader crashes, is partitioned or its backend is killed, Postgres drops the lock _and_ aborts the open tick transaction together, so a stale leader can never commit after a new one took over (tested with `pg_terminate_backend`). No TTL to tune and nothing to renew: a clean shutdown hands over within one standby retry interval, a killed process as soon as Postgres sees the session end. The lock key includes `current_schema()`, so deployments sharing a database elect separately. | Needs a real session: through PgBouncer only in session mode (or a direct connection for the scheduler). One connection per replica is held for its lifetime. A **lease row** (`UPDATE leader SET holder, expires_at WHERE expires_at < now()`) would work through transaction pooling and be visible in an admin UI, but needs a renewal loop, a TTL that trades failover time against false failovers, and an explicit fencing token on every write, or a paused leader can act after its lease expired. On a half-open TCP partition, failover waits for keepalive / server timeouts. |
| **Claim with `FOR UPDATE SKIP LOCKED` + advance `next_run_at` in the same transaction**, even though only the leader ticks                              | Defence in depth: correctness does not depend on the election being perfect. Two ticks that ever overlapped (tested by running two ticks concurrently) partition the due rows instead of double-firing, and a crashed tick rolls back the job insert and the advance together. Batches are bounded; a full batch re-ticks at once.                                                                                                                                                                                                                                       | A schedule's row is locked for the length of a tick (milliseconds); a `pause` racing a tick waits for it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| **Deterministic idempotency key** `schedule:<id>:<occurrence ISO>` on the existing jobs unique index                                                    | A third guard that needs no new machinery: replaying the same occurrence (manual `next_run_at` edits, a future bug) inserts nothing and logs `scheduler.duplicate_suppressed`. The key also tells operators which occurrence produced a job.                                                                                                                                                                                                                                                                                                                             | The key occupies the producer-facing idempotency namespace of the queue (prefix `schedule:`); purging the job frees it, as with any key.                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **Misfire policies** `skip` / `fire_once` (default) / `catch_up` + grace window + catch-up limit                                                        | After an outage, what is right depends on the job: a cache refresh should run once (`fire_once`), a per-hour billing roll-up needs every hour (`catch_up`, but bounded by `SCHEDULER_MAX_CATCH_UP` so a week-long outage does not dump 10k stale jobs), a "send the 9am digest" job should not run at 3pm (`skip`). Occurrences within the grace window are never misfires, so normal ticks and failovers always fire. Every occurrence is fired or dropped exactly once; `next_run_at` then jumps past `now`.                                                           | `skipped` counts in logs are a lower bound on long outages (the walk stops once the policy is decided; `skippedMore` says so). The grace window must be at least two ticks (validated).                                                                                                                                                                                                                                                                                                                                                                                                  |
| **Database clock everywhere** (`now()` of the tick transaction for occurrences, `greatest(runAt, now())` for delayed jobs)                              | Skewed scheduler or API hosts cannot fire early, late or twice; the same clock that workers lease against decides when a job is due.                                                                                                                                                                                                                                                                                                                                                                                                                                     | One extra `SELECT now()` per tick / create.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| **Jobs are enqueued with `run_at` = the occurrence** (clamped to now)                                                                                   | Workers see when a run was _meant_ to happen; late catch-up runs are ordered by their original time.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | None significant.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Pause stores `next_run_at = NULL`; resume recomputes from now**                                                                                       | A paused period is not replayed on resume (resume means "from now on"), and a CHECK keeps `enabled` and `next_run_at` in step so the tick can never pick up a paused schedule. Pause / resume are idempotent.                                                                                                                                                                                                                                                                                                                                                            | To replay a paused window, enqueue jobs explicitly.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| **Per-schedule savepoints** in the tick                                                                                                                 | One broken schedule (e.g. its payload became too large after `JOB_MAX_PAYLOAD_BYTES` was lowered, or its zone disappeared from the tz database) records `lastError` and retries later, instead of rolling back and blocking every other schedule in the batch. Infrastructure errors still fail the whole tick, and repeated failures make the leader step down.                                                                                                                                                                                                         | A savepoint per schedule (cheap).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **Delayed jobs promoted to `queued` before leasing**, ready index on `queued` only (expand/contract: old index kept)                                    | The M2 lease index sorted by priority first and could not skip future rows, so every poll walked all not-yet-due high-priority delayed/backoff jobs. Now the lease scan sees only runnable rows and promotion is a `run_at` range scan. `run_at <= now()` is still re-checked at lease time, so a job is never leased early.                                                                                                                                                                                                                                             | One extra small statement per poll; a job becomes runnable at the next poll after `run_at` (the same latency as before).                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **Cron library**: `cron-parser` v5 (maintained, Luxon timezones), wrapped in `src/lib/cron.ts`                                                          | Correct DST behaviour and IANA zones without writing a cron engine; the wrapper pins the semantics we promise (5 fields or 6 with seconds = 0, minute resolution, explicit errors instead of the library's lenient padding of short expressions, "never matches" rejected).                                                                                                                                                                                                                                                                                              | One more dependency (plus Luxon); sub-minute schedules are intentionally unsupported.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| **Separate scheduler process**, not inside the API or the worker                                                                                        | Its own scaling (2 replicas are enough), its own shutdown semantics and one long-lived connection, without coupling API latency or worker drains to it. Same image, different command.                                                                                                                                                                                                                                                                                                                                                                                   | One more deployable; compose and CI run it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

### Milestone 3

| Decision                                                                                                           | Why                                                                                                                                                                                                                                                                                                                                                                                                                                      | Trade-off                                                                                                                                                                                                                                                                                                     |
| ------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **DLQ = `status = dead` + metadata columns on `jobs`**, not a separate `dead_letters` table                        | The move is a single-row UPDATE, so it can be part of the existing fenced fail/reap statement. `job_attempts` keeps its FK to the same id, so history needs no copying. A replay is an UPDATE of the same row: the job keeps its id (clients polling `GET /jobs/:id` see it come back) and its idempotency key (a producer retrying enqueue still gets this job, not a duplicate). A CHECK ties `dead` to its metadata for every writer. | DLQ rows share the `jobs` table and its vacuum load. A partial index keeps DLQ reads proportional to the DLQ, and the lease index is partial on runnable rows, so the hot path does not see dead rows. A separate table would make "archive the DLQ elsewhere" easier; retention via purge covers it for now. |
| **Dead-letter inside the fail / reap statement** instead of a follow-up write in the `onJobFailed` hook            | Atomic with recording the attempt: no window where a crash leaves a job terminal but outside the DLQ, and the `WHERE lease_token = $mine` fence means a stale worker matches zero rows, so it cannot dead-letter a job another worker re-leased (tested). The hook stays as a post-commit notification point.                                                                                                                            | The hook is now notification only; side effects in it (paging, webhooks) are best effort and may be lost on a crash, which is acceptable for alerts.                                                                                                                                                          |
| **`failed` retired**: backfilled into the DLQ and forbidden by a CHECK                                             | One terminal-failure state that is always inspectable and replayable; the CHECK proves no code path still writes `failed`. Postgres cannot drop enum values safely, so the value stays.                                                                                                                                                                                                                                                  | A dead enum value in the type. `?status=failed` on the jobs list is valid but always empty.                                                                                                                                                                                                                   |
| **Poison detection by lease-expiry count** (`lease_expiries`, default threshold 3) plus typed non-retryable errors | A job that OOMs or hangs its worker would otherwise get `max_attempts` (up to 25) chances to take down a worker each time. Counting expiries on the row is O(1) in the reap statement, unlike scanning attempt history. Typed `InvalidPayloadError` / `NonRetryableError` (or `retryable: false`) give operators a reason that says "fix the producer" vs "fix the handler".                                                             | A job that legitimately runs longer than its lease without heartbeating (event-loop blocked) is classified as poison after 3 expiries; the threshold is configurable and the job can be replayed.                                                                                                             |
| **Replay = guarded single UPDATE `WHERE status = 'dead'`** (no explicit locks, no SERIALIZABLE)                    | Under READ COMMITTED a concurrent replay blocks on the row lock, then re-checks the WHERE on the committed row, sees it is no longer dead and matches nothing. So exactly one of N concurrent replays (or a replay racing a purge) wins; the loser gets `409 JOB_NOT_DEAD_LETTERED` with the job's current status. Bulk replay and bulk purge use `FOR UPDATE SKIP LOCKED` so concurrent batches partition the DLQ.                      | Replay is safe, not idempotent in the "same response twice" sense: a retried replay request returns 409 (the job's current state is in `details`); an `Idempotency-Key` on replay could be added later.                                                                                                       |
| **Replay resets `attempts` and keeps `attempt_number`**, no payload override                                       | The operator fixed the cause (deployed a handler fix, downstream recovered) and wants a full retry budget; history stays truthful. Changing a payload in place would break the idempotency-key fingerprint contract; the fix for a bad payload is to purge and re-enqueue.                                                                                                                                                               | No in-place payload edits from the DLQ.                                                                                                                                                                                                                                                                       |
| **Bulk operations bounded** (`limit` <= 1000 replay, <= 10000 purge) and bulk purge **requires `olderThan`**       | Short statements and bounded row locks / cascades; an operator cannot wipe a DLQ with an accidental bare `DELETE`. Callers loop until the count is below `limit`.                                                                                                                                                                                                                                                                        | Large clean-ups need several calls.                                                                                                                                                                                                                                                                           |
| **DLQ endpoints scoped by queue** (`/queues/:name/dlq/...`), except the read-only `GET /dlq/:jobId`                | A replay or purge must name the queue it acts on, so a wrong id cannot touch another queue's DLQ (404). Reading by id alone is convenient from a log line.                                                                                                                                                                                                                                                                               | Slightly longer URLs.                                                                                                                                                                                                                                                                                         |
| **Structured events** (`event: dlq.*`) logged at the route / worker                                                | Every DLQ state change is one JSON log line with a stable key, request id, queue, job id and reason: alertable and auditable without a separate audit table.                                                                                                                                                                                                                                                                             | Logs are the audit trail; a durable audit table can come with M6 multi-tenancy.                                                                                                                                                                                                                               |

### Milestone 2

| Decision                                                                                           | Why                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Trade-off                                                                                                                                                                                                                             |
| -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`FOR UPDATE SKIP LOCKED`** in Postgres for leasing, rather than advisory locks or a Redis queue  | Workers partition the ready set instead of blocking on each other: a row another worker is leasing is skipped, not waited for. The lease is a normal row update, so it is transactional with the attempt row and protected by constraints. Advisory locks would need a separate lock id space, are session-scoped (leak with pooled connections / PgBouncer) and are not visible as job state. A Redis list (BRPOPLPUSH-style) would add a second source of truth and lose jobs on a Redis failover without AOF `always`. | Every lease is a write on the primary; throughput is bounded by Postgres (thousands of jobs/s, not hundreds of thousands). Idle workers poll until Redis wake-ups land in M5.                                                         |
| **Lease = one statement** (CTE: `SELECT ... SKIP LOCKED` then `UPDATE` then `INSERT job_attempts`) | One round trip, atomic, and row locks live only for that statement, never while a handler runs. Same for complete / fail / release / reap.                                                                                                                                                                                                                                                                                                                                                                                | Data-modifying CTEs are dense SQL; each is covered by integration tests.                                                                                                                                                              |
| **Visibility timeout + heartbeats** (lease 30s, heartbeat 10s by default)                          | A crashed or partitioned worker's jobs come back after at most one lease duration, while long jobs keep their lease as long as the worker is alive. Config validation requires at least two heartbeats per lease so one slow heartbeat does not lose the job.                                                                                                                                                                                                                                                             | Tuning: a shorter lease recovers crashed jobs sooner but costs more heartbeat writes (`concurrency / heartbeat interval` updates/s per worker) and risks false expiry during GC pauses or DB hiccups; a longer lease delays recovery. |
| **Fencing token** (`lease_token`, new uuid per lease) on every write                               | A worker that paused past its lease (GC, network) and then finishes cannot overwrite the outcome of the worker that re-leased the job: its `UPDATE ... WHERE lease_token = $mine` touches zero rows and the result is discarded and logged. A lost heartbeat also aborts the handler's `AbortSignal`.                                                                                                                                                                                                                     | One extra uuid column; handlers that ignore their signal may still finish their side effects (see at-least-once).                                                                                                                     |
| **At-least-once delivery; handlers must be idempotent**                                            | Exactly-once execution is impossible across a crash between "side effect done" and "outcome recorded". The system guarantees no job is lost and no job runs concurrently with itself; a job can run again after a crash, a timeout or a lost lease. Handlers make that safe with idempotency keys downstream (e.g. `job.id`), upserts or check-before-act.                                                                                                                                                                | Pushes a design duty to handler authors; documented in the handler contract and `JobContext`.                                                                                                                                         |
| **Database clock for every lease and retry timestamp** (`now()`, never `Date.now()`)               | Worker hosts with skewed clocks cannot shorten or extend leases or make retries run early.                                                                                                                                                                                                                                                                                                                                                                                                                                | Each timestamp needs a DB round trip, which already happens.                                                                                                                                                                          |
| **Exponential backoff with full jitter**: `random(0, min(cap, base x 2^(n-1)))`                    | Spreads retries of jobs that failed together (e.g. a downstream outage) so they do not return in synchronized waves; full jitter does the least total work under contention (AWS "Exponential Backoff and Jitter"). Capped so `max_attempts = 25` does not mean a multi-day wait. Pure function with injectable randomness, unit tested.                                                                                                                                                                                  | Individual retries can come back almost immediately (delay near 0); "equal jitter" would guarantee a minimum wait at the cost of more clustering.                                                                                     |
| **Reaper in every worker**, not a leader-elected singleton                                         | `SKIP LOCKED` makes concurrent reapers safe and a heartbeat that lands first wins (the `WHERE` is re-checked after the lock), so no coordination or extra service.                                                                                                                                                                                                                                                                                                                                                        | N reapers poll a tiny partial index; negligible, and M4's leader election can take it over if needed.                                                                                                                                 |
| **Expired lease counts as an attempt; graceful release does not**                                  | A job that keeps crashing its worker (OOM, segfault in a native lib) must eventually stop, so reaped attempts count. A deploy is not the job's fault, so a released attempt is refunded and the job is runnable at once. `attempt_number` keeps numbering every execution, so history stays complete.                                                                                                                                                                                                                     | `attempts` and `attempt_number` can differ; documented in the data model.                                                                                                                                                             |
| **Exhausted jobs end in `failed` + `onJobFailed` hook**, not `dead` yet (superseded in M3)         | Milestone scope: DLQ inspect/replay/purge is M3. The hook is the single seam where M3 moves jobs to `dead`, and it already fires from both the worker and the reaper.                                                                                                                                                                                                                                                                                                                                                     | Until M3, `failed` covers both non-retryable and exhausted jobs (the reason is in the logs and `last_error`).                                                                                                                         |
| **Handlers keyed by queue name**, worker leases only queues it can run                             | One queue = one kind of work keeps routing trivial and lets operators scale or isolate a workload by running workers with `WORKER_QUEUES`. A worker never leases a job it has no handler for; a typo in `WORKER_QUEUES` fails at startup.                                                                                                                                                                                                                                                                                 | Several job types in one queue need a dispatch on payload inside the handler.                                                                                                                                                         |
| **Per-attempt timeout + `AbortSignal`**                                                            | A hung handler would otherwise heartbeat forever and pin a slot. On timeout the attempt is `timed_out` and retried; the signal lets cooperative handlers stop their I/O.                                                                                                                                                                                                                                                                                                                                                  | JavaScript cannot kill a promise: a handler that ignores its signal keeps running in the background after the worker has moved on.                                                                                                    |
| **Bounded concurrency with "lease only free slots"**                                               | No local buffer of leased-but-not-started jobs, so a worker never holds leases it cannot service, and a full batch immediately polls again while a partial one sleeps (poll interval jittered +-20% so replicas do not poll in lockstep).                                                                                                                                                                                                                                                                                 | Up to one poll interval of latency for new jobs when idle (until M5 wake-ups).                                                                                                                                                        |
| **Graceful shutdown with a drain budget below the orchestrator grace period**                      | `WORKER_SHUTDOWN_TIMEOUT_MS` (25s) < compose `stop_grace_period` (40s) / k8s default 30s, so the process finishes or releases everything before SIGKILL. Node is PID 1 in the image (no npm wrapper), so it actually receives SIGTERM.                                                                                                                                                                                                                                                                                    | Jobs longer than the drain budget are interrupted on every deploy (released, not penalized); very long jobs should checkpoint.                                                                                                        |

### Milestone 1

| Decision                                                                                                   | Why                                                                                                                                                                                                                                                                                                                                                                                                             | Trade-off                                                                                                                                                                               |
| ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Postgres is the source of truth** for jobs (Redis only for wake-ups and caching, M5)                     | A job queue's core promise is "never lose or double-run a job". Postgres gives durable, transactional writes, constraints that hold for every writer, and `FOR UPDATE SKIP LOCKED` for contention-free leasing. Enqueue can share a transaction with a producer's own data (transactional outbox) in future.                                                                                                    | Lower peak throughput than an in-memory broker, and the jobs table needs vacuum/retention care. Fine up to thousands of jobs/s per primary; see scaling notes.                          |
| **Own ~200-line SQL migration runner** instead of node-pg-migrate / Prisma / Knex                          | Plain `.sql` files are exactly what runs in production and are easy to review. The runner covers the parts that matter: ordered versions, one transaction per file, `schema_migrations` with sha256 checksums (editing an applied file fails), an advisory lock so concurrent replicas serialize, `lock_timeout` for DDL, and an opt-out for `CREATE INDEX CONCURRENTLY`. No extra dependency and fully tested. | No down migrations (deliberate: roll forward with a new migration), no generator CLI. If the team grows, swapping to node-pg-migrate is easy because the files are plain SQL.           |
| **Separate `migrate` step** (compose service / CI step), not "migrate on app start"                        | Schema changes happen once, before any new API replica takes traffic, and a failed migration blocks the rollout instead of crash-looping every replica.                                                                                                                                                                                                                                                         | One more deploy step. The advisory lock still makes migrate-on-start safe if someone chooses it.                                                                                        |
| **Idempotency via a partial unique index + `INSERT ... ON CONFLICT DO NOTHING`**                           | Check-then-insert races under concurrency. With `ON CONFLICT`, exactly one concurrent request wins and the rest are told "already exists" by the database itself. The 25-way concurrency test proves exactly one job is created.                                                                                                                                                                                | Under READ COMMITTED a loser cannot see the winner's row in the same statement's snapshot, so the existing row is read with a second statement (one extra round trip, only on replays). |
| `DO NOTHING` + re-select rather than a no-op `DO UPDATE ... RETURNING`                                     | Replays stay read-only: no row lock, no dead tuple, `updated_at` untouched.                                                                                                                                                                                                                                                                                                                                     | The extra `SELECT` above.                                                                                                                                                               |
| **Request fingerprint** = `v1:` + sha256 of canonical JSON (sorted keys) of the normalized request         | Distinguishes an honest retry (replay, `200` + `Idempotent-Replayed: true`) from a client bug that reuses a key for a different job (`422 IDEMPOTENCY_KEY_REUSED`, as in the IETF Idempotency-Key draft). Key order and explicitly-sent defaults do not change the fingerprint.                                                                                                                                 | Keys live as long as the job row. Once retention (M3/M8) deletes a job its key can be reused; a separate key-expiry window would be needed for stricter guarantees.                     |
| Replays return the job's **current** state                                                                 | The producer learns what happened to the job it already created (it may be running or done).                                                                                                                                                                                                                                                                                                                    | Not a byte-for-byte copy of the first response.                                                                                                                                         |
| **Keyset pagination** over `(created_at DESC, id DESC)` with an opaque, versioned base64url cursor         | O(limit) per page at any depth (no `OFFSET` scans), stable while new jobs arrive, and served directly by an index. The cursor carries the exact microsecond timestamp text from Postgres, never a JS `Date`, so rows within the same millisecond are never skipped or repeated (tested at 1 µs resolution).                                                                                                     | No "jump to page N" and no total count (an exact `count(*)` would be expensive on a hot table anyway).                                                                                  |
| `limit + 1` fetch                                                                                          | The last page returns `nextCursor: null` instead of forcing an extra empty request.                                                                                                                                                                                                                                                                                                                             | One extra row read per page.                                                                                                                                                            |
| **`run_at` computed from the database clock** (`now() + delay`)                                            | Several API replicas with skewed clocks still agree on when a delayed job becomes runnable; workers (M2) compare against the same clock.                                                                                                                                                                                                                                                                        | The API can't compute `run_at` without a round trip (it doesn't need to).                                                                                                               |
| Constraints in the DB **and** policy limits in the app                                                     | CHECKs (priority 0-100, attempts within max, key needs a fingerprint, queue name format) protect against any writer. The API applies tighter product limits (e.g. `maxAttempts <= 25`) that can change without a migration.                                                                                                                                                                                     | Two places to keep in sync; the DB range is deliberately wider where policy may move.                                                                                                   |
| Payload limit on the **parsed** payload (UTF-8 bytes of its JSON), plus a coarse route `bodyLimit`         | Gives an exact, documented limit (`JOB_MAX_PAYLOAD_BYTES`, `413`) independent of whitespace/escaping, while the transport cap stops huge bodies before parsing.                                                                                                                                                                                                                                                 | Bodies between the two limits are parsed before being rejected.                                                                                                                         |
| `pg` errors mapped to domain errors at the repository boundary                                             | Services never look at SQLSTATEs. `22P05` (e.g. NUL in jsonb) becomes 400, `57014` (statement timeout) and connection failures become 503, and unknown errors stay a logged 500 without leaking internals.                                                                                                                                                                                                      | The mapping table needs to grow with new failure modes.                                                                                                                                 |
| Explicit pool timeouts (`PG_CONNECTION_TIMEOUT_MS`, `PG_STATEMENT_TIMEOUT_MS`) and a pool `error` listener | A sick database becomes fast 503s rather than piled-up requests. An idle client dropping (failover) can't crash the process.                                                                                                                                                                                                                                                                                    | Long analytical queries need their own connection settings (migrations turn `statement_timeout` off).                                                                                   |
| UUID v4 job ids generated by Postgres                                                                      | Unguessable, globally unique, safe to expose.                                                                                                                                                                                                                                                                                                                                                                   | Random keys spread B-tree inserts; UUID v7 (time-ordered) is a candidate if PK index write amplification shows up in M8 load tests.                                                     |

### Milestone 0

| Decision                                                               | Why                                                        | Trade-off                                            |
| ---------------------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------- |
| Layered folders (`routes` -> `services` -> `repositories` -> `domain`) | Keeps HTTP concerns out of business logic; easy to test    | Slightly more boilerplate                            |
| Zod for env config                                                     | Fail fast before listen; typed `Env` everywhere            | Since M5 `REDIS_URL` is optional (unset = Redis off) |
| pino + `x-request-id`                                                  | Production JSON logs; correlate a request across layers    | Pretty transport only in development                 |
| Typed `AppError` hierarchy                                             | Consistent `{ error: { code, message, details? } }` bodies | Call sites must throw domain errors, not raw `Error` |
| Vitest + Fastify `inject`                                              | Fast tests without opening a port                          | -                                                    |

## Project layout

```
migrations/           # forward-only SQL migrations (NNNN_name.sql)
observability/        # Prometheus scrape config, Grafana provisioning + dashboard JSON (Milestone 7)
src/
  app.ts              # composition root: config -> pool -> repositories -> services -> routes
  server.ts           # process entry: loadEnv -> listen -> graceful shutdown
  worker.ts           # worker process entry: loadWorkerEnv -> runtime -> SIGTERM drain
  scheduler.ts        # scheduler process entry: loadSchedulerEnv -> runtime -> SIGTERM release
  cli/                # api-key operator CLI (tenants, keys, tenant rate limits; Milestone 6)
  config/             # zod env schema (app, database subset, worker, scheduler)
  db/                 # pg pool, query helper, pg error mapping, migration runner + CLI
  domain/             # AppError hierarchy (incl. 401 / 403 / 429), job + schedule + stats + queue +
                      # tenant / API key / principal models, limits, lease + dead-letter types,
                      # QueueSignals contract
  lib/                # logger, error mapper, keyset cursor, request fingerprint, cron wrapper,
                      # single-flight, log throttle, API key token format, TTL/LRU cache
  redis/              # ioredis client, keys, coalescer, wake-up publisher + subscriber,
                      # stats cache store (Lua CAS) + invalidator, QueueSignals (Milestone 5),
                      # token-bucket rate limit store (Lua, Milestone 6)
  observability/      # Prometheus registry + metric classes, /metrics listener, series limiter,
                      # queue-depth collector, OpenTelemetry tracing (Milestone 7)
  plugins/            # request-id, central error handler, auth hook (Milestone 6),
                      # observability hooks: spans, trace_id in logs, HTTP histogram (Milestone 7)
  repositories/       # Tenant, ApiKey, Queue, Job, Lease, DeadLetter, Schedule, QueueStats and
                      # Dashboard repositories (all SQL)
  routes/             # /health, /ready, jobs, DLQ, schedules, stats, dashboard, API keys, queue settings
                      # + zod request schemas, rate-limit header mapping
  services/           # HealthService, JobService, DlqService, ScheduleService, StatsService +
                      # StatsCache policy, ApiKeyService, QueueService, RateLimiter, DashboardService
  scheduler/          # Scheduler loop, advisory-lock leader election, tick, misfire planner, runtime
  worker/             # Worker loop, Reaper, handler registry, backoff + retry policy,
                      # runtime (composition root), built-in handlers
tests/                # unit tests (no database needed); tests/worker/, tests/scheduler/
tests/integration/    # real-Postgres (+ real-Redis) tests (each suite migrates its own schema
                      # and uses its own Redis key prefix)
```

## Setup

### Prerequisites

- Node.js 20+
- PostgreSQL 16 (local install or via Docker Compose)
- Redis 7+ (optional; local install or via Docker Compose). Without it workers poll, stats are uncached and enqueues are not rate limited
- Docker + Docker Compose (optional, for the full stack)

### Local

```bash
cp .env.example .env
npm ci

# Start Postgres and Redis (or use your own), then apply migrations
docker compose up -d postgres redis
export $(grep -v '^#' .env | xargs)
npm run migrate:dev

# Every route except /health and /ready needs an API key. Mint the first admin key
# for the "default" tenant (it owns all pre-M6 data); the token is printed once.
export PQ_TOKEN=$(npm run --silent api-key:dev -- create --tenant default --role admin --name local-dev | jq -r .token)
AUTH="authorization: Bearer $PQ_TOKEN"

npm run dev
# -> http://127.0.0.1:3000/ready (public)
curl -s http://localhost:3000/auth/whoami -H "$AUTH"

# In another terminal: run a worker (consumes the built-in echo + demo queues)
npm run worker:dev

# Enqueue a job that fails twice, then succeeds, and watch the worker logs
# (the response carries RateLimit-Limit / -Remaining / -Reset)
curl -si -X POST http://localhost:3000/queues/demo/jobs -H "$AUTH" -H 'content-type: application/json' \
  -d '{"payload":{"durationMs":500,"failUntilAttempt":3},"maxAttempts":5}'

# In a third terminal: run the scheduler (start a second one to see leader election)
npm run scheduler:dev

# Echo a job every minute
curl -s -X POST http://localhost:3000/queues/echo/schedules -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"every-minute","cron":"* * * * *","payload":{"hello":"world"}}'

# Queue stats (x-cache: MISS, then HIT until the queue changes)
curl -si http://localhost:3000/queues/demo/stats -H "$AUTH"

# Cap the demo queue at 2 running jobs fleet-wide and 5 enqueues/s (burst 10)
curl -s -X PUT http://localhost:3000/queues/demo/settings -H "$AUTH" -H 'content-type: application/json' \
  -d '{"maxConcurrency":2,"rateLimit":{"perSec":5,"burst":10}}'

# A producer-only key for a service (can enqueue and read, cannot manage)
curl -s -X POST http://localhost:3000/api-keys -H "$AUTH" -H 'content-type: application/json' \
  -d '{"name":"billing-service","role":"producer"}'
```

Required env vars are documented in `.env.example`. Config validation fails the process immediately if anything required is missing or invalid.

### Docker Compose (migrate + app + worker + scheduler x2 + Postgres 16 + Redis 7 + Prometheus + Grafana + Jaeger)

```bash
docker compose up --build
docker compose up -d --scale worker=3   # more workers; SKIP LOCKED shares the queue
# Bootstrap a tenant and its first admin key with the CLI shipped in the image
docker compose run --rm app node dist/cli/api-key.js create --tenant acme --role admin --name bootstrap --create-tenant
# Observability (Milestone 7)
#   Grafana     http://localhost:3001   (PulseQueue overview dashboard, anonymous read-only; admin/admin to edit)
#   Prometheus  http://localhost:9090   (Status -> Targets: app, worker and scheduler replicas)
#   Jaeger      http://localhost:16686  (service pulsequeue-api: request -> enqueue -> job.execute)
docker compose exec app wget -qO- --header "Authorization: Bearer pulsequeue-dev-metrics-token" http://127.0.0.1:9464/metrics | head
TRACING_ENABLED=false docker compose up -d   # metrics only, no trace export
```

Postgres and Redis become healthy, the one-shot `migrate` service applies the schema and exits, and only then do `app` and `worker` start. Redis is a soft dependency (`required: false`): it is started first so the first wake-up is not missed, but every service runs without it. It runs with persistence off and an LRU memory cap because it only holds derived, disposable state. The worker runs from the same image (`node dist/worker.js`), reports health through a liveness file its loop touches, and gets a 40s `stop_grace_period` so `docker compose stop` drains in-flight jobs (25s budget) before Docker would SIGKILL it. The `scheduler` service runs 2 replicas of `node dist/scheduler.js` from the same image: one leads, the other waits on the advisory lock; `stop_grace_period` (20s) exceeds the 10s tick-finish budget so leadership is released cleanly. The image is a multi-stage Node 20 build that runs as a non-root user and ships the `migrations/` directory. Since Milestone 7 every app container serves metrics on `:9464` inside the compose network only (not published to the host), Prometheus discovers each replica through Docker DNS, and traces go to Jaeger over OTLP/HTTP; none of the app services depend on the observability containers, so stopping them affects nothing but visibility.

### Tests

```bash
npm test                    # unit tests; integration suites are skipped without a database
TEST_DATABASE_URL=postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue npm test
                            # unit + integration against real Postgres
npm run test:unit           # unit only
npm run test:integration    # integration only (needs TEST_DATABASE_URL or DATABASE_URL)
TEST_REDIS_URL=redis://localhost:6379 TEST_DATABASE_URL=... npm test
                            # also runs the Redis suites (wake-ups, stats cache, outages)
# Observability tests need no collector: spans go to an in-memory exporter and
# metrics listeners bind ephemeral ports (METRICS_PORT=0).
```

### Worker configuration

| Variable                         | Default        | Meaning                                                              |
| -------------------------------- | -------------- | -------------------------------------------------------------------- |
| `WORKER_ID`                      | host-pid-rand  | identity in logs, `jobs.locked_by`, `job_attempts.worker_id`         |
| `WORKER_QUEUES`                  | all registered | comma-separated queues to consume (each needs a handler)             |
| `WORKER_CONCURRENCY`             | `10`           | max jobs running at once in this process                             |
| `WORKER_POLL_INTERVAL_MS`        | `1000`         | idle poll interval (+-20% jitter); a full batch re-polls immediately |
| `WORKER_WAKEUP_FALLBACK_POLL_MS` | `30000`        | idle poll while subscribed to wake-ups (>= poll interval)            |
| `WORKER_LEASE_DURATION_MS`       | `30000`        | visibility timeout without a heartbeat                               |
| `WORKER_HEARTBEAT_INTERVAL_MS`   | `10000`        | lease renewal period; must be at most half the lease                 |
| `WORKER_JOB_TIMEOUT_MS`          | `300000`       | per-attempt cap; recorded as `timed_out` and retried                 |
| `WORKER_REAPER_INTERVAL_MS`      | `15000`        | how often each worker reclaims expired leases                        |
| `WORKER_SHUTDOWN_TIMEOUT_MS`     | `25000`        | drain budget on SIGTERM before leases are released                   |
| `WORKER_POISON_LEASE_EXPIRIES`   | `3`            | dead-letter a job as poison after this many lease expiries (1-100)   |
| `WORKER_LIVENESS_FILE`           | unset          | file the loop touches; used by the compose healthcheck               |
| `RETRY_BACKOFF_BASE_MS`          | `1000`         | ceiling of the first retry delay; doubles per attempt                |
| `RETRY_BACKOFF_MAX_MS`           | `300000`       | cap for any retry delay                                              |

The worker also reads `DATABASE_URL`, the `PG_*` pool settings, the Redis settings below, `NODE_ENV` and `LOG_LEVEL` (it does not need `PORT`). With `REDIS_URL` set and `WAKEUP_ENABLED=true` it holds two Redis connections (commands + subscriber). Size the worker's `PG_POOL_MAX` between about `WORKER_CONCURRENCY / 2 + 2` and `WORKER_CONCURRENCY + 2` (the 10 / 10 defaults are fine): heartbeats and outcome writes are short and share the pool with the lease and reaper queries. If the pool is too small, outcome writes wait for a connection and fail after `PG_CONNECTION_TIMEOUT_MS`; the job is then re-run after its lease expires (never lost, but wasted work).

Integration suites create a uniquely named schema per suite (`pq_test_<random>`), run the real migrations into it, and drop it afterwards, so they run in parallel and never touch existing data. CI runs them against a `postgres:16` service container after running the compiled migration CLI twice (to prove it is idempotent) and a worker smoke test (start the compiled worker, send SIGTERM, require exit code 0). The worker suites include a concurrency test (8 workers with their own pools, 400 jobs: each runs exactly once and never concurrently with itself), lease-expiry reclaim with fencing of the crashed worker, retry-then-succeed, exhaust-to-failed, timeouts, heartbeats outliving the lease, drain and drain-timeout release, and the real `src/worker.ts` process draining a job on SIGTERM. The DLQ suites cover the `0003` invariants and backfill, stale-lease fencing of dead-lettering, poison detection, 10 concurrent replays of one job (exactly one wins), replay-vs-purge races, concurrent bulk replays (`SKIP LOCKED`, no overlap), and the HTTP API end to end with a real worker. The scheduling suites cover promotion and "never leased before `run_at`", the schedules API, two schedulers in one process (exactly one leads, failover on graceful stop and on a terminated session, concurrent ticks never duplicate, a failed tick rolls back), each misfire policy after a simulated 5 h outage, and two real `src/scheduler.ts` processes handing over leadership on SIGTERM. The Redis suites (run when `TEST_REDIS_URL` is set; CI runs a `redis:7-alpine` service) use a unique key prefix per test and cover channel isolation, coalescing, dirty-flag TTLs, the refresh claim and CAS, subscriber recovery through a TCP proxy that cuts and restores connections, the stats API cache states and stampede protection across two API instances, `/ready` degraded during an outage, pickup latency with a 20s fallback poll, push vs poll query counts, delayed jobs and retries on time in push mode, and the real `src/worker.ts` picking up a job via a wake-up. The Milestone 6 suites cover migrations `0005`-`0007` (backfill into `default`, constraints, hash length), the key repository and the CLI, the key API (only the digest at rest, rotation with a grace period, revoke, roles, cross-tenant `404`s), tenant isolation of jobs, DLQ, schedules and stats, the Lua token bucket (Redis clock refill, all-or-nothing, 200 concurrent takes over 4 connections admit exactly the burst, outage fails fast), the rate-limited API across two API instances with fail-open and fail-closed outages, and concurrency caps (parallel lease calls from many connections never over-lease, overlapping capped queues never deadlock, three real workers never exceed the cap).

### Scheduler configuration

| Variable                             | Default       | Meaning                                                                         |
| ------------------------------------ | ------------- | ------------------------------------------------------------------------------- |
| `SCHEDULER_ID`                       | host-pid-rand | identity in logs                                                                |
| `SCHEDULER_TICK_INTERVAL_MS`         | `1000`        | leader loop period (100-60000, +-10% jitter); a full batch re-ticks immediately |
| `SCHEDULER_LEADER_RETRY_INTERVAL_MS` | `5000`        | how often a standby retries the lock (+-20% jitter) = worst-case failover delay |
| `SCHEDULER_BATCH_SIZE`               | `100`         | max due schedules claimed per tick transaction (1-1000)                         |
| `SCHEDULER_MISFIRE_GRACE_MS`         | `60000`       | an occurrence older than this is a misfire; must be >= 2x the tick interval     |
| `SCHEDULER_MAX_CATCH_UP`             | `10`          | `catch_up` policy: max missed occurrences replayed per schedule (0-1000)        |
| `SCHEDULER_SHUTDOWN_TIMEOUT_MS`      | `10000`       | budget to finish the in-flight tick on SIGTERM before releasing leadership      |
| `SCHEDULER_LIVENESS_FILE`            | unset         | file the loop touches (leader and standby); used by the compose healthcheck     |

The scheduler also reads `DATABASE_URL`, `PG_*`, the Redis settings (it publishes wake-ups for the queues it fired into), `NODE_ENV` and `LOG_LEVEL`. It uses one dedicated connection (the lock session; `PG_STATEMENT_TIMEOUT_MS` applies to it) and no pool. Point it at Postgres directly or through PgBouncer in **session** mode: advisory locks are session state.

### Redis configuration (API, worker and scheduler)

| Variable                         | Default | Meaning                                                                               |
| -------------------------------- | ------- | ------------------------------------------------------------------------------------- |
| `REDIS_URL`                      | unset   | unset = every Redis feature off (pure Postgres, as in M4)                             |
| `REDIS_KEY_PREFIX`               | `pq`    | namespace for keys and channels, so environments can share one Redis                  |
| `REDIS_CONNECT_TIMEOUT_MS`       | `2000`  | connect + handshake budget per attempt                                                |
| `REDIS_COMMAND_TIMEOUT_MS`       | `500`   | per-command budget; slower = treated as unavailable (fallback), never a slow request  |
| `REDIS_RECONNECT_BASE_MS`        | `100`   | reconnect backoff: `random(cap/2, cap)`, `cap = min(MAX, BASE x 2^(n-1))`             |
| `REDIS_RECONNECT_MAX_MS`         | `5000`  | reconnect backoff cap                                                                 |
| `WAKEUP_ENABLED`                 | `true`  | publish and subscribe to wake-ups                                                     |
| `WAKEUP_COALESCE_MS`             | `50`    | per-queue publish window (about 2 publishes per queue per window at most)             |
| `STATS_CACHE_ENABLED`            | `true`  | cache `GET /queues/:name/stats` (API) and write dirty flags (all processes)           |
| `STATS_CACHE_TTL_MS`             | `10000` | hard expiry of entries and dirty flags = staleness bound if invalidations are lost    |
| `STATS_CACHE_MIN_REFRESH_MS`     | `1000`  | API only: a dirty entry younger than this is served as `STALE`; must be below the TTL |
| `STATS_INVALIDATION_COALESCE_MS` | `100`   | per-queue dirty-flag write window                                                     |

Keep `STATS_CACHE_TTL_MS` and `REDIS_KEY_PREFIX` identical across the API, workers and the scheduler of one environment: they all write the same dirty flags.

### Auth and rate limit configuration (API)

| Variable                           | Default | Meaning                                                                                                                         |
| ---------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `AUTH_CACHE_TTL_MS`                | `5000`  | how long an instance trusts a verified key (0-60000) = how long a revoked key may still work on other instances; `0` = no cache |
| `AUTH_CACHE_MAX_ENTRIES`           | `10000` | verified keys cached per instance (LRU)                                                                                         |
| `RATE_LIMIT_ENABLED`               | `true`  | enqueue rate limiting (needs `REDIS_URL`; without Redis it is off)                                                              |
| `RATE_LIMIT_TENANT_PER_SEC`        | `100`   | default tenant refill rate; per-tenant override via `api-key tenant-rate-limit`                                                 |
| `RATE_LIMIT_TENANT_BURST`          | `200`   | default tenant bucket size (>= 1)                                                                                               |
| `RATE_LIMIT_FAIL_MODE`             | `open`  | Redis unavailable: `open` = allow (logged), `closed` = `503` + `Retry-After: 1` (requires `REDIS_URL`)                          |
| `RATE_LIMIT_SETTINGS_CACHE_TTL_MS` | `5000`  | per-instance cache of queue rate limits; a `PUT` applies at once on the serving instance                                        |

Authentication cannot be switched off. Workers and the scheduler do not authenticate (they talk to Postgres directly) and ignore these settings; concurrency caps need no configuration beyond `PUT /queues/:name/settings`.

### Observability configuration (API, worker and scheduler)

| Variable                         | Default     | Meaning                                                                                                 |
| -------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------- |
| `METRICS_PORT`                   | unset       | port of this process's `/metrics` listener; unset = no listener, `0` = ephemeral (tests)                |
| `METRICS_HOST`                   | `127.0.0.1` | listener address; a non-loopback address in production requires `METRICS_TOKEN`                         |
| `METRICS_TOKEN`                  | unset       | bearer token scrapers must send (>= 16 chars, constant-time compare)                                    |
| `METRICS_MAX_QUEUE_SERIES`       | `1000`      | distinct tenant/queue label pairs per process; further pairs are recorded as `__other__`                |
| `METRICS_QUEUE_DEPTH_ENABLED`    | `true`      | API only: export `pulsequeue_queue_depth` (read from Postgres on scrape)                                |
| `METRICS_QUEUE_DEPTH_CACHE_MS`   | `5000`      | API only: reuse a depth snapshot this long (0 = every scrape)                                           |
| `METRICS_QUEUE_DEPTH_TIMEOUT_MS` | `2000`      | API only: `statement_timeout` of the depth query; on timeout the previous snapshot is served            |
| `DASHBOARD_LATENCY_MAX_SAMPLES`  | `100000`    | API only: most recent attempts used per latency percentile computation                                  |
| `TRACING_ENABLED`                | `false`     | OpenTelemetry tracing (API and worker)                                                                  |
| `OTEL_EXPORTER_OTLP_ENDPOINT`    | unset       | OTLP/HTTP base URL, required when tracing is on (spans go to `<url>/v1/traces`)                         |
| `OTEL_SERVICE_NAME`              | per role    | defaults to `pulsequeue-api` / `pulsequeue-worker`                                                      |
| `OTEL_TRACES_SAMPLER_RATIO`      | `1`         | head sampling for new traces (0-1); parent-based, so a sampled inbound `traceparent` is always followed |

### Scripts

| Script                         | Purpose                                                                                   |
| ------------------------------ | ----------------------------------------------------------------------------------------- |
| `npm run dev`                  | tsx watch server                                                                          |
| `npm run build`                | compile to `dist/`                                                                        |
| `npm start`                    | run compiled server                                                                       |
| `npm run worker`               | run compiled worker                                                                       |
| `npm run worker:dev`           | tsx watch worker                                                                          |
| `npm run scheduler`            | run compiled scheduler                                                                    |
| `npm run scheduler:dev`        | tsx watch scheduler                                                                       |
| `npm run migrate`              | apply migrations (compiled, used in Docker/CI)                                            |
| `npm run migrate:dev`          | apply migrations from source via tsx                                                      |
| `npm run api-key -- <cmd>`     | operator CLI (compiled): `create`, `list`, `revoke`, `tenant-create`, `tenant-rate-limit` |
| `npm run api-key:dev -- <cmd>` | the same CLI from source via tsx                                                          |
| `npm run lint`                 | ESLint                                                                                    |
| `npm run typecheck`            | `tsc --noEmit`                                                                            |
| `npm test`                     | Vitest (unit + integration when a DB is set)                                              |
| `npm run test:unit`            | unit tests only                                                                           |
| `npm run test:integration`     | integration tests only                                                                    |
| `npm run format`               | Prettier write                                                                            |
| `npm run format:check`         | Prettier check                                                                            |

CI runs format check, lint, typecheck, build, migrations, worker (must subscribe to wake-ups) and scheduler SIGTERM smoke tests, an API smoke test (bootstrap an admin key with the compiled CLI; public probes, `401` without a key, `201, 201, 429 + Retry-After` at burst 2), a metrics smoke test (API and worker `/metrics` listeners: `401` without the token, expected series with it, `404` on the API port) and tests on Node 20 against `postgres:16-alpine` and `redis:7-alpine` service containers (see `.github/workflows/ci.yml`).

## API

All responses are JSON. Every response echoes `x-request-id` (inbound header or a generated UUID).

### Authentication, roles and tenancy (Milestone 6)

Every endpoint except `GET /health` and `GET /ready` requires an API key:

```http
Authorization: Bearer pq_ab12cd34ef56_kq3V...43 base64url chars...
```

A key belongs to one tenant and has one role. All resources (queues, jobs, schedules, DLQ entries, stats, API keys) are scoped to the key's tenant: queue names are per tenant, and another tenant's job / schedule / key ids answer `404`. The examples below omit the header for brevity.

| Permission | Endpoints                                                                                                                                                                                                               | `admin` | `producer` | `reader` |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------- | -------- |
| `read`     | `GET /jobs/:id`, `GET /queues/:name/jobs`, `GET /queues/:name/stats`, `GET /queues/:name/dlq`, `GET /dlq/:jobId`, `GET /queues/:name/schedules`, `GET /schedules/:id`, `GET /queues/:name/settings`, `GET /auth/whoami` | yes     | yes        | yes      |
| `enqueue`  | `POST /queues/:name/jobs`                                                                                                                                                                                               | yes     | yes        | no       |
| `manage`   | DLQ replay / purge, schedule create / pause / resume / delete, `PUT /queues/:name/settings`, all `/api-keys` endpoints                                                                                                  | yes     | no         | no       |

| Situation                                                                                                      | Response                                                                                                                                                                            |
| -------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| No `Authorization` header, not `Bearer`, malformed token, unknown prefix, wrong secret, revoked or expired key | `401 UNAUTHORIZED` + `WWW-Authenticate: Bearer realm="pulsequeue"` (the message says what is wrong with the header, never whether a well-formed key exists, was revoked or expired) |
| Valid key, role lacks the permission                                                                           | `403 FORBIDDEN` (`details.required`, `details.role`)                                                                                                                                |

The first admin key of a tenant comes from the CLI (`npm run api-key -- create --tenant <id> --role admin --name <name> --create-tenant`, or `node dist/cli/api-key.js ...` in the container); later keys can be managed over HTTP. `npm run api-key -- tenant-rate-limit --tenant <id> --per-sec <n> --burst <n>` (or `--clear`) overrides a tenant's enqueue budget.

### Rate limits (Milestone 6)

`POST /queues/:name/jobs` spends one token from the tenant's bucket and, if the queue has a rate limit, from the queue's bucket (both or neither). Other endpoints are not rate limited. When limiting ran, successful enqueues (including `200` idempotent replays) carry:

| Header                | Meaning                                                                          |
| --------------------- | -------------------------------------------------------------------------------- |
| `RateLimit-Limit`     | size (burst) of the bucket closest to refusing                                   |
| `RateLimit-Remaining` | tokens left in that bucket                                                       |
| `RateLimit-Reset`     | seconds until that bucket is full again                                          |
| `Retry-After`         | on `429` only: whole seconds (>= 1) until one token is available in every bucket |

```http
HTTP/1.1 429 Too Many Requests
retry-after: 1
ratelimit-limit: 200
ratelimit-remaining: 0
ratelimit-reset: 2
```

```json
{
  "error": {
    "code": "RATE_LIMITED",
    "message": "Enqueue rate limit exceeded (tenant); retry in 1s",
    "details": { "scope": "tenant", "retryAfterMs": 10 }
  }
}
```

`details.scope` is `tenant` or `queue`. Requests rejected by validation (`400`, `413`) do not spend tokens. Without `REDIS_URL` (or with `RATE_LIMIT_ENABLED=false`) there are no rate-limit headers and no `429`s. If Redis is unreachable, `RATE_LIMIT_FAIL_MODE=open` lets enqueues through without headers, `closed` answers `503 SERVICE_UNAVAILABLE` (`details.reason: "rate_limit_unavailable"`, `Retry-After: 1`).

### `POST /queues/:name/jobs`

Enqueue a job. The queue is created on first use. Queue names: 1-64 chars of `a-z 0-9 . _ -`, starting with a letter or digit.

| Body field    | Type     | Default | Rules                                                       |
| ------------- | -------- | ------- | ----------------------------------------------------------- |
| `payload`     | any JSON | -       | required; at most `JOB_MAX_PAYLOAD_BYTES` (default 256 KiB) |
| `priority`    | integer  | `0`     | `0`-`100`, higher runs first                                |
| `delayMs`     | integer  | `0`     | `0`-`2592000000` (30 days); `> 0` makes the job `scheduled` |
| `runAt`       | string   | -       | ISO-8601 with offset, at most 30 days ahead; past = now     |
| `maxAttempts` | integer  | `3`     | `1`-`25`                                                    |

`delayMs` and `runAt` are mutually exclusive (`400`, `details.fieldErrors.runAt`). Either way a delayed job is never leased before its `run_at` (database clock). Unknown fields are rejected. Optional header `Idempotency-Key`: 1-255 printable ASCII characters, scoped to the queue.

```bash
curl -i -X POST http://localhost:3000/queues/emails/jobs \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: welcome-email-user-42' \
  -d '{"payload":{"to":"user42@example.com","template":"welcome"},"priority":10,"maxAttempts":5}'
```

```http
HTTP/1.1 201 Created
location: /jobs/222efd5a-939e-47e4-ad55-410a8bd63c6a
```

```json
{
  "id": "222efd5a-939e-47e4-ad55-410a8bd63c6a",
  "queue": "emails",
  "status": "queued",
  "priority": 10,
  "payload": { "to": "user42@example.com", "template": "welcome" },
  "attempts": 0,
  "maxAttempts": 5,
  "runAt": "2026-10-20T04:30:34.201Z",
  "idempotencyKey": "welcome-email-user-42",
  "lastError": null,
  "finishedAt": null,
  "createdAt": "2026-10-20T04:30:34.201Z",
  "updatedAt": "2026-10-20T04:30:34.201Z"
}
```

Idempotency semantics:

| Situation                                                | Response                                                                |
| -------------------------------------------------------- | ----------------------------------------------------------------------- |
| New key (or no key)                                      | `201 Created`, new job                                                  |
| Same key, equivalent body (key order / defaults ignored) | `200 OK`, `Idempotent-Replayed: true`, the original job (current state) |
| Same key, different body                                 | `422 IDEMPOTENCY_KEY_REUSED`                                            |
| Same key on a different queue                            | independent job (`201`)                                                 |

Errors: `400 VALIDATION_ERROR` / `BAD_REQUEST` (bad input or malformed JSON), `401 UNAUTHORIZED`, `403 FORBIDDEN` (`reader` key), `413 PAYLOAD_TOO_LARGE`, `422 IDEMPOTENCY_KEY_REUSED`, `429 RATE_LIMITED` (tenant or queue budget spent; see [Rate limits](#rate-limits-milestone-6)), `503 SERVICE_UNAVAILABLE` (database unavailable or timed out, or rate limiting unavailable in `closed` mode).

### `GET /jobs/:id`

```bash
curl http://localhost:3000/jobs/222efd5a-939e-47e4-ad55-410a8bd63c6a
```

Returns the job object above. `400` for a non-UUID id, `404 NOT_FOUND` for an unknown job.

### `GET /queues/:name/jobs`

Newest first, keyset-paginated.

| Query    | Default | Rules                                                                  |
| -------- | ------- | ---------------------------------------------------------------------- |
| `status` | -       | one of `queued`, `scheduled`, `running`, `succeeded`, `failed`, `dead` |
| `limit`  | `50`    | `1`-`200`                                                              |
| `cursor` | -       | opaque value from a previous page's `nextCursor`                       |

```bash
curl 'http://localhost:3000/queues/emails/jobs?status=queued&limit=2'
```

```json
{
  "data": [
    { "id": "8c0d...", "queue": "emails", "status": "queued", "...": "..." },
    { "id": "222e...", "queue": "emails", "status": "queued", "...": "..." }
  ],
  "nextCursor": "eyJ2IjoxLCJ0IjoiMjAyNi0xMC0yMFQwNDozMDozNC4yMDExMjNaIiwiaWQiOiIyMjJlLi4uIn0"
}
```

```bash
curl "http://localhost:3000/queues/emails/jobs?status=queued&limit=2&cursor=$NEXT_CURSOR"
```

`nextCursor` is `null` on the last page. Unknown queue -> `404` (reads never create queues). Malformed cursor, out-of-range limit or unknown parameters -> `400`.

### `GET /queues/:name/stats`

Point-in-time counts and backlog ages for one queue (Milestone 5). `404 NOT_FOUND` for a queue that was never used (not cached). Responses carry `cache-control: no-store` and `x-cache`:

| `x-cache` | Meaning                                                                                                          |
| --------- | ---------------------------------------------------------------------------------------------------------------- |
| `HIT`     | cached snapshot, the queue has not changed since it was taken                                                    |
| `STALE`   | the queue changed, but the snapshot is younger than `STATS_CACHE_MIN_REFRESH_MS` or another request refreshes it |
| `MISS`    | computed from Postgres now and cached                                                                            |
| `BYPASS`  | cache disabled, `REDIS_URL` unset or Redis unavailable: computed from Postgres                                   |

```bash
curl -si http://localhost:3000/queues/emails/stats
# HTTP/1.1 200 OK
# x-cache: HIT
```

```json
{
  "queue": "emails",
  "counts": { "queued": 120, "scheduled": 8, "running": 10, "succeeded": 5230, "dead": 3 },
  "total": 5371,
  "oldestQueuedAt": "2026-10-24T04:59:58.120Z",
  "oldestQueuedAgeMs": 1880,
  "nextScheduledAt": "2026-10-24T05:00:30.000Z",
  "oldestRunningSince": "2026-10-24T04:59:41.500Z",
  "computedAt": "2026-10-24T05:00:00.000Z"
}
```

All numbers come from one statement (one snapshot, database clock). `oldestQueuedAgeMs` is relative to `computedAt`, so a cached body stays self-consistent; `computedAt` tells you how old the snapshot is. Staleness is at most `STATS_CACHE_MIN_REFRESH_MS` after a change, or `STATS_CACHE_TTL_MS` if the invalidation was lost.

### Dead-letter queue

A job is dead-lettered (status `dead`) when it exhausts `maxAttempts`, throws a `NonRetryableError` / `InvalidPayloadError` (or an error with `retryable: false`), or its lease expires `WORKER_POISON_LEASE_EXPIRIES` times. DLQ entries are job objects with an extra `deadLetter` block:

```json
{
  "id": "5f1c...",
  "queue": "billing",
  "status": "dead",
  "attempts": 3,
  "maxAttempts": 3,
  "lastError": "Error: payment provider down",
  "finishedAt": "2026-10-22T04:31:49.773Z",
  "...": "other job fields",
  "deadLetter": {
    "reason": "attempts_exhausted",
    "deadLetteredAt": "2026-10-22T04:31:49.773Z",
    "attempts": 3,
    "lastError": "Error: payment provider down",
    "queue": "billing",
    "replayCount": 0,
    "lastReplayedAt": null
  }
}
```

`reason` is one of `attempts_exhausted`, `non_retryable`, `invalid_payload`, `poison_lease_expired`.

#### `GET /queues/:name/dlq`

Most recently dead-lettered first, keyset-paginated like `GET /queues/:name/jobs`.

| Query    | Default | Rules                                            |
| -------- | ------- | ------------------------------------------------ |
| `reason` | -       | one of the reasons above                         |
| `limit`  | `50`    | `1`-`200`                                        |
| `cursor` | -       | opaque value from a previous page's `nextCursor` |

```bash
curl 'http://localhost:3000/queues/billing/dlq?reason=attempts_exhausted&limit=20'
# -> { "data": [ <DLQ entry>, ... ], "nextCursor": "..." | null }
```

Unknown queue -> `404`; bad `reason`, cursor or limit -> `400`.

#### `GET /dlq/:jobId`

One DLQ entry plus its full attempt history (oldest first). `404` if the job does not exist or is not dead-lettered (`details.status` tells you where it is), `400` for a non-UUID id.

```json
{
  "...": "DLQ entry fields",
  "attemptHistory": [
    {
      "attemptNumber": 1,
      "status": "failed",
      "workerId": "worker-1",
      "error": "Error: payment provider down",
      "startedAt": "2026-10-22T04:31:40.100Z",
      "finishedAt": "2026-10-22T04:31:40.180Z"
    }
  ]
}
```

#### `POST /queues/:name/dlq/:jobId/replay`

Move one job back to `queued` (or `scheduled` with `delayMs`) with a fresh retry budget. The job keeps its id, payload, idempotency key and attempt history. Body optional: `{}` or `{"delayMs": 60000}` (`0`-`2592000000`).

```bash
curl -i -X POST http://localhost:3000/queues/billing/dlq/5f1c.../replay
# 200 OK, Location: /jobs/5f1c..., body = the job (status queued, attempts 0)
```

| Situation                                                 | Response                                                |
| --------------------------------------------------------- | ------------------------------------------------------- |
| Job is dead in this queue                                 | `200`, job in its new state; logs `event: dlq.replayed` |
| Job is not dead (already replayed, running, succeeded...) | `409 JOB_NOT_DEAD_LETTERED`, `details.status`           |
| Concurrent replays of the same job                        | exactly one `200`, every other `409`                    |
| Unknown job, unknown queue or the job is in another queue | `404 NOT_FOUND`                                         |

#### `POST /queues/:name/dlq/replay`

Bulk replay, oldest dead-lettered first. Body optional: `{"limit": 100, "reason": "attempts_exhausted", "delayMs": 0}` (`limit` `1`-`1000`, default `100`). Concurrent bulk replays never replay the same job twice (`SKIP LOCKED`).

```json
{ "replayed": 2, "jobIds": ["5f1c...", "9ab0..."] }
```

#### `DELETE /queues/:name/dlq/:jobId`

Permanently delete one dead-lettered job and its attempts. `204` on success, `409 JOB_NOT_DEAD_LETTERED` if it is not dead, `404` if unknown or in another queue. Purging frees its idempotency key.

#### `DELETE /queues/:name/dlq?olderThan=<ISO-8601>&limit=<n>`

Bulk purge of jobs dead-lettered before `olderThan` (required, with a zone, e.g. `2026-10-01T00:00:00Z`; pass the current time to purge everything). `limit` `1`-`10000`, default `1000`; repeat while `purged == limit`.

```bash
curl -X DELETE 'http://localhost:3000/queues/billing/dlq?olderThan=2026-10-15T00:00:00Z'
# -> { "purged": 42 }
```

### Schedules

A schedule enqueues a job into its queue at every occurrence of a cron expression, evaluated in an IANA timezone. Requires a running scheduler process (`npm run scheduler`). Schedule object:

```json
{
  "id": "0b7e...",
  "queue": "reports",
  "name": "daily-digest",
  "cron": "0 9 * * 1-5",
  "timezone": "Asia/Kolkata",
  "payload": { "report": "digest" },
  "priority": 0,
  "maxAttempts": 3,
  "misfirePolicy": "fire_once",
  "enabled": true,
  "nextRunAt": "2026-10-26T03:30:00.000Z",
  "lastRunAt": "2026-10-23T03:30:00.000Z",
  "lastJobId": "8d21...",
  "fireCount": 12,
  "lastError": null,
  "createdAt": "2026-10-10T08:00:00.000Z",
  "updatedAt": "2026-10-23T03:30:00.412Z"
}
```

#### `POST /queues/:name/schedules`

| Body field      | Type     | Default     | Rules                                                                                                           |
| --------------- | -------- | ----------- | --------------------------------------------------------------------------------------------------------------- |
| `name`          | string   | -           | required, unique per queue; 1-128 chars `A-Z a-z 0-9 . _ : -`, starting alphanumeric                            |
| `cron`          | string   | -           | required; 5 fields (`min hour dom month dow`), 6 with a leading seconds field of `0`, or `@hourly`-style macros |
| `timezone`      | string   | `UTC`       | IANA zone, e.g. `Europe/Berlin`                                                                                 |
| `payload`       | any JSON | -           | required; job payload template, at most `JOB_MAX_PAYLOAD_BYTES`                                                 |
| `priority`      | integer  | `0`         | `0`-`100`                                                                                                       |
| `maxAttempts`   | integer  | `3`         | `1`-`25`                                                                                                        |
| `misfirePolicy` | string   | `fire_once` | `skip`, `fire_once` or `catch_up` (see below)                                                                   |
| `enabled`       | boolean  | `true`      | `false` creates the schedule paused                                                                             |

```bash
curl -i -X POST http://localhost:3000/queues/reports/schedules -H 'content-type: application/json' \
  -d '{"name":"daily-digest","cron":"0 9 * * 1-5","timezone":"Asia/Kolkata","payload":{"report":"digest"}}'
# 201 Created, Location: /schedules/0b7e..., body = the schedule
```

`400 VALIDATION_ERROR` for an invalid expression, zone or field (for example `details.fieldErrors.cron: ["Invalid cron expression: expected 5 fields ..."]`, an expression that never matches, or seconds other than `0`), `409 CONFLICT` if the name exists in the queue, `413 PAYLOAD_TOO_LARGE`.

Misfire policies decide what happens to occurrences older than `SCHEDULER_MISFIRE_GRACE_MS` (default 60s) when the scheduler gets to them, e.g. after every scheduler was down. Occurrences within the window always fire.

| Policy      | After a 5 h outage of an hourly schedule                                 |
| ----------- | ------------------------------------------------------------------------ |
| `skip`      | nothing fires for the missed hours; the schedule continues with the next |
| `fire_once` | one job (for the latest missed hour)                                     |
| `catch_up`  | one job per missed hour, at most `SCHEDULER_MAX_CATCH_UP`, newest kept   |

#### `GET /queues/:name/schedules`

Newest first, keyset-paginated. Query: `enabled` (`true` / `false`), `limit` (`1`-`200`, default `50`), `cursor`. Returns `{ "data": [...], "nextCursor": "..." | null }`; unknown queue -> `404`.

#### `GET /schedules/:id`

The schedule; `404` if unknown, `400` for a non-UUID id.

#### `POST /schedules/:id/pause` and `POST /schedules/:id/resume`

`200` with the schedule. Pause sets `enabled: false` and `nextRunAt: null`; resume computes the next occurrence after now (a paused period is not replayed). Both are idempotent.

#### `DELETE /schedules/:id`

`204`. Jobs already enqueued by the schedule are not touched.

### Queue settings (Milestone 6)

#### `GET /queues/:name/settings`

Any role. `404` if the tenant has no such queue.

```json
{
  "queue": "emails",
  "maxConcurrency": 5,
  "rateLimit": { "perSec": 50, "burst": 100 },
  "updatedAt": "2026-10-27T05:12:09.481Z"
}
```

#### `PUT /queues/:name/settings`

Admin only. Full replacement: both fields are required and `null` clears a limit; the queue is created if it does not exist yet, so limits can be set before the first job. Returns `200` with the settings.

| Body field       | Type            | Rules                                                                                                     |
| ---------------- | --------------- | --------------------------------------------------------------------------------------------------------- |
| `maxConcurrency` | integer or null | `1`-`10000`: max jobs of this queue running at once across all workers                                    |
| `rateLimit`      | object or null  | `{ "perSec": 1-100000, "burst": 1-1000000 }`: enqueue token bucket for this queue, on top of the tenant's |

```bash
curl -X PUT http://localhost:3000/queues/emails/settings -H 'content-type: application/json' \
  -d '{"maxConcurrency":5,"rateLimit":{"perSec":50,"burst":100}}'
```

`maxConcurrency` applies to the next lease. Lowering it below the number of running jobs does not stop them; no new job of the queue is leased until enough have finished. The queue rate limit applies at once on the instance that served the `PUT` and within `RATE_LIMIT_SETTINGS_CACHE_TTL_MS` on the others. Logged as `queue.settings_updated`.

### API keys (Milestone 6)

All `/api-keys` endpoints are admin only and act on the caller's tenant. A key resource never contains the secret:

```json
{
  "id": "7c0e3f5e-2b8f-4a59-9a0a-1f9a7d3b52c1",
  "tenantId": "acme",
  "name": "billing-service",
  "prefix": "ab12cd34ef56",
  "role": "producer",
  "createdAt": "2026-10-27T05:10:00.000Z",
  "expiresAt": null,
  "lastUsedAt": "2026-10-27T05:11:42.120Z",
  "revokedAt": null,
  "rotatedFrom": null
}
```

#### `POST /api-keys`

Body `{ "name": "billing-service", "role": "admin" | "producer" | "reader", "expiresAt"?: ISO-8601 in the future }`. Returns `201`, `Location: /api-keys/:id`, `Cache-Control: no-store` and the key plus `"token": "pq_ab12cd34ef56_..."`. **The token is shown only in this response**; only its SHA-256 digest is stored.

#### `GET /api-keys?limit=<1-200, default 50>&cursor=<opaque>&includeRevoked=<true|false>`

Keyset page `{ "data": [...], "nextCursor": "..." | null }`, newest first; revoked keys are hidden unless `includeRevoked=true`.

#### `GET /api-keys/:id`

One key (`404` if it is not in the caller's tenant).

#### `DELETE /api-keys/:id`

Revoke. Idempotent (`200` with the key, `revokedAt` set; revoking twice keeps the first timestamp). Takes effect at once on the serving instance and within `AUTH_CACHE_TTL_MS` (5s) on others. A key may revoke itself.

#### `POST /api-keys/:id/rotate`

Body (optional) `{ "gracePeriodSeconds": 0-604800 (default 0), "expiresAt"?: ISO-8601 }`. Issues a new key with the same name and role (`rotatedFrom` = old id; `201` with the new token, like create) and revokes the old one now, or lets it expire at the end of the grace period so clients can switch over without downtime. `409 CONFLICT` if the key is already revoked or expired.

### `GET /auth/whoami`

Any role. Returns the calling key's identity, handy when debugging client configuration:

```json
{ "tenantId": "acme", "keyId": "7c0e3f5e-...", "keyPrefix": "ab12cd34ef56", "role": "producer" }
```

### Dashboard (Milestone 7)

Any role (`reader` is enough); everything is scoped to the caller's tenant, and another tenant's queue is `404`. Windows and buckets are durations `<int><s|m|h|d>` (`30s`, `5m`, `1h`, `7d`); invalid values are `400 VALIDATION_ERROR`. All numbers are computed from Postgres per request.

| Parameter | Endpoints                | Default | Bounds                                                                     |
| --------- | ------------------------ | ------- | -------------------------------------------------------------------------- |
| `window`  | throughput, failure-rate | `1h`    | 10s-7d                                                                     |
| `window`  | latency                  | `1h`    | 10s-1d (percentiles sort their samples)                                    |
| `bucket`  | throughput               | `1m`    | 10s-1d, `<= window`, window must be a whole multiple, at most 1440 buckets |
| `limit`   | `GET /dashboard/queues`  | `100`   | 1-500                                                                      |
| `after`   | `GET /dashboard/queues`  |         | queue name from `nextAfter` (keyset by name)                               |

#### `GET /dashboard/queues?limit=<n>&after=<name>`

```json
{
  "data": [
    {
      "queue": "emails",
      "counts": { "queued": 120, "scheduled": 4, "running": 10, "dead": 2 },
      "maxConcurrency": 10,
      "oldestQueuedAt": "2026-10-28T04:00:00.000Z",
      "oldestQueuedAgeMs": 42000
    }
  ],
  "nextAfter": null,
  "computedAt": "2026-10-28T04:00:42.000Z"
}
```

#### `GET /dashboard/queues/:name/throughput?window=1h&bucket=1m`

Buckets are aligned to the bucket size (epoch-based, like Prometheus steps), cover `[from, to)`, and are zero-filled. `enqueued` counts jobs created, `succeeded` / `failed` / `timedOut` count finished attempts, `deadLettered` counts jobs moved to the DLQ.

```json
{
  "queue": "emails",
  "from": "2026-10-28T03:01:00.000Z",
  "to": "2026-10-28T04:01:00.000Z",
  "bucketMs": 60000,
  "buckets": [
    {
      "start": "2026-10-28T03:01:00.000Z",
      "enqueued": 31,
      "succeeded": 29,
      "failed": 2,
      "timedOut": 0,
      "deadLettered": 0
    }
  ],
  "totals": { "enqueued": 1800, "succeeded": 1750, "failed": 41, "timedOut": 3, "deadLettered": 5 }
}
```

#### `GET /dashboard/queues/:name/latency?window=1h`

Execution time (`finished_at - started_at`) and queue wait (`started_at - ready_at`, the time a runnable job waited for a worker, on the database clock) over finished attempts (`succeeded`, `failed`, `timed_out`; released attempts are excluded). Percentiles are exact (`percentile_cont`) over at most `sampleLimit` most recent attempts; `truncated: true` means older ones were left out. With no attempts every value is `null` and `samples` is `0`.

```json
{
  "queue": "emails",
  "from": "2026-10-28T03:00:42.000Z",
  "to": "2026-10-28T04:00:42.000Z",
  "unit": "ms",
  "executionMs": { "samples": 1794, "p50": 182.4, "p95": 940.1, "p99": 2210.7, "max": 4980.2 },
  "queueWaitMs": { "samples": 1794, "p50": 3.1, "p95": 48.9, "p99": 310.5, "max": 1204.4 },
  "truncated": false,
  "sampleLimit": 100000
}
```

#### `GET /dashboard/queues/:name/failure-rate?window=1h`

`attemptFailureRate` = (failed + timed out) / finished attempts (how flaky the handler is); `jobFailureRate` = dead-lettered / (succeeded + dead-lettered) jobs in the window (how often retries do not save a job). `null` when the denominator is zero.

```json
{
  "queue": "emails",
  "from": "2026-10-28T03:00:42.000Z",
  "to": "2026-10-28T04:00:42.000Z",
  "attempts": { "succeeded": 1750, "failed": 41, "timedOut": 3, "total": 1794 },
  "attemptFailureRate": 0.0245,
  "jobs": { "succeeded": 1750, "deadLettered": 5 },
  "jobFailureRate": 0.0028
}
```

### Metrics (Milestone 7)

Served by each process on its own listener (`METRICS_PORT`), **not** on the API port (`GET /metrics` there is `404`). `GET` / `HEAD /metrics` only, Prometheus text format; with `METRICS_TOKEN` set a scrape needs `Authorization: Bearer <token>` (`401` + `WWW-Authenticate: Bearer` otherwise).

```bash
curl -H "Authorization: Bearer $METRICS_TOKEN" http://127.0.0.1:9464/metrics
# pulsequeue_http_request_duration_seconds_bucket{le="0.05",method="POST",route="/queues/:name/jobs",status_code="201",role="api"} 41
# pulsequeue_jobs_enqueued_total{tenant="acme",queue="emails",result="created",role="api"} 41
# pulsequeue_queue_depth{tenant="acme",queue="emails",status="queued",role="api"} 120
```

Useful PromQL: `histogram_quantile(0.95, sum by (le) (rate(pulsequeue_job_execution_seconds_bucket{queue="emails"}[5m])))` (p95 execution), `sum(rate(pulsequeue_jobs_dead_lettered_total[5m])) > 0` (DLQ inflow alert), `max by (queue) (pulsequeue_queue_depth{status="queued"})` (backlog; `max` because every API replica exports the same database value), `sum(pulsequeue_scheduler_is_leader) != 1` (leadership alert).

### `GET /health`

Liveness probe. Does **not** check Postgres or Redis.

```json
{ "status": "ok", "service": "pulsequeue", "timestamp": "2026-10-20T04:00:00.000Z" }
```

### `GET /ready`

Readiness probe. Each check has a `READINESS_TIMEOUT_MS` budget; failure reasons are logged, not returned.

| `status`    | HTTP  | When                                                                               |
| ----------- | ----- | ---------------------------------------------------------------------------------- |
| `ready`     | `200` | every check passes                                                                 |
| `degraded`  | `200` | only optional checks fail (Redis): still serving, workers poll, stats are uncached |
| `not_ready` | `503` | a required check fails (Postgres)                                                  |

```json
{
  "status": "degraded",
  "checks": { "app": "ok", "postgres": "ok", "redis": "fail" },
  "timestamp": "2026-10-24T04:00:00.000Z"
}
```

Without `REDIS_URL` there is no `redis` check. Alert on `degraded` (or on `redis.*` log events); do not let the load balancer act on it.

### Errors

Codes: `VALIDATION_ERROR` / `BAD_REQUEST` (400), `UNAUTHORIZED` (401, with `WWW-Authenticate: Bearer`), `FORBIDDEN` (403), `NOT_FOUND` (404, also for other tenants' resources), `CONFLICT` / `JOB_NOT_DEAD_LETTERED` (409), `PAYLOAD_TOO_LARGE` (413), `IDEMPOTENCY_KEY_REUSED` (422), `RATE_LIMITED` (429, with `Retry-After` and `RateLimit-*`), `SERVICE_UNAVAILABLE` (503), `INTERNAL_ERROR` (500). All errors use a consistent shape:

```json
{
  "error": {
    "code": "IDEMPOTENCY_KEY_REUSED",
    "message": "Idempotency-Key was already used with a different request",
    "details": { "idempotencyKey": "welcome-email-user-42", "jobId": "222efd5a-..." }
  }
}
```

## Scaling notes

- **API tier** is stateless: run N replicas behind a load balancer. Each holds a pool of `PG_POOL_MAX` connections, so keep `replicas x PG_POOL_MAX` well under Postgres `max_connections`; beyond a few dozen replicas put PgBouncer (transaction pooling) in front. Nothing here relies on session state, so transaction pooling is compatible.
- **Enqueue cost** is one indexed `SELECT` for the queue (cacheable in-process later) plus one `INSERT` touching the PK, the lease index (partial) and two listing indexes; idempotency adds one partial unique index entry only when a key is sent.
- **Workers** scale horizontally (`--scale worker=N` or more pods): `SKIP LOCKED` partitions the ready set without coordination, and each lease call asks only for free slots. Total in-flight jobs = replicas x `WORKER_CONCURRENCY`; size it to what downstream dependencies tolerate, not just to CPU, since most handlers are I/O bound.
- **Leasing cost**: `jobs_lease_idx` contains only runnable rows, so it stays small even with millions of finished jobs, and a lease of n jobs is one statement. With one queue per worker the scan is an ordered index walk; a worker consuming many queues pays a small sort over the candidates. Since M4 the lease index holds only `queued` rows and due delayed jobs are promoted in bounded batches first, so far-future high-priority jobs are never walked by the lease scan.
- **Heartbeat load** is `replicas x WORKER_CONCURRENCY / heartbeat interval` single-row updates per second (100 slots at 10s = 10 writes/s), which is why the heartbeat default is a third of the lease rather than something like 1s.
- **Connections**: every worker replica holds up to `PG_POOL_MAX` connections; add replicas x pool size of API and workers together against `max_connections`, and use PgBouncer in transaction mode beyond a few dozen processes (no session state is used: leases are rows, not advisory locks).
- **Crash recovery time** is bounded by `WORKER_LEASE_DURATION_MS + WORKER_REAPER_INTERVAL_MS`; graceful shutdowns release immediately.
- **Table growth**: `jobs` is append-heavy and its status updates are not HOT (status is indexed); heartbeats are also not HOT because `lease_expires_at` is indexed, which is one more reason not to heartbeat too often. Dead-lettered jobs are purged explicitly (`DELETE /queues/:name/dlq?olderThan=`, e.g. from a daily cron); plan retention for succeeded jobs (M8), per-table autovacuum tuning, and time-based partitioning of `jobs` / `job_attempts` once volume demands it.
- **DLQ** reads, bulk replays and purges walk the partial `jobs_dlq_idx`, so their cost is proportional to the DLQ, not the table; bulk operations are bounded per call so their row locks and cascades stay short. A mass replay after an outage (say 100k jobs) is best done in `limit=1000` batches with a `delayMs` spread so downstreams are not hit all at once.
- **Scheduler**: one leader does all firing, so run 2 replicas for availability, not throughput. A tick costs one indexed claim plus one insert and one update per due schedule; at the default batch of 100 with immediate re-ticks a single leader fires thousands of schedules per second, far beyond typical needs (100k schedules at one per minute are about 1.7k fires/s spread over the minute). Each replica holds one connection. Failover time is about `SCHEDULER_LEADER_RETRY_INTERVAL_MS` on graceful exit or a killed process; on a network partition it is bounded by TCP keepalive and Postgres' `tcp_keepalives_*` / `idle_session_timeout` settings. If firing ever outgrows one leader, shard schedules by a hash of the id across several lock keys.
- **Pagination** cost is O(limit) at any depth thanks to keyset cursors; there are no `OFFSET` scans or total counts.
- **Redis (M5)** removes idle polling: with wake-ups an idle worker issues one lease query per `WORKER_WAKEUP_FALLBACK_POLL_MS` (30s) instead of one per second, so 100 idle workers drop from ~100 to ~3 empty lease queries per second (plus one tiny `min(run_at)` query per idle cycle), while pickup latency falls from up to a poll interval to a few milliseconds. Publishes are coalesced per queue, so Redis load does not grow with enqueue rate. Each worker holds two Redis connections, the API and scheduler one each. Postgres stays the lease authority, so losing Redis degrades latency, not correctness.
- **Stats** cost one aggregate query per queue per `STATS_CACHE_MIN_REFRESH_MS` at most, cluster-wide, however many dashboards poll; the count is O(jobs in the queue), so very large queues need retention or counter tables (M8). Redis memory is tiny (one small hash and flag per queue) and every key has a TTL.
- **Redis topology**: a single primary is enough; on Redis Cluster the stats keys are hash-tagged per queue, and pub/sub is broadcast cluster-wide (sharded pub/sub would be the next step at very large scale). A failover that loses writes only loses hints.
- **Auth (M6)** costs one unique-index probe on `api_keys.prefix` plus one SHA-256 per uncached request; with the 5s per-instance cache a busy producer costs about one lookup per key per instance every 5s. `last_used_at` writes are throttled to one per key per minute and run off the request path, so auth adds no write load proportional to traffic.
- **Rate limiting (M6)** is one `EVALSHA` per enqueue (two keys, O(1)), on a separate concern from Postgres; a single Redis primary handles well over 100k such calls per second. Bucket keys carry a `{t:<tenant>}` hash tag, so on Redis Cluster both buckets of one request live in one slot and tenants spread across shards. Idle buckets expire as soon as they would be full, so memory is proportional to _active_ tenants and queues. The queue limit lookup is cached per instance for 5s.
- **Concurrency caps (M6)** cost nothing for uncapped queues (same single lease statement as M5). For capped queues the lease runs in a short transaction that locks the capped queue rows and counts running jobs through the partial `jobs_running_queue_idx` (in-flight rows only), so leasers of the _same_ capped queue serialize for a few milliseconds; with many workers on one capped queue, keep `WORKER_CONCURRENCY` modest and let wake-ups, not tight polling, drive leases. Enqueues are never blocked by these locks (`FOR NO KEY UPDATE` does not conflict with the foreign-key `KEY SHARE` lock).
- **Multi-tenancy** is row-level in shared tables; per-tenant rate limits and per-queue caps are the noisy-neighbour controls. Very large tenants could later be moved to their own database or partition by `tenant_id`.
- **Dashboard API (M7)** queries are index-only range scans of `job_attempts_queue_finished_idx` bounded by the window, so their cost is proportional to the attempts of one queue in that window, not to table size; latency percentiles sort at most `DASHBOARD_LATENCY_MAX_SAMPLES` rows. Throughput over 7 days of a very busy queue is the most expensive call; at that volume, minute rollup tables maintained by the worker (or a materialized view refreshed per minute) would make every dashboard query O(buckets).
- **Metrics (M7)**: a scrape is in-memory except `queue_depth`, which costs at most one grouped count per `METRICS_QUEUE_DEPTH_CACHE_MS` per API instance (scrapers beyond the first hit the cache). Series count is roughly (queues x statuses x histogram buckets) per process, capped by `METRICS_MAX_QUEUE_SERIES`; with 1000 queues the worker's histograms dominate (about 1000 x 2 histograms x 15 series), well within one Prometheus. Beyond that, drop the per-queue label on histograms or use recording rules.
- **Tracing (M7)** adds two spans per enqueue and one per attempt, batched and exported off the hot path; at high volume set `OTEL_TRACES_SAMPLER_RATIO` (for example `0.05`) or tail-sample in an OTel collector. Writing `traceparent` is 55 bytes per job.
- Capacity numbers and k6 results land in Milestone 8.

## License

MIT (to be confirmed when the public repo is finalized).
