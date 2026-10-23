# PulseQueue

A production-style distributed job queue for Node.js services, built in TypeScript with Fastify, PostgreSQL and Redis. Producers enqueue jobs over an HTTP API, workers lease and execute them with retries and exponential backoff, failed jobs go to a dead-letter queue, and a dashboard API reports queue health.

This project is being built milestone by milestone. See [ROADMAP.md](ROADMAP.md) for the plan and progress. **Current state: Milestone 4 (delayed jobs, cron schedules and a leader-elected scheduler) is done.**

## Architecture

```mermaid
flowchart LR
    P[Producer services] -->|"POST /queues/:name/jobs<br/>Idempotency-Key"| API
    O[Operators / dashboards] -->|"GET /jobs/:id<br/>GET /queues/:name/jobs"| API
    OP[Operators / runbooks] -->|"GET /queues/:name/dlq, GET /dlq/:jobId<br/>POST .../dlq/:jobId/replay, POST .../dlq/replay<br/>DELETE .../dlq/:jobId, DELETE .../dlq?olderThan="| API
    OPS[Operators / producers] -->|"POST /queues/:name/schedules, GET .../schedules<br/>GET /schedules/:id, POST /schedules/:id/pause, /resume<br/>DELETE /schedules/:id"| API
    LB[Load balancer / k8s probes] -->|"/health, /ready"| API

    subgraph API[Fastify API]
        direction TB
        RT[Routes<br/>zod validation, thin<br/>jobs + dlq + schedules] --> SV[Services<br/>JobService, DlqService,<br/>ScheduleService, HealthService]
        SV --> RP[Repositories<br/>Queue, Job, DeadLetter, Schedule<br/>all SQL lives here]
    end

    subgraph PG["PostgreSQL 16"]
        direction TB
        JT[jobs<br/>queued / scheduled / running / succeeded<br/>jobs_ready_idx: queued only<br/>jobs_scheduled_due_idx: by run_at]
        ST[schedules<br/>cron, timezone, misfire_policy<br/>next_run_at, partial schedules_due_idx]
        DQ[DLQ = jobs WHERE status = dead<br/>dead_reason, dead_lettered_at, replay_count<br/>partial index jobs_dlq_idx]
        AT[job_attempts<br/>full history, kept across DLQ + replays]
        JT -.- AT
        DQ -.- AT
    end

    RP -->|"pg Pool<br/>replay / purge: one guarded statement<br/>WHERE status = dead"| PG
    MG[migrate job<br/>schema_migrations + advisory lock] -->|DDL| PG

    subgraph SP[Scheduler - 2 replicas, one leader]
        direction TB
        SL[Leader<br/>holds pg_try_advisory_lock on its own session<br/>ticks every SCHEDULER_TICK_INTERVAL_MS]
        SS[Standby<br/>retries the lock every<br/>SCHEDULER_LEADER_RETRY_INTERVAL_MS]
        SL --> MP[Misfire planner<br/>skip / fire_once / catch_up<br/>grace window, catch-up limit]
    end

    SL -->|"tick = ONE transaction on the lock session:<br/>claim due schedules FOR UPDATE SKIP LOCKED,<br/>INSERT job ON CONFLICT (key schedule:id:fireTime),<br/>advance next_run_at"| PG
    SS -.->|"lock attempt only"| PG

    subgraph WP[Worker pool - N processes x WORKER_CONCURRENCY]
        direction TB
        WL[Worker loop<br/>promote due scheduled jobs, lease free slots,<br/>poll with jitter] --> HR["Handler registry<br/>queue name to handler"]
        WL --> RPOL[Retry policy<br/>retry with backoff, or dead-letter:<br/>non_retryable / invalid_payload / attempts_exhausted]
        WL --> HB[Heartbeats<br/>extend lease while running]
        RE[Reaper<br/>reclaims expired leases<br/>poison: N lease expiries]
        WL --> LR[LeaseRepository<br/>writes fenced on lease_token]
        HB --> LR
        RE --> LR
    end

    LR -->|"lease: FOR UPDATE SKIP LOCKED<br/>complete / retry / dead-letter / release<br/>(dead-letter is part of the fenced fail/reap statement)"| PG
    WP -->|"onJobFailed hook after the move<br/>(alerts, metrics)"| HK[Notifications]
    API -.->|notify - M5| R[(Redis 7)]
    R -.->|wake up - M5| WP
```

Solid lines exist today; dotted lines are planned milestones.

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

### Planned

- Redis pub/sub wake-ups and cached stats (M5)
- API keys, multi-tenancy, rate limits (M6)
- Dashboard API, Prometheus, OpenTelemetry (M7)
- Load / chaos testing and capacity notes (M8)

## Enqueue request flow

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant R as Route (jobs.ts)
    participant S as JobService
    participant Q as QueueRepository
    participant J as JobRepository
    participant PG as PostgreSQL

    P->>R: POST /queues/emails/jobs (Idempotency-Key: order-42)
    R->>R: zod: queue name, header, body (strict, ranges)
    R->>S: enqueue(input)
    S->>S: apply defaults, check payload byte budget, fingerprint = sha256(canonical JSON)
    S->>Q: ensure("emails")
    Q->>PG: SELECT by name, on miss INSERT ... ON CONFLICT (name) DO NOTHING
    S->>J: insertIdempotent(job, key, fingerprint)
    J->>PG: INSERT ... ON CONFLICT (queue_id, idempotency_key) DO NOTHING RETURNING
    alt new key
        PG-->>J: inserted row
        J-->>S: inserted = true
        S-->>R: created
        R-->>P: 201 Created + Location: /jobs/:id
    else key already used
        J->>PG: SELECT by (queue_id, idempotency_key) in a fresh statement
        J-->>S: existing job, inserted = false
        alt same fingerprint
            S-->>R: replay
            R-->>P: 200 OK + Idempotent-Replayed: true (original job)
        else different fingerprint
            S-->>R: IdempotencyKeyReusedError
            R-->>P: 422 IDEMPOTENCY_KEY_REUSED
        end
    end
```

Routes only parse input and shape output. `JobService` owns the business rules (defaults, payload budget, initial status, replay semantics, pagination). Repositories own every SQL statement and translate driver errors into domain errors. Domain types, limits and errors live under `src/domain/`.

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

On SIGTERM/SIGINT the worker stops leasing (a lease that races the signal is released untouched), waits up to `WORKER_SHUTDOWN_TIMEOUT_MS` for in-flight jobs, then aborts the rest and **releases** their leases: they become `queued` immediately (no wait for the lease to expire), and the attempt is recorded as `released` and refunded so deploys do not eat retry budget. Then the pool closes and the process exits 0.

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

## Data model

```mermaid
erDiagram
    queues ||--o{ jobs : contains
    queues ||--o{ schedules : "recurring jobs"
    jobs ||--o{ job_attempts : "attempt history"

    queues {
        bigint id PK
        text name UK "a-z0-9._- max 64"
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
    }
```

`jobs.attempts` counts attempts that consumed retry budget (a released attempt is refunded); `job_attempts.attempt_number` numbers every execution, released ones included. Since Milestone 3 a terminal failure ends in `dead`: the job stays in `jobs` with `dead_reason`, `dead_lettered_at`, `last_error` and `attempts`, its queue is the original queue, and every attempt stays in `job_attempts`. A replay resets `attempts` (retry budget) and `lease_expiries`, clears the DLQ fields and bumps `replay_count`; `attempt_number` keeps counting, so the history reads "failed, failed, failed, replay, succeeded".

Schema invariants added in [`migrations/0002_worker_leasing.sql`](migrations/0002_worker_leasing.sql): a job is `running` **if and only if** it holds a complete lease (`jobs_running_has_lease`), at most one `running` attempt exists per job (`job_attempts_one_running_per_job_uq`, so a double lease would fail loudly), and an attempt has `finished_at` exactly when it is no longer running.

Invariants added in [`migrations/0003_dead_letter_queue.sql`](migrations/0003_dead_letter_queue.sql): a job is `dead` **if and only if** it has `dead_reason` and `dead_lettered_at` (`jobs_dead_has_metadata`), and `failed` can no longer be written (`jobs_no_legacy_failed`).

Invariants added in [`migrations/0004_scheduling.sql`](migrations/0004_scheduling.sql): a schedule is `enabled` **if and only if** it has a `next_run_at` (`schedules_enabled_has_next_run`), names are unique per queue and match the API's name rule, and priority / attempts / cron / timezone lengths are range-checked.

Indexes (see [`0001`](migrations/0001_create_queues_jobs_attempts.sql), [`0002`](migrations/0002_worker_leasing.sql), [`0003`](migrations/0003_dead_letter_queue.sql) and [`0004`](migrations/0004_scheduling.sql)):

| Index                                                                                      | Serves                                                                                                       |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `jobs_lease_idx (queue_id, priority DESC, run_at, id) WHERE status IN (queued, scheduled)` | M2/M3 lease query; kept only for workers still running during a rolling deploy, dropped in a later migration |
| `jobs_ready_idx (queue_id, priority DESC, run_at, id) WHERE status = queued`               | M4 lease query: only runnable rows, so the scan never walks a future job                                     |
| `jobs_scheduled_due_idx (queue_id, run_at, id) WHERE status = scheduled`                   | promotion: range scan touching only the delayed / backoff rows that are due                                  |
| `schedules_due_idx (next_run_at) WHERE enabled`                                            | scheduler claim, in due order; paused schedules are not in it                                                |
| `schedules_queue_created_idx (queue_id, created_at DESC, id DESC)`                         | `GET /queues/:name/schedules` keyset pages                                                                   |
| `jobs_queue_created_idx (queue_id, created_at DESC, id DESC)`                              | `GET /queues/:name/jobs` keyset pages; also covers the `queue_id` foreign key                                |
| `jobs_queue_status_created_idx (queue_id, status, created_at DESC, id DESC)`               | the same listing filtered by `status`                                                                        |
| `jobs_queue_idempotency_key_uq (queue_id, idempotency_key) WHERE key IS NOT NULL`          | race-safe idempotency via `ON CONFLICT`; partial, so keyless jobs cost nothing                               |
| `job_attempts_job_attempt_uq (job_id, attempt_number)`                                     | one row per attempt; attempt history in order                                                                |
| `jobs_running_lease_expiry_idx (lease_expires_at) WHERE status = running`                  | reaper scan for expired leases; holds only in-flight rows, so it is tiny                                     |
| `job_attempts_one_running_per_job_uq (job_id) WHERE status = running`                      | invariant: never two in-flight attempts of one job                                                           |
| `jobs_dlq_idx (queue_id, dead_lettered_at DESC, id DESC) WHERE status = dead`              | DLQ keyset listing, bulk replay (oldest first) and `olderThan` purges; partial, so it is as small as the DLQ |

The integration suite runs `EXPLAIN` on the lease, reaper and pagination queries and asserts that they use these indexes.

## Design decisions and trade-offs

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
| **Postgres is the source of truth** for jobs (Redis only for wake-ups later)                               | A job queue's core promise is "never lose or double-run a job". Postgres gives durable, transactional writes, constraints that hold for every writer, and `FOR UPDATE SKIP LOCKED` for contention-free leasing. Enqueue can share a transaction with a producer's own data (transactional outbox) in future.                                                                                                    | Lower peak throughput than an in-memory broker, and the jobs table needs vacuum/retention care. Fine up to thousands of jobs/s per primary; see scaling notes.                          |
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
| Zod for env config                                                     | Fail fast before listen; typed `Env` everywhere            | Requires REDIS_URL before Redis is used (M5)         |
| pino + `x-request-id`                                                  | Production JSON logs; correlate a request across layers    | Pretty transport only in development                 |
| Typed `AppError` hierarchy                                             | Consistent `{ error: { code, message, details? } }` bodies | Call sites must throw domain errors, not raw `Error` |
| Vitest + Fastify `inject`                                              | Fast tests without opening a port                          | -                                                    |

## Project layout

```
migrations/           # forward-only SQL migrations (NNNN_name.sql)
src/
  app.ts              # composition root: config -> pool -> repositories -> services -> routes
  server.ts           # process entry: loadEnv -> listen -> graceful shutdown
  worker.ts           # worker process entry: loadWorkerEnv -> runtime -> SIGTERM drain
  scheduler.ts        # scheduler process entry: loadSchedulerEnv -> runtime -> SIGTERM release
  config/             # zod env schema (app, database subset, worker, scheduler)
  db/                 # pg pool, query helper, pg error mapping, migration runner + CLI
  domain/             # AppError hierarchy, job + schedule models, limits, lease + dead-letter types
  lib/                # logger, error mapper, keyset cursor, request fingerprint, cron wrapper
  plugins/            # request-id, central error handler
  repositories/       # Queue, Job, Lease, DeadLetter and Schedule repositories (all SQL)
  routes/             # /health, /ready, jobs, DLQ and schedules APIs + zod request schemas
  services/           # HealthService, JobService, DlqService, ScheduleService (business rules)
  scheduler/          # Scheduler loop, advisory-lock leader election, tick, misfire planner, runtime
  worker/             # Worker loop, Reaper, handler registry, backoff + retry policy,
                      # runtime (composition root), built-in handlers
tests/                # unit tests (no database needed); tests/worker/, tests/scheduler/
tests/integration/    # real-Postgres tests (each suite migrates its own schema)
```

## Setup

### Prerequisites

- Node.js 20+
- PostgreSQL 16 (local install or via Docker Compose)
- Docker + Docker Compose (optional, for the full stack)

### Local

```bash
cp .env.example .env
npm ci

# Start Postgres (or use your own), then apply migrations
docker compose up -d postgres
export $(grep -v '^#' .env | xargs)
npm run migrate:dev

npm run dev
# -> http://127.0.0.1:3000/ready

# In another terminal: run a worker (consumes the built-in echo + demo queues)
npm run worker:dev

# Enqueue a job that fails twice, then succeeds, and watch the worker logs
curl -s -X POST http://localhost:3000/queues/demo/jobs -H 'content-type: application/json' \
  -d '{"payload":{"durationMs":500,"failUntilAttempt":3},"maxAttempts":5}'

# In a third terminal: run the scheduler (start a second one to see leader election)
npm run scheduler:dev

# Echo a job every minute
curl -s -X POST http://localhost:3000/queues/echo/schedules -H 'content-type: application/json' \
  -d '{"name":"every-minute","cron":"* * * * *","payload":{"hello":"world"}}'
```

Required env vars are documented in `.env.example`. Config validation fails the process immediately if anything required is missing or invalid.

### Docker Compose (migrate + app + worker + scheduler x2 + Postgres 16 + Redis 7)

```bash
docker compose up --build
docker compose up -d --scale worker=3   # more workers; SKIP LOCKED shares the queue
```

Postgres and Redis become healthy, the one-shot `migrate` service applies the schema and exits, and only then do `app` and `worker` start. The worker runs from the same image (`node dist/worker.js`), reports health through a liveness file its loop touches, and gets a 40s `stop_grace_period` so `docker compose stop` drains in-flight jobs (25s budget) before Docker would SIGKILL it. The `scheduler` service runs 2 replicas of `node dist/scheduler.js` from the same image: one leads, the other waits on the advisory lock; `stop_grace_period` (20s) exceeds the 10s tick-finish budget so leadership is released cleanly. The image is a multi-stage Node 20 build that runs as a non-root user and ships the `migrations/` directory.

### Tests

```bash
npm test                    # unit tests; integration suites are skipped without a database
TEST_DATABASE_URL=postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue npm test
                            # unit + integration against real Postgres
npm run test:unit           # unit only
npm run test:integration    # integration only (needs TEST_DATABASE_URL or DATABASE_URL)
```

### Worker configuration

| Variable                       | Default        | Meaning                                                              |
| ------------------------------ | -------------- | -------------------------------------------------------------------- |
| `WORKER_ID`                    | host-pid-rand  | identity in logs, `jobs.locked_by`, `job_attempts.worker_id`         |
| `WORKER_QUEUES`                | all registered | comma-separated queues to consume (each needs a handler)             |
| `WORKER_CONCURRENCY`           | `10`           | max jobs running at once in this process                             |
| `WORKER_POLL_INTERVAL_MS`      | `1000`         | idle poll interval (+-20% jitter); a full batch re-polls immediately |
| `WORKER_LEASE_DURATION_MS`     | `30000`        | visibility timeout without a heartbeat                               |
| `WORKER_HEARTBEAT_INTERVAL_MS` | `10000`        | lease renewal period; must be at most half the lease                 |
| `WORKER_JOB_TIMEOUT_MS`        | `300000`       | per-attempt cap; recorded as `timed_out` and retried                 |
| `WORKER_REAPER_INTERVAL_MS`    | `15000`        | how often each worker reclaims expired leases                        |
| `WORKER_SHUTDOWN_TIMEOUT_MS`   | `25000`        | drain budget on SIGTERM before leases are released                   |
| `WORKER_POISON_LEASE_EXPIRIES` | `3`            | dead-letter a job as poison after this many lease expiries (1-100)   |
| `WORKER_LIVENESS_FILE`         | unset          | file the loop touches; used by the compose healthcheck               |
| `RETRY_BACKOFF_BASE_MS`        | `1000`         | ceiling of the first retry delay; doubles per attempt                |
| `RETRY_BACKOFF_MAX_MS`         | `300000`       | cap for any retry delay                                              |

The worker also reads `DATABASE_URL`, the `PG_*` pool settings, `NODE_ENV` and `LOG_LEVEL` (it does not need `PORT` or `REDIS_URL`). Size the worker's `PG_POOL_MAX` between about `WORKER_CONCURRENCY / 2 + 2` and `WORKER_CONCURRENCY + 2` (the 10 / 10 defaults are fine): heartbeats and outcome writes are short and share the pool with the lease and reaper queries. If the pool is too small, outcome writes wait for a connection and fail after `PG_CONNECTION_TIMEOUT_MS`; the job is then re-run after its lease expires (never lost, but wasted work).

Integration suites create a uniquely named schema per suite (`pq_test_<random>`), run the real migrations into it, and drop it afterwards, so they run in parallel and never touch existing data. CI runs them against a `postgres:16` service container after running the compiled migration CLI twice (to prove it is idempotent) and a worker smoke test (start the compiled worker, send SIGTERM, require exit code 0). The worker suites include a concurrency test (8 workers with their own pools, 400 jobs: each runs exactly once and never concurrently with itself), lease-expiry reclaim with fencing of the crashed worker, retry-then-succeed, exhaust-to-failed, timeouts, heartbeats outliving the lease, drain and drain-timeout release, and the real `src/worker.ts` process draining a job on SIGTERM. The DLQ suites cover the `0003` invariants and backfill, stale-lease fencing of dead-lettering, poison detection, 10 concurrent replays of one job (exactly one wins), replay-vs-purge races, concurrent bulk replays (`SKIP LOCKED`, no overlap), and the HTTP API end to end with a real worker. The scheduling suites cover promotion and "never leased before `run_at`", the schedules API, two schedulers in one process (exactly one leads, failover on graceful stop and on a terminated session, concurrent ticks never duplicate, a failed tick rolls back), each misfire policy after a simulated 5 h outage, and two real `src/scheduler.ts` processes handing over leadership on SIGTERM.

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

The scheduler also reads `DATABASE_URL`, `PG_*`, `NODE_ENV` and `LOG_LEVEL`. It uses one dedicated connection (the lock session; `PG_STATEMENT_TIMEOUT_MS` applies to it) and no pool. Point it at Postgres directly or through PgBouncer in **session** mode: advisory locks are session state.

### Scripts

| Script                     | Purpose                                        |
| -------------------------- | ---------------------------------------------- |
| `npm run dev`              | tsx watch server                               |
| `npm run build`            | compile to `dist/`                             |
| `npm start`                | run compiled server                            |
| `npm run worker`           | run compiled worker                            |
| `npm run worker:dev`       | tsx watch worker                               |
| `npm run scheduler`        | run compiled scheduler                         |
| `npm run scheduler:dev`    | tsx watch scheduler                            |
| `npm run migrate`          | apply migrations (compiled, used in Docker/CI) |
| `npm run migrate:dev`      | apply migrations from source via tsx           |
| `npm run lint`             | ESLint                                         |
| `npm run typecheck`        | `tsc --noEmit`                                 |
| `npm test`                 | Vitest (unit + integration when a DB is set)   |
| `npm run test:unit`        | unit tests only                                |
| `npm run test:integration` | integration tests only                         |
| `npm run format`           | Prettier write                                 |
| `npm run format:check`     | Prettier check                                 |

CI runs format check, lint, typecheck, build, migrations, worker and scheduler SIGTERM smoke tests and tests on Node 20 (see `.github/workflows/ci.yml`).

## API

All responses are JSON. Every response echoes `x-request-id` (inbound header or a generated UUID).

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

Errors: `400 VALIDATION_ERROR` / `BAD_REQUEST` (bad input or malformed JSON), `413 PAYLOAD_TOO_LARGE`, `422 IDEMPOTENCY_KEY_REUSED`, `503 SERVICE_UNAVAILABLE` (database unavailable or timed out).

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

### `GET /health`

Liveness probe. Does **not** check Postgres or Redis.

```json
{ "status": "ok", "service": "pulsequeue", "timestamp": "2026-10-20T04:00:00.000Z" }
```

### `GET /ready`

Readiness probe: `200` when every dependency check passes, `503` otherwise. Each check has a `READINESS_TIMEOUT_MS` budget; failure reasons are logged, not returned.

```json
{
  "status": "ready",
  "checks": { "app": "ok", "postgres": "ok" },
  "timestamp": "2026-10-20T04:00:00.000Z"
}
```

### Errors

Codes: `VALIDATION_ERROR` / `BAD_REQUEST` (400), `NOT_FOUND` (404), `CONFLICT` / `JOB_NOT_DEAD_LETTERED` (409), `PAYLOAD_TOO_LARGE` (413), `IDEMPOTENCY_KEY_REUSED` (422), `SERVICE_UNAVAILABLE` (503), `INTERNAL_ERROR` (500). All errors use a consistent shape:

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
- **Redis (M5)** removes idle polling with pub/sub wake-ups; Postgres stays the lease authority, so losing Redis degrades latency, not correctness.
- Capacity numbers and k6 results land in Milestone 8.

## License

MIT (to be confirmed when the public repo is finalized).
