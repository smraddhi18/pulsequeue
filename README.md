# PulseQueue

A production-style distributed job queue for Node.js services, built in TypeScript with Fastify, PostgreSQL and Redis. Producers enqueue jobs over an HTTP API, workers lease and execute them with retries and exponential backoff, failed jobs go to a dead-letter queue, and a dashboard API reports queue health.

This project is being built milestone by milestone. See [ROADMAP.md](ROADMAP.md) for the plan and progress. **Current state: Milestone 2 (workers, leasing, heartbeats, retries with backoff, graceful shutdown) is done.**

## Architecture

```mermaid
flowchart LR
    P[Producer services] -->|"POST /queues/:name/jobs<br/>Idempotency-Key"| API
    O[Operators / dashboards] -->|"GET /jobs/:id<br/>GET /queues/:name/jobs"| API
    LB[Load balancer / k8s probes] -->|"/health, /ready"| API

    subgraph API[Fastify API]
        direction TB
        RT[Routes<br/>zod validation, thin] --> SV[Services<br/>JobService, HealthService]
        SV --> RP[Repositories<br/>all SQL lives here]
    end

    RP -->|pg Pool| PG[(PostgreSQL 16<br/>queues, jobs, job_attempts)]
    MG[migrate job<br/>schema_migrations + advisory lock] -->|DDL| PG

    subgraph WP[Worker pool - N processes x WORKER_CONCURRENCY]
        direction TB
        WL[Worker loop<br/>lease free slots, poll with jitter] --> HR["Handler registry<br/>queue name to handler"]
        WL --> HB[Heartbeats<br/>extend lease while running]
        RE[Reaper<br/>reclaims expired leases]
        WL --> LR[LeaseRepository<br/>writes fenced on lease_token]
        HB --> LR
        RE --> LR
    end

    LR -->|"lease: FOR UPDATE SKIP LOCKED<br/>complete / retry with backoff / fail / release"| PG
    WP -.->|onJobFailed hook - M3| DLQ[Dead-letter queue - M3]
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
- Exhausted or non-retryable jobs end `failed` and fire an `onJobFailed` hook, the seam the dead-letter queue plugs into in Milestone 3
- Graceful shutdown on SIGTERM/SIGINT: stop leasing, drain in-flight jobs up to `WORKER_SHUTDOWN_TIMEOUT_MS`, release the leases of anything unfinished (without burning an attempt), close the pool
- All tuning via zod-validated env (e.g. heartbeat must fit twice into the lease); structured logs carry `workerId`, `jobId`, `attempt` and `attemptId`
- docker-compose `worker` service (same image, liveness-file healthcheck, `stop_grace_period` above the drain budget); CI boots the compiled worker and checks it exits 0 on SIGTERM
- Tests: backoff/jitter, retry policy and the worker loop against an in-memory store; real-Postgres tests for leasing order, fencing, reaping, 8 workers x 400 jobs (each runs exactly once, never concurrently), retry-then-succeed, exhaust-to-failed, timeouts, heartbeats outliving the lease, drain and drain-timeout release, and the real worker process handling SIGTERM

### Planned

- Dead-letter queue and replay (M3)
- Delayed / cron scheduling (M4)
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

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue
    [*] --> scheduled: enqueue with delayMs
    queued --> running: leased (SKIP LOCKED), attempts + 1
    scheduled --> running: leased once run_at is reached
    running --> running: heartbeat extends the lease
    running --> succeeded: handler resolved
    running --> scheduled: threw or timed out, attempts left (run_at = now + backoff)
    running --> failed: NonRetryableError or last attempt failed
    running --> queued: reaper - lease expired (attempt counts)
    running --> queued: shutdown release (attempt refunded)
    succeeded --> [*]
    failed --> [*]
    failed --> dead: dead-letter hook (M3)
```

## Worker execution flow

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker loop
    participant PG as PostgreSQL
    participant H as Handler
    participant R as Reaper (in every worker)

    W->>PG: lease(queues, limit = free slots) in ONE statement
    Note over W,PG: SELECT ... ORDER BY priority DESC, run_at, id<br/>LIMIT n FOR UPDATE SKIP LOCKED, then UPDATE to running<br/>(new lease_token, attempts + 1) and INSERT a job_attempts row
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
    else NonRetryableError or last attempt
        W->>PG: job failed, then the onJobFailed hook runs (DLQ in M3)
    end
    R->>PG: every WORKER_REAPER_INTERVAL_MS reclaim expired leases (SKIP LOCKED)
    Note over R,PG: attempts left: queued again. Last attempt: failed.<br/>The dead worker's attempt is recorded as timed_out.
```

On SIGTERM/SIGINT the worker stops leasing (a lease that races the signal is released untouched), waits up to `WORKER_SHUTDOWN_TIMEOUT_MS` for in-flight jobs, then aborts the rest and **releases** their leases: they become `queued` immediately (no wait for the lease to expire), and the attempt is recorded as `released` and refunded so deploys do not eat retry budget. Then the pool closes and the process exits 0.

### Writing a handler

```ts
import { NonRetryableError } from './worker/errors.js';
import type { HandlerRegistry } from './worker/registry.js';

export function registerEmailHandlers(registry: HandlerRegistry): void {
  registry.register<{ to: string; template: string }>('emails', async ({ job, signal, logger }) => {
    if (!job.payload.to) throw new NonRetryableError('missing recipient'); // no retry
    // Idempotent: the provider de-duplicates on our job id, so a re-run sends nothing twice.
    await mailer.send({ ...job.payload, idempotencyKey: job.id }, { signal });
    logger.info('email sent'); // already tagged with jobId, queue, attempt, workerId
  });
}
```

Register handlers in `src/worker/handlers/index.ts` (it ships `echo` and `demo`). Resolve = success; throw = failed attempt (retried with backoff); `NonRetryableError` = fail now. Pass `signal` to anything that can be cancelled: it fires on timeout, lost lease or shutdown. The `demo` queue exercises the failure paths from the API, e.g. `{"payload":{"durationMs":500,"failUntilAttempt":3},"maxAttempts":5}`.

## Data model

```mermaid
erDiagram
    queues ||--o{ jobs : contains
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

`jobs.attempts` counts attempts that consumed retry budget (a released attempt is refunded); `job_attempts.attempt_number` numbers every execution, released ones included. Until Milestone 3, jobs that exhaust `max_attempts` end in `failed` (with `last_error`) and the worker's `onJobFailed` hook fires; M3 turns that hook into the move to `dead`.

Schema invariants added in [`migrations/0002_worker_leasing.sql`](migrations/0002_worker_leasing.sql): a job is `running` **if and only if** it holds a complete lease (`jobs_running_has_lease`), at most one `running` attempt exists per job (`job_attempts_one_running_per_job_uq`, so a double lease would fail loudly), and an attempt has `finished_at` exactly when it is no longer running.

Indexes (see [`0001`](migrations/0001_create_queues_jobs_attempts.sql) and [`0002`](migrations/0002_worker_leasing.sql)):

| Index                                                                                      | Serves                                                                                                             |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `jobs_lease_idx (queue_id, priority DESC, run_at, id) WHERE status IN (queued, scheduled)` | M2 lease query `... ORDER BY priority DESC, run_at, id LIMIT n FOR UPDATE SKIP LOCKED`; partial, so it stays small |
| `jobs_queue_created_idx (queue_id, created_at DESC, id DESC)`                              | `GET /queues/:name/jobs` keyset pages; also covers the `queue_id` foreign key                                      |
| `jobs_queue_status_created_idx (queue_id, status, created_at DESC, id DESC)`               | the same listing filtered by `status`                                                                              |
| `jobs_queue_idempotency_key_uq (queue_id, idempotency_key) WHERE key IS NOT NULL`          | race-safe idempotency via `ON CONFLICT`; partial, so keyless jobs cost nothing                                     |
| `job_attempts_job_attempt_uq (job_id, attempt_number)`                                     | one row per attempt; attempt history in order                                                                      |
| `jobs_running_lease_expiry_idx (lease_expires_at) WHERE status = running`                  | reaper scan for expired leases; holds only in-flight rows, so it is tiny                                           |
| `job_attempts_one_running_per_job_uq (job_id) WHERE status = running`                      | invariant: never two in-flight attempts of one job                                                                 |

The integration suite runs `EXPLAIN` on the lease, reaper and pagination queries and asserts that they use these indexes.

## Design decisions and trade-offs

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
| **Exhausted jobs end in `failed` + `onJobFailed` hook**, not `dead` yet                            | Milestone scope: DLQ inspect/replay/purge is M3. The hook is the single seam where M3 moves jobs to `dead`, and it already fires from both the worker and the reaper.                                                                                                                                                                                                                                                                                                                                                     | Until M3, `failed` covers both non-retryable and exhausted jobs (the reason is in the logs and `last_error`).                                                                                                                         |
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
  config/             # zod env schema (app, database subset, worker)
  db/                 # pg pool, query helper, pg error mapping, migration runner + CLI
  domain/             # AppError hierarchy, job model, limits, lease types
  lib/                # logger, error mapper, keyset cursor, request fingerprint
  plugins/            # request-id, central error handler
  repositories/       # QueueRepository, JobRepository, LeaseRepository (all SQL)
  routes/             # /health, /ready, jobs API + zod request schemas
  services/           # HealthService, JobService (business rules)
  worker/             # Worker loop, Reaper, handler registry, backoff + retry policy,
                      # runtime (composition root), built-in handlers
tests/                # unit tests (no database needed); tests/worker/ for the worker
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
```

Required env vars are documented in `.env.example`. Config validation fails the process immediately if anything required is missing or invalid.

### Docker Compose (migrate + app + worker + Postgres 16 + Redis 7)

```bash
docker compose up --build
docker compose up -d --scale worker=3   # more workers; SKIP LOCKED shares the queue
```

Postgres and Redis become healthy, the one-shot `migrate` service applies the schema and exits, and only then do `app` and `worker` start. The worker runs from the same image (`node dist/worker.js`), reports health through a liveness file its loop touches, and gets a 40s `stop_grace_period` so `docker compose stop` drains in-flight jobs (25s budget) before Docker would SIGKILL it. The image is a multi-stage Node 20 build that runs as a non-root user and ships the `migrations/` directory.

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
| `WORKER_LIVENESS_FILE`         | unset          | file the loop touches; used by the compose healthcheck               |
| `RETRY_BACKOFF_BASE_MS`        | `1000`         | ceiling of the first retry delay; doubles per attempt                |
| `RETRY_BACKOFF_MAX_MS`         | `300000`       | cap for any retry delay                                              |

The worker also reads `DATABASE_URL`, the `PG_*` pool settings, `NODE_ENV` and `LOG_LEVEL` (it does not need `PORT` or `REDIS_URL`). Size the worker's `PG_POOL_MAX` between about `WORKER_CONCURRENCY / 2 + 2` and `WORKER_CONCURRENCY + 2` (the 10 / 10 defaults are fine): heartbeats and outcome writes are short and share the pool with the lease and reaper queries. If the pool is too small, outcome writes wait for a connection and fail after `PG_CONNECTION_TIMEOUT_MS`; the job is then re-run after its lease expires (never lost, but wasted work).

Integration suites create a uniquely named schema per suite (`pq_test_<random>`), run the real migrations into it, and drop it afterwards, so they run in parallel and never touch existing data. CI runs them against a `postgres:16` service container after running the compiled migration CLI twice (to prove it is idempotent) and a worker smoke test (start the compiled worker, send SIGTERM, require exit code 0). The worker suites include a concurrency test (8 workers with their own pools, 400 jobs: each runs exactly once and never concurrently with itself), lease-expiry reclaim with fencing of the crashed worker, retry-then-succeed, exhaust-to-failed, timeouts, heartbeats outliving the lease, drain and drain-timeout release, and the real `src/worker.ts` process draining a job on SIGTERM.

### Scripts

| Script                     | Purpose                                        |
| -------------------------- | ---------------------------------------------- |
| `npm run dev`              | tsx watch server                               |
| `npm run build`            | compile to `dist/`                             |
| `npm start`                | run compiled server                            |
| `npm run worker`           | run compiled worker                            |
| `npm run worker:dev`       | tsx watch worker                               |
| `npm run migrate`          | apply migrations (compiled, used in Docker/CI) |
| `npm run migrate:dev`      | apply migrations from source via tsx           |
| `npm run lint`             | ESLint                                         |
| `npm run typecheck`        | `tsc --noEmit`                                 |
| `npm test`                 | Vitest (unit + integration when a DB is set)   |
| `npm run test:unit`        | unit tests only                                |
| `npm run test:integration` | integration tests only                         |
| `npm run format`           | Prettier write                                 |
| `npm run format:check`     | Prettier check                                 |

CI runs format check, lint, typecheck, build, migrations, a worker SIGTERM smoke test and tests on Node 20 (see `.github/workflows/ci.yml`).

## API

All responses are JSON. Every response echoes `x-request-id` (inbound header or a generated UUID).

### `POST /queues/:name/jobs`

Enqueue a job. The queue is created on first use. Queue names: 1-64 chars of `a-z 0-9 . _ -`, starting with a letter or digit.

| Body field    | Type     | Default | Rules                                                       |
| ------------- | -------- | ------- | ----------------------------------------------------------- |
| `payload`     | any JSON | -       | required; at most `JOB_MAX_PAYLOAD_BYTES` (default 256 KiB) |
| `priority`    | integer  | `0`     | `0`-`100`, higher runs first                                |
| `delayMs`     | integer  | `0`     | `0`-`2592000000` (30 days); `> 0` makes the job `scheduled` |
| `maxAttempts` | integer  | `3`     | `1`-`25`                                                    |

Unknown fields are rejected. Optional header `Idempotency-Key`: 1-255 printable ASCII characters, scoped to the queue.

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

All errors use a consistent shape:

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
- **Leasing cost**: `jobs_lease_idx` contains only runnable rows, so it stays small even with millions of finished jobs, and a lease of n jobs is one statement. With one queue per worker the scan is an ordered index walk; a worker consuming many queues pays a small sort over the candidates. Many far-future scheduled jobs with high priority are walked past by the `run_at` filter; if that shows up in M8 load tests, a separate "promote scheduled -> queued" step with a `status = queued` index is the fix.
- **Heartbeat load** is `replicas x WORKER_CONCURRENCY / heartbeat interval` single-row updates per second (100 slots at 10s = 10 writes/s), which is why the heartbeat default is a third of the lease rather than something like 1s.
- **Connections**: every worker replica holds up to `PG_POOL_MAX` connections; add replicas x pool size of API and workers together against `max_connections`, and use PgBouncer in transaction mode beyond a few dozen processes (no session state is used: leases are rows, not advisory locks).
- **Crash recovery time** is bounded by `WORKER_LEASE_DURATION_MS + WORKER_REAPER_INTERVAL_MS`; graceful shutdowns release immediately.
- **Table growth**: `jobs` is append-heavy and its status updates are not HOT (status is indexed); heartbeats are also not HOT because `lease_expires_at` is indexed, which is one more reason not to heartbeat too often. Plan retention for finished jobs (M3/M8), per-table autovacuum tuning, and time-based partitioning of `jobs` / `job_attempts` once volume demands it.
- **Pagination** cost is O(limit) at any depth thanks to keyset cursors; there are no `OFFSET` scans or total counts.
- **Redis (M5)** removes idle polling with pub/sub wake-ups; Postgres stays the lease authority, so losing Redis degrades latency, not correctness.
- Capacity numbers and k6 results land in Milestone 8.

## License

MIT (to be confirmed when the public repo is finalized).
