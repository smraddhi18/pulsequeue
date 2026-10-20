# PulseQueue

A production-style distributed job queue for Node.js services, built in TypeScript with Fastify, PostgreSQL and Redis. Producers enqueue jobs over an HTTP API, workers lease and execute them with retries and exponential backoff, failed jobs go to a dead-letter queue, and a dashboard API reports queue health.

This project is being built milestone by milestone. See [ROADMAP.md](ROADMAP.md) for the plan and progress. **Current state: Milestone 1 (job model, enqueue API, idempotency, cursor pagination) is done.**

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

    W[Worker pool - M2] -.->|lease with SKIP LOCKED| PG
    API -.->|notify - M5| R[(Redis 7)]
    R -.->|wake up - M5| W
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

### Planned

- Workers with `SELECT ... FOR UPDATE SKIP LOCKED`, retries, heartbeats (M2)
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
        timestamptz created_at
        timestamptz updated_at
    }
    job_attempts {
        bigint id PK
        uuid job_id FK
        int attempt_number "unique per job"
        job_attempt_status status
        text worker_id
        text error
        timestamptz started_at
        timestamptz finished_at
    }
```

Job lifecycle: `queued` (runnable now) or `scheduled` (waiting for `run_at`) -> `running` (leased, M2) -> `succeeded`, or back to `scheduled` for a retry with backoff, or terminal `failed` (non-retryable) / `dead` (exhausted `max_attempts`, dead-letter in M3).

Indexes (see [`migrations/0001_create_queues_jobs_attempts.sql`](migrations/0001_create_queues_jobs_attempts.sql)):

| Index                                                                                      | Serves                                                                                                             |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `jobs_lease_idx (queue_id, priority DESC, run_at, id) WHERE status IN (queued, scheduled)` | M2 lease query `... ORDER BY priority DESC, run_at, id LIMIT n FOR UPDATE SKIP LOCKED`; partial, so it stays small |
| `jobs_queue_created_idx (queue_id, created_at DESC, id DESC)`                              | `GET /queues/:name/jobs` keyset pages; also covers the `queue_id` foreign key                                      |
| `jobs_queue_status_created_idx (queue_id, status, created_at DESC, id DESC)`               | the same listing filtered by `status`                                                                              |
| `jobs_queue_idempotency_key_uq (queue_id, idempotency_key) WHERE key IS NOT NULL`          | race-safe idempotency via `ON CONFLICT`; partial, so keyless jobs cost nothing                                     |
| `job_attempts_job_attempt_uq (job_id, attempt_number)`                                     | one row per attempt; attempt history in order                                                                      |

The integration suite runs `EXPLAIN` on the lease and pagination queries and asserts that they use these indexes with no `Sort` node.

## Design decisions and trade-offs

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
  config/             # zod env schema (app + database subset)
  db/                 # pg pool, query helper, pg error mapping, migration runner + CLI
  domain/             # AppError hierarchy, job model, limits
  lib/                # logger, error mapper, keyset cursor, request fingerprint
  plugins/            # request-id, central error handler
  repositories/       # QueueRepository, JobRepository (all SQL)
  routes/             # /health, /ready, jobs API + zod request schemas
  services/           # HealthService, JobService (business rules)
tests/                # unit tests (no database needed)
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
```

Required env vars are documented in `.env.example`. Config validation fails the process immediately if anything required is missing or invalid.

### Docker Compose (migrate + app + Postgres 16 + Redis 7)

```bash
docker compose up --build
```

Postgres and Redis become healthy, the one-shot `migrate` service applies the schema and exits, and only then does `app` start. The image is a multi-stage Node 20 build that runs as a non-root user and ships the `migrations/` directory.

### Tests

```bash
npm test                    # unit tests; integration suites are skipped without a database
TEST_DATABASE_URL=postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue npm test
                            # unit + integration against real Postgres
npm run test:unit           # unit only
npm run test:integration    # integration only (needs TEST_DATABASE_URL or DATABASE_URL)
```

Integration suites create a uniquely named schema per suite (`pq_test_<random>`), run the real migrations into it, and drop it afterwards, so they run in parallel and never touch existing data. CI runs them against a `postgres:16` service container after running the compiled migration CLI twice (to prove it is idempotent).

### Scripts

| Script                     | Purpose                                        |
| -------------------------- | ---------------------------------------------- |
| `npm run dev`              | tsx watch server                               |
| `npm run build`            | compile to `dist/`                             |
| `npm start`                | run compiled server                            |
| `npm run migrate`          | apply migrations (compiled, used in Docker/CI) |
| `npm run migrate:dev`      | apply migrations from source via tsx           |
| `npm run lint`             | ESLint                                         |
| `npm run typecheck`        | `tsc --noEmit`                                 |
| `npm test`                 | Vitest (unit + integration when a DB is set)   |
| `npm run test:unit`        | unit tests only                                |
| `npm run test:integration` | integration tests only                         |
| `npm run format`           | Prettier write                                 |
| `npm run format:check`     | Prettier check                                 |

CI runs format check, lint, typecheck, build, migrations and tests on Node 20 (see `.github/workflows/ci.yml`).

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
- **Leasing (M2)**: `jobs_lease_idx` contains only runnable rows, so it stays small even with millions of finished jobs, and `SKIP LOCKED` lets many workers pull from the same queue without blocking on each other.
- **Table growth**: `jobs` is append-heavy and its status updates are not HOT (status is indexed). Plan retention for finished jobs (M3/M8), per-table autovacuum tuning, and time-based partitioning of `jobs` / `job_attempts` once volume demands it.
- **Pagination** cost is O(limit) at any depth thanks to keyset cursors; there are no `OFFSET` scans or total counts.
- **Redis (M5)** removes idle polling with pub/sub wake-ups; Postgres stays the lease authority, so losing Redis degrades latency, not correctness.
- Capacity numbers and k6 results land in Milestone 8.

## License

MIT (to be confirmed when the public repo is finalized).
