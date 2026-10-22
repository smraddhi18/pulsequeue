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

- [ ] Delayed jobs and cron-style recurring jobs
- [ ] Leader election so only one scheduler fires each tick

## Milestone 5: Redis fast path and caching

- [ ] Redis pub/sub wake-ups so idle workers stop polling
- [ ] Cached queue stats with explicit invalidation

## Milestone 6: Auth, multi-tenancy and rate limiting

- [ ] API keys per tenant, hashed at rest
- [ ] Per-tenant and per-queue rate limits (token bucket in Redis)
- [ ] Queue-level concurrency limits

## Milestone 7: Dashboard API and observability

- [ ] Queue depth, throughput, latency and failure-rate endpoints
- [ ] Prometheus metrics, OpenTelemetry tracing across enqueue and execution
- [ ] Grafana dashboard in docker-compose

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
