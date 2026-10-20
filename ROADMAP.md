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

- [ ] Worker process that leases jobs with SELECT ... FOR UPDATE SKIP LOCKED
- [ ] Visibility timeout and heartbeats so crashed workers release jobs
- [ ] Retries with exponential backoff and jitter
- [ ] Graceful shutdown that drains in-flight jobs
- [ ] Concurrency tests for leasing against real Postgres (many workers, no double-run)

## Milestone 3: Dead-letter queue and replay

- [ ] Move jobs to DLQ after max attempts, keep full attempt history
- [ ] DLQ inspect, replay and purge endpoints
- [ ] Poison-message detection

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
