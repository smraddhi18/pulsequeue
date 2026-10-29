# Load test results (Milestone 8)

Every number on this page was measured on 2026-10-29 (IST) with the scripts in
[`loadtest/`](../loadtest), by **k6** (latency, request rates, status counts)
plus `psql` (drain timing, row counts, table sizes) and `/proc/<pid>/stat`
(CPU per process). Nothing here is extrapolated except the clearly labelled
capacity plan at the end. Raw k6 summaries were exported with
`--summary-export`; to reproduce, see [`loadtest/README.md`](../loadtest/README.md).

## Environment

| Item          | Value                                                                                                                            |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Machine       | one shared Linux VM, 8 vCPU Intel Xeon, 16 GB RAM (about 2.5 GB free: other workloads ran alongside)                             |
| Kernel        | Linux 6.12                                                                                                                       |
| Node.js       | v20.19.2, compiled build (`dist/`), `NODE_ENV=production`, `LOG_LEVEL=info` (JSON logs to a file)                                |
| PostgreSQL    | 16.15, local socket over TCP, stock settings: `max_connections=100`, `shared_buffers=128MB`, `synchronous_commit=on`, `fsync=on` |
| Redis         | 8.0.2, local, no persistence tuning                                                                                              |
| k6            | v2.3.0, on the same machine (it competes for the same CPUs)                                                                      |
| Topology      | 1 API process (`PG_POOL_MAX=10`), 2 worker processes (`WORKER_CONCURRENCY=10`) unless stated, Redis wake-ups + rate limiter on   |
| Workload      | queue `demo` with `{"durationMs":0}`: the handler returns at once, so worker numbers are the queue's own overhead                |
| Rate limiting | on (one Lua `EVALSHA` per enqueue) but the `loadtest` tenant's bucket is raised to 100k/s so it never limits                     |

Everything (k6, API, workers, Postgres, Redis) shares 8 vCPUs, so these are
conservative single-box numbers, not a tuned cluster. Run-to-run variation on
this shared VM is roughly 10-20% on the tail percentiles.

## Results

### 1. Enqueue throughput (`enqueue-throughput.js`, open model)

Constant arrival rate, `POST /queues/demo/jobs`, workers draining at the same time.

| Offered rate | Duration | Achieved  | p50     | p95      | p99      | max    | Errors | Dropped iterations | CPU (1 core = 100%)                         |
| ------------ | -------- | --------- | ------- | -------- | -------- | ------ | ------ | ------------------ | ------------------------------------------- |
| 500/s        | 30 s     | 500/s     | 1.50 ms | 6.74 ms  | 43.3 ms  | 81 ms  | 0      | 0                  | not sampled                                 |
| 1000/s       | 30 s     | 1000/s    | 1.54 ms | 6.54 ms  | 34.9 ms  | 232 ms | 0      | 0                  | not sampled                                 |
| **1000/s**   | **60 s** | **998/s** | 1.59 ms | 8.40 ms  | 71.3 ms  | 411 ms | 0      | 82 (0.14%)         | API 63%, workers 27%, Postgres 125%, k6 28% |
| 1500/s       | 30 s     | 1496/s    | 2.00 ms | 19.5 ms  | 99.6 ms  | 231 ms | 0      | 107 (0.24%)        | not sampled                                 |
| 2000/s       | 60 s     | 1947/s    | 3.31 ms | 178.7 ms | 351.4 ms | 738 ms | 0      | 3132 (2.6%)        | API 90%, workers 53%, Postgres 263%, k6 50% |

- At 1000/s every threshold passed (p95 < 100 ms, p99 < 250 ms, < 1% failed, < 1% dropped).
- At 2000/s the thresholds **failed** (p95 179 ms, 2.6% of arrivals dropped):
  the single API process was at about 90% of one core, so **one Node API
  process saturates at about 1,900-2,000 enqueues/s** on this hardware. There
  were still no errors, only latency.
- All 149k jobs enqueued in the step runs were completed by the two workers
  during the runs (0 left queued afterwards).

### 2. Mixed read/write (`mixed.js`, 1000 req/s for 60 s)

60% enqueue, 25% `GET /jobs/:id`, 10% `GET /queues/demo/jobs?limit=20`, 5% `GET /queues/demo/stats`.

| Endpoint                   | p50     | p95     | p99     | max    |
| -------------------------- | ------- | ------- | ------- | ------ |
| enqueue                    | 1.56 ms | 6.16 ms | 38.5 ms | 206 ms |
| get job                    | 0.68 ms | 2.06 ms | 17.3 ms | 65 ms  |
| list jobs (keyset)         | 1.06 ms | 3.45 ms | 28.0 ms | 70 ms  |
| queue stats (Redis-cached) | 0.53 ms | 2.42 ms | 47.4 ms | 129 ms |

60,003 requests, 999.9/s, 0 failed, 0 dropped, all thresholds passed.
CPU: API 58%, workers 17%, Postgres 90%, k6 29%.

### 3. Idempotent-retry storm (`idempotency-storm.js`)

50 VUs (closed loop) each send the same 200 idempotency keys 3 times
(30,000 requests): round one is a real race of up to 50 simultaneous first
writes per key.

| Metric                              | Result                       |
| ----------------------------------- | ---------------------------- |
| `201 Created` (new jobs)            | **200**, exactly one per key |
| `200` + `Idempotent-Replayed: true` | 29,800                       |
| Same key -> different job id        | 0                            |
| Unexpected status (409 / 5xx)       | 0                            |
| Rows in Postgres for these keys     | 200 (checked with `psql`)    |
| Throughput                          | 2,665 req/s                  |
| Latency p50 / p95 / p99 / max       | 16.3 / 26.4 / 42.3 / 515 ms  |

The run was repeated with a fresh key namespace: again exactly 200 jobs.

### 4. Rate limiting (`rate-limit.js`)

Tenant `ratelimit` limited to 50/s with burst 100, offered 200 enqueues/s for 30 s.

| Metric                                              | Result                                    |
| --------------------------------------------------- | ----------------------------------------- |
| Offered                                             | 6,001                                     |
| Accepted (`201`)                                    | **1,599** (ideal 100 + 50 x 30 = 1,600)   |
| Rejected (`429 RATE_LIMITED`)                       | 4,402                                     |
| Other statuses                                      | 0                                         |
| `429` with `Retry-After >= 1` and `RateLimit-Reset` | 100%                                      |
| `429` latency p50 / p95 / p99                       | 1.03 / 1.52 / 1.84 ms (no Postgres write) |

### 5. Worker drain throughput (`drain-benchmark.sh`)

About 30,000 queued no-op `demo` jobs, then N worker processes started; time
until nothing is queued or running (includes about 1 s of process startup).

| Workers x concurrency | Time   | Jobs/s |
| --------------------- | ------ | ------ |
| 1 x 10                | 69.0 s | 435    |
| 2 x 10                | 32.2 s | 931    |
| 4 x 10                | 16.5 s | 1,814  |
| 8 x 10                | 11.8 s | 2,539  |
| 1 x 40                | 18.6 s | 1,614  |

Each slot cycles in about 23-25 ms per no-op job (10 / 435 and 40 / 1,614):
lease, attempt row, outcome write and the next lease are round trips, so
throughput scales almost linearly with total slots until Postgres and the
shared CPUs saturate (8 x 10 gave 2,539/s, not 3,480/s). CPU was not sampled
during the drains.

### 6. Real Postgres restart under load (manual drill)

`sudo pg_ctlcluster 16 main restart` during `enqueue-throughput.js` at 500/s
for 40 s (API + 2 workers), restart took 2.7 s wall clock (10:13:45-10:13:48 IST).

| Metric                          | Result                                              |
| ------------------------------- | --------------------------------------------------- |
| Requests                        | 19,986                                              |
| `201`                           | 19,791                                              |
| `503 SERVICE_UNAVAILABLE`       | 193 (0.96%), all within 10:13:45-10:13:46 IST       |
| Other errors / timeouts         | 0                                                   |
| `503` latency p50 / p95 / max   | 2.0 / 241 / 251 ms (fast failure, no hung requests) |
| Processes crashed               | 0 (API and both workers kept running)               |
| Jobs left unfinished afterwards | 0 (every job in the database `succeeded`)           |

Pool clients killed by the restart were logged (`Idle Postgres client error`)
and replaced; workers logged `Lease query failed`, backed off and resumed.
`/ready`, polled every 0.5 s, never caught the short outage window.

## Capacity plan (derived from the numbers above)

Assumptions (state them, then check them in production):

- Planning figure per API process: **1,000 enqueues/s** (measured with thresholds
  passing at about 63% of one core; saturation was about 1,950/s).
- Per worker slot: `1 / (handler time + 0.025 s)` jobs/s (25 ms measured queue
  overhead per job, no-op handler).
- Peak = 5 x the daily average. Postgres sized like this VM's stock config.
- Storage: **about 770 bytes per finished job** (jobs row + one attempt, with
  indexes, tiny payload; measured as 236 MB + 155 MB for 535,603 jobs).

Example: **10 million jobs/day, 200 ms average handler**.

| Quantity             | Calculation                                       | Result                                                                                                                                                                                                        |
| -------------------- | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Average / peak rate  | 10M / 86,400 = 116/s; x 5                         | **580 jobs/s at peak**                                                                                                                                                                                        |
| API processes        | 580 / 1,000 = 0.6                                 | 1 (run **2** for availability)                                                                                                                                                                                |
| Worker slots         | 580 / (1 / 0.225 s) = 580 / 4.4                   | 131 slots -> **14 workers x 10** (or 4 x 40)                                                                                                                                                                  |
| Postgres connections | 2 x 10 (API) + 14 x 10 (workers) + 2 (schedulers) | 162 > `max_connections` 100: raise `max_connections` (about 200, memory permitting) or put PgBouncer in transaction mode in front of the API and workers (no session state; the scheduler needs session mode) |
| Storage growth       | 10M x 770 B                                       | **about 7.7 GB/day**: needs a retention job or time partitioning before it fills the disk                                                                                                                     |
| Rate limiter (Redis) | 580 `EVALSHA`/s                                   | negligible for one Redis primary                                                                                                                                                                              |

What changes first as volume grows: one Node API process tops out at about
2,000 enqueues/s (run more replicas; they are stateless), then Postgres write
throughput and WAL (at 2,000 enqueues/s plus completions it used about 2.6
cores here), then table and index bloat from finished jobs (retention,
autovacuum tuning, partitioning).
