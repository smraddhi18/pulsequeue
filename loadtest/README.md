# Load tests (k6)

[k6](https://grafana.com/docs/k6/) scripts for PulseQueue's HTTP API. Each script has
pass/fail **thresholds** (k6 exits non-zero when one is crossed), takes its
target and key from the environment, and works against any deployment.
Measured results and the machine they came from are in
[`docs/load-test-results.md`](../docs/load-test-results.md).

| Script                  | Model                        | What it checks                                                                                                         |
| ----------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `enqueue-throughput.js` | open (constant arrival rate) | enqueue latency at a fixed rate: p95 / p99, < 1% errors, < 1% dropped arrivals                                         |
| `mixed.js`              | open                         | 60% enqueue, 25% get job, 10% list, 5% stats, with per-endpoint latency thresholds                                     |
| `idempotency-storm.js`  | closed (per-VU iterations)   | many VUs retrying the same keys: exactly one `201` per key, the rest `200` + `Idempotent-Replayed`, one job id per key |
| `rate-limit.js`         | open, above the tenant limit | accepted = burst + rate x duration (+-10%), the rest `429` with `Retry-After` / `RateLimit-*`, no other statuses       |
| `smoke.js`              | closed, 5 VUs, 10 s          | every main endpoint answers, zero failures (used in CI)                                                                |

Why an open model for throughput: a closed loop of VUs slows down when the
server does, which hides overload. With a constant arrival rate, a slow server
shows up as latency and as `dropped_iterations`.

## Common settings

| Variable   | Default                 | Meaning                                                                  |
| ---------- | ----------------------- | ------------------------------------------------------------------------ |
| `BASE_URL` | `http://127.0.0.1:3000` | API origin                                                               |
| `API_KEY`  | none (required)         | bearer token; `setup()` checks it with `/auth/whoami` and `/ready` first |
| `QUEUE`    | `demo`                  | target queue (`demo` is the built-in no-op handler)                      |

Per script: `RATE`, `DURATION`, `VUS`, `P95_MS`, `P99_MS` (throughput, mixed);
`KEYS`, `VUS`, `ROUNDS`, `RUN_ID` (storm); `RATE`, `DURATION_S`,
`LIMIT_PER_SEC`, `LIMIT_BURST`, `TOLERANCE` (rate limit). See the header of each file.

## Against any deployment

```bash
k6 run -e BASE_URL=https://queue.example.com -e API_KEY=pq_... -e RATE=200 loadtest/enqueue-throughput.js
```

Use a dedicated tenant: the scripts create real jobs. For throughput runs raise
that tenant's rate limit (`npm run api-key -- tenant-rate-limit --tenant <t> --per-sec 100000 --burst 1000000`)
or you are measuring the limiter.

## Local stack in one command

`run-local.sh` migrates the database, creates a `loadtest` tenant (limit raised)
and a `ratelimit` tenant (50/s, burst 100), starts the compiled API on port 3100
plus `WORKERS` workers, waits for `/ready`, runs the script and stops everything.

```bash
npm run build
export DATABASE_URL=postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue REDIS_URL=redis://localhost:6379
bash loadtest/run-local.sh enqueue-throughput.js -e RATE=1000 -e DURATION=60s
bash loadtest/run-local.sh mixed.js -e RATE=1000
bash loadtest/run-local.sh idempotency-storm.js -e KEYS=200 -e VUS=50
bash loadtest/run-local.sh rate-limit.js -e RATE=200 -e DURATION_S=30 -e LIMIT_PER_SEC=50 -e LIMIT_BURST=100
WORKERS=4 bash loadtest/drain-benchmark.sh      # worker throughput: fill with no workers, then time the drain
```

The scripts are invoked with `bash` because the executable bit is not guaranteed after every checkout or upload.
Logs and the k6 summary JSON go to `$OUT_DIR` (default `/tmp/pulsequeue-loadtest`).
`drain-benchmark.sh` also needs `psql`. Use a throwaway database: runs leave
hundreds of thousands of finished jobs behind.

## Reading results

- Run k6 on a different machine from the API when you can; on one box k6
  competes for the same CPUs (the published numbers say when it did).
- Look at p99 and `dropped_iterations` before the average.
- Step the rate up (500, 1000, 1500, ...) and note where thresholds start
  failing; that knee, not the maximum achieved rate, is the capacity figure.
