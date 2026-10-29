#!/usr/bin/env bash
# Worker throughput: fill the demo queue with no workers running, then start
# WORKERS worker processes and time how long they take to drain it.
# Measures dequeue + complete (lease, attempt rows, outcome) with a no-op
# handler, i.e. the queue's own overhead per job.
#
#   DATABASE_URL=... REDIS_URL=... WORKERS=2 bash loadtest/drain-benchmark.sh
#
# Env: JOBS (default 30000, enqueued by k6 at 1000/s), WORKERS (default 2),
#      WORKER_CONCURRENCY (default 10), PSQL (default psql), plus run-local.sh's.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
: "${DATABASE_URL:?DATABASE_URL is required}"
: "${REDIS_URL:?REDIS_URL is required}"
JOBS=${JOBS:-30000}
WORKERS=${WORKERS:-2}
PSQL=${PSQL:-psql}
OUT_DIR=${OUT_DIR:-/tmp/pulsequeue-loadtest}
export NODE_ENV=production LOG_LEVEL=${LOG_LEVEL:-info} DATABASE_URL REDIS_URL

backlog() {
  "$PSQL" "$DATABASE_URL" -Atc \
    "SELECT count(*) FROM jobs j JOIN queues q ON q.id = j.queue_id
     WHERE q.name = 'demo' AND j.status IN ('queued', 'scheduled', 'running')"
}
[ "$(backlog)" = 0 ] || { echo "demo queue is not empty; drain it first" >&2; exit 1; }

# 1. Fill: API only, no workers.
WORKERS=0 OUT_DIR="$OUT_DIR" bash "$HERE/run-local.sh" enqueue-throughput.js \
  -e RATE=1000 -e DURATION="$((JOBS / 1000))s" >/dev/null || true
queued=$(backlog)
echo "queued: $queued demo jobs"

# 2. Drain with N workers and time it.
cd "$ROOT"
pids=()
trap 'for p in "${pids[@]}"; do kill -TERM "$p" 2>/dev/null || true; done; wait 2>/dev/null || true' EXIT
start=$(date +%s.%N)
for i in $(seq 1 "$WORKERS"); do
  WORKER_ID="drain-$i" WORKER_QUEUES=demo node dist/worker.js >"$OUT_DIR/drain-worker-$i.log" 2>&1 &
  pids+=($!)
done
while [ "$(backlog)" != 0 ]; do sleep 0.2; done
end=$(date +%s.%N)
awk -v n="$queued" -v s="$start" -v e="$end" -v w="$WORKERS" -v c="${WORKER_CONCURRENCY:-10}" \
  'BEGIN { t = e - s; printf "drained %d jobs with %d workers x concurrency %d in %.1fs = %.0f jobs/s (includes ~1s process startup)\n", n, w, c, t, n / t }'
