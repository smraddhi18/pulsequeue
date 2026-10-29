#!/usr/bin/env bash
# Run one k6 script against a freshly started local stack (built API +
# workers on this machine, Postgres and Redis already running), then stop it.
#
#   DATABASE_URL=postgres://... REDIS_URL=redis://... bash loadtest/run-local.sh enqueue-throughput.js -e RATE=500
#
# Env:
#   DATABASE_URL, REDIS_URL  required (the database is migrated first)
#   PORT                     API port (default 3100)
#   WORKERS                  worker processes to start (default 2; 0 = API only)
#   WORKER_CONCURRENCY       per worker (default 10)
#   LOG_LEVEL                API/worker log level (default info, logs go to $OUT_DIR)
#   OUT_DIR                  where logs and the k6 summary JSON go (default /tmp/pulsequeue-loadtest)
#   K6                       k6 binary (default: k6 on PATH)
#
# Two tenants are prepared: "loadtest" (limit raised to 100k/s so throughput runs
# measure the API, not the limiter) and "ratelimit" (50/s, burst 100) whose key
# is passed to rate-limit.js. Everything else gets the loadtest tenant's key.
set -euo pipefail

SCRIPT=${1:?usage: run-local.sh <script.js> [k6 args...]}
shift
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
: "${DATABASE_URL:?DATABASE_URL is required}"
: "${REDIS_URL:?REDIS_URL is required}"
PORT=${PORT:-3100}
WORKERS=${WORKERS:-2}
OUT_DIR=${OUT_DIR:-/tmp/pulsequeue-loadtest}
K6=${K6:-k6}
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)
export NODE_ENV=production LOG_LEVEL=${LOG_LEVEL:-info} DATABASE_URL REDIS_URL

cd "$ROOT"
[ -f dist/server.js ] || npm run build >/dev/null
node dist/db/migrate-cli.js >/dev/null

key() { # tenant role name
  node dist/cli/api-key.js create --tenant "$1" --role "$2" --name "$3" --create-tenant |
    node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).token))'
}
LOAD_KEY=$(key loadtest admin "k6-$(date +%s)")
node dist/cli/api-key.js tenant-rate-limit --tenant loadtest --per-sec 100000 --burst 1000000 >/dev/null
RL_KEY=$(key ratelimit producer "k6-$(date +%s)")
node dist/cli/api-key.js tenant-rate-limit --tenant ratelimit --per-sec 50 --burst 100 >/dev/null

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

PORT=$PORT HOST=127.0.0.1 node dist/server.js >"$OUT_DIR/api.log" 2>&1 &
pids+=($!)
for i in $(seq 1 "$WORKERS"); do
  WORKER_ID="lt-worker-$i" WORKER_QUEUES=demo,echo node dist/worker.js >"$OUT_DIR/worker-$i.log" 2>&1 &
  pids+=($!)
done

for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$PORT/ready" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$PORT/ready" >/dev/null

API_KEY=$LOAD_KEY
[ "$(basename "$SCRIPT" .js)" = rate-limit ] && API_KEY=$RL_KEY
name=$(basename "$SCRIPT" .js)
set +e
"$K6" run --quiet -e BASE_URL="http://127.0.0.1:$PORT" -e API_KEY="$API_KEY" \
  --summary-export "$OUT_DIR/$name.json" "$@" "$HERE/$(basename "$SCRIPT")"
status=$?
set -e
echo "summary: $OUT_DIR/$name.json  logs: $OUT_DIR/*.log"
exit $status
