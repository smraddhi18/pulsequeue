// Enqueue throughput: an open model (constant arrival rate), so a slow server
// shows up as latency and dropped iterations instead of quietly lowering the
// request rate the way a closed VU loop would.
//
//   k6 run -e API_KEY=pq_... -e RATE=500 -e DURATION=60s loadtest/enqueue-throughput.js
//
// RATE      enqueues per second              (default 300)
// DURATION  steady-state length              (default 60s)
// VUS       pre-allocated VUs                (default 100; raise if k6 warns)
// P95_MS / P99_MS  latency thresholds        (default 100 / 250)
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, QUEUE, authHeaders, int, jobBody, preflight } from './lib/config.js';

const RATE = int('RATE', 300);
const VUS = int('VUS', 100);

export const options = {
  scenarios: {
    enqueue: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: VUS,
      maxVUs: VUS * 4,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
    'http_req_duration{name:enqueue}': [
      `p(95)<${int('P95_MS', 100)}`,
      `p(99)<${int('P99_MS', 250)}`,
    ],
    // Fewer than 1% of the planned arrivals may be skipped for lack of VUs.
    dropped_iterations: [`rate<${RATE * 0.01}`],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  return preflight(http);
}

export default function () {
  const res = http.post(`${BASE_URL}/queues/${QUEUE}/jobs`, jobBody(0), {
    headers: authHeaders(),
    tags: { name: 'enqueue' },
  });
  check(res, { 'enqueue 201': (r) => r.status === 201 });
}
