// Mixed traffic: producers enqueueing while dashboards and clients read.
// Default split per iteration: 60% enqueue, 25% GET /jobs/:id (a job this VU
// created), 10% list /queues/:q/jobs, 5% /queues/:q/stats (Redis-cached).
//
//   k6 run -e API_KEY=pq_... -e RATE=400 loadtest/mixed.js
//
// RATE, DURATION, VUS as in enqueue-throughput.js.
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, QUEUE, authHeaders, int, jobBody, preflight } from './lib/config.js';

const RATE = int('RATE', 300);
const VUS = int('VUS', 100);
const recent = []; // per-VU ring of job ids this VU created

export const options = {
  scenarios: {
    mixed: {
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
    'http_req_duration{name:enqueue}': ['p(95)<150', 'p(99)<300'],
    'http_req_duration{name:get_job}': ['p(95)<100', 'p(99)<250'],
    'http_req_duration{name:list_jobs}': ['p(95)<200', 'p(99)<400'],
    'http_req_duration{name:stats}': ['p(95)<100', 'p(99)<250'],
    dropped_iterations: [`rate<${RATE * 0.01}`],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  return preflight(http);
}

function enqueue() {
  const res = http.post(`${BASE_URL}/queues/${QUEUE}/jobs`, jobBody(0), {
    headers: authHeaders(),
    tags: { name: 'enqueue' },
  });
  if (check(res, { 'enqueue 201': (r) => r.status === 201 })) {
    recent.push(res.json('id'));
    if (recent.length > 50) recent.shift();
  }
}

export default function () {
  const roll = Math.random();
  if (roll < 0.6 || recent.length === 0) {
    enqueue();
  } else if (roll < 0.85) {
    const id = recent[Math.floor(Math.random() * recent.length)];
    const res = http.get(`${BASE_URL}/jobs/${id}`, {
      headers: authHeaders(),
      tags: { name: 'get_job' },
    });
    check(res, { 'get job 200': (r) => r.status === 200 });
  } else if (roll < 0.95) {
    const res = http.get(`${BASE_URL}/queues/${QUEUE}/jobs?limit=20`, {
      headers: authHeaders(),
      tags: { name: 'list_jobs' },
    });
    check(res, { 'list 200': (r) => r.status === 200 });
  } else {
    const res = http.get(`${BASE_URL}/queues/${QUEUE}/stats`, {
      headers: authHeaders(),
      tags: { name: 'stats' },
    });
    check(res, { 'stats 200': (r) => r.status === 200 });
  }
}
