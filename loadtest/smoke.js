// Smoke: a few seconds of light traffic over the main endpoints, cheap enough
// for CI. Proves the deployed API is wired up (auth, Postgres, Redis, workers
// optional) without saying anything about capacity.
//
//   k6 run -e API_KEY=pq_... loadtest/smoke.js
import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, QUEUE, authHeaders, jobBody, preflight } from './lib/config.js';

export const options = {
  vus: 5,
  duration: __ENV.DURATION || '10s',
  thresholds: {
    http_req_failed: ['rate==0'],
    checks: ['rate==1'],
    http_req_duration: ['p(95)<500'],
  },
};

export function setup() {
  return preflight(http);
}

export default function () {
  const res = http.post(`${BASE_URL}/queues/${QUEUE}/jobs`, jobBody(0), {
    headers: authHeaders({ 'idempotency-key': `smoke-${__VU}-${__ITER % 5}` }),
  });
  check(res, { 'enqueue 201/200': (r) => r.status === 201 || r.status === 200 });
  const id = res.json('id');
  check(http.get(`${BASE_URL}/jobs/${id}`, { headers: authHeaders() }), {
    'get 200': (r) => r.status === 200,
  });
  check(http.get(`${BASE_URL}/queues/${QUEUE}/jobs?limit=5`, { headers: authHeaders() }), {
    'list 200': (r) => r.status === 200,
  });
  check(http.get(`${BASE_URL}/queues/${QUEUE}/stats`, { headers: authHeaders() }), {
    'stats 200': (r) => r.status === 200,
  });
}
