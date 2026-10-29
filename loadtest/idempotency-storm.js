// Idempotent-retry storm: many clients retrying the same requests at once,
// the way a fleet of producers behaves after a network blip.
//
// Every VU walks the same list of KEYS idempotency keys, ROUNDS times, so the
// first round is a real race (VUS concurrent first writes per key) and later
// rounds are replays. The contract under test:
//   - exactly one 201 per key across all VUs  (idem_created == KEYS)
//   - every other response is 200 + Idempotent-Replayed: true
//   - a key always maps to the same job id     (idem_id_mismatch == 0)
//   - no 409/5xx                               (http_req_failed < 0.1%)
//
//   k6 run -e API_KEY=pq_... -e KEYS=200 -e VUS=50 loadtest/idempotency-storm.js
//
// RUN_ID namespaces the keys (default: a timestamp from setup), so reruns
// against the same database start fresh.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';
import { BASE_URL, QUEUE, authHeaders, int, jobBody, preflight } from './lib/config.js';

const KEYS = int('KEYS', 200);
const VUS = int('VUS', 50);
const ROUNDS = int('ROUNDS', 3);

const created = new Counter('idem_created');
const replayed = new Counter('idem_replayed');
const mismatch = new Counter('idem_id_mismatch');
const unexpected = new Counter('idem_unexpected_status');

export const options = {
  scenarios: {
    storm: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: KEYS * ROUNDS,
      maxDuration: __ENV.MAX_DURATION || '5m',
    },
  },
  thresholds: {
    idem_created: [`count==${KEYS}`],
    idem_id_mismatch: ['count==0'],
    idem_unexpected_status: ['count==0'],
    http_req_failed: ['rate<0.001'],
    'http_req_duration{name:enqueue_idem}': ['p(95)<250', 'p(99)<500'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  preflight(http);
  return { runId: __ENV.RUN_ID || `${Date.now()}` };
}

const seen = {}; // per VU: key -> job id

export default function (data) {
  const i = exec.vu.iterationInScenario % KEYS;
  const key = `storm-${data.runId}-${i}`;
  const res = http.post(`${BASE_URL}/queues/${QUEUE}/jobs`, jobBody(0), {
    headers: authHeaders({ 'idempotency-key': key }),
    tags: { name: 'enqueue_idem' },
  });
  if (res.status === 201) {
    created.add(1);
  } else if (res.status === 200 && res.headers['Idempotent-Replayed'] === 'true') {
    replayed.add(1);
  } else {
    unexpected.add(1);
    return;
  }
  const id = res.json('id');
  if (seen[key] === undefined) seen[key] = id;
  else if (seen[key] !== id) mismatch.add(1);
  check(res, { 'one job per key': () => seen[key] === id });
}
