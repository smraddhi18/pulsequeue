// Rate-limit behaviour: offer more than the tenant's limit and check the
// token bucket admits the burst plus the sustained rate, rejects the rest
// with 429 + Retry-After + RateLimit-* headers, and never errors.
//
// Point it at a tenant with a known limit, e.g.
//   npm run api-key -- tenant-rate-limit --tenant ratelimit --per-sec 50 --burst 100
//   k6 run -e API_KEY=<key of that tenant> -e LIMIT_PER_SEC=50 -e LIMIT_BURST=100 \
//          -e RATE=200 -e DURATION_S=30 loadtest/rate-limit.js
//
// The queue must not have its own tighter rate limit, and the bucket should
// be full when the run starts (wait burst/rate seconds after any other run).
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { BASE_URL, QUEUE, authHeaders, int, jobBody, preflight } from './lib/config.js';

const RATE = int('RATE', 200);
const DURATION_S = int('DURATION_S', 30);
const LIMIT_PER_SEC = int('LIMIT_PER_SEC', 50);
const LIMIT_BURST = int('LIMIT_BURST', 100);
const TOLERANCE = Number(__ENV.TOLERANCE || 0.1); // +-10% for clock/scheduling jitter

const ideal = LIMIT_BURST + LIMIT_PER_SEC * DURATION_S;
const accepted = new Counter('ratelimit_accepted');
const limited = new Counter('ratelimit_limited');
const other = new Counter('ratelimit_other_status');

http.setResponseCallback(http.expectedStatuses(200, 201, 429));

export const options = {
  scenarios: {
    overload: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: `${DURATION_S}s`,
      preAllocatedVUs: int('VUS', 50),
      maxVUs: int('VUS', 50) * 4,
    },
  },
  thresholds: {
    ratelimit_accepted: [
      `count>=${Math.floor(ideal * (1 - TOLERANCE))}`,
      `count<=${Math.ceil(ideal * (1 + TOLERANCE))}`,
    ],
    ratelimit_other_status: ['count==0'],
    http_req_failed: ['rate<0.001'], // 429 counts as expected here (200 = setup probes)
    checks: ['rate>0.999'],
    // A rejection must be cheaper than an enqueue: no Postgres write.
    'http_req_duration{status:429}': ['p(95)<50'],
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
  if (res.status === 201) {
    accepted.add(1);
    check(res, {
      'accepted has RateLimit-Remaining': (r) => r.headers['Ratelimit-Remaining'] !== undefined,
    });
  } else if (res.status === 429) {
    limited.add(1);
    check(res, {
      '429 has Retry-After': (r) => Number(r.headers['Retry-After']) >= 1,
      '429 has RateLimit-Reset': (r) => r.headers['Ratelimit-Reset'] !== undefined,
      '429 body is RATE_LIMITED': (r) => r.json('error.code') === 'RATE_LIMITED',
    });
  } else {
    other.add(1);
  }
}

export function handleSummary(data) {
  const acc = data.metrics.ratelimit_accepted?.values.count ?? 0;
  const lim = data.metrics.ratelimit_limited?.values.count ?? 0;
  const line =
    `rate-limit: offered ~${RATE * DURATION_S}, accepted ${acc}, limited ${lim}, ` +
    `ideal accepted ${ideal} (burst ${LIMIT_BURST} + ${LIMIT_PER_SEC}/s x ${DURATION_S}s)\n`;
  return { stdout: line };
}
