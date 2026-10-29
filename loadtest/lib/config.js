// Shared settings for every k6 script. Everything is overridable with -e / env:
//   BASE_URL  API origin                   (default http://127.0.0.1:3000)
//   API_KEY   Bearer token (pq_...)        (required for everything but /health, /ready)
//   QUEUE     queue the scripts write to   (default demo: the built-in no-op handler)
import { fail } from 'k6';

export const BASE_URL = (__ENV.BASE_URL || 'http://127.0.0.1:3000').replace(/\/+$/, '');
export const QUEUE = __ENV.QUEUE || 'demo';

export function apiKey() {
  const key = __ENV.API_KEY;
  if (!key) fail('API_KEY is not set (create one with: npm run api-key -- create ...)');
  return key;
}

export function int(name, fallback) {
  const raw = __ENV[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) fail(`${name} must be a number, got "${raw}"`);
  return n;
}

export function authHeaders(extra = {}) {
  return {
    authorization: `Bearer ${apiKey()}`,
    'content-type': 'application/json',
    ...extra,
  };
}

/** Body of a cheap job: the demo handler sleeps durationMs (0 = return at once). */
export function jobBody(durationMs = 0) {
  return JSON.stringify({ payload: { durationMs } });
}

/** Fail the run early if the target is not up and the key is not valid. */
export function preflight(http) {
  const ready = http.get(`${BASE_URL}/ready`);
  if (ready.status !== 200) fail(`${BASE_URL}/ready returned ${ready.status}: ${ready.body}`);
  const who = http.get(`${BASE_URL}/auth/whoami`, { headers: authHeaders() });
  if (who.status !== 200) fail(`API_KEY rejected by /auth/whoami (${who.status}): ${who.body}`);
  return who.json();
}
