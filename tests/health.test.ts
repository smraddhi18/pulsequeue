import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { buildApp } from '../src/app.js';
import { loadEnv } from '../src/config/env.js';

describe('health routes', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    const env = loadEnv({
      NODE_ENV: 'test',
      HOST: '127.0.0.1',
      PORT: '3000',
      LOG_LEVEL: 'silent',
      // Deliberately unreachable: these tests must not depend on a running
      // database. Readiness with a live Postgres is covered in tests/integration.
      DATABASE_URL: 'postgres://pulsequeue:pulsequeue@127.0.0.1:1/pulsequeue',
      PG_CONNECTION_TIMEOUT_MS: '500',
      READINESS_TIMEOUT_MS: '1000',
      // REDIS_URL unset: Redis is optional and disabled here.
    });
    app = await buildApp({ env, logger: false });
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
  });

  it('GET /health returns liveness ok without touching dependencies', async () => {
    const res = await app.inject({ method: 'GET', url: '/health' });
    expect(res.statusCode).toBe(200);
    const body = res.json() as { status: string; service: string; timestamp: string };
    expect(body.status).toBe('ok');
    expect(body.service).toBe('pulsequeue');
    expect(body.timestamp).toBeTruthy();
  });

  it('GET /ready returns 503 with the failing check when Postgres is unreachable', async () => {
    const res = await app.inject({ method: 'GET', url: '/ready' });
    expect(res.statusCode).toBe(503);
    const body = res.json() as {
      status: string;
      checks: { app: string; postgres: string };
      timestamp: string;
    };
    expect(body.status).toBe('not_ready');
    expect(body.checks).toEqual({ app: 'ok', postgres: 'fail' });
  });

  it('omits the redis check when Redis is disabled', async () => {
    const res = await app.inject({ method: 'GET', url: '/ready' });
    expect(res.json()).not.toHaveProperty('checks.redis');
  });

  it('echoes or assigns x-request-id', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/health',
      headers: { 'x-request-id': 'test-req-123' },
    });
    expect(res.headers['x-request-id']).toBe('test-req-123');
  });

  it('returns consistent JSON for unknown routes', async () => {
    const res = await app.inject({ method: 'GET', url: '/does-not-exist' });
    expect(res.statusCode).toBe(404);
    const body = res.json() as { error: { code: string; message: string } };
    expect(body.error.code).toBe('NOT_FOUND');
  });

  it('maps malformed JSON bodies to 400 BAD_REQUEST, not 500', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/queues/emails/jobs',
      headers: { 'content-type': 'application/json' },
      payload: '{not json',
    });
    expect(res.statusCode).toBe(400);
    expect((res.json() as { error: { code: string } }).error.code).toBe('BAD_REQUEST');
  });

  it('validates enqueue input before touching the database', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/queues/emails/jobs',
      payload: { payload: {}, priority: 1000 },
    });
    expect(res.statusCode).toBe(400);
    expect((res.json() as { error: { code: string } }).error.code).toBe('VALIDATION_ERROR');
  });

  it('maps database outages on API calls to 503 SERVICE_UNAVAILABLE', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/queues/emails/jobs',
      payload: { payload: {} },
    });
    expect(res.statusCode).toBe(503);
    expect((res.json() as { error: { code: string } }).error.code).toBe('SERVICE_UNAVAILABLE');
  });
});

describe('readiness with Redis configured but unreachable', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    const env = loadEnv({
      NODE_ENV: 'test',
      LOG_LEVEL: 'silent',
      DATABASE_URL: 'postgres://pulsequeue:pulsequeue@127.0.0.1:1/pulsequeue',
      PG_CONNECTION_TIMEOUT_MS: '500',
      READINESS_TIMEOUT_MS: '1000',
      // Nothing listens on port 1: the client keeps reconnecting in the background.
      REDIS_URL: 'redis://127.0.0.1:1',
      REDIS_RECONNECT_BASE_MS: '50',
      REDIS_RECONNECT_MAX_MS: '200',
    });
    app = await buildApp({ env, logger: false });
    await app.ready();
  });

  afterAll(async () => {
    await app.close();
  });

  it('reports redis as failing without blocking startup or the probe', async () => {
    const started = Date.now();
    const res = await app.inject({ method: 'GET', url: '/ready' });
    expect(Date.now() - started).toBeLessThan(1_500);
    expect(res.statusCode).toBe(503); // Postgres is down too: that alone is not_ready
    expect(res.json()).toMatchObject({
      status: 'not_ready',
      checks: { app: 'ok', postgres: 'fail', redis: 'fail' },
    });
  });
});
