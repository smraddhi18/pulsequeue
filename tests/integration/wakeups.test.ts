import type { FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import pino from 'pino';
import { afterAll, afterEach, beforeAll, beforeEach, expect, it, vi } from 'vitest';
import { buildApp } from '../../src/app.js';
import { loadWorkerEnv } from '../../src/config/env.js';
import type { Pool } from '../../src/db/pool.js';
import type { QueueName } from '../../src/domain/job.js';
import { JobRepository } from '../../src/repositories/job.repository.js';
import { LeaseRepository } from '../../src/repositories/lease.repository.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { JobService } from '../../src/services/job.service.js';
import { HandlerRegistry, type JobHandler } from '../../src/worker/registry.js';
import { createWorkerRuntime, type WorkerRuntime } from '../../src/worker/runtime.js';
import {
  createAdminRedis,
  createSchemaPool,
  createTestDatabase,
  deleteRedisPrefix,
  describeWithRedis,
  RedisOutageProxy,
  redisTestEnv,
  requireRedisUrl,
  uniqueRedisPrefix,
  waitUntil,
  type TestDatabase,
} from './helpers.js';

const logger = pino({ level: 'silent' });
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Generous next to the wake-up path (~ms), tiny next to the fallback poll. */
const FALLBACK_POLL_MS = 20_000;

describeWithRedis('wake-ups (Postgres + Redis)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let admin: Redis;
  let prefix: string;
  let app: FastifyInstance;
  /** Enqueues without Redis: a "lost" wake-up. */
  let silentJobs: JobService;
  const cleanups: (() => Promise<unknown>)[] = [];

  beforeAll(async () => {
    db = await createTestDatabase();
    admin = createAdminRedis();
    silentJobs = new JobService(new QueueRepository(db.pool), new JobRepository(db.pool), {
      maxPayloadBytes: 10_000,
    });
  });

  afterAll(async () => {
    await admin?.quit();
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
    prefix = uniqueRedisPrefix();
    app = await buildApp({
      env: redisTestEnv(prefix, { WAKEUP_COALESCE_MS: '5' }),
      logger: false,
      pool: db.pool,
    });
    await app.ready();
    await waitUntil(async () => {
      const res = await app.inject({ method: 'GET', url: '/ready' });
      return res.json<{ status: string }>().status === 'ready';
    });
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
    await app.close();
    await deleteRedisPrefix(admin, prefix);
  });

  function startRuntime(
    handler: JobHandler,
    overrides: Record<string, string> = {},
  ): { runtime: WorkerRuntime; pool: Pool } {
    const registry = new HandlerRegistry().register('emails', handler);
    const pool = createSchemaPool(db, 4);
    const env = loadWorkerEnv({
      DATABASE_URL: db.url,
      REDIS_URL: requireRedisUrl(),
      REDIS_KEY_PREFIX: prefix,
      REDIS_RECONNECT_BASE_MS: '20',
      REDIS_RECONNECT_MAX_MS: '100',
      WORKER_CONCURRENCY: '2',
      WORKER_POLL_INTERVAL_MS: '1000',
      WORKER_WAKEUP_FALLBACK_POLL_MS: String(FALLBACK_POLL_MS),
      RETRY_BACKOFF_BASE_MS: '300',
      RETRY_BACKOFF_MAX_MS: '300',
      WAKEUP_COALESCE_MS: '5',
      ...overrides,
    });
    const runtime = createWorkerRuntime(env, { registry, logger, pool });
    cleanups.push(async () => {
      await runtime.stop();
      await pool.end();
    });
    runtime.start();
    return { runtime, pool };
  }

  /** Listening and idle (its catch-up poll after subscribing has returned). */
  async function untilPushModeIdle(runtime: WorkerRuntime): Promise<void> {
    await waitUntil(() => runtime.subscriber?.isListening === true);
    await waitUntil(() => runtime.worker.isPushMode);
    await sleep(50);
  }

  async function enqueue(payload: unknown = {}, extra: Record<string, unknown> = {}) {
    const res = await app.inject({
      method: 'POST',
      url: '/queues/emails/jobs',
      payload: { payload, ...extra },
    });
    expect(res.statusCode).toBe(201);
    return res.json<{ id: string }>().id;
  }

  async function status(id: string): Promise<string> {
    const { rows } = await db.pool.query<{ status: string }>(
      'SELECT status FROM jobs WHERE id = $1',
      [id],
    );
    return rows[0]?.status ?? 'missing';
  }

  it('an idle push-mode worker picks up a new job in milliseconds, not at the next poll', async () => {
    const started: number[] = [];
    const { runtime } = startRuntime(async () => void started.push(Date.now()));
    await untilPushModeIdle(runtime);

    for (let i = 0; i < 3; i++) {
      const enqueuedAt = Date.now();
      const id = await enqueue({ i });
      await waitUntil(async () => (await status(id)) === 'succeeded', { timeoutMs: 3_000 });
      // Poll interval 1 s, fallback 20 s: only a wake-up explains this.
      expect(started[i]! - enqueuedAt).toBeLessThan(500);
      await sleep(30);
    }
  });

  it('push mode stops idle polling: a fraction of the lease queries of poll mode', async () => {
    const lease = vi.spyOn(LeaseRepository.prototype, 'lease');
    const push = startRuntime(async () => undefined, { WORKER_POLL_INTERVAL_MS: '25' });
    await untilPushModeIdle(push.runtime);
    lease.mockClear();
    await sleep(600);
    const pushCalls = lease.mock.calls.length;

    const poll = startRuntime(async () => undefined, {
      WORKER_POLL_INTERVAL_MS: '25',
      WAKEUP_ENABLED: 'false',
    });
    expect(poll.runtime.subscriber).toBeNull();
    await sleep(100);
    lease.mockClear();
    await sleep(600);
    // The push worker is still idle here; nearly all calls are the poller's.
    const pollCalls = lease.mock.calls.length - pushCalls;

    expect(pushCalls).toBeLessThanOrEqual(1);
    expect(pollCalls).toBeGreaterThanOrEqual(10);
  });

  it('a lost wake-up costs latency, never the job: the fallback poll finds it', async () => {
    const { runtime } = startRuntime(async () => undefined, {
      WORKER_WAKEUP_FALLBACK_POLL_MS: '1500',
    });
    await untilPushModeIdle(runtime);
    const { job } = await silentJobs.enqueue({ queue: 'emails' as QueueName, payload: {} });
    await sleep(500);
    expect(await status(job.id)).toBe('queued'); // no wake-up, not polled yet
    await waitUntil(async () => (await status(job.id)) === 'succeeded', { timeoutMs: 5_000 });
  });

  it('a delayed job runs when due even though nothing publishes then (next-due cap)', async () => {
    const started: number[] = [];
    const { runtime } = startRuntime(async () => void started.push(Date.now()));
    await untilPushModeIdle(runtime);
    const enqueuedAt = Date.now();
    const id = await enqueue({}, { delayMs: 700 });
    await waitUntil(async () => (await status(id)) === 'succeeded', { timeoutMs: 5_000 });
    const latency = started[0]! - enqueuedAt;
    expect(latency).toBeGreaterThanOrEqual(650);
    expect(latency).toBeLessThan(2_000);
  });

  it('a retry runs after its backoff without waiting for the fallback poll', async () => {
    let attempts = 0;
    const { runtime } = startRuntime(async () => {
      attempts++;
      if (attempts === 1) throw new Error('transient');
    });
    await untilPushModeIdle(runtime);
    const id = await enqueue();
    await waitUntil(async () => (await status(id)) === 'succeeded', { timeoutMs: 5_000 });
    expect(attempts).toBe(2);
  });

  it('Redis outage: the worker falls back to polling, then returns to push mode', async () => {
    const proxy = await RedisOutageProxy.start();
    cleanups.push(() => proxy.stop());
    const { runtime } = startRuntime(async () => undefined, {
      REDIS_URL: proxy.url,
      WORKER_POLL_INTERVAL_MS: '100',
    });
    await untilPushModeIdle(runtime);

    await proxy.cut();
    await waitUntil(() => !runtime.worker.isPushMode, { timeoutMs: 3_000 });
    // Enqueued while Redis is unreachable for the worker: found by polling.
    const { job } = await silentJobs.enqueue({ queue: 'emails' as QueueName, payload: {} });
    await waitUntil(async () => (await status(job.id)) === 'succeeded', { timeoutMs: 2_000 });

    await proxy.restore();
    await waitUntil(() => runtime.worker.isPushMode, { timeoutMs: 5_000 });
    const enqueuedAt = Date.now();
    const id = await enqueue();
    await waitUntil(async () => (await status(id)) === 'succeeded', { timeoutMs: 3_000 });
    expect(Date.now() - enqueuedAt).toBeLessThan(1_000);
  });

  it('wake-ups are per queue: a worker is not woken for queues it does not consume', async () => {
    const lease = vi.spyOn(LeaseRepository.prototype, 'lease');
    const { runtime } = startRuntime(async () => undefined);
    await untilPushModeIdle(runtime);
    lease.mockClear();
    for (let i = 0; i < 5; i++) {
      const res = await app.inject({
        method: 'POST',
        url: '/queues/reports/jobs',
        payload: { payload: { i } },
      });
      expect(res.statusCode).toBe(201);
      await sleep(20);
    }
    await sleep(100);
    expect(lease).not.toHaveBeenCalled();
  });
});
