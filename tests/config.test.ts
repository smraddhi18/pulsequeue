import { describe, expect, it } from 'vitest';
import { loadDatabaseEnv, loadEnv, loadSchedulerEnv, loadWorkerEnv } from '../src/config/env.js';

const validEnv = {
  NODE_ENV: 'test',
  HOST: '127.0.0.1',
  PORT: '3000',
  LOG_LEVEL: 'error',
  DATABASE_URL: 'postgres://pulsequeue:pulsequeue@localhost:5432/pulsequeue',
  REDIS_URL: 'redis://localhost:6379',
} as const;

describe('loadEnv', () => {
  it('parses a valid environment', () => {
    const env = loadEnv({ ...validEnv });
    expect(env.NODE_ENV).toBe('test');
    expect(env.HOST).toBe('127.0.0.1');
    expect(env.PORT).toBe(3000);
    expect(env.LOG_LEVEL).toBe('error');
    expect(env.DATABASE_URL).toContain('postgres://');
    expect(env.REDIS_URL).toContain('redis://');
  });

  it('applies defaults for optional fields', () => {
    const env = loadEnv({
      DATABASE_URL: validEnv.DATABASE_URL,
      REDIS_URL: validEnv.REDIS_URL,
    });
    expect(env.NODE_ENV).toBe('development');
    expect(env.HOST).toBe('0.0.0.0');
    expect(env.PORT).toBe(3000);
    expect(env.LOG_LEVEL).toBe('info');
  });

  it('fails fast on missing DATABASE_URL', () => {
    expect(() =>
      loadEnv({
        REDIS_URL: validEnv.REDIS_URL,
      }),
    ).toThrow(/Invalid environment configuration/);
  });

  it('fails fast on invalid PORT', () => {
    expect(() =>
      loadEnv({
        ...validEnv,
        PORT: 'not-a-number',
      }),
    ).toThrow(/Invalid environment configuration/);
  });

  it('fails fast on invalid NODE_ENV', () => {
    expect(() =>
      loadEnv({
        ...validEnv,
        NODE_ENV: 'staging',
      }),
    ).toThrow(/Invalid environment configuration/);
  });

  it('applies database pool and job defaults', () => {
    const env = loadEnv({ ...validEnv });
    expect(env.PG_POOL_MAX).toBe(10);
    expect(env.PG_CONNECTION_TIMEOUT_MS).toBe(5_000);
    expect(env.PG_STATEMENT_TIMEOUT_MS).toBe(10_000);
    expect(env.JOB_MAX_PAYLOAD_BYTES).toBe(256 * 1024);
    expect(env.READINESS_TIMEOUT_MS).toBe(2_000);
  });

  it('fails fast on an invalid pool size', () => {
    expect(() => loadEnv({ ...validEnv, PG_POOL_MAX: '0' })).toThrow(
      /Invalid environment configuration/,
    );
  });
});

describe('loadDatabaseEnv', () => {
  it('needs only database settings (the migration CLI must not require REDIS_URL)', () => {
    const env = loadDatabaseEnv({ DATABASE_URL: validEnv.DATABASE_URL });
    expect(env.DATABASE_URL).toBe(validEnv.DATABASE_URL);
    expect(env.PG_POOL_MAX).toBe(10);
  });

  it('rejects a non-postgres URL', () => {
    expect(() => loadDatabaseEnv({ DATABASE_URL: 'mysql://x' })).toThrow(
      /Invalid environment configuration/,
    );
  });
});

describe('loadWorkerEnv', () => {
  const base = { DATABASE_URL: validEnv.DATABASE_URL };

  it('needs only database settings and applies worker defaults', () => {
    const env = loadWorkerEnv(base);
    expect(env.WORKER_CONCURRENCY).toBe(10);
    expect(env.WORKER_POLL_INTERVAL_MS).toBe(1_000);
    expect(env.WORKER_LEASE_DURATION_MS).toBe(30_000);
    expect(env.WORKER_HEARTBEAT_INTERVAL_MS).toBe(10_000);
    expect(env.WORKER_JOB_TIMEOUT_MS).toBe(300_000);
    expect(env.WORKER_REAPER_INTERVAL_MS).toBe(15_000);
    expect(env.WORKER_SHUTDOWN_TIMEOUT_MS).toBe(25_000);
    expect(env.WORKER_POISON_LEASE_EXPIRIES).toBe(3);
    expect(env.RETRY_BACKOFF_BASE_MS).toBe(1_000);
    expect(env.RETRY_BACKOFF_MAX_MS).toBe(300_000);
    expect(env.WORKER_QUEUES).toBeUndefined();
    expect(env.WORKER_ID).toBeUndefined();
  });

  it('parses WORKER_QUEUES as a de-duplicated, trimmed list', () => {
    const env = loadWorkerEnv({ ...base, WORKER_QUEUES: ' emails, reports ,emails,' });
    expect(env.WORKER_QUEUES).toEqual(['emails', 'reports']);
  });

  it.each([
    ['WORKER_QUEUES', ' , '],
    ['WORKER_QUEUES', 'Bad Queue'],
    ['WORKER_CONCURRENCY', '0'],
    ['WORKER_POLL_INTERVAL_MS', 'fast'],
    ['WORKER_ID', 'has spaces'],
    ['WORKER_POISON_LEASE_EXPIRIES', '0'],
    ['WORKER_POISON_LEASE_EXPIRIES', '101'],
  ])('rejects %s=%j', (key, value) => {
    expect(() => loadWorkerEnv({ ...base, [key]: value })).toThrow(
      /Invalid environment configuration/,
    );
  });

  it('requires at least two heartbeats per lease', () => {
    expect(() =>
      loadWorkerEnv({
        ...base,
        WORKER_LEASE_DURATION_MS: '10000',
        WORKER_HEARTBEAT_INTERVAL_MS: '6000',
      }),
    ).toThrow(/WORKER_HEARTBEAT_INTERVAL_MS/);
    expect(
      loadWorkerEnv({
        ...base,
        WORKER_LEASE_DURATION_MS: '10000',
        WORKER_HEARTBEAT_INTERVAL_MS: '5000',
      }).WORKER_HEARTBEAT_INTERVAL_MS,
    ).toBe(5_000);
  });

  it('rejects a backoff base above the cap', () => {
    expect(() =>
      loadWorkerEnv({ ...base, RETRY_BACKOFF_BASE_MS: '5000', RETRY_BACKOFF_MAX_MS: '1000' }),
    ).toThrow(/RETRY_BACKOFF_BASE_MS/);
  });
});

describe('loadSchedulerEnv', () => {
  const db = { DATABASE_URL: validEnv.DATABASE_URL };

  it('needs only DATABASE_URL and applies defaults', () => {
    const env = loadSchedulerEnv(db);
    expect(env).toMatchObject({
      SCHEDULER_TICK_INTERVAL_MS: 1_000,
      SCHEDULER_LEADER_RETRY_INTERVAL_MS: 5_000,
      SCHEDULER_BATCH_SIZE: 100,
      SCHEDULER_MISFIRE_GRACE_MS: 60_000,
      SCHEDULER_MAX_CATCH_UP: 10,
      SCHEDULER_SHUTDOWN_TIMEOUT_MS: 10_000,
    });
    expect(env.SCHEDULER_ID).toBeUndefined();
    expect(env.SCHEDULER_LIVENESS_FILE).toBeUndefined();
    expect(env).not.toHaveProperty('REDIS_URL');
  });

  it('parses overrides, including a zero catch-up limit', () => {
    const env = loadSchedulerEnv({
      ...db,
      SCHEDULER_ID: 'sched-a',
      SCHEDULER_TICK_INTERVAL_MS: '250',
      SCHEDULER_MAX_CATCH_UP: '0',
      SCHEDULER_LIVENESS_FILE: '/tmp/scheduler-alive',
    });
    expect(env).toMatchObject({
      SCHEDULER_ID: 'sched-a',
      SCHEDULER_TICK_INTERVAL_MS: 250,
      SCHEDULER_MAX_CATCH_UP: 0,
      SCHEDULER_LIVENESS_FILE: '/tmp/scheduler-alive',
    });
  });

  it.each([
    ['tick interval below 100 ms', { SCHEDULER_TICK_INTERVAL_MS: '50' }],
    ['batch size above 1000', { SCHEDULER_BATCH_SIZE: '1001' }],
    ['negative catch-up', { SCHEDULER_MAX_CATCH_UP: '-1' }],
    ['grace below 1 s', { SCHEDULER_MISFIRE_GRACE_MS: '500' }],
    ['id with spaces', { SCHEDULER_ID: 'a b' }],
    ['non-numeric interval', { SCHEDULER_LEADER_RETRY_INTERVAL_MS: 'soon' }],
  ])('rejects %s', (_label, overrides) => {
    expect(() => loadSchedulerEnv({ ...db, ...overrides })).toThrow(
      /Invalid environment configuration/,
    );
  });

  it('requires the misfire grace to cover at least two ticks', () => {
    expect(() =>
      loadSchedulerEnv({
        ...db,
        SCHEDULER_TICK_INTERVAL_MS: '30000',
        SCHEDULER_MISFIRE_GRACE_MS: '45000',
      }),
    ).toThrow(/SCHEDULER_MISFIRE_GRACE_MS: Must be at least twice/);
  });
});

describe('Redis settings (Milestone 5)', () => {
  const db = { DATABASE_URL: validEnv.DATABASE_URL };

  it('makes Redis optional for every process', () => {
    expect(loadEnv(db).REDIS_URL).toBeUndefined();
    expect(loadWorkerEnv(db).REDIS_URL).toBeUndefined();
    expect(loadSchedulerEnv(db).REDIS_URL).toBeUndefined();
  });

  it('applies Redis, wake-up and stats cache defaults', () => {
    const env = loadEnv({ ...validEnv });
    expect(env).toMatchObject({
      REDIS_KEY_PREFIX: 'pq',
      REDIS_CONNECT_TIMEOUT_MS: 2_000,
      REDIS_COMMAND_TIMEOUT_MS: 500,
      REDIS_RECONNECT_BASE_MS: 100,
      REDIS_RECONNECT_MAX_MS: 5_000,
      WAKEUP_ENABLED: true,
      WAKEUP_COALESCE_MS: 50,
      STATS_CACHE_ENABLED: true,
      STATS_CACHE_TTL_MS: 10_000,
      STATS_INVALIDATION_COALESCE_MS: 100,
      STATS_CACHE_MIN_REFRESH_MS: 1_000,
    });
    expect(loadWorkerEnv(db).WORKER_WAKEUP_FALLBACK_POLL_MS).toBe(30_000);
  });

  it.each([
    ['true', true],
    ['1', true],
    [' FALSE ', false],
    ['0', false],
  ])('parses WAKEUP_ENABLED=%j', (value, expected) => {
    expect(loadWorkerEnv({ ...db, WAKEUP_ENABLED: value }).WAKEUP_ENABLED).toBe(expected);
  });

  it.each([
    ['REDIS_URL', 'http://localhost:6379'],
    ['REDIS_KEY_PREFIX', 'has space'],
    ['WAKEUP_ENABLED', 'yes'],
    ['STATS_CACHE_TTL_MS', '50'],
    ['REDIS_COMMAND_TIMEOUT_MS', '0'],
  ])('rejects %s=%j', (key, value) => {
    expect(() => loadEnv({ ...validEnv, [key]: value })).toThrow(
      /Invalid environment configuration/,
    );
  });

  it('accepts rediss:// (TLS)', () => {
    expect(loadEnv({ ...validEnv, REDIS_URL: 'rediss://cache:6380/0' }).REDIS_URL).toBe(
      'rediss://cache:6380/0',
    );
  });

  it('rejects a reconnect base above the cap, in every process', () => {
    const bad = { ...db, REDIS_RECONNECT_BASE_MS: '6000', REDIS_RECONNECT_MAX_MS: '5000' };
    expect(() => loadEnv(bad)).toThrow(/REDIS_RECONNECT_BASE_MS/);
    expect(() => loadWorkerEnv(bad)).toThrow(/REDIS_RECONNECT_BASE_MS/);
    expect(() => loadSchedulerEnv(bad)).toThrow(/REDIS_RECONNECT_BASE_MS/);
  });

  it('requires the min refresh interval to be below the TTL', () => {
    expect(() =>
      loadEnv({ ...db, STATS_CACHE_MIN_REFRESH_MS: '10000', STATS_CACHE_TTL_MS: '10000' }),
    ).toThrow(/STATS_CACHE_MIN_REFRESH_MS/);
    expect(loadEnv({ ...db, STATS_CACHE_MIN_REFRESH_MS: '0' }).STATS_CACHE_MIN_REFRESH_MS).toBe(0);
  });

  it('requires the wake-up fallback poll to be at least the poll interval', () => {
    expect(() =>
      loadWorkerEnv({
        ...db,
        WORKER_POLL_INTERVAL_MS: '2000',
        WORKER_WAKEUP_FALLBACK_POLL_MS: '1000',
      }),
    ).toThrow(/WORKER_WAKEUP_FALLBACK_POLL_MS/);
  });
});

describe('auth and rate-limit settings (Milestone 6)', () => {
  const db = { DATABASE_URL: validEnv.DATABASE_URL };

  it('applies auth cache and rate-limit defaults', () => {
    expect(loadEnv({ ...validEnv })).toMatchObject({
      AUTH_CACHE_TTL_MS: 5_000,
      AUTH_CACHE_MAX_ENTRIES: 10_000,
      RATE_LIMIT_ENABLED: true,
      RATE_LIMIT_TENANT_PER_SEC: 100,
      RATE_LIMIT_TENANT_BURST: 200,
      RATE_LIMIT_FAIL_MODE: 'open',
      RATE_LIMIT_SETTINGS_CACHE_TTL_MS: 5_000,
    });
  });

  it('allows AUTH_CACHE_TTL_MS=0 (verify every request against Postgres)', () => {
    expect(loadEnv({ ...db, AUTH_CACHE_TTL_MS: '0' }).AUTH_CACHE_TTL_MS).toBe(0);
  });

  it.each([
    ['AUTH_CACHE_TTL_MS', '-1'],
    ['AUTH_CACHE_TTL_MS', '60001'],
    ['RATE_LIMIT_TENANT_PER_SEC', '0'],
    ['RATE_LIMIT_TENANT_BURST', '1.5'],
    ['RATE_LIMIT_FAIL_MODE', 'maybe'],
    ['RATE_LIMIT_ENABLED', 'on'],
    ['RATE_LIMIT_SETTINGS_CACHE_TTL_MS', '-5'],
    ['RATE_LIMIT_SETTINGS_CACHE_TTL_MS', '60001'],
  ])('rejects %s=%j', (key, value) => {
    expect(() => loadEnv({ ...validEnv, [key]: value })).toThrow(
      /Invalid environment configuration/,
    );
  });

  it('refuses fail-closed rate limiting without Redis (every enqueue would be rejected)', () => {
    expect(() => loadEnv({ ...db, RATE_LIMIT_FAIL_MODE: 'closed' })).toThrow(
      /RATE_LIMIT_FAIL_MODE: closed requires REDIS_URL/,
    );
    expect(
      loadEnv({ ...db, RATE_LIMIT_FAIL_MODE: 'closed', RATE_LIMIT_ENABLED: 'false' })
        .RATE_LIMIT_FAIL_MODE,
    ).toBe('closed');
    expect(loadEnv({ ...validEnv, RATE_LIMIT_FAIL_MODE: 'closed' }).RATE_LIMIT_FAIL_MODE).toBe(
      'closed',
    );
  });

  it('keeps rate-limit settings out of the worker and scheduler', () => {
    expect(loadWorkerEnv(db)).not.toHaveProperty('RATE_LIMIT_ENABLED');
    expect(loadSchedulerEnv(db)).not.toHaveProperty('AUTH_CACHE_TTL_MS');
  });
});
