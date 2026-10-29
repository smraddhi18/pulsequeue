import { randomBytes } from 'node:crypto';
import net from 'node:net';
import { Redis } from 'ioredis';
import pg from 'pg';
import { describe } from 'vitest';
import { loadDatabaseEnv, loadEnv, type Env } from '../../src/config/env.js';
import { runMigrations, type RunMigrationsOptions } from '../../src/db/migrate.js';
import { createPool, type Pool } from '../../src/db/pool.js';

/**
 * Integration tests run against a real Postgres when TEST_DATABASE_URL (or
 * DATABASE_URL) is set and are skipped otherwise, so `npm test` stays green on
 * a laptop without a database.
 *
 * Isolation: every suite gets its own freshly migrated schema (search_path is
 * set per connection), so suites can run in parallel and never touch data in
 * `public`, even if the URL points at a development database.
 */
export const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL;

export const describeWithDb: ReturnType<typeof describe.skipIf> =
  describe.skipIf(!TEST_DATABASE_URL);

export interface TestDatabase {
  url: string;
  schema: string;
  pool: Pool;
  /** Drop the schema and close pools. */
  destroy(): Promise<void>;
}

export async function createTestDatabase(
  options: { migrate?: boolean; migrations?: RunMigrationsOptions } = {},
): Promise<TestDatabase> {
  const url = requireUrl();
  const schema = `pq_test_${randomBytes(6).toString('hex')}`;

  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  try {
    await admin.query(`CREATE SCHEMA ${schema}`);
  } finally {
    await admin.end();
  }

  const pool = createPool(loadDatabaseEnv({ DATABASE_URL: url, PG_POOL_MAX: '10' }), {
    options: `-c search_path=${schema}`,
  });

  if (options.migrate !== false) {
    await runMigrations(pool, options.migrations);
  }

  return {
    url,
    schema,
    pool,
    async destroy() {
      await pool.end();
      const cleanup = new pg.Client({ connectionString: url });
      await cleanup.connect();
      try {
        await cleanup.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
      } finally {
        await cleanup.end();
      }
    },
  };
}

/**
 * An extra pool on the same isolated schema, standing in for another process
 * (e.g. one per simulated worker). The caller must end() it.
 */
export function createSchemaPool(db: TestDatabase, max = 4): Pool {
  return createPool(loadDatabaseEnv({ DATABASE_URL: db.url, PG_POOL_MAX: String(max) }), {
    options: `-c search_path=${db.schema}`,
  });
}

export function testEnv(overrides: Record<string, string> = {}): Env {
  return loadEnv({
    NODE_ENV: 'test',
    LOG_LEVEL: 'silent',
    DATABASE_URL: requireUrl(),
    ...overrides,
  });
}

function requireUrl(): string {
  if (!TEST_DATABASE_URL) {
    throw new Error('TEST_DATABASE_URL or DATABASE_URL must be set for integration tests');
  }
  return TEST_DATABASE_URL;
}

/**
 * Redis-backed suites (Milestone 5) additionally need TEST_REDIS_URL and are
 * skipped without it: Redis is optional at runtime, so the Postgres-only
 * suites above must keep passing without one (they run with REDIS_URL unset).
 *
 * Isolation: every suite gets its own REDIS_KEY_PREFIX, so keys and channels
 * never collide between parallel suites, and removes its keys afterwards.
 */
export const TEST_REDIS_URL = process.env.TEST_REDIS_URL;

export const describeWithRedis: ReturnType<typeof describe.skipIf> = describe.skipIf(
  !TEST_DATABASE_URL || !TEST_REDIS_URL,
);

export function requireRedisUrl(): string {
  if (!TEST_REDIS_URL) throw new Error('TEST_REDIS_URL must be set for Redis integration tests');
  return TEST_REDIS_URL;
}

export function uniqueRedisPrefix(): string {
  return `pqtest:${randomBytes(6).toString('hex')}`;
}

/** testEnv with Redis enabled under a suite-unique key prefix. */
export function redisTestEnv(prefix: string, overrides: Record<string, string> = {}): Env {
  return testEnv({ REDIS_URL: requireRedisUrl(), REDIS_KEY_PREFIX: prefix, ...overrides });
}

/** A plain client for assertions (inspect keys, publish, count subscribers). */
export function createAdminRedis(url = requireRedisUrl()): Redis {
  return new Redis(url, { lazyConnect: false, maxRetriesPerRequest: 1 });
}

/** Delete every key under `prefix` (SCAN, never KEYS). */
export async function deleteRedisPrefix(redis: Redis, prefix: string): Promise<void> {
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', `${prefix}:*`, 'COUNT', 500);
    cursor = next;
    if (keys.length > 0) await redis.del(...keys);
  } while (cursor !== '0');
}

/**
 * A TCP proxy that can simulate dependency failures (Milestone 5 for Redis,
 * generalised in Milestone 8 for Postgres):
 *
 * - cut(): every connection is reset and new ones are refused (the server
 *   crashed or restarted; clients see ECONNRESET / ECONNREFUSED at once).
 * - freeze(): connections stay open but no byte moves in either direction,
 *   and new connections are accepted but never answered (a silent network
 *   partition: only client-side timeouts can notice it).
 * - restore(): back to normal; frozen bytes are delivered (TCP retransmits
 *   after a healed partition).
 */
export class TcpOutageProxy {
  private server: net.Server | null = null;
  private readonly sockets = new Set<net.Socket>();
  private readonly pairs = new Set<{ client: net.Socket; upstream: net.Socket }>();
  private frozen = false;
  protected port = 0;

  protected constructor(
    private readonly targetHost: string,
    private readonly targetPort: number,
  ) {}

  static async forTarget(host: string, port: number): Promise<TcpOutageProxy> {
    const proxy = new TcpOutageProxy(host, port);
    await proxy.listen(0);
    return proxy;
  }

  get listenPort(): number {
    return this.port;
  }

  /** Connections currently open through the proxy (client side). */
  get openConnections(): number {
    return this.pairs.size;
  }

  async cut(): Promise<void> {
    const server = this.server;
    this.server = null;
    this.frozen = false;
    for (const socket of this.sockets) socket.destroy();
    this.sockets.clear();
    this.pairs.clear();
    if (server) await new Promise<void>((resolve) => server.close(() => resolve()));
  }

  freeze(): void {
    this.frozen = true;
    for (const pair of this.pairs) this.unlink(pair);
  }

  async restore(): Promise<void> {
    if (this.frozen) {
      this.frozen = false;
      for (const pair of this.pairs) this.link(pair);
    }
    if (!this.server) await this.listen(this.port);
  }

  async stop(): Promise<void> {
    await this.cut();
  }

  private link(pair: { client: net.Socket; upstream: net.Socket }): void {
    pair.client.pipe(pair.upstream);
    pair.upstream.pipe(pair.client);
  }

  private unlink(pair: { client: net.Socket; upstream: net.Socket }): void {
    pair.client.unpipe(pair.upstream);
    pair.upstream.unpipe(pair.client);
    pair.client.pause();
    pair.upstream.pause();
  }

  protected listen(port: number): Promise<void> {
    const server = net.createServer((client) => {
      const upstream = net.connect(this.targetPort, this.targetHost);
      const pair = { client, upstream };
      this.sockets.add(client);
      this.sockets.add(upstream);
      this.pairs.add(pair);
      const drop = () => {
        client.destroy();
        upstream.destroy();
        this.sockets.delete(client);
        this.sockets.delete(upstream);
        this.pairs.delete(pair);
      };
      client.on('error', drop).on('close', drop);
      upstream.on('error', drop).on('close', drop);
      if (this.frozen) this.unlink(pair);
      else this.link(pair);
    });
    this.server = server;
    return new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, '127.0.0.1', () => {
        this.port = (server.address() as net.AddressInfo).port;
        resolve();
      });
    });
  }
}

/** Redis behind a TcpOutageProxy (Milestone 5 outage tests). */
export class RedisOutageProxy extends TcpOutageProxy {
  static async start(url = requireRedisUrl()): Promise<RedisOutageProxy> {
    const target = new URL(url);
    const proxy = new RedisOutageProxy(target.hostname, Number(target.port || 6379));
    await proxy.listen(0);
    return proxy;
  }

  /** redis:// URL that goes through the proxy. */
  get url(): string {
    return `redis://127.0.0.1:${this.port}`;
  }
}

/** Postgres behind a TcpOutageProxy (Milestone 8 chaos tests). */
export class PgOutageProxy extends TcpOutageProxy {
  static async start(url = requireUrl()): Promise<PgOutageProxy> {
    const target = new URL(url);
    const proxy = new PgOutageProxy(target.hostname, Number(target.port || 5432));
    await proxy.listen(0);
    return proxy;
  }

  /**
   * postgres:// URL for `db` that goes through the proxy, with the suite's
   * search_path in the libpq startup options (also usable by child processes).
   */
  urlFor(db: TestDatabase): string {
    const url = new URL(db.url);
    url.hostname = '127.0.0.1';
    url.port = String(this.port);
    url.searchParams.set('options', `-c search_path=${db.schema}`);
    return url.toString();
  }
}

/** DATABASE_URL for a child process: the suite's schema via libpq startup options. */
export function schemaUrl(db: TestDatabase): string {
  const url = new URL(db.url);
  url.searchParams.set('options', `-c search_path=${db.schema}`);
  return url.toString();
}

/** Poll an (async) predicate until it holds. */
export async function waitUntil(
  predicate: () => Promise<boolean> | boolean,
  { timeoutMs = 10_000, intervalMs = 10 } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error('waitUntil timed out');
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}
