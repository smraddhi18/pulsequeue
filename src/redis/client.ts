import { Redis } from 'ioredis';
import type { RedisEnv } from '../config/env.js';
import type { StructuredLogger } from '../lib/logger.js';
import { LogThrottle } from '../lib/log-throttle.js';

export type RedisClient = Redis;

/** Redis settings with a URL, i.e. Redis is enabled. */
export type RedisConnectionSettings = Pick<
  RedisEnv,
  | 'REDIS_CONNECT_TIMEOUT_MS'
  | 'REDIS_COMMAND_TIMEOUT_MS'
  | 'REDIS_RECONNECT_BASE_MS'
  | 'REDIS_RECONNECT_MAX_MS'
> & { REDIS_URL: string };

export interface CreateRedisClientOptions {
  logger: StructuredLogger;
  /**
   * Connection name (CLIENT SETNAME), e.g. `pulsequeue:worker:subscriber:<id>`:
   * shows up in CLIENT LIST so operators can tell connections apart.
   */
  name: string;
  random?: () => number;
}

/** At most one redis.error line per client per interval; the rest are counted. */
const ERROR_LOG_INTERVAL_MS = 10_000;

/**
 * Reconnect delay for attempt n (1-based): random(cap/2, cap) with
 * cap = min(max, base * 2^(n-1)). "Equal jitter": never 0 (no hot loop
 * against a dead server), and replicas restarting together spread out.
 */
export function reconnectDelay(
  attempt: number,
  baseMs: number,
  maxMs: number,
  random: () => number = Math.random,
): number {
  const cap = Math.min(maxMs, baseMs * 2 ** Math.min(Math.max(attempt - 1, 0), 30));
  return Math.max(1, Math.round(cap / 2 + random() * (cap / 2)));
}

/**
 * One ioredis connection with the failure behaviour every PulseQueue
 * process wants from an *optional* dependency:
 *
 * - never throws at construction and reconnects forever with jittered
 *   exponential backoff (retryStrategy);
 * - commands fail fast while disconnected (no offline queue, no per-request
 *   retries) and are bounded by REDIS_COMMAND_TIMEOUT_MS, so callers fall
 *   back to Postgres instead of waiting on a sick Redis;
 * - connection transitions are logged once each (redis.connected,
 *   redis.disconnected), errors are throttled (redis.error), so an outage
 *   does not flood the logs.
 *
 * Pub/sub needs a dedicated connection (a subscribed connection cannot run
 * normal commands), so callers create one client per role.
 */
/**
 * Clients being closed on purpose (closeRedisClient). Their 'close' is the
 * end of a shutdown, not an outage, so it is logged at info as redis.closed
 * instead of warning that the connection was lost.
 */
const closingClients = new WeakSet<RedisClient>();

export function createRedisClient(
  settings: RedisConnectionSettings,
  options: CreateRedisClientOptions,
): RedisClient {
  const random = options.random ?? Math.random;
  const log = options.logger.child({ redisClient: options.name });

  const client = new Redis(settings.REDIS_URL, {
    connectionName: options.name,
    connectTimeout: settings.REDIS_CONNECT_TIMEOUT_MS,
    commandTimeout: settings.REDIS_COMMAND_TIMEOUT_MS,
    enableOfflineQueue: false,
    maxRetriesPerRequest: 0,
    // The wake-up subscriber re-subscribes itself on every 'ready', so it
    // knows exactly when it is listening again (see WakeupSubscriber).
    autoResubscribe: false,
    retryStrategy: (attempt: number) =>
      reconnectDelay(
        attempt,
        settings.REDIS_RECONNECT_BASE_MS,
        settings.REDIS_RECONNECT_MAX_MS,
        random,
      ),
    // After a failover the old primary may answer READONLY: reconnect so DNS
    // / Sentinel can point us at the new primary.
    reconnectOnError: (err: Error) => err.message.startsWith('READONLY'),
  });

  const errors = new LogThrottle(ERROR_LOG_INTERVAL_MS);
  let connected = false;
  let attempts = 0;

  client.on('ready', () => {
    connected = true;
    log.info({ event: 'redis.connected', reconnectAttempts: attempts }, 'Redis connected');
    attempts = 0;
  });
  client.on('close', () => {
    if (!connected) return;
    connected = false;
    if (closingClients.has(client)) {
      log.info({ event: 'redis.closed' }, 'Redis connection closed');
      return;
    }
    log.warn({ event: 'redis.disconnected' }, 'Redis connection lost; reconnecting');
  });
  client.on('reconnecting', (delayMs: number) => {
    attempts++;
    log.debug({ event: 'redis.reconnecting', attempt: attempts, delayMs }, 'Redis reconnecting');
  });
  // Without an 'error' listener ioredis prints every failed attempt to stderr.
  client.on('error', (err: Error) => {
    const decision = errors.check('error');
    if (!decision.log) return;
    log.warn(
      { event: 'redis.error', err, suppressed: decision.suppressed },
      'Redis connection error',
    );
  });

  return client;
}

export function isRedisReady(client: RedisClient): boolean {
  return client.status === 'ready';
}

/** Readiness probe: rejects unless the connection is up and answers PING. */
export async function pingRedis(client: RedisClient): Promise<void> {
  if (!isRedisReady(client)) throw new Error(`Redis is not connected (status ${client.status})`);
  await client.ping();
}

/**
 * Close gracefully: QUIT lets already-sent commands (e.g. the last wake-up
 * publishes flushed on shutdown) complete first. Falls back to a hard
 * disconnect when the connection is down or QUIT does not answer in time.
 */
export async function closeRedisClient(client: RedisClient, timeoutMs = 1_000): Promise<void> {
  if (client.status === 'end') return;
  closingClients.add(client);
  if (!isRedisReady(client)) {
    client.disconnect();
    return;
  }
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      client.quit(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Redis QUIT timed out')), timeoutMs);
      }),
    ]);
  } catch {
    client.disconnect();
  } finally {
    clearTimeout(timer);
  }
}
