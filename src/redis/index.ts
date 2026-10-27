import { NOOP_QUEUE_SIGNALS, type QueueSignals } from '../domain/signals.js';
import type { RedisEnv } from '../config/env.js';
import type { StructuredLogger } from '../lib/logger.js';
import type { RateLimitStore } from '../services/rate-limiter.js';
import type { StatsCacheStore } from '../services/stats-cache.js';
import {
  closeRedisClient,
  createRedisClient,
  pingRedis,
  type RedisClient,
  type RedisConnectionSettings,
} from './client.js';
import { redisKeys, type RedisKeys } from './keys.js';
import { RedisQueueSignals } from './queue-signals.js';
import { RedisRateLimitStore } from './rate-limit-store.js';
import { RedisStatsCacheStore } from './stats-cache-store.js';
import { StatsInvalidator } from './stats-invalidator.js';
import { WakeupPublisher } from './wakeup-publisher.js';
import { WakeupSubscriber } from './wakeup-subscriber.js';

export * from './client.js';
export * from './coalescer.js';
export * from './keys.js';
export * from './queue-signals.js';
export * from './rate-limit-store.js';
export * from './stats-cache-store.js';
export * from './stats-invalidator.js';
export * from './wakeup-publisher.js';
export * from './wakeup-subscriber.js';

export type RedisRole = 'api' | 'worker' | 'scheduler';

export interface CreateRedisServicesOptions {
  logger: StructuredLogger;
  role: RedisRole;
  /** Process identity (worker / scheduler id); used in connection names and wake-up sources. */
  id: string;
  /** API with RATE_LIMIT_ENABLED (M6): create the token-bucket store. */
  rateLimit?: boolean;
}

/**
 * Everything one process gets from Redis. With Redis disabled (REDIS_URL
 * unset) `client` is null, `signals` is a no-op and every consumer falls back
 * to its Postgres-only behaviour.
 */
export interface RedisServices {
  client: RedisClient | null;
  keys: RedisKeys;
  settings: RedisConnectionSettings | null;
  signals: QueueSignals;
  /** API with STATS_CACHE_ENABLED: the Redis stats cache store. */
  statsStore: StatsCacheStore | null;
  /** API with rate limiting on: the Redis token-bucket store. */
  rateLimitStore: RateLimitStore | null;
  /** Readiness probe (null when disabled). */
  probe: (() => Promise<void>) | null;
  /** Flush pending signals, then close the connection. Idempotent. */
  close(): Promise<void>;
}

/** Command connection for one process (publishes, cache reads, dirty flags). */
export function createRedisServices(
  env: RedisEnv,
  options: CreateRedisServicesOptions,
): RedisServices {
  const keys = redisKeys(env.REDIS_KEY_PREFIX);
  const rateLimit = options.rateLimit === true && options.role === 'api';
  if (
    env.REDIS_URL === undefined ||
    (!env.WAKEUP_ENABLED && !env.STATS_CACHE_ENABLED && !rateLimit)
  ) {
    options.logger.info(
      {
        event: 'redis.disabled',
        reason: env.REDIS_URL === undefined ? 'no_url' : 'all_features_off',
      },
      'Redis disabled: workers poll, stats are computed from Postgres, enqueues are not rate limited',
    );
    return {
      client: null,
      keys,
      settings: null,
      signals: NOOP_QUEUE_SIGNALS,
      statsStore: null,
      rateLimitStore: null,
      probe: null,
      close: async () => undefined,
    };
  }

  const settings: RedisConnectionSettings = { ...env, REDIS_URL: env.REDIS_URL };
  const client = createRedisClient(settings, {
    logger: options.logger,
    name: `pulsequeue:${options.role}:${options.id}`,
  });

  const publisher = env.WAKEUP_ENABLED
    ? new WakeupPublisher({
        client,
        keys,
        coalesceMs: env.WAKEUP_COALESCE_MS,
        logger: options.logger,
        source: `${options.role}:${options.id}`,
      })
    : null;
  // Dirty flags expire like the entries they refer to.
  const ttlMs = env.STATS_CACHE_TTL_MS;
  const invalidator = env.STATS_CACHE_ENABLED
    ? new StatsInvalidator({
        client,
        keys,
        ttlMs,
        coalesceMs: env.STATS_INVALIDATION_COALESCE_MS,
        logger: options.logger,
      })
    : null;
  const signals = new RedisQueueSignals(publisher, invalidator);
  const statsStore =
    env.STATS_CACHE_ENABLED && options.role === 'api'
      ? new RedisStatsCacheStore({ client, keys, ttlMs })
      : null;
  const rateLimitStore = rateLimit ? new RedisRateLimitStore({ client, keys }) : null;

  let closing: Promise<void> | undefined;
  return {
    client,
    keys,
    settings,
    signals,
    statsStore,
    rateLimitStore,
    probe: () => pingRedis(client),
    close: () => {
      closing ??= (async () => {
        await signals.flush();
        await closeRedisClient(client);
      })();
      return closing;
    },
  };
}

export interface CreateWakeupSubscriberOptions {
  logger: StructuredLogger;
  id: string;
  queues: readonly string[];
  onWake: (queue: string) => void;
  onListeningChange: (listening: boolean) => void;
}

/**
 * Worker only: a second, dedicated connection for SUBSCRIBE (a subscribed
 * connection cannot run other commands). null when wake-ups are disabled.
 */
export function createWakeupSubscriber(
  env: RedisEnv,
  options: CreateWakeupSubscriberOptions,
): WakeupSubscriber | null {
  if (env.REDIS_URL === undefined || !env.WAKEUP_ENABLED) return null;
  const client = createRedisClient(
    { ...env, REDIS_URL: env.REDIS_URL },
    { logger: options.logger, name: `pulsequeue:worker-subscriber:${options.id}` },
  );
  return new WakeupSubscriber({
    client,
    keys: redisKeys(env.REDIS_KEY_PREFIX),
    queues: options.queues,
    logger: options.logger,
    onWake: options.onWake,
    onListeningChange: options.onListeningChange,
  });
}
