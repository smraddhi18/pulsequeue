import { z } from 'zod';
import { QUEUE_NAME_PATTERN } from '../domain/job.js';

const postgresUrl = z
  .string()
  .min(1)
  .refine((v) => v.startsWith('postgres://') || v.startsWith('postgresql://'), {
    message: 'Must be a postgres:// or postgresql:// URL',
  });

const redisUrl = z
  .string()
  .min(1)
  .refine((v) => v.startsWith('redis://') || v.startsWith('rediss://'), {
    message: 'Must be a redis:// or rediss:// URL',
  });

const positiveInt = z.coerce.number().int().positive();

/** "true"/"false"/"1"/"0" (any case) -> boolean; anything else is a config error. */
const booleanFlag = (defaultValue: boolean) =>
  z
    .string()
    .trim()
    .toLowerCase()
    .pipe(z.enum(['true', 'false', '1', '0']))
    .default(defaultValue ? 'true' : 'false')
    .transform((v) => v === 'true' || v === '1');

/**
 * Database settings. Split out so the migration CLI can validate only what it
 * needs (it must not require REDIS_URL just to apply schema changes).
 */
const databaseEnvShape = {
  DATABASE_URL: postgresUrl,
  /** Max connections per process. Total = replicas x PG_POOL_MAX; keep under max_connections. */
  PG_POOL_MAX: positiveInt.max(200).default(10),
  /** Close idle clients after this long so we release server slots. */
  PG_IDLE_TIMEOUT_MS: positiveInt.default(30_000),
  /** Fail fast when the pool cannot get a connection (surfaces as 503, not a hung request). */
  PG_CONNECTION_TIMEOUT_MS: positiveInt.default(5_000),
  /** Server-side statement_timeout for every pooled connection. */
  PG_STATEMENT_TIMEOUT_MS: positiveInt.default(10_000),
} as const;

const databaseEnvSchema = z.object(databaseEnvShape);

/**
 * Redis settings (Milestone 5), shared by the API, worker and scheduler.
 * Redis is an accelerator, never the source of truth: with REDIS_URL unset
 * every Redis feature is off and the system behaves exactly as before
 * (workers poll, stats are computed from Postgres on every request).
 */
const redisEnvShape = {
  /** Unset = Redis features disabled. */
  REDIS_URL: redisUrl.optional(),
  /** Namespace for every key and channel, so environments can share one Redis. */
  REDIS_KEY_PREFIX: z
    .string()
    .regex(/^[A-Za-z0-9._:-]{1,64}$/, 'Only letters, digits and ._:- are allowed (max 64)')
    .default('pq'),
  /** TCP connect + handshake budget per attempt. */
  REDIS_CONNECT_TIMEOUT_MS: positiveInt.max(60_000).default(2_000),
  /** Per-command budget: a slow Redis must not slow down requests, they fall back to Postgres. */
  REDIS_COMMAND_TIMEOUT_MS: positiveInt.max(60_000).default(500),
  /** Reconnect backoff: delay = random(cap/2, cap), cap = min(MAX, BASE * 2^(attempt-1)). */
  REDIS_RECONNECT_BASE_MS: positiveInt.max(60_000).default(100),
  REDIS_RECONNECT_MAX_MS: positiveInt.max(300_000).default(5_000),
  /** Publish wake-ups on enqueue/replay/retry and let idle workers block on them instead of polling. */
  WAKEUP_ENABLED: booleanFlag(true),
  /** Per-queue publish coalescing window: at most ~2 publishes per queue per window. */
  WAKEUP_COALESCE_MS: positiveInt.max(10_000).default(50),
  /** Cache GET /queues/:name/stats in Redis (API) and mark it dirty on writes (all processes). */
  STATS_CACHE_ENABLED: booleanFlag(true),
  /**
   * Hard expiry of a cached stats entry (and of dirty flags): the staleness
   * bound if every invalidation for a queue were lost (Redis blip, crashed
   * writer). Shared so every process writes dirty flags with the same expiry.
   */
  STATS_CACHE_TTL_MS: positiveInt.min(100).max(3_600_000).default(10_000),
  /** Per-queue invalidation coalescing window (dirty-flag writes). */
  STATS_INVALIDATION_COALESCE_MS: positiveInt.max(60_000).default(100),
} as const;

export const redisEnvSchema = z.object(redisEnvShape);

function refineRedis(
  env: { REDIS_RECONNECT_BASE_MS: number; REDIS_RECONNECT_MAX_MS: number },
  ctx: z.RefinementCtx,
): void {
  if (env.REDIS_RECONNECT_BASE_MS > env.REDIS_RECONNECT_MAX_MS) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['REDIS_RECONNECT_BASE_MS'],
      message: 'Must not exceed REDIS_RECONNECT_MAX_MS',
    });
  }
}

/**
 * API authentication (Milestone 6). Every route except /health and /ready
 * requires `Authorization: Bearer pq_<prefix>_<secret>`; there is deliberately
 * no switch to turn auth off. Bootstrap the first key with the CLI
 * (`npm run api-key -- create ...`).
 */
const authEnvShape = {
  /**
   * How long an API instance trusts a verified key before looking it up
   * again. Bounds how long a revoked key keeps working on *other* instances
   * (the instance that revoked it forgets it at once). 0 = verify every
   * request against Postgres.
   */
  AUTH_CACHE_TTL_MS: z.coerce.number().int().min(0).max(60_000).default(5_000),
  /** Max verified keys cached per instance (LRU). */
  AUTH_CACHE_MAX_ENTRIES: positiveInt.max(1_000_000).default(10_000),
} as const;

/**
 * Enqueue rate limiting (Milestone 6): Redis token buckets per tenant and per
 * queue. Defaults apply to every tenant; a tenant or queue row can override
 * them (PUT /queues/:name/settings, `api-key tenant` CLI). Without REDIS_URL
 * there is nowhere to keep shared buckets, so limiting is off (fail-open).
 */
const rateLimitEnvShape = {
  RATE_LIMIT_ENABLED: booleanFlag(true),
  /** Default sustained enqueue rate per tenant (tokens per second). */
  RATE_LIMIT_TENANT_PER_SEC: positiveInt.max(1_000_000).default(100),
  /** Default bucket capacity per tenant: how big a burst may be. */
  RATE_LIMIT_TENANT_BURST: positiveInt.max(10_000_000).default(200),
  /**
   * What an enqueue does when Redis cannot answer: open = allow it (and log
   * ratelimit.unavailable), closed = reject with 503. Open matches the M5
   * stance that Redis is an accelerator; closed suits hard quotas.
   */
  RATE_LIMIT_FAIL_MODE: z.enum(['open', 'closed']).default('open'),
  /**
   * How long an API instance caches a queue's own rate limit (read from
   * Postgres on enqueue). A PUT /queues/:name/settings applies at once on the
   * instance that served it and within this TTL on the others. 0 = no cache.
   */
  RATE_LIMIT_SETTINGS_CACHE_TTL_MS: z.coerce.number().int().min(0).max(60_000).default(5_000),
} as const;

/**
 * Prometheus metrics (Milestone 7), shared by the API, worker and scheduler.
 * Every process serves GET /metrics on its own small HTTP listener, never on
 * the API port: the API's deny-by-default auth stays untouched (tenant API
 * keys must not see other tenants' series), and workers/schedulers, which
 * have no HTTP server, expose metrics the same way.
 */
const metricsEnvShape = {
  /** Unset = no metrics listener. 0 = ephemeral port (tests). */
  METRICS_PORT: z.coerce.number().int().min(0).max(65535).optional(),
  /** Loopback by default: only a sidecar/agent on the same host can scrape. */
  METRICS_HOST: z.string().min(1).default('127.0.0.1'),
  /**
   * Optional bearer token the scraper must send (Prometheus `authorization`).
   * Required in production when METRICS_HOST is not a loopback address.
   */
  METRICS_TOKEN: z.string().min(16, 'Must be at least 16 characters').optional(),
  /**
   * Max distinct tenant/queue label pairs per process. Beyond it, new pairs
   * are recorded under tenant/queue="__other__", so a tenant creating
   * thousands of queues cannot blow up Prometheus memory.
   */
  METRICS_MAX_QUEUE_SERIES: positiveInt.max(100_000).default(1_000),
} as const;

/**
 * OpenTelemetry tracing (Milestone 7). Off by default; when on, spans are
 * exported over OTLP/HTTP (Jaeger, an OTel collector, Tempo...).
 */
const tracingEnvShape = {
  TRACING_ENABLED: booleanFlag(false),
  /** OTLP/HTTP base URL, e.g. http://jaeger:4318 (spans go to <url>/v1/traces). */
  OTEL_EXPORTER_OTLP_ENDPOINT: z
    .string()
    .url()
    .refine((v) => v.startsWith('http://') || v.startsWith('https://'), {
      message: 'Must be an http:// or https:// URL',
    })
    .optional(),
  /** Overrides the per-process default (pulsequeue-api / -worker / -scheduler). */
  OTEL_SERVICE_NAME: z.string().trim().min(1).max(100).optional(),
  /** Head sampling ratio for new traces (parent-based: a sampled caller is always followed). */
  OTEL_TRACES_SAMPLER_RATIO: z.coerce.number().min(0).max(1).default(1),
} as const;

const LOOPBACK_HOSTS = new Set(['127.0.0.1', '::1', 'localhost']);

function refineObservability(
  env: {
    NODE_ENV: string;
    METRICS_PORT?: number | undefined;
    METRICS_HOST: string;
    METRICS_TOKEN?: string | undefined;
    TRACING_ENABLED: boolean;
    OTEL_EXPORTER_OTLP_ENDPOINT?: string | undefined;
  },
  ctx: z.RefinementCtx,
): void {
  if (
    env.METRICS_PORT !== undefined &&
    env.NODE_ENV === 'production' &&
    !LOOPBACK_HOSTS.has(env.METRICS_HOST) &&
    env.METRICS_TOKEN === undefined
  ) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['METRICS_TOKEN'],
      message:
        'Required in production when METRICS_HOST is not loopback (metrics would be readable by anyone on the network)',
    });
  }
  if (env.TRACING_ENABLED && env.OTEL_EXPORTER_OTLP_ENDPOINT === undefined) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      path: ['OTEL_EXPORTER_OTLP_ENDPOINT'],
      message: 'Required when TRACING_ENABLED is true',
    });
  }
}

const envSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    HOST: z.string().min(1).default('0.0.0.0'),
    PORT: z.coerce.number().int().positive().max(65535).default(3000),
    LOG_LEVEL: z
      .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
      .default('info'),
    ...databaseEnvShape,
    ...redisEnvShape,
    /** Max serialized size of a job payload (UTF-8 bytes of its JSON). */
    JOB_MAX_PAYLOAD_BYTES: positiveInt.max(10 * 1024 * 1024).default(256 * 1024),
    /** Readiness probe budget per dependency check. */
    READINESS_TIMEOUT_MS: positiveInt.max(30_000).default(2_000),
    /**
     * A dirty entry younger than this is still served (x-cache: STALE), which
     * caps recomputes at ~1 per queue per interval across all API instances
     * no matter how hot the queue is.
     */
    STATS_CACHE_MIN_REFRESH_MS: z.coerce.number().int().min(0).max(3_600_000).default(1_000),
    ...authEnvShape,
    ...rateLimitEnvShape,
    ...metricsEnvShape,
    ...tracingEnvShape,
    /** Export pulsequeue_queue_depth (read from Postgres on scrape) from this API instance. */
    METRICS_QUEUE_DEPTH_ENABLED: booleanFlag(true),
    /**
     * Queue depth is recomputed at most once per this interval per instance,
     * however often (and however many) scrapers hit /metrics. 0 = every scrape.
     */
    METRICS_QUEUE_DEPTH_CACHE_MS: z.coerce.number().int().min(0).max(600_000).default(5_000),
    /** statement_timeout for the depth query: a slow database yields stale gauges, not a hung scrape. */
    METRICS_QUEUE_DEPTH_TIMEOUT_MS: positiveInt.max(60_000).default(2_000),
    /**
     * Max attempts fed into one latency percentile computation (the most
     * recent ones in the window). Bounds the sort behind percentile_cont.
     */
    DASHBOARD_LATENCY_MAX_SAMPLES: positiveInt.max(10_000_000).default(100_000),
  })
  .superRefine((env, ctx) => {
    refineRedis(env, ctx);
    refineObservability(env, ctx);
    if (env.STATS_CACHE_MIN_REFRESH_MS >= env.STATS_CACHE_TTL_MS) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['STATS_CACHE_MIN_REFRESH_MS'],
        message: 'Must be lower than STATS_CACHE_TTL_MS',
      });
    }
    if (env.RATE_LIMIT_ENABLED && env.RATE_LIMIT_FAIL_MODE === 'closed' && !env.REDIS_URL) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['RATE_LIMIT_FAIL_MODE'],
        message:
          'closed requires REDIS_URL (without Redis every enqueue would be rejected); set REDIS_URL, use open, or set RATE_LIMIT_ENABLED=false',
      });
    }
  });

/**
 * Worker process settings (Milestone 2). The worker validates only what it
 * needs: database + Redis + logging + these, so it does not require HTTP
 * settings.
 */
const workerEnvSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    LOG_LEVEL: z
      .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
      .default('info'),
    ...databaseEnvShape,
    ...redisEnvShape,
    /** Stable worker identity for logs and job.locked_by; defaults to host-pid-random. */
    WORKER_ID: z
      .string()
      .trim()
      .min(1)
      .max(200)
      .regex(/^[A-Za-z0-9._:@-]+$/, 'Only letters, digits and ._:@- are allowed')
      .optional(),
    /** Comma-separated queues to consume; defaults to every queue with a registered handler. */
    WORKER_QUEUES: z
      .string()
      .optional()
      .transform((v) =>
        v === undefined
          ? undefined
          : [
              ...new Set(
                v
                  .split(',')
                  .map((q) => q.trim())
                  .filter((q) => q.length > 0),
              ),
            ],
      )
      .refine((v) => v === undefined || v.length > 0, {
        message: 'Must list at least one queue when set',
      })
      .refine((v) => v === undefined || v.every((q) => QUEUE_NAME_PATTERN.test(q)), {
        message: 'Queue names must match ^[a-z0-9][a-z0-9._-]{0,63}$',
      }),
    /** Max jobs this process runs at once. */
    WORKER_CONCURRENCY: positiveInt.max(1_000).default(10),
    /**
     * Idle poll interval (jittered +-20%) without wake-ups (Redis disabled or
     * down); a full lease batch re-polls immediately.
     */
    WORKER_POLL_INTERVAL_MS: positiveInt.min(10).max(60_000).default(1_000),
    /**
     * Idle poll interval (jittered +-20%) while subscribed to wake-ups. Only
     * a safety net for lost messages; delayed jobs and retries get their own
     * timer from the next due run_at.
     */
    WORKER_WAKEUP_FALLBACK_POLL_MS: positiveInt.min(10).max(600_000).default(30_000),
    /** Visibility timeout: how long a lease lives without a heartbeat. */
    WORKER_LEASE_DURATION_MS: positiveInt.min(1_000).max(3_600_000).default(30_000),
    /** How often a running job extends its lease. Must fit at least twice into the lease. */
    WORKER_HEARTBEAT_INTERVAL_MS: positiveInt.min(100).default(10_000),
    /** Hard cap on one attempt; the attempt is recorded as timed_out and retried. */
    WORKER_JOB_TIMEOUT_MS: positiveInt.min(100).max(86_400_000).default(300_000),
    /** How often this process looks for expired leases (crashed workers). */
    WORKER_REAPER_INTERVAL_MS: positiveInt.min(100).max(600_000).default(15_000),
    /**
     * Poison-message threshold: a job whose lease expired this many times (it
     * keeps crashing or hanging its worker) is dead-lettered with reason
     * poison_lease_expired even if it has attempts left.
     */
    WORKER_POISON_LEASE_EXPIRIES: positiveInt.max(100).default(3),
    /** Budget for draining in-flight jobs on SIGTERM before their leases are released. */
    WORKER_SHUTDOWN_TIMEOUT_MS: positiveInt.max(600_000).default(25_000),
    /** Optional file touched by the worker loop; container healthchecks read its mtime. */
    WORKER_LIVENESS_FILE: z.string().min(1).optional(),
    /** Retry backoff: delay = random(0, min(MAX, BASE * 2^(attempt-1))) ("full jitter"). */
    RETRY_BACKOFF_BASE_MS: positiveInt.max(3_600_000).default(1_000),
    RETRY_BACKOFF_MAX_MS: positiveInt.max(86_400_000).default(300_000),
    ...metricsEnvShape,
    ...tracingEnvShape,
  })
  .superRefine((env, ctx) => {
    refineRedis(env, ctx);
    refineObservability(env, ctx);
    if (env.WORKER_WAKEUP_FALLBACK_POLL_MS < env.WORKER_POLL_INTERVAL_MS) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['WORKER_WAKEUP_FALLBACK_POLL_MS'],
        message: 'Must be at least WORKER_POLL_INTERVAL_MS (wake-ups exist to poll less, not more)',
      });
    }
    if (env.WORKER_HEARTBEAT_INTERVAL_MS * 2 > env.WORKER_LEASE_DURATION_MS) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['WORKER_HEARTBEAT_INTERVAL_MS'],
        message:
          'Must be at most half of WORKER_LEASE_DURATION_MS so one missed heartbeat does not lose the lease',
      });
    }
    if (env.RETRY_BACKOFF_BASE_MS > env.RETRY_BACKOFF_MAX_MS) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['RETRY_BACKOFF_BASE_MS'],
        message: 'Must not exceed RETRY_BACKOFF_MAX_MS',
      });
    }
  });

/**
 * Scheduler process settings (Milestone 4). Like the worker, the scheduler
 * validates only database + Redis + logging + its own tuning.
 */
const schedulerEnvSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    LOG_LEVEL: z
      .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
      .default('info'),
    ...databaseEnvShape,
    ...redisEnvShape,
    /** Stable scheduler identity for logs; defaults to host-pid-random. */
    SCHEDULER_ID: z
      .string()
      .trim()
      .min(1)
      .max(200)
      .regex(/^[A-Za-z0-9._:@-]+$/, 'Only letters, digits and ._:@- are allowed')
      .optional(),
    /** How often the leader looks for due schedules (jittered +-10%). */
    SCHEDULER_TICK_INTERVAL_MS: positiveInt.min(100).max(60_000).default(1_000),
    /** How often a follower retries to become leader (jittered +-20%). Bounds failover time. */
    SCHEDULER_LEADER_RETRY_INTERVAL_MS: positiveInt.min(100).max(300_000).default(5_000),
    /** Max schedules claimed per tick transaction; a full batch is followed immediately by another. */
    SCHEDULER_BATCH_SIZE: positiveInt.max(1_000).default(100),
    /**
     * An occurrence at most this late is "on time" and always fires. Older
     * occurrences (scheduler down, paused leader, long failover) are misfires
     * and go through the schedule's misfire policy.
     */
    SCHEDULER_MISFIRE_GRACE_MS: positiveInt.min(1_000).max(3_600_000).default(60_000),
    /** catch_up policy: max missed occurrences enqueued in one go (the most recent ones). */
    SCHEDULER_MAX_CATCH_UP: z.coerce.number().int().min(0).max(1_000).default(10),
    /** Budget for finishing the in-flight tick on SIGTERM before leadership is released. */
    SCHEDULER_SHUTDOWN_TIMEOUT_MS: positiveInt.max(600_000).default(10_000),
    /** Optional file touched by the scheduler loop (leader or follower) for container healthchecks. */
    SCHEDULER_LIVENESS_FILE: z.string().min(1).optional(),
    ...metricsEnvShape,
    ...tracingEnvShape,
  })
  .superRefine((env, ctx) => {
    refineRedis(env, ctx);
    refineObservability(env, ctx);
    if (env.SCHEDULER_MISFIRE_GRACE_MS < env.SCHEDULER_TICK_INTERVAL_MS * 2) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['SCHEDULER_MISFIRE_GRACE_MS'],
        message:
          'Must be at least twice SCHEDULER_TICK_INTERVAL_MS, or normal tick latency would count as a misfire',
      });
    }
  });

export type Env = z.infer<typeof envSchema>;
export type DatabaseEnv = z.infer<typeof databaseEnvSchema>;
export type WorkerEnv = z.infer<typeof workerEnvSchema>;
export type SchedulerEnv = z.infer<typeof schedulerEnvSchema>;
/** The Redis settings every process shares (see redisEnvShape). */
export type RedisEnv = z.infer<typeof redisEnvSchema>;
/** Metrics + tracing settings every process shares (Milestone 7). */
export type ObservabilityEnv = Pick<
  Env,
  | 'NODE_ENV'
  | 'METRICS_PORT'
  | 'METRICS_HOST'
  | 'METRICS_TOKEN'
  | 'METRICS_MAX_QUEUE_SERIES'
  | 'TRACING_ENABLED'
  | 'OTEL_EXPORTER_OTLP_ENDPOINT'
  | 'OTEL_SERVICE_NAME'
  | 'OTEL_TRACES_SAMPLER_RATIO'
>;

function parseEnv<T extends z.ZodTypeAny>(schema: T, raw: NodeJS.ProcessEnv): z.infer<T> {
  const result = schema.safeParse(raw);
  if (!result.success) {
    const issues = result.error.issues
      .map((i) => `  - ${i.path.join('.') || '(root)'}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }
  return result.data as z.infer<T>;
}

/**
 * Parse and validate process env. Throws a readable Error on bad config
 * so the process fails fast before binding a port or opening connections.
 */
export function loadEnv(raw: NodeJS.ProcessEnv = process.env): Env {
  return parseEnv(envSchema, raw);
}

/** Database-only subset, used by the migration CLI. */
export function loadDatabaseEnv(raw: NodeJS.ProcessEnv = process.env): DatabaseEnv {
  return parseEnv(databaseEnvSchema, raw);
}

/** Worker process config (database + Redis + logging + worker tuning). */
export function loadWorkerEnv(raw: NodeJS.ProcessEnv = process.env): WorkerEnv {
  return parseEnv(workerEnvSchema, raw);
}

/** Scheduler process config (database + Redis + logging + scheduler tuning). */
export function loadSchedulerEnv(raw: NodeJS.ProcessEnv = process.env): SchedulerEnv {
  return parseEnv(schedulerEnvSchema, raw);
}
