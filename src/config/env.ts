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

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  HOST: z.string().min(1).default('0.0.0.0'),
  PORT: z.coerce.number().int().positive().max(65535).default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  ...databaseEnvShape,
  REDIS_URL: redisUrl,
  /** Max serialized size of a job payload (UTF-8 bytes of its JSON). */
  JOB_MAX_PAYLOAD_BYTES: positiveInt.max(10 * 1024 * 1024).default(256 * 1024),
  /** Readiness probe budget per dependency check. */
  READINESS_TIMEOUT_MS: positiveInt.max(30_000).default(2_000),
});

/**
 * Worker process settings (Milestone 2). The worker validates only what it
 * needs: database + logging + these, so it does not require HTTP settings.
 */
const workerEnvSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    LOG_LEVEL: z
      .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
      .default('info'),
    ...databaseEnvShape,
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
    /** Idle poll interval (jittered +-20%); a full lease batch re-polls immediately. */
    WORKER_POLL_INTERVAL_MS: positiveInt.min(10).max(60_000).default(1_000),
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
  })
  .superRefine((env, ctx) => {
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
 * validates only database + logging + its own tuning.
 */
const schedulerEnvSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    LOG_LEVEL: z
      .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
      .default('info'),
    ...databaseEnvShape,
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
  })
  .superRefine((env, ctx) => {
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

/** Worker process config (database + logging + worker tuning). */
export function loadWorkerEnv(raw: NodeJS.ProcessEnv = process.env): WorkerEnv {
  return parseEnv(workerEnvSchema, raw);
}

/** Scheduler process config (database + logging + scheduler tuning). */
export function loadSchedulerEnv(raw: NodeJS.ProcessEnv = process.env): SchedulerEnv {
  return parseEnv(schedulerEnvSchema, raw);
}
