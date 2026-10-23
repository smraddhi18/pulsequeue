import pg from 'pg';
import type { Queryable } from '../db/pool.js';
import type { Logger } from '../lib/logger.js';

/**
 * Leader election for the scheduler: at most one process may evaluate
 * schedules at a time.
 *
 * Implementation: a session-level Postgres advisory lock held on a dedicated
 * connection, and every tick runs on *that same connection*. The lock and the
 * work share one session, which gives a real fence: if the session dies
 * (crash, network partition, pg_terminate_backend) Postgres releases the lock
 * and aborts the in-flight tick transaction atomically, so a stale leader can
 * never commit after a new one took over.
 *
 * Trade-offs vs. a lease row (`UPDATE leader SET holder = $me, expires_at =
 * now() + ttl WHERE expires_at < now()`):
 *   + No TTL to tune: failover is "session gone" (immediate on a clean exit or
 *     a killed process, TCP keepalive + server timeouts on a partition), not
 *     "wait for the lease to expire".
 *   + No clock or renewal loop: nothing to heartbeat, no split brain when a
 *     paused leader resumes after its lease expired.
 *   - Needs a real session: works through PgBouncer only in session mode (in
 *     transaction mode the lock would stick to a random server connection).
 *   - Holds one connection per scheduler replica for its whole life.
 *   - Invisible to anything but Postgres (`pg_locks`); a lease row is easy to
 *     show in an admin UI. Both get the same `application_name` for ops.
 */
export interface LeaderLock {
  /** True while this process believes it holds the lock on a live session. */
  readonly held: boolean;
  /** Try to become leader (non-blocking). Idempotent while held. */
  tryAcquire(): Promise<boolean>;
  /** Run `fn` on the lock-holding session. Throws if leadership is not held. */
  withSession<T>(fn: (db: Queryable) => Promise<T>): Promise<T>;
  /** Graceful step-down: unlock, then close the session. Never throws. */
  release(): Promise<void>;
  /** Abandon the session without unlocking (Postgres unlocks on disconnect). Never throws. */
  reset(): Promise<void>;
}

export interface PgAdvisoryLeaderLockOptions {
  connectionString: string;
  logger: Logger;
  /**
   * Lock namespace. The key is hashtext(lockName || ':' || current_schema()),
   * so deployments sharing a database but not a schema elect separately.
   */
  lockName?: string;
  connectionTimeoutMs: number;
  statementTimeoutMs: number;
  /** Extra libpq startup options, e.g. `-c search_path=...` (tests). */
  options?: string;
}

export const DEFAULT_LOCK_NAME = 'pulsequeue.scheduler';

export class PgAdvisoryLeaderLock implements LeaderLock {
  private client: pg.Client | null = null;
  private lost = false;

  constructor(private readonly opts: PgAdvisoryLeaderLockOptions) {}

  get held(): boolean {
    return this.client !== null && !this.lost;
  }

  async tryAcquire(): Promise<boolean> {
    if (this.held) return true;
    await this.reset();

    const client = new pg.Client({
      connectionString: this.opts.connectionString,
      application_name: 'pulsequeue-scheduler',
      connectionTimeoutMillis: this.opts.connectionTimeoutMs,
      statement_timeout: this.opts.statementTimeoutMs,
      // Detect a dead peer on an idle leader session (no query in flight).
      keepAlive: true,
      keepAliveInitialDelayMillis: 10_000,
      ...(this.opts.options !== undefined ? { options: this.opts.options } : {}),
    });
    // Without a listener a dropped connection would crash the process.
    // Losing the session means losing the lock: remember it, the loop reacts.
    client.on('error', (err) => {
      if (this.client !== client) return;
      this.lost = true;
      this.opts.logger.warn({ err }, 'Scheduler leader session error');
    });
    client.on('end', () => {
      if (this.client === client) this.lost = true;
    });

    try {
      await client.connect();
      const { rows } = await client.query<{ acquired: boolean }>(
        `SELECT pg_try_advisory_lock(hashtext($1 || ':' || current_schema())) AS acquired`,
        [this.opts.lockName ?? DEFAULT_LOCK_NAME],
      );
      if (rows[0]?.acquired !== true) {
        await endQuietly(client);
        return false;
      }
    } catch (err) {
      await endQuietly(client);
      throw err;
    }
    this.client = client;
    this.lost = false;
    return true;
  }

  async withSession<T>(fn: (db: Queryable) => Promise<T>): Promise<T> {
    const client = this.client;
    if (!client || this.lost) throw new Error('Scheduler leadership is not held');
    return fn(client);
  }

  async release(): Promise<void> {
    const client = this.client;
    if (!client) return;
    this.client = null;
    if (!this.lost) {
      // Explicit unlock so a follower can take over before our TCP close is
      // processed; closing the session would release it as well.
      await client.query('SELECT pg_advisory_unlock_all()').catch(() => undefined);
    }
    this.lost = false;
    await endQuietly(client);
  }

  async reset(): Promise<void> {
    const client = this.client;
    this.client = null;
    this.lost = false;
    if (client) await endQuietly(client);
  }
}

async function endQuietly(client: pg.Client): Promise<void> {
  try {
    await client.end();
  } catch {
    // Already closed or broken: nothing left to release.
  }
}
