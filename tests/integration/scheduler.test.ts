import type pg from 'pg';
import pino from 'pino';
import type { Queryable } from '../../src/db/pool.js';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { loadSchedulerEnv } from '../../src/config/env.js';
import type { QueueName } from '../../src/domain/job.js';
import { scheduleJobIdempotencyKey, type MisfirePolicy } from '../../src/domain/schedule.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { ScheduleRepository } from '../../src/repositories/schedule.repository.js';
import { PgAdvisoryLeaderLock } from '../../src/scheduler/leader-lock.js';
import { createSchedulerRuntime, type SchedulerRuntime } from '../../src/scheduler/runtime.js';
import { runTick, type TickOptions } from '../../src/scheduler/tick.js';
import { ScheduleService } from '../../src/services/schedule.service.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

interface LogLine {
  event?: string;
  [key: string]: unknown;
}

function capture() {
  const lines: LogLine[] = [];
  const logger = pino(
    { level: 'debug' },
    { write: (s: string) => void lines.push(JSON.parse(s) as LogLine) },
  );
  return {
    logger,
    lines,
    count: (event: string) => lines.filter((l) => l.event === event).length,
  };
}

describeWithDb('scheduler (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let service: ScheduleService;
  const locks: PgAdvisoryLeaderLock[] = [];
  const runtimes: SchedulerRuntime[] = [];

  beforeAll(async () => {
    db = await createTestDatabase();
    service = new ScheduleService(new QueueRepository(db.pool), new ScheduleRepository(db.pool), {
      maxPayloadBytes: 10_000,
    });
  });

  afterEach(async () => {
    await Promise.all(runtimes.splice(0).map((r) => r.stop()));
    await Promise.all(locks.splice(0).map((l) => l.release()));
    // Each test starts from a clean slate of schedules and jobs.
    await db.pool.query('DELETE FROM schedules');
    await db.pool.query('DELETE FROM jobs');
  });

  afterAll(async () => {
    await db?.destroy();
  });

  function newLock(logger = capture().logger): PgAdvisoryLeaderLock {
    const lock = new PgAdvisoryLeaderLock({
      connectionString: db.url,
      logger,
      connectionTimeoutMs: 5_000,
      statementTimeoutMs: 10_000,
      options: `-c search_path=${db.schema}`,
    });
    locks.push(lock);
    return lock;
  }

  const tickOptions = (overrides: Partial<TickOptions> = {}): TickOptions => ({
    batchSize: 100,
    graceMs: 60_000,
    maxCatchUp: 10,
    logger: capture().logger,
    ...overrides,
  });

  async function createSchedule(
    name: string,
    opts: { cron?: string; misfirePolicy?: MisfirePolicy; queue?: string } = {},
  ) {
    return service.create({
      queue: (opts.queue ?? 'sched') as QueueName,
      name,
      cron: opts.cron ?? '* * * * *',
      payload: { schedule: name },
      ...(opts.misfirePolicy ? { misfirePolicy: opts.misfirePolicy } : {}),
    });
  }

  /** Make a schedule due by moving next_run_at to a SQL expression (evaluated now). */
  async function setNextRun(id: string, sqlExpr: string): Promise<Date> {
    const { rows } = await db.pool.query<{ next_run_at: Date }>(
      `UPDATE schedules SET next_run_at = ${sqlExpr} WHERE id = $1 RETURNING next_run_at`,
      [id],
    );
    const at = rows[0]?.next_run_at;
    if (!at) throw new Error('schedule not found');
    return at;
  }

  async function jobsFor(scheduleId: string) {
    const { rows } = await db.pool.query<{
      id: string;
      status: string;
      idempotency_key: string;
      payload: unknown;
    }>(
      `SELECT id, status, idempotency_key, payload FROM jobs
       WHERE idempotency_key LIKE $1 ORDER BY idempotency_key`,
      [`schedule:${scheduleId}:%`],
    );
    return rows;
  }

  async function withClient<T>(fn: (c: pg.PoolClient) => Promise<T>): Promise<T> {
    const client = await db.pool.connect();
    try {
      return await fn(client);
    } finally {
      client.release();
    }
  }

  describe('leader election (advisory lock)', () => {
    it('only one of two contenders becomes leader; release hands over', async () => {
      const a = newLock();
      const b = newLock();
      expect(await a.tryAcquire()).toBe(true);
      expect(await b.tryAcquire()).toBe(false);
      expect(await a.tryAcquire()).toBe(true); // idempotent while held
      expect(b.held).toBe(false);

      await a.release();
      expect(a.held).toBe(false);
      expect(await b.tryAcquire()).toBe(true);
      expect(await a.tryAcquire()).toBe(false);
    });

    it('a crashed leader (session killed) loses the lock and a follower takes over', async () => {
      const a = newLock();
      const b = newLock();
      expect(await a.tryAcquire()).toBe(true);
      const pid = await a.withSession(async (s) => {
        const { rows } = await s.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
        return rows[0]?.pid;
      });
      await db.pool.query('SELECT pg_terminate_backend($1)', [pid]);

      await expect.poll(() => b.tryAcquire(), { timeout: 5_000 }).toBe(true);
      // The old leader notices on its next use of the session and is fenced.
      await expect(a.withSession((s) => s.query('SELECT 1'))).rejects.toThrow();
      expect(a.held).toBe(false);
    });

    it('elects separately per schema (the key includes current_schema())', async () => {
      const other = await createTestDatabase({ migrate: false });
      try {
        const here = newLock();
        const there = new PgAdvisoryLeaderLock({
          connectionString: other.url,
          logger: capture().logger,
          connectionTimeoutMs: 5_000,
          statementTimeoutMs: 10_000,
          options: `-c search_path=${other.schema}`,
        });
        locks.push(there);
        expect(await here.tryAcquire()).toBe(true);
        expect(await there.tryAcquire()).toBe(true);
      } finally {
        await other.destroy();
      }
    });
  });

  describe('tick', () => {
    it('enqueues one job per due occurrence and advances the schedule atomically', async () => {
      const s = await createSchedule('every-minute');
      const due = await setNextRun(s.id, `date_trunc('minute', now())`);
      const log = capture();

      const lock = newLock();
      await lock.tryAcquire();
      const result = await lock.withSession((c) => runTick(c, tickOptions({ logger: log.logger })));
      expect(result).toMatchObject({ claimed: 1, fired: 1, duplicates: 0, errors: 0 });

      const jobs = await jobsFor(s.id);
      expect(jobs).toHaveLength(1);
      expect(jobs[0]).toMatchObject({
        status: 'queued',
        idempotency_key: scheduleJobIdempotencyKey(s.id, due),
        payload: { schedule: 'every-minute' },
      });

      const after = await service.get(s.id);
      expect(after.fireCount).toBe(1);
      expect(after.lastRunAt).toEqual(due);
      expect(after.lastJobId).toBe(jobs[0]?.id);
      expect(after.nextRunAt?.getTime()).toBe(due.getTime() + 60_000);
      expect(log.lines.find((l) => l.event === 'scheduler.fired')).toMatchObject({
        scheduleId: s.id,
        queue: 'sched',
        jobId: jobs[0]?.id,
        scheduledFor: due.toISOString(),
      });
    });

    it('does nothing for schedules that are not due or are paused', async () => {
      const future = await createSchedule('future');
      const paused = await createSchedule('paused');
      await setNextRun(paused.id, `date_trunc('minute', now())`);
      await service.pause(paused.id);

      const result = await withClient((c) => runTick(c, tickOptions()));
      expect(result.claimed).toBe(0);
      expect(await jobsFor(future.id)).toHaveLength(0);
      expect(await jobsFor(paused.id)).toHaveLength(0);
    });

    it('re-evaluating an already fired occurrence is suppressed by the idempotency key', async () => {
      const s = await createSchedule('replayed');
      await setNextRun(s.id, `date_trunc('minute', now())`);
      await withClient((c) => runTick(c, tickOptions()));

      // Simulate a lost update: next_run_at points at the fired occurrence again.
      await setNextRun(s.id, `date_trunc('minute', now())`);
      const log = capture();
      const result = await withClient((c) => runTick(c, tickOptions({ logger: log.logger })));
      expect(result).toMatchObject({ claimed: 1, fired: 0, duplicates: 1 });
      expect(await jobsFor(s.id)).toHaveLength(1);
      expect((await service.get(s.id)).fireCount).toBe(1);
      expect(log.count('scheduler.duplicate_suppressed')).toBe(1);
    });

    it('concurrent ticks never enqueue an occurrence twice (SKIP LOCKED + key)', async () => {
      const schedules = [];
      for (let i = 0; i < 20; i++) schedules.push(await createSchedule(`c${i}`));
      for (const s of schedules) await setNextRun(s.id, `date_trunc('minute', now())`);

      const results = await Promise.all(
        Array.from({ length: 4 }, () => withClient((c) => runTick(c, tickOptions()))),
      );
      expect(results.reduce((n, r) => n + r.fired, 0)).toBe(20);
      expect(results.reduce((n, r) => n + r.claimed, 0)).toBe(20);
      const { rows } = await db.pool.query<{ n: string; distinct_keys: string }>(
        `SELECT count(*) AS n, count(DISTINCT idempotency_key) AS distinct_keys
         FROM jobs WHERE idempotency_key LIKE 'schedule:%'`,
      );
      expect(rows[0]).toEqual({ n: '20', distinct_keys: '20' });
    });

    it.each<[MisfirePolicy, number]>([
      ['skip', 0],
      ['fire_once', 1],
      ['catch_up', 3],
    ])('misfire policy %s after a 5 h outage fires %i job(s)', async (policy, expected) => {
      const s = await createSchedule(`hourly-${policy}`, {
        cron: '0 * * * *',
        misfirePolicy: policy,
      });
      // Missed occurrences: 5 h ago .. the top of the current hour (6 in total).
      await setNextRun(s.id, `date_trunc('hour', now()) - interval '5 hours'`);
      const { rows } = await db.pool.query<{ top: Date }>(
        `SELECT date_trunc('hour', now()) AS top`,
      );
      const top = rows[0]?.top ?? new Date();

      const log = capture();
      const result = await withClient((c) =>
        runTick(c, tickOptions({ graceMs: 1_000, maxCatchUp: 3, logger: log.logger })),
      );
      expect(result.fired).toBe(expected);
      // `skipped` is a lower bound: the walk stops once the policy is decided.
      expect(result.skipped > 0).toBe(expected < 6);

      const jobs = await jobsFor(s.id);
      const fired = jobs.map((j) => j.idempotency_key.split(':').slice(2).join(':')).sort();
      const expectedTimes = Array.from({ length: expected }, (_, i) =>
        new Date(top.getTime() - (expected - 1 - i) * 3_600_000).toISOString(),
      );
      expect(fired).toEqual(expectedTimes);

      const after = await service.get(s.id);
      expect(after.nextRunAt?.getTime()).toBe(top.getTime() + 3_600_000);
      expect(after.fireCount).toBe(expected);
      if (expected < 6) expect(log.count('scheduler.misfire_skipped')).toBe(1);
    });

    it('isolates a broken schedule: others still fire, the error is recorded', async () => {
      const good = await createSchedule('good');
      const bad = await createSchedule('bad');
      await setNextRun(good.id, `date_trunc('minute', now())`);
      await setNextRun(bad.id, `date_trunc('minute', now())`);
      // Bypass API validation, as if the definition were corrupted.
      await db.pool.query(`UPDATE schedules SET cron_expression = 'garbage!' WHERE id = $1`, [
        bad.id,
      ]);

      const log = capture();
      const before = Date.now();
      const result = await withClient((c) => runTick(c, tickOptions({ logger: log.logger })));
      expect(result).toMatchObject({ claimed: 2, fired: 1, errors: 1 });
      expect(await jobsFor(good.id)).toHaveLength(1);
      expect(await jobsFor(bad.id)).toHaveLength(0);

      const failed = await service.get(bad.id);
      expect(failed.lastError).toMatch(/Invalid cron expression/);
      expect(failed.enabled).toBe(true);
      expect(failed.nextRunAt?.getTime()).toBeGreaterThan(before + 50_000);
      expect(log.count('scheduler.schedule_error')).toBe(1);

      // The next successful evaluation clears the error.
      expect((await service.get(good.id)).lastError).toBeNull();
    });

    it('a failed tick rolls back everything it did', async () => {
      const s = await createSchedule('rollback');
      await setNextRun(s.id, `date_trunc('minute', now())`);
      await withClient(async (c) => {
        // A tick-level failure (here: the connection dropping) after the job
        // was inserted, while advancing the schedule.
        const flaky: Queryable = {
          query: ((text: string, values?: unknown[]) =>
            text.includes('UPDATE schedules')
              ? Promise.reject(new Error('Connection terminated unexpectedly'))
              : c.query(text, values)) as Queryable['query'],
        };
        await expect(runTick(flaky, tickOptions())).rejects.toThrow(/Database unavailable/);
      });
      expect(await jobsFor(s.id)).toHaveLength(0);
      expect((await service.get(s.id)).fireCount).toBe(0);
    });
  });

  describe('Scheduler runtime: two replicas', () => {
    function startReplica(id: string, lock?: PgAdvisoryLeaderLock) {
      const log = capture();
      const env = loadSchedulerEnv({
        DATABASE_URL: db.url,
        SCHEDULER_ID: id,
        SCHEDULER_TICK_INTERVAL_MS: '100',
        SCHEDULER_LEADER_RETRY_INTERVAL_MS: '150',
        SCHEDULER_MISFIRE_GRACE_MS: '60000',
        SCHEDULER_SHUTDOWN_TIMEOUT_MS: '2000',
      });
      const runtime = createSchedulerRuntime(env, {
        logger: log.logger,
        pgOptions: `-c search_path=${db.schema}`,
        ...(lock ? { lock } : {}),
      });
      runtimes.push(runtime);
      runtime.start();
      return { runtime, log };
    }

    it('exactly one replica leads, fires without duplicates, and fails over on stop', async () => {
      const a = startReplica('sched-a');
      const b = startReplica('sched-b');
      await expect
        .poll(
          () => a.log.count('scheduler.leader_acquired') + b.log.count('scheduler.leader_acquired'),
        )
        .toBe(1);
      // Give the follower several retry rounds to (wrongly) win.
      await new Promise((r) => setTimeout(r, 600));
      expect(
        a.log.count('scheduler.leader_acquired') + b.log.count('scheduler.leader_acquired'),
      ).toBe(1);
      const [leader, follower] = a.runtime.scheduler.role === 'leader' ? [a, b] : [b, a];
      expect(follower.runtime.scheduler.role).toBe('follower');

      const first = await createSchedule('replicated');
      await setNextRun(first.id, `date_trunc('minute', now())`);
      await expect.poll(async () => (await jobsFor(first.id)).length).toBe(1);
      expect(follower.log.count('scheduler.fired')).toBe(0);

      await leader.runtime.stop();
      expect(leader.log.count('scheduler.leader_released')).toBe(1);
      await expect.poll(() => follower.runtime.scheduler.role, { timeout: 5_000 }).toBe('leader');

      const second = await createSchedule('after-failover');
      await setNextRun(second.id, `date_trunc('minute', now())`);
      await expect.poll(async () => (await jobsFor(second.id)).length).toBe(1);
      await new Promise((r) => setTimeout(r, 300));
      expect(await jobsFor(first.id)).toHaveLength(1);
      expect(await jobsFor(second.id)).toHaveLength(1);
    });

    it('recovers leadership when the leader session is killed', async () => {
      const lockA = newLock();
      const a = startReplica('crash-a', lockA);
      await expect.poll(() => a.runtime.scheduler.role).toBe('leader');
      const b = startReplica('crash-b');
      await new Promise((r) => setTimeout(r, 300));
      expect(b.runtime.scheduler.role).toBe('follower');

      const pid = await lockA.withSession(async (s) => {
        const { rows } = await s.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
        return rows[0]?.pid;
      });
      await db.pool.query('SELECT pg_terminate_backend($1)', [pid]);

      // A notices, steps down, and the lock is re-contended: either replica
      // may win (A's process is still healthy), but never both.
      await expect.poll(() => a.log.count('scheduler.leader_lost'), { timeout: 5_000 }).toBe(1);
      const acquired = () =>
        a.log.count('scheduler.leader_acquired') + b.log.count('scheduler.leader_acquired');
      await expect.poll(acquired, { timeout: 5_000 }).toBe(2);
      await new Promise((r) => setTimeout(r, 300));
      const roles = [a.runtime.scheduler.role, b.runtime.scheduler.role].sort();
      expect(roles).toEqual(['follower', 'leader']);

      // The new leader schedules normally.
      const s = await createSchedule('after-crash');
      await setNextRun(s.id, `date_trunc('minute', now())`);
      await expect.poll(async () => (await jobsFor(s.id)).length).toBe(1);
    });
  });
});
