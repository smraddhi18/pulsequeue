import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, expect, it } from 'vitest';
import type { QueueName } from '../../src/domain/job.js';
import { QueueRepository } from '../../src/repositories/queue.repository.js';
import { ScheduleRepository } from '../../src/repositories/schedule.repository.js';
import { ScheduleService } from '../../src/services/schedule.service.js';
import { createTestDatabase, describeWithDb, type TestDatabase } from './helpers.js';

const ROOT = fileURLToPath(new URL('../../', import.meta.url));
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

interface Replica {
  id: string;
  child: ChildProcess;
  output: string[];
  exited: Promise<number | null>;
  events(): string[];
}

/**
 * End-to-end: two real `src/scheduler.ts` processes. Proves the signal wiring
 * and leader election across OS processes: exactly one leads, SIGTERM makes it
 * release leadership and exit 0, and the standby takes over.
 */
describeWithDb('scheduler process (Postgres)', { timeout: 40_000 }, () => {
  let db: TestDatabase;
  const replicas: Replica[] = [];

  beforeAll(async () => {
    db = await createTestDatabase();
  });

  afterAll(async () => {
    for (const r of replicas) r.child.kill('SIGKILL');
    await db?.destroy();
  });

  function spawnReplica(id: string): Replica {
    // Point the child at this suite's schema through libpq startup options.
    const url = new URL(db.url);
    url.searchParams.set('options', `-c search_path=${db.schema}`);
    const output: string[] = [];
    const child = spawn(process.execPath, ['--import', 'tsx', 'src/scheduler.ts'], {
      cwd: ROOT,
      env: {
        ...process.env,
        NODE_ENV: 'test',
        LOG_LEVEL: 'info',
        DATABASE_URL: url.toString(),
        SCHEDULER_ID: id,
        SCHEDULER_TICK_INTERVAL_MS: '100',
        SCHEDULER_LEADER_RETRY_INTERVAL_MS: '200',
        SCHEDULER_SHUTDOWN_TIMEOUT_MS: '3000',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout?.on('data', (d: Buffer) => output.push(d.toString()));
    child.stderr?.on('data', (d: Buffer) => output.push(d.toString()));
    const exited = new Promise<number | null>((resolve) => child.on('exit', (c) => resolve(c)));
    const replica: Replica = {
      id,
      child,
      output,
      exited,
      events: () =>
        output
          .join('')
          .split('\n')
          .flatMap((line) => {
            try {
              const parsed = JSON.parse(line) as { event?: string };
              return parsed.event ? [parsed.event] : [];
            } catch {
              return [];
            }
          }),
    };
    replicas.push(replica);
    return replica;
  }

  async function waitFor(what: string, cond: () => boolean, timeoutMs = 15_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!cond()) {
      if (Date.now() > deadline) {
        const logs = replicas.map((r) => `--- ${r.id}\n${r.output.join('')}`).join('\n');
        throw new Error(`timed out waiting for ${what}\n${logs}`);
      }
      await sleep(50);
    }
  }

  it('elects one leader, hands over on SIGTERM, and never double-fires', async () => {
    const schedules = new ScheduleService(
      new QueueRepository(db.pool),
      new ScheduleRepository(db.pool),
      { maxPayloadBytes: 10_000 },
    );
    // Minute resolution: instead of waiting for the wall clock, make a
    // schedule due by moving next_run_at to the current minute.
    const create = (name: string) =>
      schedules.create({
        queue: 'reports' as QueueName,
        name,
        cron: '* * * * *',
        payload: { name },
      });
    const makeDue = (id: string) =>
      db.pool.query(
        `UPDATE schedules SET next_run_at = date_trunc('minute', now()) WHERE id = $1`,
        [id],
      );
    const jobsFor = async (id: string) =>
      (
        await db.pool.query<{ key: string }>(
          'SELECT idempotency_key AS key FROM jobs WHERE idempotency_key LIKE $1',
          [`schedule:${id}:%`],
        )
      ).rows.map((r) => r.key);
    const before = await create('before-handover');
    const after = await create('after-handover');
    // Park both until the test makes them due.
    await db.pool.query(`UPDATE schedules SET next_run_at = now() + interval '1 hour'`);

    const a = spawnReplica('proc-a');
    await waitFor('a to lead', () => a.events().includes('scheduler.leader_acquired'));
    const b = spawnReplica('proc-b');
    await waitFor('b to start', () => b.events().includes('scheduler.started'));
    await sleep(1_000);
    expect(b.events()).not.toContain('scheduler.leader_acquired');
    await makeDue(before.id);
    await waitFor('a to fire', () => a.events().includes('scheduler.fired'));
    expect(b.events()).not.toContain('scheduler.fired');

    a.child.kill('SIGTERM');
    const code = await a.exited;
    expect(code, a.output.join('')).toBe(0);
    expect(a.events()).toContain('scheduler.leader_released');
    expect(a.events()).toContain('scheduler.stopped');

    await waitFor('b to take over', () => b.events().includes('scheduler.leader_acquired'));
    await makeDue(after.id);
    await waitFor('b to fire', () => b.events().includes('scheduler.fired'));

    // The first job came from A, the second from B (a minute rollover during
    // the test may legitimately add one more occurrence, never a duplicate).
    const keys = [...(await jobsFor(before.id)), ...(await jobsFor(after.id))];
    expect(keys.length).toBeGreaterThanOrEqual(2);
    expect(new Set(keys).size).toBe(keys.length);
    expect([...a.events(), ...b.events()]).not.toContain('scheduler.duplicate_suppressed');

    b.child.kill('SIGTERM');
    expect(await b.exited, b.output.join('')).toBe(0);
  });
});
