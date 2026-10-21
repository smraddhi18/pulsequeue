import { afterEach, describe, expect, it } from 'vitest';
import type { JobId, QueueName } from '../../src/domain/job.js';
import type { ReclaimedJob } from '../../src/domain/lease.js';
import { HandlerRegistry, type JobHandler } from '../../src/worker/registry.js';
import { NonRetryableError } from '../../src/worker/errors.js';
import {
  Worker,
  runUntilAborted,
  type JobFailedEvent,
  type WorkerOptions,
} from '../../src/worker/worker.js';
import { Reaper } from '../../src/worker/reaper.js';
import { FakeLeaseStore, silentLogger, waitFor } from './fake-store.js';

const workers: Worker[] = [];

function makeWorker(
  store: FakeLeaseStore,
  handlers: Record<string, JobHandler>,
  overrides: Partial<WorkerOptions> = {},
): Worker {
  const registry = new HandlerRegistry();
  for (const [queue, handler] of Object.entries(handlers)) registry.register(queue, handler);
  const worker = new Worker({
    store,
    registry,
    logger: silentLogger,
    workerId: 'w-test',
    queues: registry.queues(),
    concurrency: 2,
    pollIntervalMs: 10,
    leaseDurationMs: 1_000,
    heartbeatIntervalMs: 20,
    jobTimeoutMs: 2_000,
    backoff: { baseMs: 100, maxMs: 1_000 },
    random: () => 0.5,
    ...overrides,
  });
  workers.push(worker);
  return worker;
}

afterEach(async () => {
  await Promise.all(workers.splice(0).map((w) => w.stop({ timeoutMs: 100 })));
});

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe('Worker', () => {
  it('runs leased jobs and marks them succeeded', async () => {
    const store = new FakeLeaseStore();
    const seen: unknown[] = [];
    const jobs = [store.add('q', { n: 1 }), store.add('q', { n: 2 }), store.add('q', { n: 3 })];
    const worker = makeWorker(store, {
      q: async (ctx) => {
        seen.push(ctx.job.payload);
      },
    });
    worker.start();
    await waitFor(() => jobs.every((j) => j.status === 'succeeded'));
    expect(seen).toHaveLength(3);
    expect(jobs.map((j) => j.attemptStatuses)).toEqual([
      ['succeeded'],
      ['succeeded'],
      ['succeeded'],
    ]);
  });

  it('never runs more than `concurrency` jobs at once and asks only for free slots', async () => {
    const store = new FakeLeaseStore();
    for (let i = 0; i < 10; i++) store.add('q');
    let running = 0;
    let peak = 0;
    const worker = makeWorker(
      store,
      {
        q: async () => {
          running++;
          peak = Math.max(peak, running);
          await sleep(15);
          running--;
        },
      },
      { concurrency: 3 },
    );
    worker.start();
    await waitFor(() => [...store.jobs.values()].every((j) => j.status === 'succeeded'));
    expect(peak).toBeLessThanOrEqual(3);
    expect(store.maxConcurrentRunning).toBeLessThanOrEqual(3);
    expect(store.leaseCalls.every((c) => c.limit >= 1 && c.limit <= 3)).toBe(true);
  });

  it('only leases from its own queues', async () => {
    const store = new FakeLeaseStore();
    const other = store.add('other');
    const mine = store.add('mine');
    const worker = makeWorker(store, { mine: async () => undefined });
    worker.start();
    await waitFor(() => mine.status === 'succeeded');
    expect(other.status).toBe('queued');
    expect(store.leaseCalls[0]?.queues).toEqual(['mine']);
  });

  it('schedules a retry with jittered backoff, then succeeds', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q', {}, 5);
    const worker = makeWorker(store, {
      q: async (ctx) => {
        if (ctx.job.attempt < 3) throw new Error(`boom ${ctx.job.attempt}`);
      },
    });
    worker.start();
    await waitFor(() => job.status === 'succeeded');
    expect(job.attempts).toBe(3);
    expect(job.attemptStatuses).toEqual(['failed', 'failed', 'succeeded']);
    expect(job.errors).toEqual(['boom 1', 'boom 2']);
    // random() = 0.5 -> half of the ceiling: 100ms * 2^(attempt-1) / 2
    expect(job.retryDelays).toEqual([50, 100]);
  });

  it('fails the job after max attempts and calls the dead-letter hook once', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q', {}, 2);
    const events: JobFailedEvent[] = [];
    const worker = makeWorker(
      store,
      {
        q: async () => {
          throw new Error('always');
        },
      },
      { onJobFailed: (e) => void events.push(e) },
    );
    worker.start();
    await waitFor(() => job.status === 'failed');
    expect(job.attemptStatuses).toEqual(['failed', 'failed']);
    expect(events).toEqual([
      expect.objectContaining({
        jobId: job.id,
        reason: 'attempts_exhausted',
        attempt: 2,
        source: 'worker',
      }),
    ]);
  });

  it('does not retry a NonRetryableError', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q', {}, 5);
    const events: JobFailedEvent[] = [];
    const worker = makeWorker(
      store,
      {
        q: async () => {
          throw new NonRetryableError('bad payload');
        },
      },
      { onJobFailed: (e) => void events.push(e) },
    );
    worker.start();
    await waitFor(() => job.status === 'failed');
    expect(job.attempts).toBe(1);
    expect(job.errors).toEqual(['NonRetryableError: bad payload']);
    expect(events[0]?.reason).toBe('non_retryable');
  });

  it('survives a throwing dead-letter hook', async () => {
    const store = new FakeLeaseStore();
    const a = store.add('q', {}, 1);
    const worker = makeWorker(
      store,
      { q: async () => Promise.reject(new Error('x')) },
      {
        onJobFailed: () => {
          throw new Error('hook broke');
        },
      },
    );
    worker.start();
    await waitFor(() => a.status === 'failed');
    const b = store.add('q');
    await waitFor(() => b.status === 'failed');
  });

  it('times out a slow attempt, records timed_out, aborts the signal and retries', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q', {}, 2);
    let sawAbort = false;
    const worker = makeWorker(
      store,
      {
        q: async (ctx) => {
          if (ctx.job.attempt === 1) {
            await new Promise<void>((resolve) => {
              ctx.signal.addEventListener('abort', () => {
                sawAbort = true;
                resolve();
              });
            });
          }
        },
      },
      { jobTimeoutMs: 50 },
    );
    worker.start();
    await waitFor(() => job.status === 'succeeded');
    expect(sawAbort).toBe(true);
    expect(job.attemptStatuses).toEqual(['timed_out', 'succeeded']);
    expect(job.errors[0]).toMatch(/JobTimeoutError: Job attempt timed out after 50ms/);
  });

  it('heartbeats while a job runs', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q');
    const worker = makeWorker(store, { q: () => sleep(120) }, { heartbeatIntervalMs: 20 });
    worker.start();
    await waitFor(() => job.status === 'succeeded');
    expect(store.heartbeats).toBeGreaterThanOrEqual(3);
  });

  it('aborts the handler and writes nothing when a heartbeat finds the lease lost', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q');
    let aborted: unknown;
    const worker = makeWorker(store, {
      q: (ctx) =>
        new Promise<void>((resolve) => {
          ctx.signal.addEventListener('abort', () => {
            aborted = ctx.signal.reason;
            resolve();
          });
        }),
    });
    store.loseLeases = true;
    worker.start();
    await waitFor(() => aborted !== undefined);
    await waitFor(() => worker.activeJobs === 0);
    expect((aborted as Error).name).toBe('LeaseLostError');
    // Still "running" from the store's point of view: the reaper owns it now.
    expect(job.status).toBe('running');
    expect(job.attemptStatuses).toEqual(['running']);
  });

  it('keeps polling after lease errors (backs off, then recovers)', async () => {
    const store = new FakeLeaseStore();
    store.failLeases = 2;
    const job = store.add('q');
    const worker = makeWorker(store, { q: async () => undefined });
    worker.start();
    await waitFor(() => job.status === 'succeeded');
    expect(store.leaseCalls.length).toBeGreaterThanOrEqual(3);
  });

  it('graceful stop drains in-flight jobs and stops leasing', async () => {
    const store = new FakeLeaseStore();
    const first = store.add('q');
    let started = false;
    const worker = makeWorker(store, {
      q: async () => {
        started = true;
        await sleep(80);
      },
    });
    worker.start();
    await waitFor(() => started);
    const later = store.add('q');
    const result = await worker.stop({ timeoutMs: 2_000 });
    expect(result).toEqual({ drained: true, abandoned: 0 });
    expect(first.status).toBe('succeeded');
    await sleep(40);
    expect(later.status).toBe('queued');
    expect(worker.isRunning).toBe(false);
  });

  it('on drain timeout aborts jobs and releases their leases without burning an attempt', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q');
    let started = false;
    const worker = makeWorker(store, {
      q: async () => {
        started = true;
        await sleep(10_000); // ignores its signal on purpose
      },
    });
    worker.start();
    await waitFor(() => started);
    const t0 = Date.now();
    const result = await worker.stop({ timeoutMs: 50 });
    expect(Date.now() - t0).toBeLessThan(1_000);
    expect(result).toEqual({ drained: false, abandoned: 1 });
    expect(job.status).toBe('queued');
    expect(job.attempts).toBe(0);
    expect(job.attemptStatuses).toEqual(['released']);
  });

  it('releases jobs that were leased while stop() was in progress', async () => {
    const store = new FakeLeaseStore();
    const job = store.add('q');
    const worker = makeWorker(store, { q: async () => undefined });
    let stopping: Promise<unknown> | undefined;
    store.onLease = () => {
      stopping ??= worker.stop({ timeoutMs: 100 });
    };
    worker.start();
    await waitFor(() => stopping !== undefined);
    await stopping;
    expect(job.status).toBe('queued');
    expect(job.attemptStatuses).toEqual(['released']);
  });

  it('stop() is idempotent and start() cannot be called twice', async () => {
    const store = new FakeLeaseStore();
    const worker = makeWorker(store, { q: async () => undefined });
    worker.start();
    expect(() => worker.start()).toThrow(/already started/);
    const a = worker.stop({ timeoutMs: 10 });
    const b = worker.stop({ timeoutMs: 10 });
    expect(a).toBe(b);
    await a;
  });

  it('validates its options', () => {
    const store = new FakeLeaseStore();
    expect(() => makeWorker(store, { q: async () => undefined }, { queues: [] })).toThrow(/queue/);
    expect(() => makeWorker(store, { q: async () => undefined }, { concurrency: 0 })).toThrow(
      /concurrency/,
    );
    expect(() =>
      makeWorker(
        store,
        { q: async () => undefined },
        { leaseDurationMs: 100, heartbeatIntervalMs: 80 },
      ),
    ).toThrow(/heartbeat/);
  });
});

describe('runUntilAborted', () => {
  it('resolves with the work, rejects with the abort reason, and handles sync throws', async () => {
    const c1 = new AbortController();
    await expect(runUntilAborted(async () => undefined, c1.signal)).resolves.toBeUndefined();

    const c2 = new AbortController();
    const pending = runUntilAborted(() => new Promise(() => undefined), c2.signal);
    c2.abort(new Error('stop'));
    await expect(pending).rejects.toThrow('stop');

    const c3 = new AbortController();
    c3.abort(new Error('already'));
    await expect(runUntilAborted(async () => undefined, c3.signal)).rejects.toThrow('already');

    const c4 = new AbortController();
    await expect(
      runUntilAborted(() => {
        throw new Error('sync');
      }, c4.signal),
    ).rejects.toThrow('sync');
  });
});

describe('Reaper', () => {
  it('reaps in batches until a short batch, and reports exhausted jobs to the hook', async () => {
    const batches = [
      [
        {
          jobId: 'a' as JobId,
          queue: 'q' as QueueName,
          previousWorkerId: 'w1',
          attempt: 1,
          maxAttempts: 3,
          status: 'queued' as const,
        },
        {
          jobId: 'b' as JobId,
          queue: 'q' as QueueName,
          previousWorkerId: 'w1',
          attempt: 3,
          maxAttempts: 3,
          status: 'failed' as const,
        },
      ],
      [],
    ];
    let calls = 0;
    const events: JobFailedEvent[] = [];
    const reaper = new Reaper({
      store: { reapExpired: async () => (batches[calls++] ?? []) as ReclaimedJob[] },
      logger: silentLogger,
      intervalMs: 1_000,
      batchSize: 2,
      onJobFailed: (e) => void events.push(e),
    });
    const reclaimed = await reaper.tick();
    expect(reclaimed.map((r) => r.jobId)).toEqual(['a', 'b']);
    expect(calls).toBe(2);
    expect(events).toEqual([
      expect.objectContaining({ jobId: 'b', source: 'reaper', reason: 'attempts_exhausted' }),
    ]);
  });

  it('never throws from tick() and keeps running after errors until stopped', async () => {
    let calls = 0;
    const reaper = new Reaper({
      store: {
        reapExpired: async () => {
          calls++;
          throw new Error('db down');
        },
      },
      logger: silentLogger,
      intervalMs: 10,
    });
    await expect(reaper.tick()).resolves.toEqual([]);
    reaper.start();
    await waitFor(() => calls >= 3);
    await reaper.stop();
    const after = calls;
    await sleep(40);
    expect(calls).toBe(after);
  });
});
