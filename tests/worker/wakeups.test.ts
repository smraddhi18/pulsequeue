import { afterEach, describe, expect, it } from 'vitest';
import type { JobId, QueueName } from '../../src/domain/job.js';
import type { ReclaimedJob } from '../../src/domain/lease.js';
import { Reaper } from '../../src/worker/reaper.js';
import { HandlerRegistry, type JobHandler } from '../../src/worker/registry.js';
import { NonRetryableError } from '../../src/worker/errors.js';
import { Worker, type WorkerOptions } from '../../src/worker/worker.js';
import { RecordingSignals } from '../support/recording-signals.js';
import { FakeLeaseStore, silentLogger, waitFor } from './fake-store.js';
import { DEFAULT_TENANT_ID } from '../../src/domain/tenant.js';

/** FakeLeaseStore plus the optional nextDueInMs a push-mode worker uses. */
class DueAwareStore extends FakeLeaseStore {
  nextDue: number | null = null;
  failNextDue = false;
  nextDueCalls = 0;

  async nextDueInMs(): Promise<number | null> {
    this.nextDueCalls++;
    if (this.failNextDue) throw new Error('db down');
    return this.nextDue;
  }
}

const workers: Worker[] = [];
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function makeWorker(
  store: FakeLeaseStore,
  overrides: Partial<WorkerOptions> = {},
  handler: JobHandler = async () => undefined,
): Worker {
  const registry = new HandlerRegistry();
  registry.register('q', handler);
  const worker = new Worker({
    store,
    registry,
    logger: silentLogger,
    workerId: 'w-test',
    queues: ['q'],
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

describe('Worker wake-ups (Milestone 5)', () => {
  it('push mode: an idle worker stops polling and leases right after wake()', async () => {
    const store = new FakeLeaseStore();
    const worker = makeWorker(store, {
      wakeups: { isListening: true },
      fallbackPollIntervalMs: 10_000,
    });
    worker.start();
    await waitFor(() => store.leaseCalls.length === 1);
    await sleep(100);
    // Poll mode at 10 ms would have polled ~10 times by now.
    expect(store.leaseCalls).toHaveLength(1);
    expect(worker.isPushMode).toBe(true);

    const job = store.add('q');
    const woken = Date.now();
    worker.wake();
    await waitFor(() => job.status === 'succeeded');
    expect(Date.now() - woken).toBeLessThan(200);
  });

  it('poll mode while the wake-up source is not listening, and switches back and forth', async () => {
    const store = new FakeLeaseStore();
    const source = { isListening: false };
    const worker = makeWorker(store, { wakeups: source, fallbackPollIntervalMs: 10_000 });
    worker.start();
    await waitFor(() => store.leaseCalls.length >= 5);
    expect(worker.isPushMode).toBe(false);

    source.isListening = true;
    worker.wake(); // what the subscriber does on (re)subscribe
    await waitFor(() => worker.isPushMode);
    const calls = store.leaseCalls.length;
    await sleep(100);
    expect(store.leaseCalls.length).toBeLessThanOrEqual(calls + 1);

    source.isListening = false;
    worker.wake(); // what the subscriber does when the subscription is lost
    await waitFor(() => store.leaseCalls.length >= calls + 5);
    expect(worker.isPushMode).toBe(false);
  });

  it('a lost wake-up is caught by the fallback poll', async () => {
    const store = new FakeLeaseStore();
    const worker = makeWorker(store, {
      wakeups: { isListening: true },
      fallbackPollIntervalMs: 60,
    });
    worker.start();
    await waitFor(() => store.leaseCalls.length === 1);
    const job = store.add('q'); // no wake()
    await waitFor(() => job.status === 'succeeded', { timeoutMs: 1_000 });
    expect(store.leaseCalls.length).toBeGreaterThanOrEqual(2);
  });

  it('a wake-up during the lease query triggers an immediate re-poll', async () => {
    const store = new FakeLeaseStore();
    const worker = makeWorker(store, {
      wakeups: { isListening: true },
      fallbackPollIntervalMs: 10_000,
    });
    let job: ReturnType<FakeLeaseStore['add']> | undefined;
    store.onLease = () => {
      if (job) return;
      // Committed after this query's snapshot, wake-up delivered mid-query.
      worker.wake();
      job = store.add('q');
    };
    // Make the job invisible to the query that is already running.
    const lease = store.lease.bind(store);
    let first = true;
    store.lease = async (params) => {
      if (first) {
        first = false;
        store.onLease?.();
        return [];
      }
      return lease(params);
    };
    worker.start();
    await waitFor(() => job?.status === 'succeeded', { timeoutMs: 500 });
  });

  it('caps the push-mode sleep at the next scheduled job and never spins', async () => {
    const store = new DueAwareStore();
    store.nextDue = 40;
    const worker = makeWorker(store, {
      wakeups: { isListening: true },
      fallbackPollIntervalMs: 10_000,
    });
    worker.start();
    await sleep(220);
    // ~every 40 ms instead of every 10 s.
    expect(store.leaseCalls.length).toBeGreaterThanOrEqual(3);
    expect(store.leaseCalls.length).toBeLessThanOrEqual(8);

    store.nextDue = 0; // overdue: clamped to a small minimum, not a hot loop
    const before = store.leaseCalls.length;
    await sleep(100);
    expect(store.leaseCalls.length - before).toBeLessThanOrEqual(6);
  });

  it('does not query the next due time in poll mode, and survives it failing', async () => {
    const store = new DueAwareStore();
    const source = { isListening: false };
    const worker = makeWorker(store, { wakeups: source, fallbackPollIntervalMs: 50 });
    worker.start();
    await waitFor(() => store.leaseCalls.length >= 3);
    expect(store.nextDueCalls).toBe(0);

    store.failNextDue = true;
    source.isListening = true;
    worker.wake();
    const job = store.add('q');
    await waitFor(() => job.status === 'succeeded');
    await waitFor(() => store.nextDueCalls >= 2, { timeoutMs: 1_000 });
  });

  it('ignores wake() after stop and while busy', async () => {
    const store = new FakeLeaseStore();
    const worker = makeWorker(store, { concurrency: 1 }, () => sleep(30));
    store.add('q');
    worker.start();
    await waitFor(() => worker.activeJobs === 1);
    expect(() => worker.wake()).not.toThrow();
    await worker.stop({ timeoutMs: 500 });
    expect(() => worker.wake()).not.toThrow();
  });

  it('rejects a fallback poll interval below the poll interval', () => {
    expect(() =>
      makeWorker(new FakeLeaseStore(), { pollIntervalMs: 100, fallbackPollIntervalMs: 50 }),
    ).toThrow(/fallbackPollIntervalMs/);
  });
});

describe('Worker signals (Milestone 5)', () => {
  it('signals lease and completion as changes', async () => {
    const store = new FakeLeaseStore();
    const signals = new RecordingSignals();
    const job = store.add('q');
    makeWorker(store, { signals }).start();
    await waitFor(() => job.status === 'succeeded');
    await waitFor(() => signals.changed().length >= 2);
    expect(signals.changed()).toEqual(['q', 'q']); // leased, completed
    expect(signals.runnable()).toEqual([]);
  });

  it('signals a retry as runnable (it becomes due later) and a dead-letter as a change', async () => {
    const store = new FakeLeaseStore();
    const signals = new RecordingSignals();
    const retried = store.add('q', { kind: 'retry' });
    const dead = store.add('q', { kind: 'dead' });
    makeWorker(store, { signals, concurrency: 1 }, async (ctx) => {
      const { kind } = ctx.job.payload as { kind: string };
      if (kind === 'dead') throw new NonRetryableError('nope');
      if (ctx.job.attempt === 1) throw new Error('transient');
    }).start();
    await waitFor(() => retried.status === 'succeeded' && dead.status === 'dead');
    expect(signals.runnable()).toEqual(['q']);
    expect(signals.changed().length).toBeGreaterThanOrEqual(4);
  });

  it('signals released leases on shutdown as runnable', async () => {
    const store = new FakeLeaseStore();
    const signals = new RecordingSignals();
    const job = store.add('q');
    const worker = makeWorker(
      store,
      { signals },
      (ctx) =>
        new Promise((_, rej) => {
          ctx.signal.addEventListener('abort', () => rej(ctx.signal.reason));
        }),
    );
    worker.start();
    await waitFor(() => job.status === 'running');
    signals.reset();
    await worker.stop({ timeoutMs: 20 });
    expect(job.status).toBe('queued');
    expect(signals.runnable()).toEqual(['q']);
  });
});

describe('Worker signals for capped queues (Milestone 6)', () => {
  it('announces a finished capped job as runnable: a concurrency slot just freed', async () => {
    const store = new FakeLeaseStore();
    store.cappedQueues.add('q');
    const signals = new RecordingSignals();
    const ok = store.add('q');
    const dead = store.add('q', { kind: 'dead' });
    makeWorker(store, { signals, concurrency: 1 }, async (ctx) => {
      if ((ctx.job.payload as { kind?: string }).kind === 'dead') throw new NonRetryableError('x');
    }).start();
    await waitFor(() => ok.status === 'succeeded' && dead.status === 'dead');
    await waitFor(() => signals.runnable().length >= 2);
    // completed + dead-lettered; the leases themselves are only changes.
    expect(signals.runnable()).toEqual(['q', 'q']);
  });
});

describe('Reaper signals (Milestone 5)', () => {
  it('requeued jobs are runnable, dead-lettered ones only change stats', async () => {
    const signals = new RecordingSignals();
    const reclaimed: ReclaimedJob[] = [
      {
        jobId: 'a' as JobId,
        tenantId: DEFAULT_TENANT_ID,
        queue: 'emails' as QueueName,
        previousWorkerId: 'w1',
        attempt: 1,
        maxAttempts: 3,
        status: 'queued',
        deadLetterReason: null,
        leaseExpiries: 1,
      },
      {
        jobId: 'b' as JobId,
        tenantId: DEFAULT_TENANT_ID,
        queue: 'reports' as QueueName,
        previousWorkerId: 'w1',
        attempt: 3,
        maxAttempts: 3,
        status: 'dead',
        deadLetterReason: 'attempts_exhausted',
        leaseExpiries: 1,
      },
    ];
    let calls = 0;
    const reaper = new Reaper({
      store: { reapExpired: async () => (calls++ === 0 ? reclaimed : []) },
      logger: silentLogger,
      intervalMs: 1_000,
      batchSize: 10,
      signals,
    });
    await reaper.tick();
    expect(signals.runnable()).toEqual(['emails']);
    expect(signals.changed()).toEqual(['reports']);
  });
});
