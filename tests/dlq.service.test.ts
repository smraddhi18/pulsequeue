import { randomUUID } from 'node:crypto';
import { beforeEach, describe, expect, it } from 'vitest';
import type { DeadLetterReason, JobAttempt } from '../src/domain/dead-letter.js';
import { JobNotDeadLetteredError, NotFoundError, ValidationError } from '../src/domain/errors.js';
import type { JobId, Queue, QueueName } from '../src/domain/job.js';
import { decodeCursor } from '../src/lib/cursor.js';
import type {
  ListDeadLettersParams,
  PurgeOlderThanParams,
  ReplayBatchParams,
  ReplayParams,
  StoredDeadLetterJob,
} from '../src/repositories/dead-letter.repository.js';
import { DlqService, type DeadLetterStore } from '../src/services/dlq.service.js';
import { RecordingSignals } from './support/recording-signals.js';
import type { QueueStore } from '../src/services/job.service.js';

type Row = StoredDeadLetterJob & { queueId: string };

class FakeQueues implements QueueStore {
  readonly queues = new Map<string, Queue>();
  add(name: string): Queue {
    const queue: Queue = {
      id: String(this.queues.size + 1),
      name: name as QueueName,
      createdAt: new Date(),
    };
    this.queues.set(name, queue);
    return queue;
  }
  async ensure(): Promise<Queue> {
    throw new Error('DLQ must never create queues');
  }
  async findByName(name: QueueName): Promise<Queue | null> {
    return this.queues.get(name) ?? null;
  }
}

/** In-memory DeadLetterStore with the same guarded semantics (only status dead matches). */
class FakeDeadLetters implements DeadLetterStore {
  readonly rows = new Map<string, Row>();
  lastList: ListDeadLettersParams | undefined;
  lastReplay: ReplayParams | undefined;
  lastBatch: ReplayBatchParams | undefined;
  lastPurge: PurgeOlderThanParams | undefined;
  /** Simulate a concurrent replay landing between the service's read and its write. */
  stealNextReplay = false;
  private tick = 0;

  add(queue: Queue, overrides: Partial<Row> = {}): Row {
    const at = new Date(Date.UTC(2026, 9, 22, 0, 0, 0, this.tick++));
    const row: Row = {
      queueId: queue.id,
      id: randomUUID() as JobId,
      queue: queue.name,
      status: 'dead',
      payload: { n: this.tick },
      priority: 0,
      attempts: 3,
      maxAttempts: 3,
      runAt: at,
      idempotencyKey: null,
      lastError: 'boom',
      finishedAt: at,
      createdAt: at,
      updatedAt: at,
      requestFingerprint: null,
      createdAtCursor: at.toISOString().replace('Z', '000Z'),
      deadReason: 'attempts_exhausted',
      deadLetteredAt: at,
      deadLetteredAtCursor: at.toISOString().replace('Z', '000Z'),
      leaseExpiries: 0,
      replayCount: 0,
      lastReplayedAt: null,
      ...overrides,
    };
    this.rows.set(row.id, row);
    return row;
  }

  async findById(id: string): Promise<StoredDeadLetterJob | null> {
    return this.rows.get(id) ?? null;
  }

  async listByQueue(params: ListDeadLettersParams): Promise<StoredDeadLetterJob[]> {
    this.lastList = params;
    return [...this.rows.values()]
      .filter((r) => r.queueId === params.queueId && r.status === 'dead')
      .filter((r) => params.reason === undefined || r.deadReason === params.reason)
      .sort((a, b) => b.deadLetteredAt!.getTime() - a.deadLetteredAt!.getTime())
      .filter(
        (r) =>
          params.after === undefined ||
          r.deadLetteredAt!.getTime() < Date.parse(params.after.createdAt),
      )
      .slice(0, params.limit);
  }

  async listAttempts(): Promise<JobAttempt[]> {
    return [
      {
        attemptNumber: 1,
        status: 'failed',
        workerId: 'w1',
        error: 'boom',
        startedAt: new Date(),
        finishedAt: new Date(),
      },
    ];
  }

  private revive(row: Row, delayMs: number): void {
    row.status = delayMs > 0 ? 'scheduled' : 'queued';
    row.attempts = 0;
    row.deadReason = null;
    row.deadLetteredAt = null;
    row.deadLetteredAtCursor = null;
    row.replayCount++;
    row.lastReplayedAt = new Date();
  }

  async replay(params: ReplayParams): Promise<StoredDeadLetterJob | null> {
    this.lastReplay = params;
    const row = this.rows.get(params.jobId);
    if (row && this.stealNextReplay) {
      this.stealNextReplay = false;
      this.revive(row, 0); // the other caller won
      return null;
    }
    if (!row || row.queueId !== params.queueId || row.status !== 'dead') return null;
    this.revive(row, params.delayMs);
    return row;
  }

  async replayBatch(params: ReplayBatchParams): Promise<JobId[]> {
    this.lastBatch = params;
    const picked = [...this.rows.values()]
      .filter((r) => r.queueId === params.queueId && r.status === 'dead')
      .filter((r) => params.reason === undefined || r.deadReason === params.reason)
      .slice(0, params.limit);
    for (const row of picked) this.revive(row, params.delayMs);
    return picked.map((r) => r.id);
  }

  async purge(jobId: string, queueId: string): Promise<boolean> {
    const row = this.rows.get(jobId);
    if (!row || row.queueId !== queueId || row.status !== 'dead') return false;
    this.rows.delete(jobId);
    return true;
  }

  async purgeOlderThan(params: PurgeOlderThanParams): Promise<number> {
    this.lastPurge = params;
    let purged = 0;
    for (const row of this.rows.values()) {
      if (purged >= params.limit) break;
      if (
        row.queueId === params.queueId &&
        row.status === 'dead' &&
        row.deadLetteredAt! < params.olderThan
      ) {
        this.rows.delete(row.id);
        purged++;
      }
    }
    return purged;
  }
}

describe('DlqService', () => {
  let queues: FakeQueues;
  let store: FakeDeadLetters;
  let service: DlqService;
  let q: Queue;

  beforeEach(() => {
    queues = new FakeQueues();
    store = new FakeDeadLetters();
    service = new DlqService(queues, store);
    q = queues.add('emails');
  });

  describe('list', () => {
    it('pages newest-first with an opaque cursor over (dead_lettered_at, id)', async () => {
      const rows = Array.from({ length: 5 }, () => store.add(q));
      const first = await service.list({ queue: q.name, limit: 2 });
      expect(first.jobs.map((j) => j.id)).toEqual([rows[4]!.id, rows[3]!.id]);
      expect(store.lastList?.limit).toBe(3); // one extra row to detect another page
      expect(first.nextCursor).not.toBeNull();
      expect(decodeCursor(first.nextCursor!)).toEqual({
        createdAt: rows[3]!.deadLetteredAtCursor,
        id: rows[3]!.id,
      });

      const second = await service.list({ queue: q.name, limit: 2, cursor: first.nextCursor! });
      expect(second.jobs.map((j) => j.id)).toEqual([rows[2]!.id, rows[1]!.id]);
      const third = await service.list({ queue: q.name, limit: 2, cursor: second.nextCursor! });
      expect(third.jobs.map((j) => j.id)).toEqual([rows[0]!.id]);
      expect(third.nextCursor).toBeNull();
    });

    it('shapes entries with DLQ metadata and passes the reason filter through', async () => {
      store.add(q, { deadReason: 'invalid_payload', attempts: 1, replayCount: 2 });
      const page = await service.list({ queue: q.name, limit: 10, reason: 'invalid_payload' });
      expect(store.lastList?.reason).toBe('invalid_payload');
      expect(page.jobs[0]).toMatchObject({
        status: 'dead',
        deadLetter: { reason: 'invalid_payload', attempts: 1, queue: 'emails', replayCount: 2 },
      });
      expect(page.jobs[0]).not.toHaveProperty('requestFingerprint');
    });

    it('rejects a malformed cursor before touching the store, and 404s unknown queues', async () => {
      await expect(
        service.list({ queue: q.name, limit: 1, cursor: 'nope!' }),
      ).rejects.toBeInstanceOf(ValidationError);
      await expect(
        service.list({ queue: 'missing' as QueueName, limit: 1 }),
      ).rejects.toBeInstanceOf(NotFoundError);
    });
  });

  describe('get', () => {
    it('returns a dead job with its attempt history', async () => {
      const row = store.add(q);
      const job = await service.get(row.id);
      expect(job.deadLetter.reason).toBe('attempts_exhausted');
      expect(job.attemptHistory).toHaveLength(1);
    });

    it('404s a job that is not dead-lettered (and says what it is)', async () => {
      const row = store.add(q, { status: 'queued', deadReason: null, deadLetteredAt: null });
      const err = await service.get(row.id).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(NotFoundError);
      expect((err as NotFoundError).details).toMatchObject({ status: 'queued' });
      await expect(service.get(randomUUID())).rejects.toBeInstanceOf(NotFoundError);
    });
  });

  describe('replay', () => {
    it('replays into the same queue with a fresh budget and reports the previous reason', async () => {
      const row = store.add(q, { deadReason: 'non_retryable' });
      const result = await service.replay({ queue: q.name, jobId: row.id, delayMs: 5_000 });
      expect(store.lastReplay).toEqual({ jobId: row.id, queueId: q.id, delayMs: 5_000 });
      expect(result).toMatchObject({
        job: { id: row.id, status: 'scheduled', attempts: 0 },
        replayCount: 1,
        previousReason: 'non_retryable',
      });
    });

    it('defaults to an immediate replay', async () => {
      const row = store.add(q);
      expect((await service.replay({ queue: q.name, jobId: row.id })).job.status).toBe('queued');
    });

    it('409s a job that is not in the DLQ, 404s unknown jobs and jobs of another queue', async () => {
      const live = store.add(q, { status: 'running', deadReason: null, deadLetteredAt: null });
      await expect(service.replay({ queue: q.name, jobId: live.id })).rejects.toBeInstanceOf(
        JobNotDeadLetteredError,
      );
      await expect(
        service.replay({ queue: q.name, jobId: randomUUID() as JobId }),
      ).rejects.toBeInstanceOf(NotFoundError);
      const other = queues.add('reports');
      const foreign = store.add(other);
      await expect(service.replay({ queue: q.name, jobId: foreign.id })).rejects.toBeInstanceOf(
        NotFoundError,
      );
      expect(foreign.status).toBe('dead');
    });

    it('409s when a concurrent replay wins between the read and the guarded write', async () => {
      const row = store.add(q);
      store.stealNextReplay = true;
      const err = await service.replay({ queue: q.name, jobId: row.id }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(JobNotDeadLetteredError);
      expect((err as JobNotDeadLetteredError).statusCode).toBe(409);
      expect((err as JobNotDeadLetteredError).details).toMatchObject({ status: 'queued' });
      expect(row.replayCount).toBe(1); // replayed once, by the winner
    });
  });

  describe('bulk replay', () => {
    it('replays up to limit with the reason filter and reports the ids', async () => {
      const reasons: DeadLetterReason[] = [
        'invalid_payload',
        'attempts_exhausted',
        'invalid_payload',
      ];
      const rows = reasons.map((deadReason) => store.add(q, { deadReason }));
      const result = await service.replayBatch({
        queue: q.name,
        limit: 10,
        reason: 'invalid_payload',
      });
      expect(store.lastBatch).toMatchObject({ queueId: q.id, limit: 10, delayMs: 0 });
      expect(result.replayed).toBe(2);
      expect(new Set(result.jobIds)).toEqual(new Set([rows[0]!.id, rows[2]!.id]));
      expect(rows[1]!.status).toBe('dead');
    });
  });

  describe('purge', () => {
    it('deletes one dead job; 409 if not dead; 404 if unknown or in another queue', async () => {
      const dead = store.add(q);
      await service.purge(q.name, dead.id);
      expect(store.rows.has(dead.id)).toBe(false);

      const live = store.add(q, { status: 'queued', deadReason: null, deadLetteredAt: null });
      await expect(service.purge(q.name, live.id)).rejects.toBeInstanceOf(JobNotDeadLetteredError);
      await expect(service.purge(q.name, randomUUID() as JobId)).rejects.toBeInstanceOf(
        NotFoundError,
      );
      const foreign = store.add(queues.add('reports'));
      await expect(service.purge(q.name, foreign.id)).rejects.toBeInstanceOf(NotFoundError);
      expect(store.rows.has(foreign.id)).toBe(true);
    });

    it('bulk-purges with the cutoff and limit, and 404s unknown queues', async () => {
      store.add(q);
      store.add(q);
      const result = await service.purgeOlderThan({
        queue: q.name,
        olderThan: new Date('2100-01-01T00:00:00Z'),
        limit: 1,
      });
      expect(result).toEqual({ purged: 1 });
      expect(store.lastPurge).toMatchObject({ queueId: q.id, limit: 1 });
      await expect(
        service.purgeOlderThan({ queue: 'missing' as QueueName, olderThan: new Date(), limit: 1 }),
      ).rejects.toBeInstanceOf(NotFoundError);
    });
  });
});

describe('DlqService signals (Milestone 5)', () => {
  let queues: FakeQueues;
  let store: FakeDeadLetters;
  let signals: RecordingSignals;
  let service: DlqService;
  let q: Queue;

  beforeEach(() => {
    queues = new FakeQueues();
    store = new FakeDeadLetters();
    signals = new RecordingSignals();
    service = new DlqService(queues, store, { signals });
    q = queues.add('emails');
  });

  it('replay and a non-empty bulk replay make jobs runnable', async () => {
    const row = store.add(q);
    store.add(q);
    await service.replay({ queue: q.name, jobId: row.id });
    await service.replayBatch({ queue: q.name, limit: 10 });
    expect(signals.runnable()).toEqual(['emails', 'emails']);
    expect(signals.changed()).toEqual([]);
  });

  it('an empty bulk replay or a failed replay signals nothing', async () => {
    await service.replayBatch({ queue: q.name, limit: 10 });
    await expect(
      service.replay({ queue: q.name, jobId: randomUUID() as JobId }),
    ).rejects.toBeInstanceOf(NotFoundError);
    expect(signals.calls).toEqual([]);
  });

  it('purges only change stats (nothing became runnable)', async () => {
    const dead = store.add(q);
    store.add(q);
    await service.purge(q.name, dead.id);
    await service.purgeOlderThan({
      queue: q.name,
      olderThan: new Date('2100-01-01T00:00:00Z'),
      limit: 10,
    });
    await service.purgeOlderThan({
      queue: q.name,
      olderThan: new Date('2100-01-01T00:00:00Z'),
      limit: 10,
    });
    expect(signals.changed()).toEqual(['emails', 'emails']); // the empty purge is silent
    expect(signals.runnable()).toEqual([]);
  });
});
