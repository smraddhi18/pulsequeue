import type { QueueRef, QueueSignals } from '../../src/domain/signals.js';

/** QueueSignals that records calls, for asserting which changes notify what. */
export class RecordingSignals implements QueueSignals {
  readonly calls: Array<{ kind: 'runnable' | 'changed'; queue: string; tenantId: string }> = [];

  jobsRunnable(ref: QueueRef): void {
    this.calls.push({ kind: 'runnable', queue: ref.name, tenantId: ref.tenantId });
  }

  jobsChanged(ref: QueueRef): void {
    this.calls.push({ kind: 'changed', queue: ref.name, tenantId: ref.tenantId });
  }

  /** Queue names signalled runnable, in call order. */
  runnable(): string[] {
    return this.calls.filter((c) => c.kind === 'runnable').map((c) => c.queue);
  }

  /** Queue names signalled changed, in call order. */
  changed(): string[] {
    return this.calls.filter((c) => c.kind === 'changed').map((c) => c.queue);
  }

  /** `tenant:queue` of every call, in call order. */
  refs(): string[] {
    return this.calls.map((c) => `${c.kind}:${c.tenantId}:${c.queue}`);
  }

  reset(): void {
    this.calls.length = 0;
  }
}
