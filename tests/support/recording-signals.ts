import type { QueueSignals } from '../../src/domain/signals.js';

/** QueueSignals that records calls, for asserting which changes notify what. */
export class RecordingSignals implements QueueSignals {
  readonly calls: Array<{ kind: 'runnable' | 'changed'; queue: string }> = [];

  jobsRunnable(queue: string): void {
    this.calls.push({ kind: 'runnable', queue });
  }

  jobsChanged(queue: string): void {
    this.calls.push({ kind: 'changed', queue });
  }

  runnable(): string[] {
    return this.calls.filter((c) => c.kind === 'runnable').map((c) => c.queue);
  }

  changed(): string[] {
    return this.calls.filter((c) => c.kind === 'changed').map((c) => c.queue);
  }

  reset(): void {
    this.calls.length = 0;
  }
}
