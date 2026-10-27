import type { Logger } from '../lib/logger.js';
import { QUEUE_NAME_PATTERN } from '../domain/job.js';

/** What a handler sees for one attempt of one job. */
export interface JobContext<P = unknown> {
  job: {
    id: string;
    /**
     * Owner of the job's queue (M6). Workers are a shared fleet: one handler
     * for a queue name serves that name in every tenant.
     */
    tenantId: string;
    queue: string;
    payload: P;
    priority: number;
    /** 1-based counted attempt; equals maxAttempts on the last try. */
    attempt: number;
    maxAttempts: number;
    createdAt: Date;
  };
  /**
   * Aborted when the attempt times out, the lease is lost, or shutdown runs
   * out of drain time. Pass it to fetch / pg / child processes so the work
   * actually stops; the worker stops waiting either way.
   */
  signal: AbortSignal;
  /** Child logger already bound to jobId, queue, attempt and workerId. */
  logger: Logger;
}

/**
 * A job handler. Resolve = success; throw = failed attempt (retried with
 * backoff unless it is a NonRetryableError). Delivery is at-least-once, so a
 * handler must be idempotent: it can run again after a crash, a lost lease
 * or a timeout even if its side effects already happened.
 */
export type JobHandler<P = unknown> = (ctx: JobContext<P>) => Promise<void>;

/** Handlers keyed by queue name: one queue = one kind of work. */
export class HandlerRegistry {
  private readonly handlers = new Map<string, JobHandler>();

  register<P = unknown>(queue: string, handler: JobHandler<P>): this {
    if (!QUEUE_NAME_PATTERN.test(queue)) {
      throw new Error(`Invalid queue name "${queue}"`);
    }
    if (this.handlers.has(queue)) {
      throw new Error(`A handler is already registered for queue "${queue}"`);
    }
    this.handlers.set(queue, handler as JobHandler);
    return this;
  }

  get(queue: string): JobHandler | undefined {
    return this.handlers.get(queue);
  }

  has(queue: string): boolean {
    return this.handlers.has(queue);
  }

  queues(): string[] {
    return [...this.handlers.keys()].sort();
  }

  /**
   * Resolve the queues a worker consumes: the requested ones (all must have a
   * handler, so a typo fails at startup instead of silently idling) or every
   * registered queue.
   */
  resolveQueues(requested?: readonly string[]): string[] {
    if (requested === undefined) {
      const all = this.queues();
      if (all.length === 0) throw new Error('No job handlers registered');
      return all;
    }
    const missing = requested.filter((q) => !this.handlers.has(q));
    if (missing.length > 0) {
      throw new Error(`No handler registered for queue(s): ${missing.join(', ')}`);
    }
    return [...requested];
  }
}
