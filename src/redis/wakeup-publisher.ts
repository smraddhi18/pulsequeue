import type { StructuredLogger } from '../lib/logger.js';
import { isRedisReady, type RedisClient } from './client.js';
import { KeyedCoalescer } from './coalescer.js';
import type { RedisKeys } from './keys.js';
import { LogThrottle } from '../lib/log-throttle.js';

/** Wire format of a wake-up. Only the channel matters; the body is for observability. */
export interface WakeupMessage {
  queue: string;
  /** Publisher wall clock (ms); subscribers log the delivery lag. */
  sentAt: number;
  /** Publishing process, for tracing a wake-up back to its origin. */
  source: string;
}

export function encodeWakeupMessage(message: WakeupMessage): string {
  return JSON.stringify(message);
}

/** Lenient: a malformed body still counts as a wake-up for its channel. */
export function decodeWakeupMessage(raw: string): Partial<WakeupMessage> {
  try {
    const parsed: unknown = JSON.parse(raw);
    return parsed !== null && typeof parsed === 'object' ? (parsed as Partial<WakeupMessage>) : {};
  } catch {
    return {};
  }
}

export interface WakeupPublisherOptions {
  client: RedisClient;
  keys: RedisKeys;
  /** Per-queue coalescing window (WAKEUP_COALESCE_MS). */
  coalesceMs: number;
  logger: StructuredLogger;
  source: string;
}

const FAILURE_LOG_INTERVAL_MS = 10_000;

/**
 * Publishes "this queue has runnable work" on the queue's channel.
 *
 * Called after the change committed (see QueueSignals), fire-and-forget:
 * notify() never throws or blocks the request. Bursts are coalesced per
 * queue (leading + trailing edge), so 10k enqueues per second into one queue
 * cost about 2 publishes per window, not 10k. A failed or skipped publish is
 * only a latency cost: subscribers still run their fallback poll.
 */
export class WakeupPublisher {
  private readonly coalescer: KeyedCoalescer;
  private readonly inFlight = new Set<Promise<void>>();
  private readonly failures = new LogThrottle(FAILURE_LOG_INTERVAL_MS);

  constructor(private readonly options: WakeupPublisherOptions) {
    this.coalescer = new KeyedCoalescer(options.coalesceMs, (queue) => this.publish(queue));
  }

  notify(queue: string): void {
    this.coalescer.trigger(queue);
  }

  /** Send pending trailing publishes and wait for in-flight ones (shutdown). */
  async flush(): Promise<void> {
    this.coalescer.flush();
    await Promise.allSettled([...this.inFlight]);
  }

  private publish(queue: string): void {
    const { client, keys, logger, source } = this.options;
    if (!isRedisReady(client)) {
      this.logFailure(queue, new Error(`Redis is not connected (status ${client.status})`));
      return;
    }
    const message = encodeWakeupMessage({ queue, sentAt: Date.now(), source });
    const promise = client
      .publish(keys.wakeupChannel(queue), message)
      .then((receivers) => {
        logger.debug({ event: 'wakeup.published', queue, receivers }, 'Wake-up published');
      })
      .catch((err: unknown) => this.logFailure(queue, err))
      .finally(() => this.inFlight.delete(promise));
    this.inFlight.add(promise);
  }

  private logFailure(queue: string, err: unknown): void {
    const decision = this.failures.check('publish');
    if (!decision.log) return;
    this.options.logger.warn(
      { event: 'wakeup.publish_failed', queue, err, suppressed: decision.suppressed },
      'Could not publish wake-up; workers fall back to polling',
    );
  }
}
