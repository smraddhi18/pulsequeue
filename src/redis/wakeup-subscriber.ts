import type { StructuredLogger } from '../lib/logger.js';
import { closeRedisClient, isRedisReady, type RedisClient } from './client.js';
import type { RedisKeys } from './keys.js';
import { LogThrottle } from '../lib/log-throttle.js';
import { decodeWakeupMessage } from './wakeup-publisher.js';

export interface WakeupSubscriberOptions {
  /** Dedicated connection; the subscriber owns it and closes it in close(). */
  client: RedisClient;
  keys: RedisKeys;
  queues: readonly string[];
  logger: StructuredLogger;
  /** A wake-up arrived for `queue`. */
  onWake: (queue: string) => void;
  /**
   * Subscription state changed. true: (re)subscribed, so messages published
   * while we were away may be lost and the caller should poll once now.
   * false: connection lost, the caller must fall back to polling.
   */
  onListeningChange: (listening: boolean) => void;
}

const FAILURE_LOG_INTERVAL_MS = 10_000;

/**
 * Listens on the wake-up channels of a worker's queues.
 *
 * Reconnects are handled by the client (jittered backoff). On every 'ready'
 * the subscriber issues SUBSCRIBE itself instead of relying on ioredis'
 * auto-resubscribe, so `listening` flips to true only once Redis has
 * confirmed the subscription, and the caller gets a precise moment for its
 * catch-up poll. Pub/sub is fire-and-forget (no replay), so that catch-up
 * poll, plus the worker's fallback poll, is what makes a lost message cost
 * latency instead of a stuck job.
 */
export class WakeupSubscriber {
  private readonly channels: Map<string, string>;
  private readonly failures = new LogThrottle(FAILURE_LOG_INTERVAL_MS);
  private listening = false;
  private started = false;
  private closed = false;

  constructor(private readonly options: WakeupSubscriberOptions) {
    if (options.queues.length === 0) throw new Error('WakeupSubscriber needs at least one queue');
    this.channels = new Map(options.queues.map((q) => [options.keys.wakeupChannel(q), q]));
  }

  /** true while subscribed: the worker may trust wake-ups and poll rarely. */
  get isListening(): boolean {
    return this.listening;
  }

  start(): void {
    if (this.started) throw new Error('WakeupSubscriber already started');
    this.started = true;
    const { client } = this.options;
    client.on('message', this.handleMessage);
    client.on('ready', this.handleReady);
    client.on('close', this.handleClose);
    if (isRedisReady(client)) this.handleReady();
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const { client } = this.options;
    client.off('ready', this.handleReady);
    client.off('close', this.handleClose);
    client.off('message', this.handleMessage);
    this.listening = false;
    await closeRedisClient(client);
  }

  private readonly handleReady = (): void => {
    void this.subscribe();
  };

  private readonly handleClose = (): void => {
    this.setListening(false);
  };

  private readonly handleMessage = (channel: string, raw: string): void => {
    const queue = this.channels.get(channel);
    if (queue === undefined || this.closed) return;
    const message = decodeWakeupMessage(raw);
    this.options.logger.debug(
      {
        event: 'wakeup.received',
        queue,
        source: message.source,
        lagMs: typeof message.sentAt === 'number' ? Date.now() - message.sentAt : undefined,
      },
      'Wake-up received',
    );
    this.options.onWake(queue);
  };

  private async subscribe(): Promise<void> {
    const { client, logger } = this.options;
    const channels = [...this.channels.keys()];
    try {
      await client.subscribe(...channels);
    } catch (err) {
      const decision = this.failures.check('subscribe');
      if (decision.log) {
        logger.warn(
          { event: 'wakeup.subscribe_failed', err, suppressed: decision.suppressed },
          'Could not subscribe to wake-ups; staying in poll mode',
        );
      }
      // Still connected but SUBSCRIBE failed (e.g. timed out): reconnect so
      // the next 'ready' tries again instead of polling forever.
      if (!this.closed && isRedisReady(client)) client.disconnect(true);
      return;
    }
    if (this.closed || !isRedisReady(client)) return;
    logger.info({ event: 'wakeup.subscribed', channels }, 'Subscribed to wake-ups');
    this.setListening(true);
  }

  private setListening(listening: boolean): void {
    if (this.closed) return;
    const changed = this.listening !== listening;
    this.listening = listening;
    if (!listening && changed) {
      this.options.logger.warn(
        { event: 'wakeup.unsubscribed' },
        'Wake-up subscription lost; polling until Redis is back',
      );
    }
    // Always report a (re)subscribe, even if we never noticed the drop: the
    // gap may have swallowed messages.
    if (changed || listening) this.options.onListeningChange(listening);
  }
}
