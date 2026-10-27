import { describe, expect, it } from 'vitest';
import { abortableDelay, demoHandler, echoHandler } from '../../src/worker/handlers/index.js';
import { InvalidPayloadError, NonRetryableError } from '../../src/worker/errors.js';
import type { JobContext } from '../../src/worker/registry.js';
import { silentLogger } from './fake-store.js';

function ctx(payload: unknown, attempt = 1, signal = new AbortController().signal): JobContext {
  return {
    job: {
      id: 'j',
      tenantId: 'default',
      queue: 'demo',
      payload,
      priority: 0,
      attempt,
      maxAttempts: 5,
      createdAt: new Date(),
    },
    signal,
    logger: silentLogger,
  };
}

describe('built-in handlers', () => {
  it('echo succeeds', async () => {
    await expect(echoHandler(ctx({ hello: 'world' }))).resolves.toBeUndefined();
  });

  it('demo succeeds by default and fails until the configured attempt', async () => {
    await expect(demoHandler(ctx({}))).resolves.toBeUndefined();
    await expect(demoHandler(ctx({ failUntilAttempt: 3 }, 2))).rejects.toThrow(
      'Demo failure on attempt 2',
    );
    await expect(demoHandler(ctx({ failUntilAttempt: 3 }, 3))).resolves.toBeUndefined();
  });

  it('demo rejects bad payloads and nonRetryable as NonRetryableError', async () => {
    await expect(demoHandler(ctx({ durationMs: 'soon' }))).rejects.toBeInstanceOf(
      NonRetryableError,
    );
    await expect(demoHandler(ctx({ extra: 1 }))).rejects.toBeInstanceOf(NonRetryableError);
    // Bad payloads are the more specific InvalidPayloadError (DLQ reason invalid_payload).
    await expect(demoHandler(ctx({ extra: 1 }))).rejects.toBeInstanceOf(InvalidPayloadError);
    await expect(demoHandler(ctx({ nonRetryable: true }))).rejects.not.toBeInstanceOf(
      InvalidPayloadError,
    );
    await expect(demoHandler(ctx({ nonRetryable: true }))).rejects.toBeInstanceOf(
      NonRetryableError,
    );
  });

  it('abortableDelay stops on abort', async () => {
    const controller = new AbortController();
    const p = abortableDelay(10_000, controller.signal);
    controller.abort(new Error('stop'));
    await expect(p).rejects.toThrow('stop');
    await expect(abortableDelay(10_000, controller.signal)).rejects.toThrow('stop');
    await expect(abortableDelay(1, new AbortController().signal)).resolves.toBeUndefined();
  });
});
