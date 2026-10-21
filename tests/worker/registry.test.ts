import { describe, expect, it } from 'vitest';
import { HandlerRegistry } from '../../src/worker/registry.js';
import { describeError, NonRetryableError, HandlerNotFoundError } from '../../src/worker/errors.js';
import { jitter, Sleeper } from '../../src/worker/sleeper.js';
import { registerBuiltinHandlers } from '../../src/worker/handlers/index.js';

const noop = async () => undefined;

describe('HandlerRegistry', () => {
  it('registers and looks up handlers by queue', () => {
    const registry = new HandlerRegistry().register('emails', noop).register('reports', noop);
    expect(registry.get('emails')).toBe(noop);
    expect(registry.has('nope')).toBe(false);
    expect(registry.queues()).toEqual(['emails', 'reports']);
  });

  it('rejects duplicates and invalid queue names', () => {
    const registry = new HandlerRegistry().register('emails', noop);
    expect(() => registry.register('emails', noop)).toThrow(/already registered/);
    expect(() => registry.register('Bad Name', noop)).toThrow(/Invalid queue name/);
  });

  it('resolves all registered queues by default and fails with none', () => {
    expect(new HandlerRegistry().register('a', noop).resolveQueues()).toEqual(['a']);
    expect(() => new HandlerRegistry().resolveQueues()).toThrow(/No job handlers/);
  });

  it('fails fast when a requested queue has no handler', () => {
    const registry = new HandlerRegistry().register('a', noop);
    expect(registry.resolveQueues(['a'])).toEqual(['a']);
    expect(() => registry.resolveQueues(['a', 'typo'])).toThrow(/typo/);
  });

  it('ships echo and demo handlers', () => {
    expect(registerBuiltinHandlers(new HandlerRegistry()).queues()).toEqual(['demo', 'echo']);
  });
});

describe('describeError', () => {
  it('formats errors, strings and other values', () => {
    expect(describeError(new Error('plain'))).toBe('plain');
    expect(describeError(new NonRetryableError('bad'))).toBe('NonRetryableError: bad');
    expect(describeError(new HandlerNotFoundError('q'))).toMatch(/HandlerNotFoundError/);
    expect(describeError('str')).toBe('str');
    expect(describeError({ a: 1 })).toBe('Non-error thrown: {"a":1}');
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(describeError(circular)).toBe('Non-error thrown');
  });
});

describe('Sleeper / jitter', () => {
  it('wake() resolves a pending sleep early', async () => {
    const sleeper = new Sleeper();
    const started = Date.now();
    const p = sleeper.sleep(10_000);
    sleeper.wake();
    await p;
    expect(Date.now() - started).toBeLessThan(1_000);
  });

  it('jitter stays within +-ratio and never drops below 1ms', () => {
    expect(jitter(1_000, 0.2, () => 0)).toBe(800);
    expect(jitter(1_000, 0.2, () => 1)).toBe(1_200);
    expect(jitter(1_000, 0.2, () => 0.5)).toBe(1_000);
    expect(jitter(1, 0.9, () => 0)).toBe(1);
  });
});
