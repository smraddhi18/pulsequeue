import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KeyedCoalescer } from '../../src/redis/coalescer.js';

describe('KeyedCoalescer', () => {
  let fired: string[];
  let coalescer: KeyedCoalescer;

  beforeEach(() => {
    vi.useFakeTimers();
    fired = [];
    coalescer = new KeyedCoalescer(50, (key) => fired.push(key));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('fires the leading edge immediately (no added latency)', () => {
    coalescer.trigger('q');
    expect(fired).toEqual(['q']);
  });

  it('collapses a burst into one trailing fire at the end of the window', () => {
    for (let i = 0; i < 100; i++) coalescer.trigger('q');
    expect(fired).toEqual(['q']);
    vi.advanceTimersByTime(50);
    expect(fired).toEqual(['q', 'q']);
    // The trailing fire opened a new window; an idle window closes silently.
    vi.advanceTimersByTime(50);
    expect(fired).toEqual(['q', 'q']);
    expect(coalescer.openWindows).toBe(0);
  });

  it('fires at most about twice per window under sustained load, never dropping the last trigger', () => {
    for (let t = 0; t < 500; t += 5) {
      coalescer.trigger('q');
      vi.advanceTimersByTime(5);
    }
    vi.advanceTimersByTime(200);
    // 500 ms of triggers every 5 ms: 1 leading + one per elapsed window.
    expect(fired.length).toBeGreaterThanOrEqual(10);
    expect(fired.length).toBeLessThanOrEqual(12);
  });

  it('does not fire a trailing edge without a trigger inside the window', () => {
    coalescer.trigger('q');
    vi.advanceTimersByTime(60);
    expect(fired).toEqual(['q']);
    coalescer.trigger('q');
    expect(fired).toEqual(['q', 'q']);
  });

  it('keeps keys independent', () => {
    coalescer.trigger('a');
    coalescer.trigger('b');
    coalescer.trigger('a');
    expect(fired).toEqual(['a', 'b']);
    vi.advanceTimersByTime(50);
    expect(fired).toEqual(['a', 'b', 'a']);
  });

  it('flush fires pending trailing edges now and closes every window', () => {
    coalescer.trigger('a');
    coalescer.trigger('a');
    coalescer.trigger('b');
    expect(coalescer.flush()).toBe(1);
    expect(fired).toEqual(['a', 'b', 'a']);
    expect(coalescer.openWindows).toBe(0);
    vi.advanceTimersByTime(100);
    expect(fired).toHaveLength(3);
  });
});
