import { describe, expect, it } from 'vitest';
import { LogThrottle } from '../src/lib/log-throttle.js';

describe('LogThrottle', () => {
  it('logs the first event, suppresses the rest of the interval and reports the count', () => {
    let now = 0;
    const throttle = new LogThrottle(1_000, () => now);
    expect(throttle.check('a')).toEqual({ log: true, suppressed: 0 });
    now = 10;
    expect(throttle.check('a')).toEqual({ log: false, suppressed: 1 });
    expect(throttle.check('a')).toEqual({ log: false, suppressed: 2 });
    now = 1_000;
    expect(throttle.check('a')).toEqual({ log: true, suppressed: 2 });
    expect(throttle.check('a')).toEqual({ log: false, suppressed: 1 });
  });

  it('tracks keys independently', () => {
    const throttle = new LogThrottle(1_000, () => 0);
    expect(throttle.check('a').log).toBe(true);
    expect(throttle.check('b').log).toBe(true);
    expect(throttle.check('a').log).toBe(false);
  });
});
