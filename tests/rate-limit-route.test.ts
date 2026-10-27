import type { FastifyReply } from 'fastify';
import { describe, expect, it } from 'vitest';
import { RateLimitedError, ServiceUnavailableError } from '../src/domain/errors.js';
import { mapErrorToResponse as mapError } from '../src/lib/error-mapper.js';
import { enforceRateLimit, rateLimitHeaders } from '../src/routes/rate-limit.js';
import type { RateLimitInfo } from '../src/services/rate-limiter.js';

function fakeReply() {
  const headers: Record<string, string> = {};
  const reply = {
    headers: (h: Record<string, string>) => Object.assign(headers, h),
  } as unknown as FastifyReply;
  return { reply, headers };
}

const info: RateLimitInfo = { scope: 'tenant', limit: 200, remaining: 17, resetMs: 1_501 };

describe('enforceRateLimit', () => {
  it('formats the RateLimit-* fields (reset in whole seconds, rounded up)', () => {
    expect(rateLimitHeaders(info)).toEqual({
      'ratelimit-limit': '200',
      'ratelimit-remaining': '17',
      'ratelimit-reset': '2',
    });
  });

  it('adds headers when allowed and nothing when skipped', () => {
    const allowed = fakeReply();
    enforceRateLimit(allowed.reply, { outcome: 'allowed', info });
    expect(allowed.headers['ratelimit-remaining']).toBe('17');
    const skipped = fakeReply();
    enforceRateLimit(skipped.reply, { outcome: 'skipped', reason: 'unavailable' });
    expect(skipped.headers).toEqual({});
  });

  it('throws 429 with Retry-After of at least one second', () => {
    const { reply } = fakeReply();
    let thrown: unknown;
    try {
      enforceRateLimit(reply, {
        outcome: 'limited',
        retryAfterMs: 40,
        info: { ...info, scope: 'queue', remaining: 0 },
      });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(RateLimitedError);
    const mapped = mapError(thrown);
    expect(mapped.statusCode).toBe(429);
    expect(mapped.headers).toMatchObject({ 'retry-after': '1', 'ratelimit-remaining': '0' });
    expect(mapped.body.error).toMatchObject({
      code: 'RATE_LIMITED',
      details: { scope: 'queue', retryAfterMs: 40 },
    });
  });

  it('rounds Retry-After up to whole seconds', () => {
    expect(() =>
      enforceRateLimit(fakeReply().reply, { outcome: 'limited', retryAfterMs: 2_001, info }),
    ).toThrow(/retry in 3s/);
  });

  it('throws 503 with Retry-After when the store is down in closed mode', () => {
    let thrown: unknown;
    try {
      enforceRateLimit(fakeReply().reply, { outcome: 'unavailable' });
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(ServiceUnavailableError);
    expect(mapError(thrown)).toMatchObject({ statusCode: 503, headers: { 'retry-after': '1' } });
  });
});
