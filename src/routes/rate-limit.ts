import type { FastifyReply } from 'fastify';
import { RateLimitedError, ServiceUnavailableError } from '../domain/errors.js';
import type { RateLimitDecision, RateLimitInfo } from '../services/rate-limiter.js';

/**
 * HTTP side of enqueue rate limiting (M6). Header names follow the IETF
 * httpapi RateLimit fields draft: RateLimit-Limit (bucket size),
 * RateLimit-Remaining (tokens left) and RateLimit-Reset (seconds until the
 * bucket is full again). They describe the bucket closest to refusing,
 * which is the queue's when the queue has its own limit and it is tighter.
 */
export function rateLimitHeaders(info: RateLimitInfo): Record<string, string> {
  return {
    'ratelimit-limit': String(info.limit),
    'ratelimit-remaining': String(info.remaining),
    'ratelimit-reset': String(Math.ceil(info.resetMs / 1000)),
  };
}

/**
 * Apply a decision to the reply: headers when limiting ran, 429 when the
 * budget is spent, 503 when the store is down in closed mode. A skipped
 * check (disabled, or store down in open mode) adds nothing.
 */
export function enforceRateLimit(reply: FastifyReply, decision: RateLimitDecision): void {
  switch (decision.outcome) {
    case 'allowed':
      reply.headers(rateLimitHeaders(decision.info));
      return;
    case 'skipped':
      return;
    case 'limited': {
      // Whole seconds and never 0: "retry immediately" would just be refused again.
      const retryAfter = Math.max(1, Math.ceil(decision.retryAfterMs / 1000));
      throw new RateLimitedError(
        `Enqueue rate limit exceeded (${decision.info.scope}); retry in ${retryAfter}s`,
        { scope: decision.info.scope, retryAfterMs: decision.retryAfterMs },
        { 'retry-after': String(retryAfter), ...rateLimitHeaders(decision.info) },
      );
    }
    case 'unavailable':
      throw new ServiceUnavailableError(
        'Rate limiting is unavailable; enqueues are refused (RATE_LIMIT_FAIL_MODE=closed)',
        { reason: 'rate_limit_unavailable' },
        { 'retry-after': '1' },
      );
  }
}
