import { describe, expect, it } from 'vitest';
import { ZodError, z } from 'zod';
import { AppError, NotFoundError, ValidationError } from '../src/domain/errors.js';
import { mapErrorToResponse } from '../src/lib/error-mapper.js';

describe('mapErrorToResponse', () => {
  it('maps AppError subclasses to their status and code', () => {
    const mapped = mapErrorToResponse(new NotFoundError('Job not found', { id: 'abc' }));
    expect(mapped.statusCode).toBe(404);
    expect(mapped.body.error.code).toBe('NOT_FOUND');
    expect(mapped.body.error.message).toBe('Job not found');
    expect(mapped.body.error.details).toEqual({ id: 'abc' });
  });

  it('maps ValidationError to 400', () => {
    const mapped = mapErrorToResponse(new ValidationError('bad payload'));
    expect(mapped.statusCode).toBe(400);
    expect(mapped.body.error.code).toBe('VALIDATION_ERROR');
  });

  it('maps ZodError to a validation response', () => {
    const schema = z.object({ name: z.string() });
    let zodErr: ZodError | undefined;
    try {
      schema.parse({});
    } catch (e) {
      zodErr = e as ZodError;
    }
    expect(zodErr).toBeInstanceOf(ZodError);
    const mapped = mapErrorToResponse(zodErr);
    expect(mapped.statusCode).toBe(400);
    expect(mapped.body.error.code).toBe('VALIDATION_ERROR');
    expect(mapped.body.error.details).toBeDefined();
  });

  it('maps unknown errors to a generic 500 without leaking details', () => {
    const mapped = mapErrorToResponse(new Error('secret stack internals'));
    expect(mapped.statusCode).toBe(500);
    expect(mapped.body.error.code).toBe('INTERNAL_ERROR');
    expect(mapped.body.error.message).toBe('Internal server error');
    expect(mapped.body.error.details).toBeUndefined();
  });

  it('preserves custom AppError status codes', () => {
    const mapped = mapErrorToResponse(
      new AppError('rate limited', { statusCode: 429, code: 'INTERNAL_ERROR' }),
    );
    expect(mapped.statusCode).toBe(429);
  });

  it('maps Fastify client errors to their 4xx status with a stable code', () => {
    const tooLarge = Object.assign(new Error('Request body is too large'), {
      statusCode: 413,
      code: 'FST_ERR_CTP_BODY_TOO_LARGE',
    });
    const mapped = mapErrorToResponse(tooLarge);
    expect(mapped.statusCode).toBe(413);
    expect(mapped.body.error.code).toBe('PAYLOAD_TOO_LARGE');
    expect(mapped.body.error.message).toBe('Request body is too large');

    const unsupported = Object.assign(new Error('Unsupported Media Type'), {
      statusCode: 415,
      code: 'FST_ERR_CTP_INVALID_MEDIA_TYPE',
    });
    expect(mapErrorToResponse(unsupported).body.error.code).toBe('UNSUPPORTED_MEDIA_TYPE');
  });

  it('does not trust statusCode on arbitrary non-Fastify errors', () => {
    const sneaky = Object.assign(new Error('internal detail'), { statusCode: 400 });
    const mapped = mapErrorToResponse(sneaky);
    expect(mapped.statusCode).toBe(500);
    expect(mapped.body.error.message).toBe('Internal server error');
  });
});
