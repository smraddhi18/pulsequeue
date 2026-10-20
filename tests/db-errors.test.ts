import { describe, expect, it } from 'vitest';
import {
  ConflictError,
  NotFoundError,
  ServiceUnavailableError,
  ValidationError,
} from '../src/domain/errors.js';
import { mapDatabaseError } from '../src/db/errors.js';

function pgError(code: string, extra: Record<string, unknown> = {}): Error {
  return Object.assign(new Error(`pg error ${code}`), { code, ...extra });
}

describe('mapDatabaseError', () => {
  it.each([
    ['23505', ConflictError, 409],
    ['23503', NotFoundError, 404],
    ['23514', ValidationError, 400],
    ['22P02', ValidationError, 400],
    ['22P05', ValidationError, 400],
    ['22008', ValidationError, 400],
    ['57014', ServiceUnavailableError, 503],
    ['53300', ServiceUnavailableError, 503],
    ['57P01', ServiceUnavailableError, 503],
    ['08006', ServiceUnavailableError, 503],
    ['ECONNREFUSED', ServiceUnavailableError, 503],
  ])('maps SQLSTATE/errno %s to a typed domain error', (code, ErrorClass, status) => {
    const original = pgError(code);
    const mapped = mapDatabaseError(original);
    expect(mapped).toBeInstanceOf(ErrorClass);
    expect((mapped as { statusCode: number }).statusCode).toBe(status);
    expect((mapped as Error).cause).toBe(original);
  });

  it('exposes the violated constraint name but not the driver message', () => {
    const mapped = mapDatabaseError(pgError('23514', { constraint: 'jobs_priority_range' }));
    expect(mapped).toBeInstanceOf(ValidationError);
    expect((mapped as ValidationError).details).toEqual({ constraint: 'jobs_priority_range' });
    expect((mapped as Error).message).not.toContain('pg error');
  });

  it('maps pg-pool connection timeouts to 503', () => {
    const mapped = mapDatabaseError(new Error('timeout exceeded when trying to connect'));
    expect(mapped).toBeInstanceOf(ServiceUnavailableError);
  });

  it('returns unknown errors unchanged so they surface as a logged 500', () => {
    const syntax = pgError('42601');
    expect(mapDatabaseError(syntax)).toBe(syntax);
    const plain = new Error('boom');
    expect(mapDatabaseError(plain)).toBe(plain);
    expect(mapDatabaseError('string')).toBe('string');
  });

  it('passes domain errors through untouched', () => {
    const domain = new NotFoundError('nope');
    expect(mapDatabaseError(domain)).toBe(domain);
  });
});
