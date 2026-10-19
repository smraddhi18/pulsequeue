import { ZodError } from 'zod';
import { AppError } from '../domain/errors.js';

export interface ErrorResponseBody {
  error: {
    code: string;
    message: string;
    details?: unknown;
  };
}

export interface MappedError {
  statusCode: number;
  body: ErrorResponseBody;
}

/**
 * Map any thrown value to a consistent HTTP status + JSON error body.
 * Operational AppErrors keep their status/code; Zod becomes 400; everything
 * else becomes a generic 500 without leaking internals.
 */
export function mapErrorToResponse(err: unknown): MappedError {
  if (err instanceof AppError) {
    const body: ErrorResponseBody = {
      error: {
        code: err.code,
        message: err.message,
      },
    };
    if (err.details !== undefined) {
      body.error.details = err.details;
    }
    return { statusCode: err.statusCode, body };
  }

  if (err instanceof ZodError) {
    return {
      statusCode: 400,
      body: {
        error: {
          code: 'VALIDATION_ERROR',
          message: 'Validation failed',
          details: err.flatten(),
        },
      },
    };
  }

  // Fastify validation errors (schema mismatch)
  if (typeof err === 'object' && err !== null && 'validation' in err && 'statusCode' in err) {
    const fastifyErr = err as { statusCode: number; message: string; validation: unknown };
    return {
      statusCode: fastifyErr.statusCode || 400,
      body: {
        error: {
          code: 'VALIDATION_ERROR',
          message: fastifyErr.message || 'Validation failed',
          details: fastifyErr.validation,
        },
      },
    };
  }

  return {
    statusCode: 500,
    body: {
      error: {
        code: 'INTERNAL_ERROR',
        message: 'Internal server error',
      },
    },
  };
}
