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

  // Fastify's own client errors (malformed JSON, body over bodyLimit, wrong
  // content type, ...). Their messages are written for clients, so keep them.
  if (isFastifyClientError(err)) {
    return {
      statusCode: err.statusCode,
      body: {
        error: {
          code: CLIENT_ERROR_CODES[err.statusCode] ?? 'BAD_REQUEST',
          message: err.message,
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

const CLIENT_ERROR_CODES: Record<number, string> = {
  400: 'BAD_REQUEST',
  413: 'PAYLOAD_TOO_LARGE',
  415: 'UNSUPPORTED_MEDIA_TYPE',
};

function isFastifyClientError(
  err: unknown,
): err is { statusCode: number; code: string; message: string } {
  if (typeof err !== 'object' || err === null) return false;
  const candidate = err as { statusCode?: unknown; code?: unknown };
  return (
    typeof candidate.statusCode === 'number' &&
    candidate.statusCode >= 400 &&
    candidate.statusCode < 500 &&
    typeof candidate.code === 'string' &&
    candidate.code.startsWith('FST_')
  );
}
