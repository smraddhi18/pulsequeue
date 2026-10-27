/**
 * Typed domain errors. Routes never throw raw Error; services throw these
 * so the central error handler can map them to consistent JSON responses.
 */

export type ErrorCode =
  | 'INTERNAL_ERROR'
  | 'NOT_FOUND'
  | 'VALIDATION_ERROR'
  | 'BAD_REQUEST'
  | 'PAYLOAD_TOO_LARGE'
  | 'UNSUPPORTED_MEDIA_TYPE'
  | 'CONFLICT'
  | 'IDEMPOTENCY_KEY_REUSED'
  | 'JOB_NOT_DEAD_LETTERED'
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'RATE_LIMITED'
  | 'SERVICE_UNAVAILABLE';

export class AppError extends Error {
  readonly statusCode: number;
  readonly code: ErrorCode;
  readonly details?: unknown;
  /** Response headers the error requires (WWW-Authenticate, Retry-After, ...). */
  readonly headers?: Readonly<Record<string, string>>;
  readonly isOperational: boolean;

  constructor(
    message: string,
    options: {
      statusCode?: number;
      code?: ErrorCode;
      details?: unknown;
      headers?: Record<string, string>;
      cause?: unknown;
      isOperational?: boolean;
    } = {},
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = new.target.name;
    this.statusCode = options.statusCode ?? 500;
    this.code = options.code ?? 'INTERNAL_ERROR';
    if (options.details !== undefined) {
      this.details = options.details;
    }
    if (options.headers !== undefined) {
      this.headers = options.headers;
    }
    this.isOperational = options.isOperational ?? true;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class NotFoundError extends AppError {
  constructor(message = 'Resource not found', details?: unknown) {
    super(message, {
      statusCode: 404,
      code: 'NOT_FOUND',
      ...(details !== undefined ? { details } : {}),
    });
  }
}

export class ValidationError extends AppError {
  constructor(message = 'Validation failed', details?: unknown) {
    super(message, {
      statusCode: 400,
      code: 'VALIDATION_ERROR',
      ...(details !== undefined ? { details } : {}),
    });
  }
}

/**
 * A ValidationError about one request field, with the same `details` shape as
 * a flattened ZodError (`{ formErrors, fieldErrors }`), so a rule checked in a
 * service (e.g. one that depends on the current time) produces exactly the
 * same 400 body as a schema rule.
 */
export function fieldValidationError(field: string, message: string): ValidationError {
  return new ValidationError('Validation failed', {
    formErrors: [],
    fieldErrors: { [field]: [message] },
  });
}

export class ConflictError extends AppError {
  constructor(message = 'Conflict', details?: unknown) {
    super(message, {
      statusCode: 409,
      code: 'CONFLICT',
      ...(details !== undefined ? { details } : {}),
    });
  }
}

export class PayloadTooLargeError extends AppError {
  constructor(message = 'Payload too large', details?: unknown) {
    super(message, {
      statusCode: 413,
      code: 'PAYLOAD_TOO_LARGE',
      ...(details !== undefined ? { details } : {}),
    });
  }
}

/**
 * An Idempotency-Key was reused with a different request body. 422 follows the
 * IETF httpapi Idempotency-Key draft: the request is well-formed but cannot be
 * processed because it contradicts the request the key was first used with.
 */
export class IdempotencyKeyReusedError extends AppError {
  constructor(
    message = 'Idempotency-Key was already used with a different request',
    details?: unknown,
  ) {
    super(message, {
      statusCode: 422,
      code: 'IDEMPOTENCY_KEY_REUSED',
      ...(details !== undefined ? { details } : {}),
    });
  }
}

/**
 * A DLQ operation (replay, purge) targeted a job that exists but is not
 * dead-lettered, typically because a concurrent replay already moved it out.
 * 409: the request is valid but conflicts with the job's current state; the
 * caller can GET the job to see where it went.
 */
export class JobNotDeadLetteredError extends AppError {
  constructor(message = 'Job is not in the dead-letter queue', details?: unknown) {
    super(message, {
      statusCode: 409,
      code: 'JOB_NOT_DEAD_LETTERED',
      ...(details !== undefined ? { details } : {}),
    });
  }
}

/** 401: no usable credentials. Carries the RFC 6750 challenge header. */
export class UnauthorizedError extends AppError {
  constructor(message = 'Unauthorized') {
    super(message, {
      statusCode: 401,
      code: 'UNAUTHORIZED',
      headers: { 'www-authenticate': 'Bearer realm="pulsequeue"' },
    });
  }
}

/** 403: authenticated, but the key's role does not allow the operation. */
export class ForbiddenError extends AppError {
  constructor(message = 'Forbidden', details?: unknown) {
    super(message, {
      statusCode: 403,
      code: 'FORBIDDEN',
      ...(details !== undefined ? { details } : {}),
    });
  }
}

/**
 * 429: the tenant or queue spent its enqueue budget (M6). `headers` carries
 * Retry-After (whole seconds, at least 1) plus the RateLimit-* fields.
 */
export class RateLimitedError extends AppError {
  constructor(message: string, details: unknown, headers: Record<string, string>) {
    super(message, { statusCode: 429, code: 'RATE_LIMITED', details, headers });
  }
}

export class ServiceUnavailableError extends AppError {
  constructor(
    message = 'Service unavailable',
    details?: unknown,
    headers?: Record<string, string>,
  ) {
    super(message, {
      statusCode: 503,
      code: 'SERVICE_UNAVAILABLE',
      ...(details !== undefined ? { details } : {}),
      ...(headers !== undefined ? { headers } : {}),
    });
  }
}
