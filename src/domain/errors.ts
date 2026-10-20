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
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'SERVICE_UNAVAILABLE';

export class AppError extends Error {
  readonly statusCode: number;
  readonly code: ErrorCode;
  readonly details?: unknown;
  readonly isOperational: boolean;

  constructor(
    message: string,
    options: {
      statusCode?: number;
      code?: ErrorCode;
      details?: unknown;
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

export class UnauthorizedError extends AppError {
  constructor(message = 'Unauthorized') {
    super(message, { statusCode: 401, code: 'UNAUTHORIZED' });
  }
}

export class ForbiddenError extends AppError {
  constructor(message = 'Forbidden') {
    super(message, { statusCode: 403, code: 'FORBIDDEN' });
  }
}

export class ServiceUnavailableError extends AppError {
  constructor(message = 'Service unavailable', details?: unknown) {
    super(message, {
      statusCode: 503,
      code: 'SERVICE_UNAVAILABLE',
      ...(details !== undefined ? { details } : {}),
    });
  }
}
