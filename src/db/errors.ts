import {
  AppError,
  ConflictError,
  NotFoundError,
  ServiceUnavailableError,
  ValidationError,
} from '../domain/errors.js';

/**
 * Translate driver / server errors into typed domain errors at the repository
 * boundary, so services and routes never inspect SQLSTATE codes. Unknown
 * errors are returned unchanged and surface as a logged 500.
 *
 * SQLSTATE reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
 */
export function mapDatabaseError(err: unknown): unknown {
  if (err instanceof AppError) return err;
  if (typeof err !== 'object' || err === null) return err;

  const { code, constraint } = err as { code?: unknown; constraint?: unknown };
  const details = typeof constraint === 'string' ? { constraint } : undefined;

  if (typeof code === 'string') {
    switch (code) {
      case '23505': // unique_violation
        return withCause(new ConflictError('Resource already exists', details), err);
      case '23503': // foreign_key_violation
        return withCause(new NotFoundError('Referenced resource not found', details), err);
      case '23514': // check_violation
      case '23502': // not_null_violation
        return withCause(new ValidationError('Value violates a data constraint', details), err);
      case '22P02': // invalid_text_representation (e.g. bad uuid / enum literal)
      case '22P05': // untranslatable_character (e.g. \u0000 inside jsonb)
      case '22001': // string_data_right_truncation
      case '22003': // numeric_value_out_of_range
      case '22023': // invalid_parameter_value
      case '22007': // invalid_datetime_format
      case '22008': // datetime_field_overflow
        return withCause(new ValidationError('Value cannot be stored', details), err);
      case '57014': // query_canceled (statement_timeout)
        return withCause(new ServiceUnavailableError('Database query timed out'), err);
      case '53300': // too_many_connections
      case '57P01': // admin_shutdown
      case '57P02': // crash_shutdown
      case '57P03': // cannot_connect_now
        return withCause(new ServiceUnavailableError('Database unavailable'), err);
      default:
        if (code.startsWith('08')) {
          // connection_exception class
          return withCause(new ServiceUnavailableError('Database unavailable'), err);
        }
        if (NODE_NETWORK_ERRORS.has(code)) {
          return withCause(new ServiceUnavailableError('Database unavailable'), err);
        }
    }
  }

  // pg-pool raises plain Errors for these, without a code.
  const message = (err as { message?: unknown }).message;
  if (
    typeof message === 'string' &&
    (message.includes('timeout exceeded when trying to connect') ||
      message.includes('Connection terminated'))
  ) {
    return withCause(new ServiceUnavailableError('Database unavailable'), err);
  }

  return err;
}

const NODE_NETWORK_ERRORS = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ENOTFOUND',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'EAI_AGAIN',
]);

function withCause<E extends AppError>(appErr: E, cause: unknown): E {
  Object.defineProperty(appErr, 'cause', { value: cause, enumerable: false, configurable: true });
  return appErr;
}
