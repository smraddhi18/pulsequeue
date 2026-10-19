import type { FastifyInstance } from 'fastify';
import { AppError } from '../domain/errors.js';
import { mapErrorToResponse } from '../lib/error-mapper.js';

/**
 * Central error handler: maps thrown values to consistent JSON bodies and
 * logs unexpected (non-operational) errors at error level.
 */
export async function registerErrorHandler(app: FastifyInstance): Promise<void> {
  app.setErrorHandler((err, request, reply) => {
    const mapped = mapErrorToResponse(err);
    const isOperational = err instanceof AppError && err.isOperational;

    if (!isOperational && mapped.statusCode >= 500) {
      request.log.error({ err }, 'Unhandled error');
    } else {
      request.log.warn(
        { err, statusCode: mapped.statusCode, code: mapped.body.error.code },
        'Request failed',
      );
    }

    return reply.status(mapped.statusCode).send(mapped.body);
  });

  app.setNotFoundHandler((request, reply) => {
    return reply.status(404).send({
      error: {
        code: 'NOT_FOUND',
        message: `Route ${request.method} ${request.url} not found`,
      },
    });
  });
}
