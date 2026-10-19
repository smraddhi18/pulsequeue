import { randomUUID } from 'node:crypto';
import type { FastifyInstance } from 'fastify';

declare module 'fastify' {
  interface FastifyRequest {
    requestId: string;
  }
}

const REQUEST_ID_HEADER = 'x-request-id';

/**
 * Ensures every request has a stable request ID (from inbound header or a new UUID),
 * echoes it on the response, and binds it onto the request logger child.
 */
export async function registerRequestId(app: FastifyInstance): Promise<void> {
  app.decorateRequest('requestId', '');

  app.addHook('onRequest', async (request, reply) => {
    const inbound = request.headers[REQUEST_ID_HEADER];
    const requestId =
      typeof inbound === 'string' && inbound.trim().length > 0 ? inbound.trim() : randomUUID();

    request.requestId = requestId;
    reply.header(REQUEST_ID_HEADER, requestId);
    request.log = request.log.child({ requestId });
  });
}
