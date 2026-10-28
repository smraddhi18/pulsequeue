import type { FastifyInstance, FastifyRequest } from 'fastify';
import type { ApiMetrics } from '../observability/metrics.js';
import { SpanKind, SpanStatusCode, type Span, type Tracing } from '../observability/tracing.js';

declare module 'fastify' {
  interface FastifyRequest {
    /** The request's SERVER span (non-recording when tracing is off). */
    span: Span | null;
  }
}

/** Route label for requests that matched no route: raw URLs would be unbounded. */
export const UNMATCHED_ROUTE = '__unmatched__';

export interface HttpObservabilityOptions {
  metrics: ApiMetrics;
  tracing: Tracing;
}

/** The route template (/queues/:name/jobs), never the raw URL. */
export function routeLabel(request: FastifyRequest): string {
  if (request.is404) return UNMATCHED_ROUTE;
  return request.routeOptions.url ?? UNMATCHED_ROUTE;
}

/**
 * HTTP metrics and tracing for the API (Milestone 7). Register after the
 * request-id plugin and before auth: the trace ids are then bound onto the
 * request logger before auth adds its own child, so every log line of the
 * request, including auth failures, carries requestId + trace_id.
 *
 * - Metrics: one histogram observation per response (onResponse runs for
 *   errors and 404s too), labelled with the route template.
 * - Tracing: a SERVER span per request, continuing an inbound W3C
 *   traceparent when present (a caller's trace flows through the enqueue
 *   into the worker's execution span).
 */
export async function registerHttpObservability(
  app: FastifyInstance,
  options: HttpObservabilityOptions,
): Promise<void> {
  const { metrics, tracing } = options;
  app.decorateRequest('span', null);

  if (tracing.enabled) {
    app.addHook('onRequest', async (request) => {
      const inbound = request.headers.traceparent;
      const route = routeLabel(request);
      const span = tracing.startSpan(`${request.method} ${route}`, {
        kind: SpanKind.SERVER,
        parent: tracing.contextFromTraceparent(typeof inbound === 'string' ? inbound : null),
        attributes: {
          'http.request.method': request.method,
          'http.route': route,
          'url.path': request.url.split('?', 1)[0] ?? request.url,
          'pulsequeue.request_id': request.requestId,
        },
      });
      request.span = span;
      const fields = tracing.logFields(span);
      if (fields !== null) request.log = request.log.child(fields);
    });

    app.addHook('onRequestAbort', async (request) => {
      const span = request.span;
      if (span === null) return;
      span.setStatus({ code: SpanStatusCode.ERROR, message: 'client aborted' });
      span.end();
      request.span = null;
    });
  }

  app.addHook('onResponse', async (request, reply) => {
    metrics.observeHttp(
      request.method,
      routeLabel(request),
      reply.statusCode,
      reply.elapsedTime / 1000,
    );
    const span = request.span;
    if (span === null) return;
    span.setAttribute('http.response.status_code', reply.statusCode);
    if (reply.statusCode >= 500) span.setStatus({ code: SpanStatusCode.ERROR });
    span.end();
    request.span = null;
  });
}
