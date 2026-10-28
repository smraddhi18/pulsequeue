import {
  defaultTextMapGetter,
  defaultTextMapSetter,
  INVALID_SPAN_CONTEXT,
  isSpanContextValid,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace,
  type Attributes,
  type Context,
  type Span,
  type Tracer,
} from '@opentelemetry/api';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
import {
  BatchSpanProcessor,
  ParentBasedSampler,
  SimpleSpanProcessor,
  TraceIdRatioBasedSampler,
  TracerProvider,
  type SpanExporter,
} from '@opentelemetry/sdk-trace';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';

export { SpanKind, SpanStatusCode, type Context, type Span };

/** W3C trace-context header value: version-traceid-spanid-flags. */
export const TRACEPARENT_PATTERN = /^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/;

export interface TracingConfig {
  enabled: boolean;
  serviceName: string;
  serviceVersion?: string;
  /** OTLP/HTTP base URL (spans go to <endpoint>/v1/traces). */
  endpoint?: string | undefined;
  samplerRatio?: number;
  /** Tests: export synchronously to this exporter instead of OTLP. */
  exporter?: SpanExporter;
}

export interface StartSpanOptions {
  kind?: SpanKind;
  attributes?: Attributes;
  /** Parent context (from contextFromTraceparent or contextWithSpan). Default: new trace. */
  parent?: Context;
}

/**
 * The tracing surface the app, worker and scheduler use (Milestone 7).
 *
 * Context is passed explicitly (parent contexts in, spans out) instead of
 * through a global context manager: nothing here needs AsyncLocalStorage
 * propagation through pg/ioredis, and explicit plumbing cannot leak a
 * span from one job into another sharing the event loop. The cost is that
 * pg and Redis calls are not auto-instrumented (that would need the OTel
 * ESM loader hook); spans cover the request, the enqueue and each attempt.
 */
export interface Tracing {
  readonly enabled: boolean;
  startSpan(name: string, options?: StartSpanOptions): Span;
  /** Context whose parent is `span` (for starting children). */
  contextWithSpan(span: Span, parent?: Context): Context;
  /** Parse an inbound/stored traceparent; ROOT_CONTEXT when absent or invalid. */
  contextFromTraceparent(traceparent: string | null | undefined): Context;
  /** The span's traceparent, or null when the span is not recording (tracing off / unsampled). */
  traceparentOf(span: Span): string | null;
  /** Ids for log correlation; null for non-recording spans. */
  logFields(span: Span): { trace_id: string; span_id: string } | null;
  /** Flush buffered spans and stop exporting. */
  shutdown(): Promise<void>;
}

const propagator = new W3CTraceContextPropagator();

/** A shared non-recording span: tracing off costs one object, no allocation per call. */
const NOOP_SPAN: Span = trace.wrapSpanContext(INVALID_SPAN_CONTEXT);

/** Tracing turned off: every call is a no-op and nothing is ever written to jobs.traceparent. */
export const DISABLED_TRACING: Tracing = {
  enabled: false,
  startSpan: () => NOOP_SPAN,
  contextWithSpan: (_span, parent) => parent ?? ROOT_CONTEXT,
  contextFromTraceparent: () => ROOT_CONTEXT,
  traceparentOf: () => null,
  logFields: () => null,
  shutdown: async () => undefined,
};

class OtelTracing implements Tracing {
  readonly enabled = true;
  private readonly tracer: Tracer;

  constructor(private readonly provider: TracerProvider) {
    this.tracer = provider.getTracer('pulsequeue');
  }

  startSpan(name: string, options: StartSpanOptions = {}): Span {
    return this.tracer.startSpan(
      name,
      {
        kind: options.kind ?? SpanKind.INTERNAL,
        ...(options.attributes !== undefined ? { attributes: options.attributes } : {}),
      },
      options.parent ?? ROOT_CONTEXT,
    );
  }

  contextWithSpan(span: Span, parent: Context = ROOT_CONTEXT): Context {
    return trace.setSpan(parent, span);
  }

  contextFromTraceparent(traceparent: string | null | undefined): Context {
    if (typeof traceparent !== 'string' || !TRACEPARENT_PATTERN.test(traceparent)) {
      return ROOT_CONTEXT;
    }
    return propagator.extract(ROOT_CONTEXT, { traceparent }, defaultTextMapGetter);
  }

  traceparentOf(span: Span): string | null {
    if (!span.isRecording()) return null;
    const carrier: Record<string, string> = {};
    propagator.inject(trace.setSpan(ROOT_CONTEXT, span), carrier, defaultTextMapSetter);
    const value = carrier.traceparent;
    return value !== undefined && TRACEPARENT_PATTERN.test(value) ? value : null;
  }

  logFields(span: Span): { trace_id: string; span_id: string } | null {
    const ctx = span.spanContext();
    if (!span.isRecording() || !isSpanContextValid(ctx)) return null;
    return { trace_id: ctx.traceId, span_id: ctx.spanId };
  }

  async shutdown(): Promise<void> {
    await this.provider.shutdown();
  }
}

/**
 * Build tracing for one process. Disabled (the default) returns
 * DISABLED_TRACING and loads no exporter. Nothing is registered globally,
 * so several apps in one test process each get their own provider.
 */
export function createTracing(config: TracingConfig): Tracing {
  if (!config.enabled) return DISABLED_TRACING;
  const exporter =
    config.exporter ??
    (config.endpoint !== undefined
      ? new OTLPTraceExporter({ url: `${config.endpoint.replace(/\/+$/, '')}/v1/traces` })
      : undefined);
  if (exporter === undefined) {
    throw new Error('Tracing is enabled but neither an OTLP endpoint nor an exporter was given');
  }
  const provider = new TracerProvider({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: config.serviceName,
      ...(config.serviceVersion !== undefined
        ? { [ATTR_SERVICE_VERSION]: config.serviceVersion }
        : {}),
    }),
    sampler: new ParentBasedSampler({
      root: new TraceIdRatioBasedSampler(config.samplerRatio ?? 1),
    }),
    // Tests get synchronous export; production batches off the hot path.
    spanProcessors: [
      config.exporter !== undefined
        ? new SimpleSpanProcessor({ exporter })
        : new BatchSpanProcessor({ exporter }),
    ],
  });
  return new OtelTracing(provider);
}

/** Tracing config from the shared env vars (TRACING_ENABLED, OTEL_*). */
export function tracingConfigFromEnv(
  env: {
    TRACING_ENABLED: boolean;
    OTEL_EXPORTER_OTLP_ENDPOINT?: string | undefined;
    OTEL_SERVICE_NAME?: string | undefined;
    OTEL_TRACES_SAMPLER_RATIO: number;
  },
  defaultServiceName: string,
): TracingConfig {
  return {
    enabled: env.TRACING_ENABLED,
    serviceName: env.OTEL_SERVICE_NAME ?? defaultServiceName,
    endpoint: env.OTEL_EXPORTER_OTLP_ENDPOINT,
    samplerRatio: env.OTEL_TRACES_SAMPLER_RATIO,
  };
}

/** Record an error on a span (status ERROR + exception event). */
export function recordSpanError(span: Span, err: unknown): void {
  if (!span.isRecording()) return;
  if (err instanceof Error) span.recordException(err);
  span.setStatus({
    code: SpanStatusCode.ERROR,
    message: err instanceof Error ? err.message : String(err),
  });
}
