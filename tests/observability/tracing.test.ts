import { InMemorySpanExporter } from '@opentelemetry/sdk-trace';
import { afterEach, describe, expect, it } from 'vitest';
import {
  createTracing,
  DISABLED_TRACING,
  recordSpanError,
  SpanKind,
  SpanStatusCode,
  TRACEPARENT_PATTERN,
  tracingConfigFromEnv,
  type Tracing,
} from '../../src/observability/tracing.js';

describe('DISABLED_TRACING (the default)', () => {
  it('is a no-op: non-recording spans, no traceparent, no log fields', async () => {
    const span = DISABLED_TRACING.startSpan('x', { kind: SpanKind.SERVER });
    expect(span.isRecording()).toBe(false);
    expect(DISABLED_TRACING.traceparentOf(span)).toBeNull();
    expect(DISABLED_TRACING.logFields(span)).toBeNull();
    // Error recording and ending are safe on the no-op span.
    recordSpanError(span, new Error('boom'));
    span.end();
    await DISABLED_TRACING.shutdown();
  });

  it('createTracing({enabled:false}) returns it without needing an endpoint', () => {
    expect(createTracing({ enabled: false, serviceName: 's' })).toBe(DISABLED_TRACING);
  });
});

describe('createTracing (enabled)', () => {
  let exporter: InMemorySpanExporter;
  let tracing: Tracing;

  afterEach(async () => {
    await tracing?.shutdown();
  });

  function enable(samplerRatio = 1): Tracing {
    exporter = new InMemorySpanExporter();
    tracing = createTracing({ enabled: true, serviceName: 'test', exporter, samplerRatio });
    return tracing;
  }

  it('round-trips a span through its traceparent: the child joins the same trace', () => {
    const t = enable();
    const producer = t.startSpan('enqueue', { kind: SpanKind.PRODUCER });
    const traceparent = t.traceparentOf(producer);
    expect(traceparent).toMatch(TRACEPARENT_PATTERN);
    producer.end();

    const consumer = t.startSpan('execute', {
      kind: SpanKind.CONSUMER,
      parent: t.contextFromTraceparent(traceparent),
    });
    consumer.end();

    const [p, c] = exporter.getFinishedSpans();
    expect(c!.spanContext().traceId).toBe(p!.spanContext().traceId);
    expect(c!.parentSpanContext?.spanId).toBe(p!.spanContext().spanId);
    expect(traceparent).toBe(`00-${p!.spanContext().traceId}-${p!.spanContext().spanId}-01`);
  });

  it('starts a new trace for a missing or malformed traceparent', () => {
    const t = enable();
    for (const bad of [null, undefined, '', 'garbage', '00-zz-11-01']) {
      const span = t.startSpan('execute', { parent: t.contextFromTraceparent(bad) });
      span.end();
    }
    for (const span of exporter.getFinishedSpans()) expect(span.parentSpanContext).toBeUndefined();
  });

  it('contextWithSpan parents children in-process', () => {
    const t = enable();
    const parent = t.startSpan('request', { kind: SpanKind.SERVER });
    const child = t.startSpan('enqueue', { parent: t.contextWithSpan(parent) });
    child.end();
    parent.end();
    const [c] = exporter.getFinishedSpans();
    expect(c!.parentSpanContext?.spanId).toBe(parent.spanContext().spanId);
  });

  it('exposes trace_id/span_id for log correlation', () => {
    const t = enable();
    const span = t.startSpan('x');
    expect(t.logFields(span)).toEqual({
      trace_id: span.spanContext().traceId,
      span_id: span.spanContext().spanId,
    });
    span.end();
  });

  it('an unsampled root span stores no traceparent (ratio 0)', () => {
    const t = enable(0);
    const span = t.startSpan('enqueue');
    expect(span.isRecording()).toBe(false);
    expect(t.traceparentOf(span)).toBeNull();
    span.end();
    expect(exporter.getFinishedSpans()).toHaveLength(0);
  });

  it('recordSpanError sets ERROR status and an exception event', () => {
    const t = enable();
    const span = t.startSpan('x');
    recordSpanError(span, new Error('boom'));
    span.end();
    const [s] = exporter.getFinishedSpans();
    expect(s!.status).toEqual({ code: SpanStatusCode.ERROR, message: 'boom' });
    expect(s!.events.map((e) => e.name)).toContain('exception');
    expect(s!.resource.attributes['service.name']).toBe('test');
  });

  it('refuses to start without an endpoint or exporter', () => {
    expect(() => createTracing({ enabled: true, serviceName: 's' })).toThrow(/OTLP endpoint/);
  });
});

describe('tracingConfigFromEnv', () => {
  it('maps env vars and falls back to the process default service name', () => {
    expect(
      tracingConfigFromEnv(
        {
          TRACING_ENABLED: true,
          OTEL_EXPORTER_OTLP_ENDPOINT: 'http://jaeger:4318',
          OTEL_TRACES_SAMPLER_RATIO: 0.5,
        },
        'pulsequeue-worker',
      ),
    ).toEqual({
      enabled: true,
      serviceName: 'pulsequeue-worker',
      endpoint: 'http://jaeger:4318',
      samplerRatio: 0.5,
    });
  });
});
