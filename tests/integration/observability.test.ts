import { InMemorySpanExporter } from '@opentelemetry/sdk-trace';
import type { FastifyInstance } from 'fastify';
import pino from 'pino';
import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import { buildApp } from '../../src/app.js';
import { loadWorkerEnv } from '../../src/config/env.js';
import { createTracing, SpanKind, type Tracing } from '../../src/observability/tracing.js';
import { NonRetryableError } from '../../src/worker/errors.js';
import { HandlerRegistry } from '../../src/worker/registry.js';
import { createWorkerRuntime, type WorkerRuntime } from '../../src/worker/runtime.js';
import {
  createSchemaPool,
  createTestDatabase,
  describeWithDb,
  testEnv,
  waitUntil,
  type TestDatabase,
} from './helpers.js';
import { authedInject, createTestApiKey, type Inject } from './auth-helpers.js';

const TOKEN = 'test-metrics-token-0123456789';

async function scrape(port: number, token: string | null = TOKEN): Promise<Response> {
  return fetch(`http://127.0.0.1:${port}/metrics`, {
    headers: token === null ? {} : { authorization: `Bearer ${token}` },
  });
}

/** Value of the first sample of `name` whose labels include `labels` (Prometheus text format). */
function valueOf(
  text: string,
  name: string,
  labels: Record<string, string> = {},
): number | undefined {
  for (const line of text.split('\n')) {
    if (line.startsWith('#')) continue;
    const m = /^([a-zA-Z_:][\w:]*)(?:\{(.*)\})? (\S+)$/.exec(line);
    if (!m || m[1] !== name) continue;
    const have = new Map<string, string>();
    for (const pair of (m[2] ?? '').matchAll(/(\w+)="((?:[^"\\]|\\.)*)"/g))
      have.set(pair[1]!, pair[2]!);
    if (Object.entries(labels).every(([k, v]) => have.get(k) === v)) return Number(m[3]);
  }
  return undefined;
}

class LineCapture {
  readonly lines: Array<Record<string, unknown>> = [];
  write(line: string): void {
    this.lines.push(JSON.parse(line) as Record<string, unknown>);
  }
}

/**
 * Milestone 7 end to end on real Postgres: the API and worker /metrics
 * listeners (access control, content), the metric lifecycle across
 * enqueue -> execute -> fail -> DLQ, the queue-depth gauge, and W3C trace
 * propagation from an HTTP caller through jobs.traceparent into the
 * worker's execution span.
 */
describeWithDb('observability (Postgres)', { timeout: 30_000 }, () => {
  let db: TestDatabase;
  let producerToken: string;
  const cleanups: Array<() => Promise<unknown>> = [];

  beforeAll(async () => {
    db = await createTestDatabase();
    producerToken = (await createTestApiKey(db.pool, { role: 'producer' })).token;
  });

  afterAll(async () => {
    await db?.destroy();
  });

  beforeEach(async () => {
    await db.pool.query('TRUNCATE jobs, job_attempts, queues RESTART IDENTITY CASCADE');
  });

  afterEach(async () => {
    for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  });

  async function startApi(
    overrides: Record<string, string> = {},
    options: { tracing?: Tracing; logStream?: LineCapture } = {},
  ): Promise<{ app: FastifyInstance; api: Inject; metricsPort: number }> {
    const app = await buildApp({
      env: testEnv({
        METRICS_PORT: '0',
        METRICS_TOKEN: TOKEN,
        METRICS_QUEUE_DEPTH_CACHE_MS: '0',
        ...overrides,
      }),
      logger: options.logStream !== undefined,
      pool: db.pool,
      ...(options.tracing ? { tracing: options.tracing } : {}),
      ...(options.logStream ? { logStream: options.logStream } : {}),
    });
    cleanups.push(() => app.close());
    await app.ready();
    const server = app.observability.metricsServer;
    if (!server) throw new Error('metrics server expected');
    return { app, api: authedInject(app, producerToken), metricsPort: server.port };
  }

  async function startWorker(
    handlers: Record<string, () => Promise<void>>,
    options: { tracing?: Tracing; logger?: pino.Logger } = {},
  ): Promise<WorkerRuntime> {
    const registry = new HandlerRegistry();
    for (const [queue, handler] of Object.entries(handlers)) registry.register(queue, handler);
    const pool = createSchemaPool(db, 4);
    const runtime = createWorkerRuntime(
      loadWorkerEnv({
        NODE_ENV: 'test',
        DATABASE_URL: db.url,
        LOG_LEVEL: 'silent',
        WORKER_POLL_INTERVAL_MS: '20',
        WORKER_CONCURRENCY: '2',
        WORKER_SHUTDOWN_TIMEOUT_MS: '1000',
        RETRY_BACKOFF_BASE_MS: '10',
        RETRY_BACKOFF_MAX_MS: '20',
        METRICS_PORT: '0',
        METRICS_TOKEN: TOKEN,
        PG_POOL_MAX: '4',
      }),
      {
        registry,
        logger: options.logger ?? pino({ level: 'silent' }),
        pool,
        ...(options.tracing ? { tracing: options.tracing } : {}),
      },
    );
    cleanups.push(async () => {
      await runtime.stop();
      await pool.end();
    });
    runtime.start();
    await runtime.observability.metricsServer!.start();
    return runtime;
  }

  const jobRow = async (id: string) =>
    (
      await db.pool.query<{ status: string; traceparent: string | null }>(
        'SELECT status, traceparent FROM jobs WHERE id = $1',
        [id],
      )
    ).rows[0]!;

  it('serves API metrics on the metrics port only, behind the bearer token', async () => {
    const { app, api, metricsPort } = await startApi();
    await api({ method: 'POST', url: '/queues/emails/jobs', payload: { payload: {} } });
    await api({ method: 'GET', url: '/no/such/route' });

    expect((await scrape(metricsPort, null)).status).toBe(401);
    expect((await scrape(metricsPort, 'wrong-token-wrong-token')).status).toBe(401);
    const res = await scrape(metricsPort);
    expect(res.status).toBe(200);
    const text = await res.text();
    // Route templates, never raw URLs: bounded label cardinality.
    expect(
      valueOf(text, 'pulsequeue_http_request_duration_seconds_count', {
        method: 'POST',
        route: '/queues/:name/jobs',
        status_code: '201',
        role: 'api',
      }),
    ).toBe(1);
    expect(
      valueOf(text, 'pulsequeue_http_request_duration_seconds_count', {
        route: '__unmatched__',
        status_code: '404',
      }),
    ).toBe(1);
    expect(text).not.toContain('/no/such/route');

    // The public API port does not expose metrics at all.
    const main = await app.inject({ method: 'GET', url: '/metrics' });
    expect(main.statusCode).toBe(404);
    expect(main.body).not.toContain('pulsequeue_');
  });

  it('reports queue depth per tenant, queue and status from Postgres', async () => {
    const { api, metricsPort } = await startApi();
    for (let i = 0; i < 3; i++) {
      await api({ method: 'POST', url: '/queues/emails/jobs', payload: { payload: { i } } });
    }
    await api({
      method: 'POST',
      url: '/queues/emails/jobs',
      payload: { payload: {}, delayMs: 60_000 },
    });
    const text = await (await scrape(metricsPort)).text();
    const depth = (status: string) =>
      valueOf(text, 'pulsequeue_queue_depth', { tenant: 'default', queue: 'emails', status });
    expect(depth('queued')).toBe(3);
    expect(depth('scheduled')).toBe(1);
    expect(depth('running')).toBe(0);
    expect(depth('dead')).toBe(0);
  });

  it('can turn the queue-depth gauge off', async () => {
    const { api, metricsPort } = await startApi({ METRICS_QUEUE_DEPTH_ENABLED: 'false' });
    await api({ method: 'POST', url: '/queues/emails/jobs', payload: { payload: {} } });
    expect(await (await scrape(metricsPort)).text()).not.toContain('pulsequeue_queue_depth');
  });

  it('counts the lifecycle: enqueue -> execute -> fail -> retry -> dead-letter', async () => {
    const { api, metricsPort: apiPort } = await startApi();
    const runtime = await startWorker({
      ok: async () => undefined,
      flaky: async () => {
        throw new Error('always fails');
      },
      poison: async () => {
        throw new NonRetryableError('bad payload');
      },
    });

    const ids: string[] = [];
    const enqueue = async (
      queue: string,
      body: Record<string, unknown>,
      headers: Record<string, string> = {},
    ) => {
      const res = await api({
        method: 'POST',
        url: `/queues/${queue}/jobs`,
        payload: body,
        headers,
      });
      expect(res.statusCode).toBeLessThan(300);
      ids.push(res.json<{ id: string }>().id);
    };
    await enqueue('ok', { payload: {} });
    await enqueue('flaky', { payload: {}, maxAttempts: 2 });
    await enqueue('poison', { payload: {} });
    // Idempotent replay: counted as a duplicate, not a second job.
    await enqueue('ok', { payload: {} }, { 'idempotency-key': 'k1' });
    await enqueue('ok', { payload: {} }, { 'idempotency-key': 'k1' });

    await waitUntil(async () => {
      const statuses = await Promise.all(ids.map(async (id) => (await jobRow(id)).status));
      return statuses.every((s) => s === 'succeeded' || s === 'dead');
    });

    const apiText = await (await scrape(apiPort)).text();
    const enq = (queue: string, result: string) =>
      valueOf(apiText, 'pulsequeue_jobs_enqueued_total', { tenant: 'default', queue, result });
    expect(enq('ok', 'created')).toBe(2);
    expect(enq('ok', 'duplicate')).toBe(1);
    expect(enq('flaky', 'created')).toBe(1);
    expect(valueOf(apiText, 'pulsequeue_ratelimit_decisions_total', { outcome: 'skipped' })).toBe(
      5,
    );

    const res = await scrape(runtime.observability.metricsServer!.port);
    expect(res.status).toBe(200);
    const text = await res.text();
    const w = (name: string, labels: Record<string, string>) =>
      valueOf(text, name, { role: 'worker', tenant: 'default', ...labels });

    expect(w('pulsequeue_jobs_completed_total', { queue: 'ok' })).toBe(2);
    expect(w('pulsequeue_jobs_leased_total', { queue: 'flaky' })).toBe(2);
    expect(
      w('pulsequeue_jobs_failed_total', {
        queue: 'flaky',
        outcome: 'failed',
        disposition: 'retrying',
      }),
    ).toBe(1);
    expect(
      w('pulsequeue_jobs_failed_total', {
        queue: 'flaky',
        outcome: 'failed',
        disposition: 'dead_lettered',
      }),
    ).toBe(1);
    expect(
      w('pulsequeue_jobs_dead_lettered_total', {
        queue: 'flaky',
        reason: 'attempts_exhausted',
        source: 'worker',
      }),
    ).toBe(1);
    expect(
      w('pulsequeue_jobs_dead_lettered_total', {
        queue: 'poison',
        reason: 'non_retryable',
        source: 'worker',
      }),
    ).toBe(1);
    expect(w('pulsequeue_job_execution_seconds_count', { queue: 'ok', outcome: 'succeeded' })).toBe(
      2,
    );
    // Queue wait is measured on the database clock for every lease.
    expect(w('pulsequeue_job_queue_wait_seconds_count', { queue: 'flaky' })).toBe(2);
    expect(valueOf(text, 'pulsequeue_worker_jobs_in_flight')).toBe(0);
    // Process metrics are off in tests (NODE_ENV=test) to keep scrapes small.
    expect(text).not.toContain('pulsequeue_process_cpu');

    // The same story from the dashboard's side: attempts carry queue_id/ready_at.
    const { rows } = await db.pool.query<{ n: number }>(
      'SELECT count(*)::int AS n FROM job_attempts WHERE queue_id IS NULL OR ready_at IS NULL',
    );
    expect(rows[0]!.n).toBe(0);
  });

  it('propagates traceparent: HTTP caller -> enqueue span -> jobs.traceparent -> worker span', async () => {
    const apiSpans = new InMemorySpanExporter();
    const workerSpans = new InMemorySpanExporter();
    const apiTracing = createTracing({ enabled: true, serviceName: 'api', exporter: apiSpans });
    const workerTracing = createTracing({
      enabled: true,
      serviceName: 'worker',
      exporter: workerSpans,
    });
    cleanups.push(
      () => apiTracing.shutdown(),
      () => workerTracing.shutdown(),
    );
    const apiLogs = new LineCapture();
    const workerLogs = new LineCapture();
    const { api } = await startApi(
      { LOG_LEVEL: 'info' },
      { tracing: apiTracing, logStream: apiLogs },
    );

    const callerTraceId = '4bf92f3577b34da6a3ce929d0e0e4736';
    const callerSpanId = '00f067aa0ba902b7';
    const res = await api({
      method: 'POST',
      url: '/queues/traced/jobs',
      payload: { payload: {} },
      headers: { traceparent: `00-${callerTraceId}-${callerSpanId}-01` },
    });
    expect(res.statusCode).toBe(201);
    const jobId = res.json<{ id: string }>().id;

    const server = apiSpans.getFinishedSpans().find((s) => s.kind === SpanKind.SERVER)!;
    const enqueue = apiSpans.getFinishedSpans().find((s) => s.name === 'pulsequeue.enqueue')!;
    expect(server.spanContext().traceId).toBe(callerTraceId);
    expect(server.parentSpanContext?.spanId).toBe(callerSpanId);
    expect(server.attributes['http.route']).toBe('/queues/:name/jobs');
    expect(enqueue.kind).toBe(SpanKind.PRODUCER);
    expect(enqueue.parentSpanContext?.spanId).toBe(server.spanContext().spanId);
    expect(enqueue.attributes['messaging.message.id']).toBe(jobId);

    // The row stores the enqueue span's context, not the caller's.
    const stored = (await jobRow(jobId)).traceparent;
    expect(stored).toBe(`00-${callerTraceId}-${enqueue.spanContext().spanId}-01`);

    // Request logs carry trace_id for log <-> trace correlation.
    expect(apiLogs.lines.some((l) => l.trace_id === callerTraceId)).toBe(true);

    await startWorker(
      { traced: async () => undefined },
      { tracing: workerTracing, logger: pino({ level: 'info' }, workerLogs) },
    );
    await waitUntil(async () => (await jobRow(jobId)).status === 'succeeded');
    await waitUntil(() => workerSpans.getFinishedSpans().length > 0);

    const [execute] = workerSpans.getFinishedSpans();
    expect(execute!.name).toBe('job.execute');
    expect(execute!.kind).toBe(SpanKind.CONSUMER);
    expect(execute!.spanContext().traceId).toBe(callerTraceId);
    expect(execute!.parentSpanContext?.spanId).toBe(enqueue.spanContext().spanId);
    expect(execute!.attributes['pulsequeue.outcome']).toBe('succeeded');
    const attemptLog = workerLogs.lines.find((l) => l.msg === 'Job attempt succeeded');
    expect(attemptLog).toMatchObject({
      jobId,
      trace_id: callerTraceId,
      span_id: execute!.spanContext().spanId,
    });
  });

  it('records failed attempts as errored spans', async () => {
    const exporter = new InMemorySpanExporter();
    const tracing = createTracing({ enabled: true, serviceName: 'all', exporter });
    cleanups.push(() => tracing.shutdown());
    const { api } = await startApi({}, { tracing });
    const res = await api({
      method: 'POST',
      url: '/queues/broken/jobs',
      payload: { payload: {}, maxAttempts: 1 },
    });
    const jobId = res.json<{ id: string }>().id;
    await startWorker(
      {
        broken: async () => {
          throw new Error('kaput');
        },
      },
      { tracing },
    );
    await waitUntil(async () => (await jobRow(jobId)).status === 'dead');
    await waitUntil(() => exporter.getFinishedSpans().some((s) => s.name === 'job.execute'));
    const span = exporter.getFinishedSpans().find((s) => s.name === 'job.execute')!;
    expect(span.status).toMatchObject({ code: 2, message: 'kaput' });
    expect(span.attributes).toMatchObject({
      'pulsequeue.outcome': 'failed',
      'pulsequeue.disposition': 'dead_lettered',
    });
  });

  it('tracing off (the default) is a no-op: nothing stored, even with an inbound traceparent', async () => {
    const { api } = await startApi();
    const res = await api({
      method: 'POST',
      url: '/queues/plain/jobs',
      payload: { payload: {} },
      headers: { traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01' },
    });
    expect(res.statusCode).toBe(201);
    const jobId = res.json<{ id: string }>().id;
    expect((await jobRow(jobId)).traceparent).toBeNull();
    await startWorker({ plain: async () => undefined });
    await waitUntil(async () => (await jobRow(jobId)).status === 'succeeded');
  });
});
