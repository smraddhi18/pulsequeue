import { request } from 'node:http';
import pino from 'pino';
import { Counter, Registry } from 'prom-client';
import { afterEach, describe, expect, it } from 'vitest';
import { MetricsServer } from '../../src/observability/metrics-server.js';

const logger = pino({ level: 'silent' });
const TOKEN = 'metrics-token-0123456789';

interface Res {
  status: number;
  headers: Record<string, string | string[] | undefined>;
  body: string;
}

function call(
  port: number,
  { method = 'GET', path = '/metrics', headers = {} as Record<string, string> } = {},
): Promise<Res> {
  return new Promise((resolve, reject) => {
    const req = request({ host: '127.0.0.1', port, method, path, headers }, (res) => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', (chunk: string) => (body += chunk));
      res.on('end', () => resolve({ status: res.statusCode ?? 0, headers: res.headers, body }));
    });
    req.on('error', reject);
    req.end();
  });
}

describe('MetricsServer', () => {
  const servers: MetricsServer[] = [];

  afterEach(async () => {
    for (const server of servers.splice(0)) await server.stop();
  });

  async function start(token?: string, registry = new Registry()): Promise<MetricsServer> {
    new Counter({ name: 'test_things_total', help: 'things', registers: [registry] }).inc(3);
    const server = new MetricsServer({
      registry,
      host: '127.0.0.1',
      port: 0,
      ...(token !== undefined ? { token } : {}),
      logger,
    });
    servers.push(server);
    await server.start();
    return server;
  }

  it('serves the registry in the Prometheus text format', async () => {
    const server = await start();
    const res = await call(server.port);
    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toMatch(/text\/plain/);
    expect(res.body).toContain('test_things_total 3');
  });

  it('answers HEAD without a body', async () => {
    const server = await start();
    const res = await call(server.port, { method: 'HEAD' });
    expect(res.status).toBe(200);
    expect(res.body).toBe('');
  });

  it('404s every other path and 405s other methods', async () => {
    const server = await start();
    expect((await call(server.port, { path: '/' })).status).toBe(404);
    expect((await call(server.port, { path: '/metrics/x' })).status).toBe(404);
    const post = await call(server.port, { method: 'POST' });
    expect(post.status).toBe(405);
    expect(post.headers.allow).toBe('GET, HEAD');
  });

  describe('with METRICS_TOKEN', () => {
    it('rejects a missing or wrong bearer token with 401 and no metrics', async () => {
      const server = await start(TOKEN);
      const missing = await call(server.port);
      expect(missing.status).toBe(401);
      expect(missing.headers['www-authenticate']).toMatch(/^Bearer/);
      expect(missing.body).not.toContain('test_things_total');

      const wrong = await call(server.port, {
        headers: { authorization: `Bearer ${TOKEN.slice(0, -1)}x` },
      });
      expect(wrong.status).toBe(401);
      const short = await call(server.port, { headers: { authorization: 'Bearer abc' } });
      expect(short.status).toBe(401);
      const basic = await call(server.port, { headers: { authorization: `Basic ${TOKEN}` } });
      expect(basic.status).toBe(401);
    });

    it('serves metrics with the right token', async () => {
      const server = await start(TOKEN);
      const res = await call(server.port, { headers: { authorization: `Bearer ${TOKEN}` } });
      expect(res.status).toBe(200);
      expect(res.body).toContain('test_things_total 3');
    });
  });

  it('returns 500 when a collect callback throws, and keeps serving', async () => {
    const registry = new Registry();
    let fail = true;
    new Counter({
      name: 'test_flaky_total',
      help: 'flaky',
      registers: [registry],
      collect() {
        if (fail) throw new Error('boom');
      },
    });
    const server = await start(undefined, registry);
    expect((await call(server.port)).status).toBe(500);
    fail = false;
    expect((await call(server.port)).status).toBe(200);
  });

  it('start() is idempotent and stop() can be called twice', async () => {
    const server = await start();
    const port = server.port;
    await server.start();
    expect(server.port).toBe(port);
    await server.stop();
    await server.stop();
  });
});
