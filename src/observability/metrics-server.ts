import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import type { Registry } from 'prom-client';
import type { StructuredLogger } from '../lib/logger.js';

export interface MetricsServerOptions {
  registry: Registry;
  host: string;
  /** 0 = ephemeral (tests read `port` after start). */
  port: number;
  /** When set, scrapes must send `Authorization: Bearer <token>`. */
  token?: string | undefined;
  logger: StructuredLogger;
}

/**
 * GET /metrics on a dedicated listener (Milestone 7).
 *
 * Why not a route on the API: the API is deny-by-default with tenant API
 * keys, and metrics are cross-tenant operational data, so no tenant role
 * should read them. A separate port keeps that decision structural: the
 * listener is bound to loopback (or a private interface plus a token), the
 * API port 404s /metrics, and workers and schedulers, which have no HTTP
 * server, expose metrics exactly the same way.
 *
 * Deliberately not Fastify: four branches, no plugins, nothing that can be
 * misconfigured into serving more than this one path.
 */
export class MetricsServer {
  private server: Server | null = null;
  private starting: Promise<void> | null = null;
  private readonly expected: Buffer | null;

  constructor(private readonly options: MetricsServerOptions) {
    this.expected =
      options.token !== undefined ? Buffer.from(`Bearer ${options.token}`, 'utf8') : null;
  }

  /** Bound port (meaningful after start). */
  get port(): number {
    const address = this.server?.address();
    return address !== null && typeof address === 'object'
      ? (address as AddressInfo).port
      : this.options.port;
  }

  /** Idempotent: concurrent and repeated calls share one listen. */
  start(): Promise<void> {
    this.starting ??= this.listen().catch((err: unknown) => {
      this.starting = null;
      throw err;
    });
    return this.starting;
  }

  private async listen(): Promise<void> {
    const server = createServer((req, res) => {
      void this.handle(req, res);
    });
    // A scrape is one short request: no reason to keep idle sockets around.
    server.keepAliveTimeout = 5_000;
    server.requestTimeout = 10_000;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(this.options.port, this.options.host, () => {
        server.off('error', reject);
        resolve();
      });
    });
    this.server = server;
    this.options.logger.info(
      {
        event: 'metrics.listening',
        host: this.options.host,
        port: this.port,
        auth: this.expected !== null ? 'bearer' : 'none',
      },
      'metrics endpoint listening',
    );
  }

  async stop(): Promise<void> {
    await this.starting?.catch(() => undefined);
    const server = this.server;
    this.starting = null;
    if (server === null) return;
    this.server = null;
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    });
  }

  private authorized(req: IncomingMessage): boolean {
    if (this.expected === null) return true;
    const header = req.headers.authorization;
    if (typeof header !== 'string') return false;
    const given = Buffer.from(header, 'utf8');
    // timingSafeEqual needs equal lengths; the length itself is not secret.
    return given.length === this.expected.length && timingSafeEqual(given, this.expected);
  }

  private async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const path = (req.url ?? '/').split('?', 1)[0];
    if (path !== '/metrics') return send(res, 404, 'not found\n');
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      res.setHeader('allow', 'GET, HEAD');
      return send(res, 405, 'method not allowed\n');
    }
    if (!this.authorized(req)) {
      res.setHeader('www-authenticate', 'Bearer realm="metrics"');
      return send(res, 401, 'unauthorized\n');
    }
    try {
      const body = await this.options.registry.metrics();
      res.setHeader('content-type', this.options.registry.contentType);
      res.statusCode = 200;
      res.end(req.method === 'HEAD' ? undefined : body);
    } catch (err) {
      this.options.logger.error(
        { event: 'metrics.collect_failed', err },
        'metrics collection failed',
      );
      send(res, 500, 'metrics collection failed\n');
    }
  }
}

function send(res: ServerResponse, status: number, body: string): void {
  res.statusCode = status;
  res.setHeader('content-type', 'text/plain; charset=utf-8');
  res.end(body);
}
