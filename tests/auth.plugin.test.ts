import Fastify, { type FastifyInstance } from 'fastify';
import { afterEach, describe, expect, it } from 'vitest';
import { UnauthorizedError } from '../src/domain/errors.js';
import type { ApiKeyRole, Principal, TenantId } from '../src/domain/tenant.js';
import { principalOf, registerAuth } from '../src/plugins/auth.js';
import { registerErrorHandler } from '../src/plugins/error-handler.js';
import type { ApiKeyService } from '../src/services/api-key.service.js';

/** Stub: the token "role:<role>" authenticates as that role; anything else is 401. */
function stubKeys(): ApiKeyService & { calls: number } {
  const stub = {
    calls: 0,
    async authenticate(header: string | undefined): Promise<Principal> {
      stub.calls++;
      const role = /^Bearer role:(admin|producer|reader)$/.exec(header ?? '')?.[1];
      if (!role) throw new UnauthorizedError('Invalid API key');
      return {
        tenantId: 'acme' as TenantId,
        keyId: '00000000-0000-4000-8000-000000000001',
        keyPrefix: 'abcdefghijkl',
        role: role as ApiKeyRole,
        tenantRateLimit: null,
      };
    },
  };
  return stub as unknown as ApiKeyService & { calls: number };
}

describe('auth plugin', () => {
  let app: FastifyInstance | undefined;
  let keys: ApiKeyService & { calls: number };

  afterEach(async () => {
    await app?.close();
    app = undefined;
  });

  async function start(): Promise<FastifyInstance> {
    keys = stubKeys();
    app = Fastify({ logger: false });
    await registerErrorHandler(app);
    await registerAuth(app, { apiKeyService: keys });
    app.get('/open', { config: { access: 'public' } }, async (request) => ({
      principal: request.principal,
    }));
    app.get('/things', { config: { access: 'read' } }, async (request) => ({
      tenantId: principalOf(request).tenantId,
    }));
    app.post('/things', { config: { access: 'enqueue' } }, async () => ({ ok: true }));
    app.delete('/things', { config: { access: 'manage' } }, async () => ({ ok: true }));
    await app.ready();
    return app;
  }

  const as = (role: string) => ({ authorization: `Bearer role:${role}` });

  it('lets public routes through without credentials or a key lookup', async () => {
    const a = await start();
    const res = await a.inject({ method: 'GET', url: '/open' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ principal: null });
    expect(keys.calls).toBe(0);
  });

  it('answers 401 with a Bearer challenge when credentials are missing or wrong', async () => {
    const a = await start();
    for (const headers of [{}, { authorization: 'Bearer nope' }]) {
      const res = await a.inject({ method: 'GET', url: '/things', headers });
      expect(res.statusCode).toBe(401);
      expect(res.headers['www-authenticate']).toBe('Bearer realm="pulsequeue"');
      expect(res.json()).toMatchObject({ error: { code: 'UNAUTHORIZED' } });
    }
  });

  it('sets the principal for the handler', async () => {
    const a = await start();
    const res = await a.inject({ method: 'GET', url: '/things', headers: as('reader') });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ tenantId: 'acme' });
  });

  it.each([
    ['reader', 'GET', 200],
    ['reader', 'POST', 403],
    ['reader', 'DELETE', 403],
    ['producer', 'GET', 200],
    ['producer', 'POST', 200],
    ['producer', 'DELETE', 403],
    ['admin', 'GET', 200],
    ['admin', 'POST', 200],
    ['admin', 'DELETE', 200],
  ] as const)('%s %s /things -> %i', async (role, method, status) => {
    const a = await start();
    const res = await a.inject({ method, url: '/things', headers: as(role) });
    expect(res.statusCode).toBe(status);
    if (status === 403) {
      expect(res.json()).toMatchObject({ error: { code: 'FORBIDDEN', details: { role } } });
      expect(res.headers['www-authenticate']).toBeUndefined();
    }
  });

  it('leaves unknown routes to the 404 handler without a key lookup', async () => {
    const a = await start();
    const res = await a.inject({ method: 'GET', url: '/nope' });
    expect(res.statusCode).toBe(404);
    expect(keys.calls).toBe(0);
  });

  it('refuses to start when a route does not declare an access policy', async () => {
    const a = Fastify({ logger: false });
    app = a;
    await registerAuth(a, { apiKeyService: stubKeys() });
    expect(() => a.get('/forgot', async () => ({}))).toThrow(/must declare config.access/);
  });
});
