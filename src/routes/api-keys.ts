import type { FastifyInstance, FastifyReply } from 'fastify';
import type { ApiKey, IssuedApiKey } from '../domain/tenant.js';
import { principalOf } from '../plugins/auth.js';
import type { ApiKeyService } from '../services/api-key.service.js';
import {
  apiKeyParamsSchema,
  createApiKeyBodySchema,
  listApiKeysQuerySchema,
  rotateApiKeyBodySchema,
} from './api-keys.schemas.js';

/** Public JSON contract for an API key. The secret hash never appears; the token only on issue. */
export interface ApiKeyResponse {
  id: string;
  tenantId: string;
  name: string;
  prefix: string;
  role: ApiKey['role'];
  createdAt: string;
  expiresAt: string | null;
  lastUsedAt: string | null;
  revokedAt: string | null;
  rotatedFrom: string | null;
}

export function toApiKeyResponse(key: ApiKey): ApiKeyResponse {
  return {
    id: key.id,
    tenantId: key.tenantId,
    name: key.name,
    prefix: key.prefix,
    role: key.role,
    createdAt: key.createdAt.toISOString(),
    expiresAt: key.expiresAt?.toISOString() ?? null,
    lastUsedAt: key.lastUsedAt?.toISOString() ?? null,
    revokedAt: key.revokedAt?.toISOString() ?? null,
    rotatedFrom: key.rotatedFrom,
  };
}

/** 201 with the one-time plaintext token; never cached by clients or proxies. */
function sendIssued(reply: FastifyReply, issued: IssuedApiKey): FastifyReply {
  reply.header('cache-control', 'no-store');
  reply.header('location', `/api-keys/${issued.key.id}`);
  return reply.status(201).send({ ...toApiKeyResponse(issued.key), token: issued.token });
}

/**
 * API key management for the caller's own tenant (admin keys only).
 * Bootstrap — the first admin key of a tenant — is the api-key CLI.
 */
export async function apiKeyRoutes(
  app: FastifyInstance,
  opts: { apiKeyService: ApiKeyService },
): Promise<void> {
  const { apiKeyService } = opts;

  /** Identify the calling key (any role): handy for debugging client config. */
  app.get('/auth/whoami', { config: { access: 'read' } }, async (request, reply) => {
    const principal = principalOf(request);
    return reply.status(200).send({
      tenantId: principal.tenantId,
      keyId: principal.keyId,
      keyPrefix: principal.keyPrefix,
      role: principal.role,
    });
  });

  app.post('/api-keys', { config: { access: 'manage' } }, async (request, reply) => {
    const { tenantId } = principalOf(request);
    const body = createApiKeyBodySchema.parse(request.body);
    const issued = await apiKeyService.create({
      tenantId,
      name: body.name,
      role: body.role,
      ...(body.expiresAt !== undefined ? { expiresAt: body.expiresAt } : {}),
    });
    return sendIssued(reply, issued);
  });

  app.get('/api-keys', { config: { access: 'manage' } }, async (request, reply) => {
    const { tenantId } = principalOf(request);
    const query = listApiKeysQuerySchema.parse(request.query);
    const page = await apiKeyService.list({
      tenantId,
      includeRevoked: query.includeRevoked,
      limit: query.limit,
      ...(query.cursor !== undefined ? { cursor: query.cursor } : {}),
    });
    return reply
      .status(200)
      .send({ data: page.keys.map(toApiKeyResponse), nextCursor: page.nextCursor });
  });

  app.get('/api-keys/:id', { config: { access: 'manage' } }, async (request, reply) => {
    const { tenantId } = principalOf(request);
    const { id } = apiKeyParamsSchema.parse(request.params);
    return reply.status(200).send(toApiKeyResponse(await apiKeyService.get(tenantId, id)));
  });

  /** Revoke (idempotent). Revoking the calling key itself is allowed. */
  app.delete('/api-keys/:id', { config: { access: 'manage' } }, async (request, reply) => {
    const { tenantId } = principalOf(request);
    const { id } = apiKeyParamsSchema.parse(request.params);
    return reply.status(200).send(toApiKeyResponse(await apiKeyService.revoke(tenantId, id)));
  });

  app.post('/api-keys/:id/rotate', { config: { access: 'manage' } }, async (request, reply) => {
    const { tenantId } = principalOf(request);
    const { id } = apiKeyParamsSchema.parse(request.params);
    const body = rotateApiKeyBodySchema.parse(request.body ?? {});
    const issued = await apiKeyService.rotate({
      tenantId,
      id,
      gracePeriodSeconds: body.gracePeriodSeconds,
      ...(body.expiresAt !== undefined ? { expiresAt: body.expiresAt } : {}),
    });
    return sendIssued(reply, issued);
  });
}
