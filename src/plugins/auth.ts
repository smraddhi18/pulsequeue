import type { FastifyInstance, FastifyRequest } from 'fastify';
import { ForbiddenError, UnauthorizedError } from '../domain/errors.js';
import { roleAllows, type Permission, type Principal } from '../domain/tenant.js';
import type { ApiKeyService } from '../services/api-key.service.js';

/**
 * Who may call a route: 'public' (no credentials: liveness/readiness probes)
 * or the permission an API key's role must grant. Declared on every route via
 * `config: { access }`; a route without one fails app startup, so a new
 * endpoint can never ship unauthenticated by omission.
 */
export type RouteAccess = 'public' | Permission;

declare module 'fastify' {
  interface FastifyContextConfig {
    access?: RouteAccess;
  }
  interface FastifyRequest {
    /** Set by the auth hook on every non-public route; null on public ones. */
    principal: Principal | null;
  }
}

/**
 * Authentication and authorization, in an onRequest hook: it runs before the
 * body is read or parsed, so an unauthenticated client cannot make the server
 * buffer, parse or validate anything.
 *
 * 401 = no usable credentials (missing, malformed, unknown, revoked, expired);
 * 403 = valid key whose role lacks the route's permission. Tenant isolation is
 * not decided here: services scope every query by principal.tenantId, and
 * another tenant's resources are 404s.
 */
export async function registerAuth(
  app: FastifyInstance,
  opts: { apiKeyService: ApiKeyService },
): Promise<void> {
  app.decorateRequest('principal', null);

  app.addHook('onRoute', (route) => {
    const access = route.config?.access;
    if (access === undefined) {
      throw new Error(
        `Route ${String(route.method)} ${route.url} must declare config.access ('public' or a permission)`,
      );
    }
  });

  app.addHook('onRequest', async (request) => {
    // Unknown routes have no config: let the 404 handler answer.
    if (request.is404) return;
    const access = request.routeOptions.config.access;
    if (access === 'public') return;
    if (access === undefined) throw new Error('Route has no access policy');

    const principal = await opts.apiKeyService.authenticate(request.headers.authorization);
    request.principal = principal;
    request.log = request.log.child({
      tenantId: principal.tenantId,
      keyPrefix: principal.keyPrefix,
    });

    if (!roleAllows(principal.role, access)) {
      request.log.warn(
        { event: 'auth.forbidden', role: principal.role, required: access },
        'API key role does not allow this operation',
      );
      throw new ForbiddenError(`This API key's role (${principal.role}) cannot ${access}`, {
        role: principal.role,
        required: access,
      });
    }
  });
}

/** The authenticated caller of a non-public route (the auth hook guarantees it). */
export function principalOf(request: FastifyRequest): Principal {
  if (!request.principal) throw new UnauthorizedError('Not authenticated');
  return request.principal;
}
