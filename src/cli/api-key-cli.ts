import { parseArgs } from 'node:util';
import { z } from 'zod';
import type { Queryable } from '../db/pool.js';
import { AppError } from '../domain/errors.js';
import {
  API_KEY_LIMITS,
  API_KEY_ROLES,
  TENANT_ID_PATTERN,
  type TenantId,
} from '../domain/tenant.js';
import { ApiKeyRepository } from '../repositories/api-key.repository.js';
import { TenantRepository } from '../repositories/tenant.repository.js';
import { toApiKeyResponse } from '../routes/api-keys.js';
import { ApiKeyService } from '../services/api-key.service.js';

/**
 * Operator CLI for tenants and API keys. This is how the first admin key of a
 * tenant is created (the HTTP API itself requires a key), and how operators
 * set tenant quotas, which tenant admins deliberately cannot change.
 *
 *   api-key create --tenant <id> --role admin|producer|reader --name <name>
 *                  [--expires-at <iso>] [--create-tenant]
 *   api-key list   --tenant <id> [--include-revoked]
 *   api-key revoke --tenant <id> --id <uuid>
 *   api-key tenant-create --tenant <id> --name <display name>
 *   api-key tenant-rate-limit --tenant <id> (--per-sec <n> --burst <n> | --clear)
 *
 * Results are JSON on stdout (the token appears once, in `create`); messages
 * go to stderr. Exit codes: 0 ok, 1 failed, 2 usage error.
 */

export interface CliIo {
  stdout: { write(chunk: string): unknown };
  stderr: { write(chunk: string): unknown };
}

export const USAGE = `Usage:
  api-key create --tenant <id> --role <admin|producer|reader> --name <name> [--expires-at <iso>] [--create-tenant]
  api-key list --tenant <id> [--include-revoked]
  api-key revoke --tenant <id> --id <uuid>
  api-key tenant-create --tenant <id> --name <display name>
  api-key tenant-rate-limit --tenant <id> (--per-sec <n> --burst <n> | --clear)
`;

class UsageError extends Error {}

const tenantIdSchema = z
  .string({ required_error: '--tenant is required' })
  .regex(TENANT_ID_PATTERN, '--tenant must be 1-63 chars of a-z, 0-9, "-" and start alphanumeric')
  .transform((v) => v as TenantId);

const positiveInt = (flag: string) =>
  z.coerce
    .number({ invalid_type_error: `${flag} must be a number` })
    .int()
    .positive()
    .max(1_000_000);

export async function runApiKeyCli(argv: string[], db: Queryable, io: CliIo): Promise<number> {
  const [command, ...rest] = argv;
  let values: Record<string, string | boolean | undefined>;
  try {
    ({ values } = parseArgs({
      args: rest,
      strict: true,
      options: {
        tenant: { type: 'string' },
        role: { type: 'string' },
        name: { type: 'string' },
        id: { type: 'string' },
        'expires-at': { type: 'string' },
        'create-tenant': { type: 'boolean' },
        'include-revoked': { type: 'boolean' },
        'per-sec': { type: 'string' },
        burst: { type: 'string' },
        clear: { type: 'boolean' },
      },
    }));
  } catch (err) {
    io.stderr.write(`${(err as Error).message}\n\n${USAGE}`);
    return 2;
  }

  const tenants = new TenantRepository(db);
  const keys = new ApiKeyService(new ApiKeyRepository(db), { cacheTtlMs: 0, cacheMaxEntries: 1 });
  const print = (value: unknown) => io.stdout.write(`${JSON.stringify(value, null, 2)}\n`);

  try {
    switch (command) {
      case 'create': {
        const input = z
          .object({
            tenant: tenantIdSchema,
            role: z.enum(API_KEY_ROLES, {
              errorMap: () => ({ message: '--role must be admin, producer or reader' }),
            }),
            name: z
              .string({ required_error: '--name is required' })
              .trim()
              .min(1)
              .max(API_KEY_LIMITS.NAME_MAX_LENGTH),
            'expires-at': z
              .string()
              .datetime({ offset: true, message: '--expires-at must be ISO-8601 with an offset' })
              .transform((v) => new Date(v))
              .optional(),
            'create-tenant': z.boolean().optional(),
          })
          .parse(values);
        if (!(await tenants.findById(input.tenant))) {
          if (!input['create-tenant']) {
            throw new UsageError(
              `Tenant "${input.tenant}" does not exist (pass --create-tenant or run tenant-create first)`,
            );
          }
          await tenants.create({ id: input.tenant, name: input.tenant });
          io.stderr.write(`Created tenant "${input.tenant}"\n`);
        }
        const issued = await keys.create({
          tenantId: input.tenant,
          role: input.role,
          name: input.name,
          ...(input['expires-at'] !== undefined ? { expiresAt: input['expires-at'] } : {}),
        });
        print({ ...toApiKeyResponse(issued.key), token: issued.token });
        io.stderr.write(
          'Store the token now: it is not saved anywhere and cannot be shown again.\n',
        );
        return 0;
      }
      case 'list': {
        const input = z
          .object({ tenant: tenantIdSchema, 'include-revoked': z.boolean().optional() })
          .parse(values);
        const all = [];
        let cursor: string | undefined;
        do {
          const page = await keys.list({
            tenantId: input.tenant,
            includeRevoked: input['include-revoked'] ?? false,
            limit: API_KEY_LIMITS.MAX_LIST_LIMIT,
            ...(cursor !== undefined ? { cursor } : {}),
          });
          all.push(...page.keys.map(toApiKeyResponse));
          cursor = page.nextCursor ?? undefined;
        } while (cursor !== undefined);
        print(all);
        return 0;
      }
      case 'revoke': {
        const input = z
          .object({
            tenant: tenantIdSchema,
            id: z.string({ required_error: '--id is required' }).uuid('--id must be a UUID'),
          })
          .parse(values);
        print(toApiKeyResponse(await keys.revoke(input.tenant, input.id)));
        return 0;
      }
      case 'tenant-create': {
        const input = z
          .object({
            tenant: tenantIdSchema,
            name: z.string({ required_error: '--name is required' }).trim().min(1).max(200),
          })
          .parse(values);
        print(await tenants.create({ id: input.tenant, name: input.name }));
        return 0;
      }
      case 'tenant-rate-limit': {
        const input = z
          .object({
            tenant: tenantIdSchema,
            'per-sec': positiveInt('--per-sec').optional(),
            burst: positiveInt('--burst').optional(),
            clear: z.boolean().optional(),
          })
          .parse(values);
        const perSec = input['per-sec'];
        const burst = input.burst;
        const clear = input.clear === true;
        if (clear === (perSec !== undefined || burst !== undefined)) {
          throw new UsageError('Pass either --per-sec and --burst, or --clear');
        }
        if (!clear && (perSec === undefined || burst === undefined)) {
          throw new UsageError('--per-sec and --burst go together');
        }
        const updated = await tenants.setRateLimit(
          input.tenant,
          clear ? null : { perSec: perSec!, burst: burst! },
        );
        if (!updated) throw new UsageError(`Tenant "${input.tenant}" does not exist`);
        print(updated);
        return 0;
      }
      default:
        io.stderr.write(
          `${command === undefined ? 'Missing command' : `Unknown command "${command}"`}\n\n${USAGE}`,
        );
        return 2;
    }
  } catch (err) {
    if (err instanceof UsageError) {
      io.stderr.write(`${err.message}\n`);
      return 2;
    }
    if (err instanceof z.ZodError) {
      io.stderr.write(`${err.issues.map((i) => i.message).join('\n')}\n\n${USAGE}`);
      return 2;
    }
    if (err instanceof AppError && err.isOperational && err.statusCode < 500) {
      const detail = err.details !== undefined ? ` ${JSON.stringify(err.details)}` : '';
      io.stderr.write(`${err.message}${detail}\n`);
      return 1;
    }
    throw err;
  }
}
