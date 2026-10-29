import type { Logger } from 'pino';

/** The process surface we need; injectable so tests do not touch the real process. */
export interface ProcessLike {
  on(event: 'unhandledRejection', listener: (reason: unknown) => void): unknown;
  on(event: 'uncaughtException', listener: (err: Error, origin: string) => void): unknown;
}

export interface FatalHandlerOptions {
  /** Defaults to process.exit. */
  exit?: (code: number) => void;
  /** Defaults to the real process. */
  proc?: ProcessLike;
}

/**
 * Milestone 8: crash loudly and structurally on a programming error.
 *
 * Node already exits on an unhandled rejection, but it prints a bare stack to
 * stderr that log pipelines do not parse, and nothing says which process or
 * component died. We log one `fatal` JSON line (pino writes stdout
 * synchronously, so it lands before exit) and exit 1.
 *
 * Exiting, rather than logging and carrying on, is deliberate: after an
 * unexpected throw the process state is unknown (a half-updated in-memory
 * structure, a leaked lease loop). Crashing is safe by design: leases expire
 * and the reaper requeues the jobs, lease_token fencing rejects anything the
 * dead process might still have written, and the supervisor (Docker restart
 * policy, Kubernetes) starts a fresh process. A graceful drain is not
 * attempted, since it would run on top of the corrupted state.
 */
export function installFatalHandlers(
  logger: Pick<Logger, 'fatal'>,
  options: FatalHandlerOptions = {},
): void {
  const exit = options.exit ?? ((code: number) => process.exit(code));
  const proc: ProcessLike = options.proc ?? process;
  let exiting = false;

  const die = (event: string, err: unknown, extra: Record<string, unknown>, msg: string) => {
    try {
      logger.fatal({ event, err, ...extra }, msg);
    } finally {
      if (!exiting) {
        exiting = true;
        exit(1);
      }
    }
  };

  proc.on('unhandledRejection', (reason) => {
    die('process.unhandled_rejection', reason, {}, 'Unhandled promise rejection, exiting');
  });
  proc.on('uncaughtException', (err, origin) => {
    die('process.uncaught_exception', err, { origin }, 'Uncaught exception, exiting');
  });
}
