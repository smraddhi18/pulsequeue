import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import { installFatalHandlers, type ProcessLike } from '../src/lib/process-handlers.js';

function setup() {
  const proc = new EventEmitter();
  const logger = { fatal: vi.fn() };
  const exit = vi.fn();
  installFatalHandlers(logger, { proc: proc as unknown as ProcessLike, exit });
  return { proc, logger, exit };
}

describe('installFatalHandlers', () => {
  it('logs an unhandled rejection as one fatal line and exits 1', () => {
    const { proc, logger, exit } = setup();
    const reason = new Error('lost promise');
    proc.emit('unhandledRejection', reason);

    expect(logger.fatal).toHaveBeenCalledWith(
      { event: 'process.unhandled_rejection', err: reason },
      'Unhandled promise rejection, exiting',
    );
    expect(exit).toHaveBeenCalledWith(1);
  });

  it('logs an uncaught exception with its origin and exits 1', () => {
    const { proc, logger, exit } = setup();
    const err = new Error('boom');
    proc.emit('uncaughtException', err, 'uncaughtException');

    expect(logger.fatal).toHaveBeenCalledWith(
      { event: 'process.uncaught_exception', err, origin: 'uncaughtException' },
      'Uncaught exception, exiting',
    );
    expect(exit).toHaveBeenCalledWith(1);
  });

  it('exits once even if a second error arrives while exiting', () => {
    const { proc, logger, exit } = setup();
    proc.emit('unhandledRejection', new Error('first'));
    proc.emit('uncaughtException', new Error('second'), 'uncaughtException');
    expect(logger.fatal).toHaveBeenCalledTimes(2);
    expect(exit).toHaveBeenCalledTimes(1);
  });

  it('still exits when the logger itself throws', () => {
    const proc = new EventEmitter();
    const exit = vi.fn();
    const logger = {
      fatal: vi.fn(() => {
        throw new Error('stdout closed');
      }),
    };
    installFatalHandlers(logger, { proc: proc as unknown as ProcessLike, exit });
    expect(() => proc.emit('unhandledRejection', 'plain string reason')).toThrow('stdout closed');
    expect(exit).toHaveBeenCalledWith(1);
  });
});
