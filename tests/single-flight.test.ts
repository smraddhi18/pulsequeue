import { describe, expect, it } from 'vitest';
import { SingleFlight } from '../src/lib/single-flight.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('SingleFlight', () => {
  it('runs fn once for concurrent callers of a key and shares the result', async () => {
    const flights = new SingleFlight<number>();
    const gate = deferred<number>();
    let calls = 0;
    const fn = () => {
      calls++;
      return gate.promise;
    };
    const all = Promise.all(Array.from({ length: 20 }, () => flights.run('q', fn)));
    gate.resolve(42);
    expect(await all).toEqual(Array(20).fill(42));
    expect(calls).toBe(1);
    expect(flights.size).toBe(0);
  });

  it('shares errors too, and runs again once settled (no caching)', async () => {
    const flights = new SingleFlight<number>();
    let calls = 0;
    const failing = async () => {
      calls++;
      throw new Error('boom');
    };
    const results = await Promise.allSettled([
      flights.run('q', failing),
      flights.run('q', failing),
    ]);
    expect(results.map((r) => r.status)).toEqual(['rejected', 'rejected']);
    expect(calls).toBe(1);
    await expect(flights.run('q', async () => 7)).resolves.toBe(7);
  });

  it('keeps keys apart', async () => {
    const flights = new SingleFlight<string>();
    const [a, b] = await Promise.all([
      flights.run('a', async () => 'A'),
      flights.run('b', async () => 'B'),
    ]);
    expect([a, b]).toEqual(['A', 'B']);
  });

  it('fresh: never joins an execution that started earlier; later callers join the fresh one', async () => {
    const flights = new SingleFlight<string>();
    const first = deferred<string>();
    const second = deferred<string>();
    const old = flights.run('q', () => first.promise);
    const fresh = flights.run('q', () => second.promise, { fresh: true });
    const joiner = flights.run('q', async () => 'unused');
    first.resolve('old');
    expect(await old).toBe('old');
    // The old execution settling must not evict the fresh one.
    expect(flights.size).toBe(1);
    second.resolve('new');
    expect(await fresh).toBe('new');
    expect(await joiner).toBe('new');
    expect(flights.size).toBe(0);
  });

  it('turns a synchronous throw into a rejection', async () => {
    const flights = new SingleFlight<number>();
    await expect(
      flights.run('q', () => {
        throw new Error('sync');
      }),
    ).rejects.toThrow('sync');
  });
});
