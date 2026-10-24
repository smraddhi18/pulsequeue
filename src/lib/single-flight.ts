/**
 * In-process request coalescing: concurrent calls with the same key share
 * one execution of `fn` (and its result or error). The entry is removed when
 * the call settles, so the next call after that runs `fn` again; this
 * deduplicates concurrent work, it does not cache.
 */
export class SingleFlight<T> {
  private readonly inFlight = new Map<string, Promise<T>>();

  /**
   * Join the execution in flight for `key`, or start one. With
   * `fresh: true` always start a new execution (callers that need a result
   * computed *after* this moment, not one that started earlier); later
   * callers join the fresh one.
   */
  run(key: string, fn: () => Promise<T>, options: { fresh?: boolean } = {}): Promise<T> {
    const existing = this.inFlight.get(key);
    if (existing && !options.fresh) return existing;
    const promise = Promise.resolve().then(fn);
    this.inFlight.set(key, promise);
    const cleanup = () => {
      // A fresh execution may have replaced ours; only remove our own entry.
      if (this.inFlight.get(key) === promise) this.inFlight.delete(key);
    };
    promise.then(cleanup, cleanup);
    return promise;
  }

  get size(): number {
    return this.inFlight.size;
  }
}
