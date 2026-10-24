/**
 * Per-key leading + trailing edge coalescing.
 *
 * The first trigger for a key fires at once (no added latency for the common
 * single enqueue); triggers inside the following window collapse into one
 * trailing fire at the end of it, which then opens a new window. So a key
 * fires at most about twice per window, however hot it is, and the last
 * trigger is never dropped: a change made after the leading fire is always
 * followed by another fire.
 */
export class KeyedCoalescer {
  private readonly windows = new Map<string, { timer: NodeJS.Timeout; pending: boolean }>();

  constructor(
    private readonly windowMs: number,
    private readonly fire: (key: string) => void,
  ) {}

  trigger(key: string): void {
    const window = this.windows.get(key);
    if (window) {
      window.pending = true;
      return;
    }
    this.fire(key);
    this.open(key);
  }

  /** Fire every pending trailing edge now and close all windows (graceful shutdown). */
  flush(): number {
    let fired = 0;
    const windows = [...this.windows];
    this.windows.clear();
    for (const [key, window] of windows) {
      clearTimeout(window.timer);
      if (window.pending) {
        this.fire(key);
        fired++;
      }
    }
    return fired;
  }

  /** Keys with an open window (tests, metrics). */
  get openWindows(): number {
    return this.windows.size;
  }

  private open(key: string): void {
    const timer = setTimeout(() => {
      const window = this.windows.get(key);
      this.windows.delete(key);
      if (window?.pending) {
        this.fire(key);
        this.open(key);
      }
    }, this.windowMs);
    // Never keep a process alive just to deliver a hint.
    timer.unref();
    this.windows.set(key, { timer, pending: false });
  }
}
