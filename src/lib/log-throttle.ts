/**
 * Log at most once per `intervalMs` per key and count what was suppressed.
 * Redis outages produce one failure per command; without this, a dead Redis
 * under load would turn into thousands of identical warnings per second.
 */
export class LogThrottle {
  private readonly state = new Map<string, { lastAt: number; suppressed: number }>();

  constructor(
    private readonly intervalMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  /** `log: true` means emit now; `suppressed` is how many were skipped since the last one. */
  check(key: string): { log: boolean; suppressed: number } {
    const now = this.now();
    const entry = this.state.get(key);
    if (!entry || now - entry.lastAt >= this.intervalMs) {
      const suppressed = entry?.suppressed ?? 0;
      this.state.set(key, { lastAt: now, suppressed: 0 });
      return { log: true, suppressed };
    }
    entry.suppressed++;
    return { log: false, suppressed: entry.suppressed };
  }
}
