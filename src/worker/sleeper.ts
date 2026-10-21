/**
 * A sleep that can be cut short. The worker loop sleeps between polls and is
 * woken early when a slot frees up or shutdown starts.
 */
export class Sleeper {
  private pending: { timer: NodeJS.Timeout; resolve: () => void } | null = null;

  sleep(ms: number): Promise<void> {
    this.wake();
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pending = null;
        resolve();
      }, ms);
      this.pending = { timer, resolve };
    });
  }

  wake(): void {
    const pending = this.pending;
    if (!pending) return;
    this.pending = null;
    clearTimeout(pending.timer);
    pending.resolve();
  }
}

/** Spread `ms` by +-ratio so many workers started together do not poll in lockstep. */
export function jitter(ms: number, ratio = 0.2, random: () => number = Math.random): number {
  const delta = ms * ratio * (random() * 2 - 1);
  return Math.max(1, Math.round(ms + delta));
}
