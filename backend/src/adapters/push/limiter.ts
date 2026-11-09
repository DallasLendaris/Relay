const noop = (): void => {};

/** Counting semaphore: caps how many async operations run at once. FIFO. */
export class Limiter {
  private active = 0;
  // Queue with a moving head: Array.shift() is O(n) and queues here hold tens of thousands.
  private waiters: Array<() => void> = [];
  private head = 0;

  constructor(readonly limit: number) {
    if (!Number.isInteger(limit) || limit < 1) throw new RangeError('limit must be >= 1');
  }

  get inFlight(): number {
    return this.active;
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    if (this.active >= this.limit) {
      // release() hands the slot straight to the next waiter, so `active` stays accurate.
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    } else {
      this.active++;
    }
    try {
      return await fn();
    } finally {
      const next = this.waiters[this.head];
      if (next) {
        this.waiters[this.head++] = noop; // release the closure
        if (this.head > 1024 && this.head * 2 > this.waiters.length) {
          this.waiters = this.waiters.slice(this.head);
          this.head = 0;
        }
        next();
      } else {
        this.active--;
      }
    }
  }
}
