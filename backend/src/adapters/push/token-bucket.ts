/**
 * Token bucket: `take(n)` resolves once n permits are available, refilling at `ratePerSecond`
 * up to `burst` (default one second of rate). One bucket per Firebase project keeps a project's
 * quota from making any other project wait.
 *
 * Callers are served in order. A request larger than the burst is allowed: it takes the bucket
 * into debt and later callers wait for the debt to refill, so the long-run rate is still capped.
 */
export class TokenBucket {
  private tokens: number;
  private last: number;
  private burst: number;
  private rate: number;

  constructor(
    ratePerSecond: number,
    burst: number = ratePerSecond,
    private readonly now: () => number = Date.now,
    private readonly sleep: (ms: number) => Promise<void> = (ms) =>
      new Promise((resolve) => setTimeout(resolve, ms)),
  ) {
    if (!Number.isFinite(ratePerSecond) || ratePerSecond <= 0) {
      throw new RangeError('ratePerSecond must be > 0');
    }
    if (!Number.isFinite(burst) || burst < 1) throw new RangeError('burst must be >= 1');
    this.rate = ratePerSecond;
    this.burst = burst;
    this.tokens = burst;
    this.last = now();
  }

  /** The current refill rate in permits per second. */
  get ratePerSecond(): number {
    return this.rate;
  }

  /**
   * Changes the rate and burst (default one second of the new rate). Refills at the old rate up to
   * now first, then clamps the tokens to the new capacity; debt is kept, so lowering the rate
   * never forgives permits already reserved.
   */
  setRate(ratePerSecond: number, burst: number = ratePerSecond): void {
    if (!Number.isFinite(ratePerSecond) || ratePerSecond <= 0) {
      throw new RangeError('ratePerSecond must be > 0');
    }
    if (!Number.isFinite(burst) || burst < 1) throw new RangeError('burst must be >= 1');
    this.refill();
    this.rate = ratePerSecond;
    this.burst = burst;
    this.tokens = Math.min(this.tokens, burst);
  }

  /** Permits available right now (negative while in debt). */
  available(): number {
    this.refill();
    return this.tokens;
  }

  /** Reserves n permits and waits until they are covered. */
  async take(n: number): Promise<void> {
    if (n <= 0) return;
    this.refill();
    // Reserve first, then wait for the debt to refill: concurrent callers queue behind each other.
    this.tokens -= n;
    if (this.tokens >= 0) return;
    await this.sleep(Math.ceil((-this.tokens / this.rate) * 1000));
  }

  private refill(): void {
    const t = this.now();
    const elapsed = Math.max(0, t - this.last);
    this.last = t;
    this.tokens = Math.min(this.burst, this.tokens + (elapsed / 1000) * this.rate);
  }
}
