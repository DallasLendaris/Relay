/**
 * Circuit breaker on permanent failures.
 *
 * A sandbox/production mismatch or a wrong bundle id makes APNs call EVERY token dead. Clearing
 * them would wipe everyone's push target, so when failures look systemic we do not clear.
 *
 * The breaker targets that systemic case, where nearly 100% of results are permanent. It must not
 * trip on ordinary token churn: real apps routinely see 1-5% dead tokens, and the 1M simulation
 * at a 0.5% rate opened an earlier, tighter (2% per batch, 5% rolling) version 6 times on batches
 * of 121-238. A false open delays clearing and retries dead tokens, so the thresholds sit far
 * above normal churn and far below a misconfiguration.
 *
 * Opens (for the batch being judged) when either:
 *   - at least 20 of the batch's results are permanent AND they are more than 25% of the batch, or
 *   - the rolling permanent rate over the last 1,000 results (process-wide) is above 20%, once
 *     the window holds at least 200 results and 20 permanents (so a small first batch with a few
 *     genuinely dead tokens cannot trip it).
 */
export const BATCH_RATE_LIMIT = 0.25;
export const BATCH_MIN_PERMANENT = 20;
export const ROLLING_WINDOW = 1000;
export const ROLLING_RATE_LIMIT = 0.2;
export const ROLLING_MIN_SAMPLES = 200;
export const ROLLING_MIN_PERMANENT = 20;

export interface BreakerVerdict {
  open: boolean;
  reason?: 'batch_rate' | 'rolling_rate';
  batchRate: number;
  rollingRate: number;
}

export class PermanentBreaker {
  /** Ring buffer of the last ROLLING_WINDOW results: 1 = permanent. */
  private readonly ring = new Uint8Array(ROLLING_WINDOW);
  private head = 0;
  private filled = 0;
  private permanents = 0;

  /** Records a batch's results, then judges the batch. */
  record(total: number, permanent: number): BreakerVerdict {
    for (let i = 0; i < total; i++) this.push(i < permanent ? 1 : 0);
    const batchRate = total === 0 ? 0 : permanent / total;
    const rollingRate = this.filled === 0 ? 0 : this.permanents / this.filled;
    let reason: BreakerVerdict['reason'];
    if (permanent >= BATCH_MIN_PERMANENT && batchRate > BATCH_RATE_LIMIT) reason = 'batch_rate';
    else if (
      this.filled >= ROLLING_MIN_SAMPLES &&
      this.permanents >= ROLLING_MIN_PERMANENT &&
      rollingRate > ROLLING_RATE_LIMIT
    ) {
      reason = 'rolling_rate';
    }
    return { open: reason !== undefined, ...(reason ? { reason } : {}), batchRate, rollingRate };
  }

  private push(v: 0 | 1): void {
    if (this.filled === ROLLING_WINDOW) this.permanents -= this.ring[this.head] ?? 0;
    else this.filled++;
    this.ring[this.head] = v;
    this.permanents += v;
    this.head = (this.head + 1) % ROLLING_WINDOW;
  }
}
