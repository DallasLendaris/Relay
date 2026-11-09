/**
 * Circuit breaker on permanent failures.
 *
 * A sandbox/production mismatch or a wrong bundle id makes APNs call EVERY token dead. Clearing
 * them would wipe everyone's push target, so when failures look systemic we do not clear.
 *
 * Opens (for the batch being judged) when either:
 *   - more than 2% of the batch's results are permanent AND at least 5 are, or
 *   - the rolling permanent rate over the last 1,000 results (process-wide) is above 5%, once
 *     the window holds at least 100 results and 5 permanents (so a tiny first batch with one
 *     genuinely dead token cannot trip it).
 */
export const BATCH_RATE_LIMIT = 0.02;
export const BATCH_MIN_PERMANENT = 5;
export const ROLLING_WINDOW = 1000;
export const ROLLING_RATE_LIMIT = 0.05;
export const ROLLING_MIN_SAMPLES = 100;

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
      this.permanents >= BATCH_MIN_PERMANENT &&
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
