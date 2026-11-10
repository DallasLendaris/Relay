import type { PushTarget } from '../contracts.js';

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
 *
 * Those two process-wide rules cannot see a misconfiguration of ONE platform (an APNs key or
 * topic, one FCM project) under 20% of the traffic, nor judge the first couple of hundred results
 * or a misconfiguration that starts mid-alert in small batches. So every breaker also keeps
 * PlatformWindows (`platforms`): per platform key (ios, expo, android:<FCM project>) the last
 * PLATFORM_WINDOW and the last PLATFORM_SHORT_WINDOW results, judged with a small-sample rule
 * (isSystemic: at least SYSTEMIC_MIN_PERMANENT permanents AND a 95% Wilson lower bound on the
 * permanent rate above SYSTEMIC_RATE). The sender retries permanents of a systemic platform
 * instead of treating them as dead, and DeadTargetGate (dead-target-gate.ts) holds every clear
 * for a short delay and re-checks the target's platform before clearing.
 *
 * Only fresh traffic is evidence. A re-check batch (a key with an `:h`, `:v` or `:u` segment:
 * recipients already answered permanent once while a breaker said their platform looked
 * misconfigured, sender.ts carriesEvidence) is about 100% permanent when the tokens really are
 * dead, so recording its results would make a healthy platform look systemic and re-queue more
 * re-checks: a self-feeding loop. The sender records nothing of those batches. They are judged
 * by DeadTargetGate (dead-target-gate.ts, "Re-check verdicts") on positive fresh evidence:
 * `healthyAfter` before the re-check was sent and fresh results after it, never on the absence
 * of evidence (a cold process, a process that saw only the healthy past, no traffic at all).
 *
 * Staleness: a platform window that has recorded nothing for PLATFORM_EVIDENCE_TTL_MS is reset
 * at its next result, so a verdict left by the end of one alert's traffic (a chance "systemic"
 * or a healthy one) never judges the next alert's first results.
 */
export const BATCH_RATE_LIMIT = 0.25;
export const BATCH_MIN_PERMANENT = 20;
export const ROLLING_WINDOW = 1000;
export const ROLLING_RATE_LIMIT = 0.2;
export const ROLLING_MIN_SAMPLES = 200;
export const ROLLING_MIN_PERMANENT = 20;

/** Per-platform windows: the last 200 results (rate) and the last 20 (a fresh onset). */
export const PLATFORM_WINDOW = 200;
export const PLATFORM_SHORT_WINDOW = 20;
/** isSystemic: a permanent rate whose 95% Wilson lower bound is above 20%... */
export const SYSTEMIC_RATE = 0.2;
/** ...from at least 5 permanents. */
export const SYSTEMIC_MIN_PERMANENT = 5;
/** A platform window idle this long is stale: its next result starts it afresh. */
export const PLATFORM_EVIDENCE_TTL_MS = 10 * 60_000;
const WILSON_Z = 1.96;

/** Lower bound of the 95% Wilson score interval for k successes in n trials. */
export function wilsonLower(k: number, n: number, z = WILSON_Z): number {
  if (n <= 0) return 0;
  const p = k / n;
  const z2 = z * z;
  const centre = p + z2 / (2 * n);
  const margin = z * Math.sqrt((p * (1 - p)) / n + z2 / (4 * n * n));
  return Math.max(0, (centre - margin) / (1 + z2 / n));
}

/**
 * The small-sample rule. Examples: 5 of 5 (lower bound 0.57), 60 of 60, 8 of the last 20 (0.22)
 * and 55 of 200 (0.22) are systemic; 4 of 4 (too few), 7 of 20 (0.18), 3 of 60 and 10 of 200
 * (normal 5% churn) are not. At 5% churn the short window reaches 8 of 20 with probability
 * ~3e-6 per result, so a 1M-device fan-out (about 1M results over all windows) sees it about once.
 * Such a false alarm cannot feed itself (re-check results never feed the windows). Its dead tokens
 * are re-checked and cleared once fresh traffic proves the platform healthy around the re-check;
 * if fresh traffic has ended, each is pushed at most 1 + MAX_BLIND_RECHECKS more times and kept
 * (dead-target-gate.ts "Re-check verdicts").
 */
export function isSystemic(permanents: number, samples: number): boolean {
  return permanents >= SYSTEMIC_MIN_PERMANENT && wilsonLower(permanents, samples) > SYSTEMIC_RATE;
}

/** Fixed-size ring of the last results: 1 = permanent. Optionally keeps each result's time. */
class Ring {
  private readonly ring: Uint8Array;
  private readonly times: Float64Array | undefined;
  private head = 0;
  filled = 0;
  permanents = 0;
  constructor(
    private readonly size: number,
    timed = false,
  ) {
    this.ring = new Uint8Array(size);
    this.times = timed ? new Float64Array(size) : undefined;
  }
  push(v: 0 | 1, atMs = 0): void {
    if (this.filled === this.size) this.permanents -= this.ring[this.head] ?? 0;
    else this.filled++;
    this.ring[this.head] = v;
    if (this.times) this.times[this.head] = atMs;
    this.permanents += v;
    this.head = (this.head + 1) % this.size;
  }
  /** Time of the oldest result in the ring (a timed ring that is full); else undefined. */
  get oldestMs(): number | undefined {
    if (!this.times || this.filled < this.size) return undefined;
    return this.times[this.head];
  }
  get systemic(): boolean {
    return isSystemic(this.permanents, this.filled);
  }
}

/** The breaker's notion of "a platform": FCM projects are separate (own keys and quotas). */
export const platformKeyOf = (target: PushTarget): string =>
  target.platform === 'android' ? `android:${target.project ?? 'default'}` : target.platform;

/**
 * Rolling windows per platform key. A handful of keys (platforms x FCM projects), each a fixed
 * ~400 bytes, so memory is bounded by the configuration.
 */
export class PlatformWindows {
  private readonly windows = new Map<
    string,
    {
      long: Ring;
      short: Ring;
      seq: number;
      lastSystemicSeq: number;
      lastSystemicMs: number;
      newestMs: number;
    }
  >();

  /**
   * Records one fresh result at `nowMs` (the sender passes its clock; without it the result has
   * no time, so it never counts as evidence for healthyAfter and never ages out).
   */
  record(key: string, permanent: boolean, nowMs = 0): void {
    let w = this.windows.get(key);
    if (!w) {
      w = {
        long: new Ring(PLATFORM_WINDOW),
        short: new Ring(PLATFORM_SHORT_WINDOW, true),
        seq: 0,
        lastSystemicSeq: -1,
        lastSystemicMs: -Infinity,
        newestMs: nowMs,
      };
      this.windows.set(key, w);
    } else if (nowMs - w.newestMs > PLATFORM_EVIDENCE_TTL_MS) {
      // Stale: start afresh, verdict included (no held batch is that old: holds last 30 s), so a
      // batch that began on the stale state is not judged by it. seq keeps counting, and
      // lastSystemicMs stays as the time of the last suspicion (only ever a stricter bound).
      w.long = new Ring(PLATFORM_WINDOW);
      w.short = new Ring(PLATFORM_SHORT_WINDOW, true);
      w.lastSystemicSeq = -1;
    }
    const v = permanent ? 1 : 0;
    w.long.push(v);
    w.short.push(v, nowMs);
    w.seq++;
    w.newestMs = Math.max(w.newestMs, nowMs);
    if (w.long.systemic || w.short.systemic) {
      w.lastSystemicSeq = w.seq;
      w.lastSystemicMs = Math.max(w.lastSystemicMs, nowMs);
    }
  }

  /** Whether this platform's recent results look like a misconfiguration (either window). */
  systemic(key: string): boolean {
    const w = this.windows.get(key);
    return w !== undefined && (w.long.systemic || w.short.systemic);
  }

  /**
   * Results recorded for this platform so far; result number `seq() + 1` is the next one.
   * `systemicSince(key, seq(key))` also counts the platform's state right now.
   */
  seq(key: string): number {
    return this.windows.get(key)?.seq ?? 0;
  }

  /**
   * Whether the platform looked systemic at any point from result number `from` on. A held clear
   * whose failure was followed by a systemic stretch must not be cleared even if the
   * misconfiguration was fixed (and the window healed) before the hold ended.
   */
  systemicSince(key: string, from: number): boolean {
    const w = this.windows.get(key);
    return w !== undefined && w.lastSystemicSeq >= from;
  }

  /** Number of the last result that left the platform systemic (-1: never). */
  lastSystemicSeq(key: string): number {
    return this.windows.get(key)?.lastSystemicSeq ?? -1;
  }

  /** Time of the last result that left the platform systemic (-Infinity: never). */
  lastSystemicMs(key: string): number {
    return this.windows.get(key)?.lastSystemicMs ?? -Infinity;
  }

  /**
   * Positive evidence that the platform is healthy after `afterMs`: the whole short window
   * (PLATFORM_SHORT_WINDOW fresh results) was recorded strictly after `afterMs` and after the
   * last systemic verdict, neither window is systemic now, and the newest result is not older
   * than `newestAtLeastMs`. A cold, stale or quiet window is never healthy: no evidence is not
   * evidence of health.
   */
  healthyAfter(key: string, afterMs: number, newestAtLeastMs = -Infinity): boolean {
    const w = this.windows.get(key);
    if (!w || w.long.systemic || w.short.systemic || w.newestMs < newestAtLeastMs) return false;
    const oldest = w.short.oldestMs;
    // Locally, order by result number (a batch's results share one timestamp); `afterMs` comes
    // from another clock (or process), so only time can order it.
    const oldestSeq = w.seq - w.short.filled + 1;
    return oldest !== undefined && oldest > afterMs && oldestSeq > w.lastSystemicSeq;
  }

  stats(key: string): { samples: number; permanents: number; recentPermanents: number } {
    const w = this.windows.get(key);
    return {
      samples: w?.long.filled ?? 0,
      permanents: w?.long.permanents ?? 0,
      recentPermanents: w?.short.permanents ?? 0,
    };
  }
}

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
  /** Per-platform windows; the sender records every result here too (see the file comment). */
  readonly platforms = new PlatformWindows();

  /** Records a batch's results, then judges the batch. */
  record(total: number, permanent: number): BreakerVerdict {
    for (let i = 0; i < total; i++) this.push(i < permanent ? 1 : 0);
    const batchRate = total === 0 ? 0 : permanent / total;
    const rollingRate = this.filled === 0 ? 0 : this.permanents / this.filled;
    let reason: BreakerVerdict['reason'];
    if (permanent >= BATCH_MIN_PERMANENT && batchRate > BATCH_RATE_LIMIT) reason = 'batch_rate';
    else if (this.rollingOpen) reason = 'rolling_rate';
    return { open: reason !== undefined, ...(reason ? { reason } : {}), batchRate, rollingRate };
  }

  /** The rolling rule on the results recorded so far, without recording anything. */
  get rollingOpen(): boolean {
    return (
      this.filled >= ROLLING_MIN_SAMPLES &&
      this.permanents >= ROLLING_MIN_PERMANENT &&
      this.rollingRate > ROLLING_RATE_LIMIT
    );
  }

  /** Results in the rolling window (it only grows, from the very first result, up to ROLLING_WINDOW). */
  get samples(): number {
    return this.filled;
  }
  /** Permanent results in the rolling window. */
  get permanentSamples(): number {
    return this.permanents;
  }
  get rollingRate(): number {
    return this.filled === 0 ? 0 : this.permanents / this.filled;
  }
  private push(v: 0 | 1): void {
    if (this.filled === ROLLING_WINDOW) this.permanents -= this.ring[this.head] ?? 0;
    else this.filled++;
    this.ring[this.head] = v;
    this.permanents += v;
    this.head = (this.head + 1) % ROLLING_WINDOW;
  }
}
