import type { Logger } from '../../delivery/types.js';
import type { PushTarget } from '../contracts.js';
import { ROLLING_MIN_SAMPLES, type BreakerVerdict, type PermanentBreaker } from './breaker.js';
import type { DeadTargetItem, DeadTargetQueue } from './dead-target-queue.js';
import { emfLine, type MetricWriter } from './metrics.js';

/** Longest a dead target waits for evidence before the gate decides on what it has. */
export const DEAD_TARGET_HOLD_MS = 60_000;

export interface DeadTargetGateOptions {
  breaker: PermanentBreaker;
  /** Where released dead targets go (the bounded cleanup queue). */
  queue: Pick<DeadTargetQueue, 'enqueue'>;
  logger: Logger;
  writeMetric?: MetricWriter;
  /** Results the breaker must have seen (all batches, from the first result) before clears pass. Default ROLLING_MIN_SAMPLES (200). */
  minSamples?: number;
  /** Default DEAD_TARGET_HOLD_MS (60 s). */
  holdMs?: number;
  now?: () => number;
  /** Injectable for tests. Default setTimeout, unref'd. */
  setTimer?: (fn: () => void, ms: number) => { cancel(): void };
}

/**
 * Holds dead-target clears until the circuit breaker has enough evidence.
 *
 * A misconfiguration (wrong bundle id, sandbox/production mismatch) makes every token look dead.
 * The breaker catches it once it has seen enough results, but most batches hold only a few
 * recipients, so the first couple of hundred results arrive before either breaker rule can
 * judge. Clearing their "dead" targets at once wiped the push targets of hundreds of devices per
 * misconfigured sender. So, while the breaker has seen fewer than `minSamples` results:
 *
 *   - dead targets are held here (never more than `minSamples` of them: each is one result);
 *   - as soon as the breaker has `minSamples` results and the batch verdict is closed, everything
 *     held is released to the cleanup queue, and from then on dead targets pass straight through;
 *   - if the breaker opens (either rule), everything held is dropped: never cleared, counted as
 *     DeadTargetHeldDropped. Those recipients keep their tokens; a later alert finds them again;
 *   - if `holdMs` passes first (little traffic), the gate decides on what it has: drop when the
 *     window looks systemic (>= 20 permanents and > 20%), otherwise release. The next dead
 *     targets start a new hold. shutdown() makes the same decision at once.
 *
 * Normal sending is never delayed: the gate only defers the asynchronous cleanup.
 */
export class DeadTargetGate {
  private held: DeadTargetItem[] = [];
  private timer: { cancel(): void } | undefined;
  private readonly minSamples: number;
  private readonly holdMs: number;
  private readonly write: MetricWriter;
  private readonly setTimer: NonNullable<DeadTargetGateOptions['setTimer']>;

  constructor(private readonly opts: DeadTargetGateOptions) {
    this.minSamples = opts.minSamples ?? ROLLING_MIN_SAMPLES;
    this.holdMs = opts.holdMs ?? DEAD_TARGET_HOLD_MS;
    this.write = opts.writeMetric ?? ((l) => console.log(l));
    this.setTimer =
      opts.setTimer ??
      ((fn, ms) => {
        const t = setTimeout(fn, ms);
        t.unref();
        return { cancel: () => clearTimeout(t) };
      });
  }

  get heldCount(): number {
    return this.held.length;
  }

  /**
   * Called once per settled batch, after the breaker recorded it, with that batch's dead targets
   * (empty when the verdict is open: the sender retries them instead).
   */
  submit(
    dead: ReadonlyArray<{ deviceId: string; target: PushTarget }>,
    verdict: BreakerVerdict,
  ): void {
    if (verdict.open) {
      this.drop('breaker_open');
      return;
    }
    if (this.opts.breaker.samples >= this.minSamples) {
      this.release('evidence');
      for (const d of dead) this.opts.queue.enqueue(d.deviceId, d.target);
      return;
    }
    if (dead.length === 0) return;
    for (const d of dead) this.held.push({ deviceId: d.deviceId, target: d.target });
    this.timer ??= this.setTimer(() => {
      this.timer = undefined;
      this.decide('hold_timeout');
    }, this.holdMs);
  }

  /** Graceful shutdown: decide on the held targets now (before the cleanup queue is flushed). */
  shutdown(): void {
    this.decide('shutdown');
  }

  private decide(why: 'hold_timeout' | 'shutdown'): void {
    if (this.held.length === 0) return;
    if (this.opts.breaker.looksSystemic) this.drop(why);
    else this.release(why);
  }

  private take(): DeadTargetItem[] {
    this.timer?.cancel();
    this.timer = undefined;
    const items = this.held;
    this.held = [];
    return items;
  }

  private release(why: string): void {
    if (this.held.length === 0) return;
    const items = this.take();
    for (const d of items) this.opts.queue.enqueue(d.deviceId, d.target);
    this.opts.logger.info('sender.dead_targets_released', {
      count: items.length,
      reason: why,
      samples: this.opts.breaker.samples,
      rollingRate: Number(this.opts.breaker.rollingRate.toFixed(4)),
    });
  }

  private drop(why: string): void {
    if (this.held.length === 0) return;
    const items = this.take();
    this.opts.logger.error('sender.dead_targets_dropped', {
      count: items.length,
      reason: why,
      samples: this.opts.breaker.samples,
      rollingRate: Number(this.opts.breaker.rollingRate.toFixed(4)),
    });
    this.write(emfLine('DeadTargetHeldDropped', items.length));
  }
}
