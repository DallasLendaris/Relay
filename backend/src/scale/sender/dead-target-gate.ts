import type { Logger } from '../../delivery/types.js';
import type { SendBatch } from '../contracts.js';
import { ROLLING_MIN_SAMPLES, type BreakerVerdict, type PermanentBreaker } from './breaker.js';
import type { DeadTargetQueue } from './dead-target-queue.js';
import { emfLine, type MetricWriter } from './metrics.js';

/** Longest a dead target waits for evidence before the gate decides on what it has. */
export const DEAD_TARGET_HOLD_MS = 60_000;

type Recipient = SendBatch['recipients'][number];

/** What a held group keeps of its source batch: everything but the recipient list. */
export type HeldSource = Pick<
  SendBatch,
  'alertId' | 'sourceId' | 'relaySeverity' | 'batchKey' | 'attempt'
>;

/**
 * Dead targets held from one settled batch, with what is needed to re-queue them: the batch they
 * came from (without its recipients) and its retry deadline (alert end or 30 min budget).
 */
export interface HeldGroup {
  source: HeldSource;
  firstAttemptMs: number;
  deadlineMs: number;
  recipients: Recipient[];
}

/**
 * Re-queues held recipients as retry batches. Resolves with how many were re-queued; the rest
 * (retry budget spent, or the enqueue failed) are lost to this alert and must be logged there.
 */
export type HeldRequeuer = (groups: HeldGroup[]) => Promise<number>;

export interface DeadTargetGateOptions {
  breaker: PermanentBreaker;
  /** Where released dead targets go (the bounded cleanup queue). */
  queue: Pick<DeadTargetQueue, 'enqueue'>;
  /**
   * Where held recipients go when the breaker says the "dead" tokens were a misconfiguration.
   * Without it they are only dropped (counted as DeadTargetHeldDropped).
   */
  requeue?: HeldRequeuer;
  logger: Logger;
  writeMetric?: MetricWriter;
  /** Results the breaker must have seen (all batches, from the first result) before clears pass. Default ROLLING_MIN_SAMPLES (200). */
  minSamples?: number;
  /** Default DEAD_TARGET_HOLD_MS (60 s). */
  holdMs?: number;
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
 *   - if the breaker opens (either rule), nothing held is cleared: the recipients keep their
 *     tokens and are re-queued as retry batches (`requeue`, DeadTargetHeldRequeued), exactly like
 *     the recipients the open breaker turns into retries, so they are delivered once the
 *     misconfiguration is fixed, within the same 30-minute retry budget;
 *   - if `holdMs` passes first (little traffic), the gate decides on what it has: re-queue when
 *     the window looks systemic (>= 100 results, >= 20 permanents, > 20%), otherwise release. The next dead
 *     targets start a new hold. shutdown() makes the same decision at once; settle() then waits
 *     for the re-queue writes.
 *
 * Normal sending is never delayed: the gate only defers the asynchronous cleanup, and re-queues
 * in the background.
 */
export class DeadTargetGate {
  private held: HeldGroup[] = [];
  private timer: { cancel(): void } | undefined;
  private readonly pending = new Set<Promise<void>>();
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
    return this.held.reduce((n, g) => n + g.recipients.length, 0);
  }

  /**
   * Called once per settled batch, after the breaker recorded it, with that batch's dead targets
   * (empty when the verdict is open: the sender retries them instead) and their batch context.
   */
  submit(dead: HeldGroup, verdict: BreakerVerdict): void {
    if (verdict.open) {
      this.divert('breaker_open');
      return;
    }
    const breaker = this.opts.breaker;
    if (breaker.samples >= this.minSamples) {
      // `verdict` was computed when this batch settled; the batch then awaited its re-enqueue and
      // markSent while other batches went on recording. Never clear on that stale verdict alone:
      // judge the window as it is now. With >= 200 samples looksSystemic is exactly the rolling
      // rule, i.e. the verdict the next batch would get. These dead targets were not retried by
      // the sender (its verdict was closed), so they join the held ones and are re-queued.
      if (breaker.looksSystemic) {
        if (dead.recipients.length > 0) {
          this.held.push({ ...dead, recipients: [...dead.recipients] });
        }
        this.divert('breaker_open_since_settled');
        return;
      }
      this.release('evidence');
      for (const d of dead.recipients) this.opts.queue.enqueue(d.deviceId, d.target);
      return;
    }
    if (dead.recipients.length === 0) return;
    this.held.push({ ...dead, recipients: [...dead.recipients] });
    this.timer ??= this.setTimer(() => {
      this.timer = undefined;
      this.decide('hold_timeout');
    }, this.holdMs);
  }

  /** Graceful shutdown: decide on the held targets now (before the cleanup queue is flushed). */
  shutdown(): void {
    this.decide('shutdown');
  }

  /**
   * Waits up to `timeoutMs` for background re-queues. Resolves with how many are still running
   * (0 when all finished).
   */
  async settle(timeoutMs: number): Promise<number> {
    if (this.pending.size === 0) return 0;
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      Promise.all([...this.pending]),
      new Promise<void>((r) => (timer = setTimeout(r, timeoutMs))),
    ]);
    clearTimeout(timer);
    return this.pending.size;
  }

  private decide(why: 'hold_timeout' | 'shutdown'): void {
    if (this.held.length === 0) return;
    if (this.opts.breaker.looksSystemic) this.divert(why);
    else this.release(why);
  }

  private take(): HeldGroup[] {
    this.timer?.cancel();
    this.timer = undefined;
    const groups = this.held;
    this.held = [];
    return groups;
  }

  private fields(why: string, count: number): Record<string, unknown> {
    return {
      count,
      reason: why,
      samples: this.opts.breaker.samples,
      rollingRate: Number(this.opts.breaker.rollingRate.toFixed(4)),
    };
  }

  private release(why: string): void {
    if (this.held.length === 0) return;
    const groups = this.take();
    let count = 0;
    for (const g of groups) {
      for (const d of g.recipients) this.opts.queue.enqueue(d.deviceId, d.target);
      count += g.recipients.length;
    }
    this.opts.logger.info('sender.dead_targets_released', this.fields(why, count));
  }

  /** The "dead" tokens were a misconfiguration: clear nobody, re-queue everyone held. */
  private divert(why: string): void {
    if (this.held.length === 0) return;
    const groups = this.take();
    const count = groups.reduce((n, g) => n + g.recipients.length, 0);
    const requeue = this.opts.requeue;
    if (!requeue) {
      this.opts.logger.error('sender.dead_targets_dropped', this.fields(why, count));
      this.write(emfLine('DeadTargetHeldDropped', count));
      return;
    }
    this.opts.logger.error('sender.dead_targets_requeued', this.fields(why, count));
    const p: Promise<void> = requeue(groups)
      .catch((err: unknown) => {
        this.opts.logger.error('sender.dead_targets_requeue_threw', {
          error: err instanceof Error ? err.name : 'Error',
        });
        return 0;
      })
      .then((requeued) => {
        if (requeued > 0) this.write(emfLine('DeadTargetHeldRequeued', requeued));
        // Not re-queued (budget spent, enqueue failed): these miss this alert; tokens are kept.
        if (count - requeued > 0) this.write(emfLine('DeadTargetHeldDropped', count - requeued));
      })
      .finally(() => this.pending.delete(p));
    this.pending.add(p);
  }
}
