import type { Logger } from '../../delivery/types.js';
import type { SendBatch } from '../contracts.js';
import { platformKeyOf, type PermanentBreaker } from './breaker.js';
import type { DeadTargetQueue } from './dead-target-queue.js';
import { emfLine, type MetricWriter } from './metrics.js';

/** How long every dead-target clear waits before it is judged and released (or re-queued). */
export const DEAD_TARGET_HOLD_MS = 30_000;
/**
 * Most recipients held at once. Held entries live for DEAD_TARGET_HOLD_MS, so this is reached only
 * at about 670 dead targets per second; past it the oldest are decided early (never dropped).
 */
export const MAX_HELD_RECIPIENTS = 20_000;

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
  /**
   * Per platform key: the number of this batch's first result on that platform
   * (PlatformWindows.seq + 1 before it was recorded). At release a recipient is re-queued if its
   * platform is systemic now OR was at any point since then (PlatformWindows.systemicSince).
   */
  since?: Record<string, number>;
}

/**
 * Re-queues held recipients as retry batches. Resolves with how many were re-queued; the rest
 * (retry budget spent, or the enqueue failed) are lost to this alert and must be logged there.
 */
export type HeldRequeuer = (groups: HeldGroup[]) => Promise<number>;

export interface DeadTargetGateOptions {
  /** Its per-platform windows (`breaker.platforms`) decide each held clear. */
  breaker: PermanentBreaker;
  /** Where released dead targets go (the bounded cleanup queue). */
  queue: Pick<DeadTargetQueue, 'enqueue'>;
  /**
   * Where held recipients go when their platform looks misconfigured. Without it they are only
   * dropped (counted as DeadTargetHeldDropped).
   */
  requeue?: HeldRequeuer;
  logger: Logger;
  writeMetric?: MetricWriter;
  /** Default DEAD_TARGET_HOLD_MS (30 s). 0 decides at once (tests of the cleanup itself). */
  holdMs?: number;
  /** Default MAX_HELD_RECIPIENTS. */
  maxHeld?: number;
  /** Clock, ms. Default Date.now. */
  now?: () => number;
  /** Injectable for tests. Default setTimeout, unref'd. */
  setTimer?: (fn: () => void, ms: number) => { cancel(): void };
}

interface Entry {
  group: HeldGroup;
  due: number;
}

/**
 * Holds EVERY dead-target clear for `holdMs`, then judges it against its own platform.
 *
 * A misconfiguration (wrong APNs key or topic, sandbox/production mismatch, a broken FCM
 * project) makes every token of that platform look dead. The breaker's batch and rolling rules
 * cannot see it in small batches, before they have 200 results, when it starts mid-alert, or
 * when the platform is a small share of the traffic. So no clear goes out at once: each settled
 * batch's dead targets wait `holdMs`, and at release each recipient is judged by
 * its platform (`breaker.platforms`, key platformKeyOf(target)) with every result that arrived in
 * the meantime:
 *
 *   - not systemic now, and not at any point since its batch was recorded: the clear goes to the
 *     cleanup queue (normal dead-token churn of 0.5-5% is cleared `holdMs` after the push that
 *     found it);
 *   - systemic now or at any point since (a misconfiguration fixed before the hold ended still
 *     counts: its "dead" tokens were not dead): nothing is cleared; the recipients keep their tokens and are re-queued as retry
 *     batches (`requeue`, DeadTargetHeldRequeued), so they are delivered once the
 *     misconfiguration is fixed, within the same 30-minute retry budget.
 *
 * Bound on wrong clears per misconfiguration onset, per platform key: a clear is released only if
 * the platform is not systemic `holdMs` after the failure. On a cold start isSystemic needs 5
 * permanents, so at most 4 tokens are cleared, and only if fewer than 5 of that platform's results
 * arrive within `holdMs`. After a healthy history the short window needs 8 permanents of its last
 * 20, so at most 7 are cleared, and none if 8 or more results of the platform arrive within
 * `holdMs` of the onset (the sender itself retries permanents of a systemic platform from then
 * on, without the gate).
 *
 * Memory: held entries live `holdMs`; at most `maxHeld` recipients are held, past that the oldest
 * are decided early. shutdown() decides everything at once; settle() waits for the re-queue
 * writes. Normal sending is never delayed: the gate only defers the asynchronous cleanup.
 */
export class DeadTargetGate {
  private held: Entry[] = [];
  private heldRecipients = 0;
  private peak = 0;
  private timer: { cancel(): void; due: number } | undefined;
  private readonly pending = new Set<Promise<void>>();
  private readonly holdMs: number;
  private readonly maxHeld: number;
  private readonly now: () => number;
  private readonly write: MetricWriter;
  private readonly setTimer: NonNullable<DeadTargetGateOptions['setTimer']>;

  constructor(private readonly opts: DeadTargetGateOptions) {
    this.holdMs = opts.holdMs ?? DEAD_TARGET_HOLD_MS;
    this.maxHeld = opts.maxHeld ?? MAX_HELD_RECIPIENTS;
    this.now = opts.now ?? Date.now;
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
    return this.heldRecipients;
  }

  /** Most recipients ever held at once (bounded by `maxHeld`). */
  get peakHeld(): number {
    return this.peak;
  }

  /** Called once per settled batch with that batch's dead targets and their batch context. */
  submit(dead: HeldGroup): void {
    if (dead.recipients.length === 0) return;
    const group = { ...dead, recipients: [...dead.recipients] };
    if (this.holdMs <= 0) {
      this.decide([group], 'no_hold');
      return;
    }
    this.held.push({ group, due: this.now() + this.holdMs });
    this.heldRecipients += group.recipients.length;
    // Over the bound: decide the oldest now rather than hold more.
    if (this.heldRecipients > this.maxHeld) {
      const early: HeldGroup[] = [];
      while (this.heldRecipients > this.maxHeld && this.held.length > 1) early.push(this.shift());
      this.decide(early, 'overflow');
    }
    this.peak = Math.max(this.peak, this.heldRecipients);
    this.arm();
  }

  /** Graceful shutdown: decide everything held now (before the cleanup queue is flushed). */
  shutdown(): void {
    this.timer?.cancel();
    this.timer = undefined;
    const all: HeldGroup[] = [];
    while (this.held.length > 0) all.push(this.shift());
    this.decide(all, 'shutdown');
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

  private shift(): HeldGroup {
    const e = this.held.shift() as Entry;
    this.heldRecipients -= e.group.recipients.length;
    return e.group;
  }

  /** One timer, for the oldest entry (entries are in submit order, so in due order). */
  private arm(): void {
    const first = this.held[0];
    if (!first || this.timer) return;
    const due = first.due;
    const t = this.setTimer(() => this.onTimer(due), Math.max(0, due - this.now()));
    this.timer = { cancel: () => t.cancel(), due };
  }

  private onTimer(due: number): void {
    this.timer = undefined;
    // The timer fired for `due`: that time has come, whatever the clock says.
    const cutoff = Math.max(this.now(), due);
    const ready: HeldGroup[] = [];
    while (this.held[0] && this.held[0].due <= cutoff) ready.push(this.shift());
    this.decide(ready, 'hold_elapsed');
    this.arm();
  }

  /** Judges each recipient by its platform now: clear, or re-queue with its tokens kept. */
  private decide(groups: HeldGroup[], why: string): void {
    if (groups.length === 0) return;
    const platforms = this.opts.breaker.platforms;
    const requeue: HeldGroup[] = [];
    const systemicKeys = new Set<string>();
    let released = 0;
    for (const g of groups) {
      const keep: Recipient[] = [];
      for (const r of g.recipients) {
        const key = platformKeyOf(r.target);
        const from = g.since?.[key];
        if (platforms.systemic(key) || (from !== undefined && platforms.systemicSince(key, from))) {
          keep.push(r);
          systemicKeys.add(key);
        } else {
          this.opts.queue.enqueue(r.deviceId, r.target);
          released++;
        }
      }
      if (keep.length > 0) requeue.push({ ...g, recipients: keep });
    }
    if (released > 0) {
      this.opts.logger.info('sender.dead_targets_released', { count: released, reason: why });
    }
    if (requeue.length > 0) this.divert(requeue, why, [...systemicKeys]);
  }

  /** The "dead" tokens were a misconfiguration: clear nobody, re-queue them. */
  private divert(groups: HeldGroup[], why: string, keys: string[]): void {
    const count = groups.reduce((n, g) => n + g.recipients.length, 0);
    const fields = {
      count,
      reason: why,
      platforms: keys.map((k) => ({ platform: k, ...this.opts.breaker.platforms.stats(k) })),
    };
    const requeue = this.opts.requeue;
    if (!requeue) {
      this.opts.logger.error('sender.dead_targets_dropped', fields);
      this.write(emfLine('DeadTargetHeldDropped', count));
      return;
    }
    this.opts.logger.error('sender.dead_targets_requeued', fields);
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
