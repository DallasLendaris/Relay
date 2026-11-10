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
/**
 * A re-check permanent is cleared only if its platform's latest fresh result, when the re-check
 * began, was at most this old (HeldGroup.primed): older evidence leaves room for an onset nobody
 * has seen yet.
 */
export const RECHECK_EVIDENCE_MAX_AGE_MS = 60_000;

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
   * Per platform key: PlatformWindows.seq when the batch started sending (so the platform's
   * state at that moment counts too). At release a recipient is re-queued if its platform is
   * systemic now OR was at any point since then (PlatformWindows.systemicSince). The windows
   * hold fresh traffic only: re-check batches never record (sender.ts carriesEvidence).
   */
  since?: Record<string, number>;
  /**
   * Set for a re-check batch's permanents (its key does not carry evidence): they are judged by
   * the re-check rule (see the class comment), and need `primed`.
   */
  recheck?: boolean;
  /**
   * Re-check only, per platform key: whether, when the re-check began, the platform was
   * PlatformWindows.healthyAfter its latest suspicion (`suspectedAt`, or this process's own last
   * systemic verdict, whichever is later), with its newest fresh result at most
   * RECHECK_EVIDENCE_MAX_AGE_MS old.
   */
  primed?: Record<string, boolean>;
  /**
   * Per platform key, epoch ms of the latest moment any process judged this lineage's platform
   * suspicious (sender.ts RecheckFields.suspectedAt). Carried into every re-queue.
   */
  suspectedAt?: Record<string, number>;
  /**
   * Set by the gate on a re-queue made without fresh evidence either way (an "unconfirmed"
   * re-check): sender.ts requeueHeld bounds those per lineage (MAX_BLIND_RECHECKS).
   */
  blind?: boolean;
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
 * batch's dead targets wait `holdMs`, and at release each recipient is judged by its platform
 * (`breaker.platforms`, key platformKeyOf(target)) with every result that arrived meanwhile.
 *
 * Fresh verdicts (the dead target came from a batch whose results were recorded):
 *   - not systemic now, and not at any point since its batch began: the clear goes to the
 *     cleanup queue (normal dead-token churn of 0.5-5% is cleared `holdMs` after the push that
 *     found it);
 *   - systemic now or at any point since (a misconfiguration fixed before the hold ended still
 *     counts: its "dead" tokens were not dead): nothing is cleared; the recipients keep their
 *     tokens and are re-queued as retry batches (`requeue`, DeadTargetHeldRequeued), so they are
 *     delivered once the misconfiguration is fixed, within the same 30-minute retry budget.
 *
 * Re-check verdicts (`recheck`: the recipient was already permanent once under suspicion, and the
 * re-check's own results are not evidence). Breakers are per process and SQS hands a re-check to
 * any sender task, often one that never saw the misconfiguration (or a fresh, cold task), so the
 * absence of a systemic verdict proves nothing. A re-check permanent is CLEARED only on positive
 * fresh evidence, in the process judging it, that its platform was healthy around the push:
 *   - before (`primed`): when the re-check began, the platform's last 20 fresh results
 *     (PLATFORM_SHORT_WINDOW) were all recorded after the lineage's latest suspicion (carried
 *     with the batch from whichever process raised it, RecheckFields) and after this process's
 *     own last systemic verdict, neither window was systemic, and the newest of them was at most
 *     RECHECK_EVIDENCE_MAX_AGE_MS old; and
 *   - after: no systemic verdict at any point from the re-check's start to the decision (`holdMs`
 *     later), as for fresh verdicts.
 * Otherwise the token is kept and the recipient re-queued. CONFIRMED (a fresh result recorded
 * after the re-check began left the platform systemic) re-queues within the 30-minute budget, as
 * for fresh verdicts. UNCONFIRMED (no fresh evidence either way: a cold or quiet process, fresh
 * traffic over, a systemic state left by traffic that has ended) re-queues as `:u`, at most
 * MAX_BLIND_RECHECKS times per lineage, minutes apart (sender.ts requeueHeld), then gives up
 * with the token kept (the next alert's fresh traffic judges it again).
 *
 * Bounds on wrong clears (live tokens cleared), per platform key:
 *   - Fresh: a clear is released only if the platform is not systemic `holdMs` after the
 *     failure. On a cold start isSystemic needs 5 permanents, so at most 4 tokens per process
 *     are cleared, and only if fewer than 5 of that platform's results arrive at that process
 *     within `holdMs`; after a healthy history at most 7 (the short window needs 8 of its last
 *     20). These are per process: with P sender tasks, at most 4P (cold) or 7P per onset.
 *   - Re-check: zero for a misconfiguration present at the push that was also present during any
 *     of the 20 fresh results before it in the judging task (a misconfiguration that a breaker
 *     reported for this lineage and that lasted until the push is one: the evidence must postdate
 *     the suspicion), or that shows 8 permanents in the judging task's fresh traffic within
 *     `holdMs` after the push (8 make a warm window systemic; a 40% one keeps the 200-result
 *     window systemic). So a receiving task that is cold, idle, or saw only healthy traffic from
 *     before the suspicion never clears: the multi-process failure of G6 cannot happen. A wrong
 *     clear needs a new episode that nobody had reported for the lineage, that began within
 *     RECHECK_EVIDENCE_MAX_AGE_MS before the push (after the task's last healthy fresh result),
 *     and that the task sees fewer than 8 failures of before the decision; only the re-check
 *     recipients that task pushed in that interval are exposed. This is the same blind spot as a
 *     fresh onset (at most 7 fresh clears per task), widened to the re-checks in flight.
 *   - Unconfirmed: never cleared. A chance systemic verdict as the last state fresh traffic left
 *     costs each dead token it caught 2 + MAX_BLIND_RECHECKS pushes in all, not one per retry
 *     step until the 30-minute budget.
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
    const requeue: HeldGroup[] = [];
    const systemicKeys = new Set<string>();
    const blindKeys = new Set<string>();
    let released = 0;
    let unconfirmed = 0;
    for (const g of groups) {
      const keep: Recipient[] = [];
      const blind: Recipient[] = [];
      for (const r of g.recipients) {
        const key = platformKeyOf(r.target);
        const verdict = g.recheck ? this.judgeRecheck(g, key) : this.judgeFresh(g, key);
        if (verdict === 'clear') {
          this.opts.queue.enqueue(r.deviceId, r.target);
          released++;
        } else if (verdict === 'keep') {
          keep.push(r);
          systemicKeys.add(key);
        } else {
          blind.push(r);
          blindKeys.add(key);
        }
      }
      const base = { ...g, suspectedAt: this.suspicion(g, [...keep, ...blind]) };
      delete base.blind;
      if (keep.length > 0) requeue.push({ ...base, recipients: keep });
      if (blind.length > 0) {
        requeue.push({ ...base, recipients: blind, blind: true });
        unconfirmed += blind.length;
      }
    }
    if (released > 0) {
      this.opts.logger.info('sender.dead_targets_released', { count: released, reason: why });
    }
    if (unconfirmed > 0) {
      // Not an alarm: the platform is not known to be misconfigured, only not known to be healthy.
      this.opts.logger.warn('sender.dead_targets_unconfirmed', {
        count: unconfirmed,
        reason: why,
        platforms: [...blindKeys],
      });
    }
    if (requeue.length > 0) this.divert(requeue, why, [...systemicKeys]);
  }

  /** Fresh results were recorded for this batch: systemic now or since it began keeps the token. */
  private judgeFresh(g: HeldGroup, key: string): 'clear' | 'keep' {
    const platforms = this.opts.breaker.platforms;
    const from = g.since?.[key];
    const held =
      platforms.systemic(key) || (from !== undefined && platforms.systemicSince(key, from));
    return held ? 'keep' : 'clear';
  }

  /** Re-check permanents: clear only on positive fresh evidence (see the class comment). */
  private judgeRecheck(g: HeldGroup, key: string): 'clear' | 'keep' | 'blind' {
    const platforms = this.opts.breaker.platforms;
    const from = g.since?.[key];
    if (from === undefined) return 'blind';
    // A fresh result recorded after the re-check began says misconfigured: confirmed.
    if (platforms.lastSystemicSeq(key) > from) return 'keep';
    const healthy = g.primed?.[key] === true && !platforms.systemicSince(key, from);
    return healthy ? 'clear' : 'blind';
  }

  /**
   * The suspicion a re-queue carries, per platform key of its recipients: the latest of what the
   * group carried and this process's own last systemic verdict; for a key with neither, now (the
   * re-queue itself is a suspicion: evidence must come after it).
   */
  private suspicion(g: HeldGroup, recipients: Recipient[]): Record<string, number> {
    const platforms = this.opts.breaker.platforms;
    const out: Record<string, number> = { ...g.suspectedAt };
    for (const r of recipients) {
      const key = platformKeyOf(r.target);
      const at = Math.max(out[key] ?? -Infinity, platforms.lastSystemicMs(key));
      out[key] = Number.isFinite(at) ? at : this.now();
    }
    return out;
  }

  /** The "dead" tokens were a misconfiguration: clear nobody, re-queue them. */
  private divert(groups: HeldGroup[], why: string, keys: string[]): void {
    const count = groups.reduce((n, g) => n + g.recipients.length, 0);
    const confirmed = groups.reduce((n, g) => n + (g.blind ? 0 : g.recipients.length), 0);
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
    if (confirmed > 0) this.opts.logger.error('sender.dead_targets_requeued', fields);
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
