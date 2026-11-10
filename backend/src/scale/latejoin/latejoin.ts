import { createHash } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { alertEndTime, claimExpiry, refusalFor } from '../../delivery/eligibility.js';
import type { Logger } from '../../delivery/types.js';
import type {
  AlertStore,
  Clock,
  DeviceStore,
  IdempotencyStore,
  KeyInspection,
  PlaceStore,
} from '../../ports.js';
import { targetOf } from '../cell-index/util.js';
import {
  QUEUE_FOR_SEVERITY,
  type PushTarget,
  type SendBatch,
  type SendQueue,
} from '../contracts.js';
import { bucketFor, densePartFor } from '../fanout/hash.js';
import { bucketKey, planMarkerKey, plannedKeyFor } from '../fanout/keys.js';
import {
  hasMember,
  PLAN_HASH_BYTES,
  SENT_HASH_BYTES,
  TARGET_HASH_BYTES,
  targetMemberOf,
} from '../fanout/member-set.js';
import { primaryCellOf } from '../fanout/primary-cell.js';
import { pushTargetHash, type PushTargetMeta } from './push-target-rules.js';

/** The part of a late-join batch key that identifies the device: a short hash, never the id. */
export const lateKeyOf = (deviceId: string): string =>
  createHash('sha256').update(deviceId).digest('hex').slice(0, 16);

/** At most this many late-join pushes per physical push target per hour. */
export const RATE_LIMIT_PER_HOUR = 5;
const HOUR_MS = 60 * 60 * 1000;
/** A claim is held this long while we enqueue; it is marked sent (or released) right after. */
const LEASE_MS = 60 * 1000;

export interface LateJoinDeps {
  devices: Pick<DeviceStore, 'get'> & {
    /** When the device's target last changed; null/absent means "unknown" (never treated as planned). */
    pushTargetMeta?(deviceId: string): Promise<PushTargetMeta | null>;
  };
  places: Pick<PlaceStore, 'listForDevice'>;
  alerts: Pick<AlertStore, 'listActiveForCells'>;
  /** Alert signature check (alert keys). An alert that fails is never late-joined. */
  verifyAlert(alert: Alert): boolean;
  /**
   * The sender's idempotency table: batch claims, per-target claims and rate-limit slots.
   * `peek` is required: the "was the batch sent?" probe must not claim the batch key.
   */
  idempotency: IdempotencyStore & Required<Pick<IdempotencyStore, 'peek'>>;
  sendQueue: SendQueue;
  makeCheckInToken(alertId: string, deviceId: string, h3Cell: string, expiresAt: Date): string;
  isDeliveryPaused(): Promise<boolean>;
  clock: Clock;
  logger: Logger;
}

export interface LateJoinResult {
  refused?: 'paused';
  /** Batches enqueued (one recipient each). */
  sent: number;
  /** Alerts skipped because their signature did not verify (the caller may refresh keys). */
  unsigned: number;
  /**
   * Alerts held back because the planned fan-out may still carry this device (G5): its cell is not
   * planned yet, or its planned batch includes it and is not sent yet. Only with `waitForPlan`;
   * the caller re-runs the intent shortly (claims make the re-run safe for the alerts sent now).
   */
  deferred: number;
  skipped: {
    noTarget: number;
    noPlaces: number;
    notSendable: number;
    nonPrimary: number;
    planned: number;
    duplicateTarget: number;
    rateLimited: number;
  };
}

const emptyResult = (): LateJoinResult => ({
  sent: 0,
  unsigned: 0,
  deferred: 0,
  skipped: {
    noTarget: 0,
    noPlaces: 0,
    notSendable: 0,
    nonPrimary: 0,
    planned: 0,
    duplicateTarget: 0,
    rateLimited: 0,
  },
});

/**
 * Sends the alerts already active over a device's places to that one device, because it missed
 * the planned fan-out (it registered a place or a push target after the planner read the cell).
 *
 * Trust: the ONLY input from the message is the device id. The device, its places, its push
 * target and the active alerts all come from Aurora here, so a forged intent cannot name a
 * recipient, a cell or an alert. Each alert's signature is verified with the alert keys before
 * anything is minted.
 *
 * For each active, signed alert over the device's cells:
 *   1. Primary-cell rule (same as the shard worker): the device is sent from the smallest of its
 *      cells in the alert area; its other cells add nothing.
 *   2. REL-12 / G5: skip exactly when the planned fan-out carries this device (plannedFate below):
 *      the sender's record of the device's planned batch says whether the batch it SENT held the
 *      device; before that exists, the shard worker's plan record of the cell says whether the
 *      planned read could have included it. While that is open (cell not planned yet, or the
 *      device is in a planned batch not yet sent) the alert is deferred, with `waitForPlan`, and
 *      the caller re-runs the intent; the wait is bounded by the caller, after which the device is
 *      sent (a duplicate push, same collapse key, is better than a missed one).
 *   3. SEC-A-1: claim `${alertId}:pt:${sha256(platform+token)}`. One check-in token per physical
 *      push target per alert, however many device identities the phone made. 'sent' = skip.
 *   4. Rate limit: 5 sends per push target per hour (slots in the same table).
 *   5. Mint the v2 check-in token and enqueue a SendBatch; then mark both claims sent. If the
 *      enqueue fails, release both and throw so SQS redelivers the intent.
 */
export async function processLateJoin(
  intent: { deviceId: string; reason?: string },
  deps: LateJoinDeps,
  opts: LateJoinOptions = {},
): Promise<LateJoinResult> {
  const result = emptyResult();
  if (await deps.isDeliveryPaused()) return { ...result, refused: 'paused' };

  const now = deps.clock.now();
  const device = await deps.devices.get(intent.deviceId);
  const target = device ? targetOf(device) : null;
  if (!device || !target) {
    result.skipped.noTarget++;
    return result;
  }
  const places = await deps.places.listForDevice(device.id);
  const deviceCells = [...new Set(places.map((p) => p.h3Cell))].sort();
  if (deviceCells.length === 0) {
    result.skipped.noPlaces++;
    return result;
  }

  const meta = (await deps.devices.pushTargetMeta?.(device.id)) ?? null;
  const targetHash = pushTargetHash(target);
  const active = await deps.alerts.listActiveForCells(deviceCells, now);

  for (const alert of active) {
    const queue = QUEUE_FOR_SEVERITY[alert.relaySeverity];
    if (!queue) {
      result.skipped.notSendable++; // statements are in-app only
      continue;
    }
    const refusal = refusalFor(alert, now, deps.verifyAlert);
    if (refusal === 'unsigned') {
      result.unsigned++;
      deps.logger.error('latejoin.unsigned_alert', { alertId: alert.id });
      continue;
    }
    if (refusal) {
      result.skipped.notSendable++;
      continue;
    }
    const areaCells = new Set(alert.area.h3Cells);
    const h3Cell = primaryCellOf(deviceCells, areaCells);
    if (h3Cell === undefined) continue; // not in this alert's area (the store matches loosely)

    const fate = await plannedFate(
      { alert, h3Cell, deviceId: device.id, target, places, meta, reason: intent.reason },
      deps.idempotency,
      opts.waitForPlan === true,
    );
    if (fate === 'planned') {
      result.skipped.planned++;
      continue;
    }
    if (fate === 'wait') {
      result.deferred++;
      continue;
    }

    const outcome = await sendOne({ alert, queue, device, target, targetHash, h3Cell, now }, deps);
    if (outcome === 'sent') result.sent++;
    else if (outcome === 'duplicate') result.skipped.duplicateTarget++;
    else result.skipped.rateLimited++;
  }
  return result;
}

export interface LateJoinOptions {
  /**
   * Defer (result.deferred) an alert whose planned fan-out may still carry the device, instead of
   * deciding now. The handler sets it while the intent has waited less than PLAN_WAIT_GIVE_UP_MS.
   * Off: decide now, failing toward a send (the REL-12 timestamp rule where nothing exact is known).
   */
  waitForPlan?: boolean;
}

/**
 * Whether the planned fan-out carries this device for this alert (G5): 'planned' (skip), 'send'
 * (late join must), or 'wait' (not knowable yet; only when `mayWait`).
 *
 * K is the device's planned batch key: its (cell, bucket), or its dense part when the bucket key
 * is a dense tombstone (plannedKeyFor). The sender records K's recipients when it marks K sent
 * (SENT set, exact); the shard worker records, per cell (per part when dense), every recipient it
 * put in batches once they are decided (PLAN set; written even when empty).
 *   - K sent with a SENT set: not in it = 'send'. In it = 'planned' only when the batch pushed
 *     the device's CURRENT target (G5b): the plan record's target set holds (device, target), or,
 *     without a target set, the target predates the alert (REL-12). Else 'send' (a token rotated
 *     after the read: the batch pushed the old, possibly dead, token).
 *   - K not sent, device in the PLAN set but its target set holds it under another target = 'send'.
 *   - K not sent (absent or leased), PLAN record present, device not in the PLAN set: no planned
 *     batch holds the device (its read came before the registration) = 'send', no waiting.
 *   - K not sent, device in the PLAN set (it registered during the read, or long before): K is
 *     queued or being sent and only its SENT set will say whether the sent K holds it (a re-plan
 *     can put two versions of K in the queue) = 'wait'.
 *   - No PLAN record: the shard worker has not finished this cell, and its read may include the
 *     device = 'wait'.
 * Without exact data (an older sender or store, a split batch, a set over its size cap, or the
 * wait given up) it falls back to REL-12: skip only when K is sent or leased AND the device's
 * place and push target both predate `alert.sent` (never later than the planner's read) and the
 * intent is not 'outbox' (the drainer indexed the device late); otherwise send.
 *
 * Reads, all strongly consistent GetItems on the idempotency table and never claims (a claim to
 * probe would make the real sender see 'leased'): plannedKeyFor's 2 (normal key and its split or
 * dense marker; 3-4 in a dense cell), plus the plan record when K is not sent or holds the device.
 */
async function plannedFate(
  c: {
    alert: Alert;
    h3Cell: string;
    deviceId: string;
    /** The device's CURRENT push target (Aurora). */
    target: PushTarget;
    places: ReadonlyArray<{ h3Cell: string; createdAt: string }>;
    meta: PushTargetMeta | null;
    reason: string | undefined;
  },
  idempotency: Required<Pick<IdempotencyStore, 'peek'>> & Pick<IdempotencyStore, 'inspect'>,
  mayWait: boolean,
): Promise<'planned' | 'send' | 'wait'> {
  const { alert, h3Cell, deviceId } = c;
  const predates = c.reason !== 'outbox' && predatesAlert(c);
  const store = idempotency.inspect?.bind(idempotency);
  if (!store) {
    // An older store: only the REL-12 rule (and only predating devices need the probe).
    if (!predates) return 'send';
    const { state } = await plannedKeyFor(alert, h3Cell, deviceId, (k) => idempotency.peek(k));
    return state !== 'absent' ? 'planned' : 'send';
  }
  const seen = new Map<string, KeyInspection>();
  const inspect = async (key: string): Promise<KeyInspection> => {
    let got = seen.get(key);
    if (!got) seen.set(key, (got = await store(key)));
    return got;
  };
  const { key, state } = await plannedKeyFor(
    alert,
    h3Cell,
    deviceId,
    async (k) => (await inspect(k)).state,
  );
  const part =
    key === bucketKey(alert, h3Cell, bucketFor(deviceId)) ? undefined : densePartFor(deviceId);
  const planKey = planMarkerKey(alert, h3Cell, part);
  // True only when a plan record exists, has its set, and the device is not in it.
  const notInPlan = async (): Promise<boolean> => {
    const plan = await inspect(planKey);
    return (
      plan.state === 'sent' &&
      plan.members !== undefined &&
      !hasMember(plan.members, deviceId, PLAN_HASH_BYTES)
    );
  };
  /**
   * G5b: whether the plan carried the device with its CURRENT target. 'yes'/'no' from the plan
   * record's target set (present only when exactly one plan wrote the key, before enqueueing it,
   * so a sent K is that plan's); 'unknown' without it. A device whose token changed after the read
   * (an app reinstall: the old token is dead) is 'no', so late join pushes the new token.
   */
  const carriedTarget = async (): Promise<'yes' | 'no' | 'unknown'> => {
    const plan = await inspect(planKey);
    if (plan.state !== 'sent' || !plan.targets) return 'unknown';
    return hasMember(plan.targets, targetMemberOf(deviceId, c.target), TARGET_HASH_BYTES)
      ? 'yes'
      : 'no';
  };

  if (state === 'sent') {
    const sentSet = seen.get(key)?.members;
    if (sentSet) {
      // The sent set names devices, not targets: a device in it is skipped only when the batch
      // pushed its current target (target set), or, without one, when that target predates the
      // alert and so every plan's read (REL-12). Otherwise send: at worst a duplicate.
      if (!hasMember(sentSet, deviceId, SENT_HASH_BYTES)) return 'send';
      const carried = await carriedTarget();
      return carried === 'yes' || (carried === 'unknown' && predates) ? 'planned' : 'send';
    }
    // No exact record of the sent batch: REL-12, except that a device no plan put in a batch
    // still gets its push.
    return predates && !(await notInPlan()) ? 'planned' : 'send';
  }
  if (await notInPlan()) return 'send';
  // In the plan, but with an older target (G5b): its batch pushes that one. Send now.
  if ((await carriedTarget()) === 'no') return 'send';
  // The cell is not planned yet, or the device is in a planned batch that is not sent yet.
  if (mayWait) return 'wait';
  return state === 'leased' && predates ? 'planned' : 'send';
}

/** REL-12: the device's place in this cell and its push target both predate the alert. */
function predatesAlert(c: {
  alert: Alert;
  h3Cell: string;
  places: ReadonlyArray<{ h3Cell: string; createdAt: string }>;
  meta: PushTargetMeta | null;
}): boolean {
  const sent = Date.parse(c.alert.sent);
  const placeAt = Math.min(
    ...c.places.filter((p) => p.h3Cell === c.h3Cell).map((p) => Date.parse(p.createdAt)),
  );
  // Unknown times (no meta, unparseable) never count as planned: fail toward a duplicate.
  if (!c.meta || !Number.isFinite(sent) || !Number.isFinite(placeAt)) return false;
  return placeAt < sent && c.meta.changedAt.getTime() < sent;
}

async function sendOne(
  c: {
    alert: Alert;
    queue: 'urgent' | 'standard';
    device: { id: string; language: 'en' | 'es' };
    target: PushTarget;
    targetHash: string;
    h3Cell: string;
    now: Date;
  },
  deps: LateJoinDeps,
): Promise<'sent' | 'duplicate' | 'rate_limited'> {
  const { alert, device, now } = c;
  const { idempotency } = deps;
  const lease = new Date(now.getTime() + LEASE_MS);

  const ptKey = `${alert.id}:pt:${c.targetHash}`;
  const pt = await idempotency.claim(ptKey, lease);
  if (pt === 'sent') return 'duplicate';
  // Another attempt is minting for this same push target right now. Retry later: by then it is
  // 'sent' and we skip, or its lease expired and we take over.
  if (pt === 'leased') throw new Error('late-join claim for this push target is in flight');

  const hour = Math.floor(now.getTime() / HOUR_MS);
  let slot: string | undefined;
  try {
    for (let n = 0; n < RATE_LIMIT_PER_HOUR && !slot; n++) {
      const key = `latejoin-rate:${c.targetHash}:${hour}:${n}`;
      if ((await idempotency.claim(key, lease)) === 'claimed') slot = key;
    }
    if (!slot) {
      await idempotency.release(ptKey);
      deps.logger.warn('latejoin.rate_limited', { alertId: alert.id });
      return 'rate_limited';
    }

    const batch: SendBatch = {
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: alert.relaySeverity,
      // The device id is hashed: batch keys are logged (and stored), so they must not carry it.
      batchKey: `${alert.id}:${alert.relaySeverity}:late:${lateKeyOf(device.id)}`,
      recipients: [
        {
          deviceId: device.id,
          target: c.target,
          language: device.language,
          checkInToken: deps.makeCheckInToken(alert.id, device.id, c.h3Cell, alertEndTime(alert)),
        },
      ],
      attempt: 0,
    };
    await deps.sendQueue.enqueue(c.queue, [batch]);
  } catch (err) {
    await idempotency.release(ptKey).catch(() => undefined);
    if (slot) await idempotency.release(slot).catch(() => undefined);
    throw err;
  }
  await idempotency.markSent(ptKey, claimExpiry(alert));
  await idempotency.markSent(slot as string, new Date((hour + 2) * HOUR_MS));
  deps.logger.info('latejoin.sent', { alertId: alert.id, queue: c.queue });
  return 'sent';
}
