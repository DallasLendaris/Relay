import { createHash } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { alertEndTime, claimExpiry, refusalFor } from '../../delivery/eligibility.js';
import type { Logger } from '../../delivery/types.js';
import type { AlertStore, Clock, DeviceStore, IdempotencyStore, PlaceStore } from '../../ports.js';
import { targetOf } from '../cell-index/util.js';
import {
  QUEUE_FOR_SEVERITY,
  type PushTarget,
  type SendBatch,
  type SendQueue,
} from '../contracts.js';
import { bucketFor } from '../fanout/hash.js';
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
 *   2. REL-12: skip when the planned fan-out already covered this device. Its (cell, bucket)
 *      batch key (fanout helpers) is claimed AND the device's place and push target both predate
 *      the alert (`alert.sent`, which is never later than the planner's read). A device that
 *      registered after the alert was sent can be missing from an already-sent batch, so it is
 *      NOT skipped: a duplicate push (same collapse key) is better than a missed one. The same
 *      holds for an intent with `reason: 'outbox'` (the drainer indexed the device late).
 *   3. SEC-A-1: claim `${alertId}:pt:${sha256(platform+token)}`. One check-in token per physical
 *      push target per alert, however many device identities the phone made. 'sent' = skip.
 *   4. Rate limit: 5 sends per push target per hour (slots in the same table).
 *   5. Mint the v2 check-in token and enqueue a SendBatch; then mark both claims sent. If the
 *      enqueue fails, release both and throw so SQS redelivers the intent.
 */
export async function processLateJoin(
  intent: { deviceId: string; reason?: string },
  deps: LateJoinDeps,
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

    // An 'outbox' intent means the index learned of this device late (the API's write-through
    // failed and the drainer redid it), so the fan-out may not have seen it even though its
    // place predates the alert: no skip. The per-target claim below still stops a duplicate.
    if (
      intent.reason !== 'outbox' &&
      (await wasPlanned(alert, h3Cell, device.id, places, meta, deps.idempotency))
    ) {
      result.skipped.planned++;
      continue;
    }

    const outcome = await sendOne({ alert, queue, device, target, targetHash, h3Cell, now }, deps);
    if (outcome === 'sent') result.sent++;
    else if (outcome === 'duplicate') result.skipped.duplicateTarget++;
    else result.skipped.rateLimited++;
  }
  return result;
}

/** True when the planned fan-out demonstrably covered this device for this alert (REL-12). */
async function wasPlanned(
  alert: Alert,
  h3Cell: string,
  deviceId: string,
  places: ReadonlyArray<{ h3Cell: string; createdAt: string }>,
  meta: PushTargetMeta | null,
  idempotency: Required<Pick<IdempotencyStore, 'peek'>>,
): Promise<boolean> {
  const sent = Date.parse(alert.sent);
  const placeAt = Math.min(
    ...places.filter((p) => p.h3Cell === h3Cell).map((p) => Date.parse(p.createdAt)),
  );
  // Unknown times (no meta, unparseable) never count as planned: fail toward a duplicate.
  if (!meta || !Number.isFinite(sent) || !Number.isFinite(placeAt)) return false;
  if (!(placeAt < sent && meta.changedAt.getTime() < sent)) return false;

  // The key the shard worker used for this device's (cell, bucket). A pure read: claiming it to
  // probe would make the real sender see 'leased' and delay an urgent batch.
  const key = `${alert.id}:${alert.relaySeverity}:${h3Cell}:${bucketFor(deviceId)}`;
  return (await idempotency.peek(key)) !== 'absent'; // 'sent' or 'leased': planned
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
