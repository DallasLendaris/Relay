/**
 * Inline (dev, FANOUT_MODE=inline) planning of a follow-up push: one DeliveryJob per device with a
 * saved place in the previous version's area. The worker decides at send time whether the device
 * was sent that version (its delivery key) and dedupes the follow-up (docs/cancellation.md).
 */
import type { Alert } from '@relay/shared';
import type { DeliveryJob } from '../ports.js';
import { createCheckInToken } from '../tokens/index.js';
import { alertEndTime } from './eligibility.js';
import {
  followUpCoversKeptCells,
  followUpKindFor,
  followUpRefusal,
  versionTag,
  type FollowUpRefusal,
  type FollowUpRequest,
} from './follow-up.js';
import type { PlanDeps } from './plan-delivery.js';

export interface FollowUpPlanDeps extends PlanDeps {
  /** Cells the version dropped (AlertStore.listDroppedCells). */
  droppedCells?(alertId: string, sourceId: string): Promise<string[]>;
}

export interface FollowUpPlanResult {
  refused?: FollowUpRefusal | 'paused' | 'no_previous_version';
  devices: number;
  enqueued: number;
  skippedNoToken: number;
}

const CELL_CHUNK = 1000;
const QUEUE_BATCH = 10;

export async function planFollowUpDelivery(
  alert: Alert,
  request: FollowUpRequest,
  deps: FollowUpPlanDeps,
): Promise<FollowUpPlanResult> {
  const result: FollowUpPlanResult = { devices: 0, enqueued: 0, skippedNoToken: 0 };
  const version = versionTag(alert.sourceId);
  const refusal = followUpRefusal(alert, request, version, deps.clock.now(), deps.verifyAlert);
  if (refusal) return { ...result, refused: refusal };
  const previousSeverity =
    alert.supersedes?.relaySeverity ?? (request === 'cancel' ? alert.relaySeverity : undefined);
  if (!previousSeverity) return { ...result, refused: 'no_previous_version' };
  if (await deps.isDeliveryPaused()) return { ...result, refused: 'paused' };

  const current = new Set(alert.area.h3Cells);
  let cells: string[];
  if (request === 'cancel') cells = alert.area.h3Cells;
  else {
    const kept = followUpCoversKeptCells(alert) ? alert.area.h3Cells : [];
    const dropped = (await deps.droppedCells?.(alert.id, alert.sourceId)) ?? [];
    cells = [...new Set([...kept, ...dropped])];
  }

  // Per device: the cell for its token, and whether any of its matched places is still covered.
  const byDevice = new Map<string, { cell: string; inArea: boolean }>();
  for (let i = 0; i < cells.length; i += CELL_CHUNK) {
    for (const m of await deps.places.listMatchesForCells(cells.slice(i, i + CELL_CHUNK))) {
      const prev = byDevice.get(m.deviceId);
      const inArea = current.has(m.h3Cell);
      if (!prev) byDevice.set(m.deviceId, { cell: m.h3Cell, inArea });
      else if (inArea && !prev.inArea) byDevice.set(m.deviceId, { cell: m.h3Cell, inArea });
    }
  }
  result.devices = byDevice.size;

  const expiresAt = alertEndTime(alert);
  const jobs: DeliveryJob[] = [];
  for (const [deviceId, where] of byDevice) {
    const device = await deps.devices.get(deviceId);
    if (!device?.expoPushToken) {
      result.skippedNoToken++;
      continue;
    }
    jobs.push({
      alertId: alert.id,
      deviceId,
      expoPushToken: device.expoPushToken,
      language: device.language,
      checkInToken: createCheckInToken(
        { alertId: alert.id, deviceId, expiresAt, h3Cell: where.cell },
        deps.tokenKey,
      ),
      expiresAt: alert.expires,
      groupingKey: alert.id,
      followUp: { kind: followUpKindFor(request, where.inArea), version, previousSeverity },
    });
  }
  for (let i = 0; i < jobs.length; i += QUEUE_BATCH) {
    const batch = jobs.slice(i, i + QUEUE_BATCH);
    await deps.queue.enqueue(batch);
    result.enqueued += batch.length;
  }
  return result;
}
