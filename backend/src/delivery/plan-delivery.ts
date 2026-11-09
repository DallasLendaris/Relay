import type { Alert } from '@relay/shared';
import type {
  Clock,
  DeliveryJob,
  DeliveryQueue,
  DeviceStore,
  IdempotencyStore,
  PlaceStore,
} from '../ports.js';
import { createCheckInToken, type TokenKey } from '../tokens/index.js';
import { refusalFor, type Refusal } from './eligibility.js';
import type { Logger } from './types.js';

export interface PlanDeps {
  places: PlaceStore;
  devices: DeviceStore;
  /** Ignored. Dedupe now happens in the worker at send time. Remove from callers. */
  idempotency?: IdempotencyStore;
  queue: DeliveryQueue;
  tokenKey: TokenKey;
  /** True only if the alert's signature is valid. */
  verifyAlert(alert: Alert): boolean;
  /** Kill switch. When true, nothing is planned. */
  isDeliveryPaused(): Promise<boolean>;
  clock: Clock;
  logger: Logger;
}

export interface PlanResult {
  refused?: Refusal | 'paused';
  /** Jobs put on the queue. */
  sent: number;
  matchedPlaces: number;
  devices: number;
  enqueued: number;
  skippedNoToken: number;
}

const CELL_CHUNK = 1000;
const QUEUE_BATCH = 10;
const DEVICE_CONCURRENCY = 50;

/**
 * Decide who to notify for an alert and put one DeliveryJob per device on the queue.
 * Planning claims nothing, so it is safe to run again for the same alert (after a failed
 * enqueue or a timeout). The worker dedupes at send time.
 */
export async function planDelivery(alert: Alert, deps: PlanDeps): Promise<PlanResult> {
  const result: PlanResult = {
    sent: 0,
    matchedPlaces: 0,
    devices: 0,
    enqueued: 0,
    skippedNoToken: 0,
  };

  const refusal = refusalFor(alert, deps.clock.now(), deps.verifyAlert);
  if (refusal) {
    deps.logger.error('refusing to plan alert', { alertId: alert.id, reason: refusal });
    return { ...result, refused: refusal };
  }
  if (await deps.isDeliveryPaused()) {
    deps.logger.warn('delivery paused', { alertId: alert.id });
    return { ...result, refused: 'paused' };
  }

  // Match places to the alert's cells, in chunks so queries stay a sane size.
  // Only ids come back, so a big match cannot exceed the Data API response limit.
  const placeIds = new Set<string>();
  const deviceIds = new Set<string>();
  const cells = alert.area.h3Cells;
  for (let i = 0; i < cells.length; i += CELL_CHUNK) {
    for (const m of await deps.places.listMatchesForCells(cells.slice(i, i + CELL_CHUNK))) {
      placeIds.add(m.placeId);
      deviceIds.add(m.deviceId);
    }
  }
  result.matchedPlaces = placeIds.size;
  result.devices = deviceIds.size;

  const expires = new Date(alert.expires);
  const jobs: DeliveryJob[] = [];
  const ids = [...deviceIds];
  for (let i = 0; i < ids.length; i += DEVICE_CONCURRENCY) {
    const slice = ids.slice(i, i + DEVICE_CONCURRENCY);
    const found = await Promise.all(slice.map((id) => deps.devices.get(id)));
    for (const device of found) {
      if (!device?.expoPushToken) {
        result.skippedNoToken++;
        continue;
      }
      jobs.push({
        alertId: alert.id,
        deviceId: device.id,
        expoPushToken: device.expoPushToken,
        language: device.language,
        checkInToken: createCheckInToken(
          { alertId: alert.id, deviceId: device.id, expiresAt: expires },
          deps.tokenKey,
        ),
        expiresAt: alert.expires,
        groupingKey: alert.id,
      });
    }
  }

  // If this throws partway, the caller may simply plan again: the worker drops duplicates.
  for (let i = 0; i < jobs.length; i += QUEUE_BATCH) {
    const batch = jobs.slice(i, i + QUEUE_BATCH);
    await deps.queue.enqueue(batch);
    result.enqueued += batch.length;
  }
  result.sent = result.enqueued;
  deps.logger.info('delivery planned', { alertId: alert.id, ...result });
  return result;
}
