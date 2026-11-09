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
import type { Logger } from './types.js';

export interface PlanDeps {
  places: PlaceStore;
  devices: DeviceStore;
  idempotency: IdempotencyStore;
  queue: DeliveryQueue;
  tokenKey: TokenKey;
  /** AE-09 hook: true only if the alert's signature is valid. */
  verifyAlert(alert: Alert): boolean;
  /** Kill switch. When true, nothing is sent. */
  isDeliveryPaused(): Promise<boolean>;
  clock: Clock;
  logger: Logger;
}

export interface PlanResult {
  refused?: 'unsigned' | 'paused';
  /** Alert was cancelled/expired, so nothing was planned. */
  skipped?: 'inactive';
  sent: number;
  matchedPlaces: number;
  devices: number;
  enqueued: number;
  skippedNoToken: number;
  skippedDuplicate: number;
}

const CELL_CHUNK = 1000;
const QUEUE_BATCH = 10;
const DEVICE_CONCURRENCY = 50;

const empty = (): PlanResult => ({
  sent: 0,
  matchedPlaces: 0,
  devices: 0,
  enqueued: 0,
  skippedNoToken: 0,
  skippedDuplicate: 0,
});

/** Decide who to notify for an alert and put one DeliveryJob per device on the queue. */
export async function planDelivery(alert: Alert, deps: PlanDeps): Promise<PlanResult> {
  const result = empty();

  if (!deps.verifyAlert(alert)) {
    deps.logger.error('refusing unsigned alert', { alertId: alert.id });
    return { ...result, refused: 'unsigned' };
  }
  if (await deps.isDeliveryPaused()) {
    deps.logger.warn('delivery paused', { alertId: alert.id });
    return { ...result, refused: 'paused' };
  }
  // TODO: cancelled alerts should send a "cancelled" notification to people who were notified.
  if (alert.state === 'cancelled' || alert.state === 'expired') {
    return { ...result, skipped: 'inactive' };
  }

  // Match places to the alert's cells, in chunks so queries stay a sane size.
  const placeIds = new Set<string>();
  const deviceIds = new Set<string>();
  const cells = alert.area.h3Cells;
  for (let i = 0; i < cells.length; i += CELL_CHUNK) {
    for (const place of await deps.places.listForCells(cells.slice(i, i + CELL_CHUNK))) {
      placeIds.add(place.id);
      deviceIds.add(place.deviceId);
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
      // Same message replayed -> same key -> no second push. An update has a new sourceId.
      const claimed = await deps.idempotency.claim(`${alert.sourceId}:${device.id}`, expires);
      if (!claimed) {
        result.skippedDuplicate++;
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

  for (let i = 0; i < jobs.length; i += QUEUE_BATCH) {
    await deps.queue.enqueue(jobs.slice(i, i + QUEUE_BATCH));
  }
  result.enqueued = jobs.length;
  result.sent = jobs.length;
  deps.logger.info('delivery planned', { alertId: alert.id, ...result });
  return result;
}
