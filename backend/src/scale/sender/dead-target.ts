import type { DeviceStore, PlaceStore } from '../../ports.js';
import type { CellIndex, PushTarget } from '../contracts.js';

/**
 * Builds the sender's onDeadTarget: a dead push token must stop being used.
 *
 * - Stale report: if the device has since registered a DIFFERENT target (a newer
 *   assignPushTarget), the report is about a token the device no longer uses. Nothing is
 *   cleared, so a fresh registration is never wiped by a late failure.
 * - Device store: clearPushToken(token) for every platform. It forgets the token wherever it
 *   is held (the legacy Expo column and the native pushPlatform/pushToken pair), the same
 *   thing assignPushTarget does to a device that loses a target.
 * - Cell index: CellIndex.updateDevice with target null on every cell of the device's places.
 *   The real DynamoCellIndex only updates entries that still exist (key h3Cell + sk), so a
 *   deleted place is never resurrected, and the stored language is kept.
 *
 * The device is read first for the guard and for its language. The small window between that
 * read and the index update can only lose a registration that lands in those milliseconds,
 * and the register path rewrites the index entries when it does.
 */
export function createDeadTargetClearer(deps: {
  devices: Pick<DeviceStore, 'get' | 'clearPushToken'>;
  places: Pick<PlaceStore, 'listForDevice'>;
  cellIndex: Pick<CellIndex, 'updateDevice'>;
}): (deviceId: string, target: PushTarget) => Promise<void> {
  return async (deviceId, target) => {
    const device = await deps.devices.get(deviceId);
    const currentToken = device?.pushToken ?? device?.expoPushToken ?? null;
    if (currentToken !== null && currentToken !== target.token) return;

    await deps.devices.clearPushToken(target.token);
    const places = await deps.places.listForDevice(deviceId);
    await deps.cellIndex.updateDevice(
      deviceId,
      places.map((p) => ({ h3Cell: p.h3Cell, placeId: p.id })),
      { target: null, language: device?.language ?? 'en' },
    );
  };
}
