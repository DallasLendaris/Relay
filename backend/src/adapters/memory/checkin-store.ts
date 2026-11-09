import type { CheckInStore } from '../../ports.js';
import type { StoredCheckIn } from '../../scale/checkins/types.js';
import type { CheckIn } from '@relay/shared';

/**
 * In-memory CheckInStore for tests and local runs. Mirrors the DynamoDB version: one item per
 * `${alertId}#${deviceId}`, first write wins. `total()` stands in for the Streams aggregator's
 * totals table (the real store keeps no counter in the request path).
 */
export class MemoryCheckInStore implements CheckInStore {
  private readonly byKey = new Map<string, StoredCheckIn>();
  private readonly totalsByCell = new Map<string, number>();

  async save(checkIn: CheckIn): Promise<boolean> {
    const key = `${checkIn.alertId}#${checkIn.deviceId}`;
    if (this.byKey.has(key)) return false;
    this.byKey.set(key, {
      alertId: checkIn.alertId,
      deviceId: checkIn.deviceId,
      h3Cell: checkIn.h3Cell,
      checkedInAt: checkIn.checkedInAt,
      idempotencyKey: checkIn.idempotencyKey,
    });
    const cellKey = `${checkIn.alertId}#${checkIn.h3Cell}`;
    this.totalsByCell.set(cellKey, (this.totalsByCell.get(cellKey) ?? 0) + 1);
    return true;
  }

  /** The device's stored check-in for this alert, if any. Not part of the port. */
  async get(alertId: string, deviceId: string): Promise<StoredCheckIn | null> {
    return this.byKey.get(`${alertId}#${deviceId}`) ?? null;
  }

  /** Check-in count for one alert and H3 cell (what the aggregator's totals table holds). */
  total(alertId: string, h3Cell: string): number {
    return this.totalsByCell.get(`${alertId}#${h3Cell}`) ?? 0;
  }
}
