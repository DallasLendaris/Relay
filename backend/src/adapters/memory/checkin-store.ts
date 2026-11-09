import type { CheckIn } from '@relay/shared';
import type { CheckInStore } from '../../ports.js';

/** In-memory CheckInStore for tests and local runs. Mirrors the DynamoDB version. */
export class MemoryCheckInStore implements CheckInStore {
  private readonly byDevice = new Map<string, CheckIn>();
  private readonly totalsByCell = new Map<string, number>();

  async save(checkIn: CheckIn): Promise<boolean> {
    const key = this.keyOf(checkIn.alertId, checkIn.deviceId);
    if (this.byDevice.has(key)) return false;
    this.byDevice.set(key, checkIn);
    const cellKey = this.keyOf(checkIn.alertId, checkIn.h3Cell);
    this.totalsByCell.set(cellKey, (this.totalsByCell.get(cellKey) ?? 0) + 1);
    return true;
  }

  /** The device's stored check-in for this alert, if any. Not part of the port. */
  async get(alertId: string, deviceId: string): Promise<CheckIn | null> {
    return this.byDevice.get(this.keyOf(alertId, deviceId)) ?? null;
  }

  /** Check-in count for one alert and H3 cell (what CHECKIN_TOTALS_TABLE holds). */
  total(alertId: string, h3Cell: string): number {
    return this.totalsByCell.get(this.keyOf(alertId, h3Cell)) ?? 0;
  }

  // The alert id comes first and has a fixed shape, so this joined key cannot collide.
  private keyOf(alertId: string, rest: string): string {
    return `${alertId}|${rest}`;
  }
}
