import type { CheckIn } from '@relay/shared';
import type { CheckInStore } from '../../ports.js';

/** In-memory CheckInStore for tests and local runs. Mirrors the DynamoDB version. */
export class MemoryCheckInStore implements CheckInStore {
  private readonly byKey = new Map<string, CheckIn>();
  private readonly totalsByCell = new Map<string, number>();

  async save(checkIn: CheckIn): Promise<boolean> {
    const key = this.keyOf(checkIn.alertId, checkIn.idempotencyKey);
    if (this.byKey.has(key)) return false;
    this.byKey.set(key, checkIn);
    const cellKey = this.keyOf(checkIn.alertId, checkIn.h3Cell);
    this.totalsByCell.set(cellKey, (this.totalsByCell.get(cellKey) ?? 0) + 1);
    return true;
  }

  /** The stored check-in for this alert and idempotency key, if any. Not part of the port. */
  async get(alertId: string, idempotencyKey: string): Promise<CheckIn | null> {
    return this.byKey.get(this.keyOf(alertId, idempotencyKey)) ?? null;
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
