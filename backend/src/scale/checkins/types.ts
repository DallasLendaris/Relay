import type { CheckIn } from '@relay/shared';

/**
 * What the check-ins table stores per device and alert (see CheckInEvent in ../contracts.ts for
 * the stream view). The token and status are not stored: status is always "ok" in this release
 * and the token was already verified.
 */
export type StoredCheckIn = Pick<
  CheckIn,
  'alertId' | 'deviceId' | 'h3Cell' | 'checkedInAt' | 'idempotencyKey'
>;

/** Check-in count for one alert and H3 cell, as kept in the totals table. */
export interface CellTotal {
  alertId: string;
  h3Cell: string;
  checkedIn: number;
}
