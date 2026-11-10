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

/**
 * How long a check-in row is kept after its alert ends. A check-in says "this device was in this
 * cell at this time", so it is not kept forever. 30 days is a placeholder: COUNSEL decides the
 * real period. Override with CHECKIN_RETENTION_DAYS (read in api/handler.ts).
 */
export const DEFAULT_CHECKIN_RETENTION_DAYS = 30;

/**
 * The DynamoDB TTL (epoch seconds) for a check-in: the alert's end plus the retention period.
 * DynamoDB deletes expired rows some time after this moment (typically within 48 hours), so
 * treat it as an upper bound on retention, not an exact time.
 */
export function checkInTtlSeconds(
  alertEnd: Date,
  retentionDays: number = DEFAULT_CHECKIN_RETENTION_DAYS,
): number {
  return Math.floor(alertEnd.getTime() / 1000) + Math.round(retentionDays * 24 * 3600);
}

/** Check-in count for one alert and H3 cell, as kept in the totals table. */
export interface CellTotal {
  alertId: string;
  h3Cell: string;
  checkedIn: number;
}
