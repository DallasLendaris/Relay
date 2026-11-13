import { alertEndsAtMs, type Alert } from '@relay/shared';

/** Why an alert may not be planned or sent. Checked in the planner AND again in the worker. */
export type Refusal = 'unsigned' | 'not_actual' | 'inactive' | 'expired' | 'no_action';

const DAY_MS = 24 * 60 * 60 * 1000;

/** When the alert stops being relevant (see alertEndsAtMs in shared for the rule). */
export function alertEndTime(alert: Alert): Date {
  return new Date(alertEndsAtMs(alert));
}

/**
 * Defense in depth: an alert is sendable only if its signature verifies, it is a real
 * (not test/exercise) alert, it is active or updated, and it has not ended.
 * Returns null when sendable, otherwise the reason it was refused.
 */
export function refusalFor(
  alert: Alert,
  now: Date,
  verifyAlert: (alert: Alert) => boolean,
): Refusal | null {
  if (!verifyAlert(alert)) return 'unsigned';
  if (alert.status !== 'Actual') return 'not_actual';
  if (alert.state !== 'active' && alert.state !== 'updated') return 'inactive';
  // CAP urgency Past: "responsive action is no longer required" (a replaced, cancelled or
  // expiring product). Never pushed, even while the message has not expired yet.
  if (alert.urgency === 'Past') return 'no_action';
  // `!(a < b)` so an unparseable date (NaN) is refused too.
  if (!(now.getTime() < alertEndTime(alert).getTime())) return 'expired';
  return null;
}

/**
 * One push per device per alert per severity. A routine update at the same severity reuses
 * the key (no repeat push); an upgrade such as watch -> warning gets a new key.
 */
export const deliveryKey = (alert: Alert, deviceId: string): string =>
  `${alert.id}:${alert.relaySeverity}:${deviceId}`;

/** Claims outlive the alert by a day, so a late retry can never re-notify. */
export const claimExpiry = (alert: Alert): Date => new Date(alertEndTime(alert).getTime() + DAY_MS);
