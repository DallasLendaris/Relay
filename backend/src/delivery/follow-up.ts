/**
 * Follow-up pushes: telling a phone that was sent an alert that the alert was cancelled, changed,
 * or no longer covers it. Design and rules: docs/cancellation.md. Shared by the scale path (planner,
 * follow-up shard worker, sender) and the inline (dev) delivery worker.
 */
import { createHash } from 'node:crypto';
import type { Alert } from '@relay/shared';
import { EVENT_NAME_ES } from '../summaries/index.js';
import { alertEndTime } from './eligibility.js';
import { PUSH_POLICY, type PushPolicy } from './policy.js';
import { alertPushText, shortSenderName } from './send-jobs.js';

/** What the follow-up says: cancelled, changed (still covers the device), or no longer covers it. */
export type FollowUpKind = 'cancel' | 'update' | 'removed';

/** What ingest asks the planner for: the follow-up of a stored Cancel or of a stored Update. */
export type FollowUpRequest = 'cancel' | 'update';

/**
 * Delay of a cancel follow-up batch (and of an update follow-up that found an original batch still
 * being sent): longer than the sender's 45 s lease plus the alert caches (5 s sender, 10 s fan-out),
 * so a batch of the earlier version that was in flight when the new version committed lands first.
 */
export const FOLLOW_UP_DELAY_SECONDS = 75;

/** Short, stable tag of one alert version: the first 12 hex digits of sha256(sourceId). */
export const versionTag = (sourceId: string): string =>
  createHash('sha256').update(sourceId).digest('hex').slice(0, 12);

type VersionedAlert = Pick<Alert, 'id' | 'relaySeverity' | 'sourceId'>;

/**
 * The batch key of one (cell, bucket) of a follow-up. Distinct from the original's
 * `${id}:${severity}:${cell}:${bucket}` (so it is never deduped against it), and from any other
 * version's follow-up (the tag), but the same for every re-plan of this version.
 */
export const followUpBatchKey = (
  alert: VersionedAlert,
  kind: FollowUpKind,
  h3Cell: string,
  bucket: number,
): string =>
  `${alert.id}:${alert.relaySeverity}:fu.${kind}.${versionTag(alert.sourceId)}:${h3Cell}:${bucket}`;

/** The inline worker's per-device follow-up key. */
export const followUpDeviceKey = (
  alert: VersionedAlert,
  kind: FollowUpKind,
  deviceId: string,
): string => `${alert.id}:fu.${kind}.${versionTag(alert.sourceId)}:${deviceId}`;

const FOLLOW_UP_KEY = /:fu\.(cancel|update|removed)\.([0-9a-f]{12}):/;

/**
 * The follow-up kind and version tag a batch key carries, or null for an original batch. Keys the
 * sender derives (`:r<n>`, `:v<n>`, `:h<n>`, `:u<n>`, `:s<path>`) keep the prefix, so a retried or
 * re-checked follow-up is still read as one.
 */
export function parseFollowUpKey(key: string): { kind: FollowUpKind; version: string } | null {
  const m = FOLLOW_UP_KEY.exec(key);
  return m ? { kind: m[1] as FollowUpKind, version: m[2] as string } : null;
}

const QUIET: PushPolicy = { push: true, priority: 'normal', sound: null };

/**
 * Loudness of a follow-up. A cancellation or "no longer includes your area" is an all-clear: quiet
 * (normal priority, no sound, never time-sensitive). An update keeps the new severity's policy;
 * one that the policy would not push at all (a statement) is still sent, quietly, so a loud
 * warning is not left standing on the lock screen.
 */
export function followUpPolicy(
  kind: FollowUpKind,
  alert: Pick<Alert, 'relaySeverity'>,
): PushPolicy {
  if (kind !== 'update') return QUIET;
  const policy = PUSH_POLICY[alert.relaySeverity];
  return policy.push ? policy : QUIET;
}

/**
 * Whether an Update's follow-up also covers the cells the Update kept. Only when the severity did
 * not change (the normal fan-out reuses the same batch keys, so it re-notifies nobody), or the new
 * severity is not pushed at all. A severity change re-notifies those devices through the normal
 * fan-out under new keys, so a follow-up there would be a second push.
 */
export function followUpCoversKeptCells(
  alert: Pick<Alert, 'relaySeverity' | 'supersedes'>,
): boolean {
  const previous = alert.supersedes?.relaySeverity;
  return previous === alert.relaySeverity || !PUSH_POLICY[alert.relaySeverity].push;
}

/** The kind for one cell of a follow-up fan-out. */
export const followUpKindFor = (request: FollowUpRequest, inCurrentArea: boolean): FollowUpKind =>
  request === 'cancel' ? 'cancel' : inCurrentArea ? 'update' : 'removed';

/** Spanish event name, or the official English one when we have none. */
const eventEs = (alert: Alert): string =>
  EVENT_NAME_ES[alert.event.trim().toLowerCase()] ?? alert.event;

/**
 * Plain-language title and body of a follow-up. Fixed templates (no machine translation); the
 * Spanish ones need the same native-speaker read as the summaries (docs/summaries-spanish-review.md).
 */
export function followUpPushText(
  alert: Alert,
  kind: FollowUpKind,
  language: 'en' | 'es',
): { title: string; body: string } {
  const sender = shortSenderName(alert.official.senderName, alert.source);
  if (kind === 'update') {
    const text = alertPushText(alert, language);
    return {
      title: `${language === 'es' ? 'Actualización' : 'Updated'}: ${text.title}`,
      body: text.body,
    };
  }
  if (kind === 'cancel') {
    return language === 'es'
      ? {
          title: `Cancelado: ${eventEs(alert)}`,
          body: `${sender} canceló esta alerta. Siga las demás alertas que estén vigentes.`,
        }
      : {
          title: `Cancelled: ${alert.event}`,
          body: `${sender} cancelled this alert. Follow any other alerts still in effect.`,
        };
  }
  return language === 'es'
    ? {
        title: `Actualización: ${eventEs(alert)}`,
        body: `${sender} cambió esta alerta. Ya no incluye su zona.`,
      }
    : {
        title: `Update: ${alert.event}`,
        body: `${sender} changed this alert. It no longer includes your area.`,
      };
}

/** Why a follow-up may not be planned or sent (besides the kill switch). */
export type FollowUpRefusal = 'unsigned' | 'not_actual' | 'stale_version' | 'expired';

/**
 * Defense in depth for follow-ups, checked by the planner, the shard worker and the sender:
 * signed, Actual, still the alert's current version (`version`, a versionTag) in the state the
 * kind needs (a cancel needs `cancelled`; an update or removed needs active or updated), and the
 * alert's original end still ahead (after that there is nothing left to tell). The current-version
 * rule is what keeps a cancellation the last word: an Update follow-up still queued when a Cancel
 * lands is dropped here.
 */
export function followUpRefusal(
  alert: Alert,
  kind: FollowUpKind,
  version: string,
  now: Date,
  verifyAlert: (alert: Alert) => boolean,
): FollowUpRefusal | null {
  if (!verifyAlert(alert)) return 'unsigned';
  if (alert.status !== 'Actual') return 'not_actual';
  if (versionTag(alert.sourceId) !== version) return 'stale_version';
  const live = alert.state === 'active' || alert.state === 'updated';
  if (kind === 'cancel' ? alert.state !== 'cancelled' : !live) return 'stale_version';
  if (!(now.getTime() < alertEndTime(alert).getTime())) return 'expired';
  return null;
}
