import type { Alert } from '@relay/shared';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import type { AlertStore, Clock, DeliveryJob, IdempotencyStore, PushSender } from '../ports.js';
import { claimExpiry, deliveryKey, refusalFor } from './eligibility.js';
import {
  followUpDeviceKey,
  followUpPolicy,
  followUpPushText,
  followUpRefusal,
  versionTag,
} from './follow-up.js';
import { PUSH_POLICY } from './policy.js';
import { errorFields } from './redact.js';
import { sendJobs } from './send-jobs.js';
import type { Logger } from './types.js';

/** Errors that will never succeed on retry. Keep the claim and don't make SQS retry these. */
const PERMANENT = new Set(['DeviceNotRegistered', 'MessageTooBig']);
// InvalidCredentials is deliberately NOT here: it means our Expo access token or project
// setup is wrong. That is a configuration problem to fix and retry, not a dead device.

/** Lambda timeout of the worker (infra tests check the stack uses the same number). */
export const WORKER_TIMEOUT_MS = 20_000;
/** SQS visibility timeout of the delivery queue (infra tests check the stack matches). */
export const VISIBILITY_TIMEOUT_MS = 30_000;
/**
 * How long a claim holds before another attempt may take over. It must outlast the worker
 * timeout (so a live attempt is never doubled) and stay under 2x the visibility timeout (so a
 * crashed attempt is retried by the second redelivery at the latest).
 */
export const LEASE_MS = 45_000;
/** Wait before re-reading an alert whose signature failed (an update may have just committed). */
const REREAD_DELAY_MS = 200;

export interface WorkerDeps {
  sender: PushSender;
  alerts: Pick<AlertStore, 'get'>;
  idempotency: IdempotencyStore;
  /** True only if the alert's signature is valid. */
  verifyAlert(alert: Alert): boolean;
  /** Kill switch. While paused, nothing is sent and every record is retried later. */
  isDeliveryPaused(): Promise<boolean>;
  /**
   * Drops the cached alert keyring and reloads it, so a newly rotated key is picked up.
   * Called at most once per invocation, only after a signature failed.
   */
  refreshKeys?(): Promise<void>;
  /** Called when Expo says a token is dead (DeviceNotRegistered). */
  onDeadToken(token: string): Promise<void>;
  clock: Clock;
  logger: Logger;
  /** Injectable so tests don't wait. */
  sleep?: (ms: number) => Promise<void>;
}

type Loaded = Alert | 'missing' | 'refused' | 'error';

/**
 * Lambda core for the delivery queue. Returns batchItemFailures so SQS retries
 * only the records that failed (enable ReportBatchItemFailures on the event source).
 *
 * Per job: claim the dedupe key with a lease, send, then mark it sent. A retryable failure
 * releases the claim and fails the record. If the Lambda dies after claiming, the lease
 * runs out and the SQS redelivery claims again, so a push is never silently dropped.
 * Dedupe lives here (not in the planner) so a failed enqueue or a timeout can never
 * suppress a push.
 */
export async function deliveryWorker(event: SQSEvent, deps: WorkerDeps): Promise<SQSBatchResponse> {
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const failAll = (): SQSBatchResponse => ({
    batchItemFailures: event.Records.map((r) => ({ itemIdentifier: r.messageId })),
  });

  if (await deps.isDeliveryPaused()) {
    deps.logger.warn('delivery paused, not sending', { records: event.Records.length });
    return failAll(); // retried later; they reach the DLQ if the pause lasts too long
  }

  const failures: string[] = [];
  const parsed: Array<{ messageId: string; job: DeliveryJob }> = [];
  const followUps: Array<{ messageId: string; job: DeliveryJob }> = [];
  for (const record of event.Records) {
    const job = parseJob(record.body);
    if (job?.followUp) followUps.push({ messageId: record.messageId, job });
    else if (job) parsed.push({ messageId: record.messageId, job });
    else {
      deps.logger.error('unparseable delivery job', { messageId: record.messageId });
      failures.push(record.messageId); // retried, then lands in the DLQ for a human to see
    }
  }

  // Re-read each alert from the store and re-check it: a row edited after planning is never pushed.
  const now = deps.clock.now();
  const loaded = new Map<string, Loaded>();
  let refreshing: Promise<void> | undefined;
  for (const id of new Set(parsed.map((p) => p.job.alertId))) {
    loaded.set(id, await loadAlert(id));
  }

  async function loadAlert(id: string): Promise<Loaded> {
    try {
      let alert = await deps.alerts.get(id);
      let refusal = alert ? refusalFor(alert, now, deps.verifyAlert) : null;
      if (alert && refusal === 'unsigned') {
        // Torn read: an update may have committed between our reads. Look once more.
        await sleep(REREAD_DELAY_MS);
        alert = await deps.alerts.get(id);
        refusal = alert ? refusalFor(alert, now, deps.verifyAlert) : null;
      }
      if (alert && refusal === 'unsigned' && deps.refreshKeys) {
        // Maybe our cached keyring lacks a newly rotated key. Reload it once and check again.
        refreshing ??= deps.refreshKeys();
        await refreshing;
        refusal = refusalFor(alert, now, deps.verifyAlert);
      }
      if (refusal === 'unsigned') {
        // Never ack this: a real alert signed with a key we don't know yet must not be lost.
        // SQS retries, and a truly tampered row ends up in the DLQ for a human.
        deps.logger.error('delivery.signature_failed', { alertId: id });
        return 'error';
      }
      if (refusal) deps.logger.error('refusing to send alert', { alertId: id, reason: refusal });
      return !alert ? 'missing' : refusal ? 'refused' : alert;
    } catch (err) {
      // A DB error fails only this alert's records, not the whole batch.
      // Class and code only: a database message can quote the row (SI-11).
      deps.logger.error('reading alert failed', { alertId: id, ...errorFields(err) });
      return 'error';
    }
  }

  const toSend: Array<{ messageId: string; job: DeliveryJob; key: string; alert: Alert }> = [];
  for (const { messageId, job } of parsed) {
    const alert = loaded.get(job.alertId);
    if (!alert || typeof alert === 'string') {
      // A refused alert (not actual, inactive, expired) is dropped for good (acknowledged). A
      // missing one may be read lag, and a read or signature error may be temporary: retry those.
      if (alert !== 'refused') failures.push(messageId);
      continue;
    }
    if (!PUSH_POLICY[alert.relaySeverity].push) {
      deps.logger.info('informational alert, not pushing', { alertId: alert.id });
      continue; // acknowledged
    }
    const key = deliveryKey(alert, job.deviceId);
    // The same key twice in one batch (a re-planned alert): the first copy handles it.
    // If that one fails, its own retry sends, so this copy can be acknowledged.
    if (toSend.some((t) => t.key === key)) {
      deps.logger.info('duplicate in batch skipped', { alertId: alert.id });
      continue;
    }
    try {
      const leaseUntil = new Date(deps.clock.now().getTime() + LEASE_MS);
      const claim = await deps.idempotency.claim(key, leaseUntil);
      if (claim === 'claimed') toSend.push({ messageId, job, key, alert });
      else if (claim === 'sent') {
        deps.logger.info('duplicate skipped', { alertId: alert.id });
      } else {
        // Another attempt is in flight. Never ack: if it dies, the retry after its lease sends.
        deps.logger.info('claim leased by another attempt', { alertId: alert.id });
        failures.push(messageId);
      }
    } catch (err) {
      // The key embeds the device id, so it is not logged.
      deps.logger.error('claim failed', { alertId: alert.id, ...errorFields(err) });
      failures.push(messageId);
    }
  }

  if (toSend.length) {
    const verified = new Map(toSend.map((t) => [t.alert.id, t.alert]));
    // sendJobs reads alerts from this map, so the text sent is the text we just verified.
    const results = await sendJobs(
      toSend.map((t) => t.job),
      {
        sender: deps.sender,
        alerts: { get: async (id) => verified.get(id) ?? null },
        logger: deps.logger,
      },
    ).catch((err: unknown) => {
      deps.logger.error('send threw', errorFields(err));
      return null;
    });

    for (const [i, entry] of toSend.entries()) {
      const r = results?.[i] ?? { ok: false as const, error: 'SendFailed' };
      if (r.ok || PERMANENT.has(r.error)) {
        // Sent, or never going to succeed: either way this push must not be repeated.
        if (!r.ok && r.error === 'DeviceNotRegistered') {
          await deps.onDeadToken(entry.job.expoPushToken).catch((err: unknown) => {
            deps.logger.error('clearing dead token failed', {
              alertId: entry.alert.id,
              ...errorFields(err),
            });
          });
        }
        // If this fails the push already went out. Ack anyway: failing the record would
        // re-send once the lease expires. The lease simply runs out.
        await deps.idempotency
          .markSent(entry.key, claimExpiry(entry.alert), {
            // Which version this device was sent (follow-ups skip the current one).
            version: versionTag(entry.alert.sourceId),
          })
          .catch((err: unknown) => {
            // The key embeds the device id, so it is not logged (nor is the error text).
            deps.logger.error('marking sent failed', {
              alertId: entry.alert.id,
              ...errorFields(err),
            });
          });
        continue;
      }
      // Retryable: give the claim back so the retry can send, and fail the record.
      await deps.idempotency.release(entry.key).catch((err: unknown) => {
        deps.logger.error('releasing claim failed', {
          alertId: entry.alert.id,
          ...errorFields(err),
        });
      });
      failures.push(entry.messageId);
    }
  }
  if (followUps.length > 0) failures.push(...(await sendFollowUps(followUps, deps)));
  return { batchItemFailures: failures.map((itemIdentifier) => ({ itemIdentifier })) };
}

/** Tags of keys written without a real send (delivery/follow-up.ts, fanout/follow-up.ts). */
const TOMBSTONE_VERSION = 'tombstone';
const FOLLOW_UP_LEASE_MS = LEASE_MS;

/**
 * Inline follow-ups (docs/cancellation.md). Per job: the device's delivery key for the version it
 * was sent says whether it got one (absent: no follow-up; for a cancel the key is first marked sent
 * so a late original can never go out; leased: retry later, so the follow-up lands after it; sent
 * for the current version: it already has the new text). Then the follow-up's own key is claimed,
 * the push is sent quietly (or at the new severity's loudness for an update), and the key marked
 * sent. Returns the message ids to retry.
 */
async function sendFollowUps(
  jobs: Array<{ messageId: string; job: DeliveryJob }>,
  deps: WorkerDeps,
): Promise<string[]> {
  const failures: string[] = [];
  const now = deps.clock.now();
  const alerts = new Map<string, Alert | null | 'error'>();
  for (const id of new Set(jobs.map((j) => j.job.alertId))) {
    alerts.set(
      id,
      await deps.alerts.get(id).catch((err: unknown) => {
        deps.logger.error('reading alert failed', { alertId: id, ...errorFields(err) });
        return 'error' as const;
      }),
    );
  }
  const store = deps.idempotency;
  const toSend: Array<{ messageId: string; job: DeliveryJob; key: string; alert: Alert }> = [];
  for (const { messageId, job } of jobs) {
    const fu = job.followUp;
    const alert = alerts.get(job.alertId);
    if (!fu) continue;
    if (!alert || alert === 'error') {
      failures.push(messageId);
      continue;
    }
    const refusal = followUpRefusal(alert, fu.kind, fu.version, now, deps.verifyAlert);
    if (refusal === 'unsigned') {
      deps.logger.error('delivery.signature_failed', { alertId: alert.id });
      failures.push(messageId);
      continue;
    }
    if (refusal) {
      deps.logger.info('followup.refused', { alertId: alert.id, reason: refusal });
      continue; // final: acknowledged
    }
    try {
      const original = `${alert.id}:${fu.previousSeverity}:${job.deviceId}`;
      let seen = store.inspect
        ? await store.inspect(original)
        : { state: await peekOf(store, original) };
      if (seen.state === 'absent' && fu.kind === 'cancel') {
        const claim = await store.claim(original, new Date(now.getTime() + FOLLOW_UP_LEASE_MS));
        if (claim === 'claimed') {
          await store.markSent(original, claimExpiry(alert), { version: TOMBSTONE_VERSION });
          continue; // never sent: nothing to follow up, and now it never will be
        }
        seen = { state: claim };
      }
      if (seen.state === 'absent') continue;
      if (seen.state === 'leased') {
        failures.push(messageId); // the original is going out right now: follow up after it
        continue;
      }
      if (
        'version' in seen &&
        (seen.version === TOMBSTONE_VERSION || seen.version === versionTag(alert.sourceId))
      ) {
        continue;
      }
      const key = followUpDeviceKey(alert, fu.kind, job.deviceId);
      const claim = await store.claim(key, new Date(now.getTime() + FOLLOW_UP_LEASE_MS));
      if (claim === 'claimed') toSend.push({ messageId, job, key, alert });
      else if (claim === 'leased') failures.push(messageId);
    } catch (err) {
      deps.logger.error('claim failed', { alertId: alert.id, ...errorFields(err) });
      failures.push(messageId);
    }
  }
  if (toSend.length === 0) return failures;
  const results = await deps.sender
    .send(toSend.map((t) => followUpMessage(t.job, t.alert)))
    .catch((err: unknown) => {
      deps.logger.error('send threw', errorFields(err));
      return null;
    });
  for (const [i, entry] of toSend.entries()) {
    const r = results?.[i] ?? { ok: false as const, error: 'SendFailed' };
    if (r.ok || PERMANENT.has(r.error)) {
      // Dead tokens are left to the original alert's own path (the follow-up never clears them).
      await deps.idempotency.markSent(entry.key, claimExpiry(entry.alert)).catch((err: unknown) => {
        deps.logger.error('marking sent failed', { alertId: entry.alert.id, ...errorFields(err) });
      });
      continue;
    }
    await deps.idempotency.release(entry.key).catch(() => undefined);
    failures.push(entry.messageId);
  }
  return failures;
}

async function peekOf(store: IdempotencyStore, key: string): Promise<'absent' | 'leased' | 'sent'> {
  if (!store.peek) throw new Error('follow-ups need IdempotencyStore.peek or inspect');
  return store.peek(key);
}

function followUpMessage(job: DeliveryJob, alert: Alert) {
  const kind = job.followUp?.kind ?? 'cancel';
  const { title, body } = followUpPushText(alert, kind, job.language);
  const policy = followUpPolicy(kind, alert);
  return {
    to: job.expoPushToken,
    title,
    body,
    data: {
      alertId: job.alertId,
      checkInToken: job.checkInToken,
      expiresAt: job.expiresAt,
      groupingKey: job.groupingKey,
      followUp: kind,
    },
    categoryId: 'alert-checkin',
    priority: policy.priority,
    ...(policy.sound ? { sound: policy.sound } : {}),
  };
}

function parseJob(body: string): DeliveryJob | null {
  try {
    const j = JSON.parse(body) as Partial<DeliveryJob> | null;
    if (
      j &&
      typeof j.alertId === 'string' &&
      typeof j.deviceId === 'string' &&
      typeof j.expoPushToken === 'string' &&
      typeof j.checkInToken === 'string' &&
      typeof j.expiresAt === 'string' &&
      typeof j.groupingKey === 'string' &&
      (j.followUp === undefined ||
        (['cancel', 'update', 'removed'].includes(j.followUp.kind) &&
          typeof j.followUp.version === 'string' &&
          typeof j.followUp.previousSeverity === 'string'))
    ) {
      return j as DeliveryJob;
    }
  } catch {
    // fall through
  }
  return null;
}
