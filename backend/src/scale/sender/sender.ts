/**
 * processBatch: sends one SendBatch to APNs / FCM and settles it.
 *
 * Order of operations (the order is the safety argument):
 *   1. claim batchKey with a 45 s lease ('sent' = already done, 'leased' = someone is on it)
 *   2. re-read the alert and re-check eligibility and the kill switch
 *   3. send all recipients (sub-chunks of SENDER_CHUNK_SIZE, all in flight at once)
 *   4. permanent failures: clear the dead target; retryable failures: re-queue as a NEW batch
 *      (`${base}:r${attempt + 1}`, only those recipients) up to MAX_ATTEMPT
 *   5. markSent(batchKey), always last
 *
 * Accepted trade-off: if step 4's re-enqueue fails we release the claim and throw, so the SQS
 * message becomes visible again and the WHOLE batch is retried. Recipients that were already
 * pushed in step 3 may then get one duplicate push. That needs an SQS outage at exactly the
 * wrong moment, and a duplicate alert is far better than a lost one, so we accept it. (Both
 * pushes share the alert's collapse key, so a phone shows one notification, not two.)
 */
import type { Alert } from '@relay/shared';
import { alertEndTime, claimExpiry, refusalFor } from '../../delivery/eligibility.js';
import { PUSH_POLICY } from '../../delivery/policy.js';
import { alertPushText } from '../../delivery/send-jobs.js';
import type { Logger } from '../../delivery/types.js';
import type { AlertStore, Clock, IdempotencyStore } from '../../ports.js';
import {
  QUEUE_FOR_SEVERITY,
  SENDER_CHUNK_SIZE,
  type NativePushMessage,
  type NativePushResult,
  type NativePushSender,
  type PushTarget,
  type SendBatch,
  type SendQueue,
} from '../contracts.js';

/** How long a claim holds. Must outlast a slow batch; the consumer extends SQS visibility to match. */
export const SENDER_LEASE_MS = 45_000;
/** Highest attempt number that is still sent; a batch at this attempt gives up on its failures. */
export const MAX_ATTEMPT = 5;
/** Wait before re-reading an alert whose signature failed (an update may have just committed). */
const REREAD_DELAY_MS = 200;

export interface SenderDeps {
  idempotency: IdempotencyStore;
  alerts: Pick<AlertStore, 'get'>;
  /** True only if the alert's signature is valid. */
  verifyAlert(alert: Alert): boolean;
  /** Kill switch. While paused nothing is sent and the message is retried later. */
  isDeliveryPaused(): Promise<boolean>;
  push: NativePushSender;
  sendQueue: SendQueue;
  /** Clears a dead target in the device store and the cell index. Failures are logged, never thrown. */
  onDeadTarget(deviceId: string, target: PushTarget): Promise<void>;
  clock: Clock;
  logger: Logger;
  /** Drops any cached copy of the alert before the signature re-read. */
  invalidateAlert?(id: string): void;
  /** Reloads the alert keyring so a newly rotated key is picked up (called at most once per batch). */
  refreshKeys?(): Promise<void>;
  /** Injectable so tests don't wait. */
  sleep?: (ms: number) => Promise<void>;
}

export type BatchReason =
  | 'sent'
  | 'duplicate'
  | 'leased'
  | 'alert_missing'
  | 'unsigned'
  | 'paused'
  | 'not_actual'
  | 'inactive'
  | 'expired'
  | 'informational'
  | 'stale_severity';

export interface BatchOutcome {
  /** True: this batch is finished (the SQS message may be deleted). False: leave it to retry. */
  ack: boolean;
  reason: BatchReason;
  sent: number;
  permanentFailed: number;
  retried: number;
  gaveUp: number;
}

const outcome = (
  ack: boolean,
  reason: BatchReason,
  over: Partial<BatchOutcome> = {},
): BatchOutcome => ({
  ack,
  reason,
  sent: 0,
  permanentFailed: 0,
  retried: 0,
  gaveUp: 0,
  ...over,
});

/** `${alertId}:${severity}:${cell}:${bucket}` without any `:r<n>` retry suffix. */
export const baseBatchKey = (batchKey: string): string => batchKey.replace(/:r\d+$/, '');

type Loaded = { alert: Alert } | { fail: BatchReason };

async function loadEligible(batch: SendBatch, deps: SenderDeps, now: Date): Promise<Loaded> {
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  let alert = await deps.alerts.get(batch.alertId);
  if (!alert) return { fail: 'alert_missing' }; // maybe read lag: retry
  let refusal = refusalFor(alert, now, deps.verifyAlert);
  if (refusal === 'unsigned') {
    // Torn read: an update may have committed between our reads. Look once more.
    await sleep(REREAD_DELAY_MS);
    deps.invalidateAlert?.(batch.alertId);
    alert = (await deps.alerts.get(batch.alertId)) ?? alert;
    refusal = refusalFor(alert, now, deps.verifyAlert);
  }
  if (refusal === 'unsigned' && deps.refreshKeys) {
    // Maybe our cached keyring lacks a newly rotated key. Reload once and check again.
    await deps.refreshKeys();
    refusal = refusalFor(alert, now, deps.verifyAlert);
  }
  // A signature failure is never acked: a real alert signed with a key we don't know yet must
  // not be lost. SQS retries, and a truly tampered row ends up in the DLQ for a human.
  if (refusal) return { fail: refusal };
  return { alert };
}

function buildMessages(batch: SendBatch, alert: Alert, now: Date): NativePushMessage[] {
  const policy = PUSH_POLICY[alert.relaySeverity];
  const end = alertEndTime(alert);
  const ttlSeconds = Math.max(1, Math.floor((end.getTime() - now.getTime()) / 1000));
  const text = { en: alertPushText(alert, 'en'), es: alertPushText(alert, 'es') };
  return batch.recipients.map((r) => ({
    target: r.target,
    title: text[r.language].title,
    body: text[r.language].body,
    data: { alertId: alert.id, checkInToken: r.checkInToken, expiresAt: end.toISOString() },
    priority: policy.priority,
    sound: policy.sound !== null,
    collapseKey: alert.id,
    ttlSeconds,
    category: 'alert-checkin' as const,
  }));
}

async function sendAll(
  messages: NativePushMessage[],
  deps: SenderDeps,
  batchKey: string,
): Promise<NativePushResult[]> {
  const chunks: NativePushMessage[][] = [];
  for (let i = 0; i < messages.length; i += SENDER_CHUNK_SIZE) {
    chunks.push(messages.slice(i, i + SENDER_CHUNK_SIZE));
  }
  const perChunk = await Promise.all(
    chunks.map(async (chunk): Promise<NativePushResult[]> => {
      try {
        const res = await deps.push.send(chunk);
        // Fewer results than messages is an adapter bug: treat the missing ones as retryable.
        return chunk.map((_m, i) => res[i] ?? { ok: false, kind: 'retryable', error: 'NoResult' });
      } catch (err) {
        deps.logger.error('sender.chunk_threw', {
          batchKey,
          size: chunk.length,
          // Name only: the message text could carry a token.
          error: err instanceof Error ? err.name : 'Error',
        });
        return chunk.map(() => ({ ok: false, kind: 'retryable', error: 'SendThrew' }));
      }
    }),
  );
  return perChunk.flat();
}

export async function processBatch(batch: SendBatch, deps: SenderDeps): Promise<BatchOutcome> {
  const { idempotency, logger } = deps;
  const claim = await idempotency.claim(
    batch.batchKey,
    new Date(deps.clock.now().getTime() + SENDER_LEASE_MS),
  );
  if (claim === 'sent') return outcome(true, 'duplicate');
  if (claim === 'leased') return outcome(false, 'leased'); // another attempt is live; retry later

  const release = async () => {
    await idempotency.release(batch.batchKey).catch((err: unknown) => {
      logger.error('sender.release_failed', { batchKey: batch.batchKey, error: String(err) });
    });
  };

  try {
    const now = deps.clock.now();
    const loaded = await loadEligible(batch, deps, now);
    if ('fail' in loaded) {
      await release();
      const reason = loaded.fail;
      const final = reason === 'not_actual' || reason === 'inactive' || reason === 'expired';
      logger.error(final ? 'sender.alert_refused' : 'sender.alert_unavailable', {
        alertId: batch.alertId,
        batchKey: batch.batchKey,
        reason,
      });
      return outcome(final, reason);
    }
    const { alert } = loaded;
    if (!PUSH_POLICY[alert.relaySeverity].push) {
      await release();
      return outcome(true, 'informational');
    }
    if (alert.relaySeverity !== batch.relaySeverity) {
      // The alert was re-severitied after this batch was planned; the upgrade has its own
      // fan-out and keys. Sending this stale batch would use the wrong urgency.
      await release();
      logger.warn('sender.stale_batch', { alertId: alert.id, batchKey: batch.batchKey });
      return outcome(true, 'stale_severity');
    }
    if (await deps.isDeliveryPaused()) {
      await release();
      logger.warn('sender.paused', { batchKey: batch.batchKey });
      return outcome(false, 'paused');
    }

    const messages = buildMessages(batch, alert, now);
    const results = await sendAll(messages, deps, batch.batchKey);

    let sent = 0;
    const dead: Array<{ deviceId: string; target: PushTarget }> = [];
    const failed: SendBatch['recipients'] = [];
    results.forEach((r, i) => {
      const rec = batch.recipients[i];
      if (!rec) return;
      if (r.ok) sent++;
      else if (r.kind === 'permanent') dead.push({ deviceId: rec.deviceId, target: rec.target });
      else failed.push(rec);
    });

    await Promise.all(
      dead.map((d) =>
        deps.onDeadTarget(d.deviceId, d.target).catch((err: unknown) => {
          logger.error('sender.clear_dead_target_failed', {
            deviceId: d.deviceId,
            error: err instanceof Error ? err.name : 'Error',
          });
        }),
      ),
    );

    let retried = 0;
    let gaveUp = 0;
    if (failed.length > 0) {
      if (batch.attempt >= MAX_ATTEMPT) {
        gaveUp = failed.length;
        logger.error('delivery.gave_up', {
          alertId: alert.id,
          batchKey: batch.batchKey,
          attempt: batch.attempt,
          count: gaveUp,
        });
      } else {
        const retryBatch: SendBatch = {
          ...batch,
          batchKey: `${baseBatchKey(batch.batchKey)}:r${batch.attempt + 1}`,
          recipients: failed,
          attempt: batch.attempt + 1,
        };
        // See the file header: a failure here releases the claim and fails the whole message.
        await deps.sendQueue.enqueue(QUEUE_FOR_SEVERITY[batch.relaySeverity] ?? 'standard', [
          retryBatch,
        ]);
        retried = failed.length;
      }
    }

    // Last: only now is the batch finished for good. If this fails the pushes already went out;
    // ack anyway (failing the message would re-send once the lease runs out).
    await idempotency.markSent(batch.batchKey, claimExpiry(alert)).catch((err: unknown) => {
      logger.error('sender.mark_sent_failed', { batchKey: batch.batchKey, error: String(err) });
    });
    logger.info('sender.batch_done', {
      alertId: alert.id,
      batchKey: batch.batchKey,
      attempt: batch.attempt,
      sent,
      permanentFailed: dead.length,
      retried,
      gaveUp,
    });
    return outcome(true, 'sent', { sent, permanentFailed: dead.length, retried, gaveUp });
  } catch (err) {
    // Re-enqueue failed, or the alert read / kill switch read threw. Give the claim back so
    // the redelivered message can take it immediately, and fail the message.
    await release();
    throw err;
  }
}
