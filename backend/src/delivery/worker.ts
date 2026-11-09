import type { Alert } from '@relay/shared';
import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import type { AlertStore, Clock, DeliveryJob, IdempotencyStore, PushSender } from '../ports.js';
import { claimExpiry, deliveryKey, refusalFor } from './eligibility.js';
import { sendJobs } from './send-jobs.js';
import type { Logger } from './types.js';

/** Errors that will never succeed on retry. Keep the claim and don't make SQS retry these. */
const PERMANENT = new Set(['DeviceNotRegistered', 'InvalidCredentials', 'MessageTooBig']);

export interface WorkerDeps {
  sender: PushSender;
  alerts: Pick<AlertStore, 'get'>;
  idempotency: IdempotencyStore;
  /** True only if the alert's signature is valid. */
  verifyAlert(alert: Alert): boolean;
  /** Kill switch. While paused, nothing is sent and every record is retried later. */
  isDeliveryPaused(): Promise<boolean>;
  /** Called when Expo says a token is dead (DeviceNotRegistered). */
  onDeadToken(token: string): Promise<void>;
  clock: Clock;
  logger: Logger;
}

/**
 * Lambda core for the delivery queue. Returns batchItemFailures so SQS retries
 * only the records that failed (enable ReportBatchItemFailures on the event source).
 *
 * Per job: claim the dedupe key, send, and on a retryable failure release the claim and
 * fail the record, so the retry can send. Dedupe lives here (not in the planner) so a
 * failed enqueue or a timeout can never silently suppress a push.
 */
export async function deliveryWorker(event: SQSEvent, deps: WorkerDeps): Promise<SQSBatchResponse> {
  const failAll = (): SQSBatchResponse => ({
    batchItemFailures: event.Records.map((r) => ({ itemIdentifier: r.messageId })),
  });

  if (await deps.isDeliveryPaused()) {
    deps.logger.warn('delivery paused, not sending', { records: event.Records.length });
    return failAll(); // retried later; they reach the DLQ if the pause lasts too long
  }

  const failures: string[] = [];
  const parsed: Array<{ messageId: string; job: DeliveryJob }> = [];
  for (const record of event.Records) {
    const job = parseJob(record.body);
    if (job) parsed.push({ messageId: record.messageId, job });
    else {
      deps.logger.error('unparseable delivery job', { messageId: record.messageId });
      failures.push(record.messageId); // retried, then lands in the DLQ for a human to see
    }
  }

  // Re-read each alert from the store and re-check it: a row edited after planning is never pushed.
  const now = deps.clock.now();
  const loaded = new Map<string, Alert | 'missing' | 'refused'>();
  for (const id of new Set(parsed.map((p) => p.job.alertId))) {
    const alert = await deps.alerts.get(id);
    const refusal = alert ? refusalFor(alert, now, deps.verifyAlert) : null;
    if (refusal) deps.logger.error('refusing to send alert', { alertId: id, reason: refusal });
    loaded.set(id, !alert ? 'missing' : refusal ? 'refused' : alert);
  }
  const verified = new Map<string, Alert>();
  for (const [id, a] of loaded) if (typeof a !== 'string') verified.set(id, a);

  const toSend: Array<{ messageId: string; job: DeliveryJob; key: string }> = [];
  for (const { messageId, job } of parsed) {
    const alert = verified.get(job.alertId);
    if (!alert) {
      // A refused alert is dropped for good (acknowledged). A missing one may be read lag: retry.
      if (loaded.get(job.alertId) === 'missing') failures.push(messageId);
      continue;
    }
    const key = deliveryKey(alert, job.deviceId);
    try {
      if (await deps.idempotency.claim(key, claimExpiry(alert)))
        toSend.push({ messageId, job, key });
      else deps.logger.info('duplicate skipped', { alertId: alert.id, deviceId: job.deviceId });
    } catch (err) {
      deps.logger.error('claim failed', { key, error: String(err) });
      failures.push(messageId);
    }
  }

  if (toSend.length) {
    // sendJobs reads alerts from this map, so the text sent is the text we just verified.
    const results = await sendJobs(
      toSend.map((t) => t.job),
      {
        sender: deps.sender,
        alerts: { get: async (id) => verified.get(id) ?? null },
        logger: deps.logger,
      },
    ).catch((err: unknown) => {
      deps.logger.error('send threw', { error: String(err) });
      return null;
    });

    for (const [i, entry] of toSend.entries()) {
      const r = results?.[i] ?? { ok: false as const, error: 'SendFailed' };
      if (r.ok) continue;
      if (PERMANENT.has(r.error)) {
        // Keep the claim: this push must never be retried.
        if (r.error === 'DeviceNotRegistered') {
          await deps.onDeadToken(entry.job.expoPushToken).catch((err: unknown) => {
            deps.logger.error('clearing dead token failed', { error: String(err) });
          });
        }
        continue;
      }
      // Retryable: give the claim back so the retry can send, and fail the record.
      await deps.idempotency.release(entry.key).catch((err: unknown) => {
        deps.logger.error('releasing claim failed', { key: entry.key, error: String(err) });
      });
      failures.push(entry.messageId);
    }
  }
  return { batchItemFailures: failures.map((itemIdentifier) => ({ itemIdentifier })) };
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
      typeof j.groupingKey === 'string'
    ) {
      return j as DeliveryJob;
    }
  } catch {
    // fall through
  }
  return null;
}
