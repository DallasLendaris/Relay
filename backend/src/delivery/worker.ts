import type { SQSBatchResponse, SQSEvent } from 'aws-lambda';
import type { AlertStore, DeliveryJob, PushSender } from '../ports.js';
import { sendJobs } from './send-jobs.js';
import type { Logger } from './types.js';

/** Errors that will never succeed on retry (the token is dead). Don't make SQS retry these. */
const PERMANENT = new Set(['DeviceNotRegistered', 'InvalidCredentials', 'MessageTooBig']);

/**
 * Lambda core for the delivery queue. Returns batchItemFailures so SQS retries
 * only the records that failed (enable ReportBatchItemFailures on the event source).
 */
export async function deliveryWorker(
  event: SQSEvent,
  deps: { sender: PushSender; alerts: Pick<AlertStore, 'get'>; logger: Logger },
): Promise<SQSBatchResponse> {
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

  let results;
  try {
    results = await sendJobs(
      parsed.map((p) => p.job),
      deps,
    );
  } catch (err) {
    deps.logger.error('send threw', { error: String(err) });
    return { batchItemFailures: event.Records.map((r) => ({ itemIdentifier: r.messageId })) };
  }

  results.forEach((r, i) => {
    const entry = parsed[i];
    if (entry && !r.ok && !PERMANENT.has(r.error)) failures.push(entry.messageId);
  });
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
