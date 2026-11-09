import type { Alert } from '@relay/shared';
import type { AlertStore, DeliveryJob, PushMessage, PushSender } from '../ports.js';
import { PUSH_POLICY } from './policy.js';
import { redactPushTokens } from './redact.js';
import type { Logger, PolicyPushMessage, SendResult } from './types.js';

const SEVERITY_WORD: Record<Alert['relaySeverity'], string> = {
  emergency: 'Emergency',
  warning: 'Warning',
  watch: 'Watch',
  advisory: 'Advisory',
  statement: 'Statement',
};

const SEVERITY_WORD_ES: Record<Alert['relaySeverity'], string> = {
  emergency: 'Emergencia',
  warning: 'Aviso',
  watch: 'Vigilancia',
  advisory: 'Advertencia',
  statement: 'Declaración',
};

export function buildMessage(job: DeliveryJob, alert: Alert): PushMessage & PolicyPushMessage {
  // Spanish uses the Relay summary. Without one we fall back to the official English text.
  const spanish =
    job.language === 'es'
      ? alert.summaries.find((s) => s.language === 'es' && s.text.trim())
      : undefined;
  const body = spanish
    ? spanish.text.trim()
    : alert.official.headline?.trim() || alert.official.description.trim().slice(0, 140);
  const policy = PUSH_POLICY[alert.relaySeverity];
  const word = spanish ? SEVERITY_WORD_ES : SEVERITY_WORD;
  return {
    to: job.expoPushToken,
    title: `${word[alert.relaySeverity]}: ${alert.event}`,
    body,
    data: {
      alertId: job.alertId,
      checkInToken: job.checkInToken,
      expiresAt: job.expiresAt,
      groupingKey: job.groupingKey,
    },
    categoryId: 'alert-checkin',
    priority: policy.priority,
    ...(policy.sound ? { sound: policy.sound } : {}),
  };
}

/**
 * Send jobs through the PushSender. Returns one result per job, in order.
 * A DeliveryJob only carries ids, so the alert text is looked up in `alerts`
 * (once per distinct alertId).
 */
export async function sendJobs(
  jobs: DeliveryJob[],
  deps: { sender: PushSender; alerts: Pick<AlertStore, 'get'>; logger: Logger },
): Promise<SendResult[]> {
  const alerts = new Map<string, Alert | null>();
  for (const id of new Set(jobs.map((j) => j.alertId))) alerts.set(id, await deps.alerts.get(id));

  const results: SendResult[] = new Array<SendResult>(jobs.length);
  const messages: PushMessage[] = [];
  const messageIndex: number[] = [];
  jobs.forEach((job, i) => {
    const alert = alerts.get(job.alertId);
    if (!alert) {
      results[i] = { ok: false, error: 'AlertNotFound' };
      return;
    }
    messages.push(buildMessage(job, alert));
    messageIndex.push(i);
  });

  const sent = messages.length ? await deps.sender.send(messages) : [];
  messageIndex.forEach((jobIdx, k) => {
    results[jobIdx] = sent[k] ?? { ok: false, error: 'NoResult' };
  });
  results.forEach((r, i) => {
    if (!r.ok)
      deps.logger.warn('push failed', {
        deviceId: jobs[i]?.deviceId,
        error: redactPushTokens(r.error),
      });
  });
  return results;
}
