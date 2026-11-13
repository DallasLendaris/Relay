import type { Alert } from '@relay/shared';
import type { AlertStore, DeliveryJob, PushMessage, PushSender } from '../ports.js';
import { EVENT_NAME_ES, RELAY_SUMMARY_LABEL } from '../summaries/index.js';
import { PUSH_POLICY } from './policy.js';
import { redactPushTokens } from './redact.js';
import type { Logger, PolicyPushMessage, SendResult } from './types.js';

const SEVERITY_WORD_ES: Record<Alert['relaySeverity'], string> = {
  emergency: 'Emergencia',
  warning: 'Aviso',
  watch: 'Vigilancia',
  advisory: 'Advertencia',
  statement: 'Comunicado',
};

/** Longest part of a push body that comes from a raw NWS description. */
export const MAX_DESCRIPTION_CHARS = 140;

/**
 * Plain text from a raw NWS description for a push body: drops "* WHAT..." style section markers
 * and the "...ALL CAPS..." wrapper, joins lines, and keeps whole sentences up to `limit`
 * characters. A first sentence that is too long is cut at a word boundary and ends with "…".
 */
export function cleanDescription(description: string, limit = MAX_DESCRIPTION_CHARS): string {
  const flat = description
    .replace(/\*\s*[A-Za-z][A-Za-z ]*\.\.\.\s*/g, ' ')
    .replace(/^\s*\.\.\.+\s*/, '')
    .replace(/\s*\.\.\.+\s*$/, '')
    .replace(/\s+/g, ' ')
    .trim();
  if (flat === '') return '';
  const sentences = flat.match(/[^.!?]+(?:[.!?]+(?=\s|$)|$)/g)?.map((x) => x.trim()) ?? [flat];
  let out = '';
  for (const sentence of sentences) {
    const next = out ? `${out} ${sentence}` : sentence;
    if (next.length <= limit) {
      out = next;
      continue;
    }
    if (out) return out; // whole sentences only, once at least one fits
    const room = flat.slice(0, limit - 1);
    const cut = room.lastIndexOf(' ');
    const words = (cut > 0 ? room.slice(0, cut) : room).replace(/[\s,;:.\-–]+$/, '');
    return `${words}…`;
  }
  return out;
}

/** "NWS Hanford CA" becomes "NWS Hanford": the sender name without the trailing state code. */
export function shortSenderName(senderName: string, source: Alert['source'] = 'nws'): string {
  const name = senderName.trim().replace(/\s+[A-Z]{2}$/, '');
  return name || source.toUpperCase();
}

/** The official one-line text: the NWS headline, or a clean start of the description. */
function officialLine(alert: Alert): string {
  return (
    alert.official.headline?.trim() || cleanDescription(alert.official.description) || alert.event
  );
}

/** " – EMERGENCY" or " – PDS" for the highest tiers, unless the event name already says so. */
function titleSuffix(alert: Alert): string {
  if (alert.relaySeverity === 'emergency' && !/emergency/i.test(alert.event)) {
    return ' – EMERGENCY';
  }
  if (alert.pds === true && !/\bpds\b|particularly dangerous/i.test(alert.event)) return ' – PDS';
  return '';
}

/** Spanish tier labels for the title suffix (never leave an English tier word in a Spanish title). */
export const TITLE_SUFFIX_ES = {
  emergency: ' – EMERGENCIA',
  pds: ' – PELIGRO PARTICULARMENTE ALTO',
} as const;

function titleSuffixEs(alert: Alert): string {
  const en = titleSuffix(alert);
  if (en === ' – EMERGENCY') return TITLE_SUFFIX_ES.emergency;
  if (en === ' – PDS') return TITLE_SUFFIX_ES.pds;
  return '';
}

/**
 * Spanish title: the Spanish event name (it already carries Aviso / Vigilancia / Advertencia)
 * plus the Spanish tier label. An event we have no Spanish name for keeps the old shape, Spanish
 * tier word then the official English event name, but still gets the Spanish suffix.
 */
function spanishTitle(alert: Alert): string {
  const suffix = titleSuffixEs(alert);
  const name = EVENT_NAME_ES[alert.event.trim().toLowerCase()];
  if (name) return `${name}${suffix}`;
  return `${SEVERITY_WORD_ES[alert.relaySeverity]}: ${alert.event}${suffix}`;
}

/**
 * The title and body for one alert in one language (shared by the Expo worker and the native
 * sender).
 * - English: title is the official event name (plus " – PDS" or " – EMERGENCY"); body is the
 *   sender and the official headline or a clean first sentence of the description.
 * - Spanish with a Relay summary: the body says it is a Relay summary and not official, and
 *   names the source. The official wording is never passed off as, or replaced by, the summary.
 * - Spanish without a summary: Spanish event name (or tier word and official event name) in the title, and the
 *   body says the official alert is English only, followed by the English text.
 */
export function alertPushText(
  alert: Alert,
  language: 'en' | 'es',
): { title: string; body: string } {
  const suffix = titleSuffix(alert);
  if (language === 'en') {
    const sender = shortSenderName(alert.official.senderName, alert.source);
    return { title: `${alert.event}${suffix}`, body: `${sender}: ${officialLine(alert)}` };
  }
  const title = spanishTitle(alert);
  const summary = alert.summaries.find((s) => s.language === 'es' && s.text.trim());
  if (summary) {
    return {
      title,
      body: `${RELAY_SUMMARY_LABEL.es} (no oficial): ${summary.text.trim()} Fuente: ${alert.source.toUpperCase()}`,
    };
  }
  const official = alert.source === 'nws' ? 'Alerta oficial del NWS' : 'Alerta oficial';
  return { title, body: `${official} (solo en inglés): ${officialLine(alert)}` };
}

export function buildMessage(job: DeliveryJob, alert: Alert): PushMessage & PolicyPushMessage {
  const { title, body } = alertPushText(alert, job.language);
  const policy = PUSH_POLICY[alert.relaySeverity];
  return {
    to: job.expoPushToken,
    title,
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
