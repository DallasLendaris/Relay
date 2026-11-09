import type { Alert } from '@relay/shared';
import {
  TEMPLATES_BY_CODE,
  TEMPLATES_BY_EVENT,
  type Language,
  type SummaryTemplate,
  type TemplateSet,
} from './templates.js';

export { TEMPLATES_BY_CODE, TEMPLATES_BY_EVENT } from './templates.js';
export type { Language, SummaryTemplate, TemplateSet } from './templates.js';

/** The UI must show this label next to any Relay-written summary. */
export const RELAY_SUMMARY_LABEL = { en: 'Relay summary', es: 'Resumen de Relay' } as const;

export const FALLBACK_LINE = {
  en: 'See official message.',
  es: 'Lea el mensaje oficial.',
} as const;

export interface SummaryOptions {
  timeZone: 'America/Los_Angeles';
}

const LANGUAGES: Language[] = ['en', 'es'];
const LOCALES: Record<Language, string> = { en: 'en-US', es: 'es-US' };

function findTemplates(alert: Alert): TemplateSet | undefined {
  const byCode = alert.eventCode
    ? TEMPLATES_BY_CODE[alert.eventCode.trim().toUpperCase()]
    : undefined;
  return byCode ?? TEMPLATES_BY_EVENT[alert.event.trim().toLowerCase()];
}

/**
 * Formats a time as "6 PM Tuesday" (en) or "las 6 p. m. del martes" (es), in the given time zone.
 * Returns null for a missing or invalid date. Built by hand from Intl parts because
 * Intl's own es-US pattern does not give us the "p. m." / "del martes" wording.
 */
export function formatEnd(
  iso: string | null | undefined,
  lang: Language,
  timeZone: string,
): string | null {
  if (!iso) return null;
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return null;

  const clock = new Intl.DateTimeFormat('en-US', {
    timeZone,
    hour: 'numeric',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date);
  const hour24 = Number(clock.find((p) => p.type === 'hour')?.value);
  const minute = Number(clock.find((p) => p.type === 'minute')?.value);
  const weekday = new Intl.DateTimeFormat(LOCALES[lang], { timeZone, weekday: 'long' }).format(
    date,
  );

  const hour12 = hour24 % 12 === 0 ? 12 : hour24 % 12;
  const clockText = `${hour12}${minute === 0 ? '' : `:${String(minute).padStart(2, '0')}`}`;
  const isPm = hour24 >= 12;

  if (lang === 'en') return `${clockText} ${isPm ? 'PM' : 'AM'} ${weekday}`;
  // Spanish: "la 1" for one o'clock, "las" for every other hour.
  const article = hour12 === 1 ? 'la' : 'las';
  return `${article} ${clockText} ${isPm ? 'p. m.' : 'a. m.'} del ${weekday}`;
}

/** Splits a template into sentences. Done before filling in values so "p. m." is not split. */
export function sentencesOf(text: string): string[] {
  return text.split(/(?<=[.!?])\s+/);
}

function render(
  template: SummaryTemplate,
  lang: Language,
  values: Record<string, string | null>,
): SummaryTemplate {
  const fallback = FALLBACK_LINE[lang];
  const out: string[] = [];
  for (const sentence of sentencesOf(template.text)) {
    let missing = false;
    const filled = sentence.replace(/\{(\w+)\}/g, (_m, key: string) => {
      const value = values[key];
      if (!value) missing = true;
      return value ?? '';
    });
    // A sentence missing a value is replaced by the fallback line (shown at most once).
    const next = missing ? fallback : filled;
    if (!out.includes(next)) out.push(next);
  }
  return { whatToDo: template.whatToDo, text: out.join(' ') };
}

/**
 * Plain-language summaries for an alert, in English and Spanish.
 * Returns [] when we have no template for the event. Only the event name, area description and
 * end time are taken from the alert.
 */
export function summariesFor(alert: Alert, opts: SummaryOptions): Alert['summaries'] {
  const templates = findTemplates(alert);
  if (!templates) return [];
  return LANGUAGES.map((language) => {
    const { whatToDo, text } = render(templates[language], language, {
      event: alert.event.trim() || null,
      area: alert.area.description.trim() || null,
      end: formatEnd(alert.expires, language, opts.timeZone),
    });
    return { language, whatToDo, text };
  });
}
