import type { Alert } from '@relay/shared';
import {
  TEMPLATES_BY_CODE,
  TEMPLATES_BY_EVENT,
  type Language,
  type SummaryTemplate,
  type TemplateSet,
} from './templates.js';

export { EVENT_NAME_ES, TEMPLATES_BY_CODE, TEMPLATES_BY_EVENT } from './templates.js';
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

const DAY_MS = 24 * 60 * 60 * 1000;
/** An alert that starts later than this after it was sent reads "starting <time>", not "in effect". */
export const ONSET_FUTURE_MS = 10 * 60 * 1000;

function findTemplates(alert: Alert): TemplateSet | undefined {
  const byCode = alert.eventCode
    ? TEMPLATES_BY_CODE[alert.eventCode.trim().toUpperCase()]
    : undefined;
  return byCode ?? TEMPLATES_BY_EVENT[alert.event.trim().toLowerCase()];
}

/**
 * Formats a time as "6 PM PT Tuesday" (en) or "las 6 p. m. PT del martes" (es), in the given time
 * zone, with the date added ("Tuesday, November 10" / "martes 10 de noviembre") when asked.
 * Spanish says "la medianoche" / "el mediodía" for 12 a. m. / 12 p. m.
 * Returns null for a missing or invalid date. Built by hand from Intl parts because
 * Intl's own es-US pattern does not give us the "p. m." / "del martes" wording.
 */
export function formatEnd(
  iso: string | null | undefined,
  lang: Language,
  timeZone: string,
  withDate = false,
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
  const dayParts = new Intl.DateTimeFormat(LOCALES[lang], {
    timeZone,
    month: 'long',
    day: 'numeric',
  }).formatToParts(date);
  const month = dayParts.find((p) => p.type === 'month')?.value ?? '';
  const day = dayParts.find((p) => p.type === 'day')?.value ?? '';

  const hour12 = hour24 % 12 === 0 ? 12 : hour24 % 12;
  const clockText = `${hour12}${minute === 0 ? '' : `:${String(minute).padStart(2, '0')}`}`;
  const isPm = hour24 >= 12;

  if (lang === 'en') {
    const when = `${clockText} ${isPm ? 'PM' : 'AM'} PT ${weekday}`;
    return withDate ? `${when}, ${month} ${day}` : when;
  }
  const dateText = withDate ? ` ${day} de ${month}` : '';
  if (minute === 0 && hour24 === 0) return `la medianoche PT del ${weekday}${dateText}`;
  if (minute === 0 && hour24 === 12) return `el mediodía PT del ${weekday}${dateText}`;
  // Spanish: "la 1" for one o'clock, "las" for every other hour.
  const article = hour12 === 1 ? 'la' : 'las';
  return `${article} ${clockText} ${isPm ? 'p. m.' : 'a. m.'} PT del ${weekday}${dateText}`;
}

/** Splits a template into sentences. Done before filling in values so "p. m." is not split. */
export function sentencesOf(text: string): string[] {
  return text.split(/(?<=[.!?])\s+/);
}

/** Rewrites the lead sentence of a template for an alert that has not started yet. */
function futureLead(sentence: string, lang: Language, onset: string): string {
  const body = sentence.replace(/\.$/, '');
  if (lang === 'en') {
    if (!body.includes(' is in effect for ')) return sentence;
    return `${body.replace(' is in effect for ', ' is expected for ')}, starting ${onset}.`;
  }
  if (!body.startsWith('Hay ')) return sentence;
  // "a partir de el mediodía" is written "a partir del mediodía".
  const from = onset.startsWith('el ') ? `del ${onset.slice(3)}` : `de ${onset}`;
  return `Habrá ${body.slice(4)}, a partir ${from}.`;
}

function render(
  template: SummaryTemplate,
  lang: Language,
  values: Record<string, string | null>,
  onset: string | null,
): SummaryTemplate {
  const fallback = FALLBACK_LINE[lang];
  const out: string[] = [];
  sentencesOf(template.text).forEach((sentence, index) => {
    let missing = false;
    const filled = sentence.replace(/\{(\w+)\}/g, (_m, key: string) => {
      const value = values[key];
      if (!value) missing = true;
      return value ?? '';
    });
    // A sentence missing a value is replaced by the fallback line (shown at most once).
    let next = missing ? fallback : filled;
    if (!missing && index === 0 && onset) next = futureLead(next, lang, onset);
    if (!out.includes(next)) out.push(next);
  });
  return { whatToDo: template.whatToDo, text: out.join(' ') };
}

/**
 * Plain-language summaries for an alert, in English and Spanish.
 * Returns [] when we have no template for the event, and for a cancelled alert (a cancelled alert
 * must never read "in effect"). Only the event name, area description, start and end time are
 * taken from the alert. The end is `ends`, or `expires` when `ends` is missing. Times are judged
 * against when the alert was sent, so the text does not change when the alert is re-signed: an
 * onset more than 10 minutes after `sent` reads "starting <time>" instead of "in effect", and a
 * time more than 24 hours after `sent` includes its date.
 */
export function summariesFor(alert: Alert, opts: SummaryOptions): Alert['summaries'] {
  if (alert.state === 'cancelled') return [];
  const templates = findTemplates(alert);
  if (!templates) return [];
  const sentMs = Date.parse(alert.sent);
  const endIso = alert.ends ?? alert.expires;
  const far = (iso: string | null): boolean => iso !== null && Date.parse(iso) - sentMs > DAY_MS;
  const futureOnset =
    alert.onset !== null && Date.parse(alert.onset) - sentMs > ONSET_FUTURE_MS ? alert.onset : null;
  return LANGUAGES.map((language) => {
    const { whatToDo, text } = render(
      templates[language],
      language,
      {
        event: alert.event.trim() || null,
        area: alert.area.description.trim() || null,
        end: formatEnd(endIso, language, opts.timeZone, far(endIso)),
      },
      futureOnset ? formatEnd(futureOnset, language, opts.timeZone, far(futureOnset)) : null,
    );
    return { language, whatToDo, text };
  });
}
