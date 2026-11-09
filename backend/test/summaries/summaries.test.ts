import { describe, expect, it } from 'vitest';
import { AlertSchema, type Alert } from '@relay/shared';
import {
  FALLBACK_LINE,
  RELAY_SUMMARY_LABEL,
  TEMPLATES_BY_CODE,
  TEMPLATES_BY_EVENT,
  formatEnd,
  sentencesOf,
  summariesFor,
} from '../../src/summaries/index.js';

const opts = { timeZone: 'America/Los_Angeles' } as const;

function makeAlert(overrides: Partial<Alert> = {}): Alert {
  return {
    id: '6f1c1d2e-3b4a-4c5d-8e9f-0a1b2c3d4e5f',
    source: 'nws',
    sourceId: 'urn:oid:2.49.0.1.840.0.test.001',
    event: 'Flash Flood Warning',
    eventCode: 'FFW',
    relaySeverity: 'warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: {
      description: 'Santa Clara Valley',
      geometry: null,
      ugcCodes: ['CAZ513'],
      sameCodes: ['006085'],
      h3Cells: [],
    },
    sent: '2026-01-12T20:00:00-08:00',
    effective: '2026-01-12T20:00:00-08:00',
    onset: null,
    // Tuesday January 13 2026, 6 PM Pacific.
    expires: '2026-01-13T18:00:00-08:00',
    ends: null,
    official: {
      headline: null,
      description: 'Official text.',
      instruction: null,
      senderName: 'NWS San Francisco Bay Area',
      language: 'en',
    },
    summaries: [],
    signature: null,
    ...overrides,
  };
}

// One sample event per template, to check every template renders.
const SAMPLE_EVENTS: Array<[string, string | null]> = [
  ...Object.keys(TEMPLATES_BY_CODE).map((c): [string, string | null] => ['Some Event', c]),
  ...Object.keys(TEMPLATES_BY_EVENT).map((e): [string, string | null] => [e, null]),
];

describe('summariesFor', () => {
  it.each(SAMPLE_EVENTS)('renders en and es for event %s code %s', (event, eventCode) => {
    const result = summariesFor(makeAlert({ event, eventCode }), opts);
    expect(result.map((s) => s.language)).toEqual(['en', 'es']);
    for (const s of result) {
      expect(s.whatToDo.length).toBeGreaterThan(0);
      expect(s.text).toContain('Santa Clara Valley');
      expect(s.text).not.toMatch(/[{}]/);
      expect(s.text).not.toContain(FALLBACK_LINE.en);
      expect(s.text).not.toContain(FALLBACK_LINE.es);
    }
  });

  it('covers flood, fire and heat with the confirmed SAME codes', () => {
    for (const code of ['FFW', 'FFA', 'FLW', 'FLA', 'FRW']) {
      expect(TEMPLATES_BY_CODE[code]).toBeDefined();
    }
    for (const name of ['red flag warning', 'extreme heat warning', 'heat advisory']) {
      expect(TEMPLATES_BY_EVENT[name]).toBeDefined();
    }
  });

  it('matches by event name when eventCode is null or unknown, ignoring case', () => {
    const byName = summariesFor(makeAlert({ event: 'Red Flag Warning', eventCode: null }), opts);
    expect(byName).toHaveLength(2);
    const unknownCode = summariesFor(makeAlert({ event: 'Heat Advisory', eventCode: 'ZZZ' }), opts);
    expect(unknownCode).toHaveLength(2);
  });

  it('prefers the code over the name', () => {
    const result = summariesFor(makeAlert({ event: 'Heat Advisory', eventCode: 'FFW' }), opts);
    expect(result[0]?.whatToDo).toBe(TEMPLATES_BY_CODE['FFW']?.en.whatToDo);
  });

  it('returns [] for unknown events', () => {
    expect(
      summariesFor(makeAlert({ event: 'Winter Storm Watch', eventCode: 'WSA' }), opts),
    ).toEqual([]);
    expect(summariesFor(makeAlert({ event: 'Earthquake', eventCode: null }), opts)).toEqual([]);
  });

  it('formats the end time in local time in both languages', () => {
    const [en, es] = summariesFor(makeAlert(), opts);
    expect(en?.text).toContain('until 6 PM Tuesday.');
    expect(es?.text).toContain('hasta las 6 p. m. del martes.');
    expect(en?.text).toContain('Flash Flood Warning');
  });

  it('converts UTC to Pacific time', () => {
    // 02:30 UTC Wednesday is 6:30 PM Tuesday in Pacific standard time.
    expect(formatEnd('2026-01-14T02:30:00Z', 'en', opts.timeZone)).toBe('6:30 PM Tuesday');
    expect(formatEnd('2026-01-14T02:30:00Z', 'es', opts.timeZone)).toBe(
      'las 6:30 p. m. del martes',
    );
  });

  it('handles 1 o clock, noon and morning in Spanish and English', () => {
    expect(formatEnd('2026-01-13T13:00:00-08:00', 'es', opts.timeZone)).toBe(
      'la 1 p. m. del martes',
    );
    expect(formatEnd('2026-01-13T12:00:00-08:00', 'en', opts.timeZone)).toBe('12 PM Tuesday');
    expect(formatEnd('2026-01-13T09:00:00-08:00', 'es', opts.timeZone)).toBe(
      'las 9 a. m. del martes',
    );
  });

  it('uses daylight time in summer', () => {
    expect(formatEnd('2026-07-14T01:00:00Z', 'en', opts.timeZone)).toBe('6 PM Monday');
  });

  it('falls back when the end time is missing or invalid', () => {
    for (const expires of ['not a date', '']) {
      const [en, es] = summariesFor(makeAlert({ expires }), opts);
      expect(en?.text).toBe(
        `A Flash Flood Warning is in effect for Santa Clara Valley. ${FALLBACK_LINE.en}`,
      );
      expect(es?.text).toBe(
        `Hay un aviso de inundación repentina para Santa Clara Valley. ${FALLBACK_LINE.es}`,
      );
    }
  });

  it('falls back when the area is missing', () => {
    const alert = makeAlert();
    const [en, es] = summariesFor({ ...alert, area: { ...alert.area, description: '  ' } }, opts);
    expect(en?.text).toBe(`${FALLBACK_LINE.en} It lasts until 6 PM Tuesday.`);
    expect(es?.text).toBe(`${FALLBACK_LINE.es} Dura hasta las 6 p. m. del martes.`);
  });

  it('shows the fallback line only once when both area and time are missing', () => {
    const alert = makeAlert({ expires: 'bad' });
    const [en] = summariesFor({ ...alert, area: { ...alert.area, description: '' } }, opts);
    expect(en?.text).toBe(FALLBACK_LINE.en);
  });

  it('passes the shared Alert schema', () => {
    for (const event of Object.keys(TEMPLATES_BY_EVENT)) {
      const alert = makeAlert({ event, eventCode: null });
      const parsed = AlertSchema.safeParse({ ...alert, summaries: summariesFor(alert, opts) });
      expect(parsed.success).toBe(true);
    }
  });

  it('exports the Relay summary label', () => {
    expect(RELAY_SUMMARY_LABEL).toEqual({ en: 'Relay summary', es: 'Resumen de Relay' });
  });

  it('keeps whatToDo to 1 to 3 steps', () => {
    for (const set of Object.values(TEMPLATES_BY_CODE)) {
      for (const lang of ['en', 'es'] as const) {
        const steps = set[lang].whatToDo.split(/(?<=\.)\s+/);
        expect(steps.length).toBeGreaterThanOrEqual(1);
        expect(steps.length).toBeLessThanOrEqual(3);
      }
    }
  });
});

/** Rough English syllable counter: counts vowel groups, drops a silent final "e". */
function syllables(word: string): number {
  const w = word.toLowerCase().replace(/[^a-z]/g, '');
  if (w.length <= 3) return w.length > 0 ? 1 : 0;
  const stripped = w.replace(/(?:[^laeiouy]es|ed|[^laeiouy]e)$/, '').replace(/^y/, '');
  return Math.max(1, stripped.match(/[aeiouy]{1,2}/g)?.length ?? 1);
}

describe('English readability', () => {
  // Fill placeholders with short stand-ins. Real place names are exempt from the syllable rule.
  const fill = (text: string) =>
    text
      .replace('{event}', 'Flood Warning')
      .replace('{area}', 'Alameda')
      .replace('{end}', '6 PM Tuesday');
  const sets = new Set([...Object.values(TEMPLATES_BY_CODE), ...Object.values(TEMPLATES_BY_EVENT)]);

  it('the syllable heuristic behaves', () => {
    expect(syllables('flood')).toBe(1);
    expect(syllables('information')).toBe(4);
    expect(syllables('possible')).toBe(3);
    expect(syllables('neighbors')).toBe(2);
    expect(syllables('make')).toBe(1);
  });

  it('averages 12 words per sentence or fewer, with no word over 4 syllables', () => {
    const sentences: string[] = [];
    for (const set of sets) {
      sentences.push(...sentencesOf(fill(set.en.text)), ...sentencesOf(set.en.whatToDo));
    }
    const counts = sentences.map((s) => s.split(/\s+/).filter(Boolean).length);
    const average = counts.reduce((a, b) => a + b, 0) / counts.length;
    expect(average).toBeLessThanOrEqual(12);

    const longWords = sentences.flatMap((s) => s.split(/\s+/)).filter((w) => syllables(w) > 4);
    expect(longWords).toEqual([]);
  });
});
