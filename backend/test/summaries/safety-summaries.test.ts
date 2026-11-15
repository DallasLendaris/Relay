import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import {
  FALLBACK_LINE,
  TEMPLATES_BY_CODE,
  TEMPLATES_BY_EVENT,
  formatEnd,
  sentencesOf,
  summariesFor,
} from '../../src/summaries/index.js';

const opts = { timeZone: 'America/Los_Angeles' } as const;

function makeAlert(over: Partial<Alert> = {}): Alert {
  return {
    id: '6f1c1d2e-3b4a-4c5d-8e9f-0a1b2c3d4e5f',
    source: 'nws',
    sourceId: 'urn:oid:s1',
    event: 'Heat Advisory',
    eventCode: null,
    relaySeverity: 'advisory',
    capSeverity: 'Minor',
    urgency: 'Expected',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: { description: 'Fresno', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: [] },
    // Monday November 9 2026, 10 AM Pacific.
    sent: '2026-11-09T10:00:00-08:00',
    effective: '2026-11-09T10:00:00-08:00',
    onset: null,
    expires: '2026-11-09T18:00:00-08:00',
    ends: null,
    official: {
      headline: null,
      description: 'Official text.',
      instruction: null,
      senderName: 'NWS Hanford CA',
      language: 'en',
    },
    summaries: [],
    signature: null,
    ...over,
  };
}

const text = (a: Alert, lang: 'en' | 'es'): string =>
  summariesFor(a, opts).find((s) => s.language === lang)?.text ?? '';

describe('SAFE-3: templates for the top California events', () => {
  const EVENTS = [
    'Tornado Warning',
    'Tornado Watch',
    'Tsunami Warning',
    'Tsunami Advisory',
    'Tsunami Watch',
    'Extreme Wind Warning',
    'High Wind Warning',
    'Wind Advisory',
    'Winter Storm Warning',
    'Winter Weather Advisory',
    'Dust Storm Warning',
    'Severe Thunderstorm Warning',
    'Severe Thunderstorm Watch',
    'Earthquake Warning',
    'Evacuation Immediate',
    'Shelter In Place Warning',
    'Air Quality Alert',
    'Dense Smoke Advisory',
    'Special Weather Statement',
    'Beach Hazards Statement',
  ];

  it.each(EVENTS)('%s has English and Spanish text, with the area and no leftovers', (event) => {
    const alert = makeAlert({ event });
    const result = summariesFor(alert, opts);
    expect(result.map((s) => s.language)).toEqual(['en', 'es']);
    for (const s of result) {
      expect(s.whatToDo.length).toBeGreaterThan(0);
      expect(s.text).toContain('Fresno');
      expect(s.text).not.toMatch(/[{}]/);
      expect(s.text).not.toContain(FALLBACK_LINE.en);
      expect(s.text).not.toContain(FALLBACK_LINE.es);
    }
  });

  it('every template follows the lead-sentence shape the future-onset rewrite relies on', () => {
    for (const set of new Set([
      ...Object.values(TEMPLATES_BY_CODE),
      ...Object.values(TEMPLATES_BY_EVENT),
    ])) {
      expect(sentencesOf(set.en.text)[0]).toContain(' is in effect for ');
      expect(sentencesOf(set.es.text)[0]).toMatch(/^Hay /);
    }
  });

  it('the Spanish tier words match the NWS convention', () => {
    expect(text(makeAlert({ event: 'Tornado Warning' }), 'es')).toContain('aviso de tornado');
    expect(text(makeAlert({ event: 'Tornado Watch' }), 'es')).toContain('vigilancia de tornado');
    expect(text(makeAlert({ event: 'Wind Advisory' }), 'es')).toContain('advertencia de viento');
  });
});

describe('SAFE-9: Spanish corrections', () => {
  it('uses the plural for flash floods and Revise instead of Fíjese', () => {
    for (const set of Object.values(TEMPLATES_BY_EVENT)) {
      const all = `${set.es.text} ${set.es.whatToDo}`;
      expect(all).not.toContain('inundación repentina');
      expect(all).not.toContain('Fíjese');
    }
    expect(text(makeAlert({ event: 'Flash Flood Warning' }), 'es')).toContain(
      'inundaciones repentinas',
    );
    expect(summariesFor(makeAlert({ event: 'Extreme Heat Warning' }), opts)[1]?.whatToDo).toContain(
      'Revise cómo están',
    );
  });

  it('says medianoche and mediodía for 12 a. m. and 12 p. m.', () => {
    expect(formatEnd('2026-11-10T00:00:00-08:00', 'es', opts.timeZone)).toBe(
      'la medianoche PT del martes',
    );
    expect(formatEnd('2026-11-10T12:00:00-08:00', 'es', opts.timeZone)).toBe(
      'el mediodía PT del martes',
    );
    expect(formatEnd('2026-11-10T12:30:00-08:00', 'es', opts.timeZone)).toBe(
      'las 12:30 p. m. PT del martes',
    );
    const a = makeAlert({
      sent: '2026-11-09T20:00:00-08:00',
      expires: '2026-11-10T00:00:00-08:00',
    });
    expect(text(a, 'es')).toContain('hasta la medianoche PT del martes');
  });
});

describe('SAFE-2 and SAFE-10: end, onset, date and time zone', () => {
  it('uses ends when it is set, otherwise expires, and says PT', () => {
    const withEnds = makeAlert({
      expires: '2026-11-09T12:00:00-08:00',
      ends: '2026-11-09T20:00:00-08:00',
    });
    expect(text(withEnds, 'en')).toContain('until 8 PM PT Monday');
    expect(text(withEnds, 'es')).toContain('hasta las 8 p. m. PT del lunes');
    expect(text(makeAlert(), 'en')).toContain('until 6 PM PT Monday');
  });

  it('uses ends even when it is earlier than expires', () => {
    const a = makeAlert({
      expires: '2026-11-09T18:00:00-08:00',
      ends: '2026-11-09T15:00:00-08:00',
    });
    expect(text(a, 'en')).toContain('until 3 PM PT Monday');
  });

  it('an onset more than 10 minutes ahead reads "starting", never "in effect"', () => {
    const a = makeAlert({
      onset: '2026-11-09T14:00:00-08:00',
      expires: '2026-11-09T12:00:00-08:00',
      ends: '2026-11-09T20:00:00-08:00',
    });
    expect(text(a, 'en')).toBe(
      'A Heat Advisory is expected for Fresno, starting 2 PM PT Monday. It will be hot until 8 PM PT Monday.',
    );
    expect(text(a, 'es')).toBe(
      'Habrá una advertencia de calor para Fresno, a partir de las 2 p. m. PT del lunes. Va a hacer calor hasta las 8 p. m. PT del lunes.',
    );
    expect(text(a, 'en')).not.toContain('in effect');
  });

  it('Spanish onset at noon uses "a partir del mediodía"', () => {
    const a = makeAlert({ onset: '2026-11-09T12:00:00-08:00', ends: '2026-11-09T20:00:00-08:00' });
    expect(text(a, 'es')).toContain('a partir del mediodía PT del lunes');
  });

  it('an onset within 10 minutes (or in the past) is still "in effect"', () => {
    expect(text(makeAlert({ onset: '2026-11-09T10:09:00-08:00' }), 'en')).toContain('in effect');
    expect(text(makeAlert({ onset: '2026-11-09T09:00:00-08:00' }), 'en')).toContain('in effect');
    expect(text(makeAlert({ onset: '2026-11-09T10:11:00-08:00' }), 'en')).not.toContain(
      'in effect',
    );
  });

  it('adds the date when the end or the start is more than 24 hours away', () => {
    const far = makeAlert({
      onset: '2026-11-11T09:00:00-08:00',
      ends: '2026-11-12T20:00:00-08:00',
      expires: '2026-11-12T20:00:00-08:00',
    });
    expect(text(far, 'en')).toContain('starting 9 AM PT Wednesday, November 11');
    expect(text(far, 'en')).toContain('until 8 PM PT Thursday, November 12');
    expect(text(far, 'es')).toContain('a partir de las 9 a. m. PT del miércoles 11 de noviembre');
    expect(text(far, 'es')).toContain('hasta las 8 p. m. PT del jueves 12 de noviembre');
    // exactly inside 24 hours: no date
    expect(text(makeAlert({ expires: '2026-11-10T09:00:00-08:00' }), 'en')).toContain(
      'until 9 AM PT Tuesday.',
    );
  });

  it('does not depend on the clock: the same alert gives the same text', () => {
    const a = makeAlert({ onset: '2026-11-09T14:00:00-08:00' });
    expect(summariesFor(a, opts)).toEqual(summariesFor({ ...a }, opts));
  });
});

describe('SAFE-4: no summaries for a cancelled alert', () => {
  it('returns [] when the alert is cancelled', () => {
    const a = makeAlert({ event: 'Flash Flood Warning', eventCode: 'FFW' });
    expect(summariesFor(a, opts)).toHaveLength(2);
    expect(summariesFor({ ...a, state: 'cancelled' }, opts)).toEqual([]);
  });
});
