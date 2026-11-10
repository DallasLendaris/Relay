import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import {
  MAX_DESCRIPTION_CHARS,
  alertPushText,
  cleanDescription,
  shortSenderName,
} from '../../src/delivery/send-jobs.js';
import { EVENT_NAME_ES, TEMPLATES_BY_EVENT, summariesFor } from '../../src/summaries/index.js';
import { makeAlert } from './fakes.js';

const withSummaries = (a: Alert): Alert => ({
  ...a,
  summaries: summariesFor(a, { timeZone: 'America/Los_Angeles' }),
});

const tornado = makeAlert({
  event: 'Tornado Warning',
  eventCode: 'TOR',
  official: {
    headline: 'Tornado Warning issued November 10 at 2:00AM PST until 6:00AM PST by NWS Hanford',
    description: '* WHAT...A tornado.',
    instruction: null,
    senderName: 'NWS Hanford CA',
    language: 'en-US',
  },
});

describe('GOV-4 / SAFE-7: English push text', () => {
  it('title is the official event name; body names the sender and uses the headline', () => {
    expect(alertPushText(tornado, 'en')).toEqual({
      title: 'Tornado Warning',
      body: 'NWS Hanford: Tornado Warning issued November 10 at 2:00AM PST until 6:00AM PST by NWS Hanford',
    });
  });

  it('adds PDS or EMERGENCY to the title', () => {
    expect(alertPushText({ ...tornado, pds: true }, 'en').title).toBe('Tornado Warning – PDS');
    expect(alertPushText({ ...tornado, relaySeverity: 'emergency' }, 'en').title).toBe(
      'Tornado Warning – EMERGENCY',
    );
    // an event name that already says Emergency is not repeated
    const named = {
      ...tornado,
      event: 'Flash Flood Emergency',
      relaySeverity: 'emergency' as const,
    };
    expect(alertPushText(named, 'en').title).toBe('Flash Flood Emergency');
    expect(alertPushText(makeAlert(), 'en').title).toBe('Flash Flood Warning');
  });

  it('without a headline, uses a cleaned first sentence of the description (SAFE-8)', () => {
    const a = makeAlert({
      official: {
        ...tornado.official,
        headline: null,
        description:
          '* WHAT...Flash flooding caused by excessive rainfall.\n\n* WHERE...Fresno County.\n\n* WHEN...Until 6 AM.',
      },
    });
    expect(alertPushText(a, 'en').body).toBe(
      'NWS Hanford: Flash flooding caused by excessive rainfall. Fresno County. Until 6 AM.',
    );
  });

  it('falls back to the event name when there is no text and "NWS" when there is no sender', () => {
    const a = makeAlert({
      official: { ...tornado.official, headline: null, description: '', senderName: '' },
    });
    expect(alertPushText(a, 'en').body).toBe('NWS: Flash Flood Warning');
  });
});

describe('SAFE-8: cleanDescription', () => {
  it('strips section markers and joins lines', () => {
    expect(cleanDescription('* WHAT...Heavy rain.\n* IMPACTS...Roads flood.')).toBe(
      'Heavy rain. Roads flood.',
    );
    expect(cleanDescription('...A TORNADO WARNING REMAINS IN EFFECT...')).toBe(
      'A TORNADO WARNING REMAINS IN EFFECT',
    );
    expect(cleanDescription('   ')).toBe('');
  });

  it('keeps whole sentences within the limit', () => {
    const long = `${'Short sentence here. '.repeat(10)}`;
    const out = cleanDescription(long, 50);
    expect(out).toBe('Short sentence here. Short sentence here.');
    expect(out.length).toBeLessThanOrEqual(50);
  });

  it('cuts a too-long first sentence at a word boundary with an ellipsis', () => {
    const sentence = `${'flooding '.repeat(40)}ends.`;
    const out = cleanDescription(sentence);
    expect(out.endsWith('…')).toBe(true);
    expect(out.length).toBeLessThanOrEqual(MAX_DESCRIPTION_CHARS);
    expect(out.slice(0, -1).endsWith('flooding')).toBe(true); // not mid-word
  });

  it('cuts a single huge word at the limit', () => {
    const out = cleanDescription('x'.repeat(300));
    expect(out).toHaveLength(MAX_DESCRIPTION_CHARS);
    expect(out.endsWith('…')).toBe(true);
  });
});

describe('shortSenderName', () => {
  it('drops the trailing state code', () => {
    expect(shortSenderName('NWS San Francisco Bay Area CA')).toBe('NWS San Francisco Bay Area');
    expect(shortSenderName('NWS Hanford CA')).toBe('NWS Hanford');
    expect(shortSenderName('NWS')).toBe('NWS');
    expect(shortSenderName('  ')).toBe('NWS');
    expect(shortSenderName('', 'ipaws')).toBe('IPAWS');
  });
});

describe('GOV-4 / SAFE-7: Spanish push text', () => {
  it('with a summary: labelled as an unofficial Relay summary with the source', () => {
    const a = withSummaries(tornado);
    const { title, body } = alertPushText(a, 'es');
    expect(title).toBe('Aviso de tornado');
    expect(body.startsWith('Resumen de Relay (no oficial): ')).toBe(true);
    expect(body.endsWith(' Fuente: NWS')).toBe(true);
    expect(body).toContain('aviso de tornado');
    // the official headline is never used as, or mixed into, the summary
    expect(body).not.toContain('issued');
  });

  it('Spanish title: Spanish event name plus EMERGENCIA / PELIGRO PARTICULARMENTE ALTO, no English tier words', () => {
    const emergency = withSummaries({ ...tornado, relaySeverity: 'emergency' });
    expect(alertPushText(emergency, 'es').title).toBe('Aviso de tornado – EMERGENCIA');
    expect(alertPushText({ ...withSummaries(tornado), pds: true }, 'es').title).toBe(
      'Aviso de tornado – PELIGRO PARTICULARMENTE ALTO',
    );
    expect(alertPushText(makeAlert({ relaySeverity: 'statement' }), 'es').title).toBe(
      'Aviso de inundaciones repentinas',
    );
    // an event that already says "Emergency" gets no second label
    const named = makeAlert({ event: 'Flash Flood Emergency', relaySeverity: 'emergency' });
    expect(alertPushText(named, 'es').title).toBe('Emergencia por inundaciones repentinas');
    // an event with no Spanish name keeps its English name but still gets Spanish labels
    const unknown = makeAlert({ event: 'Avalanche Warning', eventCode: null, pds: true });
    expect(alertPushText(unknown, 'es').title).toBe(
      'Aviso: Avalanche Warning – PELIGRO PARTICULARMENTE ALTO',
    );
  });

  it('every template event has a Spanish name, and no Spanish title carries an English tier word', () => {
    for (const key of Object.keys(TEMPLATES_BY_EVENT)) expect(EVENT_NAME_ES[key]).toBeTruthy();
    for (const [event, name] of Object.entries(EVENT_NAME_ES)) {
      for (const relaySeverity of ['emergency', 'warning'] as const) {
        const a = makeAlert({ event, eventCode: null, relaySeverity, pds: true });
        const { title } = alertPushText(a, 'es');
        expect(title, event).toContain(name);
        expect(title, event).not.toMatch(/\b(Warning|Watch|Advisory|Emergency|PDS|EMERGENCY)\b/);
      }
    }
  });

  it('without a template: Spanish title, body says official and English only', () => {
    const a = makeAlert({
      event: 'Avalanche Warning',
      eventCode: null,
      official: { ...tornado.official, headline: 'Avalanche Warning in effect until 6 PM' },
    });
    expect(summariesFor(a, { timeZone: 'America/Los_Angeles' })).toEqual([]);
    expect(alertPushText(a, 'es')).toEqual({
      title: 'Aviso: Avalanche Warning',
      body: 'Alerta oficial del NWS (solo en inglés): Avalanche Warning in effect until 6 PM',
    });
  });

  it('without a template and without a headline: cleaned English description', () => {
    const a = makeAlert({
      event: 'Avalanche Warning',
      eventCode: null,
      official: { ...tornado.official, headline: null, description: '* WHAT...Slides likely.' },
    });
    expect(alertPushText(a, 'es').body).toBe(
      'Alerta oficial del NWS (solo en inglés): Slides likely.',
    );
  });

  it('a Spanish alert from another source is not called NWS', () => {
    const a = makeAlert({ source: 'usgs', event: 'Avalanche Warning', eventCode: null });
    expect(alertPushText(a, 'es').body.startsWith('Alerta oficial (solo en inglés): ')).toBe(true);
  });

  it('an empty Spanish summary counts as no summary', () => {
    const a = makeAlert({ summaries: [{ language: 'es', whatToDo: 'x', text: '   ' }] });
    expect(alertPushText(a, 'es').body.startsWith('Alerta oficial del NWS')).toBe(true);
  });
});
