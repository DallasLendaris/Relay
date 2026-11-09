import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { describe, expect, it, vi } from 'vitest';
import { AlertSchema } from '@relay/shared';
import { normalizeFeature } from '../../src/ingest/normalize.js';
import { parseFeed } from '../../src/ingest/feed.js';

const opts = { newId: () => randomUUID() };

// Test-only loose type so fixtures can be edited without casts everywhere.
type Loose = Record<string, any>;
function features(file: string): unknown[] {
  const body = readFileSync(new URL(`../fixtures/nws/${file}`, import.meta.url), 'utf8');
  const feed = parseFeed(body);
  if ('error' in feed) throw new Error(feed.error);
  return feed.features;
}

/** Deep-copies the first feature of a fixture and lets the test change it. */
function tweak(mutate: (f: Loose) => void, file = 'active-ca-mixed.json'): unknown {
  const f = structuredClone(features(file)[0]) as Loose;
  mutate(f);
  return f;
}

function accept(feature: unknown) {
  const r = normalizeFeature(feature, opts);
  if (!r.ok) throw new Error(r.reason);
  return r.alert;
}

describe('accepted fixtures', () => {
  for (const file of ['active-ca-mixed.json', 'update-chain.json']) {
    it(`accepts every alert in ${file} and matches the schema`, () => {
      for (const f of features(file)) {
        const alert = accept(f);
        expect(() => AlertSchema.parse(alert)).not.toThrow();
        expect(alert.source).toBe('nws');
        expect(alert.sourceId.startsWith('urn:oid:')).toBe(true);
        expect(alert.area.h3Cells).toEqual([]);
        expect(alert.signature).toBeNull();
      }
    });
  }

  it('maps fields and keeps offset dates as given', () => {
    const alert = accept(features('active-ca-mixed.json')[0]);
    expect(alert.event).toBe('Flash Flood Warning');
    expect(alert.relaySeverity).toBe('warning');
    expect(alert.sent).toMatch(/-08:00$/);
    expect(alert.area.geometry?.type).toBe('Polygon');
    expect(alert.official.language).toBe('en-US');
  });

  it('uses null geometry when the feature has none', () => {
    const alert = accept(features('active-ca-mixed.json')[1]);
    expect(alert.area.geometry).toBeNull();
    expect(alert.area.ugcCodes.length).toBeGreaterThan(0);
  });

  it('takes the first SAME event code, else null', () => {
    const withCode = accept(tweak((f) => (f.properties.eventCode = { SAME: ['FFW', 'XXX'] })));
    expect(withCode.eventCode).toBe('FFW');
    const without = accept(tweak((f) => delete f.properties.eventCode));
    expect(without.eventCode).toBeNull();
  });
});

describe('sourceId normalization', () => {
  const bare = 'urn:oid:2.49.0.1.840.0.test.001.1';
  const url = `https://api.weather.gov/alerts/${bare}`;

  it('accepts the URL form, bare form, @id and feature.id', () => {
    const forms: Array<(f: Loose) => void> = [
      (f) => (f.properties.id = url),
      (f) => (f.properties.id = bare),
      (f) => {
        delete f.properties.id;
        f.properties['@id'] = url;
      },
      (f) => {
        delete f.properties.id;
        delete f.properties['@id'];
        f.id = url;
      },
    ];
    for (const form of forms) {
      expect(accept(tweak(form)).sourceId).toBe(bare);
    }
  });

  it('normalizes references from identifier or @id', () => {
    const alert = accept(
      tweak((f) => {
        f.properties.references = [
          { '@id': url, sent: '2026-11-09T06:10:00-08:00' },
          { identifier: 'urn:oid:other', sent: '2026-11-09T07:00:00-08:00' },
        ];
      }),
    );
    expect(alert.references).toEqual([
      { sourceId: bare, sent: '2026-11-09T06:10:00-08:00' },
      { sourceId: 'urn:oid:other', sent: '2026-11-09T07:00:00-08:00' },
    ]);
  });
});

describe('bad references', () => {
  it('drops a bad reference, keeps the alert and warns', () => {
    const warn = vi.fn();
    const r = normalizeFeature(
      tweak((f) => {
        f.properties.references = [
          { identifier: 'urn:oid:good', sent: '2026-11-09T07:00:00-08:00' },
          { identifier: 'urn:oid:no-date' },
          'garbage',
        ];
      }),
      { ...opts, warn },
    );
    expect(r.ok).toBe(true);
    if (!r.ok) return;
    expect(r.alert.references).toEqual([
      { sourceId: 'urn:oid:good', sent: '2026-11-09T07:00:00-08:00' },
    ]);
    expect(warn).toHaveBeenCalledTimes(2);
  });
});

describe('message type to state', () => {
  it('maps Alert, Update and Cancel', () => {
    const [original, update] = features('update-chain.json').map(accept);
    expect(original?.state).toBe('active');
    expect(update?.state).toBe('updated');
    expect(update?.references).toHaveLength(1);
    const cancel = accept(features('cancel.json')[0]);
    expect(cancel.state).toBe('cancelled');
    expect(cancel.messageType).toBe('Cancel');
  });

  it('rejects Ack and Error', () => {
    const r = normalizeFeature(
      tweak((f) => (f.properties.messageType = 'Ack')),
      opts,
    );
    expect(r.ok).toBe(false);
  });
});

describe('rejections', () => {
  it('rejects every non-Actual alert with a status reason', () => {
    const list = features('non-actual.json');
    expect(list).toHaveLength(3);
    const reasons = list.map((f) => {
      const r = normalizeFeature(f, opts);
      if (r.ok) throw new Error('should be rejected');
      expect(r.sourceId).toMatch(/^urn:oid:/);
      return r.reason;
    });
    expect(reasons).toEqual(['status Test', 'status Exercise', 'status System']);
  });

  it('rejects every malformed alert without throwing', () => {
    const list = features('malformed.json');
    expect(list).toHaveLength(4);
    for (const f of list) {
      const r = normalizeFeature(f, opts);
      expect(r.ok).toBe(false);
      if (!r.ok) expect(r.reason.length).toBeGreaterThan(0);
    }
  });

  it('rejects each missing required field', () => {
    for (const field of ['event', 'sent', 'effective', 'expires', 'status', 'messageType']) {
      const r = normalizeFeature(
        tweak((f) => delete f.properties[field]),
        opts,
      );
      expect(r.ok, field).toBe(false);
    }
    const noId = normalizeFeature(
      tweak((f) => {
        delete f.id;
        delete f.properties.id;
        delete f.properties['@id'];
      }),
      opts,
    );
    expect(noId).toMatchObject({ ok: false, reason: 'missing id', sourceId: null });
  });

  it('rejects values outside the CAP enums and bad dates', () => {
    for (const [field, value] of [
      ['severity', 'Catastrophic'],
      ['urgency', 'Soon'],
      ['certainty', 'Maybe'],
      ['expires', 'tomorrow'],
      ['sent', '2026-13-45T99:99:00-08:00'],
      ['onset', 'garbage'],
    ] as const) {
      const r = normalizeFeature(
        tweak((f) => (f.properties[field] = value)),
        opts,
      );
      expect(r.ok, field).toBe(false);
    }
  });

  it('never throws on junk input', () => {
    for (const junk of [null, undefined, 42, 'x', [], {}, { properties: 5 }, { properties: {} }]) {
      expect(normalizeFeature(junk, opts).ok).toBe(false);
    }
    const throwing = {
      newId: (): string => {
        throw new Error('boom');
      },
    };
    expect(normalizeFeature(features('active-ca-mixed.json')[0], throwing).ok).toBe(false);
  });
});
