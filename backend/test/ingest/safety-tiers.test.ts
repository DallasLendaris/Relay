import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { noopLogger } from '../../src/delivery/index.js';
import { PUSH_POLICY } from '../../src/delivery/policy.js';
import { normalizeFeature } from '../../src/ingest/normalize.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { summariesFor } from '../../src/summaries/index.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

type Loose = Record<string, any>;
const newId = (): string => '00000000-0000-4000-8000-000000000000';

const POLYGON = {
  type: 'Polygon',
  coordinates: [
    [
      [-119.82, 36.72],
      [-119.8, 36.72],
      [-119.8, 36.74],
      [-119.82, 36.74],
      [-119.82, 36.72],
    ],
  ],
};

function feature(over: Loose = {}): Loose {
  const { parameters, ...rest } = over;
  return {
    geometry: POLYGON,
    properties: {
      id: 'urn:oid:2.49.0.1.840.0.tor.001',
      status: 'Actual',
      messageType: 'Alert',
      event: 'Tornado Warning',
      severity: 'Extreme',
      urgency: 'Immediate',
      certainty: 'Observed',
      sent: '2026-11-09T10:00:00-08:00',
      effective: '2026-11-09T10:00:00-08:00',
      expires: '2026-11-09T10:45:00-08:00',
      areaDesc: 'Fresno',
      description: 'A tornado was seen.',
      headline: 'Tornado Warning issued November 9 at 10:00AM PST until 10:45AM PST by NWS Hanford',
      senderName: 'NWS Hanford CA',
      references: [],
      parameters,
      ...rest,
    },
  };
}

function norm(f: Loose) {
  const r = normalizeFeature(f, { newId });
  if (!r.ok) throw new Error(r.reason);
  return r.alert;
}

describe('SAFE-1: damage threat sets the emergency tier', () => {
  it('Tornado Warning CATASTROPHIC is an emergency, CONSIDERABLE is a PDS warning', () => {
    const emergency = norm(feature({ parameters: { tornadoDamageThreat: ['CATASTROPHIC'] } }));
    expect(emergency.relaySeverity).toBe('emergency');
    expect(emergency.pds).toBeUndefined();

    const pds = norm(feature({ parameters: { tornadoDamageThreat: ['CONSIDERABLE'] } }));
    expect(pds.relaySeverity).toBe('warning');
    expect(pds.pds).toBe(true);

    const base = norm(feature({ parameters: { tornadoDamageThreat: ['BASE'] } }));
    expect(base.relaySeverity).toBe('warning');
    expect(base.pds).toBeUndefined();
    expect('pds' in norm(feature())).toBe(false);
  });

  it('Flash Flood Warning uses flashFloodDamageThreat', () => {
    const ffw = (threat: string) =>
      norm(
        feature({
          parameters: { flashFloodDamageThreat: [threat] },
          event: 'Flash Flood Warning',
        }),
      );
    expect(ffw('CATASTROPHIC').relaySeverity).toBe('emergency');
    expect(ffw('CONSIDERABLE')).toMatchObject({ relaySeverity: 'warning', pds: true });
  });

  it('ignores the other event damage threat and a threat on a watch', () => {
    const wrongKey = norm(
      feature({
        event: 'Flash Flood Warning',
        parameters: { tornadoDamageThreat: ['CATASTROPHIC'] },
      }),
    );
    expect(wrongKey.relaySeverity).toBe('warning');
    const watch = norm(
      feature({ event: 'Tornado Watch', parameters: { tornadoDamageThreat: ['CATASTROPHIC'] } }),
    );
    expect(watch.relaySeverity).toBe('watch');
  });

  it.each([null, 'x', [], { tornadoDamageThreat: 'CATASTROPHIC' }, { tornadoDamageThreat: [7] }])(
    'a malformed parameters value (%j) leaves the tier alone',
    (parameters) => {
      expect(norm(feature({ parameters })).relaySeverity).toBe('warning');
    },
  );

  for (const [event, key] of [
    ['Tornado Warning', 'tornadoDamageThreat'],
    ['Flash Flood Warning', 'flashFloodDamageThreat'],
  ] as const) {
    it(`${event}: CONSIDERABLE -> CATASTROPHIC update re-notifies as an emergency`, async () => {
      const store = new InMemoryAlertStore();
      const published: Array<{ alertId: string; relaySeverity: string }> = [];
      const engine = createIngestPipeline({
        alertStore: store,
        places: new MemoryPlaceStore(),
        devices: new MemoryDeviceStore(),
        queue: new MemoryDeliveryQueue(),
        alertSigning: TEST_SIGNING,
        tokenKeys: { current: { id: 't1', secret: 't' }, all: new Map([['t1', 't']]) },
        isDeliveryPaused: async () => false,
        fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
        userAgent: 'relay-test',
        clock: { now: () => new Date('2026-11-09T18:20:00Z') },
        logger: noopLogger,
        env: { FANOUT_MODE: 'queue', PLAN_QUEUE_URL: 'unused' },
        planQueue: {
          publish: async (m: { alertId: string; relaySeverity: string }) => {
            published.push(m);
          },
        } as never,
      });
      const run = async (f: Loose): Promise<void> => {
        const n = engine.normalize(f);
        if (!n.ok) throw new Error(n.reason);
        n.alert.area.h3Cells = await engine.cellsFor(n.alert.area);
        const result = await engine.apply(n.alert);
        await engine.onApplied?.(result);
      };

      await run(
        feature({
          event,
          parameters: { [key]: ['CONSIDERABLE'] },
          id: 'urn:oid:chain.1',
        }),
      );
      await run(
        feature({
          event,
          messageType: 'Update',
          id: 'urn:oid:chain.2',
          sent: '2026-11-09T10:10:00-08:00',
          references: [{ identifier: 'urn:oid:chain.1', sent: '2026-11-09T10:00:00-08:00' }],
          parameters: { [key]: ['CATASTROPHIC'] },
        }),
      );

      expect(published.map((m) => m.relaySeverity)).toEqual(['warning', 'emergency']);
      // Same alert id, so the shard batch key `${alertId}:${relaySeverity}:...` changes: re-notify.
      expect(new Set(published.map((m) => m.alertId)).size).toBe(1);
      const stored = await store.get(published[0]?.alertId ?? '');
      expect(stored).toMatchObject({ relaySeverity: 'emergency', state: 'updated' });
      expect(stored?.pds).toBeUndefined();
      expect(PUSH_POLICY[stored?.relaySeverity ?? 'statement']).toMatchObject({
        priority: 'high',
        sound: 'default',
      });
    });
  }
});

describe('SAFE-11: practice and demo products are not actual', () => {
  it.each(['Practice Tornado Warning', 'Demo Warning', 'Tornado Warning Demonstration'])(
    'rejects %s even with status Actual',
    (event) => {
      const r = normalizeFeature(feature({ event }), { newId });
      expect(r.ok).toBe(false);
      if (!r.ok) expect(r.reason).toMatch(/^not_actual/);
    },
  );

  it('still accepts a normal event', () => {
    expect(normalizeFeature(feature(), { newId }).ok).toBe(true);
  });
});

describe('SAFE-6: an Immediate Severe statement is promoted to a quiet push', () => {
  const sps = (severity: string, urgency: string, event = 'Special Weather Statement') =>
    norm(feature({ event, severity, urgency })).relaySeverity;

  it('promotes Immediate + Severe/Extreme', () => {
    expect(sps('Severe', 'Immediate')).toBe('advisory');
    expect(sps('Extreme', 'Immediate')).toBe('advisory');
    expect(sps('Severe', 'Immediate', 'Air Quality Alert')).toBe('advisory');
  });

  it('leaves other statements informational', () => {
    expect(sps('Moderate', 'Immediate')).toBe('statement');
    expect(sps('Severe', 'Expected')).toBe('statement');
    expect(PUSH_POLICY[sps('Minor', 'Future')].push).toBe(false);
    expect(PUSH_POLICY[sps('Severe', 'Immediate')]).toEqual({
      push: true,
      priority: 'normal',
      sound: null,
    });
  });
});

describe('SAFE-2 end to end through normalize', () => {
  it('uses ends over expires and says starting for a future onset', () => {
    const a = norm(
      feature({
        event: 'Heat Advisory',
        severity: 'Minor',
        urgency: 'Expected',
        sent: '2026-11-09T10:00:00-08:00',
        onset: '2026-11-09T14:00:00-08:00',
        expires: '2026-11-09T12:00:00-08:00',
        ends: '2026-11-09T20:00:00-08:00',
      }),
    );
    const en = summariesFor(a, { timeZone: 'America/Los_Angeles' }).find(
      (s) => s.language === 'en',
    );
    expect(en?.text).toContain('starting 2 PM PT Monday');
    expect(en?.text).toContain('until 8 PM PT Monday');
    expect(en?.text).not.toContain('in effect');
  });
});
