import { beforeEach, describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import { SigningAlertStore } from '../../src/adapters/signing-alert-store.js';
import type { NwsClient, NwsFetchResult } from '../../src/ingest/nws-client.js';
import {
  RECONCILE_AFTER,
  clearReconcileState,
  clearSaveFailures,
  expiredReferencesOf,
  pollOnce,
  type NormalizeResult,
  type PollDeps,
  type Reconciler,
} from '../../src/ingest/poll.js';
import type { SeenStore } from '../../src/ports.js';
import { verifyAlertSignature } from '../../src/signing/index.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

const NOW = new Date('2026-11-09T18:30:00Z');
const key = TEST_SIGNING.signer;

function alert(over: Partial<Alert> = {}): Alert {
  return {
    id: '6f1c1d2e-3b4a-4c5d-8e9f-0a1b2c3d4e5f',
    source: 'nws',
    sourceId: 'urn:oid:A1',
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
    area: { description: 'Fresno', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: ['c1'] },
    sent: '2026-11-09T10:00:00-08:00',
    effective: '2026-11-09T10:00:00-08:00',
    onset: null,
    expires: '2026-11-09T14:00:00-08:00', // 22:00Z, after NOW
    ends: null,
    official: {
      headline: 'h',
      description: 'd',
      instruction: null,
      senderName: 'NWS Hanford CA',
      language: 'en',
    },
    summaries: [],
    signature: null,
    ...over,
  };
}

const feature = (id: string, refs: string[] = []): unknown => ({
  properties: { id: `https://api.weather.gov/alerts/${id}`, refs },
});

/** Reads the id (and chain) from the fake feature. */
function normalize(f: unknown): NormalizeResult {
  const p = (f as { properties: { id: string; refs: string[] } }).properties;
  const sourceId = p.id.replace('https://api.weather.gov/alerts/', '');
  return {
    ok: true,
    alert: alert({
      sourceId,
      id: '00000000-0000-4000-8000-0000000000aa',
      references: p.refs.map((r) => ({ sourceId: r, sent: '2026-11-09T10:00:00-08:00' })),
    }),
  };
}

class Seen implements SeenStore {
  ids = new Set<string>();
  async has(id: string) {
    return this.ids.has(id);
  }
  async mark(id: string) {
    this.ids.add(id);
  }
}

type Step = NwsFetchResult | { status: 'ok'; features: unknown[]; extra?: Record<string, unknown> };

function setup(steps: Step[], opts: { listFails?: boolean; saveFails?: boolean } = {}) {
  const inner = new InMemoryAlertStore();
  const store = new SigningAlertStore(inner, key);
  const published: Alert[] = [];
  let i = 0;
  const client: NwsClient & { commits: number } = {
    commits: 0,
    commit() {
      this.commits += 1;
    },
    async fetchActiveCalifornia() {
      const step = steps[Math.min(i++, steps.length - 1)] as Step;
      if (step.status === 'ok' && 'features' in step) {
        return {
          status: 'ok',
          body: JSON.stringify({
            type: 'FeatureCollection',
            features: step.features,
            ...step.extra,
          }),
          fetchedAt: NOW,
        };
      }
      return step as NwsFetchResult;
    },
  };
  const reconcile: Reconciler = {
    now: () => NOW,
    async listActive(now) {
      if (opts.listFails) throw new Error('db down');
      return inner.listActiveBySource('nws', now);
    },
    async cancel(a) {
      if (opts.saveFails) throw new Error('save failed');
      await store.save({ ...a, state: 'cancelled' });
      const saved = await store.get(a.id);
      if (saved) published.push(saved);
    },
  };
  const deps: PollDeps = {
    client,
    archive: new MemoryRawArchive(),
    seen: new Seen(),
    normalize,
    parseFeed: (body) => ({ features: (JSON.parse(body) as { features: unknown[] }).features }),
    apply: async (a) => ({ action: 'ignored', alert: a, reason: 'test' }),
    cellsFor: async () => [],
    log: () => {},
    reconcile,
  };
  return { deps, inner, store, published, client };
}

beforeEach(() => {
  clearReconcileState();
  clearSaveFailures();
});

describe('SAFE-12: feed reconciliation', () => {
  it('ends a stored alert that stays out of the feed for 2 consecutive 200 polls', async () => {
    const t = setup([
      { status: 'ok', features: [feature('urn:oid:OTHER')] },
      { status: 'ok', features: [feature('urn:oid:OTHER')] },
    ]);
    await t.store.save(alert());

    await pollOnce(t.deps);
    expect((await t.inner.get(alert().id))?.state).toBe('active'); // one miss only counts
    expect(t.published).toHaveLength(0);
    expect(t.client.commits).toBe(0); // pending: the next poll must be a full 200, not a 304

    await pollOnce(t.deps);
    const stored = await t.inner.get(alert().id);
    expect(stored?.state).toBe('cancelled');
    expect(stored?.summaries).toEqual([]);
    expect(stored?.signature).not.toBeNull();
    expect(verifyAlertSignature(stored as Alert, TEST_SIGNING.publicKeys)).toEqual({
      ok: true,
    });
    expect(t.published.map((p) => p.id)).toEqual([alert().id]); // republished
    expect(RECONCILE_AFTER).toBe(2);
  });

  it('a reappearing alert resets the count (no flapping)', async () => {
    const t = setup([
      { status: 'ok', features: [feature('urn:oid:OTHER')] },
      { status: 'ok', features: [feature('urn:oid:A1')] },
      { status: 'ok', features: [feature('urn:oid:OTHER')] },
    ]);
    await t.store.save(alert());
    await pollOnce(t.deps);
    await pollOnce(t.deps);
    await pollOnce(t.deps);
    expect((await t.inner.get(alert().id))?.state).toBe('active');
  });

  it('a chain id counts as present (the feed lists the update, not the original)', async () => {
    const t = setup([{ status: 'ok', features: [feature('urn:oid:A2', ['urn:oid:A1'])] }]);
    await t.store.save(alert());
    await pollOnce(t.deps);
    await pollOnce(t.deps);
    await pollOnce(t.deps);
    expect((await t.inner.get(alert().id))?.state).toBe('active');
    // and the stored side of the chain
    const t2 = setup([{ status: 'ok', features: [feature('urn:oid:A1')] }]);
    await t2.store.save(
      alert({ sourceId: 'urn:oid:A2', references: [{ sourceId: 'urn:oid:A1', sent: 'x' }] }),
    );
    await pollOnce(t2.deps);
    await pollOnce(t2.deps);
    expect((await t2.inner.get(alert().id))?.state).toBe('active');
  });

  it('never acts on a 304 or an error, and a 304 does not count as a miss', async () => {
    const t = setup([
      { status: 'ok', features: [feature('urn:oid:OTHER')] },
      { status: 'not_modified' },
      { status: 'error', httpStatus: 503, message: 'down' },
      { status: 'not_modified' },
    ]);
    await t.store.save(alert());
    for (let n = 0; n < 4; n++) await pollOnce(t.deps);
    expect((await t.inner.get(alert().id))?.state).toBe('active');
    expect(t.published).toHaveLength(0);
  });

  it('leaves alerts that already ended, other sources and cancelled ones alone', async () => {
    const t = setup([{ status: 'ok', features: [] }]);
    await t.store.save(
      alert({ id: '00000000-0000-4000-8000-000000000001', expires: '2026-11-09T08:00:00-08:00' }),
    ); // 16:00Z, over
    await t.store.save(
      alert({ id: '00000000-0000-4000-8000-000000000002', source: 'usgs', sourceId: 'u1' }),
    );
    await t.store.save(
      alert({ id: '00000000-0000-4000-8000-000000000003', sourceId: 'x', state: 'cancelled' }),
    );
    await pollOnce(t.deps);
    await pollOnce(t.deps);
    expect(t.published).toHaveLength(0);
    expect((await t.inner.get('00000000-0000-4000-8000-000000000002'))?.state).toBe('active');
  });

  it('an alert whose end is later through `ends` still counts as not ended', async () => {
    const t = setup([{ status: 'ok', features: [] }]);
    await t.store.save(
      alert({ expires: '2026-11-09T08:00:00-08:00', ends: '2026-11-09T14:00:00-08:00' }),
    );
    await pollOnce(t.deps);
    await pollOnce(t.deps);
    expect((await t.inner.get(alert().id))?.state).toBe('cancelled');
  });

  it('expiredReferences end an alert at once', async () => {
    const t = setup([
      {
        status: 'ok',
        features: [feature('urn:oid:A1')], // still listed, but NWS says it expired
        extra: { expiredReferences: [{ identifier: 'urn:oid:A1' }] },
      },
    ]);
    await t.store.save(alert());
    await pollOnce(t.deps);
    expect((await t.inner.get(alert().id))?.state).toBe('cancelled');
    expect([
      ...expiredReferencesOf(
        JSON.stringify({
          expiredReferences: ['https://api.weather.gov/alerts/urn:oid:B', 7, { x: 1 }],
        }),
      ),
    ]).toEqual(['urn:oid:B']);
    expect(expiredReferencesOf('not json expiredReferences').size).toBe(0);
  });

  it('is skipped when a feature could not be read, and failures keep the ETag uncommitted', async () => {
    const t = setup([{ status: 'ok', features: [feature('urn:oid:OTHER')] }], { saveFails: true });
    t.deps.normalize = (f) => {
      if ((f as { properties: { id: string } }).properties.id.endsWith('BAD')) {
        throw new Error('boom');
      }
      return normalize(f);
    };
    await t.store.save(alert());
    // unreadable feature: nothing is reconciled
    const bad = setup([{ status: 'ok', features: [feature('urn:oid:BAD')] }]);
    bad.deps.normalize = t.deps.normalize;
    await bad.store.save(alert());
    await pollOnce(bad.deps);
    await pollOnce(bad.deps);
    expect((await bad.inner.get(alert().id))?.state).toBe('active');

    // a failing cancel is an error and is retried on the next poll
    await pollOnce(t.deps);
    const counts = await pollOnce(t.deps);
    expect(counts.errors).toBe(1);
    expect(t.client.commits).toBe(0);
    expect((await t.inner.get(alert().id))?.state).toBe('active');
  });

  it('a failing store read is an error, not a crash', async () => {
    const t = setup([{ status: 'ok', features: [] }], { listFails: true });
    const counts = await pollOnce(t.deps);
    expect(counts.errors).toBe(1);
  });
});
