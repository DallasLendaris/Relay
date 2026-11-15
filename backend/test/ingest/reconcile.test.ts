import { beforeEach, describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import { SigningAlertStore } from '../../src/adapters/signing-alert-store.js';
import type { NwsClient, NwsFetchResult } from '../../src/ingest/nws-client.js';
import {
  RECONCILE_AFTER,
  RECONCILE_MIN_ABSENT_MS,
  clearReconcileState,
  clearSaveFailures,
  pollOnce,
  type NormalizeResult,
  type ApplyResult,
  type PollDeps,
  type Reconciler,
} from '../../src/ingest/poll.js';
import { InMemorySeenStore } from '../../src/adapters/memory/seen-store.js';
import { verifyAlertSignature } from '../../src/signing/index.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

const T0 = new Date('2026-11-09T18:30:00Z');
const MIN = 60_000;
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

type Step = NwsFetchResult | { status: 'ok'; features: unknown[]; extra?: Record<string, unknown> };

function setup(steps: Step[], opts: { listFails?: boolean; saveFails?: boolean } = {}) {
  let nowMs = T0.getTime();
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
          fetchedAt: new Date(nowMs),
        };
      }
      return step as NwsFetchResult;
    },
  };
  const seen = new InMemorySeenStore();
  const reconcile: Reconciler = {
    now: () => new Date(nowMs),
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
    async listEnded(now) {
      return inner.listCancelledBySource('nws', now);
    },
    async restore(a, state) {
      await store.save({ ...a, state });
      const saved = (await store.get(a.id)) as Alert;
      published.push(saved);
      return saved;
    },
  };
  const applied: ApplyResult[] = [];
  const deps: PollDeps = {
    client,
    archive: new MemoryRawArchive(),
    seen,
    normalize,
    parseFeed: (body) => ({ features: (JSON.parse(body) as { features: unknown[] }).features }),
    apply: async (a) => ({
      action: 'ignored',
      alert: { ...a, state: 'cancelled' },
      reason: 'test',
    }),
    cellsFor: async () => [],
    log: () => {},
    reconcile,
    onApplied: (r) => {
      applied.push(r);
    },
  };
  return {
    deps,
    inner,
    store,
    published,
    client,
    seen,
    applied,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

beforeEach(() => {
  clearReconcileState();
  clearSaveFailures();
});

type T = ReturnType<typeof setup>;

/** n polls, 30 s apart (the poll interval). */
async function poll(t: T, n: number, stepMs = 30_000): Promise<void> {
  for (let i = 0; i < n; i++) {
    await pollOnce(t.deps);
    t.advance(stepMs);
  }
}

const OTHER: Step = { status: 'ok', features: [feature('urn:oid:OTHER')] };
const state = async (t: T, id = alert().id) => (await t.inner.get(id))?.state;

describe('SAFE-12: feed reconciliation', () => {
  it('needs 3 consecutive full reads AND 5 minutes of absence before ending an alert', async () => {
    const t = setup([OTHER]);
    await t.store.save(alert());

    await poll(t, 2);
    expect(await state(t)).toBe('active'); // the old 2-poll rule would have ended it by now
    expect(t.client.commits).toBe(0); // pending: the next poll must be a full 200, not a 304

    // many more reads, but under 5 minutes since the first miss: still active
    await poll(t, 8); // 10 polls = 4.5 minutes since the first miss
    expect(await state(t)).toBe('active');
    expect(t.published).toHaveLength(0);

    await poll(t, 1); // 5 minutes after the first miss
    const stored = await t.inner.get(alert().id);
    expect(stored?.state).toBe('cancelled');
    expect(stored?.summaries).toEqual([]);
    expect(verifyAlertSignature(stored as Alert, TEST_SIGNING.publicKeys)).toEqual({ ok: true });
    expect(t.published.map((p) => p.id)).toEqual([alert().id]); // republished
    expect((await t.seen.getEnded(alert().id))?.endedBy).toBe('feed_absence');
    expect(RECONCILE_AFTER).toBe(3);
    expect(RECONCILE_MIN_ABSENT_MS).toBe(5 * MIN);
  });

  it('the count also matters: 3 reads must pass even if 5 minutes already have', async () => {
    const t = setup([OTHER]);
    await t.store.save(alert());
    await poll(t, 1, 10 * MIN); // first miss, then ten minutes pass
    await poll(t, 1, 10 * MIN); // second miss: time is enough, count is not
    expect(await state(t)).toBe('active');
    await poll(t, 1); // third miss
    expect(await state(t)).toBe('cancelled');
  });

  it('304s and errors in between do not count as reads', async () => {
    const t = setup([
      OTHER,
      { status: 'not_modified' },
      { status: 'error', httpStatus: 503, message: 'down' },
      { status: 'not_modified' },
      OTHER,
      OTHER,
    ]);
    await t.store.save(alert());
    await poll(t, 1, 6 * MIN);
    await poll(t, 3, 6 * MIN); // 304, error, 304
    expect(await state(t)).toBe('active');
    await poll(t, 1, 6 * MIN); // second full read
    expect(await state(t)).toBe('active');
    await poll(t, 1, 6 * MIN); // third full read, long past 5 minutes
    expect(await state(t)).toBe('cancelled');
  });

  it('a reappearing alert resets the count (no flapping)', async () => {
    const t = setup([
      OTHER,
      OTHER,
      { status: 'ok', features: [feature('urn:oid:A1')] },
      OTHER,
      OTHER,
      OTHER,
    ]);
    await t.store.save(alert());
    await poll(t, 3, 4 * MIN); // miss, miss, back
    await poll(t, 3, 1 * MIN); // miss x3 but only 2 minutes since the new first miss
    expect(await state(t)).toBe('active');
  });

  it('a chain id counts as present (the feed lists the update, not the original)', async () => {
    const t = setup([{ status: 'ok', features: [feature('urn:oid:A2', ['urn:oid:A1'])] }]);
    await t.store.save(alert());
    await poll(t, 5, 3 * MIN);
    expect(await state(t)).toBe('active');
    // and the stored side of the chain
    const t2 = setup([{ status: 'ok', features: [feature('urn:oid:A1')] }]);
    await t2.store.save(
      alert({ sourceId: 'urn:oid:A2', references: [{ sourceId: 'urn:oid:A1', sent: 'x' }] }),
    );
    await poll(t2, 5, 3 * MIN);
    expect(await state(t2)).toBe('active');
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
    await poll(t, 5, 3 * MIN);
    expect(t.published).toHaveLength(0);
    expect(await state(t, '00000000-0000-4000-8000-000000000002')).toBe('active');
  });

  it('an alert whose end is later through `ends` still counts as not ended', async () => {
    const t = setup([{ status: 'ok', features: [] }]);
    await t.store.save(
      alert({ expires: '2026-11-09T08:00:00-08:00', ends: '2026-11-09T14:00:00-08:00' }),
    );
    await poll(t, 4, 3 * MIN);
    expect(await state(t)).toBe('cancelled');
  });

  it('expiredReferences end an alert at once, and it is never restored', async () => {
    const t = setup([
      {
        status: 'ok',
        // still listed, but NWS says it expired
        features: [
          {
            properties: {
              id: 'https://api.weather.gov/alerts/urn:oid:A1',
              refs: [],
              parameters: { expiredReferences: ['w-nws.webmaster@noaa.gov,urn:oid:A1,x'] },
            },
          },
        ],
      },
    ]);
    await t.store.save(alert());
    await poll(t, 1);
    expect(await state(t)).toBe('cancelled');
    expect(await t.seen.getEnded(alert().id)).toBeNull(); // not a feed-absence ending
    await poll(t, 3);
    expect(await state(t)).toBe('cancelled');
  });

  it('is skipped when a feature could not be read, and failures keep the ETag uncommitted', async () => {
    const t = setup([OTHER], { saveFails: true });
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
    await poll(bad, 6, 3 * MIN);
    expect(await state(bad)).toBe('active');

    // a failing cancel is an error and is retried on the next poll; the record is not left behind
    await poll(t, 2, 3 * MIN);
    const counts = await pollOnce(t.deps);
    expect(counts.errors).toBe(1);
    expect(t.client.commits).toBe(0);
    expect(await state(t)).toBe('active');
    expect(await t.seen.getEnded(alert().id)).toBeNull();
  });

  it('a failing store read is an error, not a crash', async () => {
    const t = setup([{ status: 'ok', features: [] }], { listFails: true });
    const counts = await pollOnce(t.deps);
    expect(counts.errors).toBe(1);
  });
});

describe('feed reconciliation: restore an alert that comes back', () => {
  const A1: Step = { status: 'ok', features: [feature('urn:oid:A1')] };

  /** Three absent reads, 3 minutes apart: the alert is ended. Push more steps to continue. */
  async function endedByAbsence(): Promise<{ t: T; steps: Step[] }> {
    const steps: Step[] = [OTHER, OTHER, OTHER];
    const t = setup(steps);
    await t.store.save(alert());
    await poll(t, 3, 3 * MIN);
    expect(await state(t)).toBe('cancelled');
    return { t, steps };
  }

  it('restores it (re-signed, saved, republished, planned again) when it reappears', async () => {
    const { t, steps } = await endedByAbsence();
    const before = t.published.length;
    steps.push(OTHER, A1);
    await poll(t, 1); // the 4th read is still absent: stays ended
    expect(await state(t)).toBe('cancelled');
    await poll(t, 1); // the feed lists A1 again
    const stored = await t.inner.get(alert().id);
    expect(stored?.state).toBe('active');
    expect(stored?.summaries.length).toBeGreaterThan(0);
    expect(verifyAlertSignature(stored as Alert, TEST_SIGNING.publicKeys)).toEqual({ ok: true });
    expect(t.published.length).toBe(before + 1); // republished
    expect(t.applied.map((r) => r.action)).toEqual(['created']);
    expect(t.applied[0]?.alert.id).toBe(alert().id);
    expect(await t.seen.getEnded(alert().id)).toBeNull();
    // and it stays active (and is not restored again) while the feed keeps listing it
    await poll(t, 3, 3 * MIN);
    expect(await state(t)).toBe('active');
    expect(t.applied).toHaveLength(1);
  });

  it('an updated alert is restored as updated', async () => {
    const steps: Step[] = [OTHER, OTHER, OTHER];
    const t = setup(steps);
    await t.store.save(alert({ state: 'updated' }));
    await poll(t, 3, 3 * MIN);
    expect(await state(t)).toBe('cancelled');
    steps.push(A1);
    await poll(t, 1);
    expect(await state(t)).toBe('updated');
    expect(t.applied.map((r) => r.action)).toEqual(['updated']);
  });

  it('an alert that expired in the meantime is not restored', async () => {
    const { t, steps } = await endedByAbsence();
    steps.push(A1);
    t.advance(24 * 60 * MIN);
    await poll(t, 1); // A1 is back, but its end has passed
    expect(await state(t)).toBe('cancelled');
    expect(t.applied).toHaveLength(0);
  });

  it('a real CAP Cancel is never undone, even if the original is still listed', async () => {
    const t = setup([A1]);
    // cancelled by a real Cancel message: no feed-absence marker exists
    await t.store.save(alert({ state: 'cancelled' }));
    await poll(t, 4, 3 * MIN);
    expect(await state(t)).toBe('cancelled');
    expect(t.applied).toHaveLength(0);
  });

  it('a Cancel message in the feed blocks restoration', async () => {
    const { t, steps } = await endedByAbsence();
    steps.push({
      status: 'ok',
      features: [feature('urn:oid:A1'), feature('urn:oid:CANCEL', ['urn:oid:A1'])],
    });
    const plain = t.deps.normalize;
    t.deps.normalize = (f) => {
      const n = plain(f);
      if (n.ok && n.alert.sourceId === 'urn:oid:CANCEL') n.alert.messageType = 'Cancel';
      return n;
    };
    await poll(t, 3, 3 * MIN);
    expect(await state(t)).toBe('cancelled');
    // The real Cancel is applied on top of the feed-absence end (it is pushed, quietly, to the
    // devices that were sent the alert: docs/cancellation.md), and the alert is never restored.
    expect(t.applied.map((r) => r.action)).toEqual(['cancelled']);
    expect(t.applied[0]?.alert.messageType).toBe('Cancel');
  });

  it('a failed re-plan puts it back to ended and retries on the next full read', async () => {
    const { t, steps } = await endedByAbsence();
    steps.push(A1);
    const ok = t.deps.onApplied;
    t.deps.onApplied = () => {
      throw new Error('planner down');
    };
    const commitsBefore = t.client.commits;
    const counts = await pollOnce(t.deps); // A1 is back, onApplied throws
    expect(counts.errors).toBeGreaterThan(0);
    expect(t.client.commits).toBe(commitsBefore);
    expect(await state(t)).toBe('cancelled');
    expect((await t.seen.getEnded(alert().id))?.endedBy).toBe('feed_absence');
    t.deps.onApplied = ok;
    await poll(t, 1);
    expect(await state(t)).toBe('active');
    expect(t.applied).toHaveLength(1);
  });
});
