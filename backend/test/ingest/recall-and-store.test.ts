import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { SigningAlertStore } from '../../src/adapters/signing-alert-store.js';
import { noopLogger } from '../../src/delivery/index.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { verifyAlertSignature } from '../../src/signing/index.js';
import { parseArgs, recallAlert } from '../../scripts/alert-recall.js';
import { alert as dbAlert, freshDb } from '../db/helpers.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

const key = TEST_SIGNING.signer;
const keys = TEST_SIGNING.publicKeys;
const ID = '6f1c1d2e-3b4a-4c5d-8e9f-0a1b2c3d4e5f';

function active(over: Partial<Alert> = {}): Alert {
  return {
    ...dbAlert(),
    id: ID,
    source: 'nws',
    sourceId: 'urn:oid:R1',
    event: 'Flash Flood Warning',
    eventCode: 'FFW',
    state: 'active',
    sent: '2026-11-09T10:00:00-08:00',
    expires: '2099-01-01T00:00:00Z',
    ends: null,
    summaries: [],
    signature: null,
    ...over,
  };
}

const logs: Array<{ level: string; msg: string; fields: Record<string, unknown> }> = [];
const logger = {
  info: (msg: string, fields: Record<string, unknown>) => logs.push({ level: 'info', msg, fields }),
  warn: (msg: string, fields: Record<string, unknown>) => logs.push({ level: 'warn', msg, fields }),
  error: (msg: string, fields: Record<string, unknown>) =>
    logs.push({ level: 'error', msg, fields }),
};

describe('OPS-3: operator recall', () => {
  it('cancels, drops summaries, re-signs and republishes', async () => {
    const store = new SigningAlertStore(new InMemoryAlertStore(), key);
    await store.save(active());
    expect((await store.get(ID))?.summaries.length).toBeGreaterThan(0);
    const published: Alert[] = [];
    const publisher = { publish: async (a: Alert) => void published.push(a) };

    const res = await recallAlert({ store, publisher, logger }, ID, 'Issued for wrong county');
    expect(res).toMatchObject({ ok: true, previousState: 'active', republished: true });
    const saved = await store.get(ID);
    expect(saved).toMatchObject({ state: 'cancelled', summaries: [] });
    expect(verifyAlertSignature(saved as Alert, keys)).toEqual({ ok: true });
    expect(published).toHaveLength(1);
    expect(published[0]?.state).toBe('cancelled');
    expect(logs.find((l) => l.msg === 'alert.recalled')?.fields).toMatchObject({
      alertId: ID,
      reason: 'Issued for wrong county',
      previousState: 'active',
    });
    // recalling again is safe (repairs a failed publish)
    expect(await recallAlert({ store, publisher, logger }, ID, 'repeat publish')).toMatchObject({
      ok: true,
      previousState: 'cancelled',
    });
  });

  it('rejects a bad id, a missing alert and a missing reason, and --dry-run changes nothing', async () => {
    const store = new SigningAlertStore(new InMemoryAlertStore(), key);
    await store.save(active());
    expect(await recallAlert({ store, logger }, 'nope', 'a good reason')).toMatchObject({
      ok: false,
    });
    expect(await recallAlert({ store, logger }, ID, '  ')).toMatchObject({ ok: false });
    expect(
      await recallAlert({ store, logger }, '00000000-0000-4000-8000-000000000000', 'a good reason'),
    ).toMatchObject({ ok: false });
    const dry = await recallAlert({ store, logger }, ID, 'a good reason', { dryRun: true });
    expect(dry).toMatchObject({ ok: true, dryRun: true, republished: false });
    expect((await store.get(ID))?.state).toBe('active');
  });

  it('parses its arguments', () => {
    expect(parseArgs([ID, '--reason', 'oops', '--dry-run'])).toEqual({
      alertId: ID,
      reason: 'oops',
      dryRun: true,
    });
    expect(() => parseArgs([ID])).toThrow(/Usage/);
    expect(() => parseArgs([ID, '--reason', 'x', '--bogus'])).toThrow(/Unknown option/);
  });
});

describe('SAFE-4: a cancelled alert gets no summaries', () => {
  it('drops earlier summaries when the alert is saved as cancelled', async () => {
    const store = new SigningAlertStore(new InMemoryAlertStore(), key);
    await store.save(active());
    const before = await store.get(ID);
    expect(before?.summaries.some((s) => s.text.includes('in effect'))).toBe(true);
    await store.save({ ...(before as Alert), state: 'cancelled' });
    expect((await store.get(ID))?.summaries).toEqual([]);
  });
});

describe('listActiveBySource', () => {
  const seed = async (store: SigningAlertStore): Promise<void> => {
    await store.save(active());
    await store.save(
      active({ id: '00000000-0000-4000-8000-000000000002', sourceId: 'u1', source: 'usgs' }),
    );
    await store.save(
      active({ id: '00000000-0000-4000-8000-000000000003', sourceId: 'c1', state: 'cancelled' }),
    );
    await store.save(
      active({
        id: '00000000-0000-4000-8000-000000000004',
        sourceId: 'e1',
        expires: '2020-01-01T00:00:00Z',
      }),
    );
  };
  const now = new Date('2026-11-09T18:00:00Z');

  it('memory store returns only active, unended alerts of that source', async () => {
    const store = new SigningAlertStore(new InMemoryAlertStore(), key);
    await seed(store);
    expect((await store.listActiveBySource('nws', now)).map((a) => a.id)).toEqual([ID]);
  });

  it('postgres store does too, with cells', async () => {
    const store = new SigningAlertStore(new PostgresAlertStore(await freshDb()), key);
    await seed(store);
    const found = await store.listActiveBySource('nws', now);
    expect(found.map((a) => a.id)).toEqual([ID]);
    expect(found[0]?.area.h3Cells.length).toBeGreaterThan(0);
    expect(verifyAlertSignature(found[0] as Alert, keys)).toEqual({ ok: true });
  }, 60_000);
});

describe('pipeline wiring', () => {
  it('createIngestPipeline reconciles through the signing store and republishes', async () => {
    const inner = new InMemoryAlertStore();
    const engine = createIngestPipeline({
      alertStore: inner,
      places: new MemoryPlaceStore(),
      devices: new MemoryDeviceStore(),
      queue: new MemoryDeliveryQueue(),
      alertSigning: TEST_SIGNING,
      tokenKeys: { current: { id: 't', secret: 't' }, all: new Map([['t', 't']]) },
      isDeliveryPaused: async () => false,
      fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
      userAgent: 'relay-test',
      clock: { now: () => new Date('2026-11-09T18:00:00Z') },
      logger: noopLogger,
    });
    expect(engine.reconcile).toBeDefined();
    await new SigningAlertStore(inner, key).save(active());
    const listed = await engine.reconcile?.listActive(new Date('2026-11-09T18:00:00Z'));
    expect(listed?.map((a) => a.id)).toEqual([ID]);
    const first = listed?.[0] as Alert;
    await engine.reconcile?.cancel(first, 'no_longer_in_feed');
    const stored = await inner.get(ID);
    expect(stored).toMatchObject({ state: 'cancelled', summaries: [] });
    expect(verifyAlertSignature(stored as Alert, keys)).toEqual({ ok: true });
  });
});
