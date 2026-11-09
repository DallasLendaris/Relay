import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryIdempotencyStore } from '../../src/adapters/memory/idempotency-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { SigningAlertStore } from '../../src/adapters/signing-alert-store.js';
import { noopLogger } from '../../src/delivery/index.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { verifyAlertSignature } from '../../src/signing/index.js';
import type { PlaceStore } from '../../src/ports.js';

const feed = JSON.parse(
  readFileSync(new URL('../fixtures/nws/active-ca-mixed.json', import.meta.url), 'utf8'),
) as { features: unknown[] };
const key = { id: 'a1', secret: 'alert-secret' };
const keys = new Map([[key.id, key.secret]]);
const ring = (id: string, secret: string) => ({
  current: { id, secret },
  all: new Map([[id, secret]]),
});

/** Run the first fixture feature through the engine's normalize + cellsFor + apply. */
async function applyFirst(places: PlaceStore, log = vi.fn()) {
  const store = new InMemoryAlertStore();
  const engine = createIngestPipeline({
    alertStore: store,
    places,
    devices: new MemoryDeviceStore(),
    idempotency: new MemoryIdempotencyStore(),
    queue: new MemoryDeliveryQueue(),
    alertKeys: ring('a1', 'alert-secret'),
    tokenKeys: ring('t1', 'token-secret'),
    isDeliveryPaused: async () => false,
    fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
    userAgent: 'relay-test',
    clock: { now: () => new Date('2026-11-09T15:00:00Z') },
    logger: { ...noopLogger, error: log },
  });
  const norm = engine.normalize(feed.features[0]);
  if (!norm.ok) throw new Error(norm.reason);
  norm.alert.area.h3Cells = await engine.cellsFor(norm.alert.area);
  const result = await engine.apply(norm.alert);
  return { engine, store, result };
}

describe('SigningAlertStore', () => {
  it('adds summaries, signs, and passes reads through', async () => {
    const { result } = await applyFirst(new MemoryPlaceStore());
    const inner = new InMemoryAlertStore();
    const store = new SigningAlertStore(inner, key);
    const alert: Alert = { ...result.alert, summaries: [], signature: null };
    await store.save(alert);

    const saved = await inner.get(alert.id);
    expect(saved?.summaries).toHaveLength(2);
    expect(saved && verifyAlertSignature(saved, keys)).toEqual({ ok: true });
    expect(await store.get(alert.id)).toEqual(saved);
    expect((await store.findBySourceId(alert.sourceId))?.id).toBe(alert.id);

    // The signature covers the summaries, so tampering with them is detected.
    const tampered = { ...saved, summaries: [] } as Alert;
    expect(verifyAlertSignature(tampered, keys).ok).toBe(false);
  });
});

describe('createIngestPipeline', () => {
  it('stores a signed alert with H3 cells', async () => {
    const { store, result } = await applyFirst(new MemoryPlaceStore());
    expect(result.action).toBe('created');
    const saved = await store.get(result.alert.id);
    expect(saved?.area.h3Cells.length).toBeGreaterThan(0);
    expect(saved && verifyAlertSignature(saved, keys)).toEqual({ ok: true });
  });

  it('logs and swallows a delivery error instead of throwing out of the poll', async () => {
    const log = vi.fn();
    const failing: PlaceStore = {
      add: async () => {},
      listForDevice: async () => [],
      remove: async () => false,
      listMatchesForCells: async () => {
        throw new Error('db down');
      },
      listForCells: async () => {
        throw new Error('db down');
      },
    };
    const { engine, result } = await applyFirst(failing, log);
    await expect(engine.onApplied?.(result)).resolves.toBeUndefined();
    expect(log).toHaveBeenCalledWith(
      'delivery planning failed',
      expect.objectContaining({ error: expect.stringContaining('db down') as string }),
    );
  });

  it('does not plan delivery for ignored results', async () => {
    const listForCells = vi.fn(async () => []);
    const places = { ...new MemoryPlaceStore(), listForCells } as unknown as PlaceStore;
    const { engine, result } = await applyFirst(places);
    listForCells.mockClear();
    await engine.onApplied?.({ ...result, action: 'ignored' });
    expect(listForCells).not.toHaveBeenCalled();
  });
});
