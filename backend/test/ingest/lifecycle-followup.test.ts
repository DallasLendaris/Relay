/**
 * Lifecycle and ingest pieces of the follow-up push (docs/cancellation.md): what a Cancel or
 * Update stores, and which follow-up the pipeline asks the planner for.
 */
import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeliveryQueue } from '../../src/adapters/memory/delivery-queue.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { noopLogger } from '../../src/delivery/index.js';
import { applyMessage } from '../../src/ingest/lifecycle.js';
import { createIngestPipeline } from '../../src/ingest/pipeline.js';
import { DeliveryPausedError, pollOnce, type PollDeps } from '../../src/ingest/poll.js';
import { MemoryPlanQueue } from '../../src/scale/fanout/queues.js';
import { makeAlert } from '../delivery/fakes.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

const NOW = new Date('2026-11-10T11:00:00Z');
const clock = { now: () => NOW };
const CELLS = ['872830828ffffff', '872830829ffffff'];

const original = (over: Partial<Alert> = {}) =>
  makeAlert({ sourceId: 'urn:oid:A', signature: null, pds: true, ...over }, CELLS);

const cancelOf = (over: Partial<Alert> = {}): Alert =>
  makeAlert(
    {
      id: '22222222-2222-4222-8222-222222222222',
      sourceId: 'urn:oid:C',
      messageType: 'Cancel',
      state: 'cancelled',
      sent: '2026-11-10T10:30:00Z',
      references: [{ sourceId: 'urn:oid:A', sent: '2026-11-10T10:00:00Z' }],
      event: 'Flash Flood Warning',
      relaySeverity: 'warning',
      official: {
        headline: 'The Flash Flood Warning has been cancelled.',
        description: 'The threat has ended.',
        instruction: null,
        senderName: 'NWS',
        language: 'en-US',
      },
      signature: null,
      ...over,
    },
    [],
  );

describe('lifecycle: Cancel and Update for follow-ups', () => {
  it('a Cancel keeps the area, severity and PDS flag of the version it cancels, and names it', async () => {
    const store = new InMemoryAlertStore();
    await applyMessage(original(), { store, clock });
    const r = await applyMessage(cancelOf({ relaySeverity: 'statement' }), { store, clock });
    expect(r.action).toBe('cancelled');
    expect(r.alert).toMatchObject({
      id: original().id,
      sourceId: 'urn:oid:C',
      state: 'cancelled',
      messageType: 'Cancel',
      relaySeverity: 'warning',
      pds: true,
      supersedes: { sourceId: 'urn:oid:A', relaySeverity: 'warning' },
    });
    expect(r.alert.area.h3Cells).toEqual(CELLS);
    expect(r.alert.official.headline).toBe('The Flash Flood Warning has been cancelled.');
  });

  it('an Update records the version it replaced', async () => {
    const store = new InMemoryAlertStore();
    await applyMessage(original({ relaySeverity: 'watch' }), { store, clock });
    const r = await applyMessage(
      makeAlert({
        sourceId: 'urn:oid:B',
        messageType: 'Update',
        sent: '2026-11-10T10:20:00Z',
        references: [{ sourceId: 'urn:oid:A', sent: '2026-11-10T10:00:00Z' }],
      }),
      { store, clock },
    );
    expect(r.action).toBe('updated');
    expect(r.alert.supersedes).toEqual({ sourceId: 'urn:oid:A', relaySeverity: 'watch' });
  });

  it('a Cancel replaces an end by feed reconciliation, but a second Cancel is ignored', async () => {
    const store = new InMemoryAlertStore();
    await applyMessage(original(), { store, clock });
    // What reconcile.cancel stores: same message, state cancelled.
    const ended = await store.findBySourceId('urn:oid:A');
    await store.save({ ...(ended as Alert), state: 'cancelled' });
    const r = await applyMessage(cancelOf(), { store, clock });
    expect(r.action).toBe('cancelled');
    expect(r.alert.messageType).toBe('Cancel');
    const again = await applyMessage(
      cancelOf({ sourceId: 'urn:oid:C2', sent: '2026-11-10T10:40:00Z' }),
      { store, clock },
    );
    expect(again).toMatchObject({ action: 'ignored', reason: 'alert already cancelled' });
  });

  it('an Update after a Cancel is ignored: the cancellation stays the last word', async () => {
    const store = new InMemoryAlertStore();
    await applyMessage(original(), { store, clock });
    await applyMessage(cancelOf(), { store, clock });
    const r = await applyMessage(
      makeAlert({
        sourceId: 'urn:oid:U',
        messageType: 'Update',
        sent: '2026-11-10T10:50:00Z',
        references: [{ sourceId: 'urn:oid:C', sent: '2026-11-10T10:30:00Z' }],
      }),
      { store, clock },
    );
    expect(r.action).toBe('ignored');
  });
});

function engineFor(opts: { paused?: boolean } = {}) {
  const alertStore = new InMemoryAlertStore();
  const planQueue = new MemoryPlanQueue();
  const engine = createIngestPipeline({
    alertStore,
    places: new MemoryPlaceStore(),
    devices: new MemoryDeviceStore(),
    queue: new MemoryDeliveryQueue(),
    alertSigning: TEST_SIGNING,
    tokenKeys: { current: { id: 't', secret: 's' }, all: new Map([['t', 's']]) },
    isDeliveryPaused: async () => opts.paused === true,
    fetch: (async () => new Response('{}', { status: 404 })) as typeof fetch,
    userAgent: 'relay-test',
    clock,
    logger: noopLogger,
    env: { FANOUT_MODE: 'queue', PLAN_QUEUE_URL: 'x' },
    planQueue,
  });
  return { engine, alertStore, planQueue };
}

describe('ingest pipeline: which follow-up it asks for', () => {
  it('a Cancel asks for the cancel follow-up of the stored (cancelled) version', async () => {
    const { engine, planQueue } = engineFor();
    await engine.onApplied?.(await engine.apply(original({ pds: undefined })));
    planQueue.messages.length = 0;
    await engine.onApplied?.(await engine.apply(cancelOf()));
    expect(planQueue.messages).toEqual([
      expect.objectContaining({ sourceId: 'urn:oid:C', followUp: 'cancel' }),
    ]);
  });

  it('an Update asks for its AlertReady and its update follow-up; a restored alert only the first', async () => {
    const { engine, planQueue } = engineFor();
    await engine.onApplied?.(await engine.apply(original({ pds: undefined })));
    planQueue.messages.length = 0;
    const update = makeAlert(
      {
        sourceId: 'urn:oid:B',
        messageType: 'Update',
        sent: '2026-11-10T10:20:00Z',
        references: [{ sourceId: 'urn:oid:A', sent: '2026-11-10T10:00:00Z' }],
      },
      CELLS,
    );
    const r = await engine.apply(update);
    await engine.onApplied?.(r);
    expect(planQueue.messages.map((m) => m.followUp ?? 'alert')).toEqual(['alert', 'update']);
    planQueue.messages.length = 0;
    await engine.onApplied?.({ ...r, restored: true });
    expect(planQueue.messages.map((m) => m.followUp ?? 'alert')).toEqual(['alert']);
  });

  it('an end by feed reconciliation asks for nothing', async () => {
    const { engine, alertStore, planQueue } = engineFor();
    await engine.onApplied?.(await engine.apply(original({ pds: undefined })));
    planQueue.messages.length = 0;
    const stored = (await alertStore.get(original().id)) as Alert;
    await engine.reconcile?.cancel(stored, 'no_longer_in_feed');
    expect(planQueue.messages).toEqual([]);
  });

  it('a paused kill switch keeps the Cancel unseen (DeliveryPausedError)', async () => {
    const { engine } = engineFor({ paused: true });
    const created = await engine.apply(original({ pds: undefined }));
    await expect(engine.onApplied?.(created)).rejects.toBeInstanceOf(DeliveryPausedError);
    await expect(engine.onApplied?.(await engine.apply(cancelOf()))).rejects.toBeInstanceOf(
      DeliveryPausedError,
    );
  });
});

describe('poll loop: a Cancel is applied without resolving its area, and replanned on replay', () => {
  it('a bare Cancel whose follow-up failed is planned again on the next poll, then marked seen', async () => {
    const store = new InMemoryAlertStore();
    await applyMessage(original({ pds: undefined }), { store, clock });
    const cancel = cancelOf();
    const body = JSON.stringify({ features: [{ cancel: true }] });
    const seen = new Set<string>();
    const cellsAsked: string[] = [];
    const applied: string[] = [];
    let failOnce = true;
    const deps: PollDeps = {
      client: {
        commit: () => undefined,
        fetchActiveCalifornia: async () => ({ status: 'ok', body, fetchedAt: NOW }),
      },
      archive: { put: async () => 'k' },
      seen: {
        has: async (id) => seen.has(id),
        mark: async (id) => void seen.add(id),
        clearEnded: async () => undefined,
      },
      normalize: () => ({ ok: true, alert: structuredClone(cancel) }),
      parseFeed: (b) => ({ features: (JSON.parse(b) as { features: unknown[] }).features }),
      cellsFor: async (area) => {
        cellsAsked.push(area.description);
        throw new Error('a Cancel area must not be resolved');
      },
      apply: (a) => applyMessage(a, { store, clock }),
      onApplied: (r) => {
        applied.push(r.action);
        if (failOnce) {
          failOnce = false;
          throw new Error('plan queue down');
        }
      },
      log: () => undefined,
    };
    const first = await pollOnce(deps);
    expect(first.errors).toBe(1);
    expect(seen.has('urn:oid:C')).toBe(false);
    const second = await pollOnce(deps);
    expect(second.errors).toBe(0);
    expect(applied).toEqual(['cancelled', 'cancelled']);
    expect(seen.has('urn:oid:C')).toBe(true);
    expect(cellsAsked).toEqual([]);
    // A third poll: already seen, nothing more.
    await pollOnce(deps);
    expect(applied).toHaveLength(2);
  });
});
