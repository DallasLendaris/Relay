/**
 * The sender's side of follow-ups (docs/cancellation.md): the key says it is a follow-up, the
 * payload is quiet for a cancel, a stale version is dropped, and every sent key records its
 * version.
 */
import { PublicAlertDocumentSchema, PushDataSchema, type Alert } from '@relay/shared';
import { cellToParent, gridDisk, latLngToCell } from 'h3-js';
import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { SigningAlertStore } from '../../../src/adapters/signing-alert-store.js';
import { followUpBatchKey, versionTag } from '../../../src/delivery/follow-up.js';
import { applyMessage } from '../../../src/ingest/lifecycle.js';
import {
  AlertPublisher,
  buildPublicDocument,
  publicAlertDocKey,
  publicAlertPointerKey,
  type ActiveIndexFile,
} from '../../../src/scale/publish/index.js';
import { processBatch, sentDetail } from '../../../src/scale/sender/sender.js';
import { makeAlert, signed } from '../../delivery/fakes.js';
import { TEST_SIGNING } from '../../signing/test-signing.js';
import { makeBatch, makeDeps, silent } from './fakes.js';

const cancelledAlert = () =>
  signed({
    sourceId: 'urn:oid:cancel',
    messageType: 'Cancel',
    state: 'cancelled',
    supersedes: { sourceId: 'urn:oid:1', relaySeverity: 'warning' },
  });

describe('sender: follow-up batches', () => {
  it('a cancel follow-up of a cancelled alert is sent quietly, with followUp in the data', async () => {
    const t = makeDeps({ publicAlertBaseUrl: 'https://cdn.example.test' });
    const alert = cancelledAlert();
    await t.alerts.save(alert);
    const batch = makeBatch({
      alertId: alert.id,
      sourceId: alert.sourceId,
      batchKey: followUpBatchKey(alert, 'cancel', 'cell1', 7),
    });
    const out = await processBatch(batch, t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'sent', sent: 3 });
    for (const m of t.push.calls.flat()) {
      expect(m).toMatchObject({ priority: 'normal', sound: false, collapseKey: alert.id });
      expect(m.data['followUp']).toBe('cancel');
      expect(PushDataSchema.parse(m.data).followUp).toBe('cancel');
      // The alert URL names the cancelled version's document.
      expect(m.data['alertUrl']).toBe(`https://cdn.example.test/${publicAlertDocKey(alert)}`);
    }
    const titles = t.push.calls.flat().map((m) => m.title);
    expect(titles[0]).toBe('Cancelled: Flash Flood Warning');
    expect(titles[1]).toMatch(/^Cancelado: /); // recipient 1 reads Spanish
    // The key records which version it carried.
    expect((await t.idempotency.inspect(batch.batchKey)).version).toBe(versionTag(alert.sourceId));
  });

  it('drops (acks) a follow-up whose version is no longer current', async () => {
    const t = makeDeps();
    const update = signed({
      sourceId: 'urn:oid:update',
      state: 'updated',
      supersedes: { sourceId: 'urn:oid:1', relaySeverity: 'warning' },
    });
    const batch = makeBatch({
      alertId: update.id,
      sourceId: update.sourceId,
      batchKey: `${followUpBatchKey(update, 'update', 'cell1', 7)}:r1`,
    });
    await t.alerts.save(cancelledAlert()); // a Cancel landed after the update was planned
    const out = await processBatch(batch, t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'stale_version', sent: 0 });
    expect(t.push.calls).toEqual([]);
  });

  it('an original batch of a cancelled alert is refused (remaining fan-out stops)', async () => {
    const t = makeDeps();
    const alert = cancelledAlert();
    await t.alerts.save(alert);
    const out = await processBatch(makeBatch({ alertId: alert.id }), t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'inactive' });
  });

  it('an update follow-up keeps the new severity loud', async () => {
    const t = makeDeps();
    const update = signed({
      sourceId: 'urn:oid:update',
      state: 'updated',
      supersedes: { sourceId: 'urn:oid:1', relaySeverity: 'warning' },
    });
    await t.alerts.save(update);
    await processBatch(
      makeBatch({
        alertId: update.id,
        sourceId: update.sourceId,
        batchKey: followUpBatchKey(update, 'update', 'cell1', 7),
      }),
      t.deps,
    );
    expect(t.push.calls.flat()[0]).toMatchObject({ priority: 'high', sound: true });
    expect(t.push.calls.flat()[0]?.title).toBe('Updated: Flash Flood Warning');
  });

  it('tags a sent key with the version whose text went out, not the one it was planned for', async () => {
    // An original batch planned for v1 is sent after a same-severity Update v2 committed: the
    // phone got v2's text, so v2's follow-up must see the key as current (no second push).
    const t = makeDeps();
    const v2 = signed({
      sourceId: 'urn:oid:update',
      state: 'updated',
      supersedes: { sourceId: 'urn:oid:1', relaySeverity: 'warning' },
    });
    await t.alerts.save(v2);
    const batch = makeBatch({ alertId: v2.id, sourceId: 'urn:oid:1' });
    const out = await processBatch(batch, t.deps);
    expect(out).toMatchObject({ ack: true, reason: 'sent' });
    expect(t.push.calls.flat()[0]?.title).not.toMatch(/^Updated/);
    expect((await t.idempotency.inspect(batch.batchKey)).version).toBe(versionTag(v2.sourceId));
  });

  it('sentDetail tags every key with its version once the batch knows its sourceId', () => {
    const batch = { batchKey: 'k', attempt: 0, recipients: [], sourceId: 'urn:oid:9' };
    expect(sentDetail(batch, silent)?.version).toBe(versionTag('urn:oid:9'));
    expect(sentDetail({ ...batch, attempt: 2 }, silent)).toEqual({
      version: versionTag('urn:oid:9'),
    });
  });
});

describe('public document of a cancelled alert', () => {
  it('a new immutable document says cancelled, keeps the cells, names the version it replaced, and leaves the index', async () => {
    const cells = gridDisk(latLngToCell(37.33, -121.89, 7), 1);
    const store = new SigningAlertStore(new InMemoryAlertStore(), TEST_SIGNING.signer);
    const clock = { now: () => new Date('2026-11-10T11:00:00Z') };
    await applyMessage(makeAlert({ signature: null }, cells), { store, clock });
    const before = (await store.get(makeAlert().id)) as Alert;
    // A bare Cancel: no area of its own.
    await applyMessage(
      makeAlert(
        {
          id: '99999999-9999-4999-8999-999999999999',
          sourceId: 'urn:oid:cancel',
          messageType: 'Cancel',
          sent: '2026-11-10T10:30:00Z',
          references: [{ sourceId: 'urn:oid:1', sent: '2026-11-10T10:00:00Z' }],
          signature: null,
        },
        [],
      ),
      { store, clock },
    );
    const after = (await store.get(before.id)) as Alert;
    const writes = new Map<string, string>();
    const publisher = new AlertPublisher({
      writer: { putJson: async (k, body) => void writes.set(k, body) },
      alerts: store,
      cellResolution: 7,
      clock,
    });
    await publisher.publish(before);
    const shard = `alerts/v1/active/${cellToParent(cells[0] as string, 4)}.json`;
    expect((JSON.parse(writes.get(shard) ?? '{}') as ActiveIndexFile).alerts).toHaveLength(1);
    await publisher.publish(after);

    const doc = PublicAlertDocumentSchema.parse(
      JSON.parse(writes.get(publicAlertDocKey(after)) ?? ''),
    );
    expect(doc).toMatchObject({
      state: 'cancelled',
      messageType: 'Cancel',
      supersedes: { sourceId: 'urn:oid:1', relaySeverity: 'warning' },
    });
    expect(doc.area.cellCount).toBe(cells.length);
    expect(doc).toEqual(buildPublicDocument(after));
    // Old and new versions have different immutable URLs; the pointer names the new one.
    expect(publicAlertDocKey(after)).not.toBe(publicAlertDocKey(before));
    expect(JSON.parse(writes.get(publicAlertPointerKey(after.id)) ?? '')).toMatchObject({
      state: 'cancelled',
      doc: `/${publicAlertDocKey(after)}`,
    });
    // The cancelled alert drops out of the shard index (its cells were kept, so the shard is known).
    expect((JSON.parse(writes.get(shard) ?? '{}') as ActiveIndexFile).alerts).toEqual([]);
  });
});
