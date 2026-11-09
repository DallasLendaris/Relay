import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { beforeEach, describe, expect, it } from 'vitest';
import { relaySeverityFor, type Alert } from '@relay/shared';
import { applyMessage, effectiveState } from '../../src/ingest/lifecycle.js';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';

// Minimal local feature -> Alert converter (the real normalizer is written elsewhere).
type Ref = { identifier: string; sent: string };
type Props = Record<string, string | null> & { references: Ref[] };
const bare = (id: string): string => id.slice(id.lastIndexOf('/') + 1);
const str = (v: string | null | undefined): string => v ?? '';

function load(file: string): Alert[] {
  const url = new URL(`../fixtures/nws/${file}`, import.meta.url);
  const json = JSON.parse(readFileSync(url, 'utf8')) as { features: { properties: Props }[] };
  return json.features.map(({ properties: p }) => {
    const messageType = str(p['messageType']) as Alert['messageType'];
    return {
      id: randomUUID(),
      source: 'nws',
      sourceId: bare(str(p['id'])),
      event: str(p['event']),
      eventCode: null,
      relaySeverity: relaySeverityFor(str(p['event'])),
      capSeverity: str(p['severity']) as Alert['capSeverity'],
      urgency: str(p['urgency']) as Alert['urgency'],
      certainty: str(p['certainty']) as Alert['certainty'],
      status: str(p['status']) as Alert['status'],
      messageType,
      state:
        messageType === 'Cancel' ? 'cancelled' : messageType === 'Update' ? 'updated' : 'active',
      references: p.references.map((r) => ({ sourceId: r.identifier, sent: r.sent })),
      area: {
        description: str(p['areaDesc']),
        geometry: null,
        ugcCodes: [],
        sameCodes: [],
        h3Cells: ['cellA'],
      },
      sent: str(p['sent']),
      effective: str(p['effective']),
      onset: p['onset'] ?? null,
      expires: str(p['expires']),
      ends: p['ends'] ?? null,
      official: {
        headline: p['headline'] ?? null,
        description: str(p['description']),
        instruction: p['instruction'] ?? null,
        senderName: str(p['senderName']),
        language: 'en',
      },
      summaries: [],
      signature: null,
    };
  });
}

const [original, update] = load('update-chain.json') as [Alert, Alert];
const [cancel] = load('cancel.json') as [Alert];
const clock = { now: () => new Date('2026-11-09T12:00:00-08:00') };

let store: InMemoryAlertStore;
const apply = (a: Alert) => applyMessage(structuredClone(a), { store, clock });

beforeEach(() => {
  store = new InMemoryAlertStore();
});

describe('applyMessage', () => {
  it('creates a new record for an original alert', async () => {
    const r = await apply(original);
    expect(r.action).toBe('created');
    expect(r.alert.state).toBe('active');
    expect(await store.get(original.id)).not.toBeNull();
  });

  it('update keeps the same Relay id and the whole chain', async () => {
    await apply(original);
    const r = await apply(update);
    expect(r.action).toBe('updated');
    expect(r.alert.id).toBe(original.id);
    expect(r.alert.state).toBe('updated');
    expect(r.alert.sourceId).toBe(update.sourceId);
    expect(r.alert.expires).toBe(update.expires);
    expect(r.alert.references.map((x) => x.sourceId)).toEqual([original.sourceId]);
    expect((await store.findBySourceId(original.sourceId))?.id).toBe(original.id);
  });

  it('cancel marks the record cancelled and keeps the chain', async () => {
    await apply(original);
    await apply(update);
    const r = await apply(cancel);
    expect(r.action).toBe('cancelled');
    expect(r.alert.id).toBe(original.id);
    expect(r.alert.state).toBe('cancelled');
    expect(r.alert.sourceId).toBe(cancel.sourceId);
    expect(r.alert.references.map((x) => x.sourceId)).toEqual([original.sourceId, update.sourceId]);
    expect(await store.listActiveForCells(['cellA'], new Date('2000-01-01T00:00:00Z'))).toEqual([]);
  });

  it('ignores a cancel for an unknown alert', async () => {
    const r = await apply(cancel);
    expect(r.action).toBe('ignored');
    expect(r.reason).toMatch(/unknown/);
    expect(await store.get(cancel.id)).toBeNull();
  });

  it('turns an update with an unknown original into a new active record', async () => {
    const r = await apply(update);
    expect(r.action).toBe('created');
    expect(r.alert.state).toBe('active');
    expect(r.alert.id).toBe(update.id);
  });

  it('treats an Alert message that has references like an update', async () => {
    await apply(original);
    const r = await apply({ ...update, messageType: 'Alert' });
    expect(r.action).toBe('updated');
    expect(r.alert.id).toBe(original.id);
  });

  it('ignores a message older than the stored one', async () => {
    await apply(original);
    // Stored data is newer than the incoming update.
    await store.save({ ...original, sent: '2026-11-09T10:00:00-08:00' });
    expect(update.sent < '2026-11-09T10:00:00-08:00').toBe(true);
    const r = await apply(update);
    expect(r.action).toBe('ignored');
    expect(r.reason).toMatch(/older/);
    expect((await store.get(original.id))?.sourceId).toBe(original.sourceId);
  });

  it('never resurrects a cancelled alert', async () => {
    await apply(original);
    // A cancel that points straight at the original.
    await apply({ ...cancel, references: [{ sourceId: original.sourceId, sent: original.sent }] });
    const r = await apply(update);
    expect(r.action).toBe('ignored');
    expect(r.alert.state).toBe('cancelled');
    expect((await store.get(original.id))?.state).toBe('cancelled');
  });

  it('ignores a replay of the same message', async () => {
    await apply(original);
    await apply(update);
    const again = await apply(update);
    expect(again.action).toBe('ignored');
    expect(again.reason).toMatch(/replay/);
    expect((await apply(original)).action).toBe('ignored');
  });
});

describe('effectiveState', () => {
  it('is expired only after expires has passed', () => {
    expect(effectiveState(original, new Date('2026-11-09T17:59:00-08:00'))).toBe('active');
    expect(effectiveState(original, new Date('2026-11-09T18:01:00-08:00'))).toBe('expired');
  });

  it('uses ends when it is later than expires', () => {
    const a = { ...original, ends: '2026-11-09T20:00:00-08:00' };
    expect(effectiveState(a, new Date('2026-11-09T19:00:00-08:00'))).toBe('active');
    expect(effectiveState(a, new Date('2026-11-09T20:01:00-08:00'))).toBe('expired');
  });

  it('ignores ends when it is earlier than expires', () => {
    const a = { ...original, ends: '2026-11-09T10:00:00-08:00' };
    expect(effectiveState(a, clock.now())).toBe('active');
  });

  it('keeps updated state until expiry and never expires a cancelled alert', () => {
    expect(effectiveState(update, clock.now())).toBe('updated');
    expect(effectiveState(cancel, new Date('2026-11-10T00:00:00-08:00'))).toBe('cancelled');
  });
});
