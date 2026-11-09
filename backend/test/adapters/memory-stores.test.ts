import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../src/adapters/memory/place-store.js';
import { InMemorySeenStore } from '../../src/adapters/memory/seen-store.js';

function alert(over: Partial<Alert>): Alert {
  return {
    id: 'id-1',
    source: 'nws',
    sourceId: 'src-1',
    event: 'Flood Warning',
    eventCode: null,
    relaySeverity: 'warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: { description: 'x', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: ['c1'] },
    sent: '2026-11-09T06:10:00-08:00',
    effective: '2026-11-09T06:10:00-08:00',
    onset: null,
    expires: '2026-11-09T18:00:00-08:00',
    ends: null,
    official: {
      headline: null,
      description: '',
      instruction: null,
      senderName: 'NWS',
      language: 'en',
    },
    summaries: [],
    signature: null,
    ...over,
  };
}

describe('InMemoryAlertStore', () => {
  it('saves, gets, and replaces by id', async () => {
    const s = new InMemoryAlertStore();
    expect(await s.get('id-1')).toBeNull();
    await s.save(alert({}));
    await s.save(alert({ event: 'Changed' }));
    expect((await s.get('id-1'))?.event).toBe('Changed');
  });

  it('does not let callers mutate stored data', async () => {
    const s = new InMemoryAlertStore();
    await s.save(alert({}));
    const got = await s.get('id-1');
    if (got) got.event = 'Mutated';
    expect((await s.get('id-1'))?.event).toBe('Flood Warning');
  });

  it('finds by current sourceId or any sourceId in references', async () => {
    const s = new InMemoryAlertStore();
    await s.save(
      alert({
        sourceId: 'src-3',
        references: [{ sourceId: 'src-1', sent: '2026-11-09T06:10:00-08:00' }],
      }),
    );
    expect((await s.findBySourceId('src-3'))?.id).toBe('id-1');
    expect((await s.findBySourceId('src-1'))?.id).toBe('id-1');
    expect(await s.findBySourceId('nope')).toBeNull();
  });

  it('lists only active or updated alerts that intersect the cells', async () => {
    const s = new InMemoryAlertStore();
    await s.save(alert({ id: 'a', state: 'active' }));
    await s.save(alert({ id: 'b', state: 'updated' }));
    await s.save(alert({ id: 'c', state: 'cancelled' }));
    await s.save(alert({ id: 'd', state: 'expired' }));
    await s.save(
      alert({ id: 'e', state: 'active', area: { ...alert({}).area, h3Cells: ['other'] } }),
    );
    const ids = (await s.listActiveForCells(['c1', 'zzz'], new Date('2000-01-01T00:00:00Z')))
      .map((a) => a.id)
      .sort();
    expect(ids).toEqual(['a', 'b']);
    expect(await s.listActiveForCells([], new Date('2000-01-01T00:00:00Z'))).toEqual([]);
  });

  it('excludes alerts that have ended, using time and not the stored state', async () => {
    const s = new InMemoryAlertStore();
    // expires 18:00-08:00 = 02:00Z next day
    await s.save(alert({ id: 'ended', state: 'active' }));
    await s.save(alert({ id: 'extended', state: 'updated', ends: '2026-11-10T06:00:00Z' }));
    const at = (iso: string) => s.listActiveForCells(['c1'], new Date(iso));
    expect((await at('2026-11-10T01:00:00Z')).map((a) => a.id).sort()).toEqual([
      'ended',
      'extended',
    ]);
    // After expires but before the later ends: only the extended one.
    expect((await at('2026-11-10T03:00:00Z')).map((a) => a.id)).toEqual(['extended']);
    expect(await at('2026-11-10T07:00:00Z')).toEqual([]);
  });
});

describe('InMemorySeenStore', () => {
  it('remembers marked source ids', async () => {
    const s = new InMemorySeenStore();
    expect(await s.has('x')).toBe(false);
    await s.mark('x', '2026-11-09T06:10:00-08:00');
    expect(await s.has('x')).toBe(true);
    expect(await s.has('y')).toBe(false);
  });
});

describe('MemoryDeviceStore push tokens', () => {
  const dev = (id: string, token: string | null) => ({
    id,
    credentialHash: id,
    expoPushToken: token,
    language: 'en' as const,
    createdAt: '2026-11-09T00:00:00.000Z',
  });

  it('assignPushToken leaves one holder; clearPushToken forgets it', async () => {
    const s = new MemoryDeviceStore();
    await s.create(dev('a', null));
    await s.create(dev('b', null));
    await s.assignPushToken('a', 't', 'en');
    await s.assignPushToken('b', 't', 'es');
    expect((await s.get('a'))?.expoPushToken).toBeNull();
    expect(await s.get('b')).toMatchObject({ expoPushToken: 't', language: 'es' });
    await s.clearPushToken('t');
    expect((await s.get('b'))?.expoPushToken).toBeNull();
  });
});

describe('MemoryPlaceStore.listMatchesForCells', () => {
  it('returns place and device ids for places in the cells', async () => {
    const s = new MemoryPlaceStore();
    const p = (id: string, h3Cell: string) => ({
      id,
      deviceId: `d-${id}`,
      label: 'x',
      shortAddress: 'x',
      latitude: 1,
      longitude: 1,
      h3Cell,
      createdAt: '2026-11-09T00:00:00.000Z',
    });
    await s.add(p('1', 'c1'));
    await s.add(p('2', 'c2'));
    expect(await s.listMatchesForCells(['c1'])).toEqual([{ placeId: '1', deviceId: 'd-1' }]);
  });
});
