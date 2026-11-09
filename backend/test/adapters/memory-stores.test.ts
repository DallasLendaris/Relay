import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import { InMemoryAlertStore } from '../../src/adapters/memory/alert-store.js';
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
