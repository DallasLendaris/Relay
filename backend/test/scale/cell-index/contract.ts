import { describe, expect, it } from 'vitest';
import type { CellIndex, CellIndexEntry } from '../../../src/scale/contracts.js';

export const CELL = '872830828ffffff';
export const OTHER_CELL = '872830829ffffff';

export const entry = (n: number, over: Partial<CellIndexEntry> = {}): CellIndexEntry => ({
  h3Cell: CELL,
  deviceId: `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`,
  placeId: `11111111-1111-4111-8111-${String(n).padStart(12, '0')}`,
  target: { platform: 'ios', token: 'a'.repeat(64) + n },
  language: 'en',
  ...over,
});

/** Lists a whole cell by following cursors. Asserts every page respects the limit. */
export async function listAll(index: CellIndex, cell: string, limit?: number) {
  const out: CellIndexEntry[] = [];
  let cursor: string | undefined;
  let pages = 0;
  do {
    const page = await index.listForCell(cell, cursor, limit);
    if (limit) expect(page.entries.length).toBeLessThanOrEqual(limit);
    out.push(...page.entries);
    cursor = page.cursor;
    pages++;
    expect(pages).toBeLessThan(10_000);
  } while (cursor);
  return { entries: out, pages };
}

const byId = (a: CellIndexEntry, b: CellIndexEntry) => a.placeId.localeCompare(b.placeId);

/** The CellIndex behavior every implementation must have. `make` gives a fresh, empty index. */
export function cellIndexContract(name: string, make: () => CellIndex): void {
  describe(`CellIndex contract: ${name}`, () => {
    it('round-trips deviceCells, and an entry without it comes back without it', async () => {
      const index = make();
      await index.put([
        entry(1, { deviceCells: [CELL, OTHER_CELL] }),
        entry(2), // old entry: no deviceCells
      ]);
      const { entries } = await listAll(index, CELL);
      expect(entries.find((e) => e.placeId === entry(1).placeId)?.deviceCells).toEqual([
        CELL,
        OTHER_CELL,
      ]);
      const old = entries.find((e) => e.placeId === entry(2).placeId);
      expect(old).toBeDefined();
      expect(old && 'deviceCells' in old).toBe(false);
    });

    it('puts and lists entries, replacing by (deviceId, placeId)', async () => {
      const index = make();
      await index.put([entry(1), entry(2)]);
      await index.put([entry(1, { language: 'es', target: null })]);
      const { entries } = await listAll(index, CELL);
      expect(entries.sort(byId)).toEqual([entry(1, { language: 'es', target: null }), entry(2)]);
    });

    it('keeps cells apart and returns nothing for an empty cell', async () => {
      const index = make();
      await index.put([entry(1), entry(2, { h3Cell: OTHER_CELL })]);
      expect((await listAll(index, CELL)).entries.map((e) => e.placeId)).toEqual([
        entry(1).placeId,
      ]);
      expect(await index.listForCell('872830000ffffff')).toEqual({ entries: [] });
    });

    it('lets one device hold several places in one cell', async () => {
      const index = make();
      const dev = entry(1).deviceId;
      await index.put([entry(1), entry(2, { deviceId: dev })]);
      expect((await listAll(index, CELL)).entries).toHaveLength(2);
    });

    it('dedupes a repeated key inside one put (last wins)', async () => {
      const index = make();
      await index.put([entry(1), entry(1, { language: 'es' })]);
      const { entries } = await listAll(index, CELL);
      expect(entries).toHaveLength(1);
      expect(entries[0]?.language).toBe('es');
    });

    it('removes one entry and ignores a missing one', async () => {
      const index = make();
      await index.put([entry(1), entry(2)]);
      await index.remove(CELL, entry(1).deviceId, entry(1).placeId);
      await index.remove(CELL, entry(9).deviceId, entry(9).placeId);
      expect((await listAll(index, CELL)).entries.map((e) => e.placeId)).toEqual([
        entry(2).placeId,
      ]);
    });

    it('keeps the Android Firebase project of a target through put, update and list', async () => {
      const index = make();
      const dev = entry(1).deviceId;
      const token = 'f'.repeat(120);
      await index.put([entry(1, { target: { platform: 'android', token, project: 'relay-b' } })]);
      expect((await listAll(index, CELL)).entries[0]?.target).toEqual({
        platform: 'android',
        token,
        project: 'relay-b',
      });
      const moved = { platform: 'android' as const, token, project: 'relay-c' };
      await index.updateDevice(dev, [{ h3Cell: CELL, placeId: entry(1).placeId }], {
        target: moved,
        language: 'en',
      });
      expect((await listAll(index, CELL)).entries[0]?.target).toEqual(moved);
      // No project stays no project (the default project).
      const plain = { platform: 'android' as const, token };
      await index.updateDevice(dev, [{ h3Cell: CELL, placeId: entry(1).placeId }], {
        target: plain,
        language: 'en',
      });
      expect((await listAll(index, CELL)).entries[0]?.target).toEqual(plain);
    });

    it('updateDevice patches target and language on that device only, and never recreates', async () => {
      const index = make();
      const dev = entry(1).deviceId;
      const second = entry(2, { deviceId: dev, h3Cell: OTHER_CELL });
      await index.put([entry(1), second, entry(3)]);
      const target = { platform: 'android' as const, token: 'f'.repeat(120) };
      await index.updateDevice(
        dev,
        [
          { h3Cell: CELL, placeId: entry(1).placeId },
          { h3Cell: OTHER_CELL, placeId: second.placeId },
          // Removed place: must not be resurrected.
          { h3Cell: CELL, placeId: 'gone' },
        ],
        { target, language: 'es' },
      );
      const a = (await listAll(index, CELL)).entries.sort(byId);
      expect(a).toEqual([{ ...entry(1), target, language: 'es' }, entry(3)]);
      expect((await listAll(index, OTHER_CELL)).entries).toEqual([
        { ...second, target, language: 'es' },
      ]);
      await index.updateDevice(dev, [{ h3Cell: CELL, placeId: entry(1).placeId }], {
        target: null,
        language: 'es',
      });
      expect(
        (await listAll(index, CELL)).entries.find((e) => e.deviceId === dev)?.target,
      ).toBeNull();
    });

    it('pages through a cell with an opaque cursor: complete, no duplicates, in limit', async () => {
      const index = make();
      const all = Array.from({ length: 103 }, (_, i) => entry(i + 1));
      await index.put(all);
      const { entries, pages } = await listAll(index, CELL, 10);
      expect(pages).toBeGreaterThanOrEqual(11);
      expect(new Set(entries.map((e) => e.placeId)).size).toBe(103);
      expect(entries.sort(byId)).toEqual(all.sort(byId));
      const first = await index.listForCell(CELL, undefined, 10);
      expect(first.cursor).toMatch(/^[A-Za-z0-9_-]+=*$/);
    });

    it('uses a default page limit of 1,000', async () => {
      const index = make();
      await index.put(Array.from({ length: 1001 }, (_, i) => entry(i + 1)));
      const first = await index.listForCell(CELL);
      expect(first.entries).toHaveLength(1000);
      expect(first.cursor).toBeDefined();
      const rest = await index.listForCell(CELL, first.cursor);
      expect(rest.entries).toHaveLength(1);
      expect(rest.cursor).toBeUndefined();
    });

    it('rejects a garbage cursor and a cursor from another cell', async () => {
      const index = make();
      await index.put(Array.from({ length: 5 }, (_, i) => entry(i + 1)));
      await expect(index.listForCell(CELL, '!!not-a-cursor')).rejects.toThrow(/cursor/);
      const page = await index.listForCell(CELL, undefined, 2);
      await expect(index.listForCell(OTHER_CELL, page.cursor, 2)).rejects.toThrow(/cursor/);
    });

    it('rejects a non-positive limit', async () => {
      await expect(make().listForCell(CELL, undefined, 0)).rejects.toThrow(/limit/);
    });
  });
}
