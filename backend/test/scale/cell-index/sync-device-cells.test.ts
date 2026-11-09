import { describe, expect, it } from 'vitest';
import {
  MemoryCellIndex,
  deviceCellsOf,
  syncDeviceCells,
} from '../../../src/scale/cell-index/index.js';
import type { DeviceTruth } from '../../../src/scale/cell-index/index.js';
import type { CellIndex, CellIndexEntry } from '../../../src/scale/contracts.js';
import { isPrimaryCell } from '../../../src/scale/fanout/primary-cell.js';

const DEVICE = 'dev-1';
const TARGET = { platform: 'ios', token: 'tok' } as const;
const UNIVERSE = ['c1', 'c2', 'c3', 'c4'];

type Place = { id: string; h3Cell: string };

/** A fake Aurora: the places and target of one device. */
class Truth {
  places: Place[] = [];
  load = async (): Promise<DeviceTruth> => ({
    target: TARGET,
    language: 'en',
    places: this.places.map((p) => ({ ...p })),
  });
}

/** Wraps an index: logs every write and calls `after` once it has landed. */
class Spy implements Pick<CellIndex, 'put' | 'remove'> {
  readonly log: string[] = [];
  constructor(
    readonly inner: MemoryCellIndex,
    private readonly after: () => Promise<void> | void = () => {},
    private readonly pause: () => Promise<void> = async () => {},
  ) {}
  async put(entries: CellIndexEntry[]): Promise<void> {
    await this.pause();
    await this.inner.put(entries);
    this.log.push(
      `put ${entries.map((e) => `${e.placeId}[${(e.deviceCells ?? []).join(',')}]`).join(' ')}`,
    );
    await this.after();
  }
  async remove(h3Cell: string, deviceId: string, placeId: string): Promise<void> {
    await this.pause();
    await this.inner.remove(h3Cell, deviceId, placeId);
    this.log.push(`remove ${placeId}`);
    await this.after();
  }
}

async function entriesOf(index: MemoryCellIndex): Promise<CellIndexEntry[]> {
  const out: CellIndexEntry[] = [];
  for (const c of UNIVERSE) out.push(...(await index.listForCell(c)).entries);
  return out;
}

/** How many entries of the device would send for an alert covering `area`. */
async function sends(index: MemoryCellIndex, area: Set<string>): Promise<number> {
  return (await entriesOf(index)).filter((e) => isPrimaryCell(e, area)).length;
}

/** Every non-empty subset of the universe. */
const subsets = (): Set<string>[] =>
  Array.from({ length: 2 ** UNIVERSE.length - 1 }, (_, m) => {
    const mask = m + 1;
    return new Set(UNIVERSE.filter((_c, i) => (mask >> i) & 1));
  });

/**
 * The safety property: for every alert area that covers a place the device keeps through the
 * whole change (`persistent`), at least one entry sends: never a miss.
 */
async function assertNoMiss(index: MemoryCellIndex, persistent: string[], when: string) {
  for (const area of subsets()) {
    if (!persistent.some((c) => area.has(c))) continue;
    const n = await sends(index, area);
    expect(n, `${when}: area ${[...area].join(',')}`).toBeGreaterThanOrEqual(1);
  }
}

/** Invariant: every cell named in any entry's deviceCells has an entry of the device. */
async function assertListedCellsHaveEntries(index: MemoryCellIndex, when: string) {
  const entries = await entriesOf(index);
  const have = new Set(entries.map((e) => e.h3Cell));
  for (const e of entries) {
    for (const c of e.deviceCells ?? []) expect(have.has(c), `${when}: ${c} listed`).toBe(true);
  }
}

async function seedIndex(truth: Truth, index: MemoryCellIndex): Promise<void> {
  await syncDeviceCells(index, { deviceId: DEVICE, load: truth.load });
}

describe('deviceCellsOf', () => {
  it('sorts and de-duplicates', () => {
    expect(deviceCellsOf([{ h3Cell: 'b' }, { h3Cell: 'a' }, { h3Cell: 'b' }])).toEqual(['a', 'b']);
    expect(deviceCellsOf([])).toEqual([]);
  });
});

describe('syncDeviceCells ordering', () => {
  it('add: puts the new entry first, then rewrites the others; no miss at any step', async () => {
    const truth = new Truth();
    truth.places = [
      { id: 'pA', h3Cell: 'c2' },
      { id: 'pB', h3Cell: 'c4' },
    ];
    const index = new MemoryCellIndex();
    await seedIndex(truth, index);

    truth.places.push({ id: 'pNew', h3Cell: 'c1' });
    const spy = new Spy(index, async () => {
      await assertNoMiss(index, ['c2', 'c4'], 'add');
      await assertListedCellsHaveEntries(index, 'add');
    });
    await syncDeviceCells(spy, { deviceId: DEVICE, load: truth.load, added: { placeId: 'pNew' } });

    expect(spy.log).toEqual(['put pNew[c1,c2,c4]', 'put pA[c1,c2,c4] pB[c1,c2,c4]']);
    expect((await entriesOf(index)).every((e) => e.deviceCells?.join() === 'c1,c2,c4')).toBe(true);
  });

  it('remove: rewrites the remaining entries without the cell first, then deletes; no miss at any step', async () => {
    const truth = new Truth();
    truth.places = [
      { id: 'pA', h3Cell: 'c1' },
      { id: 'pB', h3Cell: 'c2' },
      { id: 'pC', h3Cell: 'c4' },
    ];
    const index = new MemoryCellIndex();
    await seedIndex(truth, index);

    truth.places = truth.places.filter((p) => p.id !== 'pA');
    const spy = new Spy(index, async () => {
      await assertNoMiss(index, ['c2', 'c4'], 'remove');
      await assertListedCellsHaveEntries(index, 'remove');
    });
    await syncDeviceCells(spy, {
      deviceId: DEVICE,
      load: truth.load,
      removed: { h3Cell: 'c1', placeId: 'pA' },
    });

    expect(spy.log).toEqual(['put pB[c2,c4] pC[c2,c4]', 'remove pA']);
    expect((await entriesOf(index)).map((e) => e.placeId).sort()).toEqual(['pB', 'pC']);
  });

  it('removing one of two places in the same cell keeps the cell listed', async () => {
    const truth = new Truth();
    truth.places = [
      { id: 'pA', h3Cell: 'c1' },
      { id: 'pB', h3Cell: 'c1' },
      { id: 'pC', h3Cell: 'c3' },
    ];
    const index = new MemoryCellIndex();
    await seedIndex(truth, index);
    truth.places = truth.places.filter((p) => p.id !== 'pA');
    await syncDeviceCells(index, {
      deviceId: DEVICE,
      load: truth.load,
      removed: { h3Cell: 'c1', placeId: 'pA' },
    });
    const left = await entriesOf(index);
    expect(left.map((e) => e.placeId).sort()).toEqual(['pB', 'pC']);
    expect(left.every((e) => e.deviceCells?.join() === 'c1,c3')).toBe(true);
  });

  it('does not delete a "removed" place that Aurora still has (truth wins)', async () => {
    const truth = new Truth();
    truth.places = [{ id: 'pA', h3Cell: 'c1' }];
    const index = new MemoryCellIndex();
    await syncDeviceCells(index, {
      deviceId: DEVICE,
      load: truth.load,
      removed: { h3Cell: 'c1', placeId: 'pA' },
    });
    expect(await entriesOf(index)).toHaveLength(1);
  });

  it('a failed rewrite throws (callers enqueue a repair) and the new entry already exists', async () => {
    const truth = new Truth();
    truth.places = [{ id: 'pA', h3Cell: 'c2' }];
    const index = new MemoryCellIndex();
    await seedIndex(truth, index);
    truth.places.push({ id: 'pNew', h3Cell: 'c1' });
    let puts = 0;
    const flaky: Pick<CellIndex, 'put' | 'remove'> = {
      put: async (e) => {
        if (++puts === 2) throw new Error('dynamo down');
        await index.put(e);
      },
      remove: (...a) => index.remove(...a),
    };
    await expect(
      syncDeviceCells(flaky, { deviceId: DEVICE, load: truth.load, added: { placeId: 'pNew' } }),
    ).rejects.toThrow('dynamo down');
    // pA still has the old list [c2]; pNew lists both. Worst case a duplicate, never a miss.
    await assertNoMiss(index, ['c2'], 'after failed rewrite');
    await assertListedCellsHaveEntries(index, 'after failed rewrite');
    // The repair (same helper, no added/removed) converges.
    await seedIndex(truth, index);
    expect((await entriesOf(index)).every((e) => e.deviceCells?.join() === 'c1,c2')).toBe(true);
  });

  it('a device that no longer exists only has the removed entry deleted', async () => {
    const index = new MemoryCellIndex();
    await index.put([
      { h3Cell: 'c1', deviceId: DEVICE, placeId: 'pA', target: TARGET, language: 'en' },
    ]);
    await syncDeviceCells(index, {
      deviceId: DEVICE,
      load: async () => null,
      removed: { h3Cell: 'c1', placeId: 'pA' },
    });
    expect(await entriesOf(index)).toEqual([]);
  });
});

describe('syncDeviceCells interleaved with another change', () => {
  /** Small seeded generator so each interleaving is reproducible. */
  const rng = (seed: number) => () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 2 ** 32;
  };

  it('an add and a remove of the same device in random interleavings always converge, and a lone change never misses', async () => {
    let transientMisses = 0;
    for (let seed = 1; seed <= 400; seed++) {
      const rand = rng(seed);
      const truth = new Truth();
      truth.places = [
        { id: 'pA', h3Cell: 'c1' },
        { id: 'pB', h3Cell: 'c3' },
        { id: 'pC', h3Cell: 'c4' },
      ];
      const index = new MemoryCellIndex();
      await seedIndex(truth, index);

      // Each index call and each Aurora read waits a random number of ticks, so the two
      // requests' steps interleave differently for every seed.
      const ticks = async () => {
        for (let n = Math.floor(rand() * 4); n > 0; n--) await Promise.resolve();
      };
      const loadSlow = async () => {
        await ticks();
        return truth.load();
      };
      // persistent: pB (c3) and pC (c4) exist for the whole run.
      const check = async () => {
        for (const area of subsets()) {
          if (!['c3', 'c4'].some((c) => area.has(c))) continue;
          if ((await sends(index, area)) < 1) transientMisses++;
        }
      };
      const spy = new Spy(index, check, ticks);

      // Aurora changes first (as in the API), then each request syncs.
      truth.places.push({ id: 'pNew', h3Cell: 'c2' });
      truth.places = truth.places.filter((p) => p.id !== 'pA');
      await Promise.all([
        syncDeviceCells(spy, { deviceId: DEVICE, load: loadSlow, added: { placeId: 'pNew' } }),
        syncDeviceCells(spy, {
          deviceId: DEVICE,
          load: loadSlow,
          removed: { h3Cell: 'c1', placeId: 'pA' },
        }),
      ]);

      // Both requests saw the final Aurora state, so the index equals it exactly.
      const entries = await entriesOf(index);
      expect(entries.map((e) => e.placeId).sort(), `seed ${seed}`).toEqual(['pB', 'pC', 'pNew']);
      expect(
        entries.every((e) => e.deviceCells?.join() === 'c2,c3,c4'),
        `seed ${seed}`,
      ).toBe(true);
      await assertNoMiss(index, ['c2', 'c3', 'c4'], `seed ${seed} final`);
    }
    // When both requests read Aurora after both changes (as here), no step ever misses.
    expect(transientMisses).toBe(0);
  });

  it('a stale add that lands after a remove names a deleted cell, and the verify pass fixes it', async () => {
    // The add read Aurora BEFORE the remove, wrote after the remove deleted its entry. This is
    // the residual window: the rewrite names c1, which has no entry. It is detected and repaired
    // by the verify pass in the same call.
    const truth = new Truth();
    truth.places = [
      { id: 'pA', h3Cell: 'c1' },
      { id: 'pC', h3Cell: 'c4' },
    ];
    const index = new MemoryCellIndex();
    await seedIndex(truth, index);

    truth.places.push({ id: 'pNew', h3Cell: 'c2' });
    const staleRead = await truth.load(); // add's first read: still has pA
    let reads = 0;
    const addLoad = async () => (reads++ === 0 ? staleRead : truth.load());
    // The remove completes entirely before the add's writes start.
    truth.places = truth.places.filter((p) => p.id !== 'pA');
    await syncDeviceCells(index, {
      deviceId: DEVICE,
      load: truth.load,
      removed: { h3Cell: 'c1', placeId: 'pA' },
    });

    const spy = new Spy(index);
    await syncDeviceCells(spy, { deviceId: DEVICE, load: addLoad, added: { placeId: 'pNew' } });
    expect(spy.log.length).toBeGreaterThan(2); // a second pass ran
    await assertListedCellsHaveEntries(index, 'after verify');
    await assertNoMiss(index, ['c2', 'c4'], 'after verify');
    expect((await entriesOf(index)).every((e) => e.deviceCells?.join() === 'c2,c4')).toBe(true);
  });

  it('gives up with an error when the device keeps changing (the caller then queues a repair)', async () => {
    let n = 0;
    const load = async (): Promise<DeviceTruth> => ({
      target: TARGET,
      language: 'en',
      places: [{ id: `p${n++}`, h3Cell: 'c1' }],
    });
    await expect(
      syncDeviceCells(new MemoryCellIndex(), { deviceId: DEVICE, load }),
    ).rejects.toThrow('kept changing');
  });
});
