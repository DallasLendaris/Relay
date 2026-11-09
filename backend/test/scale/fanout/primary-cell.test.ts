import { describe, expect, it } from 'vitest';
import { isPrimaryCell, primaryCellOf } from '../../../src/scale/fanout/primary-cell.js';

const CELLS = ['a', 'b', 'c', 'd'];

/** All subsets of `items`, as arrays in input order. */
const subsetsOf = <T>(items: T[]): T[][] =>
  Array.from({ length: 2 ** items.length }, (_, mask) => items.filter((_x, i) => (mask >> i) & 1));

describe('isPrimaryCell', () => {
  it('exhaustive: for every device cell list and alert area, exactly the smallest shared cell sends', () => {
    let checked = 0;
    for (const deviceCells of subsetsOf(CELLS).filter((s) => s.length > 0)) {
      for (const area of subsetsOf(CELLS)) {
        const areaSet = new Set(area);
        const shared = deviceCells.filter((c) => areaSet.has(c)).sort();
        // The shard worker only reads cells of the alert area, so only entries in it are asked.
        const senders = deviceCells.filter(
          (c) => areaSet.has(c) && isPrimaryCell({ h3Cell: c, deviceCells }, areaSet),
        );
        if (shared.length === 0) {
          expect(senders).toEqual([]); // nothing in the area: nothing reads these entries
        } else {
          expect(senders, `${deviceCells.join()} in ${area.join()}`).toEqual([shared[0]]);
        }
        checked++;
      }
    }
    expect(checked).toBe(15 * 16);
  });

  it('is independent of the order of deviceCells', () => {
    const area = new Set(['b', 'c', 'd']);
    for (const cells of [
      ['d', 'c', 'b', 'a'],
      ['a', 'b', 'c', 'd'],
      ['c', 'a', 'd', 'b'],
    ]) {
      expect(isPrimaryCell({ h3Cell: 'b', deviceCells: cells }, area)).toBe(true);
      expect(isPrimaryCell({ h3Cell: 'c', deviceCells: cells }, area)).toBe(false);
      expect(isPrimaryCell({ h3Cell: 'a', deviceCells: cells }, area)).toBe(false);
    }
  });

  it('fails open when deviceCells is missing or empty', () => {
    const area = new Set(['a', 'b']);
    expect(isPrimaryCell({ h3Cell: 'b' }, area)).toBe(true);
    expect(isPrimaryCell({ h3Cell: 'b', deviceCells: [] }, area)).toBe(true);
  });

  it('fails open when none of the listed cells is in the alert area (inconsistent entry)', () => {
    expect(isPrimaryCell({ h3Cell: 'a', deviceCells: ['x', 'y'] }, new Set(['a', 'b']))).toBe(true);
    expect(isPrimaryCell({ h3Cell: 'a', deviceCells: ['x'] }, new Set())).toBe(true);
  });

  it('an entry whose own cell is not in its list is not primary when the list has an area cell', () => {
    // Inconsistent, but the rule is the rule: the listed primary 'b' has its own entry elsewhere.
    expect(isPrimaryCell({ h3Cell: 'a', deviceCells: ['b'] }, new Set(['a', 'b']))).toBe(false);
  });

  it('compares cell strings by plain code-unit order (same order as the sorted list)', () => {
    const cells = ['872830828ffffff', '872830829ffffff', '87283082affffff'];
    const area = new Set(cells);
    expect(isPrimaryCell({ h3Cell: cells[0] as string, deviceCells: cells }, area)).toBe(true);
    expect(isPrimaryCell({ h3Cell: cells[1] as string, deviceCells: cells }, area)).toBe(false);
  });
});

describe('primaryCellOf', () => {
  it('returns the smallest area cell, or undefined', () => {
    expect(primaryCellOf(['c', 'a', 'b'], new Set(['b', 'c']))).toBe('b');
    expect(primaryCellOf(['a'], new Set(['z']))).toBeUndefined();
    expect(primaryCellOf([], new Set(['a']))).toBeUndefined();
  });
});
