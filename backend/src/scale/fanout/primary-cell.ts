import type { CellIndexEntry } from '../contracts.js';

/**
 * Primary-cell rule: a device with saved places in several cells of one alert area must be pushed
 * from exactly one of them. A shard worker only sees its own cells, so every entry carries the
 * device's full cell list (`deviceCells`) and decides locally.
 *
 *   P = deviceCells ∩ alert area;  primary = the smallest string in P.
 *   Send from this entry only when entry.h3Cell === primary.
 *
 * Fail open: with no deviceCells (an old entry), an empty list, or an empty P (an inconsistent
 * entry, e.g. its own cell is not in the list), the entry sends. A duplicate is better than a miss.
 */
export function isPrimaryCell(
  entry: Pick<CellIndexEntry, 'h3Cell' | 'deviceCells'>,
  alertCellSet: ReadonlySet<string>,
): boolean {
  const cells = entry.deviceCells;
  if (!cells || cells.length === 0) return true;
  const primary = primaryCellOf(cells, alertCellSet);
  return primary === undefined || primary === entry.h3Cell;
}

/** The smallest of `deviceCells` that is in the alert area, or undefined when none is. */
export function primaryCellOf(
  deviceCells: readonly string[],
  alertCellSet: ReadonlySet<string>,
): string | undefined {
  let primary: string | undefined;
  for (const c of deviceCells) {
    if (alertCellSet.has(c) && (primary === undefined || c < primary)) primary = c;
  }
  return primary;
}
