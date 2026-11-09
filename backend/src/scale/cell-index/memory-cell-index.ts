import type { CellIndex, CellIndexEntry, CellIndexPage } from '../contracts.js';
import { DEFAULT_PAGE_LIMIT, decodeCursor, encodeCursor, sortKey } from './util.js';

/** In-memory CellIndex for tests and local runs. Same paging behavior as the DynamoDB one. */
export class MemoryCellIndex implements CellIndex {
  /** h3Cell -> sk -> entry */
  private readonly cells = new Map<string, Map<string, CellIndexEntry>>();

  async put(entries: CellIndexEntry[]): Promise<void> {
    for (const e of entries) {
      let cell = this.cells.get(e.h3Cell);
      if (!cell) this.cells.set(e.h3Cell, (cell = new Map()));
      cell.set(sortKey(e.deviceId, e.placeId), structuredClone(e));
    }
  }

  async remove(h3Cell: string, deviceId: string, placeId: string): Promise<void> {
    this.cells.get(h3Cell)?.delete(sortKey(deviceId, placeId));
  }

  async updateDevice(
    deviceId: string,
    cells: Array<{ h3Cell: string; placeId: string }>,
    patch: { target: CellIndexEntry['target']; language: 'en' | 'es' },
  ): Promise<void> {
    for (const { h3Cell, placeId } of cells) {
      // Only existing entries change; a removed place is not brought back.
      const entry = this.cells.get(h3Cell)?.get(sortKey(deviceId, placeId));
      if (entry) {
        entry.target = patch.target ? { ...patch.target } : null;
        entry.language = patch.language;
      }
    }
  }

  async listForCell(h3Cell: string, cursor?: string, limit?: number): Promise<CellIndexPage> {
    const size = limit ?? DEFAULT_PAGE_LIMIT;
    if (!Number.isInteger(size) || size < 1) throw new Error('limit must be a positive integer');
    let after: string | undefined;
    if (cursor !== undefined) {
      const c = decodeCursor(cursor) as { h3Cell?: unknown; sk?: unknown } | null;
      // A cursor is only valid for the cell it came from.
      if (!c || c.h3Cell !== h3Cell || typeof c.sk !== 'string') {
        throw new Error('invalid cell index cursor');
      }
      after = c.sk;
    }
    const keys = [...(this.cells.get(h3Cell)?.keys() ?? [])].sort();
    const rest = after === undefined ? keys : keys.filter((k) => k > after);
    const pageKeys = rest.slice(0, size);
    const entries = pageKeys.flatMap((k) => {
      const e = this.cells.get(h3Cell)?.get(k);
      return e ? [structuredClone(e)] : [];
    });
    const last = pageKeys[pageKeys.length - 1];
    return rest.length > size && last !== undefined
      ? { entries, cursor: encodeCursor({ h3Cell, sk: last }) }
      : { entries };
  }
}
