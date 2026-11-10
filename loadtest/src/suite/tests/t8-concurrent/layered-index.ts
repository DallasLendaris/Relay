/**
 * The cell index the shard workers read: the seed devices from disk (t2's DiskCellIndex) plus,
 * optionally, an overlay of entries the API wrote since (t9's new devices). Only `listForCell` is
 * on the fan-out path. A read takes one snapshot of the cell when it starts (disk lines, then the
 * overlay), like a consistent paged read, and pages through that snapshot; the cursor names the
 * snapshot so two jobs reading the same cell at once (two alerts) never share a position.
 */
import type {
  CellIndex,
  CellIndexEntry,
  CellIndexPage,
} from '../../../../../backend/src/scale/contracts.js';
import type { DiskCellIndex } from '../t2-fanout/seed-data.js';

export class LayeredCellIndex implements Pick<CellIndex, 'listForCell'> {
  private readonly snapshots = new Map<string, CellIndexEntry[]>();
  private seq = 0;
  /** Entries served from the overlay, for the report. */
  overlayEntries = 0;

  constructor(
    private readonly base: DiskCellIndex,
    private readonly overlay?: (cell: string) => Promise<CellIndexEntry[]>,
  ) {}

  async listForCell(h3Cell: string, cursor?: string, limit = 1000): Promise<CellIndexPage> {
    let id: string;
    let start: number;
    let all: CellIndexEntry[];
    if (cursor === undefined) {
      // Everything the disk holds for the cell in one page (the adapter pages by numeric cursor).
      const disk = (await this.base.listForCell(h3Cell, undefined, Number.MAX_SAFE_INTEGER))
        .entries;
      const extra = this.overlay ? await this.overlay(h3Cell) : [];
      this.overlayEntries += extra.length;
      all = extra.length > 0 ? disk.concat(extra) : disk;
      id = String(++this.seq);
      start = 0;
      this.snapshots.set(id, all);
    } else {
      const sep = cursor.indexOf(':');
      id = cursor.slice(0, sep);
      start = Number(cursor.slice(sep + 1));
      const snap = this.snapshots.get(id);
      if (!snap) throw new Error('unknown cell index cursor');
      all = snap;
    }
    const entries = all.slice(start, start + limit);
    const next = start + limit;
    if (next < all.length) return { entries, cursor: `${id}:${next}` };
    this.snapshots.delete(id);
    return { entries };
  }
}
