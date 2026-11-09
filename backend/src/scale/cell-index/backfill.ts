import type { SqlExecutor } from '../../db/sql.js';
import type { PushPlatform } from '../contracts.js';
import type { CellIndex, CellIndexEntry } from '../contracts.js';
import { targetOf } from './util.js';

/** A device has at most 5 places (MAX_PLACES_PER_DEVICE), so at most 5 cells. */
const MAX_DEVICE_CELLS = 5;
const NIL_UUID = '00000000-0000-0000-0000-000000000000';

export interface BackfillOptions {
  db: SqlExecutor;
  index: CellIndex;
  /** Places per Aurora page and per index write. Default 1,000. */
  pageSize?: number;
  /** Count what would be written without writing. */
  dryRun?: boolean;
  /** Called after each page with the running counts. */
  onPage?: (counts: BackfillCounts) => void;
}

export interface BackfillCounts {
  pages: number;
  placesScanned: number;
  /** Entries written (or, in a dry run, that would be written). */
  entriesPut: number;
  withTarget: number;
  withoutTarget: number;
  dryRun: boolean;
}

interface Row {
  id: string;
  device_id: string;
  h3_cell: string;
  language: 'en' | 'es';
  expo_push_token: string | null;
  push_platform: PushPlatform | null;
  push_token: string | null;
  /** Comma-separated, sorted, distinct cells of ALL the device's places. */
  device_cells: string | null;
}

/**
 * Copies every place (joined with its device's push target) from Aurora into the cell index.
 * Keyset pagination on place id keeps each Data API response small. Idempotent: put replaces
 * by key, and every entry carries the device's full `deviceCells` (computed in the same query,
 * so no extra writes), so re-running (or running while the API writes through) is harmless. It does not
 * delete index entries for places that no longer exist.
 */
export async function backfillCellIndex(opts: BackfillOptions): Promise<BackfillCounts> {
  const pageSize = opts.pageSize ?? 1000;
  const counts: BackfillCounts = {
    pages: 0,
    placesScanned: 0,
    entriesPut: 0,
    withTarget: 0,
    withoutTarget: 0,
    dryRun: opts.dryRun ?? false,
  };
  let after = NIL_UUID;
  for (;;) {
    const rows = await opts.db.query<Row>(
      `SELECT p.id, p.device_id, p.h3_cell, d.language, d.expo_push_token, d.push_platform, d.push_token,
              (SELECT string_agg(DISTINCT p2.h3_cell, ',' ORDER BY p2.h3_cell)
                 FROM places p2 WHERE p2.device_id = p.device_id) AS device_cells
         FROM places p JOIN devices d ON d.id = p.device_id
        WHERE p.id > $1::uuid
        ORDER BY p.id
        LIMIT $2::int`,
      [after, pageSize],
    );
    if (rows.length === 0) break;
    const entries = rows.map((r): CellIndexEntry => {
      const target = targetOf({
        pushPlatform: r.push_platform,
        pushToken: r.push_token,
        expoPushToken: r.expo_push_token,
      });
      if (target) counts.withTarget++;
      else counts.withoutTarget++;
      return {
        h3Cell: r.h3_cell,
        deviceId: r.device_id,
        placeId: r.id,
        target,
        language: r.language,
        deviceCells: (r.device_cells ?? r.h3_cell).split(',').sort().slice(0, MAX_DEVICE_CELLS),
      };
    });
    if (!counts.dryRun) await opts.index.put(entries);
    counts.pages++;
    counts.placesScanned += rows.length;
    counts.entriesPut += entries.length;
    opts.onPage?.({ ...counts });
    const last = rows[rows.length - 1];
    if (!last || rows.length < pageSize) break;
    after = last.id;
  }
  return counts;
}
