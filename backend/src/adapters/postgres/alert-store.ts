import type { Alert } from '@relay/shared';
import type { AlertStore } from '../../ports.js';
import { parseJson, type SqlExecutor } from '../../db/sql.js';
import { CELL_CHUNK, chunk, textArrayParam } from './common.js';

interface RecordRow {
  record: unknown;
}

/**
 * The Data API rejects requests over 64 KB. save() throws above 60,000 bytes of
 * parameters, leaving room for the SQL text and the request envelope. With cells packed
 * (see packCells) that is roughly 5,000 cells; a larger alert needs a different design
 * (for example the cells in S3), so we fail loudly instead of truncating.
 */
export const MAX_SAVE_PARAM_BYTES = 60_000;

const H3_INDEX = /^[0-9a-f]{15}$/;

/**
 * Cells as one compact string. An H3 res-7 index is 15 hex digits; as raw bytes in base64
 * it takes about 11 characters instead of 18 in a JSON array. Anything that is not a plain
 * H3 index falls back to a JSON array (only test data does that).
 */
export function packCells(cells: string[]): string {
  if (cells.every((c) => H3_INDEX.test(c))) {
    // A leading "0" makes each index 16 hex digits, exactly 8 bytes.
    return 'b64:' + Buffer.from(cells.map((c) => '0' + c).join(''), 'hex').toString('base64');
  }
  return 'raw:' + JSON.stringify(cells);
}

// SQL that reverses packCells into a text[] (the padding "0" is cut off again).
const UNPACK_CELLS = `CASE WHEN left(p.packed, 4) = 'b64:' THEN
      ARRAY(SELECT substring(encode(substring(b.d FROM n * 8 + 1 FOR 8), 'hex') FROM 2)
              FROM (SELECT decode(substr(p.packed, 5), 'base64') AS d) b,
                   generate_series(0, length(b.d) / 8 - 1) AS n
             ORDER BY n)
    ELSE ARRAY(SELECT jsonb_array_elements_text(substr(p.packed, 5)::jsonb))
    END`;

export class PostgresAlertStore implements AlertStore {
  constructor(private readonly db: SqlExecutor) {}

  async get(id: string): Promise<Alert | null> {
    const rows = await this.db.query<RecordRow>('SELECT record FROM alerts WHERE id = $1::uuid', [
      id,
    ]);
    return rows[0] ? parseJson<Alert>(rows[0].record) : null;
  }

  async findBySourceId(sourceId: string): Promise<Alert | null> {
    const rows = await this.db.query<RecordRow>(
      `SELECT a.record FROM alert_source_ids s
         JOIN alerts a ON a.id = s.alert_id
        WHERE s.source_id = $1`,
      [sourceId],
    );
    return rows[0] ? parseJson<Alert>(rows[0].record) : null;
  }

  async save(alert: Alert): Promise<void> {
    // Two parameters only, so the (large) cell list travels once: the record with its
    // cell list emptied, and the cells packed. SQL puts the cells back into the stored
    // record and derives id, source_id, state, expires and h3_cells from it.
    const params = [
      JSON.stringify({ ...alert, area: { ...alert.area, h3Cells: [] } }),
      packCells(alert.area.h3Cells),
    ];
    const bytes = Buffer.byteLength(params.join(''));
    if (bytes > MAX_SAVE_PARAM_BYTES) {
      throw new Error(
        `Alert ${alert.id} is too large to save through the Data API: ${bytes} bytes ` +
          `(${alert.area.h3Cells.length} cells), limit ${MAX_SAVE_PARAM_BYTES}`,
      );
    }
    // One statement (Data API runs one per call): upsert the alert, then register every
    // source id in its chain. The chain's ids now point at this alert.
    await this.db.query(
      `WITH p AS (SELECT $1::jsonb AS r, $2::text AS packed),
       c AS (SELECT ${UNPACK_CELLS} AS cells FROM p),
       upserted AS (
         INSERT INTO alerts (id, source_id, state, expires, h3_cells, record, updated_at)
         SELECT (p.r ->> 'id')::uuid, p.r ->> 'sourceId', p.r ->> 'state',
                (p.r ->> 'expires')::timestamptz, c.cells,
                jsonb_set(p.r, '{area,h3Cells}', to_jsonb(c.cells)), now()
           FROM p, c
         ON CONFLICT (id) DO UPDATE SET
           source_id = EXCLUDED.source_id, state = EXCLUDED.state, expires = EXCLUDED.expires,
           h3_cells = EXCLUDED.h3_cells, record = EXCLUDED.record, updated_at = now()
         RETURNING id
       )
       INSERT INTO alert_source_ids (source_id, alert_id)
       SELECT s, (SELECT id FROM upserted)
         FROM p, jsonb_array_elements_text(
                jsonb_build_array(p.r ->> 'sourceId') ||
                COALESCE((SELECT jsonb_agg(ref ->> 'sourceId')
                            FROM jsonb_array_elements(p.r -> 'references') ref), '[]'::jsonb)
              ) AS s
       ON CONFLICT (source_id) DO UPDATE SET alert_id = EXCLUDED.alert_id`,
      params,
    );
  }

  async listActiveForCells(cells: string[], now: Date): Promise<Alert[]> {
    const found = new Map<string, Alert>();
    for (const part of chunk([...new Set(cells)], CELL_CHUNK)) {
      // && is array overlap, served by the GIN index. An alert has ended once now is past
      // the later of expires and ends (GREATEST ignores a null ends).
      const rows = await this.db.query<RecordRow>(
        `SELECT record FROM alerts
          WHERE state IN ('active', 'updated') AND h3_cells && ${textArrayParam(1)}
            AND $2::timestamptz < GREATEST(expires, (record ->> 'ends')::timestamptz)`,
        [JSON.stringify(part), now.toISOString()],
      );
      for (const row of rows) {
        const alert = parseJson<Alert>(row.record);
        found.set(alert.id, alert);
      }
    }
    return [...found.values()];
  }
}
