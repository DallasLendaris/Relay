import type { Alert } from '@relay/shared';
import type { AlertStore } from '../../ports.js';
import { parseJson, type SqlExecutor } from '../../db/sql.js';
import { CELL_CHUNK, chunk, textArrayParam } from './common.js';

interface RecordRow {
  record: unknown;
}

/**
 * The Data API rejects requests over 64 KB. No single statement may carry more than
 * 60,000 bytes of parameters (room is left for the SQL text and the request envelope).
 * Cells are written in chunks, so an alert's cell count is unlimited; only the record
 * itself (text fields, without cells) must fit, and save() throws a clear error if not.
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

// SQL that reverses packCells (parameter $2) into a text[]; the padding "0" is cut off again.
const UNPACK_CELLS = `CASE WHEN left($2, 4) = 'b64:' THEN
      ARRAY(SELECT substring(encode(substring(b.d FROM n * 8 + 1 FOR 8), 'hex') FROM 2)
              FROM (SELECT decode(substr($2, 5), 'base64') AS d) b,
                   generate_series(0, length(b.d) / 8 - 1) AS n
             ORDER BY n)
    ELSE ARRAY(SELECT jsonb_array_elements_text(substr($2, 5)::jsonb))
    END`;

/** Cells per INSERT: 4,000 packed cells are about 44 KB. Unpacked (test-only) cells use fewer. */
const WRITE_CELL_CHUNK = 4000;
const WRITE_CELL_CHUNK_UNPACKED = 1000;
/** Cells per SELECT page: about 30 bytes each as JSON, so 15,000 stays under the 1 MB response limit. */
const READ_CELL_PAGE = 15_000;

function checkSize(what: string, param: string): void {
  const bytes = Buffer.byteLength(param);
  if (bytes > MAX_SAVE_PARAM_BYTES) {
    throw new Error(
      `${what} is too large for one Data API request: ${bytes} bytes, limit ${MAX_SAVE_PARAM_BYTES}`,
    );
  }
}

export class PostgresAlertStore implements AlertStore {
  constructor(private readonly db: SqlExecutor) {}

  async get(id: string): Promise<Alert | null> {
    return this.readConsistently('SELECT record FROM alerts WHERE id = $1::uuid', [id]);
  }

  async findBySourceId(sourceId: string): Promise<Alert | null> {
    return this.readConsistently(
      `SELECT a.record FROM alert_source_ids s
         JOIN alerts a ON a.id = s.alert_id
        WHERE s.source_id = $1`,
      [sourceId],
    );
  }

  /**
   * Reads one alert's record and its cells inside one transaction. Without it, an update
   * landing between the two reads would mix the old record with new cells (or the reverse),
   * and the alert signature would not match. REPEATABLE READ gives every statement in the
   * transaction the same snapshot of the database, so both reads see the same version.
   * (One SQL statement would also work, but a big alert's cells can exceed the Data API's
   * 1 MB response limit, so the cells are still read in pages.)
   */
  private async readConsistently(recordSql: string, params: unknown[]): Promise<Alert | null> {
    return this.db.transaction(async (tx) => {
      // Must be the first statement of the transaction.
      await tx.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', []);
      const rows = await tx.query<RecordRow>(recordSql, params);
      return rows[0] ? this.withCells(tx, parseJson<Alert>(rows[0].record)) : null;
    });
  }

  async save(alert: Alert): Promise<void> {
    // The record is stored without its cells; they live in alert_cells.
    const record = JSON.stringify({ ...alert, area: { ...alert.area, h3Cells: [] } });
    checkSize(`Alert ${alert.id} (without its cells)`, record);
    const cells = [...new Set(alert.area.h3Cells)];
    const size = cells.every((c) => H3_INDEX.test(c))
      ? WRITE_CELL_CHUNK
      : WRITE_CELL_CHUNK_UNPACKED;
    // Build every chunk first so an oversized one fails before anything is written.
    const chunks = chunk(cells, size).map((part) => packCells(part));
    for (const packed of chunks) checkSize(`A cell chunk of alert ${alert.id}`, packed);

    // All in one transaction: a failure part-way leaves the old alert untouched.
    await this.db.transaction(async (tx) => {
      // Upsert the alert, then register every source id in its chain (one statement).
      await tx.query(
        `WITH p AS (SELECT $1::jsonb AS r),
         upserted AS (
           INSERT INTO alerts (id, source_id, state, expires, record, updated_at)
           SELECT (r ->> 'id')::uuid, r ->> 'sourceId', r ->> 'state',
                  (r ->> 'expires')::timestamptz, r, now()
             FROM p
           ON CONFLICT (id) DO UPDATE SET
             source_id = EXCLUDED.source_id, state = EXCLUDED.state, expires = EXCLUDED.expires,
             record = EXCLUDED.record, updated_at = now()
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
        [record],
      );
      await tx.query('DELETE FROM alert_cells WHERE alert_id = $1::uuid', [alert.id]);
      for (const packed of chunks) {
        await tx.query(
          `INSERT INTO alert_cells (alert_id, h3_cell)
           SELECT $1::uuid, unnest(${UNPACK_CELLS})
           ON CONFLICT DO NOTHING`,
          [alert.id, packed],
        );
      }
    });
  }

  async listActiveForCells(cells: string[], now: Date): Promise<Alert[]> {
    const found = new Map<string, Alert>();
    for (const part of chunk([...new Set(cells)], CELL_CHUNK)) {
      // An alert has ended once now is past the later of expires and ends (GREATEST
      // ignores a null ends). The cell filter is served by the alert_cells index.
      const rows = await this.db.query<RecordRow>(
        `SELECT DISTINCT a.id, a.record FROM alerts a
           JOIN alert_cells c ON c.alert_id = a.id
          WHERE a.state IN ('active', 'updated') AND c.h3_cell = ANY(${textArrayParam(1)})
            AND $2::timestamptz < GREATEST(a.expires, (a.record ->> 'ends')::timestamptz)`,
        [JSON.stringify(part), now.toISOString()],
      );
      for (const row of rows) {
        const alert = parseJson<Alert>(row.record);
        if (!found.has(alert.id)) found.set(alert.id, await this.withCells(this.db, alert));
      }
    }
    return [...found.values()];
  }

  /**
   * Every active or updated alert of one source that has not ended yet, with its cells. Used by
   * feed reconciliation, which compares them with what NWS currently lists.
   */
  async listActiveBySource(source: Alert['source'], now: Date): Promise<Alert[]> {
    const rows = await this.db.query<RecordRow>(
      `SELECT a.id, a.record FROM alerts a
        WHERE a.state IN ('active', 'updated') AND a.record ->> 'source' = $1
          AND $2::timestamptz < GREATEST(a.expires, (a.record ->> 'ends')::timestamptz)`,
      [source, now.toISOString()],
    );
    const out: Alert[] = [];
    for (const row of rows) out.push(await this.withCells(this.db, parseJson<Alert>(row.record)));
    return out;
  }

  /**
   * Cancelled alerts of one source whose end is still in the future, with their cells. Feed
   * restoration checks these against the feed (one that NWS stopped listing, then listed again).
   */
  async listCancelledBySource(source: Alert['source'], now: Date): Promise<Alert[]> {
    const rows = await this.db.query<RecordRow>(
      `SELECT a.id, a.record FROM alerts a
        WHERE a.state = 'cancelled' AND a.record ->> 'source' = $1
          AND $2::timestamptz < GREATEST(a.expires, (a.record ->> 'ends')::timestamptz)`,
      [source, now.toISOString()],
    );
    const out: Alert[] = [];
    for (const row of rows) out.push(await this.withCells(this.db, parseJson<Alert>(row.record)));
    return out;
  }

  /** Put the alert's cells back into the record, reading them in pages (keyset on h3_cell). */
  private async withCells(db: SqlExecutor, alert: Alert): Promise<Alert> {
    const h3Cells: string[] = [];
    let after = '';
    for (;;) {
      const rows = await db.query<{ h3_cell: string }>(
        `SELECT h3_cell FROM alert_cells
          WHERE alert_id = $1::uuid AND h3_cell > $2 ORDER BY h3_cell LIMIT $3::int`,
        [alert.id, after, READ_CELL_PAGE],
      );
      for (const r of rows) h3Cells.push(r.h3_cell);
      const last = rows[rows.length - 1];
      if (!last || rows.length < READ_CELL_PAGE) break;
      after = last.h3_cell;
    }
    // JS sort, not SQL collation order: the alert signature covers h3Cells in this exact order,
    // and SigningAlertStore signs the cells sorted the same way.
    return { ...alert, area: { ...alert.area, h3Cells: h3Cells.sort() } };
  }
}
