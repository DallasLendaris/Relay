import type { Alert } from '@relay/shared';
import type { AlertStore } from '../../ports.js';
import { parseJson, type SqlExecutor } from '../../db/sql.js';
import { CELL_CHUNK, chunk, textArrayParam } from './common.js';

interface RecordRow {
  record: unknown;
}

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
    // One statement (Data API runs one per call): upsert the alert, then register every
    // source id in its chain. The chain's ids now point at this alert.
    const chainIds = [alert.sourceId, ...alert.references.map((r) => r.sourceId)];
    await this.db.query(
      `WITH upserted AS (
         INSERT INTO alerts (id, source_id, state, expires, h3_cells, record, updated_at)
         VALUES ($1::uuid, $2, $3, $4::timestamptz, ${textArrayParam(5)}, $6::jsonb, now())
         ON CONFLICT (id) DO UPDATE SET
           source_id = EXCLUDED.source_id, state = EXCLUDED.state, expires = EXCLUDED.expires,
           h3_cells = EXCLUDED.h3_cells, record = EXCLUDED.record, updated_at = now()
         RETURNING id
       )
       INSERT INTO alert_source_ids (source_id, alert_id)
       SELECT s, $1::uuid FROM jsonb_array_elements_text($7::jsonb) AS s
       ON CONFLICT (source_id) DO UPDATE SET alert_id = EXCLUDED.alert_id`,
      [
        alert.id,
        alert.sourceId,
        alert.state,
        alert.expires,
        JSON.stringify(alert.area.h3Cells),
        JSON.stringify(alert),
        JSON.stringify(chainIds),
      ],
    );
  }

  async listActiveForCells(cells: string[]): Promise<Alert[]> {
    const found = new Map<string, Alert>();
    for (const part of chunk([...new Set(cells)], CELL_CHUNK)) {
      // && is array overlap, served by the GIN index.
      const rows = await this.db.query<RecordRow>(
        `SELECT record FROM alerts
          WHERE state IN ('active', 'updated') AND h3_cells && ${textArrayParam(1)}`,
        [JSON.stringify(part)],
      );
      for (const row of rows) {
        const alert = parseJson<Alert>(row.record);
        found.set(alert.id, alert);
      }
    }
    return [...found.values()];
  }
}
