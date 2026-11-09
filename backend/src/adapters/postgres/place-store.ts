import type { Place, PlaceStore } from '../../ports.js';
import type { SqlExecutor } from '../../db/sql.js';
import { CELL_CHUNK, chunk, isoUtc, textArrayParam } from './common.js';

interface PlaceRow {
  id: string;
  device_id: string;
  label: string;
  short_address: string;
  latitude: number;
  longitude: number;
  h3_cell: string;
  created_at: string;
}

const COLUMNS = `id, device_id, label, short_address, latitude, longitude, h3_cell,
  ${isoUtc('created_at')} AS created_at`;

const toPlace = (r: PlaceRow): Place => ({
  id: r.id,
  deviceId: r.device_id,
  label: r.label,
  shortAddress: r.short_address,
  latitude: r.latitude,
  longitude: r.longitude,
  h3Cell: r.h3_cell,
  createdAt: r.created_at,
});

const NIL_UUID = '00000000-0000-0000-0000-000000000000';

export class PostgresPlaceStore implements PlaceStore {
  constructor(
    private readonly db: SqlExecutor,
    /** Rows per page in listMatchesForCells (about 100 bytes each; 5000 stays far under 1 MB). */
    private readonly pageSize = 5000,
  ) {}

  async add(place: Place): Promise<void> {
    await this.db.query(
      `INSERT INTO places (id, device_id, label, short_address, latitude, longitude, h3_cell, created_at)
       VALUES ($1::uuid, $2::uuid, $3, $4, $5::double precision, $6::double precision, $7, $8::timestamptz)`,
      [
        place.id,
        place.deviceId,
        place.label,
        place.shortAddress,
        place.latitude,
        place.longitude,
        place.h3Cell,
        place.createdAt,
      ],
    );
  }

  async listForDevice(deviceId: string): Promise<Place[]> {
    const rows = await this.db.query<PlaceRow>(
      `SELECT ${COLUMNS} FROM places WHERE device_id = $1::uuid ORDER BY created_at, id`,
      [deviceId],
    );
    return rows.map(toPlace);
  }

  async remove(deviceId: string, placeId: string): Promise<boolean> {
    const rows = await this.db.query(
      'DELETE FROM places WHERE id = $1::uuid AND device_id = $2::uuid RETURNING id',
      [placeId, deviceId],
    );
    return rows.length > 0;
  }

  async listForCells(cells: string[]): Promise<Place[]> {
    const found = new Map<string, Place>();
    for (const part of chunk([...new Set(cells)], CELL_CHUNK)) {
      const rows = await this.db.query<PlaceRow>(
        `SELECT ${COLUMNS} FROM places WHERE h3_cell = ANY(${textArrayParam(1)})`,
        [JSON.stringify(part)],
      );
      for (const row of rows) found.set(row.id, toPlace(row));
    }
    return [...found.values()];
  }

  async listMatchesForCells(
    cells: string[],
  ): Promise<Array<{ placeId: string; deviceId: string; h3Cell: string }>> {
    const out: Array<{ placeId: string; deviceId: string; h3Cell: string }> = [];
    for (const part of chunk([...new Set(cells)], CELL_CHUNK)) {
      // Keyset pagination: each page asks for ids after the last one seen, so no single
      // response passes the Data API's 1 MB limit however many places match.
      let after = NIL_UUID;
      for (;;) {
        const rows = await this.db.query<{ id: string; device_id: string; h3_cell: string }>(
          `SELECT id, device_id, h3_cell FROM places
            WHERE h3_cell = ANY(${textArrayParam(1)}) AND id > $2::uuid
            ORDER BY id LIMIT $3::int`,
          [JSON.stringify(part), after, this.pageSize],
        );
        for (const r of rows) out.push({ placeId: r.id, deviceId: r.device_id, h3Cell: r.h3_cell });
        const last = rows[rows.length - 1];
        if (!last || rows.length < this.pageSize) break;
        after = last.id;
      }
    }
    return out;
  }
}
