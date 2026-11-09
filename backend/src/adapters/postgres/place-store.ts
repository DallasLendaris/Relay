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

export class PostgresPlaceStore implements PlaceStore {
  constructor(private readonly db: SqlExecutor) {}

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
}
