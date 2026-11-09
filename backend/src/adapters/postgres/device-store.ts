import type { Device, DeviceStore } from '../../ports.js';
import type { SqlExecutor } from '../../db/sql.js';
import { isoUtc } from './common.js';

interface DeviceRow {
  id: string;
  credential_hash: string;
  expo_push_token: string | null;
  language: 'en' | 'es';
  created_at: string;
}

export class PostgresDeviceStore implements DeviceStore {
  constructor(private readonly db: SqlExecutor) {}

  async create(device: Device): Promise<void> {
    await this.db.query(
      `INSERT INTO devices (id, credential_hash, expo_push_token, language, created_at)
       VALUES ($1::uuid, $2, $3, $4, $5::timestamptz)`,
      [device.id, device.credentialHash, device.expoPushToken, device.language, device.createdAt],
    );
  }

  async get(id: string): Promise<Device | null> {
    const rows = await this.db.query<DeviceRow>(
      `SELECT id, credential_hash, expo_push_token, language, ${isoUtc('created_at')} AS created_at
         FROM devices WHERE id = $1::uuid`,
      [id],
    );
    const r = rows[0];
    if (!r) return null;
    return {
      id: r.id,
      credentialHash: r.credential_hash,
      expoPushToken: r.expo_push_token,
      language: r.language,
      createdAt: r.created_at,
    };
  }

  async update(device: Device): Promise<void> {
    await this.db.query(
      `UPDATE devices SET credential_hash = $2, expo_push_token = $3, language = $4
        WHERE id = $1::uuid`,
      [device.id, device.credentialHash, device.expoPushToken, device.language],
    );
  }

  async assignPushToken(deviceId: string, token: string, language: 'en' | 'es'): Promise<void> {
    // One statement (Data API runs one per call). The main UPDATE reads `cleared` in a
    // subquery, which forces the other devices' tokens to be cleared first, so the
    // unique index on expo_push_token never sees two holders.
    const rows = await this.db.query(
      `WITH cleared AS (
         UPDATE devices SET expo_push_token = NULL
          WHERE expo_push_token = $2 AND id <> $1::uuid
            AND EXISTS (SELECT 1 FROM devices WHERE id = $1::uuid)
         RETURNING id
       )
       UPDATE devices SET expo_push_token = $2, language = $3
        WHERE id = $1::uuid AND (SELECT count(*) FROM cleared) >= 0
       RETURNING id`,
      [deviceId, token, language],
    );
    if (rows.length === 0) throw new Error(`device ${deviceId} not found`);
  }

  async clearPushToken(token: string): Promise<void> {
    await this.db.query('UPDATE devices SET expo_push_token = NULL WHERE expo_push_token = $1', [
      token,
    ]);
  }
}
