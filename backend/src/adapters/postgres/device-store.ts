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
}
