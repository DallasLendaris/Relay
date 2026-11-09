import type { Device, DeviceStore } from '../../ports.js';
import type { SqlExecutor } from '../../db/sql.js';
import type { PushPlatform, PushTarget } from '../../scale/contracts.js';
import { isoUtc } from './common.js';

interface DeviceRow {
  id: string;
  credential_hash: string;
  expo_push_token: string | null;
  push_platform: PushPlatform | null;
  push_token: string | null;
  language: 'en' | 'es';
  created_at: string;
}

/** Native target columns for a device; a device with only the legacy Expo token counts as expo. */
function targetColumns(device: Device): [PushPlatform | null, string | null] {
  if (device.pushPlatform !== undefined || device.pushToken !== undefined) {
    return [device.pushPlatform ?? null, device.pushToken ?? null];
  }
  return device.expoPushToken ? ['expo', device.expoPushToken] : [null, null];
}

export class PostgresDeviceStore implements DeviceStore {
  constructor(private readonly db: SqlExecutor) {}

  async create(device: Device): Promise<void> {
    await this.db.query(
      `INSERT INTO devices (id, credential_hash, expo_push_token, push_platform, push_token, language, created_at)
       VALUES ($1::uuid, $2, $3, $4, $5, $6, $7::timestamptz)`,
      [
        device.id,
        device.credentialHash,
        device.expoPushToken,
        ...targetColumns(device),
        device.language,
        device.createdAt,
      ],
    );
  }

  async get(id: string): Promise<Device | null> {
    const rows = await this.db.query<DeviceRow>(
      `SELECT id, credential_hash, expo_push_token, push_platform, push_token, language,
              ${isoUtc('created_at')} AS created_at
         FROM devices WHERE id = $1::uuid`,
      [id],
    );
    const r = rows[0];
    if (!r) return null;
    return {
      id: r.id,
      credentialHash: r.credential_hash,
      expoPushToken: r.expo_push_token,
      pushPlatform: r.push_platform,
      pushToken: r.push_token,
      language: r.language,
      createdAt: r.created_at,
    };
  }

  async update(device: Device): Promise<void> {
    await this.db.query(
      `UPDATE devices SET credential_hash = $2, expo_push_token = $3, push_platform = $4,
              push_token = $5, language = $6
        WHERE id = $1::uuid`,
      [
        device.id,
        device.credentialHash,
        device.expoPushToken,
        ...targetColumns(device),
        device.language,
      ],
    );
  }

  async assignPushToken(deviceId: string, token: string, language: 'en' | 'es'): Promise<void> {
    await this.assignPushTarget(deviceId, { platform: 'expo', token }, language);
  }

  async assignPushTarget(
    deviceId: string,
    target: PushTarget,
    language: 'en' | 'es',
  ): Promise<{ displaced: string[] }> {
    // One statement (Data API runs one per call). The main UPDATE reads `cleared` in a
    // subquery, which forces the other devices' targets to be cleared first, so the unique
    // indexes never see two holders. Expo keeps expo_push_token in step with push_token.
    const rows = await this.db.query<{ id: string; displaced: string | null }>(
      `WITH cleared AS (
         UPDATE devices SET push_platform = NULL, push_token = NULL, expo_push_token = NULL
          WHERE push_platform = $2::text AND push_token = $3::text AND id <> $1::uuid
            AND EXISTS (SELECT 1 FROM devices WHERE id = $1::uuid)
         RETURNING id
       )
       UPDATE devices SET push_platform = $2::text, push_token = $3::text,
              expo_push_token = CASE WHEN $2::text = 'expo' THEN $3::text ELSE NULL END,
              language = $4
        WHERE id = $1::uuid AND (SELECT count(*) FROM cleared) >= 0
       RETURNING id, (SELECT string_agg(id::text, ',') FROM cleared) AS displaced`,
      [deviceId, target.platform, target.token, language],
    );
    const row = rows[0];
    if (!row) throw new Error(`device ${deviceId} not found`);
    return { displaced: row.displaced ? row.displaced.split(',') : [] };
  }

  async clearPushToken(token: string): Promise<void> {
    // Two index-friendly lookups (devices_expo_push_token_key and devices_push_token_idx) joined
    // by UNION inside one statement, so it is atomic and never scans the table. A single
    // `expo_push_token = $1 OR push_token = $1` predicate cannot use both indexes cheaply.
    await this.db.query(
      `WITH hit AS (
         SELECT id FROM devices WHERE expo_push_token = $1::text
         UNION
         SELECT id FROM devices WHERE push_token = $1::text
       )
       UPDATE devices SET expo_push_token = NULL, push_platform = NULL, push_token = NULL
        WHERE id IN (SELECT id FROM hit)`,
      [token],
    );
  }
}
