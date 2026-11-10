import type { Clock, Device, DeviceErasure, DeviceStore } from '../../ports.js';
import type { SqlExecutor } from '../../db/sql.js';
import type { PushPlatform, PushTarget } from '../../scale/contracts.js';
import {
  PushTokenInUseError,
  pushTargetHash,
  type PushTargetMeta,
} from '../../scale/latejoin/push-target-rules.js';
import { isoUtc } from './common.js';

interface DeviceRow {
  id: string;
  credential_hash: string;
  expo_push_token: string | null;
  push_platform: PushPlatform | null;
  push_token: string | null;
  push_project: string | null;
  language: 'en' | 'es';
  created_at: string;
}

/** Native target columns for a device; a device with only the legacy Expo token counts as expo. */
function targetColumns(device: Device): [PushPlatform | null, string | null, string | null] {
  if (device.pushPlatform !== undefined || device.pushToken !== undefined) {
    const platform = device.pushPlatform ?? null;
    return [
      platform,
      device.pushToken ?? null,
      platform === 'android' ? (device.pushProject ?? null) : null,
    ];
  }
  return device.expoPushToken ? ['expo', device.expoPushToken, null] : [null, null, null];
}

export class PostgresDeviceStore implements DeviceStore, DeviceErasure {
  constructor(
    private readonly db: SqlExecutor,
    /** Used for the push-target move rules; tests inject a fixed clock. */
    private readonly clock: Clock = { now: () => new Date() },
  ) {}

  async create(device: Device): Promise<void> {
    await this.db.query(
      `INSERT INTO devices (id, credential_hash, expo_push_token, push_platform, push_token, push_project, language, created_at)
       VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8::timestamptz)`,
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
      `SELECT id, credential_hash, expo_push_token, push_platform, push_token, push_project, language,
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
      pushProject: r.push_project,
      language: r.language,
      createdAt: r.created_at,
    };
  }

  async update(device: Device): Promise<void> {
    await this.db.query(
      `UPDATE devices SET credential_hash = $2, expo_push_token = $3, push_platform = $4,
              push_token = $5, push_project = $6, language = $7
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
    opts?: { outbox?: boolean },
  ): Promise<{ displaced: string[]; outboxIds?: Record<string, string> }> {
    if (opts?.outbox) {
      // The target change and the outbox rows commit together or not at all.
      return this.db.transaction(async (tx) => {
        const result = await new PostgresDeviceStore(tx, this.clock).assignPushTarget(
          deviceId,
          target,
          language,
        );
        // Only devices with places have cell-index entries to update.
        const ids = [deviceId, ...result.displaced];
        const rows = await tx.query<{ id: string; device_id: string }>(
          `INSERT INTO cell_index_outbox (device_id, op, payload)
           SELECT d::uuid, 'update', '{"h3Cell":"","placeId":""}'::jsonb
             FROM jsonb_array_elements_text($1::jsonb) AS d
            WHERE EXISTS (SELECT 1 FROM places WHERE device_id = d::uuid)
           RETURNING id, device_id`,
          [JSON.stringify(ids)],
        );
        return {
          ...result,
          outboxIds: Object.fromEntries(rows.map((r) => [r.device_id, r.id])),
        };
      });
    }
    // One statement (Data API runs one per call). The main UPDATE reads `cleared` in a
    // subquery, which forces the other devices' targets to be cleared first, so the unique
    // indexes never see two holders. Expo keeps expo_push_token in step with push_token.
    //
    // Move rules (push-target-rules.ts): taking a token from another device is allowed only when
    // that device was inactive for 24 h, or fewer than 2 moves of this token happened in the last
    // 24 h. `verdict` decides; when it says no, nothing is written and the row comes back with
    // allowed = false. A successful move is recorded in push_token_moves (token hash only), and
    // rows older than the window are pruned in the same statement.
    const rows = await this.db.query<{
      found: number | string;
      allowed: boolean;
      displaced: string | null;
    }>(
      `WITH me AS (SELECT id FROM devices WHERE id = $1::uuid),
       holders AS (
         SELECT id, COALESCE(push_target_updated_at, created_at) AS last_update
           FROM devices
          WHERE push_platform = $2::text AND push_token = $3::text AND id <> $1::uuid
       ),
       recent AS (
         SELECT count(*) AS n FROM push_token_moves
          WHERE token_hash = $5::text AND moved_at > $6::timestamptz - interval '24 hours'
       ),
       verdict AS (
         SELECT (
           NOT EXISTS (SELECT 1 FROM holders)
           OR (SELECT n FROM recent) < 2
           OR NOT EXISTS (
                SELECT 1 FROM holders WHERE last_update > $6::timestamptz - interval '24 hours')
         ) AS allowed
       ),
       cleared AS (
         UPDATE devices SET push_platform = NULL, push_token = NULL, push_project = NULL,
                expo_push_token = NULL
          WHERE push_platform = $2::text AND push_token = $3::text AND id <> $1::uuid
            AND EXISTS (SELECT 1 FROM me) AND (SELECT allowed FROM verdict)
         RETURNING id
       ),
       moved AS (
         INSERT INTO push_token_moves (token_hash, device_id, moved_at)
         SELECT $5::text, $1::uuid, $6::timestamptz WHERE EXISTS (SELECT 1 FROM cleared)
         RETURNING 1
       ),
       pruned AS (
         DELETE FROM push_token_moves
          WHERE token_hash = $5::text AND moved_at <= $6::timestamptz - interval '24 hours'
         RETURNING 1
       ),
       updated AS (
         UPDATE devices SET push_platform = $2::text, push_token = $3::text,
                push_project = CASE WHEN $2::text = 'android' THEN $7::text ELSE NULL END,
                expo_push_token = CASE WHEN $2::text = 'expo' THEN $3::text ELSE NULL END,
                language = $4,
                push_target_updated_at = $6::timestamptz,
                push_target_changed_at = CASE
                  WHEN push_platform IS NOT DISTINCT FROM $2::text
                   AND push_token IS NOT DISTINCT FROM $3::text
                  THEN COALESCE(push_target_changed_at, $6::timestamptz)
                  ELSE $6::timestamptz END
          WHERE id = $1::uuid AND (SELECT allowed FROM verdict)
            AND (SELECT count(*) FROM cleared) >= 0 AND (SELECT count(*) FROM moved) >= 0
            AND (SELECT count(*) FROM pruned) >= 0
         RETURNING id
       )
       SELECT (SELECT count(*) FROM me) AS found, (SELECT allowed FROM verdict) AS allowed,
              (SELECT string_agg(id::text, ',') FROM cleared) AS displaced`,
      [
        deviceId,
        target.platform,
        target.token,
        language,
        pushTargetHash(target),
        this.clock.now().toISOString(),
        target.platform === 'android' ? (target.project ?? null) : null,
      ],
    );
    const row = rows[0];
    if (!row || Number(row.found) === 0) throw new Error(`device ${deviceId} not found`);
    if (!row.allowed) throw new PushTokenInUseError();
    return { displaced: row.displaced ? row.displaced.split(',') : [] };
  }

  /** When the device's push target last changed (see PushTargetMeta), or null for no such device. */
  async pushTargetMeta(deviceId: string): Promise<PushTargetMeta | null> {
    const rows = await this.db.query<{ updated_at: string; changed_at: string }>(
      `SELECT ${isoUtc('COALESCE(push_target_updated_at, created_at)')} AS updated_at,
              ${isoUtc('COALESCE(push_target_changed_at, created_at)')} AS changed_at
         FROM devices WHERE id = $1::uuid`,
      [deviceId],
    );
    const r = rows[0];
    return r ? { updatedAt: new Date(r.updated_at), changedAt: new Date(r.changed_at) } : null;
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
       UPDATE devices SET expo_push_token = NULL, push_platform = NULL, push_token = NULL,
              push_project = NULL
        WHERE id IN (SELECT id FROM hit)`,
      [token],
    );
  }

  async deleteDevice(deviceId: string, deletedAt: Date): Promise<boolean> {
    // One statement (the Data API runs one per call): the tombstone and the delete commit
    // together, so there is never a deleted device without a tombstone. The device's places go
    // with it (places.device_id is ON DELETE CASCADE). Data-modifying CTEs always run to the end.
    const rows = await this.db.query<{ deleted: number }>(
      `WITH tomb AS (
         INSERT INTO deleted_devices (device_id, deleted_at) VALUES ($1::uuid, $2::timestamptz)
         ON CONFLICT (device_id) DO NOTHING
         RETURNING device_id
       ), gone AS (
         DELETE FROM devices WHERE id = $1::uuid RETURNING id
       )
       SELECT (SELECT count(*) FROM gone)::int AS deleted, (SELECT count(*) FROM tomb) AS tombstoned`,
      [deviceId, deletedAt.toISOString()],
    );
    return (rows[0]?.deleted ?? 0) > 0;
  }

  async listDeletedDevices(): Promise<string[]> {
    const rows = await this.db.query<{ device_id: string }>(
      'SELECT device_id FROM deleted_devices',
      [],
    );
    return rows.map((r) => r.device_id);
  }
}
