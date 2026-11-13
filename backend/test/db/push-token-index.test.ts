import { describe, expect, it } from 'vitest';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import { DEVICE_ID, device, freshDb } from './helpers.js';

const OTHER = '99999999-9999-4999-8999-999999999999';
const APNS = 'ab12'.repeat(16);

describe('devices push token indexes (migration 0007)', () => {
  it('both token columns have an index that leads with the token', async () => {
    const db = await freshDb();
    const rows = await db.query<{ indexdef: string }>(
      `SELECT indexdef FROM pg_indexes WHERE tablename = 'devices'`,
      [],
    );
    const defs = rows.map((r) => r.indexdef);
    expect(defs.some((d) => d.includes('(push_token)'))).toBe(true);
    expect(defs.some((d) => d.includes('(expo_push_token)'))).toBe(true);
  });

  it('the clearPushToken lookups use an index, not a sequential scan', async () => {
    const db = await freshDb();
    await db.query(
      `INSERT INTO devices (id, credential_hash, expo_push_token, push_platform, push_token, language, created_at)
       SELECT gen_random_uuid(), 'h' || g, CASE WHEN g % 2 = 0 THEN 'tok' || g END,
              CASE WHEN g % 2 = 0 THEN 'expo' ELSE 'ios' END, 'tok' || g, 'en', now()
         FROM generate_series(1, 2000) g`,
      [],
    );
    await db.query('ANALYZE devices', []);
    // Sequential scans are disabled, so only a missing index could still produce one.
    await db.pg.exec('SET enable_seqscan = off');
    for (const col of ['push_token', 'expo_push_token']) {
      const plan = await db.query<{ 'QUERY PLAN': string }>(
        `EXPLAIN SELECT id FROM devices WHERE ${col} = $1::text`,
        ['tok78'],
      );
      const text = plan.map((r) => r['QUERY PLAN']).join('\n');
      expect(text).toMatch(/Index/);
      expect(text).not.toMatch(/Seq Scan/);
    }
  });

  it('clearPushToken clears the device holding the token as push_token or expo_push_token', async () => {
    const db = await freshDb();
    const store = new PostgresDeviceStore(db);
    await store.create(device({ expoPushToken: null }));
    await store.create(device({ id: OTHER, credentialHash: 'b'.repeat(64), expoPushToken: null }));
    await store.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
    await store.assignPushTarget(OTHER, { platform: 'expo', token: 'ExponentPushToken[z]' }, 'en');
    await store.clearPushToken('ExponentPushToken[z]');
    expect(await store.get(OTHER)).toMatchObject({ pushToken: null, expoPushToken: null });
    expect(await store.get(DEVICE_ID)).toMatchObject({ pushToken: APNS });
    await store.clearPushToken(APNS);
    expect(await store.get(DEVICE_ID)).toMatchObject({ pushToken: null, pushPlatform: null });
  });
});
