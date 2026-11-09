import { beforeEach, describe, expect, it } from 'vitest';
import { MemoryDeviceStore } from '../../src/adapters/memory/device-store.js';
import { PostgresDeviceStore } from '../../src/adapters/postgres/device-store.js';
import type { DeviceStore } from '../../src/ports.js';
import { DEVICE_ID, device, freshDb, type PgliteExecutor } from './helpers.js';

const OTHER = '99999999-9999-4999-8999-999999999999';
const APNS = 'ab12'.repeat(16);

/** The same expectations for the memory and Postgres stores. */
function suite(name: string, make: () => Promise<{ store: DeviceStore; db?: PgliteExecutor }>) {
  describe(`push targets: ${name}`, () => {
    let store: DeviceStore;
    let db: PgliteExecutor | undefined;
    beforeEach(async () => {
      ({ store, db } = await make());
      await store.create(device({ expoPushToken: null }));
      await store.create(
        device({ id: OTHER, credentialHash: 'b'.repeat(64), expoPushToken: null }),
      );
    });

    it('stores native targets and moves them, reporting who lost it', async () => {
      expect(
        await store.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'es'),
      ).toEqual({
        displaced: [],
      });
      expect(await store.get(DEVICE_ID)).toMatchObject({
        pushPlatform: 'ios',
        pushToken: APNS,
        expoPushToken: null,
        language: 'es',
      });
      expect(await store.assignPushTarget(OTHER, { platform: 'ios', token: APNS }, 'en')).toEqual({
        displaced: [DEVICE_ID],
      });
      expect(await store.get(DEVICE_ID)).toMatchObject({ pushPlatform: null, pushToken: null });
      expect(await store.get(OTHER)).toMatchObject({ pushPlatform: 'ios', pushToken: APNS });
    });

    it('keeps expoPushToken and pushToken equal for platform expo, and clears expo when switching', async () => {
      await store.assignPushTarget(
        DEVICE_ID,
        { platform: 'expo', token: 'ExponentPushToken[x]' },
        'en',
      );
      expect(await store.get(DEVICE_ID)).toMatchObject({
        expoPushToken: 'ExponentPushToken[x]',
        pushPlatform: 'expo',
        pushToken: 'ExponentPushToken[x]',
      });
      await store.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
      expect(await store.get(DEVICE_ID)).toMatchObject({ expoPushToken: null, pushToken: APNS });
    });

    it('assignPushToken (legacy) still works and moves an Expo token', async () => {
      await store.assignPushToken(DEVICE_ID, 'ExponentPushToken[x]', 'en');
      await store.assignPushToken(OTHER, 'ExponentPushToken[x]', 'en');
      expect(await store.get(DEVICE_ID)).toMatchObject({ expoPushToken: null, pushToken: null });
      expect(await store.get(OTHER)).toMatchObject({
        expoPushToken: 'ExponentPushToken[x]',
        pushPlatform: 'expo',
      });
    });

    it('clearPushToken clears native and Expo tokens', async () => {
      await store.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
      await store.assignPushTarget(
        OTHER,
        { platform: 'expo', token: 'ExponentPushToken[y]' },
        'en',
      );
      await store.clearPushToken(APNS);
      await store.clearPushToken('ExponentPushToken[y]');
      expect(await store.get(DEVICE_ID)).toMatchObject({ pushPlatform: null, pushToken: null });
      expect(await store.get(OTHER)).toMatchObject({ expoPushToken: null, pushToken: null });
    });

    it('rejects an unknown device', async () => {
      await expect(
        store.assignPushTarget(
          '88888888-8888-4888-8888-888888888888',
          { platform: 'ios', token: APNS },
          'en',
        ),
      ).rejects.toThrow(/not found/);
    });

    it('a device created with only the legacy Expo token gets the matching native target', async () => {
      const s = (await make()).store;
      await s.create(device({ id: OTHER, expoPushToken: 'ExponentPushToken[legacy]' }));
      expect(await s.get(OTHER)).toMatchObject({
        pushPlatform: 'expo',
        pushToken: 'ExponentPushToken[legacy]',
      });
    });

    if (name === 'Postgres') {
      it('the unique index refuses a second holder, and the CHECKs refuse half a target', async () => {
        await store.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
        await expect(
          db?.query(
            "UPDATE devices SET push_platform = 'ios', push_token = $1 WHERE id = $2::uuid",
            [APNS, OTHER],
          ) ?? Promise.reject(new Error('no db')),
        ).rejects.toThrow(/devices_push_target_key|unique/);
        await expect(
          db?.query("UPDATE devices SET push_platform = 'ios' WHERE id = $1::uuid", [OTHER]) ??
            Promise.reject(new Error('no db')),
        ).rejects.toThrow(/check/i);
        await expect(
          db?.query(
            "UPDATE devices SET push_platform = 'windows', push_token = 'x' WHERE id = $1::uuid",
            [OTHER],
          ) ?? Promise.reject(new Error('no db')),
        ).rejects.toThrow(/check/i);
      });

      it('allows the same token on different platforms and many null targets', async () => {
        await store.assignPushTarget(DEVICE_ID, { platform: 'ios', token: APNS }, 'en');
        await store.assignPushTarget(OTHER, { platform: 'android', token: APNS }, 'en');
        expect((await store.get(DEVICE_ID))?.pushToken).toBe(APNS);
      });
    }
  });
}

suite('Memory', async () => ({ store: new MemoryDeviceStore() }));
suite('Postgres', async () => {
  const db = await freshDb();
  return { store: new PostgresDeviceStore(db), db };
});
