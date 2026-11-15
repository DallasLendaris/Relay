import { beforeEach, describe, expect, it } from 'vitest';
import { MemoryDeviceStore } from '../../../src/adapters/memory/device-store.js';
import { PostgresDeviceStore } from '../../../src/adapters/postgres/device-store.js';
import type { DeviceStore } from '../../../src/ports.js';
import {
  INACTIVE_MS,
  MAX_MOVES_PER_WINDOW,
  PushTokenInUseError,
  moveAllowed,
  pushTargetHash,
  type PushTargetMeta,
} from '../../../src/scale/latejoin/index.js';
import { device, freshDb } from '../../db/helpers.js';

const APNS = 'ab12'.repeat(16);
const TARGET = { platform: 'ios', token: APNS } as const;
const HOUR = 60 * 60 * 1000;
const id = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;

describe('moveAllowed (the rule)', () => {
  const now = Date.parse('2026-11-09T12:00:00Z');
  it('allows a token nobody holds', () => {
    expect(moveAllowed({ now, holdersLastUpdate: [], recentMoves: 99 })).toBe(true);
  });
  it('allows while fewer than the allowed moves happened in the window', () => {
    for (let n = 0; n < MAX_MOVES_PER_WINDOW; n++) {
      expect(moveAllowed({ now, holdersLastUpdate: [now], recentMoves: n })).toBe(true);
    }
    expect(moveAllowed({ now, holdersLastUpdate: [now], recentMoves: MAX_MOVES_PER_WINDOW })).toBe(
      false,
    );
  });
  it('allows after the holder was inactive for 24 hours, even with many recent moves', () => {
    expect(moveAllowed({ now, holdersLastUpdate: [now - INACTIVE_MS], recentMoves: 50 })).toBe(
      true,
    );
    expect(moveAllowed({ now, holdersLastUpdate: [now - INACTIVE_MS + 1], recentMoves: 50 })).toBe(
      false,
    );
  });
  it('hashes platform and token, never exposing the token', () => {
    const h = pushTargetHash(TARGET);
    expect(h).toMatch(/^[0-9a-f]{64}$/);
    expect(h).not.toContain(APNS);
    expect(pushTargetHash({ platform: 'android', token: APNS })).not.toBe(h);
  });
});

/** The same expectations for the memory and Postgres stores, on a movable clock. */
function suite(
  name: string,
  make: (clock: { now: () => Date }) => Promise<DeviceStore & PushMeta>,
) {
  describe(`push-target moves: ${name}`, () => {
    const time = { now: new Date('2026-11-09T12:00:00Z') };
    let store: DeviceStore & PushMeta;
    beforeEach(async () => {
      time.now = new Date('2026-11-09T12:00:00Z');
      store = await make({ now: () => time.now });
      for (let n = 1; n <= 6; n++) {
        await store.create(
          device({
            id: id(n),
            credentialHash: String(n).repeat(64).slice(0, 64),
            expoPushToken: null,
            createdAt: time.now.toISOString(),
          }),
        );
      }
    });
    const advance = (ms: number) => {
      time.now = new Date(time.now.getTime() + ms);
    };

    it('allows the first move and the second, refuses the third', async () => {
      await store.assignPushTarget(id(1), TARGET, 'en'); // free token: not a move
      expect(await store.assignPushTarget(id(2), TARGET, 'en')).toEqual({ displaced: [id(1)] });
      advance(HOUR);
      expect(await store.assignPushTarget(id(3), TARGET, 'en')).toEqual({ displaced: [id(2)] });
      advance(HOUR);
      await expect(store.assignPushTarget(id(4), TARGET, 'en')).rejects.toBeInstanceOf(
        PushTokenInUseError,
      );
      // Nothing changed: the holder keeps the token, the refused device has none.
      expect(await store.get(id(3))).toMatchObject({ pushPlatform: 'ios', pushToken: APNS });
      expect(await store.get(id(4))).toMatchObject({ pushPlatform: null, pushToken: null });
    });

    it('allows a move when the holder has been inactive for 24 hours', async () => {
      await store.assignPushTarget(id(1), TARGET, 'en');
      await store.assignPushTarget(id(2), TARGET, 'en');
      await store.assignPushTarget(id(3), TARGET, 'en');
      await expect(store.assignPushTarget(id(4), TARGET, 'en')).rejects.toBeInstanceOf(
        PushTokenInUseError,
      );
      advance(24 * HOUR - 1000);
      await expect(store.assignPushTarget(id(4), TARGET, 'en')).rejects.toBeInstanceOf(
        PushTokenInUseError,
      );
      advance(2000);
      expect(await store.assignPushTarget(id(4), TARGET, 'en')).toEqual({ displaced: [id(3)] });
    });

    it('a holder refreshing its own token never counts as a move', async () => {
      await store.assignPushTarget(id(1), TARGET, 'en');
      for (let i = 0; i < 5; i++) await store.assignPushTarget(id(1), TARGET, 'es');
      // Still the full allowance: two moves, then a refusal.
      await store.assignPushTarget(id(2), TARGET, 'en');
      await store.assignPushTarget(id(3), TARGET, 'en');
      await expect(store.assignPushTarget(id(4), TARGET, 'en')).rejects.toBeInstanceOf(
        PushTokenInUseError,
      );
    });

    it('moves age out of the 24 hour window', async () => {
      await store.assignPushTarget(id(1), TARGET, 'en');
      await store.assignPushTarget(id(2), TARGET, 'en'); // move 1 at t0
      advance(12 * HOUR);
      await store.assignPushTarget(id(3), TARGET, 'en'); // move 2 at t0+12h
      advance(13 * HOUR); // move 1 is now 25 h old; the holder was updated 13 h ago
      expect(await store.assignPushTarget(id(4), TARGET, 'en')).toEqual({ displaced: [id(3)] });
    });

    it('different tokens do not share a budget', async () => {
      const other = { platform: 'ios', token: 'cd34'.repeat(16) } as const;
      await store.assignPushTarget(id(1), TARGET, 'en');
      await store.assignPushTarget(id(2), TARGET, 'en');
      await store.assignPushTarget(id(3), TARGET, 'en');
      await store.assignPushTarget(id(4), other, 'en');
      expect(await store.assignPushTarget(id(5), other, 'en')).toEqual({ displaced: [id(4)] });
    });

    it('still reports an unknown device as an error, not a refusal', async () => {
      await expect(store.assignPushTarget(id(99), TARGET, 'en')).rejects.toThrow('not found');
    });

    it('tracks when the target last changed: a refresh to the same value keeps changedAt', async () => {
      await store.assignPushTarget(id(1), TARGET, 'en');
      const first = (await store.pushTargetMeta(id(1))) as PushTargetMeta;
      advance(HOUR);
      await store.assignPushTarget(id(1), TARGET, 'es');
      const same = (await store.pushTargetMeta(id(1))) as PushTargetMeta;
      expect(same.changedAt.getTime()).toBe(first.changedAt.getTime());
      expect(same.updatedAt.getTime()).toBe(first.updatedAt.getTime() + HOUR);
      advance(HOUR);
      await store.assignPushTarget(id(1), { platform: 'ios', token: 'cd34'.repeat(16) }, 'es');
      const changed = (await store.pushTargetMeta(id(1))) as PushTargetMeta;
      expect(changed.changedAt.getTime()).toBe(time.now.getTime());
    });
  });
}

type PushMeta = { pushTargetMeta(id: string): Promise<PushTargetMeta | null> };

suite('memory', async (clock) => new MemoryDeviceStore(clock));
suite('postgres', async (clock) => new PostgresDeviceStore(await freshDb(), clock));

describe('push_token_moves table (Postgres)', () => {
  it('records the token hash, never the token, and prunes rows older than the window', async () => {
    const db = await freshDb();
    const time = { now: new Date('2026-11-09T12:00:00Z') };
    const store = new PostgresDeviceStore(db, { now: () => time.now });
    for (let n = 1; n <= 3; n++) {
      await store.create(
        device({ id: id(n), credentialHash: String(n).repeat(64), expoPushToken: null }),
      );
    }
    await store.assignPushTarget(id(1), TARGET, 'en');
    await store.assignPushTarget(id(2), TARGET, 'en');
    const rows = await db.query<{ token_hash: string; device_id: string }>(
      'SELECT token_hash, device_id FROM push_token_moves',
      [],
    );
    expect(rows).toEqual([{ token_hash: pushTargetHash(TARGET), device_id: id(2) }]);
    expect(JSON.stringify(rows)).not.toContain(APNS);

    time.now = new Date(time.now.getTime() + 25 * HOUR);
    await store.assignPushTarget(id(3), TARGET, 'en'); // the old row is pruned by this statement
    const after = await db.query<{ device_id: string }>(
      'SELECT device_id FROM push_token_moves',
      [],
    );
    expect(after.map((r) => r.device_id)).toEqual([id(3)]);
  });
});
