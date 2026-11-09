import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { MAX_MOVES_PER_WINDOW, processLateJoin } from '../../../src/scale/latejoin/index.js';
import { APNS, SJ, SJ_CELL, harness, makeAlert } from './helpers.js';

/**
 * Regression for SEC-A-1 / SEC-B-1 (probes sec-a/probe2.mts and sec-b/p3.mts): one phone creates
 * many anonymous devices, gives each a place in a cell it is not in, moves its one push token
 * onto each, and checks in with the token each late-join push carries. Before the fix all 25
 * check-ins counted; now the cell total cannot exceed one per physical push target.
 */
describe('one phone cannot inflate a cell by minting device identities', () => {
  const MINTS = 25;
  /** The first device takes the free token, then at most MAX_MOVES_PER_WINDOW moves. */
  const ALLOWED_HOLDERS = 1 + MAX_MOVES_PER_WINDOW;

  async function attack(h: ReturnType<typeof harness>) {
    const alert = makeAlert();
    await h.alerts.save(alert);
    const devices: { deviceId: string; credential: string; token?: string }[] = [];
    const statuses: number[] = [];
    for (let i = 0; i < MINTS; i++) {
      const d = await h.register();
      await h.addPlace(d.credential, { ...SJ }); // a "home" with no proof of presence
      const before = h.sendQueue.batches('urgent').length;
      statuses.push((await h.setToken(d.credential)).status); // the same real token every time
      await h.drain();
      const mine = h.sendQueue
        .batches('urgent')
        .slice(before)
        .find((b) => b.recipients[0]?.deviceId === d.deviceId);
      devices.push({ ...d, ...(mine ? { token: mine.recipients[0]?.checkInToken } : {}) });
    }
    return { alert, devices, statuses };
  }

  it('limits distinct check-in tokens, and the cell total never exceeds that', async () => {
    const h = harness();
    const { alert, devices, statuses } = await attack(h);

    const tokens = new Set(devices.map((d) => d.token).filter((t): t is string => !!t));
    expect(tokens.size).toBeLessThanOrEqual(ALLOWED_HOLDERS);
    // The per-target claim makes it exactly one, however many identities got to hold the token.
    expect(tokens.size).toBe(1);

    let accepted = 0;
    for (const d of devices) {
      if (!d.token) continue;
      const res = await h.call(
        'POST',
        `/alerts/${alert.id}/checkins`,
        { status: 'ok', token: d.token, idempotencyKey: randomUUID() },
        d.credential,
      );
      if (res.status === 200) accepted++;
    }
    expect(accepted).toBeLessThanOrEqual(ALLOWED_HOLDERS);
    expect(h.checkins.total(alert.id, SJ_CELL)).toBeLessThanOrEqual(ALLOWED_HOLDERS);
    expect(h.checkins.total(alert.id, SJ_CELL)).toBe(1);

    // Everything past the allowance was refused at the push-token route.
    expect(statuses.filter((s) => s === 204)).toHaveLength(ALLOWED_HOLDERS);
    expect(statuses.filter((s) => s === 409)).toHaveLength(MINTS - ALLOWED_HOLDERS);
  });

  it('the target claim alone still holds when the move rule is bypassed (defense in depth)', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    // Hand each device the token directly, as a database-level bypass of assignPushTarget would.
    const tokens = new Set<string>();
    for (let i = 0; i < MINTS; i++) {
      const d = await h.register();
      await h.addPlace(d.credential, { ...SJ });
      const device = await h.devices.get(d.deviceId);
      if (!device) throw new Error('device missing');
      await h.devices.update({ ...device, pushPlatform: 'ios', pushToken: APNS });
      const before = h.sendQueue.batches('urgent').length;
      await processLateJoin({ deviceId: d.deviceId }, h.deps);
      for (const b of h.sendQueue.batches('urgent').slice(before)) {
        for (const r of b.recipients) tokens.add(r.checkInToken);
      }
    }
    expect(tokens.size).toBe(1);
  });

  it('the API itself never sends: only intents are queued', async () => {
    const h = harness();
    await h.alerts.save(makeAlert());
    const d = await h.register();
    await h.addPlace(d.credential);
    await h.setToken(d.credential);
    expect(h.intents.intents.length).toBeGreaterThan(0);
    expect(h.sendQueue.batches('urgent')).toEqual([]); // nothing until the Lambda runs
    await h.drain();
    expect(h.sendQueue.batches('urgent')).toHaveLength(1);
  });
});

describe('a real reinstall works within the rules', () => {
  const HOUR = 60 * 60 * 1000;

  it('moves the token to the reinstalled device and late-joins it', async () => {
    const h = harness();
    const oldInstall = await h.register();
    await h.addPlace(oldInstall.credential);
    await h.setToken(oldInstall.credential);
    await h.drain();

    h.time.now = new Date(h.time.now.getTime() + 2 * HOUR);
    await h.alerts.save(
      makeAlert({ sent: '2026-11-09T13:30:00Z', expires: '2026-11-09T20:00:00Z' }),
    );
    const fresh = await h.register(); // reinstall: a new anonymous device
    await h.addPlace(fresh.credential);
    expect((await h.setToken(fresh.credential)).status).toBe(204); // one move: allowed
    await h.drain();

    const old = await h.devices.get(oldInstall.deviceId);
    expect(old).toMatchObject({ pushPlatform: null, pushToken: null });
    const batches = h.sendQueue.batches('urgent');
    expect(batches.map((b) => b.recipients[0]?.deviceId)).toContain(fresh.deviceId);
  });

  it('a third reinstall within a day is refused while the previous install is active', async () => {
    const h = harness();
    const statuses: number[] = [];
    for (let i = 0; i < 4; i++) {
      const d = await h.register();
      statuses.push((await h.setToken(d.credential)).status);
      h.time.now = new Date(h.time.now.getTime() + HOUR);
    }
    expect(statuses).toEqual([204, 204, 204, 409]);
  });

  it('the same reinstall succeeds once the holder has been inactive for 24 hours', async () => {
    const h = harness();
    for (let i = 0; i < 3; i++) {
      const d = await h.register();
      expect((await h.setToken(d.credential)).status).toBe(204);
    }
    const blocked = await h.register();
    expect((await h.setToken(blocked.credential)).status).toBe(409);
    h.time.now = new Date(h.time.now.getTime() + 24 * HOUR + 1000);
    expect((await h.setToken(blocked.credential)).status).toBe(204);
  });

  it('moves age out of the 24 hour window', async () => {
    const h = harness();
    for (let i = 0; i < 3; i++) {
      const d = await h.register();
      await h.setToken(d.credential);
    }
    h.time.now = new Date(h.time.now.getTime() + 24 * HOUR + 1000);
    // The earlier moves are outside the window and the holder is inactive: allowed.
    const d = await h.register();
    expect((await h.setToken(d.credential)).status).toBe(204);
    const e = await h.register();
    expect((await h.setToken(e.credential)).status).toBe(204); // 1 recent move, allowed
  });

  it('a device keeps its own token on a refresh, however often it re-registers', async () => {
    const h = harness();
    const d = await h.register();
    for (let i = 0; i < 10; i++) expect((await h.setToken(d.credential)).status).toBe(204);
  });
});
