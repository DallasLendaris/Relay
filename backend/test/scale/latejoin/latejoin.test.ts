import type { SQSEvent } from 'aws-lambda';
import { cellToChildren, cellToParent } from 'h3-js';
import { describe, expect, it } from 'vitest';
import { cellForPoint } from '../../../src/geo/index.js';
import { bucketFor } from '../../../src/scale/fanout/hash.js';
import {
  RATE_LIMIT_PER_HOUR,
  pushTargetHash,
  processLateJoin,
} from '../../../src/scale/latejoin/index.js';
import { buildLateJoinHandler } from '../../../src/scale/latejoin/handler.js';
import { lateKeyOf } from '../../../src/scale/latejoin/latejoin.js';
import { createAlertVerifier, signAlertSync } from '../../../src/signing/index.js';
import { verifyCheckInToken } from '../../../src/tokens/index.js';
import { APNS, SJ_CELL, TOKEN_KEY, harness, makeAlert } from './helpers.js';
import { ATTACKER_SIGNING, TEST_SIGNING } from '../../signing/test-signing.js';

const keys = new Map([[TOKEN_KEY.id, TOKEN_KEY.secret]]);

describe('late-join Lambda core', () => {
  it('sends the active alert to a device with a target, with a v2 token for the primary cell', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    const results = await h.drain();

    expect(results.reduce((n, r) => n + r.sent, 0)).toBe(1);
    const [batch] = h.sendQueue.batches('urgent');
    expect(h.sendQueue.batches('urgent')).toHaveLength(1);
    expect(batch).toMatchObject({
      alertId: alert.id,
      sourceId: alert.sourceId,
      relaySeverity: 'warning',
      batchKey: `${alert.id}:warning:late:${lateKeyOf(deviceId)}`,
      attempt: 0,
    });
    expect(batch?.batchKey).not.toContain(deviceId); // batch keys are logged
    expect(batch?.recipients[0]).toMatchObject({
      deviceId,
      target: { platform: 'ios', token: APNS },
      language: 'en',
    });
    expect(
      verifyCheckInToken(
        batch?.recipients[0]?.checkInToken ?? '',
        { alertId: alert.id, deviceId, now: h.time.now },
        keys,
      ),
    ).toEqual({ ok: true, h3Cell: SJ_CELL, expiresAt: expect.any(Date) });
  });

  it('watch and advisory alerts go to the standard queue', async () => {
    const h = harness();
    await h.alerts.save(makeAlert({ relaySeverity: 'watch', event: 'Flood Watch' }));
    const { credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    await h.drain();
    expect(h.sendQueue.batches('standard')).toHaveLength(1);
    expect(h.sendQueue.batches('urgent')).toEqual([]);
  });

  it('skips statements, expired, cancelled and other-area alerts, and devices with no target', async () => {
    const h = harness();
    await h.alerts.save(makeAlert({ relaySeverity: 'statement' }));
    await h.alerts.save(makeAlert({ expires: '2026-11-09T10:00:00Z' }));
    await h.alerts.save(makeAlert({ state: 'cancelled' }));
    await h.alerts.save(makeAlert({ status: 'Test' }));
    const far = cellToChildren(cellToParent(SJ_CELL, 3), 7).find((c) => c !== SJ_CELL) as string;
    await h.alerts.save(makeAlert({}, [far]));
    const withTarget = await h.register();
    await h.setToken(withTarget.credential);
    await h.addPlace(withTarget.credential);
    await h.drain();
    expect(h.sendQueue.batches('urgent')).toEqual([]);
    expect(h.sendQueue.batches('standard')).toEqual([]);

    await h.alerts.save(makeAlert());
    const noTarget = await h.register();
    await h.addPlace(noTarget.credential); // no target: no intent at all
    expect(await h.drain()).toEqual([]);
    expect(h.sendQueue.batches('urgent')).toEqual([]);
  });

  it('a push token registered after the place triggers the send', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId, credential } = await h.register();
    await h.addPlace(credential);
    expect(await h.drain()).toEqual([]); // no target yet
    await h.setToken(credential);
    await h.drain();
    expect(h.sendQueue.batches('urgent').map((b) => b.batchKey)).toEqual([
      `${alert.id}:warning:late:${lateKeyOf(deviceId)}`,
    ]);
  });

  it('does not send while delivery is paused, and reports it so the record is retried', async () => {
    const h = harness({ paused: true });
    await h.alerts.save(makeAlert());
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    expect(await processLateJoin({ deviceId }, h.deps)).toMatchObject({ refused: 'paused' });
    expect(h.sendQueue.batches('urgent')).toEqual([]);
    h.control.paused = false;
    expect((await processLateJoin({ deviceId }, h.deps)).sent).toBe(1);
  });
});

describe('the intent is only a hint', () => {
  it('ignores placeId, alertId and cell fields and re-derives them from Aurora', async () => {
    const h = harness();
    const real = makeAlert();
    const elsewhereCell = cellForPoint(34.05, -118.25); // LA: the device has no place there
    const elsewhere = makeAlert({}, [elsewhereCell]);
    await h.alerts.save(real);
    await h.alerts.save(elsewhere);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    h.intents.intents.length = 0;

    const forged = {
      deviceId,
      placeId: 'not-a-place',
      reason: 'place_added',
      alertId: elsewhere.id,
      h3Cell: elsewhereCell,
      target: { platform: 'ios', token: 'ff'.repeat(32) },
      language: 'es',
      checkInToken: 'forged',
    };
    // Through the real handler: a body with extra fields is accepted and trimmed to the device id.
    const handle = buildLateJoinHandler(h.deps);
    const out = await handle({
      Records: [{ messageId: 'm1', body: JSON.stringify(forged) }],
    } as unknown as SQSEvent);
    expect(out.batchItemFailures).toEqual([]);

    const batches = h.sendQueue.batches('urgent');
    expect(batches).toHaveLength(1);
    expect(batches[0]?.alertId).toBe(real.id); // never the alert the intent named
    expect(batches[0]?.recipients).toHaveLength(1);
    expect(batches[0]?.recipients[0]).toMatchObject({
      deviceId,
      target: { platform: 'ios', token: APNS }, // the stored target, not the forged one
      language: 'en',
    });
    expect(
      verifyCheckInToken(
        batches[0]?.recipients[0]?.checkInToken ?? '',
        { alertId: real.id, deviceId, now: h.time.now },
        keys,
      ),
    ).toEqual({ ok: true, h3Cell: SJ_CELL, expiresAt: expect.any(Date) });
    expect(JSON.stringify(batches)).not.toContain('forged');
  });

  it('an intent for a device with no places sends nothing, whatever it names', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    const result = await processLateJoin(
      { deviceId, ...{ alertId: alert.id, h3Cell: SJ_CELL } } as { deviceId: string },
      h.deps,
    );
    expect(result.sent).toBe(0);
    expect(result.skipped.noPlaces).toBe(1);
    expect(h.sendQueue.batches('urgent')).toEqual([]);
  });

  it('rejects bodies that are not an intent (they go back to the queue)', async () => {
    const h = harness();
    const handle = buildLateJoinHandler(h.deps);
    const out = await handle({
      Records: [
        { messageId: 'a', body: 'not json' },
        { messageId: 'b', body: JSON.stringify({ deviceId: 'nope' }) },
        { messageId: 'c', body: JSON.stringify({ alertId: 'x' }) },
      ],
    } as unknown as SQSEvent);
    expect(out.batchItemFailures.map((f) => f.itemIdentifier)).toEqual(['a', 'b', 'c']);
  });
});

describe('alert signatures', () => {
  const SIGNING = TEST_SIGNING.signer;
  const verify = createAlertVerifier(TEST_SIGNING.publicKeys);

  it('does not late-join an unsigned or forged alert', async () => {
    const h = harness({ verifyAlert: verify });
    const bad = makeAlert(); // signature "v1.k1.sig" does not verify
    const forged = signAlertSync(makeAlert(), ATTACKER_SIGNING.signer);
    await h.alerts.save(bad);
    await h.alerts.save(forged);
    const { credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    const results = await h.drain();
    expect(h.sendQueue.batches('urgent')).toEqual([]);
    expect(results.reduce((n, r) => n + r.unsigned, 0)).toBe(4); // 2 alerts x 2 intents
    expect(h.lines.some((l) => l.includes('latejoin.unsigned_alert'))).toBe(true);
    // No claim was made for an alert that was refused.
    expect(
      await h.idempotency.claim(
        `${bad.id}:pt:${pushTargetHash({ platform: 'ios', token: APNS })}`,
        new Date(h.time.now.getTime() + 1000),
      ),
    ).toBe('claimed');
  });

  it('sends the properly signed alert and only that one', async () => {
    const h = harness({ verifyAlert: verify });
    const good = signAlertSync(makeAlert(), SIGNING);
    await h.alerts.save(good);
    await h.alerts.save(makeAlert());
    const { credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    await h.drain();
    expect(h.sendQueue.batches('urgent').map((b) => b.alertId)).toEqual([good.id]);
  });

  it('refreshes the keyring once when an alert fails to verify, then sends', async () => {
    const h = harness({ verifyAlert: () => false });
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    h.intents.intents.length = 0;
    let refreshed = 0;
    const handle = buildLateJoinHandler(h.deps, async () => {
      refreshed++;
      return { ...h.deps, verifyAlert: () => true };
    });
    const out = await handle({
      Records: [{ messageId: 'm', body: JSON.stringify({ deviceId, reason: 'push_target' }) }],
    } as unknown as SQSEvent);
    expect(out.batchItemFailures).toEqual([]);
    expect(refreshed).toBe(1);
    expect(h.sendQueue.batches('urgent')).toHaveLength(1);
  });
});

describe('multi-place devices: the primary cell', () => {
  const HOME = { label: 'Home', latitude: 37.3352, longitude: -121.8811 };
  const WORK = { label: 'Work', latitude: 37.3688, longitude: -122.0363 };
  const [LO, HI] = [
    cellForPoint(HOME.latitude, HOME.longitude),
    cellForPoint(WORK.latitude, WORK.longitude),
  ].sort() as [string, string];
  const spot = (cell: string) =>
    cellForPoint(HOME.latitude, HOME.longitude) === cell ? HOME : WORK;

  it('sends one push, with the smallest cell of (device cells ∩ alert area) in the token', async () => {
    const h = harness();
    const alert = makeAlert({}, [LO, HI]);
    await h.alerts.save(alert);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential, spot(HI));
    await h.addPlace(credential, spot(LO));
    await h.drain();
    const batches = h.sendQueue.batches('urgent');
    expect(batches).toHaveLength(1); // the second intent is deduplicated by the target claim
    expect(
      verifyCheckInToken(
        batches[0]?.recipients[0]?.checkInToken ?? '',
        { alertId: alert.id, deviceId, now: h.time.now },
        keys,
      ),
    ).toEqual({ ok: true, h3Cell: LO, expiresAt: expect.any(Date) }); // not HI, though HI was the first place
  });

  it('an alert over only the non-primary cell still goes out, from that cell', async () => {
    const h = harness();
    const alert = makeAlert({}, [HI]);
    await h.alerts.save(alert);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential, spot(LO));
    await h.addPlace(credential, spot(HI));
    await h.drain();
    const batches = h.sendQueue.batches('urgent');
    expect(batches).toHaveLength(1);
    expect(
      verifyCheckInToken(
        batches[0]?.recipients[0]?.checkInToken ?? '',
        { alertId: alert.id, deviceId, now: h.time.now },
        keys,
      ),
    ).toEqual({ ok: true, h3Cell: HI, expiresAt: expect.any(Date) });
  });
});

describe('REL-12: no duplicate of a planned push', () => {
  /** Registers a device and place at `at`, i.e. before or after the alert was sent (11:00). */
  async function deviceRegisteredAt(h: ReturnType<typeof harness>, at: string) {
    h.time.now = new Date(at);
    const { deviceId, credential } = await h.register();
    await h.addPlace(credential);
    await h.setToken(credential);
    h.intents.intents.length = 0;
    h.time.now = new Date('2026-11-09T12:00:00Z');
    return { deviceId, credential };
  }
  const plannedKey = (alertId: string, deviceId: string) =>
    `${alertId}:warning:${SJ_CELL}:${bucketFor(deviceId)}`;

  it('skips a device whose place and target predate the alert when its batch was sent', async () => {
    const h = harness();
    const alert = makeAlert(); // sent 11:00
    await h.alerts.save(alert);
    const { deviceId } = await deviceRegisteredAt(h, '2026-11-09T09:00:00Z');
    // What the sender did for the planned (cell, bucket) batch.
    const key = plannedKey(alert.id, deviceId);
    await h.idempotency.claim(key, new Date('2026-11-09T12:00:30Z'));
    await h.idempotency.markSent(key, new Date('2026-11-10T12:00:00Z'));

    const result = await processLateJoin({ deviceId }, h.deps);
    expect(result.sent).toBe(0);
    expect(result.skipped.planned).toBe(1);
    expect(h.sendQueue.batches('urgent')).toEqual([]);
  });

  it('also skips while the planned batch is still being sent (leased)', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId } = await deviceRegisteredAt(h, '2026-11-09T09:00:00Z');
    await h.idempotency.claim(plannedKey(alert.id, deviceId), new Date('2026-11-09T12:00:40Z'));
    expect((await processLateJoin({ deviceId }, h.deps)).skipped.planned).toBe(1);
  });

  it('sends when the planned batch was never claimed, and leaves that claim free for the sender', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId } = await deviceRegisteredAt(h, '2026-11-09T09:00:00Z');
    expect((await processLateJoin({ deviceId }, h.deps)).sent).toBe(1);
    expect(
      await h.idempotency.claim(plannedKey(alert.id, deviceId), new Date('2026-11-09T12:01:00Z')),
    ).toBe('claimed');
  });

  it('does NOT skip a device that registered after the alert, even though its batch was sent', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId } = await deviceRegisteredAt(h, '2026-11-09T11:30:00Z'); // after sent
    const key = plannedKey(alert.id, deviceId);
    await h.idempotency.claim(key, new Date('2026-11-09T12:00:30Z'));
    await h.idempotency.markSent(key, new Date('2026-11-10T12:00:00Z'));
    // The batch went out without this device (it joined the cell later): that is the late join.
    const result = await processLateJoin({ deviceId }, h.deps);
    expect(result.sent).toBe(1);
    expect(result.skipped.planned).toBe(0);
  });

  it('does NOT skip a device that only got its push target after the alert', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    h.time.now = new Date('2026-11-09T09:00:00Z');
    const { deviceId, credential } = await h.register();
    await h.addPlace(credential); // place before the alert, no target yet
    h.time.now = new Date('2026-11-09T11:30:00Z');
    await h.setToken(credential);
    h.time.now = new Date('2026-11-09T12:00:00Z');
    const key = plannedKey(alert.id, deviceId);
    await h.idempotency.claim(key, new Date('2026-11-09T12:00:30Z'));
    await h.idempotency.markSent(key, new Date('2026-11-10T12:00:00Z'));
    expect((await processLateJoin({ deviceId }, h.deps)).sent).toBe(1);
  });

  it('a token refresh to the same value does not make the device look new', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId, credential } = await deviceRegisteredAt(h, '2026-11-09T09:00:00Z');
    await h.setToken(credential); // same token again, now
    const key = plannedKey(alert.id, deviceId);
    await h.idempotency.claim(key, new Date('2026-11-09T12:00:30Z'));
    await h.idempotency.markSent(key, new Date('2026-11-10T12:00:00Z'));
    expect((await processLateJoin({ deviceId }, h.deps)).skipped.planned).toBe(1);
  });
});

describe('one check-in token per push target per alert, and the rate limit', () => {
  it('a second intent for the same device and alert sends nothing more', async () => {
    const h = harness();
    await h.alerts.save(makeAlert());
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    await h.drain();
    const again = await processLateJoin({ deviceId }, h.deps);
    expect(again.sent).toBe(0);
    expect(again.skipped.duplicateTarget).toBe(1);
    expect(h.sendQueue.batches('urgent')).toHaveLength(1);
  });

  it(`sends at most ${RATE_LIMIT_PER_HOUR} per push target per hour, then again in the next hour`, async () => {
    const h = harness();
    for (let i = 0; i < RATE_LIMIT_PER_HOUR + 2; i++) await h.alerts.save(makeAlert());
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    h.intents.intents.length = 0;

    const first = await processLateJoin({ deviceId }, h.deps);
    expect(first.sent).toBe(RATE_LIMIT_PER_HOUR);
    expect(first.skipped.rateLimited).toBe(2);
    expect(h.sendQueue.batches('urgent')).toHaveLength(RATE_LIMIT_PER_HOUR);

    // A rate-limited alert keeps no claim, so it goes out once the hour rolls over.
    h.time.now = new Date(h.time.now.getTime() + 60 * 60 * 1000);
    const next = await processLateJoin({ deviceId }, h.deps);
    expect(next.sent).toBe(2);
    expect(h.sendQueue.batches('urgent')).toHaveLength(RATE_LIMIT_PER_HOUR + 2);
  });

  it('the limit is per push target, not per device', async () => {
    const h = harness();
    for (let i = 0; i < RATE_LIMIT_PER_HOUR; i++) await h.alerts.save(makeAlert());
    const a = await h.register();
    await h.setToken(a.credential);
    await h.addPlace(a.credential);
    await h.drain();
    const b = await h.register();
    await h.setToken(b.credential, 'cd34'.repeat(16)); // a different phone
    await h.addPlace(b.credential);
    const results = await h.drain();
    expect(results.reduce((n, r) => n + r.sent, 0)).toBe(RATE_LIMIT_PER_HOUR);
  });

  it('releases its claims when the enqueue fails, so the redelivered intent sends', async () => {
    const h = harness();
    await h.alerts.save(makeAlert());
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    h.intents.intents.length = 0;
    h.control.enqueueFails = true;
    await expect(processLateJoin({ deviceId }, h.deps)).rejects.toThrow('sqs down');
    h.control.enqueueFails = false;
    expect((await processLateJoin({ deviceId }, h.deps)).sent).toBe(1);
    expect(h.sendQueue.batches('urgent')).toHaveLength(1);
  });

  it('retries while another attempt holds the push target claim', async () => {
    const h = harness();
    const alert = makeAlert();
    await h.alerts.save(alert);
    const { deviceId, credential } = await h.register();
    await h.setToken(credential);
    await h.addPlace(credential);
    h.intents.intents.length = 0;
    await h.idempotency.claim(
      `${alert.id}:pt:${pushTargetHash({ platform: 'ios', token: APNS })}`,
      new Date(h.time.now.getTime() + 30_000),
    );
    await expect(processLateJoin({ deviceId }, h.deps)).rejects.toThrow('in flight');
  });
});
