import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { PushMessage } from '../../../src/ports.js';
import type { NativePushMessage, NativePushResult } from '../../../src/scale/contracts.js';
import { ApnsSender } from '../../../src/adapters/push/apns-sender.js';
import { FcmSender } from '../../../src/adapters/push/fcm-sender.js';
import {
  ExpoNativeAdapter,
  RoutingPushSender,
} from '../../../src/adapters/push/routing-push-sender.js';
import { ecKeys, makeMessage, MockApns, MockFcm, rsaKeys, sleep } from './mocks.js';

describe('RoutingPushSender', () => {
  it('splits by platform, runs groups concurrently and keeps the original order', async () => {
    const calls: Record<string, string[]> = {};
    const fake = (name: string, delay: number) => ({
      async send(ms: NativePushMessage[]): Promise<NativePushResult[]> {
        calls[name] = ms.map((m) => m.target.token);
        await sleep(delay);
        // Fail tokens ending in 'x' so each result is traceable to its message.
        return ms.map((m) =>
          m.target.token.endsWith('x')
            ? { ok: false, kind: 'permanent', error: `${name}:${m.target.token}` }
            : { ok: true },
        );
      },
    });
    const router = new RoutingPushSender({
      ios: fake('ios', 60),
      android: fake('android', 60),
      expo: fake('expo', 60),
    });
    const plat = ['ios', 'android', 'expo'] as const;
    const msgs = Array.from({ length: 12 }, (_, i) =>
      makeMessage({
        target: { platform: plat[i % 3] as 'ios', token: `t${i}${i % 4 === 0 ? 'x' : ''}` },
      }),
    );
    const t0 = Date.now();
    const out = await router.send(msgs);
    expect(Date.now() - t0).toBeLessThan(150); // concurrent, not 3 x 60 ms
    expect(calls['ios']).toEqual(['t0x', 't3', 't6', 't9']);
    expect(out).toHaveLength(12);
    msgs.forEach((m, i) => {
      const token = m.target.token;
      const plt = m.target.platform;
      expect(out[i]).toEqual(
        token.endsWith('x')
          ? { ok: false, kind: 'permanent', error: `${plt}:${token}` }
          : { ok: true },
      );
    });
  });

  it('isolates a sender that throws and handles empty input', async () => {
    const ok = {
      send: async (ms: NativePushMessage[]) => ms.map((): NativePushResult => ({ ok: true })),
    };
    const boom = {
      send: async (): Promise<NativePushResult[]> => {
        throw new Error('secret-token-leak');
      },
    };
    const router = new RoutingPushSender({ ios: boom, android: ok, expo: ok });
    expect(await router.send([])).toEqual([]);
    const out = await router.send([
      makeMessage(),
      makeMessage({ target: { platform: 'android', token: 'a' } }),
    ]);
    expect(out[0]).toEqual({ ok: false, kind: 'retryable', error: 'SenderError: Error' });
    expect(out[1]).toEqual({ ok: true });
  });
});

describe('ExpoNativeAdapter', () => {
  it('maps messages to the Expo shape and results back', async () => {
    const seen: Array<PushMessage & Record<string, unknown>> = [];
    const adapter = new ExpoNativeAdapter({
      send: async (ms) => {
        seen.push(...(ms as typeof seen));
        return ms.map((m) =>
          m.to === 'ExponentPushToken[dead]'
            ? { ok: false as const, error: 'DeviceNotRegistered' }
            : m.to === 'ExponentPushToken[slow]'
              ? { ok: false as const, error: 'MessageRateExceeded for ExponentPushToken[slow]' }
              : { ok: true as const },
        );
      },
    });
    const mk = (token: string, extra = {}) =>
      makeMessage({ target: { platform: 'expo', token }, ...extra });
    const out = await adapter.send([
      mk('ExponentPushToken[a]'),
      mk('ExponentPushToken[dead]'),
      mk('ExponentPushToken[slow]', { sound: false, priority: 'normal' }),
    ]);
    expect(out).toEqual([
      { ok: true },
      { ok: false, kind: 'permanent', error: 'DeviceNotRegistered' },
      { ok: false, kind: 'retryable', error: 'MessageRateExceeded for [redacted-push-token]' },
    ]);
    expect(seen[0]).toMatchObject({
      to: 'ExponentPushToken[a]',
      categoryId: 'alert-checkin',
      priority: 'high',
      sound: 'default',
    });
    expect(seen[2]).not.toHaveProperty('sound');
    expect(seen[2]?.['priority']).toBe('normal');
  });

  it('turns a thrown error into retryable results', async () => {
    const adapter = new ExpoNativeAdapter({
      send: async () => {
        throw new Error('boom');
      },
    });
    const out = await adapter.send([makeMessage({ target: { platform: 'expo', token: 't' } })]);
    expect(out).toEqual([{ ok: false, kind: 'retryable', error: 'ExpoError: Error' }]);
  });
});

describe('RoutingPushSender with the real senders', () => {
  let apns: MockApns;
  let fcm: MockFcm;
  beforeEach(async () => {
    apns = new MockApns();
    fcm = new MockFcm();
    await fcm.start();
  });
  afterEach(async () => {
    await apns.stop();
    await fcm.stop();
  });

  it('delivers an interleaved batch to the right servers and preserves order', async () => {
    const authority = await apns.start();
    const apnsSender = new ApnsSender({
      privateKeyPem: ecKeys().privatePem,
      keyId: 'k',
      teamId: 't',
      bundleId: 'b',
      authority,
    });
    const fcmSender = new FcmSender({
      serviceAccount: { client_email: 'e', private_key: rsaKeys().privatePem },
      projectId: 'p',
      tokenUri: fcm.tokenUri,
      baseUrl: fcm.baseUrl,
    });
    const router = new RoutingPushSender({
      ios: apnsSender,
      android: fcmSender,
      expo: { send: async (ms) => ms.map((): NativePushResult => ({ ok: true })) },
    });
    const spec: Array<['ios' | 'android' | 'expo', string]> = [
      ['android', 'gone-1'],
      ['ios', 'ok-1'],
      ['expo', 'e1'],
      ['ios', 'gone-2'],
      ['android', 'ok-2'],
      ['ios', 'slow-3'],
      ['android', 'quota-3'],
      ['expo', 'e2'],
    ];
    const out = await router.send(
      spec.map(([platform, token]) => makeMessage({ target: { platform, token } })),
    );
    expect(out.map((r) => (r.ok ? 'ok' : r.kind))).toEqual([
      'permanent', // fcm gone
      'ok',
      'ok',
      'permanent', // apns gone
      'ok',
      'retryable', // apns 429
      'retryable', // fcm quota
      'ok',
    ]);
    expect(apns.records.map((r) => r.path)).toHaveLength(3);
    expect(fcm.records).toHaveLength(3);
    apnsSender.close();
  });
});
