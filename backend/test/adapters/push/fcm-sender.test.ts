import { verify } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { FcmSender } from '../../../src/adapters/push/fcm-sender.js';
import { makeMessage, MockFcm, rsaKeys } from './mocks.js';

const keys = rsaKeys();
let mock: MockFcm;
let clock = 1_700_000_000_000;

const make = (over: Partial<ConstructorParameters<typeof FcmSender>[0]> = {}): FcmSender =>
  new FcmSender({
    serviceAccount: {
      client_email: 'relay@proj.iam.gserviceaccount.com',
      private_key: keys.privatePem,
      project_id: 'proj',
    },
    tokenUri: mock.tokenUri,
    baseUrl: mock.baseUrl,
    now: () => clock,
    ...over,
  });

const android = (token: string, over = {}) =>
  makeMessage({ target: { platform: 'android', token }, ...over });

beforeEach(async () => {
  mock = new MockFcm();
  await mock.start();
});
afterEach(async () => {
  await mock.stop();
});

const decode = (part: string): Record<string, unknown> =>
  JSON.parse(Buffer.from(part, 'base64url').toString('utf8')) as Record<string, unknown>;

describe('FcmSender request shape', () => {
  it('posts the v1 message with android settings', async () => {
    const [r] = await make().send([android('tok-1')]);
    expect(r).toEqual({ ok: true });
    const rec = mock.records[0];
    expect(rec?.url).toBe('/v1/projects/proj/messages:send');
    expect(rec?.headers['authorization']).toBe('Bearer access-1');
    expect(rec?.body).toEqual({
      message: {
        token: 'tok-1',
        notification: { title: 'Evacuation order', body: 'Leave now' },
        data: { alertId: 'a1', checkInToken: 'c1', expiresAt: '2030-01-01T00:00:00Z' },
        android: {
          priority: 'HIGH',
          ttl: '3600s',
          collapse_key: 'alert-a1',
          notification: { channel_id: 'alerts', sound: 'default', click_action: 'alert-checkin' },
        },
      },
    });
  });

  it('normal priority and no sound', async () => {
    await make().send([android('tok-1', { priority: 'normal', sound: false, ttlSeconds: 90 })]);
    const a = mock.records[0]?.body.message['android'] as Record<string, any>;
    expect(a.priority).toBe('NORMAL');
    expect(a.ttl).toBe('90s');
    expect(a.notification).not.toHaveProperty('sound');
    expect(a.notification.channel_id).toBe('alerts');
  });

  it('returns an empty list for no messages without calling OAuth', async () => {
    expect(await make().send([])).toEqual([]);
    expect(mock.tokenRequests).toHaveLength(0);
  });
});

describe('FcmSender OAuth token', () => {
  it('posts a valid RS256 JWT-bearer assertion to the token endpoint', async () => {
    await make().send([android('tok-1')]);
    const req = mock.tokenRequests[0];
    expect(req?.['grant_type']).toBe('urn:ietf:params:oauth:grant-type:jwt-bearer');
    const [h, p, s] = String(req?.['assertion']).split('.') as [string, string, string];
    expect(decode(h)).toEqual({ alg: 'RS256', typ: 'JWT' });
    expect(decode(p)).toEqual({
      iss: 'relay@proj.iam.gserviceaccount.com',
      scope: 'https://www.googleapis.com/auth/firebase.messaging',
      aud: mock.tokenUri,
      iat: 1_700_000_000,
      exp: 1_700_003_600,
    });
    expect(
      verify('RSA-SHA256', Buffer.from(`${h}.${p}`), keys.publicKey, Buffer.from(s, 'base64url')),
    ).toBe(true);
  });

  it('caches the access token until 5 minutes before expiry', async () => {
    const s = make();
    await s.send([android('tok-1')]);
    clock += 54 * 60_000; // 6 minutes left
    await s.send([android('tok-2')]);
    expect(mock.tokenRequests).toHaveLength(1);
    clock += 2 * 60_000; // 4 minutes left: refresh
    expect(await s.send([android('tok-3')])).toEqual([{ ok: true }]);
    expect(mock.tokenRequests).toHaveLength(2);
    expect(mock.records[2]?.headers['authorization']).toBe('Bearer access-2');
  });

  it('shares one token fetch between concurrent sends', async () => {
    const s = make();
    await Promise.all([s.send([android('a')]), s.send([android('b')]), s.send([android('c')])]);
    expect(mock.tokenRequests).toHaveLength(1);
  });

  it('reads the endpoint from the service account when tokenUri is not given', async () => {
    const s = new FcmSender({
      serviceAccount: {
        client_email: 'x@y',
        private_key: keys.privatePem,
        project_id: 'proj',
        token_uri: mock.tokenUri,
      },
      baseUrl: mock.baseUrl,
    });
    expect(await s.send([android('tok-1')])).toEqual([{ ok: true }]);
  });

  it('a failing token endpoint makes every message retryable, and recovers', async () => {
    const s = make();
    mock.tokenStatus = 500;
    const r = await s.send([android('a'), android('b')]);
    expect(r).toEqual([
      { ok: false, kind: 'retryable', error: 'FcmAuthError: token endpoint returned 500' },
      { ok: false, kind: 'retryable', error: 'FcmAuthError: token endpoint returned 500' },
    ]);
    expect(mock.records).toHaveLength(0);
    mock.tokenStatus = 200;
    expect(await s.send([android('a')])).toEqual([{ ok: true }]);
  });

  it('a 401 from the API drops the cached token so the next send refreshes', async () => {
    const s = make();
    await s.send([android('a')]);
    mock.forced.push({ status: 401, body: { error: { status: 'UNAUTHENTICATED' } } });
    const [r] = await s.send([android('b')]);
    expect(r).toEqual({ ok: false, kind: 'retryable', error: 'Fcm401:UNAUTHENTICATED' });
    expect(await s.send([android('c')])).toEqual([{ ok: true }]);
    expect(mock.tokenRequests).toHaveLength(2);
  });
});

describe('FcmSender response mapping', () => {
  const cases: Array<[string, 'ok' | 'permanent' | 'retryable', string?]> = [
    ['tok', 'ok'],
    ['gone-1', 'permanent', 'Fcm404:UNREGISTERED'],
    ['bad-1', 'permanent', 'Fcm400:INVALID_ARGUMENT'],
    ['badpayload-1', 'retryable', 'Fcm400:INVALID_ARGUMENT'],
    ['quota-1', 'retryable', 'Fcm429:QUOTA_EXCEEDED retry-after=30'],
    ['unavail-1', 'retryable', 'Fcm503:UNAVAILABLE retry-after=5'],
    ['ise-1', 'retryable', 'Fcm500:INTERNAL'],
  ];
  for (const [token, expected, error] of cases) {
    it(`${token} -> ${expected}`, async () => {
      const [r] = await make().send([android(token)]);
      if (expected === 'ok') expect(r).toEqual({ ok: true });
      else expect(r).toEqual({ ok: false, kind: expected, error });
    });
  }

  it('a 404 that is not UNREGISTERED (for example a wrong project) stays retryable', async () => {
    mock.forced.push({ status: 404, body: { error: { status: 'NOT_FOUND' } } });
    const [r] = await make().send([android('tok')]);
    expect(r).toEqual({ ok: false, kind: 'retryable', error: 'Fcm404:NOT_FOUND' });
  });

  it('a non-JSON error page is retryable', async () => {
    mock.forced.push({ status: 502 });
    const [r] = await make().send([android('tok')]);
    expect(r?.ok === false && r.kind).toBe('retryable');
  });

  it('returns one result per message in order', async () => {
    const r = await make().send(['a', 'gone-b', 'quota-c', 'd', 'bad-e'].map((t) => android(t)));
    expect(r.map((x) => (x.ok ? 'ok' : x.kind))).toEqual([
      'ok',
      'permanent',
      'retryable',
      'ok',
      'permanent',
    ]);
  });

  it('times out as retryable', async () => {
    const [r] = await make({ timeoutMs: 100 }).send([android('hang-1')]);
    expect(r).toEqual({ ok: false, kind: 'retryable', error: 'Timeout' });
  });

  it('connection refused is retryable and never throws', async () => {
    const s = make();
    await s.send([android('warm')]); // get a token while the server is up
    const dead = make({ baseUrl: 'http://127.0.0.1:1' });
    const [r] = await dead.send([android('x')]);
    expect(r?.ok).toBe(false);
    if (r && !r.ok) expect(r.kind).toBe('retryable');
  });

  it('never includes the registration token in results or logs', async () => {
    const logs: string[] = [];
    const logger = {
      info: (m: string, f?: unknown) => logs.push(JSON.stringify([m, f])),
      warn: (m: string, f?: unknown) => logs.push(JSON.stringify([m, f])),
      error: (m: string, f?: unknown) => logs.push(JSON.stringify([m, f])),
    };
    mock.tokenStatus = 500;
    const failing = make({ logger });
    const r0 = await failing.send([android('SECRETFCM')]);
    mock.tokenStatus = 200;
    const s = make({ logger });
    const r = await s.send(
      ['gone-SECRETFCM', 'quota-SECRETFCM', 'bad-SECRETFCM'].map((t) => android(t)),
    );
    expect(JSON.stringify([r0, r]) + logs.join('')).not.toContain('SECRETFCM');
  });
});

describe('FcmSender concurrency', () => {
  it('never exceeds the in-flight cap', async () => {
    mock.delayMs = 15;
    mock.record = false;
    const r = await make({ maxInFlight: 20 }).send(
      Array.from({ length: 200 }, (_, i) => android(`tok-${i}`)),
    );
    expect(r.every((x) => x.ok)).toBe(true);
    expect(mock.total - mock.tokenRequests.length).toBe(200);
    expect(mock.maxInFlight).toBeLessThanOrEqual(20);
    expect(mock.maxInFlight).toBeGreaterThan(8);
  });
});
