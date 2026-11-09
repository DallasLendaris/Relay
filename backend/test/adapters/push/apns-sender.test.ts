import { createPublicKey, verify, type KeyObject } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApnsSender, truncateBytes } from '../../../src/adapters/push/apns-sender.js';
import { ecKeys, makeMessage, MockApns } from './mocks.js';

const keys = ecKeys();
let mock: MockApns;
let authority: string;
let sender: ApnsSender;
let clock = 1_700_000_000_000;

const make = (over: Partial<ConstructorParameters<typeof ApnsSender>[0]> = {}): ApnsSender =>
  new ApnsSender({
    privateKeyPem: keys.privatePem,
    keyId: 'KEY123',
    teamId: 'TEAM456',
    bundleId: 'edu.sjsu.relay',
    authority,
    now: () => clock,
    ...over,
  });

beforeEach(async () => {
  mock = new MockApns();
  authority = await mock.start();
  sender = make();
});
afterEach(async () => {
  sender.close();
  await mock.stop();
});

const decode = (part: string): Record<string, unknown> =>
  JSON.parse(Buffer.from(part, 'base64url').toString('utf8')) as Record<string, unknown>;

function verifyJwt(jwt: string, publicKey: KeyObject): boolean {
  const [h, p, s] = jwt.split('.') as [string, string, string];
  return verify(
    'sha256',
    Buffer.from(`${h}.${p}`),
    { key: publicKey, dsaEncoding: 'ieee-p1363' },
    Buffer.from(s, 'base64url'),
  );
}

describe('ApnsSender requests', () => {
  it('sends the documented headers and payload for a high-priority alert', async () => {
    const [r] = await sender.send([makeMessage({ token: 'ab12cd' })]);
    expect(r).toEqual({ ok: true });
    const rec = mock.records[0];
    expect(rec?.path).toBe('/3/device/ab12cd');
    expect(rec?.headers[':method']).toBe('POST');
    expect(rec?.headers['apns-topic']).toBe('edu.sjsu.relay');
    expect(rec?.headers['apns-push-type']).toBe('alert');
    expect(rec?.headers['apns-priority']).toBe('10');
    expect(rec?.headers['apns-expiration']).toBe(String(1_700_000_000 + 3600));
    expect(rec?.headers['apns-collapse-id']).toBe('alert-a1');
    expect(rec?.body).toEqual({
      alertId: 'a1',
      checkInToken: 'c1',
      expiresAt: '2030-01-01T00:00:00Z',
      aps: {
        alert: { title: 'Evacuation order', body: 'Leave now' },
        sound: 'default',
        category: 'alert-checkin',
        'thread-id': 'alert-a1',
        'interruption-level': 'time-sensitive',
      },
    });
  });

  it('omits sound and time-sensitive for a quiet normal-priority push', async () => {
    await sender.send([makeMessage({ priority: 'normal', sound: false })]);
    const rec = mock.records[0];
    expect(rec?.headers['apns-priority']).toBe('5');
    const aps = rec?.body['aps'] as Record<string, unknown>;
    expect(aps).not.toHaveProperty('sound');
    expect(aps).not.toHaveProperty('interruption-level');
    expect(aps['thread-id']).toBe('alert-a1');
  });

  it('truncates the collapse id to 64 bytes', async () => {
    await sender.send([makeMessage({ collapseKey: 'x'.repeat(100) })]);
    expect(mock.records[0]?.headers['apns-collapse-id']).toBe('x'.repeat(64));
    expect(Buffer.byteLength(truncateBytes('é'.repeat(40), 64))).toBeLessThanOrEqual(64);
    expect(truncateBytes('é'.repeat(40), 64)).toBe('é'.repeat(32));
  });

  it('uses expiration 0 when there is no time left', async () => {
    await sender.send([makeMessage({ ttlSeconds: 0 })]);
    expect(mock.records[0]?.headers['apns-expiration']).toBe('0');
  });

  it('targets the sandbox host when sandbox is set and no authority override is given', () => {
    // Construction only; no connection is opened until a send.
    const s = new ApnsSender({
      privateKeyPem: keys.privatePem,
      keyId: 'k',
      teamId: 't',
      bundleId: 'b',
      sandbox: true,
    });
    expect((s as unknown as { authority: string }).authority).toBe(
      'https://api.sandbox.push.apple.com',
    );
    const p = new ApnsSender({
      privateKeyPem: keys.privatePem,
      keyId: 'k',
      teamId: 't',
      bundleId: 'b',
    });
    expect((p as unknown as { authority: string }).authority).toBe('https://api.push.apple.com');
  });

  it('returns an empty list for no messages', async () => {
    expect(await sender.send([])).toEqual([]);
  });
});

describe('ApnsSender provider token', () => {
  it('signs a valid ES256 JWT with kid, iss and iat', async () => {
    await sender.send([makeMessage()]);
    const auth = String(mock.records[0]?.headers['authorization']);
    expect(auth.startsWith('bearer ')).toBe(true);
    const jwt = auth.slice(7);
    const [h, p] = jwt.split('.') as [string, string];
    expect(decode(h)).toEqual({ alg: 'ES256', typ: 'JWT', kid: 'KEY123' });
    expect(decode(p)).toEqual({ iss: 'TEAM456', iat: 1_700_000_000 });
    expect(verifyJwt(jwt, keys.publicKey)).toBe(true);
    // A different key must not verify.
    expect(
      verifyJwt(jwt, createPublicKey(ecKeys().publicKey.export({ type: 'spki', format: 'pem' }))),
    ).toBe(false);
  });

  it('caches the token across sends and refreshes it after 50 minutes', async () => {
    await sender.send([makeMessage(), makeMessage()]);
    clock += 49 * 60_000;
    await sender.send([makeMessage()]);
    const tokens = () => new Set(mock.records.map((r) => r.headers['authorization']));
    expect(tokens().size).toBe(1);
    clock += 2 * 60_000; // 51 minutes since issue
    await sender.send([makeMessage()]);
    expect(tokens().size).toBe(2);
    const fresh = String(mock.records[3]?.headers['authorization']).slice(7);
    expect(decode(fresh.split('.')[1] as string)['iat']).toBe(Math.floor(clock / 1000));
    expect(verifyJwt(fresh, keys.publicKey)).toBe(true);
  });

  it('403 is retryable and forces a refresh on the next send', async () => {
    mock.forced.push([403, 'ExpiredProviderToken']);
    const [bad] = await sender.send([makeMessage()]);
    expect(bad).toEqual({ ok: false, kind: 'retryable', error: 'Apns403:ExpiredProviderToken' });
    const [good] = await sender.send([makeMessage()]);
    expect(good).toEqual({ ok: true });
    expect(mock.records[0]?.headers['authorization']).not.toBe(
      mock.records[1]?.headers['authorization'],
    );
  });

  it('a burst of 403s refreshes once, not once per request', async () => {
    mock.forced.push([403, 'InvalidProviderToken'], [403, 'InvalidProviderToken']);
    const r = await sender.send([makeMessage(), makeMessage()]);
    expect(r.every((x) => !x.ok && x.kind === 'retryable')).toBe(true);
    await sender.send([makeMessage(), makeMessage(), makeMessage()]);
    const after = mock.records.slice(2).map((x) => x.headers['authorization']);
    expect(new Set(after).size).toBe(1);
  });
});

describe('ApnsSender response mapping', () => {
  const cases: Array<[string, 'ok' | 'permanent' | 'retryable', string?]> = [
    ['ok-1', 'ok'],
    ['gone-1', 'permanent', 'Apns410:Unregistered'],
    ['bad-1', 'permanent', 'Apns400:BadDeviceToken'],
    ['wrongtopic-1', 'permanent', 'Apns400:DeviceTokenNotForTopic'],
    ['slow-1', 'retryable', 'Apns429:TooManyRequests'],
    ['err-1', 'retryable', 'Apns503:ServiceUnavailable'],
    ['ise-1', 'retryable', 'Apns500:InternalServerError'],
    ['badtopic-1', 'retryable', 'Apns400:BadTopic'],
  ];
  for (const [token, expected, error] of cases) {
    it(`${token} -> ${expected}`, async () => {
      const [r] = await sender.send([makeMessage({ token })]);
      if (expected === 'ok') expect(r).toEqual({ ok: true });
      else expect(r).toEqual({ ok: false, kind: expected, error });
    });
  }

  it('returns one result per message in order, mixing outcomes', async () => {
    const tokens = ['ok-a', 'gone-b', 'slow-c', 'ok-d', 'bad-e'];
    const r = await sender.send(tokens.map((token) => makeMessage({ token })));
    expect(r.map((x) => (x.ok ? 'ok' : x.kind))).toEqual([
      'ok',
      'permanent',
      'retryable',
      'ok',
      'permanent',
    ]);
  });

  it('times out a hung request as retryable and frees its slot', async () => {
    const s = make({ timeoutMs: 100, maxInFlight: 1 });
    const t0 = Date.now();
    const r = await s.send([makeMessage({ token: 'hang-1' }), makeMessage({ token: 'ok-2' })]);
    expect(r[0]).toEqual({ ok: false, kind: 'retryable', error: 'Timeout' });
    expect(r[1]).toEqual({ ok: true }); // got the slot after the timeout
    expect(Date.now() - t0).toBeLessThan(2000);
    s.close();
  });

  it('connection failure is retryable and never throws', async () => {
    const s = make({ authority: 'http://127.0.0.1:1' });
    const [r] = await s.send([makeMessage()]);
    expect(r?.ok).toBe(false);
    if (r && !r.ok) {
      expect(r.kind).toBe('retryable');
      expect(r.error).toMatch(/^ApnsConnectionError/);
    }
    s.close();
  });

  it('a bad signing key is a retryable result, not a throw', async () => {
    const s = make({ privateKeyPem: 'not a key' });
    const [r] = await s.send([makeMessage()]);
    expect(r?.ok).toBe(false);
    s.close();
  });

  it('never puts the device token in an error string or a log', async () => {
    const logs: string[] = [];
    const logger = {
      info: (m: string, f?: unknown) => logs.push(JSON.stringify([m, f])),
      warn: (m: string, f?: unknown) => logs.push(JSON.stringify([m, f])),
      error: (m: string, f?: unknown) => logs.push(JSON.stringify([m, f])),
    };
    const s = make({ logger });
    mock.forced.push([403, 'ExpiredProviderToken']);
    const secret = 'gone-SECRETTOKEN123';
    const r1 = await s.send([makeMessage({ token: 'SECRETTOKEN123' })]);
    const r2 = await s.send([makeMessage({ token: secret })]);
    mock.dropNext = true;
    const r3 = await s.send([makeMessage({ token: 'SECRETTOKEN123' })]);
    const dead = make({ authority: 'http://127.0.0.1:1', logger });
    const r4 = await dead.send([makeMessage({ token: 'SECRETTOKEN123' })]);
    const all = JSON.stringify([r1, r2, r3, r4]) + logs.join('');
    expect(all).not.toContain('SECRETTOKEN123');
    s.close();
    dead.close();
  });
});

describe('ApnsSender sessions', () => {
  it('reconnects after GOAWAY and keeps delivering', async () => {
    const s = make({ sessions: 1 });
    mock.goawayAfterNext = true;
    expect(await s.send([makeMessage()])).toEqual([{ ok: true }]);
    expect(mock.connections).toBe(1);
    // Give the client a moment to process the GOAWAY frame.
    await new Promise((r) => setTimeout(r, 50));
    const r = await s.send([makeMessage(), makeMessage(), makeMessage()]);
    expect(r.every((x) => x.ok)).toBe(true);
    expect(mock.connections).toBe(2);
    s.close();
  });

  it('recovers from a dropped connection: that request is retryable, the next succeeds', async () => {
    const s = make({ sessions: 1 });
    await s.send([makeMessage()]);
    mock.dropNext = true;
    const [dropped] = await s.send([makeMessage()]);
    expect(dropped?.ok).toBe(false);
    if (dropped && !dropped.ok) expect(dropped.kind).toBe('retryable');
    const [after] = await s.send([makeMessage()]);
    expect(after).toEqual({ ok: true });
    expect(mock.connections).toBe(2);
    s.close();
  });

  it('spreads load over the session pool', async () => {
    const s = make({ sessions: 4 });
    mock.delayMs = 20;
    await s.send(Array.from({ length: 40 }, () => makeMessage()));
    expect(mock.connections).toBe(4);
    s.close();
  });

  it('never exceeds the in-flight cap, and uses concurrency up to it', async () => {
    const s = make({ maxInFlight: 25 });
    mock.delayMs = 15;
    mock.record = false;
    const r = await s.send(
      Array.from({ length: 300 }, (_, i) => makeMessage({ token: `ok-${i}` })),
    );
    expect(r.every((x) => x.ok)).toBe(true);
    expect(mock.total).toBe(300);
    expect(mock.maxInFlight).toBeLessThanOrEqual(25);
    expect(mock.maxInFlight).toBeGreaterThan(10);
    s.close();
  });
});
