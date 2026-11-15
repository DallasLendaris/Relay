import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ApnsSender } from '../../../src/adapters/push/apns-sender.js';
import { FcmSender } from '../../../src/adapters/push/fcm-sender.js';
import {
  PAYLOAD_LIMITS,
  sanitizeMessage,
  truncateAtWord,
} from '../../../src/adapters/push/payload.js';
import { parseRetryAfter } from '../../../src/adapters/push/retry-after.js';
import {
  validateApnsSecret,
  validateFcmServiceAccount,
} from '../../../src/adapters/push/validate-secrets.js';
import { ecKeys, makeMessage, MockApns, MockFcm, rsaKeys } from './mocks.js';

const ec = ecKeys();
const rsa = rsaKeys();

describe('truncateAtWord', () => {
  it('leaves short text alone', () => {
    expect(truncateAtWord('Leave now', 100)).toBe('Leave now');
  });
  it('cuts at a word boundary and adds an ellipsis, within the limit', () => {
    const text = 'word '.repeat(300).trim();
    const out = truncateAtWord(text, 100);
    expect(out.length).toBeLessThanOrEqual(100);
    expect(out.endsWith('…')).toBe(true);
    expect(out.slice(0, -1).endsWith('word')).toBe(true); // not mid-word
  });
  it('hard-cuts text with no spaces', () => {
    const out = truncateAtWord('x'.repeat(1000), 50);
    expect(Array.from(out)).toHaveLength(50);
    expect(out.endsWith('…')).toBe(true);
  });
  it('does not split a surrogate pair', () => {
    const out = truncateAtWord('😀'.repeat(200), 10);
    expect(Array.from(out)).toHaveLength(10);
    expect(out).not.toMatch(/[\ud800-\udbff](?![\udc00-\udfff])/);
  });
  it('sanitizeMessage applies the 100 / 600 limits and caps data values', () => {
    const m = sanitizeMessage(
      makeMessage({
        title: 'T '.repeat(400),
        body: 'B '.repeat(900),
        data: { alertId: 'a', checkInToken: 'c', big: 'z'.repeat(5000) },
      }),
    );
    expect(Array.from(m.title).length).toBeLessThanOrEqual(PAYLOAD_LIMITS.titleChars);
    expect(Array.from(m.body).length).toBeLessThanOrEqual(PAYLOAD_LIMITS.bodyChars);
    expect(m.data['big']?.length).toBe(PAYLOAD_LIMITS.dataValueChars);
  });
});

describe('parseRetryAfter', () => {
  it('reads seconds and HTTP dates, ignores junk', () => {
    expect(parseRetryAfter('30')).toBe(30);
    expect(parseRetryAfter('1.2')).toBe(2);
    const now = Date.parse('2026-11-10T10:00:00Z');
    expect(parseRetryAfter('Tue, 10 Nov 2026 10:00:45 GMT', now)).toBe(45);
    expect(parseRetryAfter('Tue, 10 Nov 2026 09:00:00 GMT', now)).toBe(0);
    expect(parseRetryAfter('soon')).toBeUndefined();
    expect(parseRetryAfter(undefined)).toBeUndefined();
  });
});

describe('APNs payload limits, 413 and Retry-After', () => {
  let mock: MockApns;
  let sender: ApnsSender;
  beforeEach(async () => {
    mock = new MockApns();
    const authority = await mock.start();
    sender = new ApnsSender({
      privateKeyPem: ec.privatePem,
      keyId: 'KEYID12345',
      teamId: 'TEAMID1234',
      bundleId: 'edu.sjsu.relay',
      authority,
    });
  });
  afterEach(async () => {
    sender.close();
    await mock.stop();
  });

  it('truncates a huge title and body so the wire JSON is at most 4096 bytes', async () => {
    const [r] = await sender.send([
      makeMessage({
        title: 'Evacuate now '.repeat(100),
        body: 'Go to higher ground. '.repeat(200),
      }),
    ]);
    expect(r).toEqual({ ok: true });
    const body = mock.records[0]?.body as { aps: { alert: { title: string; body: string } } };
    expect(Array.from(body.aps.alert.title).length).toBeLessThanOrEqual(100);
    expect(Array.from(body.aps.alert.body).length).toBeLessThanOrEqual(600);
    expect(body.aps.alert.title.endsWith('…')).toBe(true);
    expect(Buffer.byteLength(JSON.stringify(mock.records[0]?.body))).toBeLessThanOrEqual(4096);
  });

  it('drops optional data fields when the payload is still too big, keeping alertId and checkInToken', async () => {
    const data: Record<string, string> = { alertId: 'a1', checkInToken: 'c1' };
    // 10 optional values of 512 emoji (2 KB each): far over 4096 bytes together.
    for (let i = 0; i < 10; i++) data[`extra${i}`] = '😀'.repeat(512);
    const [r] = await sender.send([makeMessage({ data })]);
    expect(r).toEqual({ ok: true });
    const rec = mock.records[0]?.body as Record<string, unknown>;
    expect(rec['alertId']).toBe('a1');
    expect(rec['checkInToken']).toBe('c1');
    expect(rec['extra0']).toBeUndefined();
    expect(Buffer.byteLength(JSON.stringify(rec))).toBeLessThanOrEqual(4096);
  });

  it('a payload that cannot fit is a message-scoped permanent failure and is never sent', async () => {
    const data = { alertId: '😀'.repeat(512), checkInToken: '😀'.repeat(512) };
    const [r] = await sender.send([makeMessage({ data })]);
    expect(r).toEqual({
      ok: false,
      kind: 'permanent',
      scope: 'message',
      error: 'Apns:PayloadTooLarge',
    });
    expect(mock.records).toHaveLength(0);
  });

  it('a 413 from APNs is permanent for the message, not the token', async () => {
    mock.forced.push([413, 'PayloadTooLarge']);
    const [r] = await sender.send([makeMessage()]);
    expect(r).toEqual({
      ok: false,
      kind: 'permanent',
      scope: 'message',
      error: 'Apns413:PayloadTooLarge',
    });
  });

  it('carries Retry-After on a 429', async () => {
    mock.forced.push([429, 'TooManyRequests', '17']);
    const [r] = await sender.send([makeMessage()]);
    expect(r).toMatchObject({ ok: false, kind: 'retryable', retryAfterSeconds: 17 });
  });
});

describe('FCM payload limits and Retry-After', () => {
  let mock: MockFcm;
  let sender: FcmSender;
  beforeEach(async () => {
    mock = new MockFcm();
    await mock.start();
    sender = new FcmSender({
      serviceAccount: {
        client_email: 'relay@proj.iam.gserviceaccount.com',
        private_key: rsa.privatePem,
        project_id: 'proj',
      },
      tokenUri: mock.tokenUri,
      baseUrl: mock.baseUrl,
    });
  });
  afterEach(async () => {
    sender.close();
    await mock.stop();
  });
  const android = (over = {}) =>
    makeMessage({ target: { platform: 'android', token: 'tok' }, ...over });

  it('keeps the FCM message at most 4000 bytes', async () => {
    const [r] = await sender.send([android({ title: 'x '.repeat(500), body: 'y '.repeat(2000) })]);
    expect(r).toEqual({ ok: true });
    const n = Buffer.byteLength(JSON.stringify(mock.records[0]?.body));
    expect(n).toBeLessThanOrEqual(4000);
    const notif = mock.records[0]?.body.message['notification'] as { title: string; body: string };
    expect(Array.from(notif.title).length).toBeLessThanOrEqual(100);
    expect(Array.from(notif.body).length).toBeLessThanOrEqual(600);
  });

  it('drops optional data when over 4000 bytes, and reports PayloadTooLarge when nothing fits', async () => {
    const data: Record<string, string> = { alertId: 'a1', checkInToken: 'c1' };
    for (let i = 0; i < 10; i++) data[`extra${i}`] = '😀'.repeat(512);
    expect((await sender.send([android({ data })]))[0]).toEqual({ ok: true });
    const sentData = mock.records[0]?.body.message['data'] as Record<string, string>;
    expect(Object.keys(sentData).sort()).toEqual(['alertId', 'checkInToken']);

    const huge = { alertId: '😀'.repeat(512), checkInToken: '😀'.repeat(512) };
    const total = mock.records.length;
    expect((await sender.send([android({ data: huge })]))[0]).toEqual({
      ok: false,
      kind: 'permanent',
      scope: 'message',
      error: 'Fcm:PayloadTooLarge',
    });
    expect(mock.records).toHaveLength(total);
  });

  it('carries Retry-After seconds on a 429', async () => {
    const [r] = await sender.send([android({ target: { platform: 'android', token: 'quota-1' } })]);
    expect(r).toMatchObject({ ok: false, kind: 'retryable', retryAfterSeconds: 30 });
  });
});

describe('startup secret validation', () => {
  const goodApns = {
    keyId: 'ABCDE12345',
    teamId: 'TEAM123456',
    bundleId: 'edu.sjsu.relay',
    p8: ec.privatePem,
    sandbox: false,
  };
  const goodFcm = {
    project_id: 'relay-proj',
    client_email: 'relay@relay-proj.iam.gserviceaccount.com',
    private_key: rsa.privatePem,
    token_uri: 'https://oauth2.googleapis.com/token',
  };

  it('accepts good APNs and FCM secrets', () => {
    expect(validateApnsSecret(goodApns)).toMatchObject({ keyId: 'ABCDE12345', sandbox: false });
    expect(validateFcmServiceAccount(goodFcm)).toMatchObject({ project_id: 'relay-proj' });
    expect(validateApnsSecret({ ...goodApns, sandbox: undefined }).sandbox).toBe(false);
  });

  const badApns: Array<[string, Record<string, unknown>, RegExp]> = [
    ['placeholder p8', { ...goodApns, p8: 'REPLACE_ME' }, /p8 is still the REPLACE_ME/],
    ['placeholder keyId', { ...goodApns, keyId: 'REPLACE_ME' }, /keyId is still/],
    ['short keyId', { ...goodApns, keyId: 'ABC' }, /keyId must be 10/],
    ['long teamId', { ...goodApns, teamId: 'TEAM1234567' }, /teamId must be 10/],
    ['bad bundle id', { ...goodApns, bundleId: 'relay' }, /bundleId must look like reverse DNS/],
    ['unparseable p8', { ...goodApns, p8: 'not a key' }, /p8 does not parse/],
    ['RSA p8', { ...goodApns, p8: rsa.privatePem }, /not an EC private key/],
    ['missing p8', { ...goodApns, p8: undefined }, /p8 is missing/],
    ['non-boolean sandbox', { ...goodApns, sandbox: 'yes' }, /sandbox must be a boolean/],
  ];
  it.each(badApns)('rejects APNs: %s', (_n, secret, msg) => {
    expect(() => validateApnsSecret(secret)).toThrow(msg);
  });

  const badFcm: Array<[string, Record<string, unknown>, RegExp]> = [
    ['undefined private_key', { ...goodFcm, private_key: undefined }, /private_key is missing/],
    ['placeholder private_key', { ...goodFcm, private_key: 'REPLACE_ME' }, /private_key is still/],
    ['missing project_id', { ...goodFcm, project_id: undefined }, /project_id is missing/],
    ['missing client_email', { ...goodFcm, client_email: '' }, /client_email is missing/],
    ['non-PEM private_key', { ...goodFcm, private_key: 'abc' }, /private_key is not PEM/],
    [
      'broken PEM',
      { ...goodFcm, private_key: '-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----' },
      /does not parse/,
    ],
  ];
  it.each(badFcm)('rejects FCM: %s', (_n, secret, msg) => {
    expect(() => validateFcmServiceAccount(secret)).toThrow(msg);
  });

  it('names every bad field at once and never echoes a secret value', () => {
    let message = '';
    try {
      validateApnsSecret({ ...goodApns, keyId: 'short', p8: 'SECRETVALUE' });
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toContain('keyId');
    expect(message).toContain('p8');
    expect(message).not.toContain('SECRETVALUE');
    expect(() => validateApnsSecret('nope')).toThrow(/not a JSON object/);
  });
});
