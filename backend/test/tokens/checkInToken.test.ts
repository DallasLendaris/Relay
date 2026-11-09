import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  createCheckInToken,
  createCheckInTokenV1,
  verifyCheckInToken,
} from '../../src/tokens/index.js';

const expiresAt = new Date('2026-11-10T12:00:00Z');
const before = new Date('2026-11-10T11:00:00Z');
const base = { alertId: 'alert-1', deviceId: 'device-1' };
const CELL = '872830828ffffff';
const key = { id: 'k1', secret: 'secret-one' };
const keys = new Map([['k1', 'secret-one']]);
const makeV2 = (over: Partial<{ alertId: string; h3Cell: string }> = {}) =>
  createCheckInToken({ ...base, expiresAt, h3Cell: CELL, ...over }, key);
const makeV1 = () => createCheckInTokenV1({ ...base, expiresAt }, key);
const verify = (t: unknown, p = {}, k = keys) =>
  verifyCheckInToken(t, { ...base, now: before, ...p }, k);
const signed = (payload: object, secret = 'secret-one') => {
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  return `${body}.${createHmac('sha256', secret).update(body).digest('base64url')}`;
};

describe.each([
  ['v2', makeV2, { ok: true, h3Cell: CELL }],
  ['v1', makeV1, { ok: true, h3Cell: null }],
] as const)('check-in tokens (%s)', (_name, make, accepted) => {
  it('accepts a valid token for its own alert and device', () => {
    expect(verify(make())).toEqual(accepted);
  });

  it('rejects another alert', () => {
    expect(verify(make(), { alertId: 'alert-2' })).toEqual({ ok: false, reason: 'wrong_alert' });
  });

  it('rejects another device', () => {
    expect(verify(make(), { deviceId: 'device-2' })).toEqual({ ok: false, reason: 'wrong_device' });
  });

  it('expires with the alert, allowing 60 s of skew', () => {
    const t = make();
    expect(verify(t, { now: new Date(expiresAt.getTime() + 60_000) })).toEqual(accepted);
    expect(verify(t, { now: new Date(expiresAt.getTime() + 61_000) })).toEqual({
      ok: false,
      reason: 'expired',
    });
  });

  it('fails when any single character is changed', () => {
    const t = make();
    for (let i = 0; i < t.length; i++) {
      const c = t[i] === 'A' ? 'B' : 'A';
      const tampered = t.slice(0, i) + c + t.slice(i + 1);
      expect(verify(tampered).ok, `index ${i}`).toBe(false);
    }
  });

  it('fails for an unknown kid', () => {
    const t =
      _name === 'v2'
        ? createCheckInToken({ ...base, expiresAt, h3Cell: CELL }, { id: 'nope', secret: 'x' })
        : createCheckInTokenV1({ ...base, expiresAt }, { id: 'nope', secret: 'x' });
    expect(verify(t)).toEqual({ ok: false, reason: 'unknown_key' });
  });

  it('still verifies old tokens after key rotation', () => {
    const oldToken = make();
    const rotated = new Map([
      ['k2', 'secret-two'],
      ['k1', 'secret-one'],
    ]);
    expect(verify(oldToken, {}, rotated)).toEqual(accepted);
    // Once the old key is retired, its tokens stop verifying.
    expect(verify(oldToken, {}, new Map([['k2', 'secret-two']])).ok).toBe(false);
  });
});

describe('check-in tokens (v2 cell binding)', () => {
  it("fails when a signed payload is swapped for another token's payload", () => {
    const other = makeV2({ alertId: 'alert-2' });
    const forged = other.split('.')[0] + '.' + makeV2().split('.')[1];
    expect(verify(forged, { alertId: 'alert-2' })).toEqual({ ok: false, reason: 'bad_signature' });
  });

  it('cannot move a token to another cell: changing c breaks the signature', () => {
    const [body = '', sig = ''] = makeV2().split('.');
    const claims = JSON.parse(Buffer.from(body, 'base64url').toString('utf8')) as object;
    const moved = Buffer.from(JSON.stringify({ ...claims, c: '872830829ffffff' })).toString(
      'base64url',
    );
    expect(verify(`${moved}.${sig}`)).toEqual({ ok: false, reason: 'bad_signature' });
  });

  it('keeps the cell through key rotation', () => {
    const rotated = new Map([
      ['k2', 'secret-two'],
      ['k1', 'secret-one'],
    ]);
    const t = createCheckInToken(
      { ...base, expiresAt, h3Cell: 'cell-b' },
      { id: 'k2', secret: 'secret-two' },
    );
    expect(verify(t, {}, rotated)).toEqual({ ok: true, h3Cell: 'cell-b' });
  });

  it('rejects a correctly signed v2 payload with no cell, and an unknown version', () => {
    const v2 = { v: 2, a: 'alert-1', d: 'device-1', exp: 4_000_000_000, kid: 'k1' };
    expect(verify(signed(v2))).toEqual({ ok: false, reason: 'malformed' });
    expect(verify(signed({ ...v2, c: '' }))).toEqual({ ok: false, reason: 'malformed' });
    expect(verify(signed({ ...v2, c: 7 }))).toEqual({ ok: false, reason: 'malformed' });
    expect(verify(signed({ ...v2, c: CELL, v: 3 }))).toEqual({ ok: false, reason: 'malformed' });
  });

  it('ignores a cell claim in a v1 token (v1 never yields a cell)', () => {
    const t = signed({ v: 1, a: 'alert-1', d: 'device-1', c: CELL, exp: 4_000_000_000, kid: 'k1' });
    expect(verify(t)).toEqual({ ok: true, h3Cell: null });
  });
});

describe('check-in tokens (malformed input)', () => {
  it('never throws on malformed input', () => {
    const b64 = (s: string) => Buffer.from(s).toString('base64url');
    const inputs: unknown[] = [
      '',
      '.',
      'a.b',
      'a.b.c',
      '....',
      'not a token',
      '%%%.%%%',
      '\u0000.\u0000',
      b64('{') + '.x',
      b64('null') + '.x',
      b64('[]') + '.x',
      b64('"s"') + '.x',
      b64('{"kid":5}') + '.x',
      b64('{}') + '.x',
      undefined,
      null,
      42,
      {},
      [],
      Symbol('s'),
      'x'.repeat(100_000),
    ];
    for (const input of inputs) {
      expect(() => verify(input)).not.toThrow();
      expect(verify(input).ok).toBe(false);
    }
  });
});
