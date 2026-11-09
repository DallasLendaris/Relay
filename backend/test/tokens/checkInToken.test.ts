import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createCheckInToken, verifyCheckInToken } from '../../src/tokens/index.js';

const expiresAt = new Date('2026-11-10T12:00:00Z');
const before = new Date('2026-11-10T11:00:00Z');
const base = { alertId: 'alert-1', deviceId: 'device-1' };
const key = { id: 'k1', secret: 'secret-one' };
const keys = new Map([['k1', 'secret-one']]);
const make = () => createCheckInToken({ ...base, expiresAt }, key);
const verify = (t: unknown, p = {}, k = keys) =>
  verifyCheckInToken(t, { ...base, now: before, ...p }, k);

describe('check-in tokens', () => {
  it('accepts a valid token for its own alert and device', () => {
    expect(verify(make())).toEqual({ ok: true });
  });

  it('rejects another alert', () => {
    expect(verify(make(), { alertId: 'alert-2' })).toEqual({ ok: false, reason: 'wrong_alert' });
  });

  it('rejects another device', () => {
    expect(verify(make(), { deviceId: 'device-2' })).toEqual({ ok: false, reason: 'wrong_device' });
  });

  it('expires with the alert, allowing 60 s of skew', () => {
    const t = make();
    expect(verify(t, { now: new Date(expiresAt.getTime() + 60_000) })).toEqual({ ok: true });
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

  it("fails when a signed payload is swapped for another token's payload", () => {
    const other = createCheckInToken({ ...base, alertId: 'alert-2', expiresAt }, key);
    const forged = other.split('.')[0] + '.' + make().split('.')[1];
    expect(verify(forged, { alertId: 'alert-2' })).toEqual({ ok: false, reason: 'bad_signature' });
  });

  it('fails for an unknown kid', () => {
    const t = createCheckInToken({ ...base, expiresAt }, { id: 'nope', secret: 'x' });
    expect(verify(t)).toEqual({ ok: false, reason: 'unknown_key' });
  });

  it('still verifies old tokens after key rotation', () => {
    const oldToken = make();
    const rotated = new Map([
      ['k2', 'secret-two'],
      ['k1', 'secret-one'],
    ]);
    const newToken = createCheckInToken({ ...base, expiresAt }, { id: 'k2', secret: 'secret-two' });
    expect(verify(oldToken, {}, rotated)).toEqual({ ok: true });
    expect(verify(newToken, {}, rotated)).toEqual({ ok: true });
    // Once the old key is retired, its tokens stop verifying.
    expect(verify(oldToken, {}, new Map([['k2', 'secret-two']])).ok).toBe(false);
  });

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

  it('rejects a correctly signed payload with the wrong shape', () => {
    const body = Buffer.from(
      JSON.stringify({ v: 2, a: 'alert-1', d: 'device-1', exp: 1, kid: 'k1' }),
    ).toString('base64url');
    const sig = createHmac('sha256', 'secret-one').update(body).digest('base64url');
    expect(verify(`${body}.${sig}`)).toEqual({ ok: false, reason: 'malformed' });
  });
});
