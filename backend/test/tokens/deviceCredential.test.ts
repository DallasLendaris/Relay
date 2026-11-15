import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  createDeviceCredential,
  isStatelessCredential,
  verifyDeviceCredential,
} from '../../src/tokens/index.js';

const DEVICE = '22222222-2222-4222-8222-222222222222';
const OTHER = '33333333-3333-4333-8333-333333333333';
const key = { id: 'c1', secret: 'credential-secret-one' };
const keys = new Map([['c1', 'credential-secret-one']]);

describe('stateless device credentials', () => {
  it('has the form <deviceId>.<kid>.<base64url HMAC-SHA256(key, deviceId)>', () => {
    const c = createDeviceCredential(DEVICE, key);
    const mac = createHmac('sha256', key.secret).update(DEVICE).digest('base64url');
    expect(c).toBe(`${DEVICE}.c1.${mac}`);
    expect(isStatelessCredential(c)).toBe(true);
    expect(isStatelessCredential(`${DEVICE}.random-secret`)).toBe(false);
  });

  it('verifies and returns the device id', () => {
    expect(verifyDeviceCredential(createDeviceCredential(DEVICE, key), keys)).toBe(DEVICE);
  });

  it('rejects a tampered mac, at every character', () => {
    const c = createDeviceCredential(DEVICE, key);
    const start = c.lastIndexOf('.') + 1;
    for (let i = start; i < c.length; i++) {
      const swap = c[i] === 'A' ? 'B' : 'A';
      expect(verifyDeviceCredential(c.slice(0, i) + swap + c.slice(i + 1), keys), `i=${i}`).toBe(
        null,
      );
    }
  });

  it("rejects another device's id on this device's mac", () => {
    const mine = createDeviceCredential(DEVICE, key);
    const mac = mine.split('.')[2];
    expect(verifyDeviceCredential(`${OTHER}.c1.${mac}`, keys)).toBe(null);
  });

  it('rejects an unknown kid, a wrong key, and a retired key', () => {
    expect(verifyDeviceCredential(createDeviceCredential(DEVICE, { ...key, id: 'c9' }), keys)).toBe(
      null,
    );
    expect(
      verifyDeviceCredential(
        createDeviceCredential(DEVICE, { id: 'c1', secret: 'someone-elses-secret' }),
        keys,
      ),
    ).toBe(null);
    const c = createDeviceCredential(DEVICE, key);
    expect(verifyDeviceCredential(c, new Map([['c2', 'x']]))).toBe(null);
  });

  it('keeps old credentials valid after key rotation', () => {
    const old = createDeviceCredential(DEVICE, key);
    const rotated = new Map([
      ['c2', 'credential-secret-two'],
      ['c1', 'credential-secret-one'],
    ]);
    const fresh = createDeviceCredential(OTHER, { id: 'c2', secret: 'credential-secret-two' });
    expect(verifyDeviceCredential(old, rotated)).toBe(DEVICE);
    expect(verifyDeviceCredential(fresh, rotated)).toBe(OTHER);
  });

  it('rejects malformed input without throwing, including a non-UUID device id', () => {
    const mac = createHmac('sha256', key.secret).update('not-a-uuid').digest('base64url');
    const inputs: unknown[] = [
      '',
      '..',
      `${DEVICE}.c1`,
      `${DEVICE}.c1.`,
      `.c1.abc`,
      `${DEVICE}..abc`,
      `${DEVICE}.c1.abc.def`,
      `not-a-uuid.c1.${mac}`,
      undefined,
      null,
      42,
      {},
    ];
    for (const input of inputs) {
      expect(() => verifyDeviceCredential(input, keys)).not.toThrow();
      expect(verifyDeviceCredential(input, keys)).toBe(null);
    }
  });

  it('refuses to create a credential for a key id with a dot', () => {
    expect(() => createDeviceCredential(DEVICE, { id: 'a.b', secret: 's' })).toThrow();
    expect(() => createDeviceCredential(DEVICE, { id: '', secret: 's' })).toThrow();
  });
});
