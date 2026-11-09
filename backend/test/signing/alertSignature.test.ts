import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import {
  canonicalAlertBytes,
  createAlertVerifier,
  createMemoryAlertSigning,
  signAlert,
  signAlertSync,
  verifyAlertSignature,
} from '../../src/signing/index.js';

const alert: Alert = {
  id: '3f1c1c64-5a0e-4c1e-9d55-0d2f6a1d9a11',
  source: 'nws',
  sourceId: 'urn:oid:2.49.0.1.840.0.test',
  event: 'Flash Flood Warning',
  eventCode: 'FFW',
  relaySeverity: 'warning',
  capSeverity: 'Severe',
  urgency: 'Immediate',
  certainty: 'Likely',
  status: 'Actual',
  messageType: 'Alert',
  state: 'active',
  references: [],
  area: {
    description: 'Santa Clara County',
    geometry: null,
    ugcCodes: ['CAZ513'],
    sameCodes: ['006085'],
    h3Cells: ['872830828ffffff', '87283082effffff'],
  },
  sent: '2026-11-10T10:00:00-08:00',
  effective: '2026-11-10T10:00:00-08:00',
  onset: null,
  expires: '2026-11-10T12:00:00-08:00',
  ends: null,
  official: {
    headline: 'Flash Flood Warning',
    description: 'Flooding is happening.',
    instruction: 'Move to higher ground.',
    senderName: 'NWS Monterey',
    language: 'en-US',
  },
  summaries: [],
  signature: null,
};

const k1 = createMemoryAlertSigning('k1');
const key = k1.signer;
const keys = k1.publicKeys;
const signed = signAlertSync(alert, key);

describe('alert signing (ECDSA P-256)', () => {
  it('signs and verifies', async () => {
    expect(signed.signature).toMatch(/^v2\.k1\.[A-Za-z0-9_-]+$/);
    expect(verifyAlertSignature(signed, keys)).toEqual({ ok: true });
    expect(createAlertVerifier(keys)(signed)).toBe(true);
    expect(alert.signature).toBeNull(); // original untouched
    const viaPort = await signAlert(alert, key);
    expect(verifyAlertSignature(viaPort, keys)).toEqual({ ok: true });
  });

  it.each<[string, (a: Alert) => Alert]>([
    ['event', (a) => ({ ...a, event: 'Tornado Warning' })],
    ['expires', (a) => ({ ...a, expires: '2026-11-11T12:00:00-08:00' })],
    [
      'h3Cells order',
      (a) => ({ ...a, area: { ...a.area, h3Cells: [...a.area.h3Cells].reverse() } }),
    ],
    [
      'h3Cells content',
      (a) => ({ ...a, area: { ...a.area, h3Cells: ['872830828ffffff', '872830829ffffff'] } }),
    ],
    [
      'official text',
      (a) => ({ ...a, official: { ...a.official, instruction: 'Ignore this alert.' } }),
    ],
  ])('rejects a changed %s', (_name, tamper) => {
    const tampered = tamper(signed);
    expect(verifyAlertSignature(tampered, keys)).toEqual({ ok: false, reason: 'bad_signature' });
    expect(createAlertVerifier(keys)(tampered)).toBe(false);
  });

  it('still verifies when object keys are reordered', () => {
    const reverse = <T extends object>(o: T): T =>
      Object.fromEntries(Object.entries(o).reverse()) as T;
    const reordered = reverse({ ...signed, official: reverse(signed.official) });
    expect(Object.keys(reordered)[0]).toBe('signature');
    expect(canonicalAlertBytes(reordered).equals(canonicalAlertBytes(signed))).toBe(true);
    expect(verifyAlertSignature(reordered, keys)).toEqual({ ok: true });
  });

  it('reports unsigned and malformed', () => {
    expect(verifyAlertSignature({ ...signed, signature: null }, keys)).toEqual({
      ok: false,
      reason: 'unsigned',
    });
    for (const junk of ['garbage', 'v1.k1.abc', 'v2.k1', 'v2..abc', 'v2.k1.a.b', 'v2.k1.ab=']) {
      expect(verifyAlertSignature({ ...signed, signature: junk }, keys)).toEqual({
        ok: false,
        reason: 'malformed',
      });
    }
    const bad = { ...signed, signature: 42 } as unknown as Alert;
    expect(verifyAlertSignature(bad, keys)).toEqual({ ok: false, reason: 'malformed' });
    // A truncated tag is a bad signature, and must not throw.
    const short = { ...signed, signature: 'v2.k1.AAAA' };
    expect(verifyAlertSignature(short, keys)).toEqual({ ok: false, reason: 'bad_signature' });
  });

  it('no longer accepts the old symmetric HMAC (v1) signatures', () => {
    const tag = createHmac('sha256', 'alert-secret-one')
      .update(canonicalAlertBytes(alert))
      .digest('base64url');
    const hmac = { ...alert, signature: `v1.k1.${tag}` };
    expect(verifyAlertSignature(hmac, keys)).toEqual({ ok: false, reason: 'malformed' });
  });

  it('SEC-A-9: rejects non-canonical base64url spellings of a valid signature', () => {
    const [v, kid, tag] = String(signed.signature).split('.') as [string, string, string];
    const bytes = Buffer.from(tag, 'base64url');
    const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
    const variants = [
      ...[...alphabet].map((c) => tag.slice(0, -1) + c),
      ...[...alphabet].map((c) => tag + c),
    ].filter((t) => t !== tag && Buffer.from(t, 'base64url').equals(bytes));
    // Node's lenient decoder maps several strings to the same signature bytes...
    expect(variants.length).toBeGreaterThan(0);
    // ...and only the canonical one is accepted.
    for (const t of variants) {
      expect(verifyAlertSignature({ ...signed, signature: `${v}.${kid}.${t}` }, keys)).toEqual({
        ok: false,
        reason: 'malformed',
      });
    }
  });

  it('SEC-A-12: an own "__proto__" key is part of the signed bytes', () => {
    const json = JSON.stringify(signed).replace(/^\{/, '{"__proto__":{"headline":"forged"},');
    const withProto = JSON.parse(json) as Alert;
    expect(Object.prototype.hasOwnProperty.call(withProto, '__proto__')).toBe(true);
    expect(canonicalAlertBytes(withProto).toString()).toContain('__proto__');
    expect(canonicalAlertBytes(withProto).equals(canonicalAlertBytes(signed))).toBe(false);
    expect(verifyAlertSignature(withProto, keys)).toEqual({ ok: false, reason: 'bad_signature' });
  });

  it('fails for an unknown kid, and supports key rotation', () => {
    const k2 = createMemoryAlertSigning('k2');
    expect(verifyAlertSignature(signed, k2.publicKeys)).toEqual({
      ok: false,
      reason: 'unknown_key',
    });
    const both = new Map([...keys, ...k2.publicKeys]);
    expect(verifyAlertSignature(signAlertSync(alert, k2.signer), both)).toEqual({ ok: true });
    expect(verifyAlertSignature(signed, both)).toEqual({ ok: true }); // old key still accepted
  });

  it('a different private key under the same kid is a bad signature', () => {
    const impostor = createMemoryAlertSigning('k1');
    const forged = signAlertSync({ ...alert, event: 'Tornado Warning' }, impostor.signer);
    expect(verifyAlertSignature(forged, keys)).toEqual({ ok: false, reason: 'bad_signature' });
    // Everything a verifier holds is public key material.
    expect(keys.get('k1')?.type).toBe('public');
  });

  it('refuses a kid with a dot', () => {
    const dotted = createMemoryAlertSigning('k.1');
    expect(() => signAlertSync(alert, dotted.signer)).toThrow(/no "\."/);
  });

  it('re-signing after the pipeline adds h3Cells replaces the old signature', () => {
    const early = signAlertSync({ ...alert, area: { ...alert.area, h3Cells: [] } }, key);
    const matched = { ...early, area: { ...early.area, h3Cells: ['872830828ffffff'] } };
    expect(verifyAlertSignature(matched, keys)).toEqual({ ok: false, reason: 'bad_signature' });
    const resigned = signAlertSync(matched, key);
    expect(verifyAlertSignature(resigned, keys)).toEqual({ ok: true });
    expect(verifyAlertSignature({ ...resigned, signature: early.signature }, keys)).toEqual({
      ok: false,
      reason: 'bad_signature',
    });
  });
});

/** A copy of `a` that counts how often its `area` is read (one read per full verification). */
function counting(a: Alert): { alert: Alert; reads: () => number } {
  let n = 0;
  const copy = { ...a };
  const area = a.area;
  Object.defineProperty(copy, 'area', {
    enumerable: true,
    get: () => {
      n += 1;
      return area;
    },
  });
  return { alert: copy, reads: () => n };
}

describe('PERF-2: verification memo', () => {
  it('verifies the same alert object once, however many batches check it', () => {
    const verify = createAlertVerifier(keys);
    const c = counting(signed);
    for (let i = 0; i < 50; i++) expect(verify(c.alert)).toBe(true);
    expect(c.reads()).toBe(1);
  });

  it('never memoizes a failure', () => {
    const verify = createAlertVerifier(keys);
    const c = counting({ ...signed, event: 'Tornado Warning' });
    expect(verify(c.alert)).toBe(false);
    expect(verify(c.alert)).toBe(false);
    expect(c.reads()).toBe(2);
  });

  it('a row rewritten in place under the same id, sourceId and signature is checked again', () => {
    const verify = createAlertVerifier(keys);
    expect(verify(signed)).toBe(true);
    const rewritten: Alert = {
      ...signed,
      official: { ...signed.official, instruction: 'Ignore this alert.' },
    };
    expect(rewritten.id).toBe(signed.id);
    expect(rewritten.signature).toBe(signed.signature);
    expect(verify(rewritten)).toBe(false);
  });

  it('a new signature on the same object is verified again', () => {
    const verify = createAlertVerifier(keys);
    const obj: Alert = { ...signed };
    expect(verify(obj)).toBe(true);
    obj.signature = 'v2.k1.AAAA';
    expect(verify(obj)).toBe(false);
  });
});
