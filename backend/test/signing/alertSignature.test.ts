import { describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import {
  canonicalAlertBytes,
  createAlertVerifier,
  signAlert,
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

const key = { id: 'k1', secret: 'alert-secret-one' };
const keys = new Map([['k1', 'alert-secret-one']]);
const signed = signAlert(alert, key);

describe('alert signing', () => {
  it('signs and verifies', () => {
    expect(signed.signature).toMatch(/^v1\.k1\.[A-Za-z0-9_-]+$/);
    expect(verifyAlertSignature(signed, keys)).toEqual({ ok: true });
    expect(createAlertVerifier(keys)(signed)).toBe(true);
    expect(alert.signature).toBeNull(); // original untouched
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
    for (const junk of ['garbage', 'v2.k1.abc', 'v1.k1', 'v1..abc', 'v1.k1.a.b']) {
      expect(verifyAlertSignature({ ...signed, signature: junk }, keys)).toEqual({
        ok: false,
        reason: 'malformed',
      });
    }
    const bad = { ...signed, signature: 42 } as unknown as Alert;
    expect(verifyAlertSignature(bad, keys)).toEqual({ ok: false, reason: 'malformed' });
    // A truncated tag is a bad signature, and must not throw.
    const short = { ...signed, signature: 'v1.k1.AAAA' };
    expect(verifyAlertSignature(short, keys)).toEqual({ ok: false, reason: 'bad_signature' });
  });

  it('fails for an unknown kid, and supports key rotation', () => {
    expect(verifyAlertSignature(signed, new Map([['k2', 'other']]))).toEqual({
      ok: false,
      reason: 'unknown_key',
    });
    const newKey = { id: 'k2', secret: 'alert-secret-two' };
    const both = new Map([...keys, ['k2', newKey.secret]]);
    expect(verifyAlertSignature(signAlert(alert, newKey), both)).toEqual({ ok: true });
    expect(verifyAlertSignature(signed, both)).toEqual({ ok: true }); // old key still accepted
  });

  it('a wrong secret under the same kid is a bad signature', () => {
    expect(verifyAlertSignature(signed, new Map([['k1', 'nope']]))).toEqual({
      ok: false,
      reason: 'bad_signature',
    });
  });

  it('re-signing after the pipeline adds h3Cells replaces the old signature', () => {
    const early = signAlert({ ...alert, area: { ...alert.area, h3Cells: [] } }, key);
    const matched = { ...early, area: { ...early.area, h3Cells: ['872830828ffffff'] } };
    expect(verifyAlertSignature(matched, keys)).toEqual({ ok: false, reason: 'bad_signature' });
    const resigned = signAlert(matched, key);
    expect(verifyAlertSignature(resigned, keys)).toEqual({ ok: true });
    expect(verifyAlertSignature({ ...resigned, signature: early.signature }, keys)).toEqual({
      ok: false,
      reason: 'bad_signature',
    });
  });
});
