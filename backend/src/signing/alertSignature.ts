import { createHmac, timingSafeEqual } from 'node:crypto';
import type { Alert } from '@relay/shared';

/*
 * Alert signing. The ingest pipeline signs an alert after it passes validation;
 * the delivery step refuses anything unsigned or altered, so a row inserted
 * straight into the database can never be sent.
 *
 * KEY MANAGEMENT
 * - This key is SEPARATE from the check-in token key (src/tokens). Leaking one
 *   must not let an attacker forge the other.
 * - It comes from AWS Secrets Manager. Never commit it or put it in env files.
 * - To rotate, add the new key to the map, sign with it, and keep the old one
 *   until alerts signed with it have expired.
 * - Later, if agencies need to verify our signatures themselves, switch to KMS
 *   asymmetric signing (agencies get the public key; the private key never
 *   leaves KMS). HMAC is symmetric, so anyone who can verify can also forge.
 */

/** A signing key. `id` is written into every signature as the kid. */
export interface SigningKey {
  id: string;
  secret: string;
}

export type SignatureFailure = 'unsigned' | 'malformed' | 'unknown_key' | 'bad_signature';
export type SignatureResult = { ok: true } | { ok: false; reason: SignatureFailure };

/** Recursively sort object keys; array order is meaningful and kept. */
function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(value).sort()) {
      out[k] = sortKeys((value as Record<string, unknown>)[k]);
    }
    return out;
  }
  return value;
}

/** Deterministic JSON of the alert without its `signature` field. */
export function canonicalAlertBytes(alert: Alert): Buffer {
  const { signature: _signature, ...rest } = alert;
  return Buffer.from(JSON.stringify(sortKeys(rest)), 'utf8');
}

const mac = (secret: string, alert: Alert): Buffer =>
  createHmac('sha256', secret).update(canonicalAlertBytes(alert)).digest();

/** Returns a copy of the alert with signature = `v1.<kid>.<base64url HMAC-SHA256>`. */
export function signAlert(alert: Alert, key: SigningKey): Alert {
  // The signature is split on ".", so a dot in the kid would make it unparseable.
  if (key.id.includes('.')) throw new Error('signing key id must not contain "."');
  return { ...alert, signature: `v1.${key.id}.${mac(key.secret, alert).toString('base64url')}` };
}

/** Check an alert's signature. Never throws: any bad input yields { ok: false }. */
export function verifyAlertSignature(alert: Alert, keys: Map<string, string>): SignatureResult {
  try {
    const sig: unknown = alert.signature;
    if (sig === null || sig === undefined || sig === '') return { ok: false, reason: 'unsigned' };
    if (typeof sig !== 'string') return { ok: false, reason: 'malformed' };
    const parts = sig.split('.');
    const [version, kid, tag] = parts;
    if (parts.length !== 3 || version !== 'v1' || !kid || !tag) {
      return { ok: false, reason: 'malformed' };
    }
    const secret = keys.get(kid);
    if (secret === undefined) return { ok: false, reason: 'unknown_key' };

    const given = Buffer.from(tag, 'base64url');
    const expected = mac(secret, alert);
    // Length check first: timingSafeEqual throws on unequal lengths.
    if (given.length !== expected.length || !timingSafeEqual(given, expected)) {
      return { ok: false, reason: 'bad_signature' };
    }
    return { ok: true };
  } catch {
    return { ok: false, reason: 'malformed' };
  }
}

/** Adapter for the delivery step's `verifyAlert` hook. */
export function createAlertVerifier(keys: Map<string, string>): (alert: Alert) => boolean {
  return (alert) => verifyAlertSignature(alert, keys).ok;
}
