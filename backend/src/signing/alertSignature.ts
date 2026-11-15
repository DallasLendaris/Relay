import { createPublicKey, verify as cryptoVerify, type KeyObject } from 'node:crypto';
import type { Alert } from '@relay/shared';

/*
 * Alert signing. The ingest pipeline signs an alert after it passes validation; every step that
 * acts on a stored alert (planner, shard, sender, late-join, delivery worker, ingest itself)
 * refuses anything unsigned or altered, so a row inserted straight into the database can never
 * be sent.
 *
 * KEY MANAGEMENT (SEC-C-7, RED-2)
 * - Signatures are ECDSA P-256 over SHA-256 (AWS KMS ECC_NIST_P256 / ECDSA_SHA_256). The private
 *   key never leaves KMS. Only the alert writers (the ingest Lambda and the operator running
 *   scripts/alert-recall.ts) hold kms:Sign; everything else holds kms:GetPublicKey only, fetches
 *   the public key once per container and verifies locally. Holding the verifier therefore no
 *   longer lets anyone forge an alert (the old HMAC key did).
 * - This key is separate from the check-in token and device-credential keyrings (src/tokens).
 * - Rotation: a verifier accepts a list of public keys by kid. Add the new key to the list,
 *   switch the signer, and drop the old key after alerts signed with it have expired.
 *
 * Signature format: `v2.<kid>.<base64url DER ECDSA signature>`. `v1` (HMAC) is no longer
 * accepted.
 */

export const SIGNATURE_VERSION = 'v2';

/** Writes signatures. The private key lives behind this port (KMS in AWS, memory in tests). */
export interface AlertSigner {
  /** Written into every signature. Must not contain "." (the signature is split on it). */
  readonly kid: string;
  /** DER-encoded ECDSA P-256 signature over SHA-256(message). */
  sign(message: Buffer): Promise<Buffer>;
}

/** Public keys a verifier accepts, by kid. */
export type AlertPublicKeys = ReadonlyMap<string, KeyObject>;

export type SignatureFailure = 'unsigned' | 'malformed' | 'unknown_key' | 'bad_signature';
export type SignatureResult = { ok: true } | { ok: false; reason: SignatureFailure };

/**
 * Recursively sort object keys; array order is meaningful and kept. Output objects have a null
 * prototype so an own "__proto__" key (JSON.parse creates one) is kept as data instead of being
 * swallowed by the Object.prototype setter (SEC-A-12).
 */
function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value !== null && typeof value === 'object') {
    const out = Object.create(null) as Record<string, unknown>;
    for (const k of Object.keys(value).sort()) {
      out[k] = sortKeys((value as Record<string, unknown>)[k]);
    }
    return out;
  }
  return value;
}

/** Deterministic JSON of the alert without its `signature` field. */
export function canonicalAlertBytes(alert: Alert): Buffer {
  const rest = Object.create(null) as Record<string, unknown>;
  for (const k of Object.keys(alert)) {
    if (k !== 'signature') rest[k] = (alert as unknown as Record<string, unknown>)[k];
  }
  return Buffer.from(JSON.stringify(sortKeys(rest)), 'utf8');
}

const KID_RE = /^[A-Za-z0-9_:/-]{1,128}$/;
const TAG_RE = /^[A-Za-z0-9_-]+$/;

/** A signer that can also sign synchronously (the in-memory one). */
export interface SyncAlertSigner extends AlertSigner {
  signSync(message: Buffer): Buffer;
}

function assertKid(kid: string): void {
  if (!KID_RE.test(kid)) {
    throw new Error('signing key id must be 1-128 chars of [A-Za-z0-9_:/-] (no ".")');
  }
}

/** The order of the P-256 group; (n - 1) / 2 is the largest "low" S. */
const P256_N = 0xffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551n;
const P256_HALF_N = P256_N >> 1n;

interface DerSig {
  r: bigint;
  s: bigint;
}

/** Strict parse of SEQUENCE { INTEGER r, INTEGER s } (short-form lengths: P-256 fits). */
function parseDerSignature(der: Uint8Array): DerSig | null {
  const b = Buffer.from(der);
  if (b.length < 8 || b[0] !== 0x30 || b[1] !== b.length - 2 || b[1] === undefined) return null;
  let off = 2;
  const ints: bigint[] = [];
  for (let i = 0; i < 2; i++) {
    if (b[off] !== 0x02) return null;
    const len = b[off + 1];
    if (len === undefined || len === 0 || off + 2 + len > b.length) return null;
    ints.push(BigInt(`0x${b.subarray(off + 2, off + 2 + len).toString('hex')}`));
    off += 2 + len;
  }
  if (off !== b.length) return null;
  const [r, s] = ints;
  return r === undefined || s === undefined ? null : { r, s };
}

function derInt(v: bigint): Buffer {
  let hex = v.toString(16);
  if (hex.length % 2) hex = `0${hex}`;
  let body = Buffer.from(hex, 'hex');
  if (body.length === 0 || (body[0] ?? 0) >= 0x80) body = Buffer.concat([Buffer.of(0), body]);
  return Buffer.concat([Buffer.of(0x02, body.length), body]);
}

function encodeDerSignature({ r, s }: DerSig): Buffer {
  const body = Buffer.concat([derInt(r), derInt(s)]);
  return Buffer.concat([Buffer.of(0x30, body.length), body]);
}

/** True when the DER signature parses and its S is at most n/2 (the canonical "low-S" form). */
export function isLowS(der: Uint8Array): boolean {
  const sig = parseDerSignature(der);
  return sig !== null && sig.s > 0n && sig.s <= P256_HALF_N;
}

/**
 * Rewrites a high-S signature to its low-S twin (s -> n - s); a low-S one is returned as is.
 * KMS may return either form. (r, s) and (r, n - s) both verify, so verifiers insist on low-S
 * (see verifyAlertSignature) and signers normalize with this. Throws on a non-DER input.
 */
export function toLowS(der: Uint8Array): Buffer {
  const sig = parseDerSignature(der);
  if (!sig) throw new Error('signature is not a DER ECDSA signature');
  return sig.s <= P256_HALF_N
    ? Buffer.from(der)
    : encodeDerSignature({ r: sig.r, s: P256_N - sig.s });
}

const withSignature = (alert: Alert, kid: string, der: Buffer): Alert => ({
  ...alert,
  signature: `${SIGNATURE_VERSION}.${kid}.${toLowS(der).toString('base64url')}`,
});

/** Returns a copy of the alert with signature = `v2.<kid>.<base64url DER ECDSA>`. */
export async function signAlert(alert: Alert, signer: AlertSigner): Promise<Alert> {
  assertKid(signer.kid);
  return withSignature(alert, signer.kid, await signer.sign(canonicalAlertBytes(alert)));
}

/** signAlert for an in-memory signer, without a promise (tests and fixtures). */
export function signAlertSync(alert: Alert, signer: SyncAlertSigner): Alert {
  assertKid(signer.kid);
  return withSignature(alert, signer.kid, signer.signSync(canonicalAlertBytes(alert)));
}

/** Check an alert's signature against local public keys. Never throws; never calls KMS. */
export function verifyAlertSignature(alert: Alert, keys: AlertPublicKeys): SignatureResult {
  try {
    const sig: unknown = alert.signature;
    if (sig === null || sig === undefined || sig === '') return { ok: false, reason: 'unsigned' };
    if (typeof sig !== 'string') return { ok: false, reason: 'malformed' };
    const parts = sig.split('.');
    const [version, kid, tag] = parts;
    if (parts.length !== 3 || version !== SIGNATURE_VERSION || !kid || !tag || !TAG_RE.test(tag)) {
      return { ok: false, reason: 'malformed' };
    }
    const given = Buffer.from(tag, 'base64url');
    // SEC-A-9: Node's base64url decoder is lenient (ignores trailing bits and junk), so many
    // strings decode to the same bytes. Only the one canonical encoding is accepted, which makes
    // the signature string itself a stable identity (the verification memo relies on it).
    if (given.toString('base64url') !== tag) return { ok: false, reason: 'malformed' };
    const key = keys.get(kid);
    if (key === undefined) return { ok: false, reason: 'unknown_key' };
    // Malleability: (r, s) and (r, n - s) both verify, so a second valid string for the same
    // alert could be minted from a captured one. Only the low-S form is accepted.
    if (!isLowS(given)) return { ok: false, reason: 'bad_signature' };
    // OpenSSL rejects non-DER (re-encoded) ECDSA signatures.
    const ok = cryptoVerify('sha256', canonicalAlertBytes(alert), key, given);
    return ok ? { ok: true } : { ok: false, reason: 'bad_signature' };
  } catch {
    return { ok: false, reason: 'malformed' };
  }
}

export type AlertVerifier = (alert: Alert) => boolean;

/**
 * The delivery step's `verifyAlert` hook. PERF-2: verifying means canonicalizing the whole alert
 * (82k cells is ~6 ms), and the sender checks the same alert on every batch. A SUCCESS is
 * memoized per alert object (WeakMap, so entries die with the alert caches that hold them) and
 * per signature string: the sender's and planner's alert caches hand every batch the same object
 * for their TTL, so each cached alert version is verified once per container per cache fill.
 * A failure is never memoized.
 *
 * Why not memoize by (alertId, sourceId, signature) alone: that would trust a row rewritten in
 * place (new text or cells, old signature) for as long as the entry lived, which is exactly the
 * tampering the signature exists to catch. A row re-read from the database is a new object and
 * is verified again.
 */
export function createAlertVerifier(keys: AlertPublicKeys): AlertVerifier {
  const verified = new WeakMap<object, string>();
  return (alert: Alert): boolean => {
    const sig: unknown = alert.signature;
    if (typeof sig === 'string' && sig !== '' && verified.get(alert) === sig) return true;
    const ok = verifyAlertSignature(alert, keys).ok;
    // The memo trusts the OBJECT, so the object must not change afterwards: freeze it (deeply)
    // before remembering it. A later in-place write then throws instead of riding the memo.
    // An alert that cannot be frozen (accessor properties) is simply not memoized.
    if (ok && typeof sig === 'string' && deepFreeze(alert)) verified.set(alert, sig);
    return ok;
  };
}

/** Freezes a value and everything reachable by data properties. False if it has accessors. */
function deepFreeze(root: object): boolean {
  const stack: object[] = [root];
  const seen = new Set<object>();
  let plain = true;
  while (stack.length > 0) {
    const o = stack.pop() as object;
    if (seen.has(o)) continue;
    seen.add(o);
    for (const k of Reflect.ownKeys(o)) {
      const d = Object.getOwnPropertyDescriptor(o, k);
      if (!d) continue;
      if (d.get || d.set) plain = false;
      else if (d.value !== null && typeof d.value === 'object') stack.push(d.value as object);
    }
    Object.freeze(o);
  }
  return plain;
}

/** Parses a DER SubjectPublicKeyInfo (what KMS GetPublicKey returns) into a P-256 key. */
export function p256PublicKeyFromSpki(der: Uint8Array): KeyObject {
  const key = createPublicKey({ key: Buffer.from(der), format: 'der', type: 'spki' });
  if (key.asymmetricKeyType !== 'ec' || key.asymmetricKeyDetails?.namedCurve !== 'prime256v1') {
    throw new Error('alert verify key must be an EC P-256 public key');
  }
  return key;
}
