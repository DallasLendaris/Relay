import { createHmac, timingSafeEqual } from 'node:crypto';

/** A signing key. `id` is written into every token as `kid`. */
export interface TokenKey {
  id: string;
  secret: string;
}

export interface CreateParams {
  alertId: string;
  deviceId: string;
  /** When the alert expires; the token stops working then. */
  expiresAt: Date;
  /** The H3 cell the device was matched in. Signed into the token (v2), so check-in needs no place lookup. */
  h3Cell: string;
}

/** Parameters for a legacy v1 token (no cell). Kept so tests and old clients can be modelled. */
export type CreateParamsV1 = Omit<CreateParams, 'h3Cell'>;

export interface VerifyParams {
  alertId: string;
  deviceId: string;
  /** Injectable clock for tests; defaults to the current time. */
  now?: Date;
}

export type VerifyFailure =
  'malformed' | 'unknown_key' | 'bad_signature' | 'wrong_alert' | 'wrong_device' | 'expired';

/**
 * `h3Cell` is the cell bound into a v2 token. It is null for a v1 token, which carries no cell;
 * the caller must then fall back to a place lookup (v1 tokens exist only until their alerts expire).
 */
export type VerifyResult =
  { ok: true; h3Cell: string | null } | { ok: false; reason: VerifyFailure };

/** Allowed difference between our clock and the device/server clock. */
export const CLOCK_SKEW_SECONDS = 60;

interface PayloadV1 {
  v: 1;
  a: string; // alert id
  d: string; // device id
  exp: number; // unix seconds
  kid: string;
}

interface PayloadV2 {
  v: 2;
  a: string; // alert id
  d: string; // device id
  c: string; // H3 cell the device was matched in
  exp: number; // unix seconds
  kid: string;
}

const sign = (secret: string, data: string): string =>
  createHmac('sha256', secret).update(data).digest('base64url');

const encode = (payload: PayloadV1 | PayloadV2, key: TokenKey): string => {
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  return `${body}.${sign(key.secret, body)}`;
};

/**
 * token (v2) = base64url(JSON {v:2, a, d, c, exp, kid}) + "." + base64url(HMAC-SHA256(secret, first part))
 * The signature binds alert, device and cell, so the check-in API can trust the cell with no lookup.
 */
export function createCheckInToken(p: CreateParams, key: TokenKey): string {
  return encode(
    {
      v: 2,
      a: p.alertId,
      d: p.deviceId,
      c: p.h3Cell,
      exp: Math.floor(p.expiresAt.getTime() / 1000),
      kid: key.id,
    },
    key,
  );
}

/** Legacy v1 token (no cell). Production no longer issues these; they are accepted until they expire. */
export function createCheckInTokenV1(p: CreateParamsV1, key: TokenKey): string {
  return encode(
    {
      v: 1,
      a: p.alertId,
      d: p.deviceId,
      exp: Math.floor(p.expiresAt.getTime() / 1000),
      kid: key.id,
    },
    key,
  );
}

/**
 * Verify a token. Never throws: any bad input yields { ok: false }.
 *
 * `keys` maps key id -> secret. Keeping several keys at once lets the team
 * rotate the secret: sign new tokens with the new key while tokens already
 * sent (signed with the old key) still verify until their alert expires.
 */
export function verifyCheckInToken(
  token: unknown,
  params: VerifyParams,
  keys: Map<string, string>,
): VerifyResult {
  try {
    if (typeof token !== 'string') return fail('malformed');
    const parts = token.split('.');
    if (parts.length !== 2 || !parts[0] || !parts[1]) return fail('malformed');
    const [body, sig] = parts as [string, string];

    // The payload is untrusted until the signature checks out. We only read
    // `kid` early, because we need it to find the key.
    const claims = parseJson(body);
    if (!claims || typeof claims.kid !== 'string') return fail('malformed');

    const secret = keys.get(claims.kid);
    if (secret === undefined) return fail('unknown_key');

    // Compare the signature strings as bytes, so a non-canonical base64url
    // encoding of the same signature is also rejected.
    const given = Buffer.from(sig);
    const expected = Buffer.from(sign(secret, body));
    if (given.length !== expected.length || !timingSafeEqual(given, expected)) {
      return fail('bad_signature');
    }

    // Signature is valid, so the payload can now be trusted (shape-check anyway).
    const version = claims.v;
    if (
      (version !== 1 && version !== 2) ||
      (version === 2 && (typeof claims.c !== 'string' || claims.c === '')) ||
      typeof claims.a !== 'string' ||
      typeof claims.d !== 'string' ||
      typeof claims.exp !== 'number' ||
      !Number.isFinite(claims.exp)
    ) {
      return fail('malformed');
    }
    if (claims.a !== params.alertId) return fail('wrong_alert');
    if (claims.d !== params.deviceId) return fail('wrong_device');

    const nowSec = Math.floor((params.now ?? new Date()).getTime() / 1000);
    if (nowSec > claims.exp + CLOCK_SKEW_SECONDS) return fail('expired');
    return { ok: true, h3Cell: version === 2 ? (claims.c as string) : null };
  } catch {
    return fail('malformed');
  }
}

const fail = (reason: VerifyFailure): VerifyResult => ({ ok: false, reason });

function parseJson(body: string): Record<string, unknown> | null {
  try {
    const value: unknown = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
    return value !== null && typeof value === 'object' && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}
