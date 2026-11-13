import { createPrivateKey, sign, type KeyObject } from 'node:crypto';

const b64url = (input: Buffer | string): string => Buffer.from(input).toString('base64url');

/**
 * Sign a compact JWS. ES256 signatures use the raw r||s (IEEE P1363) form that JWT requires,
 * not Node's default DER form.
 */
export function signJwt(
  alg: 'ES256' | 'RS256',
  header: Record<string, unknown>,
  payload: Record<string, unknown>,
  privateKeyPem: string | KeyObject,
): string {
  const key = typeof privateKeyPem === 'string' ? createPrivateKey(privateKeyPem) : privateKeyPem;
  const signingInput = `${b64url(JSON.stringify({ alg, typ: 'JWT', ...header }))}.${b64url(
    JSON.stringify(payload),
  )}`;
  const signature =
    alg === 'ES256'
      ? sign('sha256', Buffer.from(signingInput), { key, dsaEncoding: 'ieee-p1363' })
      : sign('RSA-SHA256', Buffer.from(signingInput), key);
  return `${signingInput}.${b64url(signature)}`;
}

/** Short error label that never includes message text (which could echo a token). */
export function errName(err: unknown): string {
  if (err && typeof err === 'object') {
    const code = (err as { code?: unknown }).code;
    if (typeof code === 'string') return code;
    const name = (err as { name?: unknown }).name;
    if (typeof name === 'string') return name;
  }
  return 'Error';
}
