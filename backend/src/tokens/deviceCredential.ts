import { createHmac, timingSafeEqual } from 'node:crypto';
import type { TokenKey } from './checkInToken.js';

/**
 * Stateless device credentials: `<deviceId>.<kid>.<base64url HMAC-SHA256(credentialKey, deviceId)>`.
 *
 * Why: the check-in route is the only route a million phones hit within minutes. A credential
 * that is verified by recomputing an HMAC needs no database read, so that route scales with
 * Lambda alone. Other routes (places, push token) keep the hash lookup: they are low volume,
 * and the lookup also proves the device still exists. Old random credentials have two parts
 * (`<deviceId>.<secret>`) and still verify through the stored hash; the three-part form is how
 * the two are told apart.
 *
 * The key id lets the credential key rotate: new devices get the current key, old credentials
 * keep verifying as long as their key stays in the keyring.
 */
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const mac = (secret: string, deviceId: string): string =>
  createHmac('sha256', secret).update(deviceId).digest('base64url');

export function createDeviceCredential(deviceId: string, key: TokenKey): string {
  if (key.id === '' || key.id.includes('.')) {
    throw new Error('Credential key id must be non-empty and contain no dots');
  }
  return `${deviceId}.${key.id}.${mac(key.secret, deviceId)}`;
}

/** True when the credential has the stateless three-part shape (it may still be forged). */
export function isStatelessCredential(credential: string): boolean {
  return credential.split('.').length === 3;
}

/**
 * Verifies a three-part credential and returns the device id, or null. Never throws and
 * touches no store. `keys` maps key id -> secret.
 */
export function verifyDeviceCredential(
  credential: unknown,
  keys: Map<string, string>,
): string | null {
  if (typeof credential !== 'string') return null;
  const parts = credential.split('.');
  if (parts.length !== 3) return null;
  const [deviceId, kid, given] = parts as [string, string, string];
  if (!UUID.test(deviceId) || !kid || !given) return null;
  const secret = keys.get(kid);
  if (secret === undefined) return null;
  const a = Buffer.from(given);
  const b = Buffer.from(mac(secret, deviceId));
  return a.length === b.length && timingSafeEqual(a, b) ? deviceId : null;
}
