import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';

/** Shape of a keyring secret: {"current": "<kid>", "keys": {"<kid>": "<base64 secret>"}}. */
export interface Keyring {
  current: { id: string; secret: string };
  /** Every key accepted for verification (includes current), by key id. */
  all: Map<string, string>;
}

/** A key must decode to at least this many bytes. */
const MIN_KEY_BYTES = 32;
const TTL_MS = 5 * 60 * 1000;
const cache = new Map<string, { at: number; value: Keyring }>();

type SecretsClient = Pick<SecretsManagerClient, 'send'>;

/** Clears the keyring cache (tests). */
export function clearKeyringCache(): void {
  cache.clear();
}

/** Parses and validates the keyring JSON. Throws a clear error naming the secret. */
export function parseKeyring(secretString: string, secretArn: string): Keyring {
  let raw: unknown;
  try {
    raw = JSON.parse(secretString);
  } catch {
    throw new Error(`Secret ${secretArn} is not valid JSON`);
  }
  const obj = raw as { current?: unknown; keys?: unknown } | null;
  if (!obj || typeof obj.current !== 'string' || !obj.keys || typeof obj.keys !== 'object') {
    throw new Error(`Secret ${secretArn} must look like {"current": "<kid>", "keys": {...}}`);
  }
  const all = new Map<string, string>();
  for (const [kid, secret] of Object.entries(obj.keys as Record<string, unknown>)) {
    if (typeof secret !== 'string' || secret === '') {
      throw new Error(`Secret ${secretArn}: key "${kid}" must be a non-empty string`);
    }
    if (secret.startsWith('REPLACE_ME') || Buffer.from(secret, 'base64').length < MIN_KEY_BYTES) {
      // The CDK seeds a placeholder; signing with it would mean signing with a public string.
      throw new Error(
        `Secret ${secretArn}: keys not seeded (key "${kid}" is a placeholder or shorter than ` +
          `${MIN_KEY_BYTES} bytes). Put real base64 keys in the secret before deploying.`,
      );
    }
    all.set(kid, secret);
  }
  const currentSecret = all.get(obj.current);
  if (currentSecret === undefined) {
    throw new Error(`Secret ${secretArn}: current key "${obj.current}" is not in keys`);
  }
  return { current: { id: obj.current, secret: currentSecret }, all };
}

/** Loads a keyring from Secrets Manager, cached for 5 minutes per secret. */
export async function loadKeyring(
  secretArn: string,
  client: SecretsClient = new SecretsManagerClient({}),
  now: () => number = Date.now,
): Promise<Keyring> {
  const hit = cache.get(secretArn);
  if (hit && now() - hit.at < TTL_MS) return hit.value;
  const out = await client.send(new GetSecretValueCommand({ SecretId: secretArn }));
  if (!out.SecretString) throw new Error(`Secret ${secretArn} has no SecretString`);
  const value = parseKeyring(out.SecretString, secretArn);
  cache.set(secretArn, { at: now(), value });
  return value;
}

/** Reads a plain-string secret (for example the Expo access token). Not cached. */
export async function loadSecretString(
  secretArn: string,
  client: SecretsClient = new SecretsManagerClient({}),
): Promise<string> {
  const out = await client.send(new GetSecretValueCommand({ SecretId: secretArn }));
  if (!out.SecretString) throw new Error(`Secret ${secretArn} has no SecretString`);
  return out.SecretString;
}
