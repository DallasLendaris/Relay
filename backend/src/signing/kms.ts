import { createHash, type KeyObject } from 'node:crypto';
import { GetPublicKeyCommand, KMSClient, SignCommand } from '@aws-sdk/client-kms';
import {
  createAlertVerifier,
  p256PublicKeyFromSpki,
  type AlertSigner,
  type AlertVerifier,
} from './alertSignature.js';

/*
 * AWS KMS adapters for alert signing.
 *
 * Env (exact names):
 *   ALERT_SIGNING_KMS_KEY  writers only (ingest, alert-recall): key ARN or alias of the current
 *                          ECC_NIST_P256 SIGN_VERIFY key. Needs kms:Sign on it.
 *   ALERT_SIGNING_KID      writers only: the kid written into signatures (for example
 *                          "prod-alert-v1"); must match that key's entry in ALERT_VERIFY_KEYS.
 *   ALERT_VERIFY_KEYS      every verifier: JSON array of accepted keys, each
 *                          {"kid": "...", "kmsKeyId": "<arn>"} (needs kms:GetPublicKey) or
 *                          {"kid": "...", "publicKey": "<base64 DER SPKI>"} (no AWS call).
 */

type KmsClient = Pick<KMSClient, 'send'>;

/** Signs with a KMS asymmetric key. KMS sees only the SHA-256 digest (alerts exceed 4 KB). */
export class KmsAlertSigner implements AlertSigner {
  constructor(
    private readonly client: KmsClient,
    private readonly keyId: string,
    readonly kid: string,
  ) {}

  async sign(message: Buffer): Promise<Buffer> {
    const out = await this.client.send(
      new SignCommand({
        KeyId: this.keyId,
        Message: createHash('sha256').update(message).digest(),
        MessageType: 'DIGEST',
        SigningAlgorithm: 'ECDSA_SHA_256',
      }),
    );
    if (!out.Signature || out.Signature.length === 0) {
      throw new Error('KMS Sign returned no signature');
    }
    return Buffer.from(out.Signature);
  }
}

export type VerifyKeyConfig =
  { kid: string; kmsKeyId: string } | { kid: string; publicKey: string };

/** Parses ALERT_VERIFY_KEYS. Throws a clear error (no key material in it). */
export function parseVerifyKeys(json: string | undefined): VerifyKeyConfig[] {
  if (!json) throw new Error('ALERT_VERIFY_KEYS must be set');
  let raw: unknown;
  try {
    raw = JSON.parse(json);
  } catch {
    throw new Error('ALERT_VERIFY_KEYS is not valid JSON');
  }
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error('ALERT_VERIFY_KEYS must be a non-empty JSON array');
  }
  const seen = new Set<string>();
  return raw.map((e: unknown, i): VerifyKeyConfig => {
    const o = (e ?? {}) as Record<string, unknown>;
    const kid = o.kid;
    if (typeof kid !== 'string' || kid === '' || kid.includes('.')) {
      throw new Error(`ALERT_VERIFY_KEYS[${i}].kid must be a non-empty string without "."`);
    }
    if (seen.has(kid)) throw new Error(`ALERT_VERIFY_KEYS: duplicate kid "${kid}"`);
    seen.add(kid);
    if (typeof o.kmsKeyId === 'string' && o.kmsKeyId !== '') return { kid, kmsKeyId: o.kmsKeyId };
    if (typeof o.publicKey === 'string' && o.publicKey !== '')
      return { kid, publicKey: o.publicKey };
    throw new Error(`ALERT_VERIFY_KEYS[${i}] needs "kmsKeyId" or "publicKey"`);
  });
}

/** Public keys fetched from KMS, once per container per key. A failed fetch is not cached. */
const publicKeyCache = new Map<string, Promise<KeyObject>>();
/** Verifiers by ALERT_VERIFY_KEYS value, so the success memo survives across invocations. */
const verifierCache = new Map<string, Promise<AlertVerifier>>();

/** Clears the public key and verifier caches (tests). */
export function clearAlertKeyCache(): void {
  publicKeyCache.clear();
  verifierCache.clear();
}

async function fetchKmsPublicKey(client: KmsClient, keyId: string): Promise<KeyObject> {
  const out = await client.send(new GetPublicKeyCommand({ KeyId: keyId }));
  if (out.KeySpec !== 'ECC_NIST_P256' || out.KeyUsage !== 'SIGN_VERIFY') {
    throw new Error(`KMS key ${keyId} must be ECC_NIST_P256 with KeyUsage SIGN_VERIFY`);
  }
  if (!out.SigningAlgorithms?.includes('ECDSA_SHA_256')) {
    throw new Error(`KMS key ${keyId} does not support ECDSA_SHA_256`);
  }
  if (!out.PublicKey) throw new Error(`KMS GetPublicKey returned no key for ${keyId}`);
  return p256PublicKeyFromSpki(out.PublicKey);
}

function kmsPublicKey(client: KmsClient, keyId: string): Promise<KeyObject> {
  let p = publicKeyCache.get(keyId);
  if (!p) {
    p = fetchKmsPublicKey(client, keyId);
    publicKeyCache.set(keyId, p);
    p.catch(() => publicKeyCache.delete(keyId));
  }
  return p;
}

/** Resolves every configured key to a local public key (KMS GetPublicKey at most once each). */
export async function loadAlertPublicKeys(
  config: VerifyKeyConfig[],
  client: KmsClient = new KMSClient({}),
): Promise<Map<string, KeyObject>> {
  const entries = await Promise.all(
    config.map(async (c): Promise<[string, KeyObject]> => [
      c.kid,
      'kmsKeyId' in c
        ? await kmsPublicKey(client, c.kmsKeyId)
        : p256PublicKeyFromSpki(Buffer.from(c.publicKey, 'base64')),
    ]),
  );
  return new Map(entries);
}

/**
 * The container's alert verifier, built from ALERT_VERIFY_KEYS on first use and then reused
 * (public keys and the verification memo live for the container). Never calls KMS per alert.
 */
export function alertVerifierFromEnv(
  env: Record<string, string | undefined> = process.env,
  client?: KmsClient,
): Promise<AlertVerifier> {
  const json = env.ALERT_VERIFY_KEYS ?? '';
  let p = verifierCache.get(json);
  if (!p) {
    p = (async () =>
      createAlertVerifier(await loadAlertPublicKeys(parseVerifyKeys(json), client)))();
    verifierCache.set(json, p);
    p.catch(() => verifierCache.delete(json));
  }
  return p;
}

/** The writer's signer from ALERT_SIGNING_KMS_KEY and ALERT_SIGNING_KID. */
export function alertSignerFromEnv(
  env: Record<string, string | undefined> = process.env,
  client: KmsClient = new KMSClient({}),
): KmsAlertSigner {
  const keyId = env.ALERT_SIGNING_KMS_KEY;
  const kid = env.ALERT_SIGNING_KID;
  if (!keyId || !kid) throw new Error('ALERT_SIGNING_KMS_KEY and ALERT_SIGNING_KID must be set');
  return new KmsAlertSigner(client, keyId, kid);
}
