import { createHash, generateKeyPairSync, sign as cryptoSign } from 'node:crypto';
import { GetPublicKeyCommand, KMSClient, SignCommand } from '@aws-sdk/client-kms';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { Alert } from '@relay/shared';
import {
  KmsAlertSigner,
  alertSignerFromEnv,
  alertVerifierFromEnv,
  canonicalAlertBytes,
  clearAlertKeyCache,
  parseVerifyKeys,
  signAlert,
  verifyAlertSignature,
  loadAlertPublicKeys,
} from '../../src/signing/index.js';
import { makeAlert } from '../delivery/fakes.js';

const KEY_ARN = 'arn:aws:kms:us-west-2:111111111111:key/11111111-2222-3333-4444-555555555555';
const kms = mockClient(KMSClient);
const pair = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
const spki = pair.publicKey.export({ format: 'der', type: 'spki' });

/** What KMS does for ECDSA_SHA_256 over a DIGEST: sign the digest. Tests sign the message. */
let lastMessage: Buffer | undefined;
function mockKms(): void {
  kms.on(SignCommand).callsFake((input: { Message: Uint8Array; MessageType: string }) => {
    if (!lastMessage) throw new Error('test must set lastMessage');
    // The client must send the SHA-256 of the canonical bytes, not the bytes (4 KB limit).
    expect(input.MessageType).toBe('DIGEST');
    expect(Buffer.from(input.Message)).toEqual(createHash('sha256').update(lastMessage).digest());
    return { Signature: cryptoSign('sha256', lastMessage, pair.privateKey) };
  });
  kms.on(GetPublicKeyCommand).resolves({
    KeyId: KEY_ARN,
    KeySpec: 'ECC_NIST_P256',
    KeyUsage: 'SIGN_VERIFY',
    SigningAlgorithms: ['ECDSA_SHA_256'],
    PublicKey: new Uint8Array(spki),
  });
}

beforeEach(() => {
  kms.reset();
  clearAlertKeyCache();
  mockKms();
});
afterEach(() => {
  lastMessage = undefined;
});

const env = {
  ALERT_SIGNING_KMS_KEY: KEY_ARN,
  ALERT_SIGNING_KID: 'dev-alert-v1',
  ALERT_VERIFY_KEYS: JSON.stringify([{ kid: 'dev-alert-v1', kmsKeyId: KEY_ARN }]),
};

async function kmsSigned(a: Alert): Promise<Alert> {
  lastMessage = canonicalAlertBytes(a);
  return signAlert(a, alertSignerFromEnv(env, new KMSClient({})));
}

describe('KMS alert signer and verifier (mocked KMS)', () => {
  it('signs with kms:Sign (ECDSA_SHA_256 over the digest) and verifies locally', async () => {
    const a = makeAlert({ signature: null }, ['872830828ffffff']);
    const signed = await kmsSigned(a);
    expect(signed.signature).toMatch(/^v2\.dev-alert-v1\./);
    const sign = kms.commandCalls(SignCommand);
    expect(sign).toHaveLength(1);
    expect(sign[0]?.args[0].input).toMatchObject({
      KeyId: KEY_ARN,
      SigningAlgorithm: 'ECDSA_SHA_256',
      MessageType: 'DIGEST',
    });

    const verify = await alertVerifierFromEnv(env, new KMSClient({}));
    expect(verify(signed)).toBe(true);
    expect(verify({ ...signed, event: 'Tornado Warning' })).toBe(false);
  });

  it('fetches the public key once per container and never calls KMS per alert', async () => {
    const signed = await kmsSigned(makeAlert({ signature: null }));
    const client = new KMSClient({});
    for (let i = 0; i < 20; i++) {
      const verify = await alertVerifierFromEnv(env, client); // per-invocation lookups
      expect(verify({ ...signed })).toBe(true); // a fresh object each time: full verification
    }
    expect(kms.commandCalls(GetPublicKeyCommand)).toHaveLength(1);
    expect(kms.commandCalls(SignCommand)).toHaveLength(1); // only the signing above
  });

  it('does not cache a failed public key fetch', async () => {
    kms.on(GetPublicKeyCommand).rejectsOnce(new Error('throttled'));
    await expect(alertVerifierFromEnv(env, new KMSClient({}))).rejects.toThrow('throttled');
    mockKms();
    await expect(alertVerifierFromEnv(env, new KMSClient({}))).resolves.toBeTypeOf('function');
    expect(kms.commandCalls(GetPublicKeyCommand)).toHaveLength(2);
  });

  it('refuses a KMS key that is not P-256 SIGN_VERIFY', async () => {
    kms.on(GetPublicKeyCommand).resolves({
      KeySpec: 'RSA_2048',
      KeyUsage: 'SIGN_VERIFY',
      SigningAlgorithms: ['RSASSA_PSS_SHA_256'],
      PublicKey: new Uint8Array(spki),
    });
    await expect(alertVerifierFromEnv(env, new KMSClient({}))).rejects.toThrow(/ECC_NIST_P256/);
  });

  it('accepts a configured list (rotation): an old kid by inline public key, a new one by KMS', async () => {
    const old = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
    const oldSigner = {
      kid: 'dev-alert-v0',
      sign: (m: Buffer) => Promise.resolve(cryptoSign('sha256', m, old.privateKey)),
    };
    const oldSigned = await signAlert(makeAlert({ signature: null }), oldSigner);
    const newSigned = await kmsSigned(makeAlert({ signature: null }));
    const config = parseVerifyKeys(
      JSON.stringify([
        { kid: 'dev-alert-v1', kmsKeyId: KEY_ARN },
        {
          kid: 'dev-alert-v0',
          publicKey: old.publicKey.export({ format: 'der', type: 'spki' }).toString('base64'),
        },
      ]),
    );
    const keys = await loadAlertPublicKeys(config, new KMSClient({}));
    expect(verifyAlertSignature(oldSigned, keys)).toEqual({ ok: true });
    expect(verifyAlertSignature(newSigned, keys)).toEqual({ ok: true });
  });

  it('propagates a Sign failure instead of storing an unsigned alert', async () => {
    kms.on(SignCommand).rejects(new Error('AccessDeniedException'));
    const signer = new KmsAlertSigner(new KMSClient({}), KEY_ARN, 'dev-alert-v1');
    await expect(signAlert(makeAlert({ signature: null }), signer)).rejects.toThrow(
      'AccessDeniedException',
    );
  });
});

describe('ALERT_VERIFY_KEYS and signer env', () => {
  it.each([
    [undefined, /must be set/],
    ['nope', /not valid JSON/],
    ['[]', /non-empty/],
    ['[{"kid":"a.b","kmsKeyId":"x"}]', /without "\."/],
    ['[{"kid":"a","kmsKeyId":"x"},{"kid":"a","kmsKeyId":"y"}]', /duplicate/],
    ['[{"kid":"a"}]', /kmsKeyId/],
  ])('rejects %s', (json, err) => {
    expect(() => parseVerifyKeys(json)).toThrow(err);
  });

  it('the signer needs both ALERT_SIGNING_KMS_KEY and ALERT_SIGNING_KID', () => {
    expect(() => alertSignerFromEnv({ ALERT_SIGNING_KMS_KEY: KEY_ARN })).toThrow(/must be set/);
  });
});
