import { generateKeyPairSync, sign as cryptoSign, type KeyObject } from 'node:crypto';
import type { AlertPublicKeys, AlertSigner, SyncAlertSigner } from './alertSignature.js';

/** A signer and the public keys that verify it. */
export interface AlertSigning {
  signer: AlertSigner;
  publicKeys: AlertPublicKeys;
}

/** In-process ECDSA P-256 signer (same algorithm as KMS ECDSA_SHA_256). Tests and local runs. */
export class MemoryAlertSigner implements SyncAlertSigner {
  constructor(
    readonly kid: string,
    private readonly privateKey: KeyObject,
  ) {}

  signSync(message: Buffer): Buffer {
    return cryptoSign('sha256', message, this.privateKey);
  }

  sign(message: Buffer): Promise<Buffer> {
    return Promise.resolve(this.signSync(message));
  }
}

/** A fresh in-memory key pair: the signer plus a verifier key map containing its public key. */
export function createMemoryAlertSigning(kid = 'local-1'): {
  signer: MemoryAlertSigner;
  publicKey: KeyObject;
  publicKeys: AlertPublicKeys;
} {
  const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return {
    signer: new MemoryAlertSigner(kid, privateKey),
    publicKey,
    publicKeys: new Map([[kid, publicKey]]),
  };
}
