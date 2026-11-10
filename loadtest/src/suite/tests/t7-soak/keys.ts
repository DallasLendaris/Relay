/** Fixed test keys shared by the t7 processes (check-in tokens, device credentials, alert signing). */
import { createPrivateKey, createPublicKey, generateKeyPairSync } from 'node:crypto';
import { MemoryAlertSigner } from '../../../../../backend/src/signing/memory.js';

export const TOKEN_KEY = { id: 'soak-token', secret: 'soak-token-secret' };
export const DEVICE_CRED_KEY = { id: 'soak-cred', secret: 'soak-cred-secret' };
export const ALERT_KID = 'soak-alert';

/** A fresh alert signing key as PKCS8 PEM; the parent makes one and hands it to the processes by env. */
export function newAlertKeyPem(): string {
  const { privateKey } = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
}

export function alertSigningFromPem(pem: string) {
  const privateKey = createPrivateKey(pem);
  const publicKey = createPublicKey(privateKey);
  return {
    signer: new MemoryAlertSigner(ALERT_KID, privateKey),
    publicKeys: new Map([[ALERT_KID, publicKey]]),
  };
}
