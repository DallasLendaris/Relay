import { createPrivateKey } from 'node:crypto';
import type { ServiceAccount } from './fcm-sender.js';

export interface ApnsSecret {
  keyId: string;
  teamId: string;
  bundleId: string;
  p8: string;
  sandbox: boolean;
}

const PLACEHOLDER = 'REPLACE_ME';
const REVERSE_DNS = /^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$/;
const TEN = /^[A-Za-z0-9]{10}$/;

function asRecord(raw: unknown, what: string): Record<string, unknown> {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new Error(`${what} secret is not a JSON object`);
  }
  return raw as Record<string, unknown>;
}

/** Reads a required string field. Problems name the field only: values are never put in an error. */
function field(obj: Record<string, unknown>, name: string, problems: string[]): string | undefined {
  const v = obj[name];
  if (typeof v !== 'string' || v.trim() === '') {
    problems.push(`${name} is missing`);
    return undefined;
  }
  if (v.trim().startsWith(PLACEHOLDER)) {
    problems.push(`${name} is still the ${PLACEHOLDER} placeholder`);
    return undefined;
  }
  return v;
}

function finish(what: string, problems: string[]): void {
  if (problems.length > 0) throw new Error(`${what} secret is invalid: ${problems.join('; ')}`);
}

/** Validates the APNs secret at startup; throws one error naming every bad field. */
export function validateApnsSecret(raw: unknown): ApnsSecret {
  const o = asRecord(raw, 'APNs');
  const problems: string[] = [];
  const keyId = field(o, 'keyId', problems);
  const teamId = field(o, 'teamId', problems);
  const bundleId = field(o, 'bundleId', problems);
  const p8 = field(o, 'p8', problems);
  if (keyId && !TEN.test(keyId)) problems.push('keyId must be 10 letters or digits');
  if (teamId && !TEN.test(teamId)) problems.push('teamId must be 10 letters or digits');
  if (bundleId && !REVERSE_DNS.test(bundleId)) {
    problems.push('bundleId must look like reverse DNS (com.example.app)');
  }
  if (p8) {
    try {
      const key = createPrivateKey({ key: p8, format: 'pem' });
      if (key.asymmetricKeyType !== 'ec') problems.push('p8 is not an EC private key');
    } catch {
      problems.push('p8 does not parse as a PEM private key');
    }
  }
  if (o['sandbox'] !== undefined && typeof o['sandbox'] !== 'boolean') {
    problems.push('sandbox must be a boolean');
  }
  finish('APNs', problems);
  return {
    keyId: keyId as string,
    teamId: teamId as string,
    bundleId: bundleId as string,
    p8: p8 as string,
    sandbox: o['sandbox'] === true,
  };
}

/** Validates the FCM service account at startup: project_id, client_email and a PEM private_key. */
export function validateFcmServiceAccount(raw: unknown): ServiceAccount {
  const o = asRecord(raw, 'FCM service account');
  const problems: string[] = [];
  const projectId = field(o, 'project_id', problems);
  const clientEmail = field(o, 'client_email', problems);
  const privateKey = field(o, 'private_key', problems);
  if (privateKey) {
    if (!/-----BEGIN [A-Z ]*PRIVATE KEY-----/.test(privateKey)) {
      problems.push('private_key is not PEM');
    } else {
      try {
        createPrivateKey({ key: privateKey, format: 'pem' });
      } catch {
        problems.push('private_key does not parse as a PEM private key');
      }
    }
  }
  const tokenUri = o['token_uri'];
  if (tokenUri !== undefined && typeof tokenUri !== 'string') {
    problems.push('token_uri must be a string');
  }
  finish('FCM service account', problems);
  return {
    client_email: clientEmail as string,
    private_key: privateKey as string,
    project_id: projectId as string,
    ...(typeof tokenUri === 'string' ? { token_uri: tokenUri } : {}),
  };
}
