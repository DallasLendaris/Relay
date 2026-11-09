import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import { clearKeyringCache, loadKeyring, loadSecretString } from '../../src/config/secrets.js';

const sm = mockClient(SecretsManagerClient);
const ARN = 'arn:aws:secretsmanager:us-west-2:111122223333:secret:keys';
const good = JSON.stringify({ current: 'k2', keys: { k1: 'old', k2: 'new' } });

beforeEach(() => {
  sm.reset();
  clearKeyringCache();
});

describe('loadKeyring', () => {
  it('returns the current key and every key', async () => {
    sm.on(GetSecretValueCommand).resolves({ SecretString: good });
    const ring = await loadKeyring(ARN, new SecretsManagerClient({}));
    expect(ring.current).toEqual({ id: 'k2', secret: 'new' });
    expect([...ring.all.keys()]).toEqual(['k1', 'k2']);
  });

  it('caches for 5 minutes, then reloads', async () => {
    sm.on(GetSecretValueCommand).resolves({ SecretString: good });
    const client = new SecretsManagerClient({});
    let t = 1_000;
    await loadKeyring(ARN, client, () => t);
    t += 4 * 60_000;
    await loadKeyring(ARN, client, () => t);
    expect(sm.commandCalls(GetSecretValueCommand)).toHaveLength(1);
    t += 2 * 60_000;
    await loadKeyring(ARN, client, () => t);
    expect(sm.commandCalls(GetSecretValueCommand)).toHaveLength(2);
  });

  it('throws a clear error for bad JSON', async () => {
    sm.on(GetSecretValueCommand).resolves({ SecretString: '{nope' });
    await expect(loadKeyring(ARN, new SecretsManagerClient({}))).rejects.toThrow(
      /is not valid JSON/,
    );
  });

  it('throws when the shape is wrong or current is missing from keys', async () => {
    const client = new SecretsManagerClient({});
    sm.on(GetSecretValueCommand).resolves({ SecretString: '{"keys":{}}' });
    await expect(loadKeyring(ARN, client)).rejects.toThrow(/must look like/);
    sm.on(GetSecretValueCommand).resolves({
      SecretString: JSON.stringify({ current: 'zz', keys: { k1: 'a' } }),
    });
    await expect(loadKeyring(ARN, client)).rejects.toThrow(/current key "zz"/);
  });

  it('throws when the secret has no string value', async () => {
    sm.on(GetSecretValueCommand).resolves({});
    await expect(loadKeyring(ARN, new SecretsManagerClient({}))).rejects.toThrow(/no SecretString/);
  });
});

describe('loadSecretString', () => {
  it('returns the raw string', async () => {
    sm.on(GetSecretValueCommand).resolves({ SecretString: 'tok' });
    expect(await loadSecretString(ARN, new SecretsManagerClient({}))).toBe('tok');
  });
});
