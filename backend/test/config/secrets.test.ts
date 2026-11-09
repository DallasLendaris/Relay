import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it } from 'vitest';
import { clearKeyringCache, loadKeyring, loadSecretString } from '../../src/config/secrets.js';

const sm = mockClient(SecretsManagerClient);
const ARN = 'arn:aws:secretsmanager:us-west-2:111122223333:secret:keys';
const KEY_OLD = Buffer.alloc(32, 1).toString('base64');
const KEY_NEW = Buffer.alloc(32, 2).toString('base64');
const good = JSON.stringify({ current: 'k2', keys: { k1: KEY_OLD, k2: KEY_NEW } });

beforeEach(() => {
  sm.reset();
  clearKeyringCache();
});

describe('loadKeyring', () => {
  it('returns the current key and every key', async () => {
    sm.on(GetSecretValueCommand).resolves({ SecretString: good });
    const ring = await loadKeyring(ARN, new SecretsManagerClient({}));
    expect(ring.current).toEqual({ id: 'k2', secret: KEY_NEW });
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
      SecretString: JSON.stringify({ current: 'zz', keys: { k1: KEY_OLD } }),
    });
    await expect(loadKeyring(ARN, client)).rejects.toThrow(/current key "zz"/);
  });

  it('throws when the secret has no string value', async () => {
    sm.on(GetSecretValueCommand).resolves({});
    await expect(loadKeyring(ARN, new SecretsManagerClient({}))).rejects.toThrow(/no SecretString/);
  });
});

describe('loadKeyring placeholder keys', () => {
  const load = (keys: Record<string, string>) => {
    sm.on(GetSecretValueCommand).resolves({ SecretString: JSON.stringify({ current: 'k', keys }) });
    return loadKeyring(ARN, new SecretsManagerClient({}));
  };

  it('rejects a REPLACE_ME placeholder with a "keys not seeded" error', async () => {
    await expect(load({ k: 'REPLACE_ME_BEFORE_DEPLOY' })).rejects.toThrow(/keys not seeded/);
  });

  it('rejects a key under 32 bytes once decoded', async () => {
    const short = Buffer.alloc(31, 7).toString('base64');
    await expect(load({ k: short })).rejects.toThrow(/keys not seeded/);
  });

  it('rejects when any key in the ring is bad, and accepts exactly 32 bytes', async () => {
    await expect(load({ k: KEY_NEW, old: 'REPLACE_ME' })).rejects.toThrow(/keys not seeded/);
    await expect(load({ k: KEY_NEW })).resolves.toMatchObject({ current: { id: 'k' } });
  });
});

describe('loadSecretString', () => {
  it('returns the raw string', async () => {
    sm.on(GetSecretValueCommand).resolves({ SecretString: 'tok' });
    expect(await loadSecretString(ARN, new SecretsManagerClient({}))).toBe('tok');
  });
});
