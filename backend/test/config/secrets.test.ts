import { GetSecretValueCommand, SecretsManagerClient } from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
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

describe('loadKeyring: stale on error (SEC-A-6)', () => {
  const denied = () => Object.assign(new Error('Rate exceeded'), { name: 'ThrottlingException' });
  let warnings: string[];
  beforeEach(() => {
    warnings = [];
    vi.spyOn(console, 'warn').mockImplementation((l) => void warnings.push(String(l)));
    vi.spyOn(console, 'log').mockImplementation(() => {});
  });
  afterEach(() => vi.restoreAllMocks());

  it('serves the last good keyring when Secrets Manager errors after the cache expired', async () => {
    const client = new SecretsManagerClient({});
    sm.on(GetSecretValueCommand).resolves({ SecretString: good });
    let t = 0;
    const first = await loadKeyring(ARN, client, () => t);
    sm.on(GetSecretValueCommand).rejects(denied());
    t = 6 * 60_000;
    const second = await loadKeyring(ARN, client, () => t);
    expect(second).toBe(first);
    expect(warnings.some((w) => w.includes('config.stale_value_served'))).toBe(true);
    t = 59 * 60_000;
    expect(await loadKeyring(ARN, client, () => t)).toBe(first);
  });

  it('does not call Secrets Manager on every request during an outage', async () => {
    const client = new SecretsManagerClient({});
    sm.on(GetSecretValueCommand).resolves({ SecretString: good });
    let t = 0;
    await loadKeyring(ARN, client, () => t);
    sm.on(GetSecretValueCommand).rejects(denied());
    t = 6 * 60_000;
    for (let i = 0; i < 10; i++) await loadKeyring(ARN, client, () => t);
    expect(sm.commandCalls(GetSecretValueCommand)).toHaveLength(2);
  });

  it('retries after the backoff and recovers (a rotated keyring is picked up)', async () => {
    const client = new SecretsManagerClient({});
    sm.on(GetSecretValueCommand).resolves({ SecretString: good });
    let t = 0;
    await loadKeyring(ARN, client, () => t);
    sm.on(GetSecretValueCommand).rejects(denied());
    t = 6 * 60_000;
    await loadKeyring(ARN, client, () => t);
    sm.on(GetSecretValueCommand).resolves({
      SecretString: JSON.stringify({ current: 'k3', keys: { k3: KEY_NEW } }),
    });
    t += 20_000;
    expect((await loadKeyring(ARN, client, () => t)).current.id).toBe('k3');
  });

  it('throws once the last good keyring is over an hour old', async () => {
    const client = new SecretsManagerClient({});
    sm.on(GetSecretValueCommand).resolves({ SecretString: good });
    let t = 0;
    await loadKeyring(ARN, client, () => t);
    sm.on(GetSecretValueCommand).rejects(denied());
    t = 61 * 60_000;
    await expect(loadKeyring(ARN, client, () => t)).rejects.toThrow('Rate exceeded');
  });

  it('throws when nothing was ever loaded', async () => {
    sm.on(GetSecretValueCommand).rejects(denied());
    await expect(loadKeyring(ARN, new SecretsManagerClient({}))).rejects.toThrow('Rate exceeded');
  });
});
