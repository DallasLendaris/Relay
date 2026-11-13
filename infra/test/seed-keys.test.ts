import { describe, expect, it } from 'vitest';
import { KEY_PLACEHOLDER } from '../lib/config-stack.js';
import { KEYRING_NAMES, planSeed, type Keyring } from '../scripts/seed-keys.js';

const FRESH = 'bmV3LWtleS1ieXRlcy1ub3QtcmVhbC0zMi1ieXRlcyE=';

describe('seed-keys (SEC-C-5 / SEC-A-3 / OPS-8)', () => {
  it('no longer manages an alert-signing keyring (alerts use KMS)', () => {
    expect(KEYRING_NAMES).toEqual(['token-keys', 'device-credential-keys']);
  });

  it('replaces the CDK placeholder in place', () => {
    const plan = planSeed({ current: 'k1', keys: { k1: KEY_PLACEHOLDER } }, false, FRESH);
    expect(plan).toEqual({
      action: 'write',
      current: 'k1',
      ring: { current: 'k1', keys: { k1: FRESH }, highestKid: 1 },
    });
  });

  it('skips a seeded ring without --rotate', () => {
    expect(planSeed({ current: 'k1', keys: { k1: 'old' } }, false, FRESH).action).toBe('skip');
  });

  it('rotation after a hand deletion never overwrites the live key (the probe)', () => {
    // k1 retired by hand; k3 is live. The old code picked k{count+1} = k3 and overwrote it.
    const ring: Keyring = { current: 'k3', keys: { k2: 'two', k3: 'three' } };
    const plan = planSeed(ring, true, FRESH);
    expect(plan.action).toBe('write');
    if (plan.action !== 'write') return;
    expect(plan.current).toBe('k4');
    expect(plan.ring.keys).toEqual({ k2: 'two', k3: 'three', k4: FRESH });
  });

  it('never reissues a kid removed by hand (high-water mark)', () => {
    // k5 was issued and then deleted; k4 is current again.
    const ring: Keyring = { current: 'k4', keys: { k4: 'four' }, highestKid: 5 };
    const plan = planSeed(ring, true, FRESH);
    expect(plan.action === 'write' && plan.current).toBe('k6');
    expect(plan.action === 'write' && plan.ring.highestKid).toBe(6);
  });

  it('refuses rings it does not understand instead of writing', () => {
    expect(() => planSeed({ current: 'x', keys: { x: 'a' } }, true, FRESH)).toThrow(/kid/);
    expect(() =>
      planSeed({ current: 'k2', keys: { k1: KEY_PLACEHOLDER, k2: 'b' } }, true, FRESH),
    ).toThrow(/placeholder/);
  });
});
