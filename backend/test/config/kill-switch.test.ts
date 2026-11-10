import { GetParameterCommand, SSMClient } from '@aws-sdk/client-ssm';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createKillSwitch } from '../../src/config/kill-switch.js';

const ssm = mockClient(SSMClient);
const value = (v: string) => ({ Parameter: { Value: v } });
const throttled = () => Object.assign(new Error('Rate exceeded'), { name: 'ThrottlingException' });

let logs: string[];
beforeEach(() => {
  ssm.reset();
  logs = [];
  const capture = (...a: unknown[]) => void logs.push(String(a[0]));
  vi.spyOn(console, 'log').mockImplementation(capture);
  vi.spyOn(console, 'warn').mockImplementation(capture);
  vi.spyOn(console, 'error').mockImplementation(capture);
});
afterEach(() => vi.restoreAllMocks());

const logged = (msg: string) =>
  logs.map((l) => JSON.parse(l) as Record<string, unknown>).filter((l) => l['msg'] === msg);

describe('kill switch', () => {
  it('accepts "running" and "paused" (any case, trimmed)', async () => {
    const client = new SSMClient({});
    for (const [raw, paused] of [
      ['paused', true],
      [' Paused ', true],
      ['running', false],
      ['RUNNING\n', false],
    ] as const) {
      ssm.on(GetParameterCommand).resolves(value(raw));
      expect(await createKillSwitch('/relay/kill', client)()).toBe(paused);
    }
    expect(logged('killswitch.invalid_value')).toHaveLength(0);
  });

  it('OPS-7: any other value is treated as paused (fail closed) and logged as an error with a metric', async () => {
    const client = new SSMClient({});
    for (const typo of ['pause', 'paused!', 'true', 'stop', '', 'off', 'runing']) {
      logs.length = 0;
      ssm.on(GetParameterCommand).resolves(value(typo));
      expect(await createKillSwitch('/relay/kill', client)(), typo).toBe(true);
      const errors = logged('killswitch.invalid_value');
      expect(errors).toHaveLength(1);
      expect(errors[0]?.['level']).toBe('error');
      expect(logs.some((l) => l.includes('KillSwitchInvalidValue'))).toBe(true);
    }
  });

  it('caches for 30 seconds', async () => {
    ssm.on(GetParameterCommand).resolves(value('running'));
    let t = 0;
    const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t);
    await isPaused();
    t = 29_000;
    await isPaused();
    expect(ssm.commandCalls(GetParameterCommand)).toHaveLength(1);

    ssm.on(GetParameterCommand).resolves(value('paused'));
    t = 31_000;
    expect(await isPaused()).toBe(true);
    expect(ssm.commandCalls(GetParameterCommand)).toHaveLength(2);
  });

  it('treats a missing parameter as running', async () => {
    ssm
      .on(GetParameterCommand)
      .rejects(Object.assign(new Error('x'), { name: 'ParameterNotFound' }));
    expect(await createKillSwitch('/relay/kill', new SSMClient({}))()).toBe(false);
  });

  describe('REL-1: stale on error', () => {
    it('serves the last good value when SSM throttles after the cache expired, and logs it', async () => {
      let t = 0;
      const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t);
      ssm.on(GetParameterCommand).resolves(value('running'));
      expect(await isPaused()).toBe(false);

      ssm.on(GetParameterCommand).rejects(throttled());
      t = 31_000;
      expect(await isPaused()).toBe(false); // not a throw
      const stale = logged('config.stale_value_served');
      expect(stale).toHaveLength(1);
      expect(stale[0]).toMatchObject({ source: 'killswitch', error: 'ThrottlingException' });
      expect(logs.some((l) => l.includes('StaleValueServed') && l.includes('_aws'))).toBe(true);
    });

    it('serves a stale "paused" too, and does not hammer SSM while it is failing', async () => {
      let t = 0;
      const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t);
      ssm.on(GetParameterCommand).resolves(value('paused'));
      await isPaused();
      ssm.on(GetParameterCommand).rejects(throttled());
      t = 31_000;
      expect(await isPaused()).toBe(true);
      const calls = ssm.commandCalls(GetParameterCommand).length;
      t = 33_000; // inside the 5 s backoff
      expect(await isPaused()).toBe(true);
      expect(ssm.commandCalls(GetParameterCommand)).toHaveLength(calls);
    });

    it('recovers when SSM comes back', async () => {
      let t = 0;
      const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t);
      ssm.on(GetParameterCommand).resolves(value('running'));
      await isPaused();
      ssm.on(GetParameterCommand).rejects(throttled());
      t = 31_000;
      await isPaused();
      ssm.on(GetParameterCommand).resolves(value('paused'));
      t = 40_000;
      expect(await isPaused()).toBe(true);
    });

    it('fails closed (paused, no throw) when nothing was ever loaded', async () => {
      ssm.on(GetParameterCommand).rejects(throttled());
      expect(await createKillSwitch('/relay/kill', new SSMClient({}), () => 0)()).toBe(true);
      expect(logged('killswitch.read_failed_closed')).toHaveLength(1);
    });

    it('fails closed once the last good value is older than 10 minutes', async () => {
      let t = 0;
      const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t);
      ssm.on(GetParameterCommand).resolves(value('running'));
      await isPaused();
      ssm.on(GetParameterCommand).rejects(throttled());
      t = 9 * 60_000;
      expect(await isPaused()).toBe(false); // still inside the window
      t = 10 * 60_000 + 1_000;
      expect(await isPaused()).toBe(true);
    });

    it('shares one read between concurrent callers', async () => {
      ssm.on(GetParameterCommand).resolves(value('running'));
      const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => 0);
      await Promise.all([isPaused(), isPaused(), isPaused()]);
      expect(ssm.commandCalls(GetParameterCommand)).toHaveLength(1);
    });
  });

  describe('cold-start jitter', () => {
    it('waits a random 0-2 s once, before the first read only', async () => {
      ssm.on(GetParameterCommand).resolves(value('running'));
      const sleeps: number[] = [];
      let t = 0;
      const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t, {
        coldJitterMs: 2_000,
        random: () => 0.5,
        sleep: async (ms) => void sleeps.push(ms),
      });
      await isPaused();
      t = 60_000; // cache expired: a refresh, not a cold start
      await isPaused();
      expect(sleeps).toEqual([1_000]);
      expect(ssm.commandCalls(GetParameterCommand)).toHaveLength(2);
    });

    it('has no jitter by default', async () => {
      ssm.on(GetParameterCommand).resolves(value('running'));
      const sleeps: number[] = [];
      await createKillSwitch('/relay/kill', new SSMClient({}), () => 0, {
        sleep: async (ms) => void sleeps.push(ms),
      })();
      expect(sleeps).toEqual([]);
    });
  });
});

describe('kill switch metrics (OPS-1 follow-up)', () => {
  /** EMF lines for one metric name: its value. */
  const emf = (name: string): number[] =>
    logs
      .filter((l) => l.startsWith('{') && l.includes('"_aws"'))
      .map((l) => JSON.parse(l) as Record<string, unknown>)
      .filter((l) => name in l)
      .map((l) => {
        const m = (l['_aws'] as { CloudWatchMetrics: Array<{ Namespace: string }> })
          .CloudWatchMetrics[0];
        expect(m?.Namespace).toBe('Relay/Config');
        expect(l['Source']).toBe('killswitch');
        return l[name] as number;
      });

  it('emits KillSwitchPaused 1 for paused, 0 for running, once per real read', async () => {
    const client = new SSMClient({});
    ssm.on(GetParameterCommand).resolves(value('paused'));
    const isPaused = createKillSwitch('/relay/kill', client, () => 0);
    await isPaused();
    await isPaused(); // cached: no second metric
    expect(emf('KillSwitchPaused')).toEqual([1]);
    logs.length = 0;
    ssm.on(GetParameterCommand).resolves(value('running'));
    await createKillSwitch('/relay/kill', client, () => 0)();
    expect(emf('KillSwitchPaused')).toEqual([0]);
    expect(emf('KillSwitchInvalidValue')).toEqual([]);
    expect(emf('KillSwitchFailedClosed')).toEqual([]);
  });

  it('an invalid value emits KillSwitchInvalidValue and counts as paused', async () => {
    ssm.on(GetParameterCommand).resolves(value('pause'));
    expect(await createKillSwitch('/relay/kill', new SSMClient({}), () => 0)()).toBe(true);
    expect(emf('KillSwitchInvalidValue')).toEqual([1]);
    expect(emf('KillSwitchPaused')).toEqual([1]);
  });

  it('fail-closed with no fresh value (never loaded) emits KillSwitchFailedClosed', async () => {
    ssm.on(GetParameterCommand).rejects(throttled());
    expect(await createKillSwitch('/relay/kill', new SSMClient({}), () => 0)()).toBe(true);
    expect(emf('KillSwitchFailedClosed')).toEqual([1]);
    expect(emf('KillSwitchPaused')).toEqual([1]);
  });

  it('fail-closed after the stale value expired emits it; serving a stale value does not', async () => {
    let t = 0;
    const isPaused = createKillSwitch('/relay/kill', new SSMClient({}), () => t);
    ssm.on(GetParameterCommand).resolves(value('running'));
    await isPaused();
    ssm.on(GetParameterCommand).rejects(throttled());
    t = 9 * 60_000;
    expect(await isPaused()).toBe(false);
    expect(emf('KillSwitchFailedClosed')).toEqual([]);
    t = 10 * 60_000 + 1_000;
    expect(await isPaused()).toBe(true);
    expect(emf('KillSwitchFailedClosed')).toEqual([1]);
  });

  it('a missing parameter still means running, and emits KillSwitchParameterMissing', async () => {
    ssm
      .on(GetParameterCommand)
      .rejects(Object.assign(new Error('x'), { name: 'ParameterNotFound' }));
    expect(await createKillSwitch('/relay/kill', new SSMClient({}), () => 0)()).toBe(false);
    expect(emf('KillSwitchParameterMissing')).toEqual([1]);
    expect(emf('KillSwitchPaused')).toEqual([0]);
  });
});
