import { describe, expect, it } from 'vitest';
import { FcmProjectSender } from '../../../src/adapters/push/fcm-projects.js';
import {
  EcsFleetSize,
  FixedFleetSize,
  fcmRateFromEnv,
  fleetSizeFromEnv,
  perTaskRate,
  type FleetSizeSource,
} from '../../../src/adapters/push/fleet-size.js';
import { TokenBucket } from '../../../src/adapters/push/token-bucket.js';
import type { NativePushSender } from '../../../src/scale/contracts.js';

const noopSender: NativePushSender = { send: async () => [] };

function fakeEcs(counts: Array<number | Error | undefined>, desired?: number) {
  const calls: any[] = [];
  let i = 0;
  return {
    calls,
    send: async (cmd: any) => {
      calls.push(cmd.input);
      const v = counts[Math.min(i++, counts.length - 1)];
      if (v instanceof Error) throw v;
      return { services: v === undefined ? [] : [{ runningCount: v, desiredCount: desired ?? v }] };
    },
  } as any;
}

describe('fleet-wide FCM rate', () => {
  it('divides the fleet budget by the running task count and follows the source', () => {
    let n = 10;
    const source: FleetSizeSource = { size: () => n, refresh: async () => {} };
    const { provider, mode } = fcmRateFromEnv({}, source);
    expect(mode).toBe('fleet');
    expect(provider()).toBe(900);
    n = 100;
    expect(provider()).toBe(90);
    expect(perTaskRate(9000, new FixedFleetSize(0))).toBe(9000);
  });

  it('honours FCM_PROJECT_FLEET_RATE_PER_SEC; an explicit per-task rate wins', () => {
    const source = new FixedFleetSize(3);
    expect(fcmRateFromEnv({ FCM_PROJECT_FLEET_RATE_PER_SEC: '3000' }, source).provider()).toBe(
      1000,
    );
    const o = fcmRateFromEnv(
      { FCM_PROJECT_RATE_PER_SEC: '500', FCM_PROJECT_FLEET_RATE_PER_SEC: '3000' },
      source,
    );
    expect(o.mode).toBe('per-task');
    expect(o.provider()).toBe(500);
    expect(() => fcmRateFromEnv({ FCM_PROJECT_RATE_PER_SEC: '0' }, source)).toThrow(/positive/);
    expect(() => fcmRateFromEnv({ FCM_PROJECT_FLEET_RATE_PER_SEC: 'x' }, source)).toThrow(
      /positive/,
    );
  });

  it('FcmProjectSender re-rates every project bucket when the fleet changes', () => {
    let n = 10;
    const source: FleetSizeSource = { size: () => n, refresh: async () => {} };
    const sender = new FcmProjectSender({
      projects: [
        { projectId: 'a', sender: noopSender },
        { projectId: 'b', sender: noopSender },
      ],
      rateProvider: fcmRateFromEnv({}, source).provider,
    });
    expect(sender.ratePerSecond).toBe(900);
    n = 100;
    expect(sender.refreshRate()).toBe(90);
    expect(sender.ratePerSecond).toBe(90);
  });

  it('defaults to 1 task without ECS env (local dev)', () => {
    const src = fleetSizeFromEnv({}, () => {
      throw new Error('no ECS client wanted');
    });
    expect(src.size()).toBe(1);
  });
});

describe('EcsFleetSize', () => {
  const opts = { cluster: 'c', service: 's', jitter: 0, fallback: 100 };

  it('reads runningCount, caches for the ttl, then refreshes', async () => {
    let t = 0;
    const ecs = fakeEcs([12, 40]);
    const f = new EcsFleetSize(ecs, { ...opts, now: () => t });
    await f.refresh();
    expect(f.size()).toBe(12);
    expect(ecs.calls[0]).toEqual({ cluster: 'c', services: ['s'] });
    t = 14_000;
    await f.refresh();
    expect(ecs.calls).toHaveLength(1);
    t = 16_000;
    await f.refresh();
    expect(f.size()).toBe(40);
  });

  it('keeps the last value on error or a missing service, and never goes below 1', async () => {
    let t = 0;
    const errors: unknown[] = [];
    const ecs = fakeEcs([25, new Error('Throttling'), undefined, 0]);
    const f = new EcsFleetSize(ecs, {
      ...opts,
      staleAfterMs: 120_000,
      now: () => t,
      onError: (e) => errors.push(e),
    });
    await f.refresh();
    t += 20_000;
    await f.refresh();
    expect(f.size()).toBe(25);
    t += 20_000;
    await f.refresh();
    expect(f.size()).toBe(25);
    expect(errors).toHaveLength(2);
    t += 20_000;
    await f.refresh();
    expect(f.size()).toBe(1);
  });

  it('uses desiredCount when it leads runningCount (scale-out)', async () => {
    const f = new EcsFleetSize(fakeEcs([10], 100), opts);
    await f.refresh();
    expect(f.size()).toBe(100);
  });

  it('assumes the fallback fleet until the first read, and a good read replaces it', async () => {
    let t = 0;
    const ecs = fakeEcs([new Error('AccessDenied'), 20]);
    const f = new EcsFleetSize(ecs, { ...opts, fallback: 80, now: () => t });
    expect(f.size()).toBe(80);
    expect(fcmRateFromEnv({}, f).provider()).toBe(9000 / 80);
    await f.refresh();
    expect(f.inFallback).toBe(true);
    expect(f.size()).toBe(80);
    t = 20_000;
    await f.refresh();
    expect(f.inFallback).toBe(false);
    expect(f.size()).toBe(20);
  });

  it('falls back again when reads keep failing past staleAfterMs', async () => {
    let t = 0;
    const ecs = fakeEcs([20, new Error('Throttling')]);
    const f = new EcsFleetSize(ecs, { ...opts, fallback: 100, staleAfterMs: 60_000, now: () => t });
    await f.refresh();
    expect(f.size()).toBe(20);
    t = 30_000;
    await f.refresh();
    expect(f.size()).toBe(20);
    t = 70_000;
    await f.refresh();
    expect(f.size()).toBe(100);
  });

  it('reads FCM_FLEET_SIZE_FALLBACK from the environment', () => {
    const src = fleetSizeFromEnv(
      { ECS_CLUSTER: 'c', ECS_SERVICE: 's', FCM_FLEET_SIZE_FALLBACK: '50' },
      () => fakeEcs([1]),
    );
    expect(src.size()).toBe(50);
  });

  it('adds jitter to the cache time and shares one in-flight request', async () => {
    const ecs = fakeEcs([5]);
    let t = 0;
    const f = new EcsFleetSize(ecs, { ...opts, jitter: 0.5, random: () => 1, now: () => t });
    await Promise.all([f.refresh(), f.refresh()]);
    expect(ecs.calls).toHaveLength(1);
    t = 20_000; // 15 s ttl + 7.5 s jitter = 22.5 s
    await f.refresh();
    expect(ecs.calls).toHaveLength(1);
    t = 23_000;
    await f.refresh();
    expect(ecs.calls).toHaveLength(2);
  });

  it('is chosen from ECS_CLUSTER and ECS_SERVICE', async () => {
    const ecs = fakeEcs([7]);
    const src = fleetSizeFromEnv({ ECS_CLUSTER: 'c', ECS_SERVICE: 's' }, () => ecs);
    await src.refresh();
    expect(src.size()).toBe(7);
  });
});

describe('TokenBucket.setRate', () => {
  const clock = () => {
    let t = 0;
    return { now: () => t, advance: (ms: number) => (t += ms) };
  };

  it('refills at the old rate first, then clamps tokens to the new capacity', async () => {
    const c = clock();
    const b = new TokenBucket(100, 100, c.now, async () => {});
    await b.take(100); // empty
    c.advance(500); // 50 at the old rate
    b.setRate(10, 10);
    expect(b.ratePerSecond).toBe(10);
    expect(b.available()).toBe(10); // 50 clamped to the new burst
    c.advance(500);
    expect(b.available()).toBe(10);
  });

  it('keeps debt and refills at the new rate; raising the rate does not add tokens', async () => {
    const c = clock();
    const b = new TokenBucket(10, 10, c.now, async () => {});
    await b.take(30); // debt 20
    b.setRate(100, 100);
    expect(b.available()).toBe(-20);
    c.advance(1000);
    expect(b.available()).toBe(80);
    expect(() => b.setRate(0)).toThrow(RangeError);
  });
});
