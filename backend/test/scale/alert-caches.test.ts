import type { Alert } from '@relay/shared';
import { describe, expect, it } from 'vitest';
import { CachedAlertReader, MAX_CACHED_READER_ALERTS } from '../../src/scale/fanout/alert-cache.js';
import { CachedAlertStore, MAX_CACHED_ALERTS } from '../../src/scale/sender/cached-alerts.js';

const alertOf = (id: string): Alert => ({ id }) as unknown as Alert;

/** An inner store that counts reads and can be told to return null. */
const inner = () => {
  const calls: string[] = [];
  return {
    calls,
    get: async (id: string): Promise<Alert | null> => {
      calls.push(id);
      return id.startsWith('missing') ? null : alertOf(id);
    },
  };
};

const cases = [
  {
    name: 'CachedAlertStore (sender)',
    cap: MAX_CACHED_ALERTS,
    make: (i: ReturnType<typeof inner>, now: () => number, ttl: number, max?: number) =>
      new CachedAlertStore(i, ttl, now, max),
    size: (c: unknown) => (c as { cache: Map<string, unknown> }).cache.size,
  },
  {
    name: 'CachedAlertReader (fan-out)',
    cap: MAX_CACHED_READER_ALERTS,
    make: (i: ReturnType<typeof inner>, now: () => number, ttl: number, max?: number) =>
      new CachedAlertReader(i, ttl, now, max),
    size: (c: unknown) => (c as { cache: Map<string, unknown> }).cache.size,
  },
] as const;

describe.each(cases)('$name memory bounds', ({ cap, make, size }) => {
  it('uses memory-safe default caps (a statewide alert is ~1.5 MB)', () => {
    expect(cap).toBeLessThanOrEqual(64);
    const i = inner();
    const c = make(i, () => 0, 1_000);
    return (async () => {
      for (let n = 0; n < cap * 3; n++) await c.get(`a${n}`);
      expect(size(c)).toBe(cap);
    })();
  });

  it('sweeps entries older than the TTL when a new entry is inserted', async () => {
    let t = 0;
    const i = inner();
    const c = make(i, () => t, 1_000);
    for (let n = 0; n < 10; n++) await c.get(`a${n}`);
    expect(size(c)).toBe(10);
    t = 1_500; // all ten are expired; none is read again
    await c.get('fresh');
    expect(size(c)).toBe(1);
  });

  it('keeps live entries when sweeping, and still serves them from memory', async () => {
    let t = 0;
    const i = inner();
    const c = make(i, () => t, 1_000);
    await c.get('old');
    t = 600;
    await c.get('young');
    t = 1_200; // old expired, young live
    await c.get('new');
    expect(size(c)).toBe(2);
    await c.get('young');
    expect(i.calls).toEqual(['old', 'young', 'new']);
  });

  it('an expired entry that is read again is replaced, not duplicated, and refetched', async () => {
    let t = 0;
    const i = inner();
    const c = make(i, () => t, 1_000);
    await c.get('a');
    t = 2_000;
    await c.get('a');
    expect(i.calls).toEqual(['a', 'a']);
    expect(size(c)).toBe(1);
  });

  it('still shares concurrent reads and does not cache misses', async () => {
    const i = inner();
    const c = make(i, () => 0, 1_000);
    await Promise.all([c.get('x'), c.get('x'), c.get('x')]);
    expect(i.calls).toEqual(['x']);
    await Promise.all([c.get('missing1'), c.get('missing1')]);
    expect(i.calls).toEqual(['x', 'missing1']);
    await c.get('missing1');
    expect(i.calls).toEqual(['x', 'missing1', 'missing1']);
    expect(size(c)).toBe(1);
  });

  it('evicts the oldest entry once over the cap', async () => {
    const i = inner();
    const c = make(i, () => 0, 60_000, 3);
    for (const id of ['a', 'b', 'c', 'd']) await c.get(id);
    expect(size(c)).toBe(3);
    await c.get('a'); // evicted: refetched
    await c.get('d'); // kept
    expect(i.calls).toEqual(['a', 'b', 'c', 'd', 'a']);
  });
});
