import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  LatencyRecorder,
  checkExactlyOnce,
  formatExactlyOnce,
  formatLatencySummary,
  logRunReport,
  percentile,
  summarizeLatencies,
  throughput,
} from '../src/report.js';
import { createLatencySampler, createRng } from '../src/mock-common.js';
import { startMockFcm, type MockFcm } from '../src/mock-fcm.js';
import { NO_LATENCY, pool, postJson } from './helpers.js';

describe('latency and throughput', () => {
  it('computes nearest-rank percentiles', () => {
    const samples = Array.from({ length: 100 }, (_, i) => 100 - i); // unsorted 100..1
    const s = summarizeLatencies(samples);
    expect(s).toMatchObject({ count: 100, minMs: 1, maxMs: 100, p50Ms: 50, p95Ms: 95, p99Ms: 99 });
    expect(s.meanMs).toBeCloseTo(50.5);
    expect(percentile([], 0.5)).toBe(0);
    expect(formatLatencySummary(s)).toContain('p99=99.0ms');
  });

  it('records timed operations', async () => {
    const rec = new LatencyRecorder();
    for (let i = 0; i < 3000; i++) rec.record(i); // forces the buffer to grow
    await rec.time(() => new Promise((r) => setTimeout(r, 10)));
    expect(rec.count).toBe(3001);
    expect(rec.summary().maxMs).toBeGreaterThanOrEqual(2999);
  });

  it('computes throughput', () => {
    expect(throughput(1000, 0, 2000)).toEqual({ count: 1000, seconds: 2, perSecond: 500 });
    expect(throughput(5, 10, 10).perSecond).toBe(0);
  });

  it('samples a log-normal latency with the requested p50 and p99', () => {
    const sample = createLatencySampler({ p50Ms: 20, p99Ms: 150 }, createRng(3));
    const s = summarizeLatencies(Array.from({ length: 50_000 }, sample));
    expect(s.p50Ms).toBeGreaterThan(19);
    expect(s.p50Ms).toBeLessThan(21);
    expect(s.p99Ms).toBeGreaterThan(130);
    expect(s.p99Ms).toBeLessThan(170);
  });
});

describe('exactly-once check', () => {
  it('passes when every token was delivered once', () => {
    const r = checkExactlyOnce(
      ['a', 'b'],
      new Map([
        ['a', 1],
        ['b', 1],
      ]),
    );
    expect(r).toMatchObject({ ok: true, expected: 2, deliveredOnce: 2 });
  });

  it('lists misses, duplicates and unexpected tokens', () => {
    const delivered = new Map([
      ['a', 1],
      ['b', 3],
      ['z', 1],
    ]);
    const r = checkExactlyOnce(['a', 'b', 'c'], delivered);
    expect(r.ok).toBe(false);
    expect(r.missing).toEqual(['c']);
    expect(r.duplicates).toEqual([{ token: 'b', count: 3 }]);
    expect(r.unexpected).toEqual(['z']);
  });

  it('excuses expected tokens that were terminally rejected, but not duplicates', () => {
    const r = checkExactlyOnce(['a', 'gone'], new Map([['a', 1]]), { excused: new Set(['gone']) });
    expect(r).toMatchObject({ ok: true, excused: 1, deliveredOnce: 1 });
    const dup = checkExactlyOnce(['gone'], new Map([['gone', 2]]), { excused: new Set(['gone']) });
    expect(dup.ok).toBe(false);
  });

  it('prints and logs without leaking full tokens', () => {
    const token = 'abcdef0123456789abcdef0123456789';
    const r = checkExactlyOnce([token, 'x'], new Map([['x', 2]]));
    const text = formatExactlyOnce(r);
    expect(text).toContain('FAIL');
    expect(text).toContain('abcdef01...');
    expect(text).not.toContain(token);
    const log = vi.spyOn(console, 'log').mockImplementation(() => {});
    logRunReport('test.report', { exactlyOnce: r });
    const line = String(log.mock.calls[0]?.[0]);
    log.mockRestore();
    expect(JSON.parse(line).exactlyOnce.missing).toBe(1);
    expect(line).not.toContain(token);
  });
});

describe('exactly-once against a mock with a retrying client', () => {
  let mock: MockFcm | undefined;
  afterEach(async () => {
    await mock?.close();
  });

  it('every token is delivered exactly once despite 429/500, and 410s are the only gap', async () => {
    mock = await startMockFcm({
      latency: NO_LATENCY,
      ratePerSecond: 0,
      seed: 9,
      errorMix: { unregistered: 0.03, tooManyRequests: 0.05, serverError: 0.05 },
    });
    const m = mock;
    const tokenRes = await fetch(`${m.url}/token`, {
      method: 'POST',
      body: JSON.stringify({ assertion: 'x' }),
    });
    const access = ((await tokenRes.json()) as { access_token: string }).access_token;
    const tokens = Array.from({ length: 2000 }, (_, i) => `tok-${i}`);
    await pool(tokens.length, 50, async (i) => {
      for (let attempt = 0; attempt < 20; attempt++) {
        const res = await postJson(
          `${m.url}/v1/projects/p/messages:send`,
          JSON.stringify({ message: { token: tokens[i] } }),
          access,
        );
        if (res.status !== 429 && res.status !== 500) return;
      }
    });
    const stats = m.stats();
    expect(stats.tooManyRequests + stats.serverError).toBeGreaterThan(50);
    expect(stats.unregistered).toBeGreaterThan(10);

    const result = checkExactlyOnce(tokens, m.counters.delivered, {
      excused: m.counters.unregistered,
    });
    expect(result.ok).toBe(true);
    expect(result.excused).toBe(m.counters.unregistered.size);
    expect(result.deliveredOnce + result.excused).toBe(tokens.length);

    // Without excusing the 410 tokens the check reports them as missing.
    const strict = checkExactlyOnce(tokens, m.counters.delivered);
    expect(strict.ok).toBe(false);
    expect(strict.missing.length).toBe(m.counters.unregistered.size);

    // A sender bug that double-sends is caught.
    m.setErrorMix({ unregistered: 0, tooManyRequests: 0, serverError: 0 });
    const first = tokens.find((t) => m.counters.deliveredCount(t) === 1) as string;
    const again = await fetch(`${m.url}/v1/projects/p/messages:send`, {
      method: 'POST',
      headers: { authorization: `Bearer ${access}` },
      body: JSON.stringify({ message: { token: first } }),
    });
    expect(again.status).toBe(200);
    const dup = checkExactlyOnce(tokens, m.counters.delivered, {
      excused: m.counters.unregistered,
    });
    expect(dup.ok).toBe(false);
    expect(dup.duplicates).toEqual([{ token: first, count: 2 }]);
  }, 60_000);
});
