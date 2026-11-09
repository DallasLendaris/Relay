import { afterEach, describe, expect, it } from 'vitest';
import { startMockApns, type MockApns } from '../src/mock-apns.js';
import { DEFAULT_ERROR_MIX } from '../src/mock-common.js';
import {
  GOOD_HEADERS,
  NO_ERRORS,
  NO_LATENCY,
  apnsPost,
  connectH2,
  hexToken,
  pool,
} from './helpers.js';

let mock: MockApns | undefined;
afterEach(async () => {
  await mock?.close();
  mock = undefined;
});

describe('mock APNs', () => {
  it('validates the required headers, path and body', async () => {
    mock = await startMockApns({ latency: NO_LATENCY, errorMix: NO_ERRORS, goawayProbability: 0 });
    const c = connectH2(mock.url);
    const t = hexToken(1);
    const { authorization: _a, ...noAuth } = GOOD_HEADERS;
    const { 'apns-topic': _t, ...noTopic } = GOOD_HEADERS;
    const { 'apns-push-type': _p, ...noType } = GOOD_HEADERS;

    expect((await apnsPost(c, t, noAuth)).status).toBe(403);
    expect((await apnsPost(c, t, { ...GOOD_HEADERS, authorization: 'basic abc' })).status).toBe(
      403,
    );
    const topic = await apnsPost(c, t, noTopic);
    expect([topic.status, JSON.parse(topic.body).reason]).toEqual([400, 'MissingTopic']);
    const type = await apnsPost(c, t, { ...GOOD_HEADERS, 'apns-push-type': 'nope' });
    expect([type.status, JSON.parse(type.body).reason]).toEqual([400, 'InvalidPushType']);
    expect((await apnsPost(c, t, noType)).status).toBe(400);
    expect((await apnsPost(c, 'not-hex')).status).toBe(400);
    expect((await apnsPost(c, t, GOOD_HEADERS, '')).status).toBe(400);

    const ok = await apnsPost(c, t);
    expect(ok.status).toBe(200);
    expect(ok.headers['apns-id']).toBeTruthy();
    // Rejected requests never count as deliveries.
    expect(mock.counters.deliveredCount(t)).toBe(1);
    expect(mock.stats().rejected).toBe(7);
    c.close();
  });

  it('works over TLS with a generated self-signed certificate', async () => {
    mock = await startMockApns({ tls: true, latency: NO_LATENCY, errorMix: NO_ERRORS });
    expect(mock.url.startsWith('https://localhost:')).toBe(true);
    expect(mock.cert).toContain('BEGIN CERTIFICATE');
    const c = connectH2(mock.url, mock.cert);
    expect((await apnsPost(c, hexToken(2))).status).toBe(200);
    c.close();
  });

  it('enforces the per-connection concurrent stream cap', async () => {
    mock = await startMockApns({
      maxConcurrentStreams: 5,
      latency: { p50Ms: 40, p99Ms: 40 },
      errorMix: NO_ERRORS,
      goawayProbability: 0,
    });
    const c = connectH2(mock.url);
    // A well-behaved client waits for the server's SETTINGS, then queues behind the cap.
    await new Promise((resolve) => c.once('remoteSettings', resolve));
    const results = await Promise.all(
      Array.from({ length: 40 }, (_, i) => apnsPost(c, hexToken(i))),
    );
    expect(results.every((r) => r.status === 200)).toBe(true);
    const stats = mock.stats();
    expect(stats.peakConcurrentStreams).toBe(5);
    expect(stats.refusedStreams).toBe(0);
    expect(stats.ok).toBe(40);
    c.close();
  });

  it('refuses streams over the cap from a client that bursts before seeing SETTINGS', async () => {
    mock = await startMockApns({
      maxConcurrentStreams: 5,
      latency: { p50Ms: 40, p99Ms: 40 },
      errorMix: NO_ERRORS,
      goawayProbability: 0,
    });
    const c = connectH2(mock.url);
    const results = await Promise.allSettled(
      Array.from({ length: 40 }, (_, i) => apnsPost(c, hexToken(i))),
    );
    const refused = results.filter(
      (r) => r.status === 'rejected' && String(r.reason).includes('REFUSED_STREAM'),
    ).length;
    expect(refused).toBeGreaterThan(0);
    // nghttp2 refuses these itself, before the handler runs, so they are not in refusedStreams.
    expect(mock.stats().peakConcurrentStreams).toBeLessThanOrEqual(5);
    // Everything not refused was served.
    expect(mock.stats().ok).toBe(40 - refused);
    c.close();
  });

  it('sends occasional GOAWAY and clients can reconnect without losing messages', async () => {
    mock = await startMockApns({
      latency: NO_LATENCY,
      errorMix: NO_ERRORS,
      goawayProbability: 0.05,
      seed: 7,
    });
    let session = connectH2(mock.url);
    let sawGoaway = 0;
    const watch = (s: ReturnType<typeof connectH2>) =>
      s.on('goaway', () => {
        sawGoaway++;
        s.close();
      });
    watch(session);
    for (let i = 0; i < 300; i++) {
      if (session.closed || session.destroyed) {
        session = connectH2(mock.url);
        watch(session);
      }
      const r = await apnsPost(session, hexToken(i));
      expect(r.status).toBe(200);
    }
    session.close();
    expect(mock.stats().goaways).toBeGreaterThan(3);
    expect(sawGoaway).toBeGreaterThan(3);
    expect(mock.stats().connections).toBeGreaterThan(3);
    expect(mock.counters.delivered.size).toBe(300);
  });

  it('injects the default error mix statistically (20k messages, seeded)', async () => {
    mock = await startMockApns({ latency: NO_LATENCY, goawayProbability: 0, seed: 42 });
    expect(DEFAULT_ERROR_MIX).toEqual({
      unregistered: 0.005,
      tooManyRequests: 0.002,
      serverError: 0.001,
    });
    const sessions = Array.from({ length: 4 }, () => connectH2(mock?.url ?? ''));
    const statuses = new Map<number, number>();
    const N = 20_000;
    await pool(N, 200, async (i) => {
      const r = await apnsPost(sessions[i % 4] as ReturnType<typeof connectH2>, hexToken(i));
      statuses.set(r.status, (statuses.get(r.status) ?? 0) + 1);
    });
    sessions.forEach((s) => s.close());
    const s410 = statuses.get(410) ?? 0;
    const s429 = statuses.get(429) ?? 0;
    const s500 = statuses.get(500) ?? 0;
    // Expected 100 / 40 / 20 with standard deviations of about 10 / 6 / 4.5.
    expect(s410).toBeGreaterThan(60);
    expect(s410).toBeLessThan(140);
    expect(s429).toBeGreaterThan(15);
    expect(s429).toBeLessThan(70);
    expect(s500).toBeGreaterThan(5);
    expect(s500).toBeLessThan(45);
    expect(statuses.get(200)).toBe(N - s410 - s429 - s500);

    const stats = mock.stats();
    expect([stats.unregistered, stats.tooManyRequests, stats.serverError]).toEqual([
      s410,
      s429,
      s500,
    ]);
    // Only accepted messages count as delivered; 410 tokens were never delivered.
    expect(mock.counters.delivered.size).toBe(stats.ok);
    expect(mock.counters.unregistered.size).toBe(s410);
  }, 60_000);

  it('gives the same error sequence for the same seed', async () => {
    const run = async () => {
      const m = await startMockApns({ latency: NO_LATENCY, goawayProbability: 0, seed: 5 });
      const c = connectH2(m.url);
      const out: number[] = [];
      for (let i = 0; i < 2000; i++) out.push((await apnsPost(c, hexToken(i))).status);
      c.close();
      await m.close();
      return out;
    };
    expect(await run()).toEqual(await run());
  }, 30_000);

  it('counts messages per token so exactly-once can be asserted', async () => {
    mock = await startMockApns({ latency: NO_LATENCY, errorMix: NO_ERRORS });
    const c = connectH2(mock.url);
    await apnsPost(c, hexToken(10));
    await apnsPost(c, hexToken(10));
    await apnsPost(c, hexToken(11));
    expect(mock.counters.deliveredCount(hexToken(10))).toBe(2);
    expect(mock.counters.deliveredCount(hexToken(11))).toBe(1);
    expect(mock.counters.attempts.get(hexToken(10))).toBe(2);
    mock.reset();
    expect(mock.counters.delivered.size).toBe(0);
    c.close();
  });
});
