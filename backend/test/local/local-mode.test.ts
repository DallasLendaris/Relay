import { randomUUID } from 'node:crypto';
import { existsSync, readdirSync } from 'node:fs';
import { cellToParent, latLngToCell } from 'h3-js';
import type { AddressInfo } from 'node:net';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { loadLocalConfig, REPLAY_DIR } from '../../src/local/config.js';
import { covers, pickDemoPoints } from '../../src/local/pick-points.js';
import { createLocalRelay, feedStartTime } from '../../src/local/relay.js';
import { INSIDE_POINT, runScenario } from '../../src/local/scenario.js';
import { createLocalServer } from '../../src/local/server.js';
import type { Server } from 'node:http';

const SAMPLE = join(REPLAY_DIR, 'constructed-sample.json');
const SAMPLE_ZONES = join(REPLAY_DIR, 'constructed-sample.zones.json');
const replayRelay = () =>
  createLocalRelay({
    feed: 'replay',
    push: 'log',
    replayFile: SAMPLE,
    replayZonesFile: SAMPLE_ZONES,
  });

describe('local mode: the PoC path', () => {
  it('runs feed -> signed alert -> push -> "I\'m okay" counted once, control phone untouched', async () => {
    const result = await runScenario(replayRelay());
    expect(result.alertEvent).toBe('Flash Flood Warning');
    expect(result.okCount).toBe(1);
    expect(result.pushesToControl).toBe(0);
    expect(result.millisFeedToPush).toBeLessThan(60_000);
  });

  it('starts the replay clock at the feed time, so its alerts are active', () => {
    const relay = replayRelay();
    const now = relay.clock.now().getTime();
    expect(now).toBeGreaterThanOrEqual(Date.parse('2026-11-09T06:11:00-08:00'));
    expect(now).toBeLessThan(Date.parse('2026-11-09T18:00:00-08:00'));
    expect(feedStartTime('{"features":[]}')).toBeInstanceOf(Date);
  });

  it('pushes to a phone that saves its place after the alert already arrived (late join)', async () => {
    const relay = replayRelay();
    await relay.pollAndDeliver(); // the alert is stored and marked seen before the phone exists
    expect(relay.sentMessages).toHaveLength(0);

    const call = async (method: string, path: string, body: unknown, credential?: string) => {
      const res = await relay.handle({
        rawPath: path,
        headers: credential ? { authorization: `Bearer ${credential}` } : {},
        body: JSON.stringify(body),
        requestContext: { http: { method } },
      });
      expect(res.statusCode, res.body).toBeLessThan(300);
      return JSON.parse(res.body || '{}') as Record<string, string>;
    };
    const device = await call('POST', '/devices', {});
    const token = `ExponentPushToken[late-${randomUUID().slice(0, 6)}]`;
    await call(
      'PUT',
      '/devices/me/push-token',
      { expoPushToken: token, language: 'en' },
      device['credential'],
    );
    await call('POST', '/places', { label: 'Home', ...INSIDE_POINT }, device['credential']);

    const pushes = relay.sentMessages.filter((m) => m.to === token);
    expect(pushes.map((m) => m.title)).toContain('Flash Flood Warning');
    // The next poll does not push the same alert again.
    await relay.pollAndDeliver();
    expect(relay.sentMessages.filter((m) => m.to === token)).toHaveLength(pushes.length);
  });
});

describe('local mode: HTTP server', () => {
  let server: Server | undefined;
  afterEach(() => new Promise<void>((r) => (server ? server.close(() => r()) : r())));

  async function start() {
    server = createLocalServer(replayRelay(), { feed: 'replay', push: 'log' });
    await new Promise<void>((r) => server?.listen(0, '127.0.0.1', () => r()));
    return `http://127.0.0.1:${(server?.address() as AddressInfo).port}`;
  }

  it('answers /health, serves the real API, and keeps tokens out of /local/status', async () => {
    const base = await start();
    expect(await (await fetch(`${base}/health`)).json()).toEqual({ ok: true });

    const reg = await fetch(`${base}/devices`, { method: 'POST', body: '{}' });
    expect(reg.status).toBe(201);
    const { credential } = (await reg.json()) as { credential: string };
    const token = 'ExponentPushToken[http-test]';
    await fetch(`${base}/devices/me/push-token`, {
      method: 'PUT',
      headers: { authorization: `Bearer ${credential}` },
      body: JSON.stringify({ expoPushToken: token, language: 'en' }),
    });
    const place = await fetch(`${base}/places`, {
      method: 'POST',
      headers: { authorization: `Bearer ${credential}` },
      body: JSON.stringify({ label: 'Home', ...INSIDE_POINT }),
    });
    expect(place.status).toBe(201);

    const poll = await fetch(`${base}/local/poll`, { method: 'POST' });
    expect(poll.status).toBe(200);
    const alerts = (await (
      await fetch(`${base}/alerts`, { headers: { authorization: `Bearer ${credential}` } })
    ).json()) as { alerts: Array<{ event: string }> };
    expect(alerts.alerts.map((a) => a.event)).toContain('Flash Flood Warning');

    const statusText = await (await fetch(`${base}/local/status`)).text();
    expect(statusText).toContain('Flash Flood Warning');
    expect(statusText).not.toContain(token);
    expect(statusText).not.toContain('checkInToken');
  });

  it('refuses unauthenticated calls, unknown routes and oversized bodies', async () => {
    const base = await start();
    expect((await fetch(`${base}/places`)).status).toBe(401);
    expect((await fetch(`${base}/nope`)).status).toBe(404);
    const big = await fetch(`${base}/devices`, { method: 'POST', body: 'x'.repeat(70 * 1024) });
    expect(big.status).toBe(413);
  });
});

describe('local mode: config', () => {
  it('defaults to replay + log push on port 8787', () => {
    const c = loadLocalConfig({});
    expect(c).toMatchObject({ feed: 'replay', push: 'log', port: 8787, pollSeconds: 30 });
    expect(c.replayFile).toMatch(/\.json$/);
  });

  it('rejects bad values instead of guessing', () => {
    expect(() => loadLocalConfig({ RELAY_FEED: 'kafka' })).toThrow(/RELAY_FEED/);
    expect(() => loadLocalConfig({ RELAY_PUSH: 'sms' })).toThrow(/RELAY_PUSH/);
    expect(() => loadLocalConfig({ PORT: '0' })).toThrow(/PORT/);
    expect(() => loadLocalConfig({ RELAY_POLL_SECONDS: '5' })).toThrow(/30/);
  });

  it('uses RELAY_REPLAY_FILE and finds its zones file next to it', () => {
    const c = loadLocalConfig({ RELAY_REPLAY_FILE: SAMPLE });
    expect(c.replayFile).toBe(SAMPLE);
    expect(c.replayZonesFile).toBe(SAMPLE_ZONES);
  });
});

describe('local mode: picking demo points from a feed', () => {
  it('places the resident inside a pushable alert and the control outside all of them', async () => {
    const points = await pickDemoPoints(replayRelay);
    expect(points.inside.length).toBeGreaterThan(0);
    expect(points.outside).not.toBeNull();
    const result = await runScenario(replayRelay(), () => {}, points.inside[0], points.outside!);
    expect(result.okCount).toBe(1);
    expect(result.pushesToControl).toBe(0);
  });

  it('covers() handles mixed-resolution cell sets', () => {
    const fine = latLngToCell(37.35, -121.9, 7);
    expect(covers([fine], INSIDE_POINT)).toBe(true);
    expect(covers([cellToParent(fine, 5)], INSIDE_POINT)).toBe(true);
    expect(covers([fine], { latitude: 34.05, longitude: -118.24 })).toBe(false);
  });
});

// Real NWS responses recorded by `npm run capture-nws` (locally or by the capture workflow).
const RECORDINGS = existsSync(REPLAY_DIR)
  ? readdirSync(REPLAY_DIR).filter((f) => /^recorded-.*(?<!\.zones)\.json$/.test(f))
  : [];

describe.skipIf(RECORDINGS.length === 0)('local mode: recorded NWS responses', () => {
  it.each(RECORDINGS)('%s ingests with no errors and no unknown zones', async (file) => {
    const replayFile = join(REPLAY_DIR, file);
    const zones = replayFile.replace(/\.json$/, '.zones.json');
    const relay = createLocalRelay({
      feed: 'replay',
      push: 'log',
      replayFile,
      ...(existsSync(zones) ? { replayZonesFile: zones } : {}),
    });
    const { counts } = await relay.pollAndDeliver();
    expect(counts.errors).toBe(0);
    expect(counts.unknownZones).toBe(0);
    expect(counts.fetchFailed).toBe(0);
  });
});
