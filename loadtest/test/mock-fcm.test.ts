import { afterEach, describe, expect, it } from 'vitest';
import { startMockFcm, type MockFcm } from '../src/mock-fcm.js';
import { NO_ERRORS, NO_LATENCY, pool, postJson } from './helpers.js';

const sendUrl = (m: MockFcm) => `${m.url}/v1/projects/relay-test/messages:send`;
const msg = (token: string) => JSON.stringify({ message: { token } });

let mock: MockFcm | undefined;
afterEach(async () => {
  await mock?.close();
  mock = undefined;
});

async function getAccessToken(m: MockFcm): Promise<string> {
  const res = await fetch(`${m.url}/token`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion: 'a.b.c',
    }),
  });
  expect(res.status).toBe(200);
  const body = (await res.json()) as { access_token: string; expires_in: number };
  expect(body.expires_in).toBe(3600);
  return body.access_token;
}

function send(m: MockFcm, accessToken: string | undefined, token: string | undefined) {
  return fetch(`${m.url}/v1/projects/relay-test/messages:send`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
    },
    body: JSON.stringify({ message: { ...(token ? { token } : {}), data: { a: '1' } } }),
  });
}

const tok = (i: number) => `fcm-token-${i}`;

describe('mock FCM', () => {
  it('issues OAuth tokens, requires them, and expires them', async () => {
    let clock = 0;
    mock = await startMockFcm({
      latency: NO_LATENCY,
      errorMix: NO_ERRORS,
      ratePerSecond: 0,
      now: () => clock,
      tokenTtlSeconds: 3600,
    });
    expect((await send(mock, undefined, tok(1))).status).toBe(401);
    expect((await send(mock, 'made-up', tok(1))).status).toBe(401);
    const bad = await fetch(`${mock.url}/token`, { method: 'POST', body: '{}' });
    expect(bad.status).toBe(400);

    const access = await getAccessToken(mock);
    const ok = await send(mock, access, tok(1));
    expect(ok.status).toBe(200);
    expect(((await ok.json()) as { name: string }).name).toMatch(
      /^projects\/relay-test\/messages\//,
    );

    clock = 3600 * 1000 + 1;
    expect((await send(mock, access, tok(2))).status).toBe(401);
    expect(mock.stats().oauthRequests).toBe(2);
  });

  it('rejects a message without a token', async () => {
    mock = await startMockFcm({ latency: NO_LATENCY, errorMix: NO_ERRORS, ratePerSecond: 0 });
    const access = await getAccessToken(mock);
    const res = await send(mock, access, undefined);
    expect(res.status).toBe(400);
    expect(((await res.json()) as { error: { status: string } }).error.status).toBe(
      'INVALID_ARGUMENT',
    );
  });

  it('enforces the token-bucket rate limit and returns Retry-After', async () => {
    let clock = 0;
    mock = await startMockFcm({
      latency: NO_LATENCY,
      errorMix: NO_ERRORS,
      ratePerSecond: 100,
      burst: 100,
      now: () => clock,
    });
    const access = await getAccessToken(mock);
    const statuses: number[] = [];
    let retryAfter: string | null = null;
    for (let i = 0; i < 150; i++) {
      const res = await send(mock, access, tok(i));
      statuses.push(res.status);
      if (res.status === 429) retryAfter = res.headers.get('retry-after');
    }
    expect(statuses.filter((s) => s === 200)).toHaveLength(100);
    expect(statuses.filter((s) => s === 429)).toHaveLength(50);
    expect(Number(retryAfter)).toBeGreaterThanOrEqual(1);
    expect(mock.stats().rateLimited).toBe(50);
    expect(mock.stats().tooManyRequests).toBe(0);

    // Half a second refills 50 tokens; the rejected 50 retry and exactly 50 get in.
    clock += 500;
    let accepted = 0;
    for (let i = 100; i < 200; i++) {
      if ((await send(mock, access, tok(i))).status === 200) accepted++;
    }
    expect(accepted).toBe(50);
    // Rate-limited requests were never delivered.
    expect(mock.counters.delivered.size).toBe(150);
  });

  it('defaults to a 10,000 per second quota', async () => {
    const clock = 0;
    mock = await startMockFcm({ latency: NO_LATENCY, errorMix: NO_ERRORS, now: () => clock });
    const access = await getAccessToken(mock);
    // Fire 10,050 requests with the clock frozen: exactly the 10,000 burst gets through.
    let ok = 0;
    let limited = 0;
    await pool(10_050, 100, async (i) => {
      const res = await postJson(sendUrl(mock as MockFcm), msg(tok(i)), access);
      if (res.status === 200) ok++;
      else if (res.status === 429) limited++;
    });
    expect(ok).toBe(10_000);
    expect(limited).toBe(50);
  }, 60_000);

  it('injects the default error mix statistically (20k messages, seeded)', async () => {
    mock = await startMockFcm({ latency: NO_LATENCY, ratePerSecond: 0, seed: 42 });
    const access = await getAccessToken(mock);
    const statuses = new Map<number, number>();
    const codes = new Set<string>();
    let retryAfterOn429 = true;
    const N = 20_000;
    await pool(N, 200, async (i) => {
      const res = await postJson(sendUrl(mock as MockFcm), msg(tok(i)), access);
      statuses.set(res.status, (statuses.get(res.status) ?? 0) + 1);
      if (res.status === 429 && !res.retryAfter) retryAfterOn429 = false;
      if (res.status !== 200) {
        const body = JSON.parse(res.body) as { error: { details?: Array<{ errorCode: string }> } };
        codes.add(body.error.details?.[0]?.errorCode ?? '');
      }
    });
    const unreg = statuses.get(404) ?? 0;
    const quota = statuses.get(429) ?? 0;
    const server = statuses.get(500) ?? 0;
    expect(unreg).toBeGreaterThan(60);
    expect(unreg).toBeLessThan(140);
    expect(quota).toBeGreaterThan(15);
    expect(quota).toBeLessThan(70);
    expect(server).toBeGreaterThan(5);
    expect(server).toBeLessThan(45);
    expect(statuses.get(200)).toBe(N - unreg - quota - server);
    expect(retryAfterOn429).toBe(true);
    expect([...codes].sort()).toEqual(['INTERNAL', 'QUOTA_EXCEEDED', 'UNREGISTERED']);
    expect(mock.stats().tooManyRequests).toBe(quota);
    expect(mock.counters.delivered.size).toBe(mock.stats().ok);
  }, 60_000);

  it('counts messages per token', async () => {
    mock = await startMockFcm({ latency: NO_LATENCY, errorMix: NO_ERRORS, ratePerSecond: 0 });
    const access = await getAccessToken(mock);
    await send(mock, access, 'dup');
    await send(mock, access, 'dup');
    await send(mock, access, 'once');
    expect(mock.counters.deliveredCount('dup')).toBe(2);
    expect(mock.counters.deliveredCount('once')).toBe(1);
    expect(mock.counters.deliveredCount('never')).toBe(0);
  });
});
