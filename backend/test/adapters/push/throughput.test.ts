import { fork, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { ApnsSender } from '../../../src/adapters/push/apns-sender.js';
import { FcmSender } from '../../../src/adapters/push/fcm-sender.js';
import { ecKeys, makeMessage, rsaKeys } from './mocks.js';

const N = 20_000;
const CHUNK = 100; // the sender service sends SENDER_CHUNK_SIZE recipients per call, many calls at once
const LIMIT_MS = 10_000;

interface Stats {
  total: number;
  maxInFlight: number;
  tokenRequests: number;
}

/** Start the mock in its own process. */
async function startServer(mode: 'apns' | 'fcm'): Promise<{
  port: number;
  stats: () => Promise<Stats>;
  stop: () => void;
}> {
  const child: ChildProcess = fork(
    fileURLToPath(new URL('./throughput-server.mjs', import.meta.url)),
    [mode],
  );
  const port = await new Promise<number>((resolve, reject) => {
    child.once('error', reject);
    child.once('message', (m) => resolve((m as { ready: number }).ready));
  });
  return {
    port,
    stats: () =>
      new Promise<Stats>((resolve) => {
        child.once('message', (m) => resolve((m as { stats: Stats }).stats));
        child.send('stats');
      }),
    stop: () => child.kill(),
  };
}

/** Send N messages the way the sender service does: many chunks concurrently. */
async function blast(
  send: (m: ReturnType<typeof makeMessage>[]) => Promise<Array<{ ok: boolean }>>,
  platform: 'ios' | 'android',
): Promise<{ ms: number; ok: number }> {
  const msgs = Array.from({ length: N }, (_, i) =>
    makeMessage({ target: { platform, token: `ok-${i}` } }),
  );
  const chunks: (typeof msgs)[] = [];
  for (let i = 0; i < N; i += CHUNK) chunks.push(msgs.slice(i, i + CHUNK));
  const t0 = performance.now();
  const results = await Promise.all(chunks.map((c) => send(c)));
  const ms = performance.now() - t0;
  return { ms, ok: results.flat().filter((r) => r.ok).length };
}

describe('throughput against local mocks', () => {
  it(`APNs: ${N} messages in under ${LIMIT_MS / 1000} s`, async () => {
    const server = await startServer('apns');
    const sender = new ApnsSender({
      privateKeyPem: ecKeys().privatePem,
      keyId: 'k',
      teamId: 't',
      bundleId: 'b',
      authority: `http://127.0.0.1:${server.port}`,
    });
    try {
      await sender.send([makeMessage()]); // connect before timing
      const { ms, ok } = await blast((m) => sender.send(m), 'ios');
      const stats = await server.stats();
      console.log(
        `THROUGHPUT apns: ${N} messages in ${ms.toFixed(0)} ms = ${Math.round((N / ms) * 1000)} msg/s (peak server in-flight ${stats.maxInFlight})`,
      );
      expect(ok).toBe(N);
      expect(stats.total).toBe(N + 1);
      expect(stats.maxInFlight).toBeLessThanOrEqual(500);
      expect(ms).toBeLessThan(LIMIT_MS);
    } finally {
      sender.close();
      server.stop();
    }
  }, 60_000);

  it(`FCM: ${N} messages in under ${LIMIT_MS / 1000} s`, async () => {
    const server = await startServer('fcm');
    const base = `http://127.0.0.1:${server.port}`;
    const sender = new FcmSender({
      serviceAccount: { client_email: 'e', private_key: rsaKeys().privatePem },
      projectId: 'p',
      tokenUri: `${base}/token`,
      baseUrl: base,
    });
    try {
      await sender.send([makeMessage({ target: { platform: 'android', token: 'warm' } })]);
      const { ms, ok } = await blast((m) => sender.send(m), 'android');
      const stats = await server.stats();
      console.log(
        `THROUGHPUT fcm: ${N} messages in ${ms.toFixed(0)} ms = ${Math.round((N / ms) * 1000)} msg/s (peak server in-flight ${stats.maxInFlight})`,
      );
      expect(ok).toBe(N);
      expect(stats.tokenRequests).toBe(1);
      expect(stats.total).toBe(N + 1);
      expect(stats.maxInFlight).toBeLessThanOrEqual(200);
      expect(ms).toBeLessThan(LIMIT_MS);
    } finally {
      sender.close();
      server.stop();
    }
  }, 60_000);
});
