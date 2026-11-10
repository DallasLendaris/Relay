/**
 * t5 child process: a mock APNs or FCM server with scripted fault injection, a control port, and
 * the exactly-once verification against the expected-token file. One process per platform, kept
 * alive across scenarios (`/reset` between them).
 *
 * Args: --kind apns|fcm --expected FILE --fcm-rate N
 * Sends the message { url, control } to the parent once listening.
 *
 * Control (JSON over HTTP on 127.0.0.1):
 *   POST /reset  { rules, vclock }   clear counters, install fault rules (virtual time starts at the first request)
 *   POST /mark?name=X                remember the time of the next accepted push after now (time to resume)
 *   GET  /stats                      live counters (cheap; polled by the parent)
 *   GET  /report                     verification against the expected file (expensive, once per scenario)
 */
import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { numberArg, parseArgs } from '../../../mock-common.js';
import { startMockApns, type InjectedResponse, type MockApns } from '../../../mock-apns.js';
import { startMockFcm, type MockFcm } from '../../../mock-fcm.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import {
  isDeadToken,
  json,
  sleep,
  startServer,
  type FaultKind,
  type FaultRule,
  type VClock,
} from './common.js';

type Platform = 'apns' | 'fcm';

function apnsResponse(kind: FaultKind, retryAfterS: number | undefined): InjectedResponse {
  const headers: Record<string, string> =
    retryAfterS === undefined ? {} : { 'retry-after': String(Math.ceil(retryAfterS)) };
  switch (kind) {
    case 'throttle':
      return { status: 429, body: { reason: 'TooManyRequests' }, headers };
    case 'unavailable':
      return { status: 503, body: { reason: 'ServiceUnavailable' }, headers };
    case 'unregistered':
      return { status: 410, body: { reason: 'Unregistered', timestamp: Date.now() } };
    case 'badtoken':
      return { status: 400, body: { reason: 'BadDeviceToken' } };
  }
}

const fcmError = (code: number, status: string, errorCode: string, extra: object[] = []) => ({
  error: {
    code,
    message: 'injected',
    status,
    details: [
      { '@type': 'type.googleapis.com/google.firebase.fcm.v1.FcmError', errorCode },
      ...extra,
    ],
  },
});

function fcmResponse(kind: FaultKind, retryAfterS: number | undefined): InjectedResponse {
  const headers: Record<string, string> =
    retryAfterS === undefined ? {} : { 'retry-after': String(Math.ceil(retryAfterS)) };
  switch (kind) {
    case 'throttle':
      return {
        status: 429,
        body: fcmError(429, 'RESOURCE_EXHAUSTED', 'QUOTA_EXCEEDED'),
        headers,
      };
    case 'unavailable':
      return { status: 503, body: fcmError(503, 'UNAVAILABLE', 'UNAVAILABLE'), headers };
    case 'unregistered':
      return { status: 404, body: fcmError(404, 'NOT_FOUND', 'UNREGISTERED') };
    case 'badtoken':
      return {
        status: 400,
        body: fcmError(400, 'INVALID_ARGUMENT', 'INVALID_ARGUMENT', [
          {
            '@type': 'type.googleapis.com/google.rpc.BadRequest',
            fieldViolations: [{ field: 'message.token', description: 'invalid' }],
          },
        ]),
      };
  }
}

runWorkerMain(async () => {
  const args = parseArgs(process.argv.slice(2));
  const platform = (args['kind'] ?? 'apns') as Platform;
  const expectedFile = args['expected'] ?? '';
  const fcmRate = numberArg(args, 'fcm-rate') ?? 10_000;
  const latency = { p50Ms: numberArg(args, 'p50') ?? 20, p99Ms: numberArg(args, 'p99') ?? 150 };

  // ----- scenario state -----
  let rules: FaultRule[] = [];
  let clock: VClock = { epochMs: Date.now(), scale: 1 };
  let deadRate = 0;
  let t0Real: number | null = null;
  let injected: Record<string, number> = {};
  let deadAnswered = new Set<string>();
  let okTimeline: number[] = []; // accepted pushes per real second since the first request
  let firstOkAt: number | null = null;
  let lastOkAt: number | null = null;
  const marks = new Map<string, { at: number; firstOkAfter: number | null }>();
  let rngState = 12345;
  const random = (): number => {
    rngState = (Math.imul(rngState, 1664525) + 1013904223) >>> 0;
    return rngState / 4294967296;
  };

  const inject = (token: string): InjectedResponse | undefined => {
    const now = Date.now();
    if (t0Real === null) t0Real = now;
    const tS = ((now - t0Real) / 1000) * clock.scale;
    for (const r of rules) {
      if (r.platform && r.platform !== platform) continue;
      if (tS < r.fromS || tS >= r.toS) continue;
      const hit = r.byToken ? isDeadToken(token, r.p) : random() < r.p;
      if (!hit) continue;
      injected[r.kind] = (injected[r.kind] ?? 0) + 1;
      if (r.kind === 'unregistered') deadAnswered.add(token);
      return platform === 'apns'
        ? apnsResponse(r.kind, r.retryAfterS)
        : fcmResponse(r.kind, r.retryAfterS);
    }
    return undefined;
  };

  let server: MockApns | MockFcm;
  const noErrors = { unregistered: 0, tooManyRequests: 0, serverError: 0 };
  if (platform === 'apns') {
    server = await startMockApns({
      trackAttempts: false,
      errorMix: noErrors,
      goawayProbability: 0,
      latency,
      inject,
    });
  } else {
    server = await startMockFcm({
      trackAttempts: false,
      errorMix: noErrors,
      ratePerSecond: fcmRate,
      latency,
      inject,
    });
  }

  // Count accepted pushes per second without touching the mock: sample its counter.
  let lastOk = 0;
  const sampler = setInterval(() => {
    const ok = server.stats().ok;
    const delta = ok - lastOk;
    lastOk = ok;
    if (t0Real === null) return;
    const sec = Math.floor((Date.now() - t0Real) / 1000);
    while (okTimeline.length <= sec) okTimeline.push(0);
    okTimeline[sec] = (okTimeline[sec] ?? 0) + delta;
    if (delta > 0) {
      const now = Date.now();
      if (firstOkAt === null) firstOkAt = now;
      lastOkAt = now;
      for (const m of marks.values())
        if (m.firstOkAfter === null && now >= m.at) m.firstOkAfter = now;
    }
  }, 100);
  sampler.unref();

  const control = await startServer(async (method, path, query, body) => {
    if (method === 'POST' && path === '/reset') {
      const b = json<{ rules: FaultRule[]; vclock: VClock; deadRate?: number }>(body);
      server.reset();
      rules = b.rules;
      clock = b.vclock;
      deadRate = b.deadRate ?? 0;
      t0Real = null;
      injected = {};
      deadAnswered = new Set();
      okTimeline = [];
      lastOk = 0;
      firstOkAt = null;
      lastOkAt = null;
      marks.clear();
      return { ok: true };
    }
    if (method === 'POST' && path === '/mark') {
      marks.set(query.get('name') ?? 'mark', { at: Date.now(), firstOkAfter: null });
      return { ok: true };
    }
    if (method === 'GET' && path === '/stats') {
      const s = server.stats();
      return {
        platform,
        ok: s.ok,
        received: s.received,
        injected,
        firstOkAt,
        lastOkAt,
        t0Real,
        marks: Object.fromEntries(marks),
        okTimeline,
        rateLimited: 'rateLimited' in s ? s.rateLimited : 0,
        deadRate,
      };
    }
    if (method === 'GET' && path === '/report') {
      await sleep(300); // let the sampler and in-flight responses settle
      const counters = server.counters;
      let expected = 0;
      let deliveredExpected = 0;
      let missing = 0;
      let missingExcused = 0;
      let duplicateTokens = 0;
      let extraDeliveries = 0;
      const dupSample: string[] = [];
      if (expectedFile) {
        const rl = createInterface({ input: createReadStream(expectedFile, 'utf8') });
        for await (const line of rl) {
          const token = line.trim();
          if (token === '') continue;
          expected++;
          const n = counters.deliveredCount(token);
          if (n === 0) {
            if (deadAnswered.has(token)) missingExcused++;
            else missing++;
          } else {
            deliveredExpected++;
            if (n > 1) {
              duplicateTokens++;
              extraDeliveries += n - 1;
              if (dupSample.length < 5) dupSample.push(token.slice(0, 8));
            }
          }
        }
      }
      return {
        platform,
        expected,
        deliveredExpected,
        missing,
        missingExcused,
        duplicateTokens,
        extraDeliveries,
        dupSample,
        distinctDelivered: counters.delivered.size,
        unexpectedDelivered: counters.delivered.size - deliveredExpected,
        deadAnswered: deadAnswered.size,
        injected,
        ok: server.stats().ok,
      };
    }
    return {};
  });

  await sendMessage({
    url: server.url,
    control: `http://127.0.0.1:${control.port}`,
  });
  // Run until the parent kills us.
  await new Promise<void>(() => undefined);
  return {};
});
