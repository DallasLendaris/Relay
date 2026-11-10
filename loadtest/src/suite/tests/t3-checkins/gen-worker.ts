/**
 * t3 load generator: open-loop (each request has a scheduled send time from the arrival curve,
 * independent of earlier responses) over keep-alive HTTP agents. Latency is measured from the
 * SCHEDULED time, so a generator or server that falls behind shows up as latency, not as a lower
 * request rate. Every request carries a signed stateless device credential and a v2 check-in token.
 *
 * Generator g owns devices i with i % L === g; a device always goes to API process i % K.
 */
import http from 'node:http';
import { createCheckInToken } from '../../../../../backend/src/tokens/checkInToken.js';
import { createDeviceCredential } from '../../../../../backend/src/tokens/deviceCredential.js';
import {
  ALERT_ID,
  arrivalSec,
  bucketOf,
  cellOf,
  classOf,
  deviceIdOf,
  DEVICE_CRED_KEY,
  HIST_BUCKETS,
  isDuplicate,
  nowMs,
  TOKEN_KEY,
  type DeviceClass,
  type GenResult,
  type GenRun,
} from './common.js';

const PATH = `/alerts/${ALERT_ID}/checkins`;
const EXPECTED: Record<DeviceClass, number> = { valid: 200, tampered: 403, tombstoned: 401 };

function tamper(token: string): string {
  const dot = token.indexOf('.');
  const at = dot + 6;
  const c = token[at] === 'A' ? 'B' : 'A';
  return token.slice(0, at) + c + token.slice(at + 1);
}

async function run(r: GenRun): Promise<GenResult> {
  const agent = new http.Agent({
    keepAlive: true,
    maxSockets: r.maxSockets,
    maxFreeSockets: r.maxSockets,
  });
  const K = r.ports.length;
  const hist = new Array<number>(HIST_BUCKETS).fill(0);
  const res: GenResult = {
    type: 'result',
    hist,
    maxMs: 0,
    sent: 0,
    responses: 0,
    ok200: 0,
    s401: 0,
    s403: 0,
    s5xx: 0,
    otherStatus: 0,
    connErrors: 0,
    unexpected: 0,
    firstSchedMs: 0,
    lastDoneMs: 0,
    lastIssuedMs: 0,
    maxRssKb: 0,
  };
  const expiresAt = new Date(Date.now() + 3_600_000);
  let inflight = 0;
  let allIssued = false;
  let finished: () => void = () => {};
  const idle = new Promise<void>((resolve) => (finished = resolve));

  const issue = (i: number, second: boolean, schedMs: number): void => {
    const deviceId = deviceIdOf(i);
    const cls = classOf(i);
    let token = createCheckInToken(
      { alertId: ALERT_ID, deviceId, expiresAt, h3Cell: cellOf(i) },
      TOKEN_KEY,
    );
    if (cls === 'tampered') token = tamper(token);
    const body = JSON.stringify({
      status: 'ok',
      token,
      idempotencyKey: `t3-${i}-${second ? 'b' : 'a'}`,
    });
    const expected = EXPECTED[cls];
    let settled = false;
    const done = (status: number): void => {
      if (settled) return;
      settled = true;
      const t = nowMs();
      const ms = t - schedMs;
      hist[bucketOf(ms)] = (hist[bucketOf(ms)] ?? 0) + 1;
      if (ms > res.maxMs) res.maxMs = ms;
      res.lastDoneMs = t;
      res.responses++;
      if (status === 200) res.ok200++;
      else if (status === 401) res.s401++;
      else if (status === 403) res.s403++;
      else if (status >= 500) res.s5xx++;
      else if (status === 0) res.connErrors++;
      else res.otherStatus++;
      if (status !== expected) res.unexpected++;
      if (--inflight === 0 && allIssued) finished();
    };
    inflight++;
    res.sent++;
    if (!second) res.lastIssuedMs = nowMs();
    const req = http.request(
      {
        host: '127.0.0.1',
        port: r.ports[i % K],
        method: 'POST',
        path: PATH,
        agent,
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(body),
          authorization: `Bearer ${createDeviceCredential(deviceId, DEVICE_CRED_KEY)}`,
        },
      },
      (resp) => {
        resp.resume();
        resp.on('end', () => done(resp.statusCode ?? 0));
        resp.on('error', () => done(0));
      },
    );
    req.setTimeout(120_000, () => req.destroy());
    req.on('error', () => done(0));
    req.end(body);
  };

  // Two ascending streams over this generator's devices: first taps, and (for 5% of the valid
  // devices) a second tap dupDelaySec later. Merge them by scheduled time.
  const schedOf = (i: number): number => r.t0Ms + arrivalSec(i, r.n, r.segments) * 1000;
  let p = r.g;
  let q = r.g;
  const advanceDup = (): void => {
    while (q < r.n && !isDuplicate(q)) q += r.L;
  };
  advanceDup();
  res.firstSchedMs = p < r.n ? schedOf(p) : r.t0Ms;

  const BURST = 1500;
  while (p < r.n || q < r.n) {
    const now = nowMs();
    let issued = 0;
    while (issued < BURST) {
      const tp = p < r.n ? schedOf(p) : Infinity;
      const tq = q < r.n ? schedOf(q) + r.dupDelaySec * 1000 : Infinity;
      const t = Math.min(tp, tq);
      if (t === Infinity || t > now) break;
      if (tp <= tq) {
        issue(p, false, tp);
        p += r.L;
      } else {
        issue(q, true, tq);
        q += r.L;
        advanceDup();
      }
      issued++;
    }
    if (p >= r.n && q >= r.n) break;
    if (issued >= BURST) await new Promise<void>((ok) => setImmediate(ok));
    else await new Promise<void>((ok) => setTimeout(ok, 1));
  }
  allIssued = true;
  if (inflight === 0) finished();
  await idle;
  agent.destroy();
  res.maxRssKb = process.resourceUsage().maxRSS;
  return res;
}

process.on('disconnect', () => process.exit(0));
process.on('message', (m: GenRun) => {
  if (m.type !== 'run') return;
  void run(m).then((out) => process.send?.(out));
});
process.send?.({ type: 'loaded' });
