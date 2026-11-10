/**
 * t9 child: the signup-surge load driver. New devices arrive on a fixed schedule (open loop: a slow API
 * does not slow the arrivals) and each one registers through the REAL API: POST /devices, PUT
 * /devices/me/push-token, POST /places (an address; the API's fake geocoder puts it in the alert
 * area, and a point it refuses as outside California is retried with a new address).
 *
 * Flows, by arrival number i:
 *   reuse   2% of devices: take the push token of a device that registered a moment ago (a reinstall:
 *           one move of that token, which the move rules allow)
 *   rapid   1% of devices, in groups of 5: five devices in a row claim the same token. The rules allow
 *           two moves in 24 h, so the answers must be 200, 200, 200, 409, 409
 *   single  everything else: a fresh token
 *
 * It keeps, per push token, whether the device that holds it at the end saved a place (that token must
 * get exactly one late-join push) and when the first place for it was saved. Those go to files for the
 * parent's exactly-once and latency checks.
 *
 * Control: run (returns at once), status, finish, stop.
 */
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { HashList } from '../t2-fanout/verify.js';
import { call as httpCall } from '../t7-soak/common.js';
import { runCtlWorker, sleep } from '../t8-concurrent/ctl.js';
import { quantile, round } from '../t8-concurrent/ledger.js';
import { addressFor } from './geocoder.js';

export interface LoadRunOptions {
  apiPort: number;
  /** Arrivals in all (devices). */
  devices: number;
  perMin: number;
  seed: number;
  outDir: string;
}

export interface Latency {
  n: number;
  p50: number;
  p99: number;
  max: number;
}

export interface LoadFinal {
  devicesCreated: number;
  startedAt: number;
  finishedAt: number;
  /** Devices fully registered per minute over the run. */
  achievedPerMin: number;
  /** How late the driver launched flows against the schedule (ms). A big number means the driver, not the API, was the limit. */
  launchLagP99: number;
  requests: number;
  errors: number;
  networkErrors: number;
  /** HTTP status counts by "call status". */
  statuses: Record<string, number>;
  latency: { create: Latency; token: Latency; place: Latency; flow: Latency };
  outsideRetries: number;
  placeGaveUp: number;
  singles: number;
  reuse: { attempted: number; moved: number; rejected: number };
  rapid: {
    groups: number;
    ok: number;
    mismatched: number;
    rejected409: number;
    patterns: Record<string, number>;
  };
  /** Tokens whose final holder saved a place: each must get exactly one late-join push. */
  expectedTokens: number;
  /** Tokens that more than one new device held at some point (reinstalls and rapid moves). */
  movedTokens: number;
  files: { tokens: string; firstPlaceAt: string; moved: string };
}

const B64URL = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

function mulberry(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function iosToken(rng: () => number): string {
  let s = '';
  for (let i = 0; i < 8; i++) s += ((rng() * 4294967296) >>> 0).toString(16).padStart(8, '0');
  return s;
}

function androidToken(rng: () => number): string {
  let s = '';
  while (s.length < 154) {
    let w = (rng() * 4294967296) >>> 0;
    for (let k = 0; k < 5; k++) {
      s += B64URL[w & 63];
      w >>>= 6;
    }
  }
  return `${s.slice(0, 22)}:APA91b${s.slice(22, 154)}`;
}

const lat = (xs: number[]): Latency => ({
  n: xs.length,
  p50: round(quantile(xs, 0.5), 1),
  p99: round(quantile(xs, 0.99), 1),
  max: round(
    xs.reduce((m, v) => Math.max(m, v), 0),
    1,
  ),
});

interface Donor {
  token: string;
  platform: 'ios' | 'android';
}

runCtlWorker(async () => {
  let opts: LoadRunOptions | undefined;
  let done = false;
  let launched = 0;
  let finishedFlows = 0;
  let startedAt = 0;
  let finishedAt = 0;
  let runError: string | undefined;

  const latCreate: number[] = [];
  const latToken: number[] = [];
  const latPlace: number[] = [];
  const latFlow: number[] = [];
  const lags: number[] = [];
  const statuses = new Map<string, number>();
  let requests = 0;
  let errors = 0;
  let networkErrors = 0;
  let outsideRetries = 0;
  let placeGaveUp = 0;
  let devicesCreated = 0;
  let singles = 0;
  const reuse = { attempted: 0, moved: 0, rejected: 0 };
  const rapid = {
    groups: 0,
    ok: 0,
    mismatched: 0,
    rejected409: 0,
    patterns: {} as Record<string, number>,
  };
  /** token -> state of its final holder. */
  const tokens = new Map<string, { holderPlaced: boolean; firstPlaceAt: number }>();
  const donors: Donor[] = [];
  const movedList = new HashList();

  const req = async (
    kind: 'create' | 'token' | 'place',
    method: string,
    path: string,
    body: string,
    credential?: string,
  ): Promise<{ status: number; body: string }> => {
    const o = opts as LoadRunOptions;
    const t0 = performance.now();
    requests++;
    let status = 0;
    let text = '';
    try {
      const r = await httpCall(
        o.apiPort,
        method,
        path,
        body,
        credential ? { authorization: `Bearer ${credential}` } : {},
        60_000,
      );
      status = r.status;
      text = r.body;
    } catch {
      networkErrors++;
    }
    const ms = performance.now() - t0;
    (kind === 'create' ? latCreate : kind === 'token' ? latToken : latPlace).push(ms);
    const k = `${kind} ${status}`;
    statuses.set(k, (statuses.get(k) ?? 0) + 1);
    return { status, body: text };
  };

  interface Registered {
    pushStatus: number;
    placeOk: boolean;
  }

  /** One device: create, push token, place. `expect` is the push-token status that is not an error. */
  const registerDevice = async (
    n: number,
    token: string,
    platform: 'ios' | 'android',
    expect: 200 | 409,
  ): Promise<Registered> => {
    const t0 = performance.now();
    const c = await req('create', 'POST', '/devices', '{}');
    if (c.status !== 201) {
      errors++;
      latFlow.push(performance.now() - t0);
      return { pushStatus: 0, placeOk: false };
    }
    devicesCreated++;
    const created = JSON.parse(c.body) as {
      deviceId: string;
      credential: string;
      fcmProject?: string;
    };
    const body = JSON.stringify({
      platform,
      token,
      language: 'en',
      ...(platform === 'android' && created.fcmProject ? { fcmProject: created.fcmProject } : {}),
    });
    const p = await req('token', 'PUT', '/devices/me/push-token', body, created.credential);
    // The API answers 204 No Content when it takes the token.
    const moved = p.status === 204 || p.status === 200;
    const effective = moved ? 200 : p.status;
    if (effective !== expect) errors++;
    if (moved) {
      const rec = tokens.get(token);
      if (rec) {
        rec.holderPlaced = false;
        movedList.add(token);
      } else tokens.set(token, { holderPlaced: false, firstPlaceAt: Number.POSITIVE_INFINITY });
    }
    let placeOk = false;
    for (let attempt = 0; attempt < 5 && !placeOk; attempt++) {
      const pl = await req(
        'place',
        'POST',
        '/places',
        JSON.stringify({ label: 'Home', address: addressFor(n, attempt) }),
        created.credential,
      );
      if (pl.status === 201) {
        placeOk = true;
        break;
      }
      if (pl.status === 422) {
        outsideRetries++;
        continue;
      }
      errors++;
      break;
    }
    if (!placeOk) placeGaveUp++;
    if (placeOk && moved) {
      const rec = tokens.get(token);
      if (rec) {
        rec.holderPlaced = true;
        rec.firstPlaceAt = Math.min(rec.firstPlaceAt, Date.now());
      }
    }
    latFlow.push(performance.now() - t0);
    return { pushStatus: effective, placeOk };
  };

  const fresh = (i: number, k = 0): { token: string; platform: 'ios' | 'android' } => {
    const rng = mulberry((opts as LoadRunOptions).seed * 1_000_003 + i * 31 + k);
    const platform = i % 5 < 3 ? 'ios' : 'android';
    return { token: platform === 'ios' ? iosToken(rng) : androidToken(rng), platform };
  };

  const single = async (i: number): Promise<void> => {
    const { token, platform } = fresh(i);
    singles++;
    const r = await registerDevice(i, token, platform, 200);
    if (r.pushStatus === 200 && r.placeOk) donors.push({ token, platform });
  };

  const reuseFlow = async (i: number): Promise<void> => {
    const donor = donors.shift();
    if (!donor) return single(i);
    reuse.attempted++;
    const r = await registerDevice(i, donor.token, donor.platform, 200);
    if (r.pushStatus === 200) reuse.moved++;
    else reuse.rejected++;
  };

  const rapidFlow = async (i: number): Promise<void> => {
    const { token, platform } = fresh(i, 7);
    const got: number[] = [];
    for (let k = 0; k < 5; k++) {
      const r = await registerDevice(i + k, token, platform, k < 3 ? 200 : 409);
      got.push(r.pushStatus);
    }
    rapid.groups++;
    const pattern = got.join(',');
    rapid.patterns[pattern] = (rapid.patterns[pattern] ?? 0) + 1;
    rapid.rejected409 += got.filter((s) => s === 409).length;
    if (pattern === '200,200,200,409,409') rapid.ok++;
    else rapid.mismatched++;
  };

  const run = async (o: LoadRunOptions): Promise<void> => {
    startedAt = Date.now();
    const t0 = performance.now();
    const gap = 60_000 / o.perMin;
    let next = 0;
    let inflight = 0;
    await new Promise<void>((resolve) => {
      const tick = (): void => {
        const now = performance.now() - t0;
        while (next < o.devices && next * gap <= now) {
          const i = next;
          const rapidStart = i % 500 === 250 && i + 5 <= o.devices;
          const kind = rapidStart ? 'rapid' : i % 100 === 5 || i % 100 === 55 ? 'reuse' : 'single';
          lags.push(now - i * gap);
          next += rapidStart ? 5 : 1;
          launched++;
          inflight++;
          const flow =
            kind === 'rapid' ? rapidFlow(i) : kind === 'reuse' ? reuseFlow(i) : single(i);
          void flow
            .catch((e: unknown) => {
              errors++;
              runError ??= e instanceof Error ? e.message : String(e);
            })
            .finally(() => {
              inflight--;
              finishedFlows++;
              if (next >= o.devices && inflight === 0) resolve();
            });
        }
        if (next < o.devices) setTimeout(tick, 4);
      };
      tick();
    });
    finishedAt = Date.now();
    done = true;
  };

  return {
    handlers: {
      run: (o: LoadRunOptions) => {
        opts = o;
        void run(o).catch((e: unknown) => {
          runError = e instanceof Error ? e.message : String(e);
          finishedAt = Date.now();
          done = true;
        });
        return { started: true };
      },
      status: () => ({
        done,
        launched,
        finishedFlows,
        devicesCreated,
        errors,
        error: runError ?? null,
      }),
      finish: async (): Promise<LoadFinal> => {
        while (!done) await sleep(50);
        const o = opts as LoadRunOptions;
        const list = new HashList();
        const first: number[] = [];
        for (const [token, rec] of tokens) {
          if (!rec.holderPlaced) continue;
          list.add(token);
          first.push(rec.firstPlaceAt);
        }
        const tokensFile = join(o.outDir, 'load-expected-tokens.bin');
        const firstFile = join(o.outDir, 'load-first-place.bin');
        const words = list.toArray();
        writeFileSync(tokensFile, Buffer.from(words.buffer, words.byteOffset, words.byteLength));
        const times = Float64Array.from(first);
        writeFileSync(firstFile, Buffer.from(times.buffer, times.byteOffset, times.byteLength));
        const movedFile = join(o.outDir, 'load-moved-tokens.bin');
        const mw = movedList.toArray();
        writeFileSync(movedFile, Buffer.from(mw.buffer, mw.byteOffset, mw.byteLength));
        const minutes = Math.max(1 / 60, (finishedAt - startedAt) / 60_000);
        return {
          devicesCreated,
          startedAt,
          finishedAt,
          achievedPerMin: Math.round(devicesCreated / minutes),
          launchLagP99: round(quantile(lags, 0.99), 1),
          requests,
          errors,
          networkErrors,
          statuses: Object.fromEntries(statuses),
          latency: {
            create: lat(latCreate),
            token: lat(latToken),
            place: lat(latPlace),
            flow: lat(latFlow),
          },
          outsideRetries,
          placeGaveUp,
          singles,
          reuse,
          rapid,
          expectedTokens: list.n,
          movedTokens: movedList.n,
          files: { tokens: tokensFile, firstPlaceAt: firstFile, moved: movedFile },
        };
      },
    },
  };
});
