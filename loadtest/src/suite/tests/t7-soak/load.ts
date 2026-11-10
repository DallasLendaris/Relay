/**
 * t7 child: the load driver. It talks to the API process over HTTP, like phones would:
 *   check-ins       steady rate, using real check-in tokens the sender processes report here
 *   place add/remove  on "flapper" and "quiet" devices (the extra places sit far from every alert area)
 *   push-token changes  flappers re-register the same token with a new language, quiet devices rotate tokens
 *   device erasure  steady rate, from a pool of devices that sit inside the alert areas
 * and it keeps the facts the parent needs to judge the run: which devices were erased and when, and how many
 * check-ins the API accepted per alert.
 */
import { randomBytes } from 'node:crypto';
import { createDeviceCredential } from '../../../../../backend/src/tokens/deviceCredential.js';
import { COUNTY_BOUNDS } from '../../../seed.js';
import { createRng } from '../../../mock-common.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import { DEVICE_CRED_KEY } from './keys.js';
import {
  QUIET_BOX,
  call,
  claimsOf,
  deferred,
  readConfig,
  readPorts,
  sleep,
  startSampler,
  startServer,
} from './common.js';

type Kind = 'E' | 'F' | 'Q';

interface Managed {
  id: string;
  cred: string;
  token: string;
  kind: Kind;
  /** The anchor place's cell (E and F); empty for Q. */
  cell: string;
  lang: 'en' | 'es';
  quiet: string[];
  busy: boolean;
  erase?: { start: number; end?: number };
}

class Reservoir {
  private readonly s: number[] = [];
  private seen = 0;
  constructor(private readonly cap = 20_000) {}
  add(ms: number): void {
    this.seen++;
    if (this.s.length < this.cap) this.s.push(ms);
    else {
      const i = Math.floor(Math.random() * this.seen);
      if (i < this.cap) this.s[i] = ms;
    }
  }
  summary(): { count: number; p50: number; p99: number; max: number } {
    const a = Float64Array.from(this.s).sort();
    const q = (p: number): number =>
      a.length === 0 ? 0 : (a[Math.min(a.length - 1, Math.ceil(p * a.length) - 1)] ?? 0);
    return {
      count: this.seen,
      p50: Math.round(q(0.5) * 10) / 10,
      p99: Math.round(q(0.99) * 10) / 10,
      max: Math.round((a[a.length - 1] ?? 0) * 10) / 10,
    };
  }
}

const TOKEN_QUEUE_CAP = 300_000;

runWorkerMain(async () => {
  const cfg = readConfig();
  const ports = readPorts();
  const apiPort = ports.apiPublic ?? 0;
  const rng = createRng(cfg.seed + 7);

  const managed: Managed[] = [];
  const byId = new Map<string, Managed>();
  const flappers: Managed[] = []; // F and Q: the place and token churners
  const erasePool: Managed[] = [];
  let eraseNext = 0;

  // ----- API client -----
  const api = async (
    method: string,
    path: string,
    cred?: string,
    body?: unknown,
  ): Promise<{ status: number; json: Record<string, unknown> | null }> => {
    const r = await call(
      apiPort,
      method,
      path,
      body === undefined ? undefined : JSON.stringify(body),
      cred ? { authorization: `Bearer ${cred}` } : {},
    );
    let json: Record<string, unknown> | null = null;
    if (r.body) {
      try {
        json = JSON.parse(r.body) as Record<string, unknown>;
      } catch {
        json = null;
      }
    }
    return { status: r.status, json };
  };

  const counters = {
    checkinsSent: 0,
    checkinsOk: 0,
    checkinsRejectedErased: 0,
    checkinsRepeatOk: 0,
    tokensExpiredSkipped: 0,
    tokensDroppedRepush: 0,
    tokensOverflow: 0,
    tokensReceived: 0,
    starvedTicks: 0,
    placeAdds: 0,
    placeRemoves: 0,
    tokenSame: 0,
    tokenRotate: 0,
    erasures: 0,
    errors: 0,
  };
  const errorSamples: string[] = [];
  const fail = (what: string): void => {
    counters.errors++;
    if (errorSamples.length < 20) errorSamples.push(what);
  };
  const acceptedByAlert = new Map<string, number>();
  const lat = {
    checkin: new Reservoir(),
    placeAdd: new Reservoir(),
    placeRemove: new Reservoir(),
    token: new Reservoir(),
    erase: new Reservoir(),
  };

  // ----- setup: the managed devices, created through the API -----
  const randomPoint = (box: { south: number; north: number; west: number; east: number }) => ({
    latitude: box.south + rng() * (box.north - box.south),
    longitude: box.west + rng() * (box.east - box.west),
  });
  const hex64 = (): string => randomBytes(32).toString('hex');

  async function createDevice(kind: Kind): Promise<void> {
    const created = await api('POST', '/devices');
    if (created.status !== 201 || !created.json) throw new Error(`create device ${created.status}`);
    const id = String(created.json['deviceId']);
    const cred = String(created.json['credential']);
    const token = hex64();
    const m: Managed = { id, cred, token, kind, cell: '', lang: 'en', quiet: [], busy: false };
    const put = await api('PUT', '/devices/me/push-token', cred, {
      platform: 'ios',
      token,
      language: 'en',
    });
    if (put.status !== 204) throw new Error(`push token ${put.status}`);
    if (kind !== 'Q') {
      const place = await api('POST', '/places', cred, {
        label: 'Home',
        ...randomPoint(COUNTY_BOUNDS),
      });
      if (place.status !== 201 || !place.json) throw new Error(`anchor place ${place.status}`);
      m.cell = String(place.json['h3Cell']);
    } else {
      const place = await api('POST', '/places', cred, {
        label: 'Away',
        ...randomPoint(QUIET_BOX),
      });
      if (place.status !== 201 || !place.json) throw new Error(`quiet place ${place.status}`);
      m.quiet.push(String(place.json['id']));
    }
    managed.push(m);
    byId.set(id, m);
    if (kind === 'E') erasePool.push(m);
    else flappers.push(m);
  }

  async function setup(e: number, f: number, q: number): Promise<void> {
    const jobs: Kind[] = [
      ...Array<Kind>(e).fill('E'),
      ...Array<Kind>(f).fill('F'),
      ...Array<Kind>(q).fill('Q'),
    ];
    let next = 0;
    const worker = async (): Promise<void> => {
      while (next < jobs.length) {
        const kind = jobs[next++];
        if (kind) await createDevice(kind);
      }
    };
    await Promise.all(Array.from({ length: 16 }, worker));
  }

  // ----- workload -----
  let on = false;
  let inflight = 0;
  const repush = new Set<string>();
  let tq: Array<[string, string]> = [];
  let tqHead = 0;
  const queued = (): number => tq.length - tqHead;

  /** Runs `op` at `rate` per second until the workload stops; `op` returns false when it had nothing to do. */
  function rateLoop(rate: number, maxInflight: number, op: () => Promise<void> | false): void {
    let owed = 0;
    let last = performance.now();
    let mine = 0;
    const tick = (): void => {
      if (!on) return;
      const now = performance.now();
      owed += ((now - last) / 1000) * rate;
      last = now;
      while (owed >= 1 && mine < maxInflight) {
        const p = op();
        if (p === false) {
          counters.starvedTicks++;
          break;
        }
        owed -= 1;
        mine++;
        inflight++;
        p.catch((e: unknown) => fail(e instanceof Error ? e.message : String(e))).finally(() => {
          mine--;
          inflight--;
        });
      }
      if (owed > rate) owed = rate; // never more than one second of catch-up
      setTimeout(tick, 20);
    };
    tick();
  }

  async function checkIn(alertId: string, token: string): Promise<void> {
    const c = claimsOf(token);
    const dev = byId.get(c.d);
    const cred = createDeviceCredential(c.d, DEVICE_CRED_KEY);
    const doOne = async (key: string, first: boolean): Promise<void> => {
      const sentAt = Date.now();
      const t0 = performance.now();
      const r = await api('POST', `/alerts/${alertId}/checkins`, cred, {
        status: 'ok',
        token,
        idempotencyKey: key,
      });
      if (first) lat.checkin.add(performance.now() - t0);
      counters.checkinsSent++;
      if (r.status === 200) {
        if (dev?.erase?.end !== undefined && dev.erase.end < sentAt) {
          fail(`check-in accepted for a device erased ${sentAt - dev.erase.end} ms earlier`);
        } else if (first) {
          counters.checkinsOk++;
          acceptedByAlert.set(alertId, (acceptedByAlert.get(alertId) ?? 0) + 1);
        } else counters.checkinsRepeatOk++;
      } else if (r.status === 401 && dev?.erase) {
        counters.checkinsRejectedErased++;
      } else {
        fail(`check-in status ${r.status} ${JSON.stringify(r.json)?.slice(0, 80)}`);
      }
    };
    await doOne(`ci-${randomBytes(6).toString('hex')}`, true);
    // One in twenty is retried with a new idempotency key: the API must answer with the original.
    if (counters.checkinsSent % 20 === 0)
      await doOne(`ci-${randomBytes(6).toString('hex')}`, false);
  }

  function nextToken(): [string, string] | undefined {
    for (;;) {
      if (tqHead >= tq.length) return undefined;
      const item = tq[tqHead++];
      if (tqHead > 50_000 && tqHead * 2 > tq.length) {
        tq = tq.slice(tqHead);
        tqHead = 0;
      }
      if (!item) return undefined;
      try {
        if (claimsOf(item[1]).exp * 1000 < Date.now() + 3000) {
          counters.tokensExpiredSkipped++;
          continue;
        }
      } catch {
        fail('unparseable check-in token');
        continue;
      }
      return item;
    }
  }

  async function placeOp(): Promise<void> {
    let m: Managed | undefined;
    for (let i = 0; i < 8 && !m; i++) {
      const c = flappers[Math.floor(rng() * flappers.length)];
      if (c && !c.busy) m = c;
    }
    if (!m) return;
    m.busy = true;
    try {
      const add = m.quiet.length === 0 || (m.quiet.length < 4 && rng() < 0.5);
      const t0 = performance.now();
      if (add) {
        const r = await api('POST', '/places', m.cred, {
          label: 'Away',
          ...randomPoint(QUIET_BOX),
        });
        lat.placeAdd.add(performance.now() - t0);
        if (r.status !== 201 || !r.json) throw new Error(`place add ${r.status}`);
        m.quiet.push(String(r.json['id']));
        counters.placeAdds++;
      } else {
        const id = m.quiet.splice(Math.floor(rng() * m.quiet.length), 1)[0];
        const r = await api('DELETE', `/places/${id}`, m.cred);
        lat.placeRemove.add(performance.now() - t0);
        if (r.status !== 204) throw new Error(`place remove ${r.status}`);
        counters.placeRemoves++;
      }
    } finally {
      m.busy = false;
    }
  }

  async function tokenOp(): Promise<void> {
    const m = flappers[Math.floor(rng() * flappers.length)];
    if (!m) return;
    const t0 = performance.now();
    let token = m.token;
    let lang = m.lang;
    if (m.kind === 'F') {
      // Same token, new language: the real endpoint and index write-through, nobody changes identity.
      lang = lang === 'en' ? 'es' : 'en';
    } else {
      token = hex64();
    }
    const r = await api('PUT', '/devices/me/push-token', m.cred, {
      platform: 'ios',
      token,
      language: lang,
    });
    lat.token.add(performance.now() - t0);
    if (r.status !== 204) throw new Error(`push token ${r.status}`);
    m.token = token;
    m.lang = lang;
    if (m.kind === 'F') counters.tokenSame++;
    else counters.tokenRotate++;
  }

  async function eraseOp(): Promise<void> {
    const m = erasePool[eraseNext++];
    if (!m) return;
    m.erase = { start: Date.now() };
    const t0 = performance.now();
    const r = await api('DELETE', '/devices/me', m.cred);
    lat.erase.add(performance.now() - t0);
    if (r.status !== 204) throw new Error(`erase ${r.status}`);
    m.erase.end = Date.now();
    counters.erasures++;
  }

  function startWorkload(): void {
    on = true;
    const r = cfg.rates;
    rateLoop(r.checkins, 256, () => {
      const item = nextToken();
      return item ? checkIn(item[0], item[1]) : false;
    });
    rateLoop(r.placeOps, 64, placeOp);
    rateLoop(r.tokenOps, 32, tokenOp);
    rateLoop(r.erasures, 16, eraseOp);
  }

  const stop = deferred();
  const sampler = startSampler('load', cfg.sampleMs, () => ({
    tokenQueue: queued(),
    inflight,
    erasedDevices: counters.erasures,
    acceptedAlerts: acceptedByAlert.size,
  }));

  const server = await startServer(async (r) => {
    const json = (): unknown => JSON.parse(r.body);
    if (r.method === 'POST' && r.path === '/tokens') {
      const items = json() as Array<[string, string]>;
      for (const it of items) {
        counters.tokensReceived++;
        if (repush.has(it[0])) {
          counters.tokensDroppedRepush++;
          continue;
        }
        tq.push(it);
      }
      const over = queued() - TOKEN_QUEUE_CAP;
      if (over > 0) {
        tqHead += over;
        counters.tokensOverflow += over;
      }
      return { body: { ok: true } };
    }
    if (r.method === 'POST' && r.path === '/setup') {
      const b = json() as { e: number; f: number; q: number };
      await setup(b.e, b.f, b.q);
      return { body: { managed: managed.length } };
    }
    if (r.method === 'POST' && r.path === '/start') {
      startWorkload();
      return { body: { ok: true } };
    }
    if (r.method === 'POST' && r.path === '/repush') {
      const b = json() as { alertId: string; on: boolean };
      if (b.on) repush.add(b.alertId);
      else repush.delete(b.alertId);
      return { body: { ok: true } };
    }
    if (r.method === 'POST' && r.path === '/stop-workload') {
      on = false;
      for (let i = 0; i < 600 && inflight > 0; i++) await sleep(100);
      return { body: { inflight } };
    }
    if (r.method === 'GET' && r.path === '/state') {
      return {
        body: {
          now: Date.now(),
          devices: managed
            .filter((m) => m.kind !== 'Q')
            .map((m) => ({ token: m.token, cell: m.cell, kind: m.kind, erase: m.erase ?? null })),
        },
      };
    }
    if (r.method === 'GET' && r.path === '/stats') {
      return {
        body: {
          counters,
          errorSamples,
          acceptedByAlert: Object.fromEntries(acceptedByAlert),
          latencyMs: {
            checkin: lat.checkin.summary(),
            placeAdd: lat.placeAdd.summary(),
            placeRemove: lat.placeRemove.summary(),
            token: lat.token.summary(),
            erase: lat.erase.summary(),
          },
          tokenQueue: queued(),
          managed: managed.length,
        },
      };
    }
    if (r.method === 'POST' && r.path === '/stop') {
      stop.resolve();
      return { body: { ok: true } };
    }
    return { status: 404, body: 'not found' };
  });

  await sendMessage({ kind: 'ready', port: server.port });
  await stop.promise;
  on = false;
  sampler();
  await sleep(50);
  server.close();
  return { counters, errorSamples, acceptedByAlert: Object.fromEntries(acceptedByAlert) };
});
