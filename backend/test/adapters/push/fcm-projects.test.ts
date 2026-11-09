import { randomUUID } from 'node:crypto';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { FcmSender } from '../../../src/adapters/push/fcm-sender.js';
import {
  FcmProjectSender,
  assignFcmProject,
  parseFcmProjectIds,
} from '../../../src/adapters/push/fcm-projects.js';
import { TokenBucket } from '../../../src/adapters/push/token-bucket.js';
import { validateFcmServiceAccounts } from '../../../src/adapters/push/validate-secrets.js';
import type { NativePushMessage, NativePushResult } from '../../../src/scale/contracts.js';
import { makeMessage, MockFcm, rsaKeys } from './mocks.js';

const keys = rsaKeys();
const IDS = ['relay-a', 'relay-b', 'relay-c'];

const android = (token: string, project?: string): NativePushMessage =>
  makeMessage({ target: { platform: 'android', token, ...(project ? { project } : {}) } });

/** A fake clock whose sleep advances time, so bucket tests are exact and instant. */
function fakeTime() {
  let t = 1_000_000;
  const slept: number[] = [];
  return {
    now: () => t,
    sleep: async (ms: number) => {
      slept.push(ms);
      t += ms;
    },
    slept,
    advance: (ms: number) => (t += ms),
  };
}

describe('assignFcmProject', () => {
  it('is stable for a device and empty when no projects are configured', () => {
    const id = randomUUID();
    expect(assignFcmProject(id, IDS)).toBe(assignFcmProject(id, IDS));
    expect(IDS).toContain(assignFcmProject(id, IDS));
    expect(assignFcmProject(id, [])).toBeUndefined();
    expect(assignFcmProject(id, ['only'])).toBe('only');
  });

  it('spreads devices roughly evenly', () => {
    const counts = new Map<string, number>();
    const n = 30_000;
    for (let i = 0; i < n; i++) {
      const p = assignFcmProject(randomUUID(), IDS) ?? '';
      counts.set(p, (counts.get(p) ?? 0) + 1);
    }
    expect([...counts.keys()].sort()).toEqual(IDS);
    for (const c of counts.values()) {
      expect(c).toBeGreaterThan((n / IDS.length) * 0.93);
      expect(c).toBeLessThan((n / IDS.length) * 1.07);
    }
  });
});

describe('parseFcmProjectIds', () => {
  it('splits, trims and rejects duplicates', () => {
    expect(parseFcmProjectIds(undefined)).toEqual([]);
    expect(parseFcmProjectIds('')).toEqual([]);
    expect(parseFcmProjectIds(' a-proj , b-proj ')).toEqual(['a-proj', 'b-proj']);
    expect(() => parseFcmProjectIds('a,a')).toThrow(/duplicate/);
  });
});

describe('validateFcmServiceAccounts', () => {
  const sa = (project_id: string) => ({
    project_id,
    client_email: `relay@${project_id}.iam.gserviceaccount.com`,
    private_key: keys.privatePem,
  });

  it('still accepts a single service account object (one project)', () => {
    const out = validateFcmServiceAccounts(sa('relay-a'));
    expect(out.map((a) => a.project_id)).toEqual(['relay-a']);
  });

  it('accepts an array, keeping the order', () => {
    expect(
      validateFcmServiceAccounts([sa('relay-b'), sa('relay-a')]).map((a) => a.project_id),
    ).toEqual(['relay-b', 'relay-a']);
  });

  it('rejects an empty array, a duplicate project and a bad entry without echoing values', () => {
    expect(() => validateFcmServiceAccounts([])).toThrow(/empty array/);
    expect(() => validateFcmServiceAccounts([sa('relay-a'), sa('relay-a')])).toThrow(/duplicate/);
    expect(() =>
      validateFcmServiceAccounts([sa('relay-a'), { ...sa('relay-b'), private_key: 'REPLACE_ME' }]),
    ).toThrow(/#1: .*private_key is still the REPLACE_ME placeholder/);
  });
});

describe('TokenBucket', () => {
  it('lets a burst through and then paces to the rate', async () => {
    const time = fakeTime();
    const bucket = new TokenBucket(100, 100, time.now, time.sleep);
    await bucket.take(100); // the burst
    expect(time.slept).toEqual([]);
    await bucket.take(50); // 50 over: 500 ms at 100/s
    expect(time.slept).toEqual([500]);
    await bucket.take(100); // another 100 owed on top of nothing left
    expect(time.slept[1]).toBe(1000);
  });

  it('refills with time, capped at the burst, and allows a request larger than the burst', async () => {
    const time = fakeTime();
    const bucket = new TokenBucket(10, 10, time.now, time.sleep);
    await bucket.take(10);
    time.advance(60_000);
    expect(bucket.available()).toBe(10); // capped at one second of rate
    await bucket.take(30); // 20 over the burst
    expect(time.slept).toEqual([2000]);
  });

  it('rejects nonsense configuration', () => {
    expect(() => new TokenBucket(0)).toThrow(RangeError);
    expect(() => new TokenBucket(10, 0)).toThrow(RangeError);
  });
});

describe('FcmProjectSender', () => {
  /** One fake sender per project that records which tokens it was given. */
  function fakes() {
    const calls = new Map<string, string[]>();
    const projects = IDS.map((projectId) => ({
      projectId,
      sender: {
        async send(ms: NativePushMessage[]): Promise<NativePushResult[]> {
          calls.set(projectId, [...(calls.get(projectId) ?? []), ...ms.map((m) => m.target.token)]);
          return ms.map((m) =>
            m.target.token.startsWith('gone-')
              ? { ok: false, kind: 'permanent', error: `${projectId}:gone` }
              : { ok: true },
          );
        },
      },
    }));
    return { calls, projects };
  }

  it('routes each message to the sender of its project, in order, defaulting to the first', async () => {
    const { calls, projects } = fakes();
    const time = fakeTime();
    const sender = new FcmProjectSender({ projects, now: time.now, sleep: time.sleep });
    const out = await sender.send([
      android('t0', 'relay-b'),
      android('t1'), // no project: the default (first) project
      android('gone-2', 'relay-c'),
      android('t3', 'relay-a'),
      android('t4', 'relay-b'),
    ]);
    expect(calls.get('relay-a')).toEqual(['t1', 't3']);
    expect(calls.get('relay-b')).toEqual(['t0', 't4']);
    expect(calls.get('relay-c')).toEqual(['gone-2']);
    expect(out).toEqual([
      { ok: true },
      { ok: true },
      { ok: false, kind: 'permanent', error: 'relay-c:gone' },
      { ok: true },
      { ok: true },
    ]);
  });

  it('fails a message for an unknown project as permanent for the message only', async () => {
    const { calls, projects } = fakes();
    const sender = new FcmProjectSender({ projects });
    const out = await sender.send([android('t0', 'not-configured'), android('t1', 'relay-b')]);
    expect(out[0]).toEqual({
      ok: false,
      kind: 'permanent',
      scope: 'message',
      error: 'UnknownFcmProject',
    });
    expect(out[1]).toEqual({ ok: true });
    expect(calls.has('not-configured')).toBe(false);
  });

  it('a sender that throws fails only its own project, as retryable', async () => {
    const { projects } = fakes();
    const boom = {
      send: async (): Promise<NativePushResult[]> => {
        throw new Error('secret-token');
      },
    };
    const sender = new FcmProjectSender({
      projects: [projects[0] ?? never(), { projectId: 'relay-b', sender: boom }],
    });
    const out = await sender.send([android('t0'), android('t1', 'relay-b')]);
    expect(out[0]).toEqual({ ok: true });
    expect(out[1]).toMatchObject({ ok: false, kind: 'retryable' });
    expect(JSON.stringify(out[1])).not.toContain('secret-token');
  });

  it('isolates the token buckets: one project at its limit does not make another wait', async () => {
    const { projects } = fakes();
    const time = fakeTime();
    const sender = new FcmProjectSender({
      projects,
      ratePerSecond: 100,
      now: time.now,
      sleep: time.sleep,
    });
    // Project A takes 100 (burst) + 200 (over): its bucket owes 2 seconds.
    await sender.send(Array.from({ length: 300 }, (_, i) => android(`a${i}`, 'relay-a')));
    expect(time.slept).toEqual([2000]);
    // Project B is untouched by A's debt: 100 messages go straight through, no sleep at all.
    // (The fake sleep advanced time by 2 s, so B's bucket is full, as it was before.)
    const before = time.slept.length;
    await sender.send(Array.from({ length: 100 }, (_, i) => android(`b${i}`, 'relay-b')));
    expect(time.slept.length).toBe(before);
  });

  it('with real timers, a saturated project does not delay the others', async () => {
    const { projects } = fakes();
    const sender = new FcmProjectSender({ projects, ratePerSecond: 1000 });
    const t0 = Date.now();
    const aDone = sender
      .send(Array.from({ length: 2000 }, (_, i) => android(`a${i}`, 'relay-a')))
      .then(() => Date.now() - t0);
    const bDone = sender
      .send(Array.from({ length: 100 }, (_, i) => android(`b${i}`, 'relay-b')))
      .then(() => Date.now() - t0);
    const [aMs, bMs] = await Promise.all([aDone, bDone]);
    expect(aMs).toBeGreaterThanOrEqual(900); // 1000 over the burst at 1000/s
    expect(bMs).toBeLessThan(400);
  });

  it('needs at least one project and refuses a duplicate id', () => {
    expect(() => new FcmProjectSender({ projects: [] })).toThrow(/at least one/);
    const { projects } = fakes();
    const first = projects[0] ?? never();
    expect(() => new FcmProjectSender({ projects: [first, first] })).toThrow(/duplicate/);
  });
});

function never(): never {
  throw new Error('unreachable');
}

describe('FcmProjectSender over real FcmSenders', () => {
  // One mock FCM per project: each issues its own OAuth tokens, as separate Firebase projects do.
  const mocks = new Map<string, MockFcm>();
  beforeEach(async () => {
    for (const id of [...IDS, 'proj']) {
      const m = new MockFcm();
      await m.start();
      mocks.set(id, m);
    }
  });
  afterEach(async () => {
    for (const m of mocks.values()) await m.stop();
    mocks.clear();
  });

  const fcm = (projectId: string) => {
    const mock = mocks.get(projectId) ?? never();
    return new FcmSender({
      serviceAccount: {
        client_email: `relay@${projectId}.iam.gserviceaccount.com`,
        private_key: keys.privatePem,
        project_id: projectId,
      },
      tokenUri: mock.tokenUri,
      baseUrl: mock.baseUrl,
    });
  };
  const tokensAt = (projectId: string) =>
    (mocks.get(projectId) ?? never()).records.map((r) => [r.body.message['token'], r.url]);

  it('posts each message to /v1/projects/<its project>/messages:send of that project only', async () => {
    const sender = new FcmProjectSender({
      projects: IDS.map((projectId) => ({ projectId, sender: fcm(projectId) })),
    });
    const out = await sender.send([
      android('tok-b', 'relay-b'),
      android('tok-default'),
      android('tok-c', 'relay-c'),
    ]);
    expect(out.every((r) => r.ok)).toBe(true);
    expect(tokensAt('relay-a')).toEqual([['tok-default', '/v1/projects/relay-a/messages:send']]);
    expect(tokensAt('relay-b')).toEqual([['tok-b', '/v1/projects/relay-b/messages:send']]);
    expect(tokensAt('relay-c')).toEqual([['tok-c', '/v1/projects/relay-c/messages:send']]);
  });

  it('backward compatible: a single FcmSender-backed project serves targets without a project', async () => {
    const sender = new FcmProjectSender({
      projects: [{ projectId: 'proj', sender: fcm('proj') }],
    });
    const [r] = await sender.send([android('tok-1')]);
    expect(r).toEqual({ ok: true });
    expect(tokensAt('proj')).toEqual([['tok-1', '/v1/projects/proj/messages:send']]);
  });
});
