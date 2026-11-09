import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import type { Alert } from '@relay/shared';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import {
  createNwsClient,
  type NwsClient,
  type NwsFetchResult,
} from '../../src/ingest/nws-client.js';
import {
  pollLoop,
  pollOnce,
  type ApplyResult,
  type NormalizeResult,
  type PollDeps,
} from '../../src/ingest/poll.js';
import { NoCellsError, TooManyCellsError } from '../../src/geo/index.js';
import type { SeenStore } from '../../src/ports.js';

const fixture = (name: string): string =>
  readFileSync(new URL(`../fixtures/nws/${name}.json`, import.meta.url), 'utf8');

// ---- fakes ----
type Feature = { properties?: { id?: string; status?: string; sent?: string } };

/** Fake normalizer: rejects non-Actual features, otherwise builds a minimal alert. */
function fakeNormalize(feature: unknown): NormalizeResult {
  const p = (feature as Feature).properties ?? {};
  const sourceId = (p.id ?? '').replace('https://api.weather.gov/alerts/', '') || null;
  if (!sourceId || p.status !== 'Actual') {
    return { ok: false, reason: 'not actual', sourceId };
  }
  const alert = {
    sourceId,
    sent: p.sent ?? '2026-11-09T00:00:00Z',
    area: { description: '', geometry: null, ugcCodes: [], sameCodes: [], h3Cells: [] },
  } as unknown as Alert;
  return { ok: true, alert };
}

class FakeSeen implements SeenStore {
  ids = new Set<string>();
  async has(id: string) {
    return this.ids.has(id);
  }
  async mark(id: string) {
    this.ids.add(id);
  }
}

function clientReturning(...results: NwsFetchResult[]): NwsClient & { commits: number } {
  let i = 0;
  return {
    commits: 0,
    commit() {
      this.commits += 1;
    },
    fetchActiveCalifornia: async () =>
      results[Math.min(i++, results.length - 1)] ?? { status: 'error', message: 'none' },
  };
}

const ok = (name: string): NwsFetchResult => ({
  status: 'ok',
  body: fixture(name),
  fetchedAt: new Date('2026-11-09T10:00:00Z'),
});

function makeDeps(client: NwsClient, over: Partial<PollDeps> = {}) {
  const archive = new MemoryRawArchive();
  const seen = new FakeSeen();
  const applied: ApplyResult[] = [];
  const deps: PollDeps = {
    client,
    archive,
    seen,
    normalize: fakeNormalize,
    parseFeed: (body) => {
      try {
        return { features: (JSON.parse(body) as { features: unknown[] }).features };
      } catch {
        return { error: 'bad json' };
      }
    },
    cellsFor: async () => ['cell-1'],
    apply: async (alert) => ({ action: 'created', alert }),
    onApplied: (r) => {
      applied.push(r);
    },
    log: () => {},
    ...over,
  };
  return { deps, archive, seen, applied };
}

describe('pollOnce', () => {
  it('counts a mixed feed, archives it, sets cells and calls the hook', async () => {
    const { deps, archive, seen, applied } = makeDeps(clientReturning(ok('active-ca-mixed')));
    const counts = await pollOnce(deps);
    expect(counts).toEqual({
      fetched: 4,
      accepted: 4,
      rejected: 0,
      skippedSeen: 0,
      applied: 4,
      errors: 0,
      fetchFailed: 0,
    });
    expect(archive.items).toHaveLength(1);
    expect(seen.ids.size).toBe(4);
    expect(applied).toHaveLength(4);
    expect(applied[0]?.alert.area.h3Cells).toEqual(['cell-1']);
  });

  it('counts rejections from non-Actual alerts', async () => {
    const { deps } = makeDeps(clientReturning(ok('non-actual')));
    const counts = await pollOnce(deps);
    expect(counts.fetched).toBe(3);
    expect(counts.rejected).toBe(3);
    expect(counts.applied).toBe(0);
  });

  it('treats an empty feed as success', async () => {
    const { deps } = makeDeps(clientReturning(ok('empty')));
    const counts = await pollOnce(deps);
    expect(counts).toEqual({
      fetched: 0,
      accepted: 0,
      rejected: 0,
      skippedSeen: 0,
      applied: 0,
      errors: 0,
      fetchFailed: 0,
    });
  });

  it('a rejected feature does not stop the others', async () => {
    let n = 0;
    const { deps } = makeDeps(clientReturning(ok('active-ca-mixed')), {
      normalize: (f) =>
        n++ === 1 ? { ok: false, reason: 'bad', sourceId: null } : fakeNormalize(f),
    });
    const counts = await pollOnce(deps);
    expect(counts.rejected).toBe(1);
    expect(counts.applied).toBe(3);
  });

  it('a throwing feature is counted as an error and the rest continue', async () => {
    let n = 0;
    const { deps, seen } = makeDeps(clientReturning(ok('active-ca-mixed')), {
      apply: async (alert) => {
        if (n++ === 0) throw new Error('db down');
        return { action: 'created', alert };
      },
    });
    const counts = await pollOnce(deps);
    expect(counts.errors).toBe(1);
    expect(counts.applied).toBe(3);
    // The failed one is not marked seen, so it is retried next poll.
    expect(seen.ids.size).toBe(3);
  });

  it('skips items already seen and does not apply them', async () => {
    const client = clientReturning(ok('active-ca-mixed'), ok('active-ca-mixed'));
    const { deps } = makeDeps(client);
    await pollOnce(deps);
    const apply = vi.fn(deps.apply);
    const second = await pollOnce({ ...deps, apply });
    expect(second.skippedSeen).toBe(4);
    expect(second.applied).toBe(0);
    expect(apply).not.toHaveBeenCalled();
  });

  it('does not call the hook for ignored results', async () => {
    const { deps, applied } = makeDeps(clientReturning(ok('active-ca-mixed')), {
      apply: async (alert) => ({ action: 'ignored', alert, reason: 'older' }),
    });
    const counts = await pollOnce(deps);
    expect(counts.applied).toBe(0);
    expect(applied).toHaveLength(0);
  });

  it('does nothing on 304', async () => {
    const { deps, archive } = makeDeps(clientReturning({ status: 'not_modified' }));
    const counts = await pollOnce(deps);
    expect(counts.fetched).toBe(0);
    expect(counts.errors).toBe(0);
    expect(archive.items).toHaveLength(0);
  });

  it('returns cleanly on a fetch error', async () => {
    const { deps } = makeDeps(clientReturning({ status: 'error', httpStatus: 503, message: 'x' }));
    const counts = await pollOnce(deps);
    expect(counts.errors).toBe(1);
    expect(counts.fetchFailed).toBe(1);
    expect(counts.applied).toBe(0);
  });

  it('keeps processing alerts when archiving fails', async () => {
    const { deps } = makeDeps(clientReturning(ok('active-ca-mixed')), {
      archive: {
        put: async () => {
          throw new Error('s3 down');
        },
      },
    });
    const counts = await pollOnce(deps);
    expect(counts.applied).toBe(4);
    expect(counts.errors).toBe(1);
  });

  it('reports a parse failure without throwing', async () => {
    const { deps } = makeDeps(
      clientReturning({ status: 'ok', body: 'nope', fetchedAt: new Date() }),
    );
    const counts = await pollOnce(deps);
    expect(counts.errors).toBe(1);
  });
});

describe('pollOnce reliability', () => {
  /** One Actual feature, so each test follows a single alert. */
  const one: NwsFetchResult = {
    status: 'ok',
    body: JSON.stringify({
      features: [
        { properties: { id: 'urn:oid:1', status: 'Actual', sent: '2026-11-09T00:00:00Z' } },
      ],
    }),
    fetchedAt: new Date('2026-11-09T10:00:00Z'),
  };

  /** onApplied that fails the first `failures` times, recording every call. */
  const flakyHook = (failures: number) => {
    const calls: ApplyResult[] = [];
    return {
      calls,
      onApplied: async (r: ApplyResult) => {
        calls.push(r);
        if (calls.length <= failures) throw new Error('sqs down');
      },
    };
  };

  /** Fake lifecycle: the first call saves, later calls report a replay of the stored copy. */
  const replayingApply = () => {
    const stored = new Map<string, Alert>();
    return async (alert: Alert): Promise<ApplyResult> => {
      const known = stored.get(alert.sourceId);
      if (known) return { action: 'ignored', alert: known, reason: 'already applied (replay)' };
      const saved = { ...alert, state: 'active' } as Alert;
      stored.set(alert.sourceId, saved);
      return { action: 'created', alert: saved };
    };
  };

  it('onApplied fails on poll 1: not marked seen; poll 2 re-runs it and only then marks seen', async () => {
    const hook = flakyHook(1);
    const { deps, seen } = makeDeps(clientReturning(one), hook);
    const first = await pollOnce(deps);
    expect(first.errors).toBe(1);
    expect(seen.ids.size).toBe(0);

    const second = await pollOnce(deps);
    expect(second.errors).toBe(0);
    expect(hook.calls).toHaveLength(2);
    expect(seen.ids.size).toBe(1);

    const third = await pollOnce(deps);
    expect(third.skippedSeen).toBe(1);
    expect(hook.calls).toHaveLength(2);
  });

  it('alert saved but not planned: the retry sees "ignored" yet onApplied still runs', async () => {
    const hook = flakyHook(1);
    const { deps, seen } = makeDeps(clientReturning(one), { ...hook, apply: replayingApply() });
    await pollOnce(deps);
    expect(seen.ids.size).toBe(0);
    const second = await pollOnce(deps);
    expect(second.errors).toBe(0);
    expect(hook.calls).toHaveLength(2);
    expect(hook.calls[1]?.alert.state).toBe('active');
    expect(seen.ids.size).toBe(1);
  });

  it('does not re-plan when the ignored result is a different (older) stored message', async () => {
    const { deps, seen, applied } = makeDeps(clientReturning(one), {
      apply: async (alert) => ({
        action: 'ignored',
        alert: { ...alert, sourceId: 'urn:other', state: 'active' } as Alert,
        reason: 'message is older than stored data',
      }),
    });
    await pollOnce(deps);
    expect(applied).toHaveLength(0);
    expect(seen.ids.size).toBe(1);
  });

  it('seen.mark throws after a successful apply: next poll calls onApplied once via the replay path', async () => {
    const calls: ApplyResult[] = [];
    const { deps, seen } = makeDeps(clientReturning(one), {
      apply: replayingApply(),
      onApplied: (r) => {
        calls.push(r);
      },
    });
    const realMark = seen.mark.bind(seen);
    let failMark = true;
    seen.mark = async (id: string) => {
      if (failMark) throw new Error('dynamo down');
      await realMark(id);
    };
    const first = await pollOnce(deps);
    expect(first.errors).toBe(1);
    expect(calls).toHaveLength(1);

    failMark = false;
    const second = await pollOnce(deps);
    expect(second.errors).toBe(0);
    expect(calls).toHaveLength(2); // the replay path planned again
    expect(seen.ids.size).toBe(1);
    await pollOnce(deps);
    expect(calls).toHaveLength(2); // and now it is seen: no more planning
  });

  it('commits the client validators only when the poll had no errors', async () => {
    const client = clientReturning(one);
    const { deps } = makeDeps(client, flakyHook(1));
    await pollOnce(deps);
    expect(client.commits).toBe(0);
    await pollOnce(deps);
    expect(client.commits).toBe(1);

    const failing = clientReturning({ status: 'error', message: 'x' });
    await pollOnce(makeDeps(failing).deps);
    expect(failing.commits).toBe(0);
  });

  it('a TooManyCellsError is a permanent rejection: error log, counted rejected, marked seen', async () => {
    const entries: Record<string, unknown>[] = [];
    const { deps, seen } = makeDeps(clientReturning(one), {
      cellsFor: async () => {
        throw new TooManyCellsError();
      },
      log: (e) => entries.push(e),
    });
    const counts = await pollOnce(deps);
    expect(counts.rejected).toBe(1);
    expect(counts.errors).toBe(0);
    expect(seen.ids.size).toBe(1);
    expect(entries.find((e) => e.event === 'poll.rejected_permanent')).toMatchObject({
      level: 'error',
    });
  });

  it('a zero-cell area (NoCellsError) is retried, not marked seen', async () => {
    const { deps, seen } = makeDeps(clientReturning(one), {
      cellsFor: async () => {
        throw new NoCellsError(['CAZ404']);
      },
    });
    const counts = await pollOnce(deps);
    expect(counts.errors).toBe(1);
    expect(seen.ids.size).toBe(0);
  });
});

describe('pollLoop', () => {
  it('polls the requested number of times, sleeping between polls', async () => {
    const { deps } = makeDeps(clientReturning({ status: 'not_modified' }));
    const sleep = vi.fn(async () => {});
    const results = await pollLoop(deps, {
      polls: 5,
      intervalMs: 12_000,
      sleep,
      remainingMs: () => 60_000,
    });
    expect(results).toHaveLength(5);
    expect(sleep).toHaveBeenCalledTimes(4);
    expect(sleep).toHaveBeenCalledWith(12_000);
  });

  it('stops early when remaining time is under interval + 10 s', async () => {
    const { deps } = makeDeps(clientReturning({ status: 'not_modified' }));
    const remaining = [60_000, 40_000, 21_999];
    let i = 0;
    const results = await pollLoop(deps, {
      polls: 5,
      intervalMs: 12_000,
      sleep: async () => {},
      remainingMs: () => remaining[Math.min(i++, remaining.length - 1)] ?? 0,
    });
    // Poll 1 runs, poll 2 passes the check (60k), poll 3 passes (40k), poll 4 fails (21.9k).
    expect(results).toHaveLength(3);
  });
});

describe('createNwsClient', () => {
  it('sends the right headers, then If-None-Match / If-Modified-Since on the second call', async () => {
    const calls: Array<{ url: string; headers: Record<string, string> }> = [];
    const fakeFetch = (async (url: string, init: RequestInit) => {
      const headers = init.headers as Record<string, string>;
      calls.push({ url, headers });
      if (headers['If-None-Match']) return new Response(null, { status: 304 });
      return new Response('{"features":[]}', {
        status: 200,
        headers: { ETag: '"abc"', 'Last-Modified': 'Mon, 09 Nov 2026 10:00:00 GMT' },
      });
    }) as unknown as typeof fetch;

    const client = createNwsClient({ fetch: fakeFetch, userAgent: 'Relay test' });
    const first = await client.fetchActiveCalifornia();
    client.commit();
    const second = await client.fetchActiveCalifornia();

    expect(first.status).toBe('ok');
    expect(second).toEqual({ status: 'not_modified' });
    expect(calls[0]?.url).toBe('https://api.weather.gov/alerts/active?area=CA');
    expect(calls[0]?.headers['User-Agent']).toBe('Relay test');
    expect(calls[0]?.headers['Accept']).toBe('application/geo+json');
    expect(calls[0]?.headers['If-None-Match']).toBeUndefined();
    expect(calls[1]?.headers['If-None-Match']).toBe('"abc"');
    expect(calls[1]?.headers['If-Modified-Since']).toBe('Mon, 09 Nov 2026 10:00:00 GMT');
  });

  it('keeps the ETag only after commit(): a failed poll gets the full body again', async () => {
    const sent: Array<string | undefined> = [];
    const fakeFetch = (async (_url: string, init: RequestInit) => {
      const h = init.headers as Record<string, string>;
      sent.push(h['If-None-Match']);
      return new Response('{"features":[]}', { status: 200, headers: { ETag: '"v1"' } });
    }) as unknown as typeof fetch;
    const client = createNwsClient({ fetch: fakeFetch, userAgent: 'u' });
    await client.fetchActiveCalifornia(); // processing "fails": no commit
    await client.fetchActiveCalifornia();
    client.commit(); // this one worked
    await client.fetchActiveCalifornia();
    expect(sent).toEqual([undefined, undefined, '"v1"']);
  });

  it('returns an error result for HTTP errors and thrown errors', async () => {
    const http = createNwsClient({
      fetch: (async () => new Response('x', { status: 500 })) as unknown as typeof fetch,
      userAgent: 'u',
    });
    expect(await http.fetchActiveCalifornia()).toMatchObject({ status: 'error', httpStatus: 500 });

    const thrown = createNwsClient({
      fetch: (async () => {
        throw new Error('boom');
      }) as unknown as typeof fetch,
      userAgent: 'u',
    });
    expect(await thrown.fetchActiveCalifornia()).toEqual({ status: 'error', message: 'boom' });
  });

  it('times out after 8 s', async () => {
    vi.useFakeTimers();
    try {
      const hanging = ((_url: string, init: RequestInit) =>
        new Promise((_resolve, reject) => {
          init.signal?.addEventListener('abort', () => reject(new Error('aborted')));
        })) as unknown as typeof fetch;
      const client = createNwsClient({ fetch: hanging, userAgent: 'u' });
      const pending = client.fetchActiveCalifornia();
      await vi.advanceTimersByTimeAsync(8000);
      expect(await pending).toMatchObject({
        status: 'error',
        message: expect.stringContaining('timed out'),
      });
    } finally {
      vi.useRealTimers();
    }
  });
});
