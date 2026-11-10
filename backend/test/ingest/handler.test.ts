import { afterEach, describe, expect, it, vi } from 'vitest';
import { MemoryRawArchive } from '../../src/adapters/memory/raw-archive.js';
import { InMemorySeenStore } from '../../src/adapters/memory/seen-store.js';
import { buildEmf, buildHandler, positiveNumber } from '../../src/ingest/handler.js';
import type { EngineDeps } from '../../src/ingest/handler.js';
import type { NwsClient } from '../../src/ingest/nws-client.js';
import type { PollCounts } from '../../src/ingest/poll.js';

const counts = (over: Partial<PollCounts>): PollCounts => ({
  fetched: 0,
  accepted: 0,
  rejected: 0,
  skippedSeen: 0,
  applied: 0,
  errors: 0,
  quarantined: 0,
  fetchFailed: 0,
  unknownZones: 0,
  publishFailed: 0,
  throttled: 0,
  ...over,
});

afterEach(() => vi.restoreAllMocks());

describe('buildEmf', () => {
  it('has the CloudWatch EMF shape and sums the polls', () => {
    const emf = buildEmf(
      [
        counts({ accepted: 2, applied: 1, errors: 1 }),
        counts({ accepted: 1, rejected: 3, errors: 2, fetchFailed: 1, quarantined: 2 }),
      ],
      new Date('2026-11-09T10:00:00Z'),
    );
    expect(emf).toEqual({
      _aws: {
        Timestamp: Date.parse('2026-11-09T10:00:00Z'),
        CloudWatchMetrics: [
          {
            Namespace: 'Relay/Ingest',
            Dimensions: [[]],
            Metrics: [
              { Name: 'Accepted', Unit: 'Count' },
              { Name: 'Rejected', Unit: 'Count' },
              { Name: 'Applied', Unit: 'Count' },
              { Name: 'Errors', Unit: 'Count' },
              { Name: 'FetchFailed', Unit: 'Count' },
              { Name: 'Quarantined', Unit: 'Count' },
              { Name: 'UnknownZones', Unit: 'Count' },
              { Name: 'PublishFailed', Unit: 'Count' },
              { Name: 'PollsOk', Unit: 'Count' },
            ],
          },
        ],
      },
      Accepted: 3,
      Rejected: 3,
      Applied: 1,
      Errors: 3,
      FetchFailed: 1,
      Quarantined: 2,
      UnknownZones: 0,
      PublishFailed: 0,
      PollsOk: 1,
    });
  });

  it('PollsOk counts polls that got an answer: a 304 or 200 yes, a fetch error or a throttled skip no', () => {
    const ok = (polls: PollCounts[]) => buildEmf(polls)['PollsOk'];
    expect(ok([counts({}), counts({ fetched: 4 })])).toBe(2);
    expect(ok([counts({ fetchFailed: 1, errors: 1 }), counts({ throttled: 1 })])).toBe(0);
    expect(ok([counts({ fetchFailed: 1 }), counts({})])).toBe(1);
    expect(ok([])).toBe(0);
  });

  it('still reports zeros when there were no polls', () => {
    expect(buildEmf([])).toMatchObject({ Accepted: 0, Errors: 0, FetchFailed: 0 });
  });
});

describe('positiveNumber', () => {
  it('uses valid values and falls back with a warning for empty or invalid ones', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    expect(positiveNumber(undefined, 5, 'X', true)).toBe(5);
    expect(warn).not.toHaveBeenCalled();
    expect(positiveNumber('3', 5, 'X', true)).toBe(3);
    for (const bad of ['', '  ', 'abc', '0', '-2', '2.5']) {
      expect(positiveNumber(bad, 5, 'X', true)).toBe(5);
    }
    expect(warn).toHaveBeenCalledTimes(6);
    expect(positiveNumber('2.5', 12, 'X', false)).toBe(2.5);
  });
});

describe('buildHandler', () => {
  const engine: EngineDeps = {
    normalize: () => ({ ok: false, reason: 'unused', sourceId: null }),
    parseFeed: () => ({ features: [] }),
    apply: async (alert) => ({ action: 'created', alert }),
    cellsFor: async () => [],
  };

  it('survives a total NWS outage, emits EMF with FetchFailed, and ignores an empty POLLS_PER_INVOCATION', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const lines: string[] = [];
    vi.spyOn(console, 'log').mockImplementation((l: unknown) => void lines.push(String(l)));
    const client: NwsClient = {
      fetchActiveCalifornia: async () => ({ status: 'error', httpStatus: 503, message: 'down' }),
      commit() {},
    };
    const handler = buildHandler(engine, {
      env: { POLLS_PER_INVOCATION: '', POLL_INTERVAL_SECONDS: '12' },
      base: { client, archive: new MemoryRawArchive(), seen: new InMemorySeenStore() },
      sleep: async () => {},
    });
    const out = await handler({}, { getRemainingTimeInMillis: () => 70_000 });

    expect(out.polls).toHaveLength(2); // the default, not 0
    expect(warn).toHaveBeenCalled();
    const emf = lines.map((l) => JSON.parse(l) as Record<string, unknown>).find((l) => '_aws' in l);
    expect(emf).toMatchObject({ Errors: 2, FetchFailed: 2, Accepted: 0 });
  });
});
