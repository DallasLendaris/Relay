/**
 * One poll of the NWS feed, and a loop that polls several times inside one Lambda invocation.
 * The normalizer, lifecycle (apply) and H3 matching are injected so this file stays small
 * and testable.
 */
import type { Alert } from '@relay/shared';
import type { RawArchive, SeenStore } from '../ports.js';
import type { NwsClient } from './nws-client.js';

export type NormalizeResult =
  { ok: true; alert: Alert } | { ok: false; reason: string; sourceId: string | null };
export type ApplyResult = {
  action: 'created' | 'updated' | 'cancelled' | 'ignored';
  alert: Alert;
  reason?: string;
};

export interface PollDeps {
  client: NwsClient;
  archive: RawArchive;
  seen: SeenStore;
  normalize(feature: unknown): NormalizeResult;
  parseFeed(body: string): { features: unknown[] } | { error: string };
  apply(alert: Alert): Promise<ApplyResult>;
  cellsFor(area: Alert['area']): Promise<string[]>;
  /** Delivery plugs in here. Called for created, updated and cancelled alerts only. */
  onApplied?(result: ApplyResult): Promise<void> | void;
  log?(entry: Record<string, unknown>): void;
}

export interface PollCounts {
  fetched: number;
  accepted: number;
  rejected: number;
  skippedSeen: number;
  applied: number;
  errors: number;
}

const emptyCounts = (): PollCounts => ({
  fetched: 0,
  accepted: 0,
  rejected: 0,
  skippedSeen: 0,
  applied: 0,
  errors: 0,
});

const defaultLog = (entry: Record<string, unknown>): void => console.log(JSON.stringify(entry));
const msg = (err: unknown): string => (err instanceof Error ? err.message : String(err));

export async function pollOnce(deps: PollDeps): Promise<PollCounts> {
  const log = deps.log ?? defaultLog;
  const counts = emptyCounts();

  const res = await deps.client.fetchActiveCalifornia();
  if (res.status === 'not_modified') {
    log({ event: 'poll.not_modified' });
    return counts;
  }
  if (res.status === 'error') {
    counts.errors += 1;
    log({ event: 'poll.fetch_error', httpStatus: res.httpStatus, message: res.message });
    return counts;
  }

  // Archive first, but never let an archive failure block alerts.
  try {
    const key = await deps.archive.put(res.body, res.fetchedAt);
    log({ event: 'poll.archived', key });
  } catch (err) {
    counts.errors += 1;
    log({ event: 'poll.archive_failed', message: msg(err) });
  }

  const feed = deps.parseFeed(res.body);
  if ('error' in feed) {
    counts.errors += 1;
    log({ event: 'poll.parse_failed', message: feed.error });
    return counts;
  }
  counts.fetched = feed.features.length;

  for (const feature of feed.features) {
    // One bad feature must not stop the rest.
    try {
      const norm = deps.normalize(feature);
      if (!norm.ok) {
        counts.rejected += 1;
        log({ event: 'poll.rejected', sourceId: norm.sourceId, reason: norm.reason });
        continue;
      }
      counts.accepted += 1;
      const alert = norm.alert;
      if (await deps.seen.has(alert.sourceId)) {
        counts.skippedSeen += 1;
        continue;
      }
      alert.area.h3Cells = await deps.cellsFor(alert.area);
      const result = await deps.apply(alert);
      // Mark seen only after apply succeeded, so a failure is retried on the next poll.
      await deps.seen.mark(alert.sourceId, alert.sent);
      if (result.action === 'ignored') {
        log({ event: 'poll.ignored', sourceId: alert.sourceId, reason: result.reason });
        continue;
      }
      counts.applied += 1;
      log({ event: 'poll.applied', sourceId: alert.sourceId, action: result.action });
      await deps.onApplied?.(result);
    } catch (err) {
      counts.errors += 1;
      log({ event: 'poll.feature_error', message: msg(err) });
    }
  }
  return counts;
}

export interface LoopOptions {
  polls: number;
  intervalMs: number;
  /** Injectable for tests. Defaults to a real timer. */
  sleep?(ms: number): Promise<void>;
  /** Milliseconds left in the Lambda invocation. */
  remainingMs(): number;
}

const SAFETY_MARGIN_MS = 10_000;

export async function pollLoop(deps: PollDeps, opts: LoopOptions): Promise<PollCounts[]> {
  const log = deps.log ?? defaultLog;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const results: PollCounts[] = [];
  for (let i = 0; i < opts.polls; i++) {
    if (i > 0) {
      // Stop if there is not enough time left for another wait plus a poll.
      if (opts.remainingMs() < opts.intervalMs + SAFETY_MARGIN_MS) {
        log({ event: 'poll.loop_stopped_low_time', remainingMs: opts.remainingMs(), polls: i });
        break;
      }
      await sleep(opts.intervalMs);
    }
    results.push(await pollOnce(deps));
  }
  return results;
}
