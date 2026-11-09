/**
 * One poll of the NWS feed, and a loop that polls several times inside one Lambda invocation.
 * The normalizer, lifecycle (apply) and H3 matching are injected so this file stays small
 * and testable.
 */
import type { Alert } from '@relay/shared';
import type { RawArchive, SeenStore } from '../ports.js';
import { TooManyCellsError } from '../geo/index.js';
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
  /** 1 when the NWS fetch itself failed (HTTP error, timeout), else 0. */
  fetchFailed: number;
}

const emptyCounts = (): PollCounts => ({
  fetched: 0,
  accepted: 0,
  rejected: 0,
  skippedSeen: 0,
  applied: 0,
  errors: 0,
  fetchFailed: 0,
});

const defaultLog = (entry: Record<string, unknown>): void => console.log(JSON.stringify(entry));
const msg = (err: unknown): string => (err instanceof Error ? err.message : String(err));

export async function pollOnce(deps: PollDeps): Promise<PollCounts> {
  const counts = emptyCounts();
  await pollInto(deps, counts);
  // Keep ETag / Last-Modified only if everything in this poll worked. Otherwise the next
  // poll must get the full body again (not a 304) so the failed features are retried.
  if (counts.errors === 0) deps.client.commit();
  return counts;
}

async function pollInto(deps: PollDeps, counts: PollCounts): Promise<void> {
  const log = deps.log ?? defaultLog;

  const res = await deps.client.fetchActiveCalifornia();
  if (res.status === 'not_modified') {
    log({ event: 'poll.not_modified' });
    return;
  }
  if (res.status === 'error') {
    counts.errors += 1;
    counts.fetchFailed = 1;
    log({ event: 'poll.fetch_error', httpStatus: res.httpStatus, message: res.message });
    return;
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
    return;
  }
  counts.fetched = feed.features.length;

  for (const feature of feed.features) {
    // One bad feature must not stop the rest.
    let current: Alert | null = null;
    try {
      const norm = deps.normalize(feature);
      if (!norm.ok) {
        counts.rejected += 1;
        log({ event: 'poll.rejected', sourceId: norm.sourceId, reason: norm.reason });
        continue;
      }
      counts.accepted += 1;
      const alert = norm.alert;
      current = alert;
      if (await deps.seen.has(alert.sourceId)) {
        counts.skippedSeen += 1;
        continue;
      }
      alert.area.h3Cells = await deps.cellsFor(alert.area);
      const result = await deps.apply(alert);

      let toPlan: ApplyResult | null = null;
      if (result.action === 'ignored') {
        // Replay of this exact message: an earlier attempt saved it but a later step (delivery
        // planning, marking seen) failed. Plan again; planDelivery is idempotent. A message
        // that is merely older than the stored one (different sourceId) has nothing to plan.
        const stored = result.alert;
        if (
          stored.sourceId === alert.sourceId &&
          (stored.state === 'active' || stored.state === 'updated')
        ) {
          toPlan = { action: stored.state === 'active' ? 'created' : 'updated', alert: stored };
          log({ event: 'poll.replay_replan', sourceId: alert.sourceId });
        } else {
          log({ event: 'poll.ignored', sourceId: alert.sourceId, reason: result.reason });
        }
      } else {
        toPlan = result;
        log({ event: 'poll.applied', sourceId: alert.sourceId, action: result.action });
      }
      if (toPlan) {
        await deps.onApplied?.(toPlan);
        counts.applied += 1;
      }
      // Mark seen LAST: only after apply and delivery planning both succeeded. If anything
      // above threw, the next poll retries this feature.
      await deps.seen.mark(alert.sourceId, alert.sent);
    } catch (err) {
      if (err instanceof TooManyCellsError) {
        // Broken geometry never gets better: reject it for good instead of retrying forever.
        counts.rejected += 1;
        log({ event: 'poll.rejected_permanent', level: 'error', message: msg(err) });
        try {
          if (current) await deps.seen.mark(current.sourceId, current.sent);
        } catch (markErr) {
          counts.errors += 1;
          log({ event: 'poll.mark_failed', message: msg(markErr) });
        }
        continue;
      }
      counts.errors += 1;
      log({ event: 'poll.feature_error', message: msg(err) });
    }
  }
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
