/**
 * One poll of the NWS feed, and a loop that polls several times inside one Lambda invocation.
 * The normalizer, lifecycle (apply) and H3 matching are injected so this file stays small
 * and testable.
 */
import { relaySeverityFor, type Alert } from '@relay/shared';
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
  /**
   * Milliseconds left in the Lambda invocation. When set, pollOnce stops before a feature if
   * less than MIN_FEATURE_BUDGET_MS remain. pollLoop fills it in from its own options.
   */
  remainingMs?(): number;
}

/**
 * Thrown by onApplied when delivery is paused (kill switch). The alert is already saved, but it
 * must stay unseen so every poll plans it again until delivery is unpaused. Not an error.
 */
export class DeliveryPausedError extends Error {
  constructor(alertId: string) {
    super(`delivery paused; alert ${alertId} stays unseen until unpaused`);
    this.name = 'DeliveryPausedError';
  }
}

/** Do not start another feature with less Lambda time than this left (planning can be slow). */
export const MIN_FEATURE_BUDGET_MS = 15_000;
/** Consecutive polls with the same save failure for one sourceId before we give up on it. */
export const QUARANTINE_AFTER = 5;

/**
 * Save-failure bookkeeping, kept in memory per warm Lambda container. A cold start resets
 * it, which is acceptable: we just need 5 failures in a row, and an alert that keeps
 * failing will fail again and be quarantined a few polls later.
 */
const saveFailures = new Map<string, { message: string; count: number }>();

/** Clears the failure counts (tests). */
export function clearSaveFailures(): void {
  saveFailures.clear();
}

/** Network and throttling errors get better on their own, so they never count toward quarantine. */
function isTransient(err: unknown): boolean {
  const text = `${err instanceof Error ? err.name : ''} ${msg(err)}`;
  return /time(d )?out|abort|throttl|unavailable|ECONN|ETIMEDOUT|EAI_AGAIN|network|rate exceeded/i.test(
    text,
  );
}

const SEVERITY_RANK = { emergency: 0, warning: 1, watch: 2, advisory: 3, statement: 4 } as const;

export interface PollCounts {
  fetched: number;
  accepted: number;
  rejected: number;
  skippedSeen: number;
  applied: number;
  errors: number;
  /** Alerts given up on after repeated identical save failures (marked seen). */
  quarantined: number;
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
  quarantined: 0,
  fetchFailed: 0,
});

const defaultLog = (entry: Record<string, unknown>): void => console.log(JSON.stringify(entry));
const msg = (err: unknown): string => (err instanceof Error ? err.message : String(err));

export async function pollOnce(deps: PollDeps): Promise<PollCounts> {
  const counts = emptyCounts();
  const complete = await pollInto(deps, counts);
  // Keep ETag / Last-Modified only if everything in this poll worked and every feature was
  // reached. Otherwise the next poll must get the full body again (not a 304) so the failed,
  // paused or not-yet-processed features are retried.
  if (counts.errors === 0 && complete) deps.client.commit();
  return counts;
}

/** Returns false when some feature was left unseen on purpose (deadline or delivery paused). */
async function pollInto(deps: PollDeps, counts: PollCounts): Promise<boolean> {
  const log = deps.log ?? defaultLog;

  const res = await deps.client.fetchActiveCalifornia();
  if (res.status === 'not_modified') {
    log({ event: 'poll.not_modified' });
    return true;
  }
  if (res.status === 'error') {
    counts.errors += 1;
    counts.fetchFailed = 1;
    log({ event: 'poll.fetch_error', httpStatus: res.httpStatus, message: res.message });
    return true;
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
    return true;
  }
  counts.fetched = feed.features.length;

  // Normalize everything first so the most urgent alerts are handled first: a slow low-priority
  // alert must not starve an emergency. Oldest first within a severity keeps updates in order.
  const queue: Alert[] = [];
  for (const feature of feed.features) {
    try {
      const norm = deps.normalize(feature);
      if (!norm.ok) {
        counts.rejected += 1;
        log({ event: 'poll.rejected', sourceId: norm.sourceId, reason: norm.reason });
        continue;
      }
      counts.accepted += 1;
      queue.push(norm.alert);
    } catch (err) {
      counts.errors += 1;
      log({ event: 'poll.feature_error', message: msg(err) });
    }
  }
  const rank = (a: Alert): number => SEVERITY_RANK[relaySeverityFor(a.event)];
  queue.sort((a, b) => rank(a) - rank(b) || Date.parse(a.sent) - Date.parse(b.sent));

  let complete = true;
  for (const [i, alert] of queue.entries()) {
    if (deps.remainingMs && deps.remainingMs() < MIN_FEATURE_BUDGET_MS) {
      // Leave the rest unseen; the next poll (no ETag commit) picks them up.
      complete = false;
      log({
        event: 'poll.deadline_reached',
        level: 'warn',
        remainingMs: deps.remainingMs(),
        deferred: queue.length - i,
      });
      break;
    }
    // One bad feature must not stop the rest.
    let stage: 'prepare' | 'save' | 'deliver' = 'prepare';
    try {
      if (await deps.seen.has(alert.sourceId)) {
        counts.skippedSeen += 1;
        continue;
      }
      alert.area.h3Cells = await deps.cellsFor(alert.area);
      stage = 'save';
      const result = await deps.apply(alert);
      stage = 'deliver';

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
      saveFailures.delete(alert.sourceId);
    } catch (err) {
      if (err instanceof DeliveryPausedError) {
        // Expected while the kill switch is on. Not an error: leave unseen, keep the ETag.
        complete = false;
        log({ event: 'poll.delivery_paused', level: 'warn', sourceId: alert.sourceId });
        continue;
      }
      if (err instanceof TooManyCellsError) {
        // Broken geometry never gets better: reject it for good instead of retrying forever.
        counts.rejected += 1;
        log({ event: 'poll.rejected_permanent', level: 'error', message: msg(err) });
        await markQuietly(deps, alert, counts, log);
        continue;
      }
      if (stage === 'save' && !isTransient(err)) {
        // Same non-transient error for the same alert, poll after poll: it will never save.
        const prev = saveFailures.get(alert.sourceId);
        const count = prev && prev.message === msg(err) ? prev.count + 1 : 1;
        saveFailures.set(alert.sourceId, { message: msg(err), count });
        if (count >= QUARANTINE_AFTER) {
          saveFailures.delete(alert.sourceId);
          counts.quarantined += 1;
          log({
            event: 'ingest.quarantined',
            level: 'error',
            sourceId: alert.sourceId,
            failures: count,
            message: msg(err),
          });
          await markQuietly(deps, alert, counts, log);
          continue;
        }
      }
      counts.errors += 1;
      log({ event: 'poll.feature_error', message: msg(err) });
    }
  }
  return complete;
}

async function markQuietly(
  deps: PollDeps,
  alert: Alert,
  counts: PollCounts,
  log: (entry: Record<string, unknown>) => void,
): Promise<void> {
  try {
    await deps.seen.mark(alert.sourceId, alert.sent);
  } catch (markErr) {
    counts.errors += 1;
    log({ event: 'poll.mark_failed', message: msg(markErr) });
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
    results.push(await pollOnce({ ...deps, remainingMs: deps.remainingMs ?? opts.remainingMs }));
  }
  return results;
}
