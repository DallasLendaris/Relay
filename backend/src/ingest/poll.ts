/**
 * One poll of the NWS feed, and a loop that polls several times inside one Lambda invocation.
 * The normalizer, lifecycle (apply) and H3 matching are injected so this file stays small
 * and testable.
 */
import type { Alert } from '@relay/shared';
import type { RawArchive, SeenStore } from '../ports.js';
import { InvalidGeometryError, TooManyCellsError } from '../geo/index.js';
import type { NwsClient } from './nws-client.js';

export type NormalizeResult =
  | { ok: true; alert: Alert }
  | { ok: false; reason: string; sourceId: string | null; permanent?: true };
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
  /** `ctx.onUnknownZones` is told which UGC zones could not be resolved (REL-7). */
  cellsFor(
    area: Alert['area'],
    ctx?: { onUnknownZones?(ugcs: string[], total: number): void },
  ): Promise<string[]>;
  /**
   * Background work started by onApplied (the public CDN publish) that must finish before the
   * invocation ends. Called once at the end of every pollOnce; reports how many publishes failed.
   * A publish failure never marks an alert unseen.
   */
  flush?(): Promise<{ publishFailed: number }>;
  /** Features of one response processed at once (default FEATURE_CONCURRENCY). */
  concurrency?: number;
  /** Delivery plugs in here. Called for created, updated and cancelled alerts only. */
  onApplied?(result: ApplyResult): Promise<void> | void;
  log?(entry: Record<string, unknown>): void;
  /**
   * Milliseconds left in the Lambda invocation. When set, pollOnce stops before a feature if
   * less than MIN_FEATURE_BUDGET_MS remain. pollLoop fills it in from its own options.
   */
  remainingMs?(): number;
  /**
   * Feed reconciliation (SAFE-12). NWS may simply stop listing an alert in /alerts/active when it
   * ends or is cancelled early, without sending a Cancel message. When set, every successful 200
   * poll compares the stored active NWS alerts with the feed (see reconcileFeed).
   */
  reconcile?: Reconciler;
}

/** What reconcileFeed needs from the engine: read the stored alerts, and end one. */
export interface Reconciler {
  now(): Date;
  /** Stored NWS alerts with state active or updated whose end is still in the future. */
  listActive(now: Date): Promise<Alert[]>;
  /** Mark the alert cancelled (state "cancelled"), save it through the signing store, republish. */
  cancel(alert: Alert, reason: 'no_longer_in_feed' | 'expired_reference'): Promise<void>;
  /** Stored NWS alerts that are cancelled but whose end is still in the future (restoration). */
  listEnded?(now: Date): Promise<Alert[]>;
  /**
   * Put a cancelled alert back to `state`: re-sign, save, republish to the CDN index. Returns the
   * alert as stored.
   */
  restore?(alert: Alert, state: 'active' | 'updated'): Promise<Alert>;
  /** Overrides RECONCILE_MIN_ABSENT_MS (tests). */
  minAbsentMs?: number;
}

/** A store that can list its active alerts by source (used to build a Reconciler). */
export interface ActiveAlertLister {
  listActiveBySource(source: Alert['source'], now: Date): Promise<Alert[]>;
  listCancelledBySource(source: Alert['source'], now: Date): Promise<Alert[]>;
}

/**
 * An alert must be missing from this many consecutive successful full (200) reads AND for at
 * least RECONCILE_MIN_ABSENT_MS before it is ended; whichever is later. NWS edge caches can flap,
 * so a live warning may vanish for a poll or two and come back.
 */
export const RECONCILE_AFTER = 3;
export const RECONCILE_MIN_ABSENT_MS = 5 * 60_000;

/**
 * Absence bookkeeping per Relay alert id, kept in memory for the warm container: how many
 * consecutive full reads missed it, and since when. A cold start forgets it, which only delays
 * ending an alert (the safe direction).
 */
const absentPolls = new Map<string, { misses: number; since: number }>();

/** Clears the reconciliation counters (tests). */
export function clearReconcileState(): void {
  absentPolls.clear();
}

const ALERT_URL_PREFIX = 'https://api.weather.gov/alerts/';

/** "https://api.weather.gov/alerts/<id>" or a bare id, as the id Relay stores as sourceId. */
function bareAlertId(raw: string): string {
  return raw.startsWith(ALERT_URL_PREFIX) ? raw.slice(ALERT_URL_PREFIX.length) : raw;
}

/**
 * Identifiers NWS says have expired or been replaced. NWS puts them on each feature, in
 * `properties.parameters.expiredReferences`: an array of strings "<sender>,<identifier>,<sent>".
 * Also accepts an object with `identifier`, a bare id, and a top-level `expiredReferences` list.
 */
export function expiredReferencesOf(body: string): Set<string> {
  const out = new Set<string>();
  if (!body.includes('expiredReferences')) return out;
  const take = (list: unknown): void => {
    if (!Array.isArray(list)) return;
    for (const item of list) {
      let raw: unknown;
      if (typeof item === 'string') {
        const parts = item.split(',').map((x) => x.trim());
        raw = parts.length >= 3 ? parts[1] : parts[0];
      } else if (typeof item === 'object' && item !== null) {
        raw =
          (item as Record<string, unknown>)['identifier'] ??
          (item as Record<string, unknown>)['@id'];
      }
      if (typeof raw !== 'string' || raw === '') continue;
      out.add(bareAlertId(raw));
    }
  };
  try {
    const doc: unknown = JSON.parse(body);
    if (typeof doc !== 'object' || doc === null) return out;
    take((doc as Record<string, unknown>)['expiredReferences']);
    const features = (doc as Record<string, unknown>)['features'];
    if (Array.isArray(features)) {
      for (const feature of features) {
        const props =
          typeof feature === 'object' && feature !== null
            ? (feature as Record<string, unknown>)['properties']
            : undefined;
        const params =
          typeof props === 'object' && props !== null
            ? (props as Record<string, unknown>)['parameters']
            : undefined;
        if (typeof params === 'object' && params !== null) {
          take((params as Record<string, unknown>)['expiredReferences']);
        }
      }
    }
  } catch {
    // Not JSON: parseFeed already reported that.
  }
  return out;
}

const endOf = (a: Alert): number =>
  Math.max(Date.parse(a.expires), a.ends ? Date.parse(a.ends) : 0);
const idsOf = (a: Alert): string[] => [a.sourceId, ...a.references.map((r) => r.sourceId)];

/**
 * End stored alerts that the feed no longer lists. Runs only after a successful 200 whose
 * features were all read (never on 304 or an error: those prove nothing about the feed).
 * - A stored alert is "in the feed" if its sourceId or any id in its chain is listed.
 * - Absent from RECONCILE_AFTER consecutive full reads AND for RECONCILE_MIN_ABSENT_MS, with its
 *   end still in the future: cancelled with reason no_longer_in_feed, and recorded as ended by
 *   feed absence (`seen.markEnded`) so it can be restored if it comes back.
 * - An id in an expiredReferences list ends the alert at once, for good (no marker).
 * Returns how many alerts are absent but still waiting for confirmation, and how many failed.
 */
export async function reconcileFeed(
  reconciler: Reconciler,
  feedIds: Set<string>,
  expired: Set<string>,
  log: (entry: Record<string, unknown>) => void,
  seen?: SeenStore,
): Promise<{ pending: number; failed: number }> {
  const now = reconciler.now();
  const minAbsentMs = reconciler.minAbsentMs ?? RECONCILE_MIN_ABSENT_MS;
  const stored = await reconciler.listActive(now);
  let pending = 0;
  let failed = 0;
  const live = new Set<string>();
  for (const alert of stored) {
    if (!(endOf(alert) > now.getTime())) continue; // already over; it expires on its own
    live.add(alert.id);
    const ids = idsOf(alert);
    // Only the alert's latest message counts: an Update lists its predecessors as expired.
    const explicit = expired.has(alert.sourceId);
    if (!explicit && ids.some((id) => feedIds.has(id))) {
      absentPolls.delete(alert.id);
      continue;
    }
    const prev = absentPolls.get(alert.id);
    const misses = (prev?.misses ?? 0) + 1;
    const since = prev?.since ?? now.getTime();
    if (!explicit && (misses < RECONCILE_AFTER || now.getTime() - since < minAbsentMs)) {
      absentPolls.set(alert.id, { misses, since });
      pending += 1;
      log({
        event: 'reconcile.absent',
        alertId: alert.id,
        sourceId: alert.sourceId,
        misses,
        absentMs: now.getTime() - since,
      });
      continue;
    }
    try {
      if (!explicit) {
        // Record why before ending it. If this fails the alert stays active (and is retried).
        await seen?.markEnded?.(alert.id, {
          endedBy: 'feed_absence',
          priorState: alert.state === 'updated' ? 'updated' : 'active',
          endedAt: now.toISOString(),
        });
      }
      try {
        await reconciler.cancel(alert, explicit ? 'expired_reference' : 'no_longer_in_feed');
      } catch (err) {
        // The alert was not ended, so the record must not say it was.
        if (!explicit) await seen?.clearEnded?.(alert.id).catch(() => undefined);
        throw err;
      }
      absentPolls.delete(alert.id);
      log({
        event: 'reconcile.cancelled',
        level: 'warn',
        alertId: alert.id,
        sourceId: alert.sourceId,
        reason: explicit ? 'expired_reference' : 'no_longer_in_feed',
      });
    } catch (err) {
      failed += 1;
      log({ event: 'reconcile.cancel_failed', alertId: alert.id, message: msg(err) });
    }
  }
  for (const id of [...absentPolls.keys()]) if (!live.has(id)) absentPolls.delete(id);
  return { pending, failed };
}

/**
 * Restore alerts that reconciliation ended for feed absence and that NWS now lists again, still
 * unexpired. A real CAP Cancel, an expiredReferences hit or a Cancel message in this feed is
 * never undone. The alert goes back to its earlier state (re-signed, saved, republished), then
 * `onApplied` runs for it so devices that were not notified yet get their push; devices that
 * were already notified are not pushed again because the fan-out batch keys and the sender's
 * claim (alertId:severity:cell:bucket) are unchanged. If that step fails the alert is ended
 * again and the marker kept, so the next full read retries.
 * Returns counts; `failed` includes a paused delivery (`paused`).
 */
export async function restoreEnded(
  deps: Pick<PollDeps, 'reconcile' | 'seen' | 'onApplied'>,
  feedIds: Set<string>,
  blocked: Set<string>,
  log: (entry: Record<string, unknown>) => void,
): Promise<{ restored: number; failed: number; paused: boolean }> {
  const { reconcile, seen } = deps;
  const out = { restored: 0, failed: 0, paused: false };
  if (!reconcile?.listEnded || !reconcile.restore || !seen?.getEnded || !seen.clearEnded) {
    return out;
  }
  const now = reconcile.now();
  for (const alert of await reconcile.listEnded(now)) {
    const ids = idsOf(alert);
    if (!ids.some((id) => feedIds.has(id)) || ids.some((id) => blocked.has(id))) continue;
    try {
      const marker = await seen.getEnded(alert.id);
      if (!marker || marker.endedBy !== 'feed_absence') continue; // a real Cancel: leave it
      const restored = await reconcile.restore(alert, marker.priorState);
      absentPolls.delete(alert.id);
      if (!feedIds.has(alert.sourceId)) {
        // Only a newer message of the chain is listed; the normal path applies it to the alert,
        // which is active again, and plans delivery for it.
        await seen.clearEnded(alert.id);
        log({ event: 'reconcile.restored', level: 'warn', alertId: alert.id, planned: false });
        out.restored += 1;
        continue;
      }
      try {
        await deps.onApplied?.({
          action: marker.priorState === 'updated' ? 'updated' : 'created',
          alert: restored,
        });
      } catch (err) {
        await reconcile.cancel(restored, 'no_longer_in_feed').catch(() => undefined);
        throw err;
      }
      await seen.clearEnded(alert.id);
      log({ event: 'reconcile.restored', level: 'warn', alertId: alert.id, planned: true });
      out.restored += 1;
    } catch (err) {
      if (err instanceof DeliveryPausedError) {
        out.paused = true;
        log({ event: 'reconcile.restore_paused', level: 'warn', alertId: alert.id });
        continue;
      }
      out.failed += 1;
      log({ event: 'reconcile.restore_failed', alertId: alert.id, message: msg(err) });
    }
  }
  return out;
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

/** Features of one NWS response processed concurrently (one chain is always processed in order). */
export const FEATURE_CONCURRENCY = 8;

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

/**
 * Thrown by our own code when an alert can never be processed (bad data, failed validation).
 * This is one of the only things that gets an alert quarantined; see isPermanent.
 */
export class PermanentAlertError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentAlertError';
  }
}

/**
 * Postgres SQLSTATE class 22 (data exception) or 23 (integrity violation), found in the Data API
 * error message ("... SQLState: 23505") or in a code / sqlState property on the error.
 */
const DATA_SQLSTATE = /(?:sql\s?state|sqlstate)\W{0,3}(2[23][0-9A-Z]{3})\b/i;

function hasDataSqlState(err: unknown): boolean {
  if (DATA_SQLSTATE.test(msg(err))) return true;
  if (typeof err !== 'object' || err === null) return false;
  const { code, sqlState } = err as { code?: unknown; sqlState?: unknown };
  return [code, sqlState].some((v) => typeof v === 'string' && /^2[23][0-9A-Z]{3}$/.test(v));
}

/**
 * ALLOWLIST: only errors known to be deterministic data problems may quarantine an alert.
 * Network, auth, throttling, "resuming" and anything unknown are outages: the alert stays
 * unseen and is retried forever, because dropping a real alert is worse than retrying.
 */
function isPermanent(err: unknown): boolean {
  return err instanceof PermanentAlertError || hasDataSqlState(err);
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
  /** UGC zones that could not be resolved, summed over alerts (REL-7). */
  unknownZones: number;
  /** Public CDN publishes that failed (the alert itself was still handled). */
  publishFailed: number;
  /** 1 when the poll was skipped to respect NWS pacing or a backoff, else 0. */
  throttled: number;
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
  unknownZones: 0,
  publishFailed: 0,
  throttled: 0,
});

const defaultLog = (entry: Record<string, unknown>): void => console.log(JSON.stringify(entry));
const msg = (err: unknown): string => (err instanceof Error ? err.message : String(err));

export async function pollOnce(deps: PollDeps): Promise<PollCounts> {
  const counts = emptyCounts();
  const complete = await pollInto(deps, counts);
  try {
    counts.publishFailed += (await deps.flush?.())?.publishFailed ?? 0;
  } catch (err) {
    counts.errors += 1;
    (deps.log ?? defaultLog)({ event: 'poll.flush_failed', message: msg(err) });
  }
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
  if (res.status === 'throttled') {
    // Not a failure: we are keeping to NWS's rate. Nothing was learned, so nothing is committed.
    counts.throttled = 1;
    log({ event: 'poll.throttled', reason: res.reason, retryInMs: res.retryInMs });
    return true;
  }
  if (res.status === 'error') {
    counts.errors += 1;
    counts.fetchFailed = 1;
    log({
      event: 'poll.fetch_error',
      httpStatus: res.httpStatus,
      message: res.message,
      ...(res.retryAfterMs !== undefined ? { retryAfterMs: res.retryAfterMs } : {}),
    });
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
  // Every id the feed lists (current ids and the ids they replace), for reconcileFeed. A feature
  // we could not even read makes the picture incomplete, so reconciliation is skipped then.
  const feedIds = new Set<string>();
  // Ids that a Cancel message in this feed refers to: never restored.
  const cancelledByFeed = new Set<string>();
  // Ids that a later message in this feed refers to (the alert has a successor here).
  const supersededIds = new Set<string>();
  let feedReadable = true;
  for (const feature of feed.features) {
    try {
      const norm = deps.normalize(feature);
      if (!norm.ok) {
        counts.rejected += 1;
        if (norm.sourceId !== null) feedIds.add(norm.sourceId);
        else feedReadable = false;
        log({
          event: 'poll.rejected',
          ...(norm.permanent ? { level: 'error' } : {}),
          sourceId: norm.sourceId,
          reason: norm.reason,
        });
        continue;
      }
      counts.accepted += 1;
      feedIds.add(norm.alert.sourceId);
      for (const ref of norm.alert.references) {
        feedIds.add(ref.sourceId);
        supersededIds.add(ref.sourceId);
      }
      if (norm.alert.messageType === 'Cancel') {
        for (const ref of norm.alert.references) cancelledByFeed.add(ref.sourceId);
      }
      queue.push(norm.alert);
    } catch (err) {
      feedReadable = false;
      counts.errors += 1;
      log({ event: 'poll.feature_error', message: msg(err) });
    }
  }
  const rank = (a: Alert): number => SEVERITY_RANK[a.relaySeverity];
  queue.sort((a, b) => rank(a) - rank(b) || Date.parse(a.sent) - Date.parse(b.sent));

  let complete = true;
  let deferred = 0;

  // Alerts ended for feed absence that are listed again come back before this poll's messages
  // are applied, so an update to one of them is applied to an active alert.
  // An Update lists the message it replaces as expired; that is not an ending, so ids that a
  // message in this feed refers to are dropped.
  const expiredIds = expiredReferencesOf(res.body);
  for (const id of supersededIds) expiredIds.delete(id);
  if (deps.reconcile && feedReadable) {
    try {
      const r = await restoreEnded(
        deps,
        feedIds,
        new Set([...expiredIds, ...cancelledByFeed]),
        log,
      );
      counts.errors += r.failed;
      if (r.failed > 0 || r.paused) complete = false;
    } catch (err) {
      counts.errors += 1;
      complete = false;
      log({ event: 'reconcile.restore_failed', message: msg(err) });
    }
  }

  /** One feature. Never throws: one bad feature must not stop the rest. */
  const processAlert = async (alert: Alert): Promise<void> => {
    if (deps.remainingMs && deps.remainingMs() < MIN_FEATURE_BUDGET_MS) {
      // Leave it unseen; the next poll (no ETag commit) picks it up.
      complete = false;
      deferred += 1;
      return;
    }
    let stage: 'prepare' | 'save' | 'deliver' = 'prepare';
    try {
      if (await deps.seen.has(alert.sourceId)) {
        counts.skippedSeen += 1;
        return;
      }
      alert.area.h3Cells = await deps.cellsFor(alert.area, {
        onUnknownZones: (ugcs, total) => {
          counts.unknownZones += ugcs.length;
          log({
            event: 'poll.unknown_zones',
            level: 'warn',
            sourceId: alert.sourceId,
            unknown: ugcs.length,
            total,
            ugcs: ugcs.slice(0, 20),
          });
        },
      });
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
        } else if (stored.state === 'cancelled' && alert.messageType === 'Cancel') {
          // A real Cancel wins over any earlier feed-absence record: never restore it.
          await deps.seen.clearEnded?.(stored.id);
          log({ event: 'poll.ignored', sourceId: alert.sourceId, reason: result.reason });
        } else if (
          stored.state === 'cancelled' &&
          (await deps.seen.getEnded?.(stored.id))?.endedBy === 'feed_absence'
        ) {
          // The alert this message belongs to was ended for feed absence and has not been
          // restored yet (restoration failed or is paused). Keep this message unseen.
          complete = false;
          counts.errors += 1;
          log({ event: 'poll.restore_pending', level: 'warn', sourceId: alert.sourceId });
          return;
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
        return;
      }
      if (err instanceof TooManyCellsError || err instanceof InvalidGeometryError) {
        // Broken geometry never gets better: reject it for good instead of retrying forever.
        counts.rejected += 1;
        log({ event: 'poll.rejected_permanent', level: 'error', message: msg(err) });
        await markQuietly(deps, alert, counts, log);
        return;
      }
      if (stage !== 'deliver' && isPermanent(err)) {
        // Same permanent data error for the same alert, poll after poll: it will never save.
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
          return;
        }
      }
      counts.errors += 1;
      log({ event: 'poll.feature_error', message: msg(err) });
    }
  };

  // Chains (an alert and the messages that reference it) are processed in order, one at a time,
  // so an Update or Cancel never overtakes the message it replaces. Different chains run
  // concurrently on a bounded pool, most urgent chain first.
  const chains = groupIntoChains(queue, rank);
  let next = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      const chain = chains[next++];
      if (!chain) return;
      for (const alert of chain) await processAlert(alert);
    }
  };
  const width = Math.max(1, Math.min(deps.concurrency ?? FEATURE_CONCURRENCY, chains.length));
  await Promise.all(Array.from({ length: width }, () => worker()));
  if (deferred > 0) {
    log({
      event: 'poll.deadline_reached',
      level: 'warn',
      remainingMs: deps.remainingMs?.(),
      deferred,
    });
  }
  if (deps.reconcile && feedReadable) {
    try {
      const { pending, failed } = await reconcileFeed(
        deps.reconcile,
        feedIds,
        expiredIds,
        log,
        deps.seen,
      );
      counts.errors += failed;
      // Absent alerts waiting for confirmation: keep the ETag uncommitted so the next poll
      // is a full 200 (a 304 would not count) and can confirm or clear them.
      if (pending > 0) complete = false;
    } catch (err) {
      counts.errors += 1;
      log({ event: 'reconcile.failed', message: msg(err) });
    }
  }
  return complete;
}

/**
 * Groups alerts that belong to one chain (same sourceId, or one references the other, directly or
 * through others) and orders each group oldest first. Groups come back most urgent first.
 */
export function groupIntoChains(queue: Alert[], rank: (a: Alert) => number): Alert[][] {
  const parent = new Map<string, string>();
  const find = (x: string): string => {
    let root = x;
    for (let p = parent.get(root); p !== undefined && p !== root; p = parent.get(root)) root = p;
    parent.set(x, root);
    return root;
  };
  const union = (a: string, b: string): void => {
    if (!parent.has(a)) parent.set(a, a);
    if (!parent.has(b)) parent.set(b, b);
    parent.set(find(a), find(b));
  };
  for (const alert of queue) {
    union(alert.sourceId, alert.sourceId);
    for (const ref of alert.references) union(alert.sourceId, ref.sourceId);
  }
  const groups = new Map<string, Alert[]>();
  for (const alert of queue) {
    const root = find(alert.sourceId);
    const group = groups.get(root);
    if (group) group.push(alert);
    else groups.set(root, [alert]);
  }
  const out = [...groups.values()];
  for (const g of out) g.sort((a, b) => Date.parse(a.sent) - Date.parse(b.sent));
  const urgency = (g: Alert[]): number => Math.min(...g.map(rank));
  return out.sort((a, b) => urgency(a) - urgency(b));
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
  /** Time between the START of one poll and the start of the next (NWS rate limit). */
  intervalMs: number;
  /** Injectable for tests. Defaults to a real timer. */
  sleep?(ms: number): Promise<void>;
  /** Injectable for tests. Defaults to Date.now. */
  now?(): number;
  /** Milliseconds left in the Lambda invocation. */
  remainingMs(): number;
}

const SAFETY_MARGIN_MS = 10_000;

export async function pollLoop(deps: PollDeps, opts: LoopOptions): Promise<PollCounts[]> {
  const log = deps.log ?? defaultLog;
  const sleep = opts.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const now = opts.now ?? Date.now;
  const results: PollCounts[] = [];
  let lastStart = 0;
  for (let i = 0; i < opts.polls; i++) {
    if (i > 0) {
      // Pace start to start, so slow processing never pushes polls closer together than
      // intervalMs, and never makes the wait longer than needed.
      const wait = Math.max(0, opts.intervalMs - (now() - lastStart));
      // Stop if there is not enough time left for the wait plus a poll.
      if (opts.remainingMs() < wait + SAFETY_MARGIN_MS) {
        log({ event: 'poll.loop_stopped_low_time', remainingMs: opts.remainingMs(), polls: i });
        break;
      }
      if (wait > 0) await sleep(wait);
    }
    lastStart = now();
    results.push(await pollOnce({ ...deps, remainingMs: deps.remainingMs ?? opts.remainingMs }));
  }
  return results;
}
