/**
 * Follow-up fan-out (docs/cancellation.md): after a CAP Cancel or Update is stored, tell the
 * devices that were sent an earlier version. Same pieces as the normal fan-out (planner -> shard
 * jobs -> cell index -> send queue -> sender) with three differences:
 *
 *  - the cells visited are the previous version's area: the cancelled alert's own (kept) cells, or
 *    for an Update the cells it dropped plus, when the severity did not change, the cells it kept;
 *  - each device is gated by the sent record of the previous version's batch that carried it (the
 *    member set and version tag the sender stores on markSent), so only devices that were sent the
 *    earlier version are targeted, each exactly once;
 *  - batch keys carry the kind and the version tag (`...:fu.<kind>.<tag>:<cell>:<bucket>`), quiet
 *    for cancel and removed, and a cancel's batches are delayed FOLLOW_UP_DELAY_SECONDS.
 */
import type { Alert } from '@relay/shared';
import { alertEndTime, claimExpiry } from '../../delivery/eligibility.js';
import {
  FOLLOW_UP_DELAY_SECONDS,
  followUpBatchKey,
  followUpCoversKeptCells,
  followUpKindFor,
  followUpRefusal,
  versionTag,
  type FollowUpKind,
  type FollowUpRequest,
} from '../../delivery/follow-up.js';
import type { KeyInspection } from '../../ports.js';
import {
  BATCH_BYTE_BUDGET,
  QUEUE_FOR_SEVERITY,
  type SendBatch,
  type SendQueueName,
} from '../contracts.js';
import { type EligibilityDeps, type FanoutRefusal } from './common.js';
import { bucketFor } from './hash.js';
import { plannedKeyFor } from './keys.js';
import { hasMember, SENT_HASH_BYTES } from './member-set.js';
import type { ResumableShardJob } from './messages.js';
import { splitToFit } from './pack.js';
import { buildShardJobs, type PlannerDeps } from './planner.js';
import { isPrimaryCell } from './primary-cell.js';
import { freshKillSwitch, type ShardWorkerDeps } from './shard-worker.js';

type Recipient = SendBatch['recipients'][number];

export type FollowUpEligibility =
  | { ok: true; alert: Alert; request: FollowUpRequest; previousSeverity: Alert['relaySeverity'] }
  | { ok: false; refused: FanoutRefusal };

/**
 * Loads the alert and decides whether its follow-up may go out now (planner and shard worker).
 * Refusals are returned; a missing alert or a failed read throws so the message is retried.
 */
export async function checkFollowUpEligibility(
  message: { alertId: string; sourceId: string; followUp?: FollowUpRequest },
  deps: EligibilityDeps,
  stage: 'planner' | 'shard',
): Promise<FollowUpEligibility> {
  const request = message.followUp;
  if (!request) throw new Error('not a follow-up message');
  const alert = await deps.alerts.get(message.alertId, message.sourceId);
  if (!alert) throw new Error(`alert ${message.alertId} not found`);
  if (alert.sourceId !== message.sourceId) {
    // A newer version was stored; it has its own follow-up (or, if cancelled, the last word).
    deps.logger.warn('followup.stale', { stage, alertId: alert.id });
    return { ok: false, refused: 'stale' };
  }
  const refusal = followUpRefusal(
    alert,
    request,
    versionTag(message.sourceId),
    deps.clock.now(),
    deps.verifyAlert,
  );
  if (refusal) {
    deps.logger.warn('followup.refused', { stage, alertId: alert.id, reason: refusal });
    return { ok: false, refused: refusal === 'stale_version' ? 'stale' : refusal };
  }
  // The keys of the version this one replaced. A Cancel keeps the severity it cancels.
  const previousSeverity =
    alert.supersedes?.relaySeverity ?? (request === 'cancel' ? alert.relaySeverity : undefined);
  if (!previousSeverity) {
    deps.logger.warn('followup.no_previous_version', { stage, alertId: alert.id });
    return { ok: false, refused: 'stale' };
  }
  if (await deps.isDeliveryPaused()) {
    deps.logger.warn('delivery paused', { stage, alertId: alert.id });
    return { ok: false, refused: 'paused' };
  }
  return { ok: true, alert, request, previousSeverity };
}

export interface FollowUpPlannerDeps extends PlannerDeps {
  /** Cells the version dropped (AlertStore.listDroppedCells). Absent: kept cells only. */
  droppedCells?(alertId: string, sourceId: string): Promise<string[]>;
}

/**
 * Turns a follow-up AlertReady into shard jobs over the previous version's area and dispatches
 * them. Deterministic and safe to repeat (keys downstream are per version, cell and bucket).
 */
export async function planFollowUp(
  ready: { alertId: string; sourceId: string; followUp?: FollowUpRequest },
  deps: FollowUpPlannerDeps,
): Promise<{ refused?: FanoutRefusal; shards: number; cells: number }> {
  const outcome = await checkFollowUpEligibility(ready, deps, 'planner');
  if (!outcome.ok) return { refused: outcome.refused, shards: 0, cells: 0 };
  const { alert, request } = outcome;
  let cells: string[];
  if (request === 'cancel') {
    cells = alert.area.h3Cells; // lifecycle keeps the cancelled version's area
  } else {
    const kept = followUpCoversKeptCells(alert) ? alert.area.h3Cells : [];
    const dropped = (await deps.droppedCells?.(alert.id, alert.sourceId)) ?? [];
    cells = [...new Set([...kept, ...dropped])].sort();
  }
  const killSwitch = { paused: false, readAtMs: deps.clock.now().getTime() };
  const jobs = buildShardJobs({ ...alert, area: { h3Cells: cells } }).map((job) => ({
    ...job,
    killSwitch,
    followUp: request,
  }));
  if (jobs.length > 0) await deps.shardDispatcher.dispatch(jobs);
  deps.logger.info('followup.planned', {
    alertId: alert.id,
    followUp: request,
    shards: jobs.length,
    cells: cells.length,
  });
  return { shards: jobs.length, cells: cells.length };
}

export interface FollowUpShardResult {
  refused?: FanoutRefusal;
  cells: number;
  entries: number;
  recipients: number;
  batches: number;
  /** Devices whose earlier-version batch never went out (not sent: no follow-up). */
  notSent: number;
  /** Devices whose batch was sent for the current version already (it carried the new text). */
  current: number;
  /** Devices in a sent batch whose member set does not hold them (sent from another cell). */
  notMember: number;
  /** Earlier-version keys a cancel marked sent so a late original batch can never go out. */
  tombstoned: number;
  /** Devices targeted although their batch was still being sent (their follow-up is delayed). */
  inFlight: number;
  continuedAt?: number;
}

const TOMBSTONE_LEASE_MS = 30_000;
/** Version tag of a key a cancel follow-up marked sent without sending (never a real tag). */
export const TOMBSTONE_VERSION = 'tombstone';
const PAGE_SIZE = 1_000;
const CONTINUE_UNDER_MS = 10_000;

/**
 * One shard of a follow-up. Cells are done one at a time (a cell's batches are enqueued before the
 * next is read), so memory is about one cell. Re-running the job (crash, continuation, retry)
 * rebuilds the same keys; the sender's claims drop the copies.
 */
export async function processFollowUpShard(
  job: ResumableShardJob,
  deps: ShardWorkerDeps,
): Promise<FollowUpShardResult> {
  const result: FollowUpShardResult = {
    cells: job.cells.length,
    entries: 0,
    recipients: 0,
    batches: 0,
    notSent: 0,
    current: 0,
    notMember: 0,
    tombstoned: 0,
    inFlight: 0,
  };
  const passed = freshKillSwitch(job, deps.clock.now().getTime());
  const outcome = await checkFollowUpEligibility(
    job,
    passed === undefined ? deps : { ...deps, isDeliveryPaused: async () => passed },
    'shard',
  );
  if (!outcome.ok) return { ...result, refused: outcome.refused };
  const { alert, request, previousSeverity } = outcome;
  const store = deps.idempotency;
  if (!store)
    throw new Error('follow-ups need the idempotency store to find who was sent the alert');

  const previous = { id: alert.id, relaySeverity: previousSeverity };
  const currentTag = versionTag(alert.sourceId);
  const currentCells = new Set(alert.area.h3Cells);
  // Fallback primary-cell rule (a sent batch without a member set): never misses, may duplicate.
  const fallbackSet = new Set([...alert.area.h3Cells, ...job.cells]);
  const expiresAt = alertEndTime(alert);
  const keep = claimExpiry(alert);
  const maxBytes = deps.maxBatchBytes ?? BATCH_BYTE_BUDGET;
  const pageSize = deps.pageSize ?? PAGE_SIZE;

  // One read per earlier-version key, shared by every device of its batch.
  const inspected = new Map<string, Promise<KeyInspection>>();
  const inspect = (key: string): Promise<KeyInspection> => {
    let p = inspected.get(key);
    if (!p) {
      p = store.inspect ? store.inspect(key) : store.peek(key).then((state) => ({ state }));
      inspected.set(key, p);
    }
    return p;
  };
  const peek = async (key: string) => (await inspect(key)).state;

  type Fate = 'send' | 'send_delayed' | 'not_sent' | 'current' | 'not_member';
  /** Whether this device was sent the earlier version from this cell. */
  const fateOf = async (
    entry: { h3Cell: string; deviceId: string; deviceCells?: string[] },
    cell: string,
  ): Promise<Fate> => {
    const planned = await plannedKeyFor(previous, cell, entry.deviceId, peek);
    let state = planned.state;
    if (state === 'absent' && request === 'cancel') {
      // Close the window where a cached read still shows the alert active: mark the key sent
      // before any follow-up goes out, so a late original batch for it is a duplicate.
      const claim = await store.claim(
        planned.key,
        new Date(deps.clock.now().getTime() + TOMBSTONE_LEASE_MS),
      );
      if (claim === 'claimed') {
        // Tagged, so a re-run of this job reads it as "never sent", not as a batch to follow up.
        await store.markSent(planned.key, keep, { version: TOMBSTONE_VERSION });
        result.tombstoned++;
        inspected.set(planned.key, Promise.resolve({ state: 'sent', version: TOMBSTONE_VERSION }));
        return 'not_sent';
      }
      inspected.delete(planned.key);
      state = claim;
    }
    if (state === 'absent') return 'not_sent';
    if (state === 'leased') return 'send_delayed';
    const sent = await inspect(planned.key);
    if (sent.version === TOMBSTONE_VERSION) return 'not_sent';
    if (sent.version === currentTag) return 'current';
    if (sent.members) {
      return hasMember(sent.members, entry.deviceId, SENT_HASH_BYTES) ? 'send' : 'not_member';
    }
    return isPrimaryCell(entry, fallbackSet) ? 'send' : 'not_member';
  };

  const queueFor = (kind: FollowUpKind): SendQueueName =>
    kind === 'update' ? (QUEUE_FOR_SEVERITY[alert.relaySeverity] ?? 'standard') : 'standard';

  const start = job.startAt ?? 0;
  for (let i = start; i < job.cells.length; i++) {
    if (
      i > start &&
      deps.remainingMs &&
      deps.continuation &&
      deps.remainingMs() < (deps.continueUnderMs ?? CONTINUE_UNDER_MS)
    ) {
      const continuation: ResumableShardJob = { ...job, startAt: i };
      await deps.continuation.dispatch([continuation]);
      result.continuedAt = i;
      break;
    }
    const cell = job.cells[i] as string;
    const kind = followUpKindFor(request, currentCells.has(cell));
    const buckets = new Map<number, { recipients: Recipient[]; delayed: boolean }>();
    const seen = new Set<string>();
    let cursor: string | undefined;
    do {
      const page = await deps.cellIndex.listForCell(cell, cursor, pageSize);
      cursor = page.cursor;
      for (const entry of page.entries) {
        result.entries++;
        if (!entry.target || seen.has(entry.deviceId)) continue;
        seen.add(entry.deviceId);
        const fate = await fateOf(entry, cell);
        if (fate === 'not_sent') {
          result.notSent++;
          continue;
        }
        if (fate === 'current') {
          result.current++;
          continue;
        }
        if (fate === 'not_member') {
          result.notMember++;
          continue;
        }
        if (fate === 'send_delayed') result.inFlight++;
        const bucket = bucketFor(entry.deviceId);
        let group = buckets.get(bucket);
        if (!group) buckets.set(bucket, (group = { recipients: [], delayed: false }));
        group.delayed ||= fate === 'send_delayed';
        group.recipients.push({
          deviceId: entry.deviceId,
          target: entry.target,
          language: entry.language,
          checkInToken: deps.makeCheckInToken(alert.id, entry.deviceId, cell, expiresAt),
        });
      }
    } while (cursor);

    const now: SendBatch[] = [];
    const later: SendBatch[] = [];
    for (const [bucket, group] of [...buckets].sort((a, b) => a[0] - b[0])) {
      const batch: SendBatch = {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        batchKey: followUpBatchKey(alert, kind, cell, bucket),
        recipients: group.recipients.sort((a, b) => (a.deviceId < b.deviceId ? -1 : 1)),
        attempt: 0,
      };
      const pieces = splitToFit(batch, maxBytes);
      (request === 'cancel' || group.delayed ? later : now).push(...pieces);
      result.batches += pieces.length;
      result.recipients += batch.recipients.length;
    }
    const queue = queueFor(kind);
    if (now.length > 0) await deps.sendQueue.enqueue(queue, now);
    if (later.length > 0) {
      await deps.sendQueue.enqueue(queue, later, { delaySeconds: FOLLOW_UP_DELAY_SECONDS });
    }
  }
  deps.logger.info('followup.shard_done', {
    alertId: alert.id,
    shardId: job.shardId,
    followUp: request,
    ...result,
  });
  return result;
}
