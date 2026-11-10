import { KILL_SWITCH_TTL_MS } from '../../config/kill-switch.js';
import { alertEndTime, claimExpiry } from '../../delivery/eligibility.js';
import type { IdempotencyStore } from '../../ports.js';
import type { Alert } from '@relay/shared';
import {
  BATCH_BYTE_BUDGET,
  CELL_BUCKETS,
  DENSE_CELL_RECIPIENTS,
  type CellIndex,
  type SendBatch,
  type SendQueue,
} from '../contracts.js';
import { checkEligibility, type EligibilityDeps, type FanoutRefusal } from './common.js';
import { bucketFor, densePartFor } from './hash.js';
import type { ResumableShardJob } from './messages.js';
import { bucketKey, denseCellKey, denseMarkerKey, densePartKey, splitMarkerKey } from './keys.js';
import { splitToFit } from './pack.js';
import { isPrimaryCell } from './primary-cell.js';
import type { ShardDispatcher } from './queues.js';

export interface ShardWorkerDeps extends EligibilityDeps {
  cellIndex: Pick<CellIndex, 'listForCell'>;
  sendQueue: SendQueue;
  /** Signed check-in token with the cell bound in (token v2). */
  makeCheckInToken(alertId: string, deviceId: string, h3Cell: string, expiresAt: Date): string;
  /**
   * The sender's idempotency store, used only for dense cells: before a dense cell's part batches
   * go out, each bucket's normal key is tombstoned (see contracts.ts DENSE_CELL_RECIPIENTS). Absent
   * (old wiring, tests): dense cells still send, logged as an error, and a tier change between two
   * plans of one alert version could then double-send those buckets.
   */
  idempotency?: Required<Pick<IdempotencyStore, 'claim' | 'markSent' | 'peek'>>;
  /**
   * Re-runs a job later (the paused queue: about 60 s, then back to the shard function). Used for
   * the cells of a dense bucket that another attempt holds, so they settle without failing the job
   * into Lambda's retries and the shard DLQ. Absent: the job throws DenseBucketContendedError.
   */
  retryLater?: (job: ResumableShardJob) => Promise<void>;
  /** Cells processed at the same time. Default 16. */
  cellConcurrency?: number;
  /** Cell-index page size. Default 1,000. */
  pageSize?: number;
  /** Batches held before they are handed to the send queue. Default 200. */
  flushBatches?: number;
  /** Or recipients held in those batches. Default 5,000. */
  flushRecipients?: number;
  /** Recipients above which a cell is dense. Default DENSE_CELL_RECIPIENTS. */
  denseCellRecipients?: number;
  /** Serialized-size budget of one batch. Default BATCH_BYTE_BUDGET. */
  maxBatchBytes?: number;
  /**
   * Milliseconds left before the Lambda is killed. With `continuation` set, the worker stops
   * taking new cells when this drops under `continueUnderMs` and hands the rest to a new job.
   */
  remainingMs?: () => number;
  /** Where the continuation ShardJob goes (the shard Lambda itself). */
  continuation?: ShardDispatcher;
  /** Default 10,000. */
  continueUnderMs?: number;
}

export interface ShardResult {
  /** Set when nothing was sent. 'paused' is not final: the handler retries it. */
  refused?: FanoutRefusal;
  cells: number;
  entries: number;
  /** Entries skipped because the device has no push target. */
  skippedNoTarget: number;
  /** Entries skipped because the device is sent from another of its cells (primary-cell rule). */
  skippedNonPrimary: number;
  /** Extra entries for a device already in the same cell (two places in one cell). */
  duplicates: number;
  /** Recipients across all batches. */
  recipients: number;
  batches: number;
  /** Cells sent in dense parts (DENSE_CELL_RECIPIENTS). */
  denseCells: number;
  /** Dense-cell buckets skipped because an earlier plan's normal batch already covered them. */
  coveredBuckets: number;
  /** Most recipients held in memory at once (cell buffers plus unflushed batches). */
  peakBufferedRecipients: number;
  /** Set when time ran short: the `startAt` of the continuation job that was dispatched. */
  continuedAt?: number;
}

const CONTINUE_UNDER_MS = 10_000;

const CELL_CONCURRENCY = 16;
const PAGE_SIZE = 1_000;
const FLUSH_BATCHES = 200;
/** Also flush once this many recipients wait (dense parts make many small batches quickly). */
const FLUSH_RECIPIENTS = 5_000;
/** How long the shard worker holds a bucket's normal key while it tombstones it. */
const TOMBSTONE_LEASE_MS = 30_000;

/**
 * The job's kill-switch value if it is fresh (younger than the worker's own cache TTL), else
 * undefined. Malformed or future-dated values are ignored, so a bad field only costs an SSM read.
 */
export function freshKillSwitch(job: ResumableShardJob, nowMs: number): boolean | undefined {
  const k = job.killSwitch as { paused?: unknown; readAtMs?: unknown } | undefined;
  if (!k || typeof k.paused !== 'boolean' || typeof k.readAtMs !== 'number') return undefined;
  const age = nowMs - k.readAtMs;
  return age >= -1_000 && age < KILL_SWITCH_TTL_MS ? k.paused : undefined;
}

const emptyResult = (cells: number): ShardResult => ({
  cells,
  entries: 0,
  skippedNoTarget: 0,
  skippedNonPrimary: 0,
  duplicates: 0,
  recipients: 0,
  batches: 0,
  denseCells: 0,
  coveredBuckets: 0,
  peakBufferedRecipients: 0,
});

type Recipient = SendBatch['recipients'][number];

/**
 * A cheap size estimate of a batch's recipients in JSON (string lengths plus the field names).
 * Never below half the real size for ASCII ids and tokens, which is all it is used for.
 */
function roughBytes(recipients: Recipient[]): number {
  let n = 300;
  for (const r of recipients) {
    n +=
      110 +
      r.deviceId.length +
      r.target.token.length +
      (r.target.project?.length ?? 0) +
      r.checkInToken.length;
  }
  return n;
}

const byDeviceId = (a: Recipient, b: Recipient): number =>
  a.deviceId < b.deviceId ? -1 : a.deviceId > b.deviceId ? 1 : 0;

export { bucketKey, denseCellKey, denseMarkerKey, densePartKey } from './keys.js';

/**
 * Thrown after a shard finished all it could, when some dense bucket's tombstone was contended and
 * there is no `retryLater` to hand the cells to (Lambda's async retry then runs the job again).
 */
export class DenseBucketContendedError extends Error {
  constructor(readonly buckets: number) {
    super(`${buckets} dense bucket(s) are held by another attempt; the shard will retry`);
    this.name = 'DenseBucketContendedError';
  }
}

/**
 * Builds and enqueues every SendBatch for one ShardJob.
 *
 * Normal cells: the cell index is paged and devices are bucketed with fnv1a32(deviceId) %
 * CELL_BUCKETS, giving one SendBatch per non-empty (cell, bucket):
 * `${alertId}:${severity}:${cell}:${bucket}`. Batch keys depend only on the alert, the cell and the
 * bucket, so re-planning, or a retry of this job after a crash partway, re-enqueues identical keys
 * and the sender's claims make that safe.
 *
 * Dense cells (more than DENSE_CELL_RECIPIENTS recipients, or a bucket over the byte budget): each
 * bucket is split into DENSE_PARTS parts by densePartFor(deviceId), key `...:${bucket}.${part}/16`.
 * The cell index returns a cell sorted by deviceId and densePartFor is monotone in that order, so
 * the worker streams: it holds only the part being read, emits it when the next part starts, and
 * never holds more than about DENSE_CELL_RECIPIENTS recipients for any one cell. Before the first
 * part goes out, every bucket's normal key is tombstoned (tombstoneDenseCell), which keeps a tier
 * change between plans exactly-once (contracts.ts).
 *
 * Cross-cell duplicates: a device with saved places in several cells of one alert area has an entry
 * in each. Every entry carries the device's full `deviceCells`, and only the entry in the
 * smallest cell of (deviceCells ∩ alert area) sends (isPrimaryCell), so a multi-cell device is in
 * exactly one batch per alert with no cross-shard coordination and no per-device write. Entries
 * without deviceCells (old data) and inconsistent entries fail open and send.
 */
export async function processShard(
  job: ResumableShardJob,
  deps: ShardWorkerDeps,
): Promise<ShardResult> {
  const passed = freshKillSwitch(job, deps.clock.now().getTime());
  const outcome = await checkEligibility(
    job,
    passed === undefined ? deps : { ...deps, isDeliveryPaused: async () => passed },
    'shard',
  );
  if (!outcome.ok) return { ...emptyResult(job.cells.length), refused: outcome.refused };
  const { alert, queue } = outcome;
  const alertCellSet = new Set(alert.area.h3Cells);

  const result = emptyResult(job.cells.length);
  const expiresAt = alertEndTime(alert); // tokens live as long as the alert does
  const concurrency = deps.cellConcurrency ?? CELL_CONCURRENCY;
  const pageSize = deps.pageSize ?? PAGE_SIZE;
  const flushAt = deps.flushBatches ?? FLUSH_BATCHES;
  const flushRecipients = deps.flushRecipients ?? FLUSH_RECIPIENTS;
  const denseAt = deps.denseCellRecipients ?? DENSE_CELL_RECIPIENTS;
  const maxBytes = deps.maxBatchBytes ?? BATCH_BYTE_BUDGET;

  // Memory accounting: recipients held in cell buffers plus unflushed batches.
  let buffered = 0;
  const hold = (n: number): void => {
    buffered += n;
    if (buffered > result.peakBufferedRecipients) result.peakBufferedRecipients = buffered;
  };

  // Batches from many (often sparse) cells are pooled so messages fill up to
  // MAX_BATCHES_PER_MESSAGE; the send queue packs them by measured size.
  let pending: SendBatch[] = [];
  let pendingRecipients = 0;
  /** Keys of batches in `pending` that were split into pieces (marked before the enqueue). */
  let splitKeys: string[] = [];
  const flush = async (force: boolean): Promise<void> => {
    if (
      pending.length === 0 ||
      (!force && pending.length < flushAt && pendingRecipients < flushRecipients)
    ) {
      return;
    }
    const toSend = pending;
    const n = pendingRecipients;
    const splits = splitKeys;
    pending = [];
    pendingRecipients = 0;
    splitKeys = [];
    try {
      // Split markers first, so late join never sees a split key as unplanned once its pieces
      // can be sent (keys.ts plannedKeyFor). A failure fails the attempt before any enqueue.
      const store = deps.idempotency;
      if (store && splits.length > 0) {
        const keep = claimExpiry(alert);
        await Promise.all(splits.map((k) => store.markSent(splitMarkerKey(k), keep)));
      }
      await deps.sendQueue.enqueue(queue, toSend);
    } finally {
      buffered -= n;
    }
  };
  /**
   * Queues one batch (its recipients stay counted in `buffered` until flushed). With `measure`, a
   * batch still over the byte budget is halved (pathological: see splitToFit).
   */
  const emit = (batch: SendBatch, measure: boolean): void => {
    let pieces: SendBatch[] = [batch];
    if (measure && batch.recipients.length > 1 && roughBytes(batch.recipients) > maxBytes / 2) {
      pieces = splitToFit(batch, maxBytes);
      if (pieces.length > 1) {
        splitKeys.push(batch.batchKey);
        deps.logger.warn('splitting oversize batch', {
          batchKey: batch.batchKey,
          recipients: batch.recipients.length,
          parts: pieces.length,
        });
      }
    }
    for (const b of pieces) {
      pending.push(b);
      pendingRecipients += b.recipients.length;
      result.batches++;
      result.recipients += b.recipients.length;
    }
  };
  const bytesOf = (b: SendBatch): number => Buffer.byteLength(JSON.stringify(b), 'utf8');

  const makeBatch = (batchKey: string, recipients: Recipient[]): SendBatch => ({
    alertId: alert.id,
    sourceId: alert.sourceId,
    relaySeverity: alert.relaySeverity,
    batchKey,
    recipients: recipients.sort(byDeviceId),
    attempt: 0,
  });

  // When one cell fails, the others stop at their next step so a dead attempt does not keep
  // sending while the retry starts.
  let failed = false;
  let contended = 0;
  const contendedCells: string[] = [];

  const doCell = async (h3Cell: string): Promise<void> => {
    // Normal mode: bucket -> recipients for the whole cell. Dense mode: (bucket -> recipients) for
    // the current part only; `emitBucket[b]` says whether bucket b's parts are sent at all.
    let buckets = new Map<number, Recipient[]>();
    let held = 0; // recipients in `buckets`
    let seen = new Set<string>();
    let dense: { part: number; emitBucket: boolean[] } | undefined;

    const release = (n: number): void => {
      held -= n;
      buffered -= n;
    };

    /** Emits the current dense part and frees it. */
    const emitPart = (part: number): void => {
      if (!dense) return;
      for (let bucket = 0; bucket < CELL_BUCKETS; bucket++) {
        const recipients = buckets.get(bucket);
        if (!recipients || recipients.length === 0) continue;
        if (dense.emitBucket[bucket]) {
          emit(makeBatch(densePartKey(alert, h3Cell, bucket, part), recipients), true);
          // Recipients move to `pending` (still counted in `buffered` until flushed).
          held -= recipients.length;
        } else {
          release(recipients.length);
        }
      }
      buckets = new Map();
    };

    /** Switches this cell to dense parts: tombstones its buckets, emits the parts already read. */
    const goDense = async (): Promise<void> => {
      const emitBucket = await tombstoneDenseCell(alert, h3Cell, deps);
      result.denseCells++;
      result.coveredBuckets += emitBucket.filter((e) => e === 'covered').length;
      const held = emitBucket.filter((e) => e === 'contended').length;
      if (held > 0) {
        contended += held;
        contendedCells.push(h3Cell);
      }
      dense = { part: -1, emitBucket: emitBucket.map((e) => e === 'emit') };
      deps.logger.info('fanout.dense_cell', { alertId: alert.id, h3Cell, recipientsSoFar: held });
      // Regroup what was read so far by part. The stream is sorted, so every part but the highest
      // seen is complete and goes out now.
      const all = [...buckets.values()].flat();
      buckets = new Map();
      all.sort(byDeviceId);
      let i = 0;
      while (i < all.length) {
        const part = densePartFor((all[i] as Recipient).deviceId);
        let j = i;
        while (j < all.length && densePartFor((all[j] as Recipient).deviceId) === part) {
          const r = all[j] as Recipient;
          const bucket = bucketFor(r.deviceId);
          let list = buckets.get(bucket);
          if (!list) buckets.set(bucket, (list = []));
          list.push(r);
          j++;
        }
        dense.part = part;
        if (j < all.length) emitPart(part);
        i = j;
      }
      await flush(false);
    };

    let cursor: string | undefined;
    do {
      if (failed) return;
      const page = await deps.cellIndex.listForCell(h3Cell, cursor, pageSize);
      cursor = page.cursor;
      for (const entry of page.entries) {
        result.entries++;
        if (!entry.target) {
          result.skippedNoTarget++;
          continue;
        }
        if (!isPrimaryCell(entry, alertCellSet)) {
          result.skippedNonPrimary++;
          continue;
        }
        if (dense) {
          const part = densePartFor(entry.deviceId);
          if (part < dense.part) {
            // The stream is not sorted by deviceId: emitted parts would be incomplete. Never
            // happens with DynamoDB (sk order); fail loudly rather than skip or double-send.
            throw new Error(`cell index for ${h3Cell} is not in deviceId order`);
          }
          if (part > dense.part) {
            emitPart(dense.part);
            dense.part = part;
            seen = new Set(); // a device's entries are all in one part
          }
        }
        if (seen.has(entry.deviceId)) {
          result.duplicates++;
          continue;
        }
        seen.add(entry.deviceId);
        const bucket = bucketFor(entry.deviceId);
        let list = buckets.get(bucket);
        if (!list) buckets.set(bucket, (list = []));
        list.push({
          deviceId: entry.deviceId,
          target: entry.target,
          language: entry.language,
          checkInToken: deps.makeCheckInToken(alert.id, entry.deviceId, h3Cell, expiresAt),
        });
        held++;
        hold(1);
      }
      if (!dense && held > denseAt) await goDense();
      if (dense) await flush(false);
    } while (cursor);
    if (failed) return;

    let normal: SendBatch[] = [];
    if (!dense) {
      for (let bucket = 0; bucket < CELL_BUCKETS; bucket++) {
        const recipients = buckets.get(bucket);
        if (recipients && recipients.length > 0) {
          normal.push(makeBatch(bucketKey(alert, h3Cell, bucket), recipients));
        }
      }
      // A cell under the dense size whose bucket would still not fit the budget goes dense too.
      // Exact measuring is a full serialization, so only for batches a cheap estimate puts near it.
      // And a cell that went dense in any earlier plan of this alert version stays dense: its
      // normal keys may be tombstones, so normal batches would be dropped as duplicates (one read).
      if (
        normal.some((b) => roughBytes(b.recipients) > maxBytes / 2 && bytesOf(b) > maxBytes) ||
        (normal.length > 0 && (await wentDense(alert, h3Cell, deps)))
      ) {
        normal = [];
        await goDense();
      }
    }
    if (dense) {
      emitPart(dense.part);
    } else {
      for (const b of normal) {
        emit(b, false); // measured just above
        held -= b.recipients.length;
      }
      buckets = new Map();
    }
    await flush(false);
  };

  // A small pool: `concurrency` workers pull the next cell, in order, until none are left or
  // time runs short. Cells before `startAt` were done by an earlier run of this job.
  const startAt = Math.min(Math.max(job.startAt ?? 0, 0), job.cells.length);
  const continueUnder = deps.continueUnderMs ?? CONTINUE_UNDER_MS;
  const canContinue = deps.remainingMs !== undefined && deps.continuation !== undefined;
  let next = startAt;
  let outOfTime = false;
  const worker = async (): Promise<void> => {
    while (next < job.cells.length && !failed) {
      if (canContinue && (deps.remainingMs?.() ?? Infinity) < continueUnder) {
        outOfTime = true;
        return;
      }
      const cell = job.cells[next++];
      if (cell === undefined) continue;
      try {
        await doCell(cell);
      } catch (err) {
        failed = true;
        throw err;
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, job.cells.length) }, worker));
  await flush(true);

  if (contended > 0) {
    // Everything else went out. Running these cells again later re-sends identical keys (deduped by
    // the sender) and settles the held buckets once the other attempt has finished or its lease ran
    // out (30 s for a shard's tombstone, 45 s for a sender's claim; the paused queue waits 60 s).
    deps.logger.warn('fanout.dense_bucket_contended', {
      alertId: alert.id,
      shardId: job.shardId,
      buckets: contended,
      cells: contendedCells.length,
      deferred: deps.retryLater !== undefined,
    });
    if (!deps.retryLater) throw new DenseBucketContendedError(contended);
    // A fresh job of just those cells; the kill switch is re-read when it runs.
    const {
      killSwitch: _ks,
      startAt: _sa,
      ...rest
    } = job as ResumableShardJob & {
      killSwitch?: unknown;
    };
    await deps.retryLater({ ...rest, cells: contendedCells });
  }

  // Every cell pulled so far has finished (the pool is joined above), so `next` is exactly the
  // first cell not yet done: the continuation neither skips nor repeats a cell.
  if (outOfTime && next < job.cells.length && deps.continuation) {
    const continuation: ResumableShardJob = { ...job, startAt: next };
    await deps.continuation.dispatch([continuation]);
    result.continuedAt = next;
    deps.logger.warn('fanout.shard_continued', {
      alertId: alert.id,
      shardId: job.shardId,
      startAt,
      continuedAt: next,
      cells: job.cells.length,
    });
  }

  deps.logger.info('shard processed', {
    alertId: alert.id,
    shardId: job.shardId,
    queue,
    ...result,
  });
  return result;
}

/** Whether an earlier plan of this alert version took this cell dense (its cell marker). */
async function wentDense(alert: Alert, h3Cell: string, deps: ShardWorkerDeps): Promise<boolean> {
  return deps.idempotency
    ? (await deps.idempotency.peek(denseCellKey(alert, h3Cell))) === 'sent'
    : false;
}

type BucketFate = 'emit' | 'covered' | 'contended';

/**
 * Takes over every bucket of a dense cell from the normal tier, so a tier change between two plans
 * of the same alert version neither double-sends nor skips (contracts.ts DENSE_CELL_RECIPIENTS).
 * Per bucket, with N its normal key and D = `${N}:dense`:
 *  - claim(N) wins: mark D sent, then N sent (the tombstone: a queued normal batch for N is now a
 *    duplicate to the sender, and late join reads N as planned). Emit the parts.
 *  - N is taken and D is sent: an earlier dense plan of this alert did the above. Emit the parts
 *    again (same keys; the sender dedupes them).
 *  - N is sent and D is not: a normal batch was sent for this bucket. 'covered': emit nothing.
 *  - N is leased and D is not: a sender is sending the normal batch, or another dense attempt is
 *    between its claim and its mark. 'contended': nothing now, the shard is retried.
 * D is marked before N: a crash between the two can only lead to a duplicate later, never a miss.
 *
 * Why tombstone first, parts after, and why that cannot miss: the order is cell marker C, then per
 * bucket D, N, then the parts. Parts first would let a still-queued normal batch and the parts both
 * send (a duplicate of every device). Tombstone first leaves one window: N is a tombstone but some
 * parts are not enqueued yet (SQS error, timeout, another cell throwing). Every way out of that
 * window runs this cell again for this alert version (the job's retry, its continuation, a redrive,
 * a re-plan), and C makes every such run take the dense path whatever the cell's size is by then,
 * which finds D sent and re-emits the parts. A normal plan that read C as absent just before C was
 * written is covered by the per-bucket cases above: its N batch is dropped (tombstone won), being
 * sent (contended: retried until it is 'covered') or sent ('covered'). The only loss is the same as
 * for any shard job: an
 * attempt that is never run again (a shard DLQ message nobody redrives), which alarms.
 */
async function tombstoneDenseCell(
  alert: Alert,
  h3Cell: string,
  deps: ShardWorkerDeps,
): Promise<BucketFate[]> {
  const store = deps.idempotency;
  if (!store) {
    deps.logger.error('fanout.dense_without_idempotency', { alertId: alert.id, h3Cell });
    return Array.from({ length: CELL_BUCKETS }, () => 'emit' as const);
  }
  const lease = new Date(deps.clock.now().getTime() + TOMBSTONE_LEASE_MS);
  const keep = claimExpiry(alert);
  // The cell marker goes first: from here on every plan of this alert version takes this cell
  // dense, so a tombstone written below is always followed (now or on a retry) by its parts.
  await store.markSent(denseCellKey(alert, h3Cell), keep);
  return Promise.all(
    Array.from({ length: CELL_BUCKETS }, async (_, bucket): Promise<BucketFate> => {
      const normal = bucketKey(alert, h3Cell, bucket);
      const marker = denseMarkerKey(normal);
      const claim = await store.claim(normal, lease);
      if (claim === 'claimed') {
        await store.markSent(marker, keep);
        await store.markSent(normal, keep);
        return 'emit';
      }
      if ((await store.peek(marker)) === 'sent') return 'emit';
      return claim === 'sent' ? 'covered' : 'contended';
    }),
  );
}
