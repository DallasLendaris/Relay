import { alertEndTime } from '../../delivery/eligibility.js';
import {
  CELL_BUCKETS,
  type CellIndex,
  type SendBatch,
  type SendQueue,
  type ShardJob,
} from '../contracts.js';
import { checkEligibility, type EligibilityDeps, type FanoutRefusal } from './common.js';
import { bucketFor } from './hash.js';
import { MAX_MESSAGE_BYTES } from './pack.js';

export interface ShardWorkerDeps extends EligibilityDeps {
  cellIndex: Pick<CellIndex, 'listForCell'>;
  sendQueue: SendQueue;
  /** Signed check-in token with the cell bound in (token v2). */
  makeCheckInToken(alertId: string, deviceId: string, h3Cell: string, expiresAt: Date): string;
  /** Cells processed at the same time. Default 16. */
  cellConcurrency?: number;
  /** Cell-index page size. Default 1,000. */
  pageSize?: number;
  /** Batches held before they are handed to the send queue. Default 200. */
  flushBatches?: number;
}

export interface ShardResult {
  /** Set when nothing was sent. 'paused' is not final: the handler retries it. */
  refused?: FanoutRefusal;
  cells: number;
  entries: number;
  /** Entries skipped because the device has no push target. */
  skippedNoTarget: number;
  /** Extra entries for a device already in the same cell (two places in one cell). */
  duplicates: number;
  /** Recipients across all batches. */
  recipients: number;
  batches: number;
}

const CELL_CONCURRENCY = 16;
const PAGE_SIZE = 1_000;
const FLUSH_BATCHES = 200;
/** Headroom under MAX_MESSAGE_BYTES for the array brackets and a single-batch message. */
const OVERSIZE_LIMIT = MAX_MESSAGE_BYTES - 1_024;

const emptyResult = (cells: number): ShardResult => ({
  cells,
  entries: 0,
  skippedNoTarget: 0,
  duplicates: 0,
  recipients: 0,
  batches: 0,
});

/**
 * Builds and enqueues every SendBatch for one ShardJob.
 *
 * Per cell, the cell index is paged and devices are bucketed with fnv1a32(deviceId) % CELL_BUCKETS,
 * giving one SendBatch per non-empty (cell, bucket): `${alertId}:${severity}:${cell}:${bucket}`.
 * Batch keys depend only on the alert, the cell and the bucket, so re-planning, or SQS retrying
 * this job after a crash partway, re-enqueues identical keys and the sender's claims make that safe.
 *
 * KNOWN GAP: a device with saved places in two different cells of the same alert appears in two
 * batches (two keys) and can get two pushes. A shard worker cannot see other shards, and a global
 * per-device claim would cost one write per device (the cost the batch-key design removes). Both
 * pushes carry the same collapseKey (the alert id), so the phone app must collapse
 * notifications by collapseKey and treat a second check-in token for the same alert as a duplicate.
 */
export async function processShard(job: ShardJob, deps: ShardWorkerDeps): Promise<ShardResult> {
  const outcome = await checkEligibility(job, deps, 'shard');
  if (!outcome.ok) return { ...emptyResult(job.cells.length), refused: outcome.refused };
  const { alert, queue } = outcome;

  const result = emptyResult(job.cells.length);
  const expiresAt = alertEndTime(alert); // tokens live as long as the alert does
  const concurrency = deps.cellConcurrency ?? CELL_CONCURRENCY;
  const pageSize = deps.pageSize ?? PAGE_SIZE;
  const flushAt = deps.flushBatches ?? FLUSH_BATCHES;

  // Batches from many (often sparse) cells are pooled so messages fill up to
  // MAX_BATCHES_PER_MESSAGE; the send queue packs them by measured size.
  let pending: SendBatch[] = [];
  const flush = async (force: boolean): Promise<void> => {
    if (pending.length === 0 || (!force && pending.length < flushAt)) return;
    const toSend = pending;
    pending = [];
    await deps.sendQueue.enqueue(queue, toSend);
  };

  // When one cell fails, the others stop at their next step so a dead attempt does not keep
  // sending while the retry starts.
  let failed = false;
  const doCell = async (h3Cell: string): Promise<void> => {
    const buckets = new Map<number, SendBatch['recipients']>();
    const seen = new Set<string>();
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
      }
    } while (cursor);

    for (let bucket = 0; bucket < CELL_BUCKETS; bucket++) {
      const recipients = buckets.get(bucket);
      if (!recipients || recipients.length === 0) continue;
      recipients.sort((a, b) => (a.deviceId < b.deviceId ? -1 : a.deviceId > b.deviceId ? 1 : 0));
      const batch: SendBatch = {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        batchKey: `${alert.id}:${alert.relaySeverity}:${h3Cell}:${bucket}`,
        recipients,
        attempt: 0,
      };
      if (failed) return;
      for (const b of splitIfOversize(batch, batch.batchKey, deps)) {
        pending.push(b);
        result.batches++;
        result.recipients += b.recipients.length;
      }
    }
    await flush(false);
  };

  // A small pool: `concurrency` workers pull the next cell until none are left.
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < job.cells.length && !failed) {
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

  deps.logger.info('shard processed', {
    alertId: alert.id,
    shardId: job.shardId,
    queue,
    ...result,
  });
  return result;
}

/**
 * Normal batches (about 300 recipients) are far below the message limit. If a bucket is so large
 * that one batch alone would not fit in a message (a cell with 100,000+ devices), halve it
 * until it fits; the halves get deterministic keys `<batchKey>:s<path>` (path = 0/1 digits).
 * Pathological and logged. Keys inside such a bucket can shift if devices are added.
 */
function splitIfOversize(
  batch: SendBatch,
  base: string,
  deps: ShardWorkerDeps,
  path = '',
): SendBatch[] {
  // Measuring costs a full serialization, so only look at batches that could be big.
  if (batch.recipients.length <= 200) return [batch];
  const bytes = Buffer.byteLength(JSON.stringify(batch), 'utf8');
  if (bytes <= OVERSIZE_LIMIT || batch.recipients.length < 2) return [batch];
  if (path === '') {
    deps.logger.warn('splitting oversize batch', {
      batchKey: batch.batchKey,
      recipients: batch.recipients.length,
    });
  }
  const mid = Math.ceil(batch.recipients.length / 2);
  const halves = [batch.recipients.slice(0, mid), batch.recipients.slice(mid)];
  return halves.flatMap((recipients, i) =>
    splitIfOversize(
      { ...batch, batchKey: `${base}:s${path}${i}`, recipients },
      base,
      deps,
      `${path}${i}`,
    ),
  );
}
