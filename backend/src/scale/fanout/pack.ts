import { MAX_BATCHES_PER_MESSAGE, type SendBatch } from '../contracts.js';

/** SQS allows 256 KB per message; stay well under it so envelope overhead never tips us over. */
export const MAX_MESSAGE_BYTES = 240 * 1024;

/** The hard SQS limit on one message body (256 KiB). */
export const SQS_MAX_MESSAGE_BYTES = 256 * 1024;

/** A single recipient is too large to be sent in any SQS message (cannot be split further). */
export class OversizeBatchError extends Error {
  constructor(
    readonly batchKey: string,
    readonly bytes: number,
    readonly limit: number,
  ) {
    super(
      `send batch ${batchKey} is ${bytes} bytes with one recipient, over the ${limit} byte message limit`,
    );
    this.name = 'OversizeBatchError';
  }
}

const sizeOf = (batch: SendBatch): number => Buffer.byteLength(JSON.stringify(batch), 'utf8');

/**
 * Wire format of a send-queue message: a JSON array of SendBatch (1 to MAX_BATCHES_PER_MESSAGE
 * entries). The sender service parses the body with JSON.parse and iterates the array.
 */
export function serializeMessage(batches: SendBatch[]): string {
  return JSON.stringify(batches);
}

/**
 * Halves `batch` (recipients in their current order) until every piece serializes to at most
 * `maxBytes`. The pieces get keys `<batchKey>:s<path>` (path of 0/1 digits), deterministic for the
 * same recipient list. This is the last resort only: the keys move if devices are added between
 * plans, so the shard worker's stable buckets and dense parts keep normal batches far below the
 * limit, and this runs only for pathological sizes. Throws OversizeBatchError if one recipient
 * alone is too big.
 */
export function splitToFit(batch: SendBatch, maxBytes: number): SendBatch[] {
  const go = (b: SendBatch, path: string): SendBatch[] => {
    const bytes = sizeOf(b);
    if (bytes <= maxBytes) return [b];
    if (b.recipients.length < 2) throw new OversizeBatchError(batch.batchKey, bytes, maxBytes);
    const mid = Math.ceil(b.recipients.length / 2);
    return [b.recipients.slice(0, mid), b.recipients.slice(mid)].flatMap((recipients, i) =>
      go({ ...b, batchKey: `${batch.batchKey}:s${path}${i}`, recipients }, `${path}${i}`),
    );
  };
  return go(batch, '');
}

/** Headroom for the message's `[ ]` around a single batch. */
const SINGLE_BATCH_LIMIT = MAX_MESSAGE_BYTES - 2;

/**
 * Packs batches, in order, into messages of at most MAX_BATCHES_PER_MESSAGE batches and at most
 * MAX_MESSAGE_BYTES serialized. The size is measured, not estimated. A single batch that is larger
 * than a message on its own is split with splitToFit rather than sent to fail at SQS: the shard
 * worker keeps batches under BATCH_BYTE_BUDGET, so this is a guard for anything that does not,
 * never the normal path.
 */
export function packBatches(batches: SendBatch[]): SendBatch[][] {
  const messages: SendBatch[][] = [];
  let current: SendBatch[] = [];
  let bytes = 2; // the surrounding [ ]
  const add = (batch: SendBatch, size: number): void => {
    const comma = current.length > 0 ? 1 : 0;
    if (
      current.length > 0 &&
      (current.length >= MAX_BATCHES_PER_MESSAGE || bytes + comma + size > MAX_MESSAGE_BYTES)
    ) {
      messages.push(current);
      current = [];
      bytes = 2;
    }
    bytes += size + (current.length > 0 ? 1 : 0);
    current.push(batch);
  };
  for (const batch of batches) {
    const size = sizeOf(batch);
    if (size <= SINGLE_BATCH_LIMIT) add(batch, size);
    else for (const piece of splitToFit(batch, SINGLE_BATCH_LIMIT)) add(piece, sizeOf(piece));
  }
  if (current.length > 0) messages.push(current);
  return messages;
}

/** Throws a clear error for a body SQS would reject as too large (instead of SQS's own error). */
export function assertMessageFits(body: string): void {
  const bytes = Buffer.byteLength(body, 'utf8');
  if (bytes > SQS_MAX_MESSAGE_BYTES) {
    throw new Error(
      `SQS message of ${bytes} bytes is over the ${SQS_MAX_MESSAGE_BYTES} byte limit; split it before sending`,
    );
  }
}
