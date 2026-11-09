import { MAX_BATCHES_PER_MESSAGE, type SendBatch } from '../contracts.js';

/** SQS allows 256 KB per message; stay well under it so envelope overhead never tips us over. */
export const MAX_MESSAGE_BYTES = 240 * 1024;

/**
 * Wire format of a send-queue message: a JSON array of SendBatch (1 to MAX_BATCHES_PER_MESSAGE
 * entries). The sender service parses the body with JSON.parse and iterates the array.
 */
export function serializeMessage(batches: SendBatch[]): string {
  return JSON.stringify(batches);
}

/**
 * Packs batches, in order, into messages of at most MAX_BATCHES_PER_MESSAGE batches and at most
 * MAX_MESSAGE_BYTES serialized. The size is measured, not estimated. A single batch that is
 * larger than the limit on its own gets a message to itself (the shard worker splits those
 * before they get here, so this should not happen).
 */
export function packBatches(batches: SendBatch[]): SendBatch[][] {
  const messages: SendBatch[][] = [];
  let current: SendBatch[] = [];
  let bytes = 2; // the surrounding [ ]
  for (const batch of batches) {
    const size = Buffer.byteLength(JSON.stringify(batch), 'utf8');
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
  }
  if (current.length > 0) messages.push(current);
  return messages;
}
