import { createHash } from 'node:crypto';

/**
 * Compact sets of device ids, stored on idempotency items so late join can tell whether a planned
 * batch carried a device (G5). A set is the sorted, de-duplicated prefixes of sha256(deviceId),
 * concatenated: never the ids themselves.
 *
 * Two widths, because the two sets are used differently:
 *  - SENT_HASH_BYTES (8): the sender's record of the batch it actually sent. Late join SKIPS a device
 *    found in it, so a false positive would be a missed alert: with 64-bit hashes and at most a few
 *    hundred recipients per batch the chance is about 1e-17 per lookup.
 *  - PLAN_HASH_BYTES (4): the shard worker's record of whom it put in a cell's batches. A device
 *    found in it is only WAITED for (until the sender's exact record exists), never skipped, so a
 *    false positive (about n / 2^32: 5e-6 for a 20,000-device cell) costs a short wait, no miss.
 */
export const SENT_HASH_BYTES = 8;
export const PLAN_HASH_BYTES = 4;

/**
 * Most bytes one set may take: DynamoDB items are capped at 400 KB, and the key and the other
 * attributes need room. 320 KB is 40,960 sent hashes (a batch is at most about 800 recipients) or
 * 81,920 plan hashes (a normal cell is at most about 20,000; a dense part about 1/16 of its cell,
 * so a cell of up to about 1.3 million devices). Over it, no set is stored and the caller logs it;
 * late join then falls back to waiting or to its timestamp rule (latejoin.ts), never to a skip.
 */
export const MAX_SET_BYTES = 320 * 1024;

const prefixOf = (deviceId: string, bytes: number): Buffer =>
  createHash('sha256').update(deviceId).digest().subarray(0, bytes);

/** The set of these ids, or undefined when it would exceed MAX_SET_BYTES. */
export function encodeMemberSet(
  deviceIds: Iterable<string>,
  bytes: number,
  maxBytes: number = MAX_SET_BYTES,
): Uint8Array | undefined {
  const unique = new Set(deviceIds);
  if (unique.size * bytes > maxBytes) return undefined;
  const hashes = [...unique].map((id) => prefixOf(id, bytes)).sort(Buffer.compare);
  const out = Buffer.alloc(hashes.length * bytes);
  let n = 0;
  for (const h of hashes) {
    // Two ids with the same prefix are one entry (membership is all that is asked).
    if (n > 0 && h.equals(out.subarray((n - 1) * bytes, n * bytes))) continue;
    h.copy(out, n * bytes);
    n++;
  }
  return new Uint8Array(out.subarray(0, n * bytes));
}

/** Whether `deviceId` is in a set made by encodeMemberSet with the same width (binary search). */
export function hasMember(set: Uint8Array, deviceId: string, bytes: number): boolean {
  if (set.length % bytes !== 0) return false; // malformed: never "present"
  const want = prefixOf(deviceId, bytes);
  const buf = Buffer.from(set.buffer, set.byteOffset, set.byteLength);
  let lo = 0;
  let hi = set.length / bytes - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const c = Buffer.compare(buf.subarray(mid * bytes, (mid + 1) * bytes), want);
    if (c === 0) return true;
    if (c < 0) lo = mid + 1;
    else hi = mid - 1;
  }
  return false;
}
