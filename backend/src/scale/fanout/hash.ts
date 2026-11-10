import { CELL_BUCKETS } from '../contracts.js';

/**
 * FNV-1a, 32-bit, over the UTF-8 bytes of the string. Written out here (not a library) because
 * batch keys depend on it: if this function ever changed, every device would move to a different
 * bucket and an in-flight alert could be double-sent or skipped on re-plan. hash.test.ts pins
 * known values so a change cannot slip through.
 */
export function fnv1a32(input: string): number {
  let hash = 0x811c9dc5;
  for (const byte of Buffer.from(input, 'utf8')) {
    hash ^= byte;
    // 32-bit multiply by the FNV prime 16777619 without losing precision.
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

/** The stable bucket (0 to CELL_BUCKETS - 1) of a device within any cell. */
export function bucketFor(deviceId: string): number {
  return fnv1a32(deviceId) % CELL_BUCKETS;
}

/**
 * The stable part (0 to DENSE_PARTS - 1 = 15) of a device in a dense cell: its id's first character
 * read as a lowercase hex digit. Two properties matter:
 *  - balanced for UUIDs (lowercase hex, as Postgres prints them): 16 equal parts;
 *  - monotone (never decreasing) in the cell index's sort order (sk = `${deviceId}#${placeId}`,
 *    compared by code unit), for ANY id: other characters map to the nearest hex digit below them.
 *    So a cell index page stream finishes part p before part p + 1 starts, and the shard worker
 *    can emit part p and free it. Non-hex ids still work, only less evenly.
 * Changing this moves devices between dense batch keys (double send on re-plan): hash.test.ts pins it.
 */
export function densePartFor(deviceId: string): number {
  const c = deviceId.charCodeAt(0);
  if (!(c >= 0x30)) return 0; // empty, or below '0'
  if (c <= 0x39) return c - 0x30; // '0'-'9'
  if (c < 0x61) return 9; // between '9' and 'a' (includes upper case)
  if (c <= 0x66) return c - 0x61 + 10; // 'a'-'f'
  return 15;
}
