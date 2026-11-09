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
