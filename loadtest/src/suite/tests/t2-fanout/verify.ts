/**
 * Exactly-once accounting without holding millions of token strings in one process: every push
 * token is reduced to a 64-bit hash (two independent 32-bit string hashes), sets are sorted
 * BigUint64Arrays and compared by merging.
 */

/** Writes the 64-bit hash of `token` into `out` at index `i` (as two uint32 words). */
export function hashInto(token: string, out: Uint32Array, i: number): void {
  let h1 = 0x811c9dc5;
  let h2 = 0x9747b28c;
  for (let k = 0; k < token.length; k++) {
    const c = token.charCodeAt(k);
    h1 = Math.imul(h1 ^ c, 0x01000193);
    h2 = Math.imul(h2 ^ c, 0x5bd1e995);
    h2 ^= h2 >>> 15;
  }
  // Little-endian: the low word first, so the BigUint64 view reads (h1 << 32) | h2.
  out[2 * i] = h2 >>> 0;
  out[2 * i + 1] = h1 >>> 0;
}

/** Growable list of token hashes. */
export class HashList {
  private words = new Uint32Array(2048);
  n = 0;

  add(token: string): void {
    if (2 * (this.n + 1) > this.words.length) {
      const bigger = new Uint32Array(this.words.length * 2);
      bigger.set(this.words);
      this.words = bigger;
    }
    hashInto(token, this.words, this.n++);
  }

  /** A compact copy, ready to send over IPC or write to disk. */
  toArray(): BigUint64Array {
    return new BigUint64Array(this.words.buffer.slice(0, this.n * 8));
  }
}

export function sortedUnique(a: BigUint64Array): BigUint64Array {
  a.sort();
  let w = 0;
  for (let r = 0; r < a.length; r++) {
    if (r === 0 || a[r] !== a[r - 1]) a[w++] = a[r] as bigint;
  }
  return a.subarray(0, w);
}

export function concat(parts: BigUint64Array[]): BigUint64Array {
  const out = new BigUint64Array(parts.reduce((s, p) => s + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

export interface ExactlyOnce {
  expected: number;
  deliveredOnce: number;
  /** Expected devices never delivered whose token the mock answered "unregistered" (dead). */
  excusedDead: number;
  missed: number;
  /** Extra deliveries (count - 1 summed) to expected devices. */
  duplicatePushes: number;
  duplicateDevices: number;
  /** Distinct tokens delivered that are not in the alert area (or not in the seed at all). */
  outsideArea: number;
  totalDeliveries: number;
}

/**
 * @param expected sorted unique hashes of every device with a place in the alert area
 * @param delivered one hash per accepted push (a multiset), any order
 * @param dead hashes answered "unregistered", any order
 */
export function exactlyOnce(
  expected: BigUint64Array,
  delivered: BigUint64Array,
  dead: BigUint64Array,
): ExactlyOnce {
  delivered.sort();
  const deadSet = sortedUnique(dead);
  const r: ExactlyOnce = {
    expected: expected.length,
    deliveredOnce: 0,
    excusedDead: 0,
    missed: 0,
    duplicatePushes: 0,
    duplicateDevices: 0,
    outsideArea: 0,
    totalDeliveries: delivered.length,
  };
  let e = 0;
  let d = 0;
  let x = 0;
  const isDead = (v: bigint): boolean => {
    while (x < deadSet.length && (deadSet[x] as bigint) < v) x++;
    return x < deadSet.length && deadSet[x] === v;
  };
  while (e < expected.length || d < delivered.length) {
    const ev = e < expected.length ? (expected[e] as bigint) : undefined;
    const dv = d < delivered.length ? (delivered[d] as bigint) : undefined;
    if (dv === undefined || (ev !== undefined && ev < dv)) {
      if (isDead(ev as bigint)) r.excusedDead++;
      else r.missed++;
      e++;
      continue;
    }
    let count = 0;
    while (d < delivered.length && delivered[d] === dv) {
      count++;
      d++;
    }
    if (ev === dv) {
      e++;
      if (count === 1) r.deliveredOnce++;
      else {
        r.duplicateDevices++;
        r.duplicatePushes += count - 1;
      }
    } else r.outsideArea++;
  }
  return r;
}

// ---------- mergeable latency histogram ----------

const RATIO = Math.log(1.04);
export const HIST_BUCKETS = 512;

export function histAdd(h: Uint32Array, ms: number): void {
  const b = Math.min(
    HIST_BUCKETS - 1,
    Math.max(0, Math.floor(Math.log(1 + Math.max(0, ms)) / RATIO)),
  );
  h[b] = (h[b] ?? 0) + 1;
}

export function histMerge(into: Uint32Array, from: ArrayLike<number>): void {
  for (let i = 0; i < HIST_BUCKETS; i++) into[i] = (into[i] ?? 0) + (from[i] ?? 0);
}

/** Upper edge of the bucket holding quantile q, ms. */
export function histQuantile(h: Uint32Array, q: number): number {
  let total = 0;
  for (const v of h) total += v;
  if (total === 0) return 0;
  const want = Math.max(1, Math.ceil(q * total));
  let run = 0;
  for (let i = 0; i < HIST_BUCKETS; i++) {
    run += h[i] ?? 0;
    if (run >= want) return Math.exp((i + 1) * RATIO) - 1;
  }
  return Math.exp(HIST_BUCKETS * RATIO) - 1;
}
