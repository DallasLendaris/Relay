/**
 * Verification helpers shared by t8 and t9: exactly-once over sorted 64-bit token-hash arrays (like
 * t2's, but keeping the offending hashes so t9 can look them up), the per-token multiplicity check
 * against what the mocks recorded, per-alert time lines, and small statistics.
 */
import { readFileSync } from 'node:fs';
import { sortedUnique } from '../t2-fanout/verify.js';

export function readWords(file: string): BigUint64Array {
  const b = readFileSync(file);
  // Copy: the Buffer may sit in Node's shared pool at an unaligned offset.
  return new BigUint64Array(b.buffer.slice(b.byteOffset, b.byteOffset + b.length));
}

export function readDoubles(file: string): Float64Array {
  const b = readFileSync(file);
  return new Float64Array(b.buffer.slice(b.byteOffset, b.byteOffset + b.length));
}

export function concatWords(parts: BigUint64Array[]): BigUint64Array {
  const out = new BigUint64Array(parts.reduce((s, p) => s + p.length, 0));
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

const CAP = 200_000;

export interface Detailed {
  expected: number;
  deliveredOnce: number;
  /** Expected devices with no accepted push whose token the mock answered "unregistered". */
  excusedDead: number;
  missed: number;
  duplicateDevices: number;
  duplicatePushes: number;
  /** Accepted pushes to a token that is not expected for this alert. */
  outside: number;
  totalDeliveries: number;
  /** The hashes behind the counts (capped at 200,000 each). */
  missedHashes: bigint[];
  duplicateHashes: bigint[];
  outsideHashes: bigint[];
}

/**
 * @param expected sorted unique token hashes the alert must reach
 * @param delivered the multiset of accepted pushes (sorted here)
 * @param dead token hashes answered "unregistered" (any order, may repeat)
 */
export function exactlyOnceDetailed(
  expected: BigUint64Array,
  delivered: BigUint64Array,
  dead: BigUint64Array,
): Detailed {
  delivered.sort();
  const deadSet = sortedUnique(dead);
  const r: Detailed = {
    expected: expected.length,
    deliveredOnce: 0,
    excusedDead: 0,
    missed: 0,
    duplicateDevices: 0,
    duplicatePushes: 0,
    outside: 0,
    totalDeliveries: delivered.length,
    missedHashes: [],
    duplicateHashes: [],
    outsideHashes: [],
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
      else {
        r.missed++;
        if (r.missedHashes.length < CAP) r.missedHashes.push(ev as bigint);
      }
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
        if (r.duplicateHashes.length < CAP) r.duplicateHashes.push(dv);
      }
    } else {
      r.outside += count;
      if (r.outsideHashes.length < CAP) r.outsideHashes.push(dv);
    }
  }
  return r;
}

export interface TokenLevel {
  /** Tokens with more accepted pushes at the mocks than expected, not excused by a lost answer. */
  extra: number;
  /** Tokens with fewer accepted pushes than expected, not excused by a dead answer. */
  missing: number;
  /** Extra pushes explained by a lost response (the sender retried a push that had been accepted). */
  excusedLost: number;
  /** Missing pushes explained by an "unregistered" answer. */
  excusedDead: number;
  tokens: number;
}

/**
 * Per-token check of the push counts the MOCKS saw, independent of the senders' bookkeeping:
 * a token's accepted pushes must equal the number of alerts that cover its device. `expected` and
 * `delivered` are sorted multisets of token hashes; `dead` and `lost` are sorted unique sets.
 */
export function tokenLevelCheck(
  expected: BigUint64Array,
  delivered: BigUint64Array,
  dead: BigUint64Array,
  lost: BigUint64Array,
): TokenLevel {
  const r: TokenLevel = { extra: 0, missing: 0, excusedLost: 0, excusedDead: 0, tokens: 0 };
  const has = (set: BigUint64Array, v: bigint, cursor: { i: number }): boolean => {
    while (cursor.i < set.length && (set[cursor.i] as bigint) < v) cursor.i++;
    return cursor.i < set.length && set[cursor.i] === v;
  };
  const dc = { i: 0 };
  const lc = { i: 0 };
  let e = 0;
  let d = 0;
  while (e < expected.length || d < delivered.length) {
    const ev = e < expected.length ? (expected[e] as bigint) : undefined;
    const dv = d < delivered.length ? (delivered[d] as bigint) : undefined;
    const h = ev === undefined ? (dv as bigint) : dv === undefined ? ev : ev < dv ? ev : dv;
    let ec = 0;
    while (e < expected.length && expected[e] === h) {
      ec++;
      e++;
    }
    let dn = 0;
    while (d < delivered.length && delivered[d] === h) {
      dn++;
      d++;
    }
    r.tokens++;
    if (dn > ec) {
      if (has(lost, h, lc)) r.excusedLost += dn - ec;
      else r.extra += dn - ec;
    } else if (dn < ec) {
      if (has(dead, h, dc)) r.excusedDead += ec - dn;
      else r.missing += ec - dn;
    }
  }
  return r;
}

// ---------- time lines ----------

export interface AlertTimeline {
  total: number;
  /** [epoch ms of the slot start, accepted pushes in it], sorted. */
  slots: Array<[number, number]>;
  slotMs: number;
}

export function mergeTimelines(
  perSender: Array<Array<Array<[number, number]>>>,
  alerts: number,
  slotMs: number,
): AlertTimeline[] {
  const out: AlertTimeline[] = [];
  for (let a = 0; a < alerts; a++) {
    const m = new Map<number, number>();
    for (const s of perSender)
      for (const [slot, n] of s[a] ?? []) m.set(slot, (m.get(slot) ?? 0) + n);
    const slots = [...m]
      .sort((x, y) => x[0] - y[0])
      .map(([s, n]): [number, number] => [s * slotMs, n]);
    out.push({ total: slots.reduce((acc, [, n]) => acc + n, 0), slots, slotMs });
  }
  return out;
}

/** Epoch ms by which a fraction q of the alert's pushes had been accepted (end of that slot); 0 if none. */
export function timeAtFraction(tl: AlertTimeline, q: number): number {
  if (tl.total === 0) return 0;
  const want = Math.max(1, Math.ceil(q * tl.total));
  let run = 0;
  for (const [t, n] of tl.slots) {
    run += n;
    if (run >= want) return t + tl.slotMs;
  }
  return 0;
}

/** Fraction of the alert's pushes accepted by epoch ms `t`. */
export function fractionAt(tl: AlertTimeline, t: number): number {
  if (tl.total === 0) return 1;
  let run = 0;
  for (const [s, n] of tl.slots) {
    if (s + tl.slotMs > t) break;
    run += n;
  }
  return run / tl.total;
}

export function lastOf(tl: AlertTimeline): number {
  const last = tl.slots[tl.slots.length - 1];
  return last ? last[0] + tl.slotMs : 0;
}

export function quantile(values: number[], q: number): number {
  if (values.length === 0) return 0;
  const s = [...values].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.max(0, Math.ceil(q * s.length) - 1))] as number;
}

export const round = (n: number, d = 0): number => Math.round(n * 10 ** d) / 10 ** d;
