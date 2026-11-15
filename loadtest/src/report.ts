/**
 * Report helpers for load runs: latency percentiles, throughput and the exactly-once check.
 * Printing never writes full push tokens; programmatic results keep them for assertions.
 */
import { logJson } from './mock-common.js';

// ---------- Latency ----------

export interface LatencySummary {
  count: number;
  minMs: number;
  meanMs: number;
  p50Ms: number;
  p90Ms: number;
  p95Ms: number;
  p99Ms: number;
  maxMs: number;
}

/** Nearest-rank percentile of an ascending-sorted array (q in 0..1). */
export function percentile(sorted: ArrayLike<number>, q: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.max(1, Math.ceil(q * sorted.length));
  return sorted[Math.min(sorted.length, rank) - 1] ?? 0;
}

export function summarizeLatencies(samplesMs: ArrayLike<number>): LatencySummary {
  const sorted = Float64Array.from(samplesMs as ArrayLike<number>).sort();
  const n = sorted.length;
  let sum = 0;
  for (let i = 0; i < n; i++) sum += sorted[i] ?? 0;
  return {
    count: n,
    minMs: sorted[0] ?? 0,
    meanMs: n === 0 ? 0 : sum / n,
    p50Ms: percentile(sorted, 0.5),
    p90Ms: percentile(sorted, 0.9),
    p95Ms: percentile(sorted, 0.95),
    p99Ms: percentile(sorted, 0.99),
    maxMs: sorted[n - 1] ?? 0,
  };
}

/** Growable sample store (1M samples is 8 MB). */
export class LatencyRecorder {
  private buf = new Float64Array(1024);
  private n = 0;

  record(ms: number): void {
    if (this.n === this.buf.length) {
      const bigger = new Float64Array(this.buf.length * 2);
      bigger.set(this.buf);
      this.buf = bigger;
    }
    this.buf[this.n++] = ms;
  }

  /** Times an async operation and records its duration. */
  async time<T>(fn: () => Promise<T>): Promise<T> {
    const t0 = performance.now();
    try {
      return await fn();
    } finally {
      this.record(performance.now() - t0);
    }
  }

  get count(): number {
    return this.n;
  }

  summary(): LatencySummary {
    return summarizeLatencies(this.buf.subarray(0, this.n));
  }
}

// ---------- Throughput ----------

export interface Throughput {
  count: number;
  seconds: number;
  perSecond: number;
}

export function throughput(count: number, startMs: number, endMs: number): Throughput {
  const seconds = Math.max(0, endMs - startMs) / 1000;
  return { count, seconds, perSecond: seconds > 0 ? count / seconds : 0 };
}

// ---------- Exactly once ----------

export interface ExactlyOnceResult {
  ok: boolean;
  expected: number;
  /** Expected tokens delivered exactly once. */
  deliveredOnce: number;
  /** Expected tokens that were excused (for example answered "unregistered") and never delivered. */
  excused: number;
  missing: string[];
  duplicates: Array<{ token: string; count: number }>;
  /** Delivered tokens that were not expected. */
  unexpected: string[];
}

export interface ExactlyOnceOptions {
  /** Tokens allowed to be absent, such as ones the mock answered 410 for. Delivered twice is still a duplicate. */
  excused?: ReadonlySet<string> | ReadonlyMap<string, unknown>;
}

/** Checks that every expected token was delivered exactly once, and nothing else was. */
export function checkExactlyOnce(
  expectedTokens: Iterable<string>,
  delivered: ReadonlyMap<string, number>,
  options: ExactlyOnceOptions = {},
): ExactlyOnceResult {
  const expectedSet = new Set<string>();
  const missing: string[] = [];
  const duplicates: Array<{ token: string; count: number }> = [];
  let deliveredOnce = 0;
  let excused = 0;
  for (const token of expectedTokens) {
    if (expectedSet.has(token)) continue;
    expectedSet.add(token);
    const count = delivered.get(token) ?? 0;
    if (count === 1) deliveredOnce++;
    else if (count > 1) duplicates.push({ token, count });
    else if (options.excused?.has(token)) excused++;
    else missing.push(token);
  }
  const unexpected: string[] = [];
  for (const token of delivered.keys()) if (!expectedSet.has(token)) unexpected.push(token);
  return {
    ok: missing.length === 0 && duplicates.length === 0 && unexpected.length === 0,
    expected: expectedSet.size,
    deliveredOnce,
    excused,
    missing,
    duplicates,
    unexpected,
  };
}

// ---------- Printing ----------

/** First 8 characters only, so logs never carry a usable push token. */
export function redactToken(token: string): string {
  return `${token.slice(0, 8)}...`;
}

export function formatLatencySummary(s: LatencySummary): string {
  const f = (n: number) => n.toFixed(1);
  return (
    `n=${s.count} min=${f(s.minMs)}ms mean=${f(s.meanMs)}ms p50=${f(s.p50Ms)}ms ` +
    `p90=${f(s.p90Ms)}ms p95=${f(s.p95Ms)}ms p99=${f(s.p99Ms)}ms max=${f(s.maxMs)}ms`
  );
}

export function formatThroughput(t: Throughput): string {
  return `${t.count} in ${t.seconds.toFixed(2)}s = ${Math.round(t.perSecond)}/s`;
}

export function formatExactlyOnce(r: ExactlyOnceResult, listLimit = 10): string {
  const lines = [
    `exactly-once: ${r.ok ? 'PASS' : 'FAIL'} (expected ${r.expected}, once ${r.deliveredOnce}, ` +
      `excused ${r.excused}, missing ${r.missing.length}, duplicated ${r.duplicates.length}, ` +
      `unexpected ${r.unexpected.length})`,
  ];
  for (const t of r.missing.slice(0, listLimit)) lines.push(`  missing ${redactToken(t)}`);
  for (const d of r.duplicates.slice(0, listLimit)) {
    lines.push(`  duplicate ${redactToken(d.token)} x${d.count}`);
  }
  for (const t of r.unexpected.slice(0, listLimit)) lines.push(`  unexpected ${redactToken(t)}`);
  return lines.join('\n');
}

/** Emits one structured JSON log line for the run, with tokens redacted. */
export function logRunReport(
  event: string,
  report: { latency?: LatencySummary; throughput?: Throughput; exactlyOnce?: ExactlyOnceResult },
  listLimit = 10,
): void {
  const { exactlyOnce, ...rest } = report;
  logJson(event, {
    ...rest,
    ...(exactlyOnce
      ? {
          exactlyOnce: {
            ok: exactlyOnce.ok,
            expected: exactlyOnce.expected,
            deliveredOnce: exactlyOnce.deliveredOnce,
            excused: exactlyOnce.excused,
            missing: exactlyOnce.missing.length,
            duplicates: exactlyOnce.duplicates.length,
            unexpected: exactlyOnce.unexpected.length,
            sampleMissing: exactlyOnce.missing.slice(0, listLimit).map(redactToken),
            sampleDuplicates: exactlyOnce.duplicates
              .slice(0, listLimit)
              .map((d) => ({ token: redactToken(d.token), count: d.count })),
          },
        }
      : {}),
  });
}
