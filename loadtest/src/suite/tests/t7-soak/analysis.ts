/** Pure helpers for the t7 verdicts: least-squares trends of heap after GC, event-loop stalls, CSV. */
import type { Sample } from './common.js';

export interface Fit {
  slope: number;
  intercept: number;
}

/** Ordinary least squares y = intercept + slope * x. Flat line for fewer than two distinct x. */
export function leastSquares(xs: number[], ys: number[]): Fit {
  const n = Math.min(xs.length, ys.length);
  if (n === 0) return { slope: 0, intercept: 0 };
  let sx = 0;
  let sy = 0;
  for (let i = 0; i < n; i++) {
    sx += xs[i] ?? 0;
    sy += ys[i] ?? 0;
  }
  const mx = sx / n;
  const my = sy / n;
  let sxx = 0;
  let sxy = 0;
  for (let i = 0; i < n; i++) {
    const dx = (xs[i] ?? 0) - mx;
    sxx += dx * dx;
    sxy += dx * ((ys[i] ?? 0) - my);
  }
  if (sxx === 0) return { slope: 0, intercept: my };
  const slope = sxy / sxx;
  return { slope, intercept: my - slope * mx };
}

export interface LeakReport {
  proc: string;
  samples: number;
  /** Slope of heapUsed after GC over the last third of the run, MB per hour (what the verdict uses). */
  slopeMbPerHour: number;
  /** Fitted growth over the last third, MB and % of the fitted heap where the last third starts. */
  lastThirdGrowthMb: number;
  lastThirdGrowthPct: number;
  /** Fitted heap at the mark and at the end, over [mark, end] (reported; not the verdict). */
  heapAtMarkMb: number;
  heapAtEndMb: number;
  growthMb: number;
  growthPct: number;
  /**
   * True when the heap still grows over the last third of the run by more than the percentage limit
   * and the absolute floor: a cache or stand-in that filled up and plateaued is not a leak.
   */
  leak: boolean;
  /** False for a process that is test code, not Relay (reported, but left out of the verdict). */
  counted: boolean;
}

export interface LeakOptions {
  /** Run length, seconds. */
  durationS: number;
  /** The "15-minute mark": growth is measured from here to the end. */
  markS: number;
  limitPct: number;
  /** Ignore growth smaller than this (a 20% rise of a 15 MB heap is not a leak). */
  floorMb: number;
  /** Processes that do not count toward the verdict (the load driver is test code). Default none. */
  uncounted?: readonly string[];
}

export function leakReport(proc: string, rows: Sample[], t0: number, o: LeakOptions): LeakReport {
  const xs = rows.map((r) => (r.t - t0) / 1000);
  const ys = rows.map((r) => r.heapMb);
  const thirdStart = (o.durationS * 2) / 3;
  const last = xs.map((x, i) => [x, ys[i] ?? 0] as const).filter(([x]) => x >= thirdStart);
  const fitLast = leastSquares(
    last.map((p) => p[0]),
    last.map((p) => p[1]),
  );
  const lastEnd = Math.max(thirdStart, xs[xs.length - 1] ?? thirdStart);
  const lastGrowth = fitLast.slope * (lastEnd - thirdStart);
  const lastStartHeap = fitLast.intercept + fitLast.slope * thirdStart;
  const lastPct = lastStartHeap > 0 ? (lastGrowth / lastStartHeap) * 100 : 0;
  const after = xs.map((x, i) => [x, ys[i] ?? 0] as const).filter(([x]) => x >= o.markS);
  const fit = leastSquares(
    after.map((p) => p[0]),
    after.map((p) => p[1]),
  );
  const endX = Math.max(o.markS, xs[xs.length - 1] ?? o.markS);
  const atMark = fit.intercept + fit.slope * o.markS;
  const atEnd = fit.intercept + fit.slope * endX;
  const growth = atEnd - atMark;
  const pct = atMark > 0 ? (growth / atMark) * 100 : 0;
  return {
    proc,
    samples: rows.length,
    slopeMbPerHour: fitLast.slope * 3600,
    lastThirdGrowthMb: lastGrowth,
    lastThirdGrowthPct: lastPct,
    heapAtMarkMb: atMark,
    heapAtEndMb: atEnd,
    growthMb: growth,
    growthPct: pct,
    leak: last.length >= 3 && lastPct > o.limitPct && lastGrowth > o.floorMb,
    counted: !(o.uncounted ?? []).includes(proc),
  };
}

export interface LoopReport {
  proc: string;
  maxP99Ms: number;
  maxStallMs: number;
  /** Seconds covered by samples whose p99 was over the limit. */
  overLimitS: number;
}

export function loopReport(
  proc: string,
  rows: Sample[],
  limitMs: number,
  sampleS: number,
): LoopReport {
  let maxP99 = 0;
  let maxStall = 0;
  let over = 0;
  for (const r of rows) {
    maxP99 = Math.max(maxP99, r.eldP99Ms);
    maxStall = Math.max(maxStall, r.eldMaxMs);
    if (r.eldP99Ms > limitMs) over += sampleS;
  }
  return { proc, maxP99Ms: maxP99, maxStallMs: maxStall, overLimitS: over };
}

export function samplesCsv(rows: Sample[], t0: number): string {
  const out = ['t_s,process,rss_mb,heap_after_gc_mb,loop_delay_p99_ms,loop_delay_max_ms,caches'];
  for (const r of [...rows].sort((a, b) => a.t - b.t)) {
    const caches = Object.entries(r.caches)
      .map(([k, v]) => `${k}=${v}`)
      .join(';');
    out.push(
      [
        ((r.t - t0) / 1000).toFixed(1),
        r.proc,
        r.rssMb,
        r.heapMb,
        r.eldP99Ms,
        r.eldMaxMs,
        caches,
      ].join(','),
    );
  }
  return `${out.join('\n')}\n`;
}

/** First, middle and last value of a series (for the report table). */
export function firstMiddleLast(values: number[]): [number, number, number] {
  if (values.length === 0) return [0, 0, 0];
  return [
    values[0] ?? 0,
    values[Math.floor((values.length - 1) / 2)] ?? 0,
    values[values.length - 1] ?? 0,
  ];
}
