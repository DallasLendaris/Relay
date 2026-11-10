/** Wraps a document client to count commands, conditional-check failures and sampled latencies. */
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';

export type DocLike = { send(command: never): Promise<unknown> };

export interface DocStats {
  /** Commands sent, by command class name. */
  commands: Record<string, number>;
  /** ConditionalCheckFailedException, by command class name. */
  conditionalFailures: Record<string, number>;
  /** TransactionCanceledException (conflicts or already-counted markers). */
  transactionCancels: number;
  /** Other errors thrown. */
  errors: number;
  /** Latency samples in ms (1 in `sampleEvery` calls). */
  latenciesMs: number[];
}

export function emptyStats(): DocStats {
  return {
    commands: {},
    conditionalFailures: {},
    transactionCancels: 0,
    errors: 0,
    latenciesMs: [],
  };
}

/** Returns a client that behaves like `doc` and records into `stats`. */
export function instrumentDoc<T extends DocLike>(doc: T, stats: DocStats, sampleEvery = 50): T {
  let n = 0;
  const send = async (command: never): Promise<unknown> => {
    const name = (command as { constructor: { name: string } }).constructor.name;
    stats.commands[name] = (stats.commands[name] ?? 0) + 1;
    const sample = ++n % sampleEvery === 0;
    const t0 = sample ? performance.now() : 0;
    try {
      return await doc.send(command);
    } catch (e) {
      if (e instanceof ConditionalCheckFailedException) {
        stats.conditionalFailures[name] = (stats.conditionalFailures[name] ?? 0) + 1;
      } else if ((e as { name?: string }).name === 'TransactionCanceledException') {
        stats.transactionCancels++;
      } else stats.errors++;
      throw e;
    } finally {
      if (sample) stats.latenciesMs.push(performance.now() - t0);
    }
  };
  return { send } as unknown as T;
}

export function sumRecord(r: Record<string, number>): number {
  return Object.values(r).reduce((a, b) => a + b, 0);
}

export function mergeStats(all: DocStats[]): DocStats {
  const out = emptyStats();
  for (const s of all) {
    for (const [k, v] of Object.entries(s.commands)) out.commands[k] = (out.commands[k] ?? 0) + v;
    for (const [k, v] of Object.entries(s.conditionalFailures)) {
      out.conditionalFailures[k] = (out.conditionalFailures[k] ?? 0) + v;
    }
    out.transactionCancels += s.transactionCancels;
    out.errors += s.errors;
    for (const l of s.latenciesMs) out.latenciesMs.push(l);
  }
  return out;
}

/** Percentile (0..100) of a list of numbers, by nearest rank; 0 for an empty list. */
export function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
  return sorted[idx] as number;
}
