/**
 * Read-through caching with stale-on-error, shared by the keyrings and the origin-guard secret.
 * A fresh entry is served for `ttlMs`. After that the value is re-fetched; if the fetch fails and
 * a last-good value is younger than `staleMs`, that value is served instead (logged as
 * `config.stale_value_served`, with a metric) and the next fetch waits `retryMs` so an outage is
 * not hit by every request. A fetch fails to the caller only when nothing usable was ever loaded.
 */
export interface CacheEntry<T> {
  value: T;
  /** When the value was last fetched successfully. */
  at: number;
  /** After a failed fetch: no new fetch before this time. */
  retryAt?: number;
}

export const CONFIG_METRIC_NAMESPACE = 'Relay/Config';

/** One CloudWatch EMF line: a metric (a count of 1 unless `value` is given) with a Source dimension. */
export function configMetric(
  name: string,
  source: string,
  now: number = Date.now(),
  value = 1,
): string {
  return JSON.stringify({
    _aws: {
      Timestamp: now,
      CloudWatchMetrics: [
        {
          Namespace: CONFIG_METRIC_NAMESPACE,
          Dimensions: [['Source']],
          Metrics: [{ Name: name, Unit: 'Count' }],
        },
      ],
    },
    Source: source,
    [name]: value,
  });
}

/** Class name only: an error message from Secrets Manager or SSM could name a secret. */
export const errorName = (err: unknown): string =>
  err instanceof Error ? err.name : 'UnknownError';

export function logStaleServed(source: string, ageMs: number, err: unknown, now: number): void {
  console.warn(
    JSON.stringify({
      level: 'warn',
      msg: 'config.stale_value_served',
      source,
      ageSeconds: Math.round(ageMs / 1000),
      error: errorName(err),
    }),
  );
  console.log(configMetric('StaleValueServed', source, now));
}

export interface ReadThroughOptions<T> {
  /** Short stable label for logs and the metric dimension: "keyring", "origin_secret". */
  source: string;
  entry: CacheEntry<T> | undefined;
  ttlMs: number;
  staleMs: number;
  /** Wait after a failed fetch before trying again (default 15 s). */
  retryMs?: number;
  now: () => number;
  load: () => Promise<T>;
  store: (entry: CacheEntry<T>) => void;
}

export async function readThrough<T>(o: ReadThroughOptions<T>): Promise<T> {
  const t = o.now();
  const hit = o.entry;
  if (hit && t - hit.at < o.ttlMs) return hit.value;
  const usable = hit !== undefined && t - hit.at < o.staleMs;
  // Inside the backoff after a failed fetch: serve the stale value quietly (already logged).
  if (hit && usable && hit.retryAt !== undefined && t < hit.retryAt) return hit.value;
  try {
    const value = await o.load();
    o.store({ value, at: o.now() });
    return value;
  } catch (err) {
    if (!hit || !usable) throw err;
    o.store({ ...hit, retryAt: t + (o.retryMs ?? 15_000) });
    logStaleServed(o.source, t - hit.at, err, t);
    return hit.value;
  }
}
