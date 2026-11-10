import type { Alert } from '@relay/shared';

/**
 * Per-warm-Lambda cache of alerts for the planner and shard workers. Without it, the planner and
 * every one of a few hundred shard workers reload the whole alert from Aurora (up to 15,000
 * cells per page) within seconds of each other. Entries live 10 s, keyed by alertId + sourceId,
 * so a cancel or an update is seen almost at once. Concurrent reads share one call; misses and
 * failures are not cached. Signature checks are not cached: they run on every call, so a key
 * refresh after an 'unsigned' result takes effect immediately.
 *
 * Memory: a statewide alert with all its H3 cells is ~1.5 MB, so the entry count is capped low
 * (MAX_CACHED_READER_ALERTS) and entries older than the TTL are swept on every insert (the map is
 * kept in insertion order, so a sweep stops at the first live entry).
 */
export const MAX_CACHED_READER_ALERTS = 32;

export class CachedAlertReader {
  private readonly cache = new Map<string, { at: number; value: Promise<Alert | null> }>();

  constructor(
    private readonly inner: { get(id: string): Promise<Alert | null> },
    private readonly ttlMs = 10_000,
    private readonly now: () => number = Date.now,
    private readonly maxEntries = MAX_CACHED_READER_ALERTS,
  ) {}

  get(id: string, sourceId = ''): Promise<Alert | null> {
    const key = `${id}\u0000${sourceId}`;
    const hit = this.cache.get(key);
    if (hit && this.now() - hit.at < this.ttlMs) return hit.value;
    const value = this.inner.get(id);
    // Delete first so a refreshed key moves to the back and the map stays ordered by `at`.
    this.cache.delete(key);
    this.sweep();
    this.cache.set(key, { at: this.now(), value });
    const drop = (): void => {
      if (this.cache.get(key)?.value === value) this.cache.delete(key);
    };
    value.then((a) => a === null && drop(), drop);
    while (this.cache.size > this.maxEntries) {
      this.cache.delete(this.cache.keys().next().value as string);
    }
    return value;
  }

  /** Drops entries older than the TTL, oldest first. */
  private sweep(): void {
    const t = this.now();
    for (const [key, e] of this.cache) {
      if (t - e.at < this.ttlMs) break;
      this.cache.delete(key);
    }
  }
}
