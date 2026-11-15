import type { Alert } from '@relay/shared';
import type { AlertStore } from '../../ports.js';

/**
 * A few seconds of caching in front of the alert store. One alert is read by thousands of
 * batches in a minute; each batch still "re-reads" it, but they share one database read per
 * TTL, so a million-recipient alert does not hammer Aurora. Concurrent reads share one call.
 * A change to the alert (cancel, expire) is seen within the TTL. Misses are not cached.
 *
 * Memory: a statewide alert with all its H3 cells is ~1.5 MB, so the entry count is capped low
 * (MAX_CACHED_ALERTS) and entries older than the TTL are swept on every insert (the map is kept
 * in insertion order, so a sweep stops at the first live entry and costs only what it frees).
 */
export const MAX_CACHED_ALERTS = 64;

export class CachedAlertStore implements Pick<AlertStore, 'get'> {
  private readonly cache = new Map<string, { at: number; value: Promise<Alert | null> }>();

  constructor(
    private readonly inner: Pick<AlertStore, 'get'>,
    private readonly ttlMs = 5_000,
    private readonly now: () => number = Date.now,
    private readonly maxEntries = MAX_CACHED_ALERTS,
  ) {}

  get(id: string): Promise<Alert | null> {
    const hit = this.cache.get(id);
    if (hit && this.now() - hit.at < this.ttlMs) return hit.value;
    const value = this.inner.get(id);
    // Delete first so a refreshed key moves to the back and the map stays ordered by `at`.
    this.cache.delete(id);
    this.sweep();
    this.cache.set(id, { at: this.now(), value });
    // Never keep failures or misses: the next caller should ask again.
    value.then(
      (a) => {
        if (a === null && this.cache.get(id)?.value === value) this.cache.delete(id);
      },
      () => {
        if (this.cache.get(id)?.value === value) this.cache.delete(id);
      },
    );
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

  invalidate(id: string): void {
    this.cache.delete(id);
  }
}
