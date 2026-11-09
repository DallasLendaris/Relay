import type { Alert } from '@relay/shared';
import type { AlertStore } from '../../ports.js';

/**
 * A few seconds of caching in front of the alert store. One alert is read by thousands of
 * batches in a minute; each batch still "re-reads" it, but they share one database read per
 * TTL, so a million-recipient alert does not hammer Aurora. Concurrent reads share one call.
 * A change to the alert (cancel, expire) is seen within the TTL. Misses are not cached.
 */
export class CachedAlertStore implements Pick<AlertStore, 'get'> {
  private readonly cache = new Map<string, { at: number; value: Promise<Alert | null> }>();

  constructor(
    private readonly inner: Pick<AlertStore, 'get'>,
    private readonly ttlMs = 5_000,
    private readonly now: () => number = Date.now,
  ) {}

  get(id: string): Promise<Alert | null> {
    const hit = this.cache.get(id);
    if (hit && this.now() - hit.at < this.ttlMs) return hit.value;
    const value = this.inner.get(id);
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
    if (this.cache.size > 1000) this.cache.delete(this.cache.keys().next().value as string);
    return value;
  }

  invalidate(id: string): void {
    this.cache.delete(id);
  }
}
