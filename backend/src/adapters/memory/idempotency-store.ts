import type { Clock, IdempotencyStore } from '../../ports.js';

interface Entry {
  state: 'leased' | 'sent';
  /** Lease end while leased; remembered-until time once sent. */
  until: number;
}

/** In-memory IdempotencyStore with leases. Entries stop counting once `until` has passed. */
export class MemoryIdempotencyStore implements IdempotencyStore {
  private readonly entries = new Map<string, Entry>();

  constructor(private readonly clock: Clock = { now: () => new Date() }) {}

  async claim(key: string, leaseUntil: Date): Promise<'claimed' | 'sent' | 'leased'> {
    const existing = this.entries.get(key);
    if (existing && existing.until > this.clock.now().getTime()) return existing.state;
    this.entries.set(key, { state: 'leased', until: leaseUntil.getTime() });
    return 'claimed';
  }

  async markSent(key: string, expiresAt: Date): Promise<void> {
    this.entries.set(key, { state: 'sent', until: expiresAt.getTime() });
  }

  async release(key: string): Promise<void> {
    this.entries.delete(key);
  }
}
