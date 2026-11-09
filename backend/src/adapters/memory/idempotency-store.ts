import type { Clock, IdempotencyStore } from '../../ports.js';

/** In-memory IdempotencyStore. A claim stops counting once its expiry has passed. */
export class MemoryIdempotencyStore implements IdempotencyStore {
  private readonly claims = new Map<string, Date>();

  constructor(private readonly clock: Clock = { now: () => new Date() }) {}

  async claim(key: string, expiresAt: Date): Promise<boolean> {
    const existing = this.claims.get(key);
    if (existing && existing.getTime() > this.clock.now().getTime()) return false;
    this.claims.set(key, expiresAt);
    return true;
  }

  async release(key: string): Promise<void> {
    this.claims.delete(key);
  }
}
