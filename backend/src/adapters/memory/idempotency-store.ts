import type { Clock, IdempotencyStore } from '../../ports.js';

interface Entry {
  state: 'leased' | 'sent';
  /** Lease end while leased; remembered-until time once sent. */
  until: number;
  /** The claim that wrote this entry (mirrors the DynamoDB store). */
  attempt: number;
}

/**
 * In-memory IdempotencyStore with leases. Entries stop counting once `until` has passed.
 * Like the DynamoDB store, release and markSent only act on this store's own latest claim.
 */
export class MemoryIdempotencyStore implements IdempotencyStore {
  private readonly entries = new Map<string, Entry>();
  private readonly attempts = new Map<string, number>();
  private nextAttempt = 1;

  constructor(private readonly clock: Clock = { now: () => new Date() }) {}

  private live(key: string): Entry | undefined {
    const e = this.entries.get(key);
    return e && e.until > this.clock.now().getTime() ? e : undefined;
  }

  async claim(key: string, leaseUntil: Date): Promise<'claimed' | 'sent' | 'leased'> {
    const existing = this.live(key);
    if (existing) return existing.state;
    const attempt = this.nextAttempt++;
    this.entries.set(key, { state: 'leased', until: leaseUntil.getTime(), attempt });
    this.attempts.set(key, attempt);
    return 'claimed';
  }

  async markSent(key: string, expiresAt: Date): Promise<void> {
    const attempt = this.attempts.get(key);
    this.attempts.delete(key);
    const existing = this.live(key);
    // Never overwrite a key a different attempt already marked sent.
    if (existing?.state === 'sent' && existing.attempt !== attempt) return;
    this.entries.set(key, { state: 'sent', until: expiresAt.getTime(), attempt: attempt ?? 0 });
  }

  async release(key: string): Promise<void> {
    const attempt = this.attempts.get(key);
    this.attempts.delete(key);
    const existing = this.entries.get(key);
    // Only give back a lease that is still ours.
    if (existing?.state === 'leased' && existing.attempt === attempt) this.entries.delete(key);
  }

  async extend(key: string, leaseUntil: Date): Promise<boolean> {
    const attempt = this.attempts.get(key);
    const existing = this.entries.get(key);
    // Only our own claim, and only while it is still a lease (never a sent key).
    if (attempt === undefined || existing?.state !== 'leased' || existing.attempt !== attempt) {
      return false;
    }
    existing.until = leaseUntil.getTime();
    return true;
  }

  async peek(key: string): Promise<'absent' | 'leased' | 'sent'> {
    return this.live(key)?.state ?? 'absent';
  }
}
