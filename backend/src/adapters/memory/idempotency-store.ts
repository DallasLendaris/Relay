import type { ChunkProgress, Clock, IdempotencyStore, KeyInspection } from '../../ports.js';

interface Entry {
  state: 'leased' | 'sent';
  /** Lease end while leased; remembered-until time once sent. */
  until: number;
  /** The claim that wrote this entry (mirrors the DynamoDB store). */
  attempt: number;
  /** Sub-chunk progress (recordChunk); kept across an expired lease or a release. */
  progress?: ChunkProgress[];
  /** Member set stored by markSent's detail or markPlanned (G5). */
  members?: Uint8Array;
  /** A plan record's (device, target) set, kept only while one plan wrote the key (G5b). */
  targets?: Uint8Array;
}

/**
 * In-memory IdempotencyStore with leases. Entries stop counting once `until` has passed.
 * Like the DynamoDB store, release and markSent only act on this store's own latest claim.
 */
export class MemoryIdempotencyStore implements IdempotencyStore {
  private readonly entries = new Map<string, Entry>();
  private readonly attempts = new Map<string, number>();
  /** What claimedProgress returns: the progress an earlier attempt left when we claimed. */
  private readonly inherited = new Map<string, ChunkProgress[]>();
  private nextAttempt = 1;

  constructor(private readonly clock: Clock = { now: () => new Date() }) {}

  private live(key: string): Entry | undefined {
    const e = this.entries.get(key);
    return e && e.until > this.clock.now().getTime() ? e : undefined;
  }

  async claim(key: string, leaseUntil: Date): Promise<'claimed' | 'sent' | 'leased'> {
    const existing = this.live(key);
    if (existing) return existing.state;
    // Like the DynamoDB store: an expired (or released) lease hands its progress to the next claim.
    const prev = this.entries.get(key);
    const progress = prev?.state === 'leased' ? (prev.progress ?? []) : [];
    const attempt = this.nextAttempt++;
    this.entries.set(key, {
      state: 'leased',
      until: leaseUntil.getTime(),
      attempt,
      ...(progress.length > 0 ? { progress: [...progress] } : {}),
    });
    this.attempts.set(key, attempt);
    this.inherited.set(
      key,
      progress.map((p) => ({ ...p, redo: [...p.redo] })),
    );
    return 'claimed';
  }

  async markSent(key: string, expiresAt: Date, detail?: { members?: Uint8Array }): Promise<void> {
    const attempt = this.attempts.get(key);
    this.attempts.delete(key);
    this.inherited.delete(key);
    const existing = this.live(key);
    // Never overwrite a key a different attempt already marked sent.
    if (existing?.state === 'sent' && existing.attempt !== attempt) return;
    this.entries.set(key, {
      state: 'sent',
      until: expiresAt.getTime(),
      attempt: attempt ?? 0,
      ...(detail?.members ? { members: new Uint8Array(detail.members) } : {}),
    });
  }

  async markPlanned(
    key: string,
    members: Uint8Array | null,
    expiresAt: Date,
    detail?: { targets?: Uint8Array },
  ): Promise<void> {
    // Like the DynamoDB store: a later plan's record replaces an earlier one, and the target set
    // is kept only when no record of the key existed (any stored item, expired or not, counts).
    const first = !this.entries.has(key);
    const targets = first ? detail?.targets : undefined;
    this.entries.set(key, {
      state: 'sent',
      until: expiresAt.getTime(),
      attempt: 0,
      ...(members ? { members: new Uint8Array(members) } : {}),
      ...(targets ? { targets: new Uint8Array(targets) } : {}),
    });
  }

  async inspect(key: string): Promise<KeyInspection> {
    const e = this.live(key);
    if (!e) return { state: 'absent' };
    if (e.state !== 'sent') return { state: e.state };
    return {
      state: 'sent',
      ...(e.members ? { members: new Uint8Array(e.members) } : {}),
      ...(e.targets ? { targets: new Uint8Array(e.targets) } : {}),
    };
  }

  async release(key: string): Promise<void> {
    const attempt = this.attempts.get(key);
    this.attempts.delete(key);
    this.inherited.delete(key);
    const existing = this.entries.get(key);
    // Only give back a lease that is still ours. With progress recorded, end the lease instead
    // of deleting, so the next claim still skips what already went out.
    if (existing?.state === 'leased' && existing.attempt === attempt) {
      if (existing.progress?.length) existing.until = 0;
      else this.entries.delete(key);
    }
  }

  async recordChunk(key: string, progress: ChunkProgress): Promise<boolean> {
    const attempt = this.attempts.get(key);
    const existing = this.entries.get(key);
    // Same guard as extend: still leased, and still this attempt's.
    if (attempt === undefined || existing?.state !== 'leased' || existing.attempt !== attempt) {
      return false;
    }
    (existing.progress ??= []).push({ ...progress, redo: [...progress.redo] });
    return true;
  }

  async claimedProgress(key: string): Promise<ChunkProgress[]> {
    return this.inherited.get(key) ?? [];
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
