import type { SeenStore } from '../../ports.js';

/** In-memory SeenStore: source message id -> sent time. */
export class InMemorySeenStore implements SeenStore {
  private readonly seen = new Map<string, string>();

  async has(sourceId: string): Promise<boolean> {
    return this.seen.has(sourceId);
  }

  async mark(sourceId: string, sent: string): Promise<void> {
    this.seen.set(sourceId, sent);
  }
}
