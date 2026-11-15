import type { EndedMarker, SeenStore } from '../../ports.js';

/** In-memory SeenStore: source message id -> sent time. */
export class InMemorySeenStore implements SeenStore {
  private readonly seen = new Map<string, string>();

  async has(sourceId: string): Promise<boolean> {
    return this.seen.has(sourceId);
  }

  async mark(sourceId: string, sent: string): Promise<void> {
    this.seen.set(sourceId, sent);
  }

  private readonly ended = new Map<string, EndedMarker>();

  async markEnded(alertId: string, marker: EndedMarker): Promise<void> {
    this.ended.set(alertId, { ...marker });
  }

  async getEnded(alertId: string): Promise<EndedMarker | null> {
    const found = this.ended.get(alertId);
    return found ? { ...found } : null;
  }

  async clearEnded(alertId: string): Promise<void> {
    this.ended.delete(alertId);
  }
}
