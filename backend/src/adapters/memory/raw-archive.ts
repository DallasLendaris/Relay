import type { RawArchive } from '../../ports.js';

/** In-memory RawArchive for tests and local runs. */
export class MemoryRawArchive implements RawArchive {
  readonly items: Array<{ key: string; body: string; fetchedAt: Date }> = [];

  async put(body: string, fetchedAt: Date): Promise<string> {
    const key = `memory/${this.items.length}`;
    this.items.push({ key, body, fetchedAt });
    return key;
  }
}
