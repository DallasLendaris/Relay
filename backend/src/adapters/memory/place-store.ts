import type { Place, PlaceErasure, PlaceStore } from '../../ports.js';

/** In-memory PlaceStore for tests and local runs. */
export class MemoryPlaceStore implements PlaceStore, PlaceErasure {
  private readonly places = new Map<string, Place>();

  async add(place: Place): Promise<void> {
    this.places.set(place.id, { ...place });
  }

  async listForDevice(deviceId: string): Promise<Place[]> {
    return [...this.places.values()]
      .filter((p) => p.deviceId === deviceId)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((p) => ({ ...p }));
  }

  async remove(deviceId: string, placeId: string): Promise<boolean> {
    const place = this.places.get(placeId);
    if (!place || place.deviceId !== deviceId) return false;
    return this.places.delete(placeId);
  }

  async removeAllForDevice(deviceId: string): Promise<number> {
    let removed = 0;
    for (const [id, p] of this.places) {
      if (p.deviceId === deviceId && this.places.delete(id)) removed++;
    }
    return removed;
  }

  async listForCells(cells: string[]): Promise<Place[]> {
    const wanted = new Set(cells);
    return [...this.places.values()].filter((p) => wanted.has(p.h3Cell)).map((p) => ({ ...p }));
  }

  async listMatchesForCells(
    cells: string[],
  ): Promise<Array<{ placeId: string; deviceId: string; h3Cell: string }>> {
    return (await this.listForCells(cells)).map((p) => ({
      placeId: p.id,
      deviceId: p.deviceId,
      h3Cell: p.h3Cell,
    }));
  }
}
