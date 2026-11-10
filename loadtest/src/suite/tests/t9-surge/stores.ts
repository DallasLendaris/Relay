/**
 * Indexed in-memory stores for t9's API process. The backend's Memory* stores are written for unit
 * tests: MemoryDeviceStore.assignPushTarget scans every device to find the holders of a push token,
 * MemoryPlaceStore.listForDevice scans every place, MemoryCellIndex.listForCell sorts a whole cell per
 * page. That is fine for a test and quadratic for 250,000 registrations in five minutes, so these
 * keep the same behaviour (the move rules come from the backend's own `moveAllowed`) behind an
 * index. test/suite-t9-stores.test.ts checks them against the backend stores operation by operation.
 */
import type {
  Clock,
  Device,
  DeviceErasure,
  DeviceStore,
  Place,
  PlaceErasure,
  PlaceStore,
} from '../../../../../backend/src/ports.js';
import { sortKey } from '../../../../../backend/src/scale/cell-index/util.js';
import type {
  CellIndex,
  CellIndexEntry,
  CellIndexPage,
  PushTarget,
} from '../../../../../backend/src/scale/contracts.js';
import {
  MOVE_WINDOW_MS,
  PushTokenInUseError,
  moveAllowed,
  pushTargetHash,
  type PushTargetMeta,
} from '../../../../../backend/src/scale/latejoin/push-target-rules.js';

function normalize(device: Device): Device {
  const platform =
    device.pushPlatform !== undefined ? device.pushPlatform : device.expoPushToken ? 'expo' : null;
  const token =
    device.pushToken !== undefined ? device.pushToken : platform ? device.expoPushToken : null;
  return { ...device, pushPlatform: platform, pushToken: token };
}

const holderKey = (d: Pick<Device, 'pushPlatform' | 'pushToken'>): string | null =>
  d.pushPlatform && d.pushToken ? `${d.pushPlatform}\u0000${d.pushToken}` : null;

export class IndexedDeviceStore implements DeviceStore, DeviceErasure {
  private readonly devices = new Map<string, Device>();
  private readonly tombstones = new Map<string, Date>();
  private readonly meta = new Map<string, PushTargetMeta>();
  private readonly moves = new Map<string, number[]>();
  /** push target -> ids of the devices holding it. */
  private readonly holders = new Map<string, Set<string>>();

  constructor(private readonly clock: Clock = { now: () => new Date() }) {}

  get size(): number {
    return this.devices.size;
  }

  /** Devices that currently hold a push target. */
  get holding(): number {
    let n = 0;
    for (const s of this.holders.values()) n += s.size;
    return n;
  }

  private index(id: string, d: Device | undefined, next: Device | undefined): void {
    const before = d ? holderKey(d) : null;
    const after = next ? holderKey(next) : null;
    if (before === after) return;
    if (before) {
      const set = this.holders.get(before);
      set?.delete(id);
      if (set && set.size === 0) this.holders.delete(before);
    }
    if (after) {
      let set = this.holders.get(after);
      if (!set) this.holders.set(after, (set = new Set()));
      set.add(id);
    }
  }

  private put(device: Device): void {
    const next = normalize(device);
    this.index(device.id, this.devices.get(device.id), next);
    this.devices.set(device.id, next);
  }

  async create(device: Device): Promise<void> {
    if (this.devices.has(device.id)) throw new Error(`device ${device.id} already exists`);
    this.put(device);
    const created = new Date(Date.parse(device.createdAt) || this.clock.now().getTime());
    this.meta.set(device.id, { updatedAt: created, changedAt: created });
  }

  async pushTargetMeta(deviceId: string): Promise<PushTargetMeta | null> {
    const m = this.meta.get(deviceId);
    return m ? { ...m } : null;
  }

  async get(id: string): Promise<Device | null> {
    const device = this.devices.get(id);
    return device ? { ...device } : null;
  }

  async update(device: Device): Promise<void> {
    if (!this.devices.has(device.id)) throw new Error(`device ${device.id} not found`);
    this.put(device);
  }

  async assignPushToken(deviceId: string, token: string, language: 'en' | 'es'): Promise<void> {
    await this.assignPushTarget(deviceId, { platform: 'expo', token }, language);
  }

  async assignPushTarget(
    deviceId: string,
    target: PushTarget,
    language: 'en' | 'es',
  ): Promise<{ displaced: string[] }> {
    const device = this.devices.get(deviceId);
    if (!device) throw new Error(`device ${deviceId} not found`);
    const now = this.clock.now();
    const key = holderKey({ pushPlatform: target.platform, pushToken: target.token }) as string;
    const holders = [...(this.holders.get(key) ?? [])]
      .filter((id) => id !== deviceId)
      .map((id) => this.devices.get(id) as Device);
    const hash = pushTargetHash(target);
    const recent = (this.moves.get(hash) ?? []).filter((t) => t > now.getTime() - MOVE_WINDOW_MS);
    const allowed = moveAllowed({
      now: now.getTime(),
      holdersLastUpdate: holders.map(
        (h) => this.meta.get(h.id)?.updatedAt.getTime() ?? Date.parse(h.createdAt),
      ),
      recentMoves: recent.length,
    });
    if (!allowed) throw new PushTokenInUseError();
    const displaced: string[] = [];
    for (const other of holders) {
      this.put({
        ...other,
        pushPlatform: null,
        pushToken: null,
        pushProject: null,
        expoPushToken: null,
      });
      displaced.push(other.id);
    }
    if (displaced.length > 0) this.moves.set(hash, [...recent, now.getTime()]);
    const previous = this.meta.get(deviceId);
    const same = device.pushPlatform === target.platform && device.pushToken === target.token;
    this.meta.set(deviceId, {
      updatedAt: now,
      changedAt: same && previous ? previous.changedAt : now,
    });
    this.put({
      ...device,
      pushPlatform: target.platform,
      pushToken: target.token,
      pushProject: target.platform === 'android' ? (target.project ?? null) : null,
      expoPushToken: target.platform === 'expo' ? target.token : null,
      language,
    });
    return { displaced };
  }

  async clearPushToken(token: string): Promise<void> {
    for (const device of [...this.devices.values()]) {
      if (device.expoPushToken === token || device.pushToken === token) {
        this.put({
          ...device,
          expoPushToken: null,
          pushPlatform: null,
          pushToken: null,
          pushProject: null,
        });
      }
    }
  }

  async deleteDevice(deviceId: string, deletedAt: Date): Promise<boolean> {
    if (!this.tombstones.has(deviceId)) this.tombstones.set(deviceId, deletedAt);
    this.meta.delete(deviceId);
    this.index(deviceId, this.devices.get(deviceId), undefined);
    return this.devices.delete(deviceId);
  }

  async listDeletedDevices(): Promise<string[]> {
    return [...this.tombstones.keys()];
  }
}

export class IndexedPlaceStore implements PlaceStore, PlaceErasure {
  private readonly places = new Map<string, Place>();
  private readonly byDevice = new Map<string, string[]>();

  get size(): number {
    return this.places.size;
  }

  async add(place: Place): Promise<void> {
    if (!this.places.has(place.id)) {
      const list = this.byDevice.get(place.deviceId);
      if (list) list.push(place.id);
      else this.byDevice.set(place.deviceId, [place.id]);
    }
    this.places.set(place.id, { ...place });
  }

  async listForDevice(deviceId: string): Promise<Place[]> {
    return (this.byDevice.get(deviceId) ?? [])
      .map((id) => this.places.get(id) as Place)
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .map((p) => ({ ...p }));
  }

  async remove(deviceId: string, placeId: string): Promise<boolean> {
    const place = this.places.get(placeId);
    if (!place || place.deviceId !== deviceId) return false;
    this.places.delete(placeId);
    const list = (this.byDevice.get(deviceId) ?? []).filter((id) => id !== placeId);
    if (list.length > 0) this.byDevice.set(deviceId, list);
    else this.byDevice.delete(deviceId);
    return true;
  }

  async removeAllForDevice(deviceId: string): Promise<number> {
    const ids = this.byDevice.get(deviceId) ?? [];
    this.byDevice.delete(deviceId);
    for (const id of ids) this.places.delete(id);
    return ids.length;
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

/**
 * The cell index as the API writes it (put, update, remove). The fan-out never pages it through
 * `listForCell`; the shard workers get a cell's entries in one call (`entriesForCell`, served over
 * HTTP by the API process) and read them like the disk seed. `listForCell` exists for the interface.
 */
export class OverlayCellIndex implements CellIndex {
  private readonly cells = new Map<string, Map<string, CellIndexEntry>>();
  private count = 0;

  get size(): number {
    return this.count;
  }

  /** Entries with a working push target (the ones a fan-out would send to). */
  get withTarget(): number {
    let n = 0;
    for (const m of this.cells.values()) for (const e of m.values()) if (e.target) n++;
    return n;
  }

  async put(entries: CellIndexEntry[]): Promise<void> {
    for (const e of entries) {
      let cell = this.cells.get(e.h3Cell);
      if (!cell) this.cells.set(e.h3Cell, (cell = new Map()));
      const sk = sortKey(e.deviceId, e.placeId);
      if (!cell.has(sk)) this.count++;
      cell.set(sk, {
        ...e,
        target: e.target ? { ...e.target } : null,
        ...(e.deviceCells ? { deviceCells: [...e.deviceCells] } : {}),
      });
    }
  }

  async remove(h3Cell: string, deviceId: string, placeId: string): Promise<void> {
    if (this.cells.get(h3Cell)?.delete(sortKey(deviceId, placeId))) this.count--;
  }

  async updateDevice(
    deviceId: string,
    cells: Array<{ h3Cell: string; placeId: string }>,
    patch: { target: CellIndexEntry['target']; language: 'en' | 'es' },
  ): Promise<void> {
    for (const { h3Cell, placeId } of cells) {
      const entry = this.cells.get(h3Cell)?.get(sortKey(deviceId, placeId));
      if (entry) {
        entry.target = patch.target ? { ...patch.target } : null;
        entry.language = patch.language;
      }
    }
  }

  /** A snapshot of one cell's entries (copies, so a later write cannot change what a reader holds). */
  entriesForCell(h3Cell: string): CellIndexEntry[] {
    const m = this.cells.get(h3Cell);
    return m ? [...m.values()].map((e) => ({ ...e })) : [];
  }

  async listForCell(h3Cell: string, _cursor?: string, limit = 1000): Promise<CellIndexPage> {
    return { entries: this.entriesForCell(h3Cell).slice(0, limit) };
  }
}
