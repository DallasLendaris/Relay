import type { Clock, Device, DeviceErasure, DeviceStore } from '../../ports.js';
import type { PushTarget } from '../../scale/contracts.js';
import {
  MOVE_WINDOW_MS,
  PushTokenInUseError,
  moveAllowed,
  pushTargetHash,
  type PushTargetMeta,
} from '../../scale/latejoin/push-target-rules.js';

/** Fills in the native target for a device that only carries the legacy Expo token. */
function normalize(device: Device): Device {
  const platform =
    device.pushPlatform !== undefined ? device.pushPlatform : device.expoPushToken ? 'expo' : null;
  const token =
    device.pushToken !== undefined ? device.pushToken : platform ? device.expoPushToken : null;
  return { ...device, pushPlatform: platform, pushToken: token };
}

/** In-memory DeviceStore for tests and local runs. */
export class MemoryDeviceStore implements DeviceStore, DeviceErasure {
  private readonly devices = new Map<string, Device>();
  private readonly tombstones = new Map<string, Date>();
  private readonly meta = new Map<string, PushTargetMeta>();
  /** Move times per token hash (the push-target move rules). */
  private readonly moves = new Map<string, number[]>();

  constructor(private readonly clock: Clock = { now: () => new Date() }) {}

  async create(device: Device): Promise<void> {
    if (this.devices.has(device.id)) throw new Error(`device ${device.id} already exists`);
    this.devices.set(device.id, normalize(device));
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
    this.devices.set(device.id, normalize(device));
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
    const holders = [...this.devices.values()].filter(
      (other) =>
        other.id !== deviceId &&
        other.pushPlatform === target.platform &&
        other.pushToken === target.token,
    );
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
      other.pushPlatform = null;
      other.pushToken = null;
      other.pushProject = null;
      other.expoPushToken = null;
      displaced.push(other.id);
    }
    if (displaced.length > 0) this.moves.set(hash, [...recent, now.getTime()]);
    const previous = this.meta.get(deviceId);
    const same = device.pushPlatform === target.platform && device.pushToken === target.token;
    this.meta.set(deviceId, {
      updatedAt: now,
      changedAt: same && previous ? previous.changedAt : now,
    });
    this.devices.set(deviceId, {
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
    for (const device of this.devices.values()) {
      if (device.expoPushToken === token || device.pushToken === token) {
        device.expoPushToken = null;
        device.pushPlatform = null;
        device.pushToken = null;
        device.pushProject = null;
      }
    }
  }

  async deleteDevice(deviceId: string, deletedAt: Date): Promise<boolean> {
    if (!this.tombstones.has(deviceId)) this.tombstones.set(deviceId, deletedAt);
    this.meta.delete(deviceId);
    return this.devices.delete(deviceId);
  }

  async listDeletedDevices(): Promise<string[]> {
    return [...this.tombstones.keys()];
  }
}
