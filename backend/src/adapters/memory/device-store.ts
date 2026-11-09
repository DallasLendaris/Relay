import type { Device, DeviceErasure, DeviceStore } from '../../ports.js';
import type { PushTarget } from '../../scale/contracts.js';

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

  async create(device: Device): Promise<void> {
    if (this.devices.has(device.id)) throw new Error(`device ${device.id} already exists`);
    this.devices.set(device.id, normalize(device));
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
    const displaced: string[] = [];
    for (const other of this.devices.values()) {
      if (
        other.id !== deviceId &&
        other.pushPlatform === target.platform &&
        other.pushToken === target.token
      ) {
        other.pushPlatform = null;
        other.pushToken = null;
        other.expoPushToken = null;
        displaced.push(other.id);
      }
    }
    this.devices.set(deviceId, {
      ...device,
      pushPlatform: target.platform,
      pushToken: target.token,
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
      }
    }
  }

  async deleteDevice(deviceId: string, deletedAt: Date): Promise<boolean> {
    if (!this.tombstones.has(deviceId)) this.tombstones.set(deviceId, deletedAt);
    return this.devices.delete(deviceId);
  }

  async listDeletedDevices(): Promise<string[]> {
    return [...this.tombstones.keys()];
  }
}
