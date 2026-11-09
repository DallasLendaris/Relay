import type { Device, DeviceStore } from '../../ports.js';

/** In-memory DeviceStore for tests and local runs. */
export class MemoryDeviceStore implements DeviceStore {
  private readonly devices = new Map<string, Device>();

  async create(device: Device): Promise<void> {
    if (this.devices.has(device.id)) throw new Error(`device ${device.id} already exists`);
    this.devices.set(device.id, { ...device });
  }

  async get(id: string): Promise<Device | null> {
    const device = this.devices.get(id);
    return device ? { ...device } : null;
  }

  async update(device: Device): Promise<void> {
    if (!this.devices.has(device.id)) throw new Error(`device ${device.id} not found`);
    this.devices.set(device.id, { ...device });
  }
}
