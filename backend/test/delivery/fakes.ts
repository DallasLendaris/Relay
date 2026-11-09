import { latLngToCell, gridDisk } from 'h3-js';
import type { Alert } from '@relay/shared';
import type { Device, DeviceStore, Place, PlaceStore } from '../../src/ports.js';

/** Small local fakes; the real in-memory Place/Device stores belong to another agent. */
export class FakePlaces implements PlaceStore {
  byCell = new Map<string, Place[]>();
  queries: number[] = [];
  async add(p: Place) {
    const list = this.byCell.get(p.h3Cell) ?? [];
    list.push(p);
    this.byCell.set(p.h3Cell, list);
  }
  async listForDevice() {
    return [];
  }
  async remove() {
    return false;
  }
  async listForCells(cells: string[]) {
    this.queries.push(cells.length);
    return cells.flatMap((c) => this.byCell.get(c) ?? []);
  }
}

export class FakeDevices implements DeviceStore {
  map = new Map<string, Device>();
  async create(d: Device) {
    this.map.set(d.id, d);
  }
  async get(id: string) {
    return this.map.get(id) ?? null;
  }
  async update(d: Device) {
    this.map.set(d.id, d);
  }
}

export const device = (id: string, token: string | null = `ExponentPushToken[${id}]`): Device => ({
  id,
  credentialHash: 'x',
  expoPushToken: token,
  language: 'en',
  createdAt: '2026-11-01T00:00:00Z',
});

export const place = (id: string, deviceId: string, lat: number, lng: number): Place => ({
  id,
  deviceId,
  label: id,
  shortAddress: id,
  latitude: lat,
  longitude: lng,
  h3Cell: latLngToCell(lat, lng, 7),
  createdAt: '2026-11-01T00:00:00Z',
});

export const SJ = { lat: 37.3382, lng: -121.8863 };

export function makeAlert(over: Partial<Alert> = {}, cells?: string[]): Alert {
  return {
    id: '11111111-1111-4111-8111-111111111111',
    source: 'nws',
    sourceId: 'urn:oid:1',
    event: 'Flash Flood Warning',
    eventCode: 'FFW',
    relaySeverity: 'warning',
    capSeverity: 'Severe',
    urgency: 'Immediate',
    certainty: 'Likely',
    status: 'Actual',
    messageType: 'Alert',
    state: 'active',
    references: [],
    area: {
      description: 'Santa Clara',
      geometry: null,
      ugcCodes: [],
      sameCodes: [],
      h3Cells: cells ?? gridDisk(latLngToCell(SJ.lat, SJ.lng, 7), 2),
    },
    sent: '2026-11-10T10:00:00Z',
    effective: '2026-11-10T10:00:00Z',
    onset: null,
    expires: '2026-11-10T14:00:00Z',
    ends: null,
    official: {
      headline: 'Flash Flood Warning until 6 PM',
      description: 'Heavy rain is causing flooding.',
      instruction: null,
      senderName: 'NWS',
      language: 'en-US',
    },
    summaries: [],
    signature: 'sig',
    ...over,
  };
}
