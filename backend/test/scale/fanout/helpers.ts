import type { Alert } from '@relay/shared';
import { cellToChildren, cellToParent, gridDisk, latLngToCell } from 'h3-js';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import type { Logger } from '../../../src/delivery/types.js';
import type {
  CellIndex,
  CellIndexEntry,
  CellIndexPage,
  PushTarget,
} from '../../../src/scale/contracts.js';
import type { PlannerDeps } from '../../../src/scale/fanout/planner.js';
import type { ShardWorkerDeps } from '../../../src/scale/fanout/shard-worker.js';
import { MemorySendQueue, MemoryShardDispatcher } from '../../../src/scale/fanout/queues.js';
import { signed, verifyAlert } from '../../delivery/fakes.js';

export { signed, verifyAlert };
export { cellToChildren, cellToParent, gridDisk, latLngToCell };

export const NOW = new Date('2026-11-10T10:05:00Z');
export const clock = { now: () => NOW };

export interface LogLine {
  level: 'info' | 'warn' | 'error';
  msg: string;
  fields?: Record<string, unknown>;
}

export function captureLogger(): { logger: Logger; lines: LogLine[] } {
  const lines: LogLine[] = [];
  const at =
    (level: LogLine['level']) =>
    (msg: string, fields?: Record<string, unknown>): void => {
      lines.push({ level, msg, ...(fields ? { fields } : {}) });
    };
  return { logger: { info: at('info'), warn: at('warn'), error: at('error') }, lines };
}

/** Small local fake of the CellIndex read path; the real one belongs to another module. */
export class FakeCellIndex implements Pick<CellIndex, 'listForCell'> {
  private readonly byCell = new Map<string, CellIndexEntry[]>();
  pages = 0;
  /** Cells being listed right now, and the most seen at once (proves the concurrency cap). */
  private active = 0;
  maxActive = 0;

  add(entry: CellIndexEntry): void {
    const list = this.byCell.get(entry.h3Cell) ?? [];
    list.push(entry);
    this.byCell.set(entry.h3Cell, list);
  }

  remove(h3Cell: string, deviceId: string): void {
    const list = this.byCell.get(h3Cell) ?? [];
    this.byCell.set(
      h3Cell,
      list.filter((e) => e.deviceId !== deviceId),
    );
  }

  async listForCell(h3Cell: string, cursor?: string, limit = 1_000): Promise<CellIndexPage> {
    this.pages++;
    this.active++;
    this.maxActive = Math.max(this.maxActive, this.active);
    // Yield so concurrent cells really interleave.
    await new Promise((r) => setImmediate(r));
    this.active--;
    const all = [...(this.byCell.get(h3Cell) ?? [])].sort((a, b) =>
      `${a.deviceId}#${a.placeId}` < `${b.deviceId}#${b.placeId}` ? -1 : 1,
    );
    const start = cursor ? Number(cursor) : 0;
    const entries = all.slice(start, start + limit);
    const next = start + limit;
    return next < all.length ? { entries, cursor: String(next) } : { entries };
  }
}

export const target = (id: string): PushTarget => ({
  platform: 'ios',
  token: `tok-${id}`.padEnd(64, '0'),
});

export const entry = (
  h3Cell: string,
  deviceId: string,
  placeId = 'home',
  withTarget = true,
): CellIndexEntry => ({
  h3Cell,
  deviceId,
  placeId,
  target: withTarget ? target(deviceId) : null,
  language: 'en',
});

/** Deterministic fake token: proves the cell and device are passed through. */
export const fakeToken = (alertId: string, deviceId: string, h3Cell: string, expiresAt: Date) =>
  `t:${alertId}:${deviceId}:${h3Cell}:${expiresAt.toISOString()}`;

export function setup(
  alert: Alert = signed({}),
  over: Partial<PlannerDeps & ShardWorkerDeps> = {},
) {
  const alerts = new InMemoryAlertStore();
  void alerts.save(alert);
  const cellIndex = new FakeCellIndex();
  const shardDispatcher = new MemoryShardDispatcher();
  const sendQueue = new MemorySendQueue();
  const { logger, lines } = captureLogger();
  const deps = {
    alerts,
    verifyAlert,
    isDeliveryPaused: async () => false,
    clock,
    logger,
    shardDispatcher,
    cellIndex,
    sendQueue,
    makeCheckInToken: fakeToken,
    ...over,
  };
  return { alerts, cellIndex, shardDispatcher, sendQueue, lines, deps };
}

/** A county-sized area: radius-14 disk of res-7 cells (about 630 cells, 3,000 km2). */
export const COUNTY_CENTER = latLngToCell(37.3382, -121.8863, 7);
export const countyCells = (): string[] => gridDisk(COUNTY_CENTER, 14);
