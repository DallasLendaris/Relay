/**
 * Test 2 sizes. Every value can be overridden through `runT2(ctx, overrides)` or an environment
 * variable (listed per field), so one scenario can be rerun alone, e.g.
 *   T2_SCENARIOS=d T2_DEVICES_STATE=2000000 npm run suite -- t2
 */
import type { SuiteContext } from '../../types.js';
import { CA_RING, countyRing } from './seed-data.js';

export type ScenarioKey = 'a' | 'b' | 'c' | 'd' | 'e';

export interface T2Overrides {
  /** T2_SCENARIOS, e.g. "a,c". Default all. */
  scenarios?: ScenarioKey[];
  /** T2_DEVICES_A (county, default 1,000,000; smoke 5,000). */
  devicesA?: number;
  /** T2_DEVICES_B (county, default 1,900,000; smoke 10,000). */
  devicesB?: number;
  /** T2_DEVICES_STATE (California, default 10,000,000; smoke 20,000). */
  devicesState?: number;
  /** T2_DEVICES_SWEEP (county, default = devicesA). */
  devicesSweep?: number;
  /** T2_SWEEP, e.g. "1,2,4,8"; the full sender count is always added. */
  sweep?: number[];
  /** T2_SENDERS (default ctx.workers / 2, at least 2). */
  senders?: number;
  /** T2_SHARD_WORKERS (default ctx.workers / 3, at least 2). */
  shardWorkers?: number;
  /** T2_SEED (default 1). */
  seed?: number;
  /**
   * T2_FCM_RATE_MODE. 'fleet' (default): each sender's per-project bucket is 9,000/s divided by
   * the sender count (FCM_PROJECT_RATE_PER_SEC set for the fleet, as docs/scale-quotas.md
   * assumes). 'per-task': every sender uses the 9,000/s default, as infra deploys it today.
   */
  fcmRateMode?: 'fleet' | 'per-task';
  /** T2_SWEEP_PROJECTS: Firebase projects in the sweep, so FCM quota never caps it (default 16). */
  sweepProjects?: number;
  /** T2_FCM_QUOTA: mock quota per project, messages/s (default 10,000 = 600,000/min). */
  fcmQuotaPerSec?: number;
  /** T2_APNS_MOCKS / T2_FCM_MOCKS: mock processes (default ctx.workers / 8, at least 1). */
  apnsMocks?: number;
  fcmMocks?: number;
  /** T2_SENDER_CONCURRENCY: SQS messages in flight per sender (default 50, as main.ts). */
  senderConcurrency?: number;
  /** T2_SHARD_JOBS: shard jobs in flight per shard-worker process (default 4). */
  jobsPerShardWorker?: number;
}

export interface Scenario {
  key: string;
  name: string;
  region: 'county' | 'state';
  devices: number;
  projects: number;
  senders: number;
  ring: Array<[number, number]>;
  areaName: string;
}

export interface T2Config {
  scale: 'full' | 'smoke';
  seed: number;
  scenarios: ScenarioKey[];
  devicesA: number;
  devicesB: number;
  devicesState: number;
  devicesSweep: number;
  sweep: number[];
  senders: number;
  shardWorkers: number;
  fcmRateMode: 'fleet' | 'per-task';
  sweepProjects: number;
  fcmQuotaPerSec: number;
  apnsMocks: number;
  fcmMocks: number;
  senderConcurrency: number;
  jobsPerShardWorker: number;
  heapMb: { sender: number; shard: number; broker: number; mock: number; seed: number };
  brokerCapMb: number;
  /** A run is declared stuck after this long without progress. */
  stallMs: number;
}

const envNum = (name: string): number | undefined => {
  const v = process.env[name];
  if (v === undefined || v === '') return undefined;
  const n = Number(v.replace(/_/g, ''));
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${name} must be a positive number`);
  return Math.floor(n);
};
const envList = (name: string): string[] | undefined => {
  const v = process.env[name];
  return v
    ? v
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
    : undefined;
};

export function resolveConfig(ctx: SuiteContext, o: T2Overrides = {}): T2Config {
  const smoke = ctx.scale === 'smoke';
  const w = Math.max(2, ctx.workers);
  const scenarios = (
    o.scenarios ??
    (envList('T2_SCENARIOS') as ScenarioKey[] | undefined) ?? ['a', 'b', 'c', 'd', 'e']
  ).filter((s): s is ScenarioKey => ['a', 'b', 'c', 'd', 'e'].includes(s));
  const senders = o.senders ?? envNum('T2_SENDERS') ?? (smoke ? 2 : Math.max(2, Math.floor(w / 2)));
  const shardWorkers =
    o.shardWorkers ?? envNum('T2_SHARD_WORKERS') ?? (smoke ? 2 : Math.max(2, Math.floor(w / 3)));
  const apnsMocks =
    o.apnsMocks ?? envNum('T2_APNS_MOCKS') ?? (smoke ? 1 : Math.max(1, Math.round(w / 8)));
  const fcmMocks =
    o.fcmMocks ?? envNum('T2_FCM_MOCKS') ?? (smoke ? 1 : Math.max(1, Math.round(w / 8)));
  const devicesA = o.devicesA ?? envNum('T2_DEVICES_A') ?? (smoke ? 5_000 : 1_000_000);
  const sweepBase = o.sweep ?? envList('T2_SWEEP')?.map(Number) ?? (smoke ? [1] : [1, 2, 4, 8]);
  const sweep = [...new Set([...sweepBase.filter((n) => n >= 1 && n < senders), senders])].sort(
    (a, b) => a - b,
  );

  // Heap caps: the sum must fit in RAM with headroom (32 GB machine: about 26 GB of caps).
  const brokerCapMb = smoke ? 64 : 768;
  const mockHeap = smoke ? 384 : 1536;
  const brokerHeap = smoke ? 256 : 768;
  const budget = Math.min(26_000, ctx.machine.memoryTotalMb * 0.75);
  const fixed = (apnsMocks + fcmMocks) * mockHeap + brokerHeap + brokerCapMb;
  const each = Math.floor((budget - fixed) / (senders + shardWorkers));
  const roleHeap = smoke ? 384 : Math.max(512, Math.min(1024, each));

  const rateMode =
    o.fcmRateMode ??
    (process.env['T2_FCM_RATE_MODE'] as T2Config['fcmRateMode'] | undefined) ??
    'fleet';
  if (rateMode !== 'fleet' && rateMode !== 'per-task') {
    throw new Error('T2_FCM_RATE_MODE must be fleet or per-task');
  }
  return {
    scale: ctx.scale,
    seed: o.seed ?? envNum('T2_SEED') ?? 1,
    scenarios,
    devicesA,
    devicesB: o.devicesB ?? envNum('T2_DEVICES_B') ?? (smoke ? 10_000 : 1_900_000),
    devicesState: o.devicesState ?? envNum('T2_DEVICES_STATE') ?? (smoke ? 20_000 : 10_000_000),
    devicesSweep: o.devicesSweep ?? envNum('T2_DEVICES_SWEEP') ?? devicesA,
    sweep,
    senders,
    shardWorkers,
    fcmRateMode: rateMode,
    sweepProjects: o.sweepProjects ?? envNum('T2_SWEEP_PROJECTS') ?? (smoke ? 2 : 16),
    fcmQuotaPerSec: o.fcmQuotaPerSec ?? envNum('T2_FCM_QUOTA') ?? 10_000,
    apnsMocks,
    fcmMocks,
    senderConcurrency: o.senderConcurrency ?? envNum('T2_SENDER_CONCURRENCY') ?? 50,
    jobsPerShardWorker: o.jobsPerShardWorker ?? envNum('T2_SHARD_JOBS') ?? 4,
    heapMb: {
      sender: roleHeap,
      shard: roleHeap,
      broker: brokerHeap,
      mock: mockHeap,
      seed: smoke ? 512 : 2048,
    },
    brokerCapMb,
    stallMs: smoke ? 45_000 : 180_000,
  };
}

/**
 * In smoke the statewide-mini alert covers only a Northern California box (Bay Area to
 * Sacramento, about 9,000 cells) instead of the whole state (about 80,000 cells), so it finishes in
 * seconds on 2 CPUs; the devices elsewhere in the state are the outside-area controls.
 */
export const NORCAL_RING: Array<[number, number]> = [
  [-123.0, 36.9],
  [-121.0, 36.9],
  [-121.0, 39.0],
  [-123.0, 39.0],
  [-123.0, 36.9],
];

export function scenarioList(cfg: T2Config): Scenario[] {
  const out: Scenario[] = [];
  const county = (key: string, name: string, devices: number, projects: number, senders: number) =>
    out.push({
      key,
      name,
      region: 'county',
      devices,
      projects,
      senders,
      ring: countyRing(),
      areaName: 'Santa Clara County (bounding box)',
    });
  const stateRing = cfg.scale === 'smoke' ? NORCAL_RING : CA_RING;
  const stateArea =
    cfg.scale === 'smoke' ? 'Northern California box (smoke)' : 'California (simplified outline)';
  const state = (key: string, name: string, projects: number) =>
    out.push({
      key,
      name,
      region: 'state',
      devices: cfg.devicesState,
      projects,
      senders: cfg.senders,
      ring: stateRing,
      areaName: stateArea,
    });
  for (const k of cfg.scenarios) {
    if (k === 'a') county('a', 'a) county, 1 FCM project', cfg.devicesA, 1, cfg.senders);
    if (k === 'b') county('b', 'b) county (large), 1 FCM project', cfg.devicesB, 1, cfg.senders);
    if (k === 'c') state('c', 'c) statewide, 1 FCM project', 1);
    if (k === 'd') state('d', 'd) statewide, 8 FCM projects', 8);
    if (k === 'e') {
      for (const n of cfg.sweep) {
        county(
          `e${n}`,
          `e) sweep: ${n} sender${n === 1 ? '' : 's'}`,
          cfg.devicesSweep,
          cfg.sweepProjects,
          n,
        );
      }
    }
  }
  return out;
}
