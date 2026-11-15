import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { describe, expect, it } from 'vitest';
import { MemoryDeviceStore } from '../../../src/adapters/memory/device-store.js';
import { MemoryPlaceStore } from '../../../src/adapters/memory/place-store.js';
import type { Device } from '../../../src/ports.js';
import { DynamoCellIndex, MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import type { CellIndexEntry, PushTarget } from '../../../src/scale/contracts.js';
import { createDeadTargetClearer } from '../../../src/scale/sender/dead-target.js';
import { place } from '../../delivery/fakes.js';

const CELL_A = '872830828ffffff';
const CELL_B = '872830829ffffff';
const IOS: PushTarget = { platform: 'ios', token: 'a'.repeat(64) };
const EXPO: PushTarget = { platform: 'expo', token: 'ExponentPushToken[abc]' };

const device = (id: string, over: Partial<Device> = {}): Device => ({
  id,
  credentialHash: 'h',
  expoPushToken: null,
  language: 'en',
  createdAt: '2026-11-01T00:00:00Z',
  ...over,
});

async function setup() {
  const devices = new MemoryDeviceStore();
  const places = new MemoryPlaceStore();
  const cellIndex = new MemoryCellIndex();
  const seed = async (deviceId: string, target: PushTarget, language: 'en' | 'es' = 'en') => {
    await devices.create(device(deviceId, { language }));
    await devices.assignPushTarget(deviceId, target, language);
    const entries: CellIndexEntry[] = [];
    for (const [i, cell] of [CELL_A, CELL_B].entries()) {
      const p = { ...place(`${deviceId}-p${i}`, deviceId, 37.3, -121.9), h3Cell: cell };
      await places.add(p);
      entries.push({ h3Cell: cell, deviceId, placeId: p.id, target, language });
    }
    await cellIndex.put(entries);
  };
  const clear = createDeadTargetClearer({ devices, places, cellIndex });
  const targetsIn = async (cell: string) =>
    Object.fromEntries(
      (await cellIndex.listForCell(cell)).entries.map((e) => [e.deviceId, e.target?.token ?? null]),
    );
  return { devices, places, cellIndex, seed, clear, targetsIn };
}

describe('createDeadTargetClearer', () => {
  it('clears a dead native target in the device store and in every cell of the device', async () => {
    const t = await setup();
    await t.seed('dev1', IOS, 'es');
    await t.seed('dev2', { platform: 'android', token: 'b'.repeat(120) });
    await t.clear('dev1', IOS);

    const after = await t.devices.get('dev1');
    expect(after).toMatchObject({ pushPlatform: null, pushToken: null, expoPushToken: null });
    expect(await t.targetsIn(CELL_A)).toEqual({ dev1: null, dev2: 'b'.repeat(120) });
    expect(await t.targetsIn(CELL_B)).toEqual({ dev1: null, dev2: 'b'.repeat(120) });
    // The other device is untouched, and the cleared entry keeps its language.
    expect(
      (await t.cellIndex.listForCell(CELL_A)).entries.find((e) => e.deviceId === 'dev1'),
    ).toMatchObject({ target: null, language: 'es' });
    expect(await t.devices.get('dev2')).toMatchObject({ pushPlatform: 'android' });
  });

  it('clears the legacy Expo token too', async () => {
    const t = await setup();
    await t.seed('dev1', EXPO);
    await t.clear('dev1', EXPO);
    expect(await t.devices.get('dev1')).toMatchObject({ expoPushToken: null, pushToken: null });
    expect(await t.targetsIn(CELL_A)).toEqual({ dev1: null });
  });

  it('ignores a late report about a token the device no longer uses', async () => {
    const t = await setup();
    await t.seed('dev1', IOS);
    const fresh: PushTarget = { platform: 'ios', token: 'c'.repeat(64) };
    await t.devices.assignPushTarget('dev1', fresh, 'en');
    await t.cellIndex.updateDevice(
      'dev1',
      [
        { h3Cell: CELL_A, placeId: 'dev1-p0' },
        { h3Cell: CELL_B, placeId: 'dev1-p1' },
      ],
      { target: fresh, language: 'en' },
    );
    await t.clear('dev1', IOS); // IOS is the OLD token
    expect(await t.devices.get('dev1')).toMatchObject({ pushToken: fresh.token });
    expect(await t.targetsIn(CELL_A)).toEqual({ dev1: fresh.token });
  });

  it('does not bring back an entry whose place was removed meanwhile', async () => {
    const t = await setup();
    await t.seed('dev1', IOS);
    await t.cellIndex.remove(CELL_B, 'dev1', 'dev1-p1');
    await t.clear('dev1', IOS);
    expect(await t.targetsIn(CELL_B)).toEqual({});
  });

  it('works for a device that no longer exists (index entries are still cleared)', async () => {
    const t = await setup();
    await t.seed('dev1', IOS);
    const stores = createDeadTargetClearer({
      devices: new MemoryDeviceStore(),
      places: t.places,
      cellIndex: t.cellIndex,
    });
    await stores('dev1', IOS);
    expect(await t.targetsIn(CELL_A)).toEqual({ dev1: null });
  });

  it('writes to the real table shape: key h3Cell + sk, guarded by attribute_exists(h3Cell)', async () => {
    const t = await setup();
    await t.seed('dev1', IOS, 'es');
    const sent: UpdateCommand[] = [];
    const doc = {
      send: async (cmd: unknown) => {
        if (!(cmd instanceof UpdateCommand)) throw new Error('unexpected command');
        sent.push(cmd);
        if (cmd.input.Key?.['h3Cell'] === CELL_B) {
          // That entry was deleted meanwhile; DynamoDB refuses and the clearer moves on.
          throw new ConditionalCheckFailedException({ message: 'gone', $metadata: {} });
        }
        return {};
      },
    };
    const clear = createDeadTargetClearer({
      devices: t.devices,
      places: t.places,
      cellIndex: new DynamoCellIndex(doc as never, 'cell-index'),
    });
    await clear('dev1', IOS);

    expect(sent).toHaveLength(2);
    const a = sent.find((c) => c.input.Key?.['h3Cell'] === CELL_A)?.input;
    expect(a?.TableName).toBe('cell-index');
    expect(a?.Key).toEqual({ h3Cell: CELL_A, sk: 'dev1#dev1-p0' });
    expect(a?.ConditionExpression).toBe('attribute_exists(h3Cell)');
    expect(a?.ExpressionAttributeValues).toMatchObject({ ':target': null, ':language': 'es' });
    // No attribute named "pk" anywhere: that was the old, wrong key name.
    expect(JSON.stringify(a)).not.toContain('"pk"');
  });
});
