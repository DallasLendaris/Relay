import { latLngToCell, gridDisk } from 'h3-js';
import { describe, expect, it } from 'vitest';
import { PostgresAlertStore } from '../../src/adapters/postgres/alert-store.js';
import { SigningAlertStore } from '../../src/adapters/signing-alert-store.js';
import { createAlertVerifier } from '../../src/signing/index.js';
import { alert, freshDb } from './helpers.js';
import { TEST_SIGNING } from '../signing/test-signing.js';

// Delivery refuses any alert whose signature fails, so an alert must still verify after a
// trip through the real Postgres store (cells are stored in a separate table and reassembled).
describe('signed alert survives a Postgres round trip', () => {
  it('verifies after save and reload, even when cells arrive unsorted with duplicates', async () => {
    const db = await freshDb();
    const key = TEST_SIGNING.signer;
    const store = new SigningAlertStore(new PostgresAlertStore(db), key);
    const verify = createAlertVerifier(TEST_SIGNING.publicKeys);

    const cells = gridDisk(latLngToCell(37.33, -121.88, 7), 3);
    const shuffled = [...cells].reverse().concat(cells.slice(0, 5));
    const input = alert({ area: { ...alert().area, h3Cells: shuffled } });

    await store.save(input);
    const loaded = await store.get(input.id);

    expect(loaded).not.toBeNull();
    if (!loaded) return;
    expect(loaded.area.h3Cells).toEqual([...new Set(cells)].sort());
    expect(verify(loaded)).toBe(true);
  }, 60_000);
});
