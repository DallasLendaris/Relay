import { describe, expect, it } from 'vitest';
import { alertEndTime } from '../../../src/delivery/eligibility.js';
import { makeCheckInTokenFor } from '../../../src/scale/fanout/handlers.js';
import { processShard } from '../../../src/scale/fanout/shard-worker.js';
import { verifyCheckInToken } from '../../../src/tokens/index.js';
import { countyCells, entry, NOW, setup, signed } from './helpers.js';

const KEY = { id: 'tk1', secret: 'shard-token-secret' };
const CELLS = countyCells().slice(0, 3) as string[];

describe('shard worker check-in tokens (the real factory used by shardHandler)', () => {
  it('every token in a shard-produced batch is a v2 token that binds its own cell', async () => {
    const alert = signed();
    const { deps, cellIndex, sendQueue } = setup(alert, {
      makeCheckInToken: makeCheckInTokenFor(KEY),
    });
    for (const cell of CELLS) {
      for (let i = 0; i < 40; i++) cellIndex.add(entry(cell, `dev-${cell.slice(-4)}-${i}`));
    }
    await processShard(
      {
        alertId: alert.id,
        sourceId: alert.sourceId,
        relaySeverity: alert.relaySeverity,
        shardId: 's:0',
        cells: CELLS,
      },
      deps,
    );

    const keys = new Map([[KEY.id, KEY.secret]]);
    const batches = sendQueue.batches('urgent');
    let checked = 0;
    for (const b of batches) {
      const cell = b.batchKey.split(':')[2] as string;
      for (const r of b.recipients) {
        // Decode the raw payload: it must be version 2 with the cell in `c`.
        const payload = JSON.parse(
          Buffer.from(r.checkInToken.split('.')[0] as string, 'base64url').toString('utf8'),
        ) as Record<string, unknown>;
        expect(payload).toMatchObject({ v: 2, a: alert.id, d: r.deviceId, c: cell, kid: KEY.id });
        expect(payload['exp']).toBe(Math.floor(alertEndTime(alert).getTime() / 1000));
        // And it verifies, returning the cell (what the check-in API relies on).
        expect(
          verifyCheckInToken(
            r.checkInToken,
            { alertId: alert.id, deviceId: r.deviceId, now: NOW },
            keys,
          ),
        ).toEqual({ ok: true, h3Cell: cell });
        checked++;
      }
    }
    expect(checked).toBe(CELLS.length * 40);
  });

  it('a token cannot be moved to another cell: changing c breaks the signature', () => {
    const make = makeCheckInTokenFor(KEY);
    const token = make('alert-1', 'dev-1', CELLS[0] as string, new Date('2026-11-10T14:00:00Z'));
    const [body, sig] = token.split('.') as [string, string];
    const forged = {
      ...(JSON.parse(Buffer.from(body, 'base64url').toString('utf8')) as object),
      c: CELLS[1],
    };
    const forgedToken = `${Buffer.from(JSON.stringify(forged)).toString('base64url')}.${sig}`;
    expect(
      verifyCheckInToken(
        forgedToken,
        { alertId: 'alert-1', deviceId: 'dev-1', now: NOW },
        new Map([[KEY.id, KEY.secret]]),
      ),
    ).toEqual({ ok: false, reason: 'bad_signature' });
  });
});
