import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { describe, expect, it, vi } from 'vitest';
import { createAggregator, TOTALS_TTL_SECONDS } from '../../../src/scale/checkins/aggregator.js';

const ALERT = '11111111-1111-4111-8111-111111111111';
let n = 0;
const record = (cell: string) => {
  n++;
  return {
    eventID: `e${n}`,
    eventName: 'INSERT' as const,
    dynamodb: {
      SequenceNumber: String(1000 + n),
      NewImage: { alertId: { S: ALERT }, h3Cell: { S: cell }, deviceId: { S: `d${n}` } },
    },
  };
};

describe('check-in totals TTL', () => {
  it('sets ttl with if_not_exists on the counter row (first write wins, later batches never extend it)', async () => {
    const sent: TransactWriteCommand[] = [];
    let t = Date.UTC(2026, 10, 9, 12);
    const run = createAggregator({
      doc: {
        send: async (cmd: unknown) => {
          sent.push(cmd as TransactWriteCommand);
          return {};
        },
      } as never,
      totalsTable: 'totals',
      now: () => new Date(t),
    });
    vi.spyOn(console, 'log').mockImplementation(() => {});
    await run({ Records: [record('c1')] } as never);
    t += 3600_000;
    await run({ Records: [record('c1')] } as never);

    const updates = sent.map((c) => c.input.TransactItems?.[0]?.Update);
    for (const u of updates) {
      expect(u?.UpdateExpression).toContain('#ttl = if_not_exists(#ttl, :ttl)');
      expect(u?.ExpressionAttributeNames).toEqual({ '#ttl': 'ttl' });
    }
    expect(updates[0]?.ExpressionAttributeValues?.[':ttl']).toBe(
      Math.floor(Date.UTC(2026, 10, 9, 12) / 1000) + TOTALS_TTL_SECONDS,
    );
    expect(TOTALS_TTL_SECONDS).toBe(30 * 24 * 3600);
  });
});
