/**
 * Follow-up planner targeting and the follow-up shard worker's gate (docs/cancellation.md), with
 * the earlier version's sent records set up by hand.
 */
import type { Alert } from '@relay/shared';
import { describe, expect, it } from 'vitest';
import { InMemoryAlertStore } from '../../../src/adapters/memory/alert-store.js';
import { MemoryIdempotencyStore } from '../../../src/adapters/memory/idempotency-store.js';
import { claimExpiry } from '../../../src/delivery/eligibility.js';
import { FOLLOW_UP_DELAY_SECONDS, versionTag } from '../../../src/delivery/follow-up.js';
import { noopLogger } from '../../../src/delivery/types.js';
import { MemoryCellIndex } from '../../../src/scale/cell-index/index.js';
import type { SendBatch, SendQueue, SendQueueName } from '../../../src/scale/contracts.js';
import {
  planFollowUp,
  processFollowUpShard,
  TOMBSTONE_VERSION,
} from '../../../src/scale/fanout/follow-up.js';
import { bucketFor } from '../../../src/scale/fanout/hash.js';
import { bucketKey } from '../../../src/scale/fanout/keys.js';
import { encodeMemberSet, SENT_HASH_BYTES } from '../../../src/scale/fanout/member-set.js';
import { isShardJob } from '../../../src/scale/fanout/messages.js';
import { MemoryShardDispatcher } from '../../../src/scale/fanout/queues.js';
import { signed, verifyAlert } from '../../delivery/fakes.js';

const NOW = new Date('2026-11-10T11:00:00Z');
const clock = { now: () => NOW };
const C1 = '872830828ffffff';
const C2 = '872830829ffffff';
const C3 = '87283082affffff';

class RecordingQueue implements SendQueue {
  readonly sent: Array<{ queue: SendQueueName; batch: SendBatch; delaySeconds?: number }> = [];
  async enqueue(queue: SendQueueName, batches: SendBatch[], opts?: { delaySeconds?: number }) {
    for (const batch of batches) {
      this.sent.push({
        queue,
        batch,
        ...(opts?.delaySeconds !== undefined ? { delaySeconds: opts.delaySeconds } : {}),
      });
    }
  }
}

async function setup(alert: Alert, devices: Array<{ id: string; cell: string }>) {
  const alerts = new InMemoryAlertStore();
  await alerts.save(alert);
  const cellIndex = new MemoryCellIndex();
  await cellIndex.put(
    devices.map((d) => ({
      h3Cell: d.cell,
      deviceId: d.id,
      placeId: `p-${d.id}`,
      target: { platform: 'ios' as const, token: `tok-${d.id}` },
      language: 'en' as const,
      deviceCells: [d.cell],
    })),
  );
  const idempotency = new MemoryIdempotencyStore(clock);
  const queue = new RecordingQueue();
  const deps = {
    alerts,
    verifyAlert,
    isDeliveryPaused: async () => false,
    clock,
    logger: noopLogger,
    cellIndex,
    sendQueue: queue,
    idempotency,
    makeCheckInToken: (a: string, d: string, c: string) => `tok:${a}:${d}:${c}`,
  };
  return { alerts, idempotency, queue, deps };
}

/** Marks the earlier version's batch for (cell, bucket of `members[0]`) sent with these members. */
async function sentBatch(
  store: MemoryIdempotencyStore,
  alert: Alert,
  severity: Alert['relaySeverity'],
  cell: string,
  members: string[],
  version: string,
) {
  const key = bucketKey(
    { id: alert.id, relaySeverity: severity },
    cell,
    bucketFor(members[0] ?? ''),
  );
  await store.claim(key, new Date(NOW.getTime() + 45_000));
  await store.markSent(key, claimExpiry(alert), {
    members: encodeMemberSet(members, SENT_HASH_BYTES) as Uint8Array,
    version,
  });
  return key;
}

const cancelled = (cells = [C1, C2]) =>
  signed(
    {
      sourceId: 'urn:oid:cancel',
      messageType: 'Cancel',
      state: 'cancelled',
      supersedes: { sourceId: 'urn:oid:orig', relaySeverity: 'warning' },
    },
    cells,
  );

const job = (alert: Alert, cells: string[], followUp: 'cancel' | 'update') => ({
  alertId: alert.id,
  sourceId: alert.sourceId,
  relaySeverity: alert.relaySeverity,
  shardId: 's:0',
  cells,
  followUp,
});

describe('follow-up shard worker: who was sent the earlier version', () => {
  it('targets members of sent earlier-version batches only, quiet and delayed for a cancel', async () => {
    const alert = cancelled();
    const { deps, idempotency, queue } = await setup(alert, [
      { id: 'dev-a', cell: C1 },
      { id: 'dev-b', cell: C1 },
    ]);
    const old = versionTag('urn:oid:orig');
    // dev-a was in the sent batch; dev-b shares its bucket only if the hash says so, so give it
    // its own record without it.
    await sentBatch(idempotency, alert, 'warning', C1, ['dev-a'], old);
    if (bucketFor('dev-b') !== bucketFor('dev-a')) {
      await sentBatch(idempotency, alert, 'warning', C1, ['dev-b-other'], old);
      // dev-b's bucket record has another member (it was sent from another cell): not targeted.
      const k = bucketKey({ id: alert.id, relaySeverity: 'warning' }, C1, bucketFor('dev-b'));
      await idempotency.markSent(k, claimExpiry(alert), {
        members: encodeMemberSet(['someone-else'], SENT_HASH_BYTES) as Uint8Array,
        version: old,
      });
    }
    const r = await processFollowUpShard(job(alert, [C1], 'cancel'), deps);
    expect(r.refused).toBeUndefined();
    const ids = queue.sent.flatMap((s) => s.batch.recipients.map((x) => x.deviceId));
    expect(ids).toEqual(['dev-a']);
    expect(queue.sent[0]).toMatchObject({
      queue: 'standard',
      delaySeconds: FOLLOW_UP_DELAY_SECONDS,
    });
    expect(queue.sent[0]?.batch.batchKey).toContain(`:fu.cancel.${versionTag(alert.sourceId)}:`);
    expect(r.notMember).toBe(1);
  });

  it('skips a batch sent for the current version (it carried the new text already)', async () => {
    const alert = signed(
      {
        sourceId: 'urn:oid:update',
        messageType: 'Update',
        state: 'updated',
        supersedes: { sourceId: 'urn:oid:orig', relaySeverity: 'warning' },
      },
      [C1],
    );
    const { deps, idempotency, queue } = await setup(alert, [{ id: 'dev-a', cell: C1 }]);
    await sentBatch(idempotency, alert, 'warning', C1, ['dev-a'], versionTag('urn:oid:update'));
    const r = await processFollowUpShard(job(alert, [C1], 'update'), deps);
    expect(r.current).toBe(1);
    expect(queue.sent).toEqual([]);
  });

  it('a cancel tombstones a batch that never went out, so a late original cannot follow it', async () => {
    const alert = cancelled([C1]);
    const { deps, idempotency, queue } = await setup(alert, [{ id: 'dev-a', cell: C1 }]);
    const r = await processFollowUpShard(job(alert, [C1], 'cancel'), deps);
    expect(r).toMatchObject({ notSent: 1, tombstoned: 1, recipients: 0 });
    expect(queue.sent).toEqual([]);
    const key = bucketKey({ id: alert.id, relaySeverity: 'warning' }, C1, bucketFor('dev-a'));
    // The original batch, still queued, now finds its key sent: a duplicate to the sender.
    expect(await idempotency.claim(key, new Date(NOW.getTime() + 45_000))).toBe('sent');
    expect((await idempotency.inspect(key)).version).toBe(TOMBSTONE_VERSION);
    // A re-run of the job reads the tombstone as "never sent".
    const again = await processFollowUpShard(job(alert, [C1], 'cancel'), deps);
    expect(again).toMatchObject({ notSent: 1, tombstoned: 0, recipients: 0 });
  });

  it('an update never tombstones (the new version uses the same keys)', async () => {
    const alert = signed(
      {
        sourceId: 'urn:oid:update',
        state: 'updated',
        supersedes: { sourceId: 'urn:oid:orig', relaySeverity: 'warning' },
      },
      [C1],
    );
    const { deps, idempotency } = await setup(alert, [{ id: 'dev-a', cell: C1 }]);
    const r = await processFollowUpShard(job(alert, [C1], 'update'), deps);
    expect(r).toMatchObject({ notSent: 1, tombstoned: 0 });
    const key = bucketKey({ id: alert.id, relaySeverity: 'warning' }, C1, bucketFor('dev-a'));
    expect(await idempotency.peek(key)).toBe('absent');
  });

  it('an earlier batch still being sent: targeted, and its follow-up delayed', async () => {
    const alert = signed(
      {
        sourceId: 'urn:oid:update',
        state: 'updated',
        supersedes: { sourceId: 'urn:oid:orig', relaySeverity: 'warning' },
      },
      [C1],
    );
    const { deps, idempotency, queue } = await setup(alert, [{ id: 'dev-a', cell: C1 }]);
    const key = bucketKey({ id: alert.id, relaySeverity: 'warning' }, C1, bucketFor('dev-a'));
    await idempotency.claim(key, new Date(NOW.getTime() + 45_000));
    const r = await processFollowUpShard(job(alert, [C1], 'update'), deps);
    expect(r.inFlight).toBe(1);
    expect(queue.sent).toEqual([
      expect.objectContaining({ queue: 'urgent', delaySeconds: FOLLOW_UP_DELAY_SECONDS }),
    ]);
  });

  it('refuses a follow-up for a version that is no longer current', async () => {
    const alert = cancelled([C1]);
    const { deps, queue } = await setup(alert, [{ id: 'dev-a', cell: C1 }]);
    const r = await processFollowUpShard(
      { ...job(alert, [C1], 'cancel'), sourceId: 'urn:oid:x' },
      deps,
    );
    expect(r.refused).toBe('stale');
    expect(queue.sent).toEqual([]);
  });
});

describe('follow-up planner: the previous version area', () => {
  async function plan(alert: Alert, request: 'cancel' | 'update', dropped: string[] = []) {
    const alerts = new InMemoryAlertStore();
    await alerts.save(alert);
    const shardDispatcher = new MemoryShardDispatcher();
    const r = await planFollowUp(
      { alertId: alert.id, sourceId: alert.sourceId, followUp: request },
      {
        alerts,
        verifyAlert,
        isDeliveryPaused: async () => false,
        clock,
        logger: noopLogger,
        shardDispatcher,
        droppedCells: async () => dropped,
      },
    );
    return {
      r,
      cells: shardDispatcher.jobs.flatMap((j) => j.cells).sort(),
      jobs: shardDispatcher.jobs,
    };
  }

  it('a cancel visits every cell of the cancelled version, as follow-up shard jobs', async () => {
    const { cells, jobs } = await plan(cancelled([C1, C2]), 'cancel');
    expect(cells).toEqual([C1, C2]);
    expect(jobs.every((j) => j.followUp === 'cancel' && isShardJob(j))).toBe(true);
  });

  it('a same-severity update visits kept and dropped cells; a severity change only dropped ones', async () => {
    const same = signed(
      {
        sourceId: 'urn:oid:u',
        state: 'updated',
        supersedes: { sourceId: 'urn:oid:o', relaySeverity: 'warning' },
      },
      [C1],
    );
    expect((await plan(same, 'update', [C3])).cells).toEqual([C1, C3].sort());
    const raised = { ...same, relaySeverity: 'emergency' as const };
    const resigned = signed({ ...raised }, [C1]);
    expect((await plan(resigned, 'update', [C3])).cells).toEqual([C3]);
  });

  it('refuses a cancel follow-up for an alert that is not cancelled (stale message)', async () => {
    const active = signed({ sourceId: 'urn:oid:cancel' }, [C1]);
    expect((await plan(active, 'cancel')).r.refused).toBe('stale');
  });
});
