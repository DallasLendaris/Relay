import { describe, expect, it } from 'vitest';
import { runCheckinBurst } from '../src/sim/checkin-burst.js';
import { runFanoutSim } from '../src/sim/fanout-sim.js';

/**
 * CI-sized version of the local million-device simulation: 20,000 devices through the real
 * planner, shard workers, processBatch, ApnsSender and FcmSender against the mock servers
 * (default error mix, FCM quota 10,000/s), then the check-in burst through the real API route
 * and aggregator.
 */
describe('fan-out simulation (20,000 devices)', () => {
  it('delivers exactly once, re-plans with zero new pushes, and counts every check-in', async () => {
    const t0 = performance.now();
    const r = await runFanoutSim({ devices: 20_000, senders: 2, collectTokens: true });

    // Every device with a live target got exactly one push; dead tokens are excused.
    expect(r.exactlyOnce.missing).toBe(0);
    expect(r.exactlyOnce.unexpectedTokens).toBe(0);
    // A duplicate is only acceptable after a lost answer (timeout); never unexplained.
    expect(r.exactlyOnce.duplicatesUnexplained).toBe(0);
    expect(r.exactlyOnce.unexpectedDuplicates).toBe(0);
    expect(r.exactlyOnce.ok).toBe(true);
    expect(r.exactlyOnce.expectedDevices).toBe(r.counts.devicesWithTarget);
    expect(r.exactlyOnce.deliveredOnce + r.exactlyOnce.excusedPermanent).toBe(
      r.exactlyOnce.expectedDevices - r.exactlyOnce.knownTwoCellDuplicates,
    );
    // Devices with places in two alert cells exist, and the primary-cell rule (seed entries carry
    // deviceCells) sends each of them once: no cross-cell duplicates at all.
    expect(r.counts.devicesInTwoOrMoreCells).toBeGreaterThan(0);
    expect(r.exactlyOnce.knownTwoCellDuplicates).toBe(0);
    // The error mix was really on: dead tokens and retries happened.
    expect(r.pass1.permanentFailed).toBeGreaterThan(0);
    expect(r.pass1.retried).toBeGreaterThan(0);
    expect(r.pass1.gaveUp).toBe(0);
    expect(r.timing.lastPushMs).not.toBeNull();

    // Re-planning the same alert sends nothing: every batch is already marked sent.
    expect(r.replan.batchesProcessed).toBe(r.counts.plannedBatches);
    expect(r.replan.byReason).toEqual({ duplicate: r.counts.plannedBatches });
    expect(r.replan.newPushRequests).toBe(0);
    expect(r.replan.newAcceptedPushes).toBe(0);
    expect(r.replan.newPushAttemptsInSender).toBe(0);

    // Check-in burst: totals add up to devices in the area minus permanent failures.
    const burst = await runCheckinBurst({
      checkInTokens: r.tokens?.checkInTokens ?? [],
      permanentFailureDevices: r.permanentFailureDevices,
      deliveredWithoutAck: r.deliveredWithoutAck,
      devicesInArea: r.counts.devicesInArea,
    });
    expect(burst.api.errors).toBe(0);
    expect(burst.aggregator.failedItems).toBe(0);
    expect(burst.verification.perCellMismatches).toBe(0);
    expect(burst.verification.totalsSum).toBe(
      r.counts.devicesInArea - r.permanentFailureDevices - r.deliveredWithoutAck,
    );
    expect(burst.verification.ok).toBe(true);

    expect(performance.now() - t0).toBeLessThan(60_000);
  }, 60_000);
});
