/**
 * Child process of t1: runs the existing single-process fan-out simulation plus the check-in burst,
 * exactly as `src/sim/run-fanout.ts` does, and sends the results to the parent (see ../lib/child.ts).
 * Args: --devices N --senders N
 */
import { numberArg, parseArgs } from '../../mock-common.js';
import { runCheckinBurst } from '../../sim/checkin-burst.js';
import { runFanoutSim } from '../../sim/fanout-sim.js';
import { runWorkerMain } from '../lib/child.js';

runWorkerMain(async () => {
  const args = parseArgs(process.argv.slice(2));
  const log = (line: string) => console.error(`[t1] ${line}`);
  const fanout = await runFanoutSim({
    devices: numberArg(args, 'devices') ?? 1_000_000,
    fcmRate: 10_000,
    senders: numberArg(args, 'senders') ?? 2,
    apnsSessions: 4,
    seed: 1,
    collectTokens: true,
    log,
  });
  const tokens = fanout.tokens;
  const burst = tokens
    ? await runCheckinBurst({
        checkInTokens: tokens.checkInTokens,
        permanentFailureDevices: fanout.permanentFailureDevices,
        deliveredWithoutAck: fanout.deliveredWithoutAck,
        devicesInArea: fanout.counts.devicesInArea,
        log,
      })
    : null;
  const { tokens: _tokens, ...fanoutJson } = fanout;
  void _tokens;
  return { fanout: fanoutJson, burst };
});
