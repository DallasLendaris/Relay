/**
 * t7 child: a mock APNs or mock FCM server (the existing loadtest mocks) in its own process, with
 * a control port. The parent closes an "epoch" after each alert has drained: the mock answers with
 * order-independent fingerprints of the tokens it delivered, then clears its per-token counters so
 * memory stays flat.
 *
 * Args: apns | fcm
 */
import { startMockApns } from '../../../mock-apns.js';
import { startMockFcm } from '../../../mock-fcm.js';
import type { TokenCounters } from '../../../mock-common.js';
import { runWorkerMain, sendMessage } from '../../lib/child.js';
import {
  addToFingerprint,
  deferred,
  emptyFingerprint,
  readConfig,
  startSampler,
  startServer,
  type Fingerprint,
} from './common.js';

export interface EpochRequest {
  /** Tokens whose outcome is ambiguous (erased during the alert): left out of the fingerprints. */
  exclude: string[];
  /** Tokens that must not have been pushed at all. */
  forbid: string[];
}

export interface EpochReply {
  delivered: Fingerprint;
  unregistered: Fingerprint;
  /** Tokens delivered or unregistered more than once (capped list) and the true count. */
  duplicateCount: number;
  duplicateTokens: string[];
  /** Per excluded token: how many times it was delivered. */
  excluded: Record<string, number>;
  /** Forbidden tokens that were pushed anyway (capped list) and the true count. */
  forbiddenHits: number;
  forbiddenTokens: string[];
  stats: Record<string, number>;
}

function closeEpoch(counters: TokenCounters, req: EpochRequest): EpochReply {
  const exclude = new Set(req.exclude);
  const delivered = emptyFingerprint();
  const unregistered = emptyFingerprint();
  const duplicateTokens: string[] = [];
  let duplicateCount = 0;
  const excluded: Record<string, number> = {};
  for (const [token, n] of counters.delivered) {
    if (exclude.has(token)) {
      excluded[token] = n;
      continue;
    }
    addToFingerprint(delivered, token);
    if (n > 1) {
      duplicateCount++;
      if (duplicateTokens.length < 2000) duplicateTokens.push(token);
    }
  }
  for (const [token, n] of counters.unregistered) {
    if (exclude.has(token)) continue;
    addToFingerprint(unregistered, token);
    if (n > 1) {
      duplicateCount++;
      if (duplicateTokens.length < 2000) duplicateTokens.push(token);
    }
  }
  let forbiddenHits = 0;
  const forbiddenTokens: string[] = [];
  for (const token of req.forbid) {
    if (counters.delivered.has(token) || counters.unregistered.has(token)) {
      forbiddenHits++;
      if (forbiddenTokens.length < 50) forbiddenTokens.push(token);
    }
  }
  counters.reset();
  return {
    delivered,
    unregistered,
    duplicateCount,
    duplicateTokens,
    excluded,
    forbiddenHits,
    forbiddenTokens,
    stats: {},
  };
}

runWorkerMain(async () => {
  const kind = process.argv[2] === 'fcm' ? 'fcm' : 'apns';
  const cfg = readConfig();
  const mock =
    kind === 'apns'
      ? await startMockApns({ seed: 1, trackAttempts: false })
      : await startMockFcm({ seed: 1, trackAttempts: false, ratePerSecond: cfg.fcmRate });
  const totals = { received: 0, ok: 0, unregistered: 0, tooManyRequests: 0, serverError: 0 };
  const stop = deferred();
  const sampler = startSampler(`mock-${kind}`, cfg.sampleMs, () => ({
    deliveredTokens: mock.counters.delivered.size,
    unregisteredTokens: mock.counters.unregistered.size,
  }));
  const server = await startServer(async (r) => {
    if (r.method === 'POST' && r.path === '/epoch') {
      const s = mock.stats();
      const reply = closeEpoch(mock.counters, JSON.parse(r.body) as EpochRequest);
      totals.received += s.received;
      totals.ok += s.ok;
      totals.unregistered += s.unregistered;
      totals.tooManyRequests += s.tooManyRequests;
      totals.serverError += s.serverError;
      reply.stats = {
        received: s.received,
        ok: s.ok,
        unregistered: s.unregistered,
        tooManyRequests: s.tooManyRequests,
        serverError: s.serverError,
        rateLimited: 'rateLimited' in s ? Number(s.rateLimited) : 0,
      };
      mock.reset();
      return { body: reply };
    }
    if (r.method === 'GET' && r.path === '/totals') {
      const s = mock.stats();
      return {
        body: {
          received: totals.received + s.received,
          ok: totals.ok + s.ok,
          unregistered: totals.unregistered + s.unregistered,
          tooManyRequests: totals.tooManyRequests + s.tooManyRequests,
          serverError: totals.serverError + s.serverError,
        },
      };
    }
    if (r.method === 'POST' && r.path === '/stop') {
      stop.resolve();
      return { body: { ok: true } };
    }
    return { status: 404, body: 'not found' };
  });
  await sendMessage({ kind: 'ready', url: mock.url, ctl: server.port });
  await stop.promise;
  sampler();
  const s = mock.stats();
  server.close();
  await mock.close();
  return {
    received: totals.received + s.received,
    ok: totals.ok + s.ok,
    unregistered: totals.unregistered + s.unregistered,
  };
});
