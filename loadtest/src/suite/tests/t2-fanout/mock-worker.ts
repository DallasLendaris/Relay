/**
 * Mock push-service process: either one mock APNs (HTTP/2 over TLS, self-signed certificate) or
 * one or more mock FCM project servers, each with its own project quota bucket (the real
 * 600,000/min-per-project limit is per Firebase project). Uses the existing loadtest mocks.
 * 'dump' writes one 64-bit token hash per accepted push (and per "unregistered" answer) to files.
 */
import { writeFileSync } from 'node:fs';
import { startMockApns, type MockApns } from '../../../mock-apns.js';
import { startMockFcm, type MockFcm } from '../../../mock-fcm.js';
import { serve } from './proc.js';
import { HashList } from './verify.js';

let apns: MockApns | undefined;
const fcm = new Map<string, MockFcm>();

const counters = () => [
  ...(apns ? [apns.counters] : []),
  ...[...fcm.values()].map((f) => f.counters),
];

serve({
  startApns: async (p: { seed: number }) => {
    apns = await startMockApns({ tls: true, seed: p.seed, trackAttempts: false });
    return { port: apns.port, cert: apns.cert };
  },
  startFcm: async (p: { projects: string[]; seed: number; quotaPerSec: number }) => {
    const ports: Record<string, number> = {};
    for (const [i, id] of p.projects.entries()) {
      const m = await startMockFcm({
        seed: p.seed + i * 101,
        ratePerSecond: p.quotaPerSec,
        trackAttempts: false,
      });
      fcm.set(id, m);
      ports[id] = m.port;
    }
    return { ports };
  },
  reset: () => {
    apns?.reset();
    for (const f of fcm.values()) f.reset();
    return true;
  },
  stats: () => {
    const s = {
      received: 0,
      ok: 0,
      unregistered: 0,
      tooManyRequests: 0,
      serverError: 0,
      rejected: 0,
      rateLimited: 0,
      goaways: 0,
      connections: 0,
    };
    if (apns) {
      const a = apns.stats();
      s.received += a.received;
      s.ok += a.ok;
      s.unregistered += a.unregistered;
      s.tooManyRequests += a.tooManyRequests;
      s.serverError += a.serverError;
      s.rejected += a.rejected;
      s.goaways += a.goaways;
      s.connections += a.connections;
    }
    for (const f of fcm.values()) {
      const x = f.stats();
      s.received += x.received;
      s.ok += x.ok;
      s.unregistered += x.unregistered;
      s.tooManyRequests += x.tooManyRequests;
      s.serverError += x.serverError;
      s.rejected += x.rejected;
      s.rateLimited += x.rateLimited;
    }
    return s;
  },
  dump: (p: { deliveredFile: string; deadFile: string }) => {
    const delivered = new HashList();
    const dead = new HashList();
    for (const c of counters()) {
      for (const [token, n] of c.delivered) for (let i = 0; i < n; i++) delivered.add(token);
      for (const token of c.unregistered.keys()) dead.add(token);
    }
    writeFileSync(p.deliveredFile, Buffer.from(delivered.toArray().buffer));
    writeFileSync(p.deadFile, Buffer.from(dead.toArray().buffer));
    return { delivered: delivered.n, dead: dead.n };
  },
});
