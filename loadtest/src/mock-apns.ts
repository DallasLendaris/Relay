/**
 * Mock APNs: an HTTP/2 server for `POST /3/device/{token}` with the validation, latency, error
 * mix, per-connection stream cap and GOAWAY behaviour the sender has to cope with.
 *
 * Run standalone: `npm run mock:apns -w @relay/loadtest -- --port 8443 --tls`
 */
import { randomUUID } from 'node:crypto';
import http2 from 'node:http2';
import type { AddressInfo } from 'node:net';
import {
  DEFAULT_ERROR_MIX,
  DEFAULT_LATENCY,
  TokenCounters,
  createLatencySampler,
  createRng,
  emptyResponseStats,
  generateSelfSignedCert,
  isMain,
  logJson,
  numberArg,
  parseArgs,
  pickOutcome,
  sleep,
  type ErrorMix,
  type LatencyConfig,
  type ResponseStats,
  type TlsMaterial,
} from './mock-common.js';

export interface MockApnsOptions {
  /** Keep a per-token attempt count (default true). Large runs turn it off to save memory. */
  trackAttempts?: boolean;
  /** 0 picks a free port. */
  port?: number;
  host?: string;
  /** `true` generates a self-signed certificate; pass key and cert to supply one; false is h2c. */
  tls?: boolean | TlsMaterial;
  seed?: number;
  latency?: LatencyConfig;
  errorMix?: ErrorMix;
  /** SETTINGS_MAX_CONCURRENT_STREAMS per connection (APNs advertises about 1,000). */
  maxConcurrentStreams?: number;
  /** Probability per request of sending a graceful GOAWAY on that connection. */
  goawayProbability?: number;
  /**
   * Fault injection (suite test t5). Called for every request that passed validation; a returned
   * response is sent instead of the random error mix (and the token is not delivered).
   */
  inject?: (token: string) => InjectedResponse | undefined;
}

/** A canned response a fault-injection hook can return (shared with the mock FCM). */
export interface InjectedResponse {
  status: number;
  body: object;
  headers?: Record<string, string>;
}

export interface MockApnsStats extends ResponseStats {
  connections: number;
  goaways: number;
  /** Streams the handler refused itself (a fallback; nghttp2 normally refuses first and those are not counted). */
  refusedStreams: number;
  /** The most streams ever open at once on one connection. */
  peakConcurrentStreams: number;
}

export interface MockApns {
  /** `http://host:port` (h2c) or `https://localhost:port`. */
  url: string;
  port: number;
  /** PEM certificate to pass as `ca` to a client; undefined for h2c. */
  cert: string | undefined;
  counters: TokenCounters;
  stats(): MockApnsStats;
  /** Clears counters and stats. */
  reset(): void;
  setErrorMix(mix: ErrorMix): void;
  close(): Promise<void>;
}

const PUSH_TYPES = new Set([
  'alert',
  'background',
  'location',
  'voip',
  'complication',
  'fileprovider',
  'mdm',
  'liveactivity',
  'pushtotalk',
]);

const PATH = /^\/3\/device\/([0-9a-fA-F]{32,200})$/;

interface SessionState {
  /** Highest stream id this connection has accepted (the GOAWAY last-stream-id). */
  lastStreamId: number;
  active: number;
  goawaySent: boolean;
}

export async function startMockApns(options: MockApnsOptions = {}): Promise<MockApns> {
  const maxStreams = options.maxConcurrentStreams ?? 1000;
  const goawayProbability = options.goawayProbability ?? 0.0005;
  const seed = options.seed ?? 1;
  let errorMix = options.errorMix ?? DEFAULT_ERROR_MIX;
  const outcomeRng = createRng(seed);
  const goawayRng = createRng(seed ^ 0x9e3779b9);
  const sampleLatency = createLatencySampler(
    options.latency ?? DEFAULT_LATENCY,
    createRng(seed + 1),
  );

  const counters = new TokenCounters(options.trackAttempts !== false);
  let stats = emptyResponseStats();
  let connections = 0;
  let goaways = 0;
  let refused = 0;
  let peak = 0;

  let tls: TlsMaterial | undefined;
  if (options.tls === true) tls = await generateSelfSignedCert();
  else if (options.tls) tls = options.tls;

  const settings = { maxConcurrentStreams: maxStreams };
  const server = tls
    ? http2.createSecureServer({ key: tls.key, cert: tls.cert, settings })
    : http2.createServer({ settings });

  const sessions = new Set<http2.Http2Session>();
  const states = new WeakMap<http2.Http2Session, SessionState>();

  server.on('session', (session) => {
    connections++;
    sessions.add(session);
    states.set(session, { lastStreamId: 0, active: 0, goawaySent: false });
    session.on('close', () => sessions.delete(session));
    session.on('error', () => session.destroy());
  });

  const finish = (stream: http2.ServerHttp2Stream, status: number, body: object, extra = {}) => {
    if (stream.destroyed || stream.closed) return;
    stream.respond({
      ':status': status,
      'content-type': 'application/json',
      'apns-id': randomUUID(),
      ...extra,
    });
    stream.end(JSON.stringify(body));
  };

  const reject = (stream: http2.ServerHttp2Stream, status: number, reason: string) => {
    stats.rejected++;
    finish(stream, status, { reason });
  };

  server.on('stream', (stream: http2.ServerHttp2Stream, headers: http2.IncomingHttpHeaders) => {
    const session = stream.session;
    const state = session ? states.get(session) : undefined;
    if (!session || !state) {
      stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR);
      return;
    }
    stream.on('error', () => {});
    if (state.goawaySent) {
      // After GOAWAY the server processes nothing new (the client was told which streams count).
      refused++;
      stream.close(http2.constants.NGHTTP2_REFUSED_STREAM);
      return;
    }
    if (state.active >= maxStreams) {
      refused++;
      stream.close(http2.constants.NGHTTP2_REFUSED_STREAM);
      return;
    }
    state.active++;
    state.lastStreamId = Math.max(state.lastStreamId, stream.id ?? 0);
    peak = Math.max(peak, state.active);
    stream.on('close', () => {
      state.active--;
    });
    stream.on('error', () => {});

    const chunks: Buffer[] = [];
    stream.on('data', (c: Buffer) => chunks.push(c));
    stream.on('end', () => {
      void handle(stream, headers, Buffer.concat(chunks).toString('utf8'), session, state);
    });
  });

  async function handle(
    stream: http2.ServerHttp2Stream,
    headers: http2.IncomingHttpHeaders,
    body: string,
    session: http2.Http2Session,
    state: SessionState,
  ): Promise<void> {
    if (headers[':method'] !== 'POST') return reject(stream, 405, 'MethodNotAllowed');
    const path = String(headers[':path'] ?? '');
    const match = PATH.exec(path);
    if (!path.startsWith('/3/device/')) return reject(stream, 404, 'BadPath');
    if (!match || !match[1]) return reject(stream, 400, 'BadDeviceToken');
    const token = match[1].toLowerCase();

    const auth = String(headers['authorization'] ?? '');
    if (!/^bearer\s+\S+$/i.test(auth)) return reject(stream, 403, 'MissingProviderToken');
    if (!headers['apns-topic']) return reject(stream, 400, 'MissingTopic');
    const pushType = String(headers['apns-push-type'] ?? '');
    if (!PUSH_TYPES.has(pushType)) return reject(stream, 400, 'InvalidPushType');
    try {
      const payload: unknown = JSON.parse(body);
      if (typeof payload !== 'object' || payload === null) throw new Error('not an object');
    } catch {
      return reject(stream, 400, 'PayloadEmpty');
    }

    stats.received++;
    counters.recordAttempt(token);
    const injected = options.inject?.(token);
    const outcome = pickOutcome(errorMix, outcomeRng);
    await sleep(sampleLatency());

    if (injected) {
      finish(stream, injected.status, injected.body, injected.headers ?? {});
    } else
      switch (outcome) {
        case 'unregistered':
          stats.unregistered++;
          counters.recordUnregistered(token);
          finish(stream, 410, { reason: 'Unregistered', timestamp: Date.now() });
          break;
        case 'tooManyRequests':
          stats.tooManyRequests++;
          finish(stream, 429, { reason: 'TooManyRequests' });
          break;
        case 'serverError':
          stats.serverError++;
          finish(stream, 500, { reason: 'InternalServerError' });
          break;
        default:
          stats.ok++;
          counters.recordDelivered(token);
          finish(stream, 200, {});
      }

    if (goawayProbability > 0 && !state.goawaySent && goawayRng() < goawayProbability) {
      state.goawaySent = true;
      goaways++;
      // Graceful: streams already accepted finish, new streams must use a new connection.
      // The last-stream-id must be the highest accepted stream: with the default of 0 the client
      // is told none were processed, discards answers already sent and resends delivered pushes.
      session.goaway(http2.constants.NGHTTP2_NO_ERROR, state.lastStreamId);
    }
  }

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(options.port ?? 0, options.host ?? '127.0.0.1', resolve);
  });
  const port = (server.address() as AddressInfo).port;

  return {
    url: tls ? `https://localhost:${port}` : `http://${options.host ?? '127.0.0.1'}:${port}`,
    port,
    cert: tls?.cert,
    counters,
    stats: () => ({
      ...stats,
      connections,
      goaways,
      refusedStreams: refused,
      peakConcurrentStreams: peak,
    }),
    reset: () => {
      counters.reset();
      stats = emptyResponseStats();
      connections = 0;
      goaways = 0;
      refused = 0;
      peak = 0;
    },
    setErrorMix: (mix) => {
      errorMix = mix;
    },
    close: () =>
      new Promise<void>((resolve) => {
        for (const s of sessions) s.destroy();
        server.close(() => resolve());
      }),
  };
}

if (isMain(import.meta.url)) {
  const args = parseArgs(process.argv.slice(2));
  const p50 = numberArg(args, 'p50');
  const p99 = numberArg(args, 'p99');
  const apns = await startMockApns({
    port: numberArg(args, 'port') ?? 8443,
    host: args['host'] ?? '127.0.0.1',
    tls: args['tls'] === 'true',
    seed: numberArg(args, 'seed'),
    latency: {
      p50Ms: p50 ?? DEFAULT_LATENCY.p50Ms,
      p99Ms: p99 ?? DEFAULT_LATENCY.p99Ms,
    },
    maxConcurrentStreams: numberArg(args, 'max-streams'),
    goawayProbability: numberArg(args, 'goaway'),
  });
  logJson('mock-apns.listening', { url: apns.url, port: apns.port });
  setInterval(() => logJson('mock-apns.stats', { ...apns.stats() }), 10_000).unref();
  const stop = () => {
    logJson('mock-apns.stopped', { ...apns.stats() });
    void apns.close().then(() => process.exit(0));
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}
