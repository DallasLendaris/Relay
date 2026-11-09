/**
 * Mock FCM: an HTTP/1.1 server for `POST /v1/projects/{id}/messages:send` plus an OAuth token
 * endpoint (`POST /token`). It enforces a project-wide token-bucket quota and answers 429 with
 * Retry-After when it is exceeded.
 *
 * Run standalone: `npm run mock:fcm -w @relay/loadtest -- --port 8080 --rate 10000`
 */
import { randomBytes, randomUUID } from 'node:crypto';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  DEFAULT_ERROR_MIX,
  DEFAULT_LATENCY,
  TokenBucket,
  TokenCounters,
  createLatencySampler,
  createRng,
  emptyResponseStats,
  isMain,
  logJson,
  numberArg,
  parseArgs,
  pickOutcome,
  sleep,
  type ErrorMix,
  type LatencyConfig,
  type ResponseStats,
} from './mock-common.js';

export interface MockFcmOptions {
  /** Keep a per-token attempt count (default true). Large runs turn it off to save memory. */
  trackAttempts?: boolean;
  port?: number;
  host?: string;
  seed?: number;
  latency?: LatencyConfig;
  errorMix?: ErrorMix;
  /**
   * Project quota in messages per second. Default 10,000, which mimics the documented
   * 600,000 per minute. Verify against docs/scale-quotas.md. 0 disables the limit.
   */
  ratePerSecond?: number;
  /** Bucket size (a burst this large is accepted at once). Default: one second of quota. */
  burst?: number;
  /** Clock for the token bucket and OAuth expiry, in milliseconds. Injectable for tests. */
  now?: () => number;
  /** Lifetime of issued OAuth access tokens. Default 3600 s, like Google. */
  tokenTtlSeconds?: number;
}

export interface MockFcmStats extends ResponseStats {
  /** Messages refused by the quota bucket (not counted in `tooManyRequests`, the injected 429s). */
  rateLimited: number;
  oauthRequests: number;
}

export interface MockFcm {
  url: string;
  port: number;
  counters: TokenCounters;
  stats(): MockFcmStats;
  reset(): void;
  setErrorMix(mix: ErrorMix): void;
  close(): Promise<void>;
}

const SEND_PATH = /^\/v1\/projects\/([^/]+)\/messages:send$/;

export async function startMockFcm(options: MockFcmOptions = {}): Promise<MockFcm> {
  const seed = options.seed ?? 1;
  const now = options.now ?? (() => performance.now());
  const rate = options.ratePerSecond ?? 10_000;
  const bucket = rate > 0 ? new TokenBucket(rate, options.burst ?? rate, now) : undefined;
  const ttlMs = (options.tokenTtlSeconds ?? 3600) * 1000;
  let errorMix = options.errorMix ?? DEFAULT_ERROR_MIX;
  const outcomeRng = createRng(seed);
  const sampleLatency = createLatencySampler(
    options.latency ?? DEFAULT_LATENCY,
    createRng(seed + 1),
  );

  const counters = new TokenCounters(options.trackAttempts !== false);
  let stats = emptyResponseStats();
  let rateLimited = 0;
  let oauthRequests = 0;
  const accessTokens = new Map<string, number>();

  const send = (res: http.ServerResponse, status: number, body: object, headers = {}) => {
    if (res.destroyed) return;
    res.writeHead(status, { 'content-type': 'application/json', ...headers });
    res.end(JSON.stringify(body));
  };

  const fail = (
    res: http.ServerResponse,
    status: number,
    state: string,
    message: string,
    errorCode?: string,
    headers = {},
  ) => {
    send(
      res,
      status,
      {
        error: {
          code: status,
          message,
          status: state,
          ...(errorCode
            ? {
                details: [
                  { '@type': 'type.googleapis.com/google.firebase.fcm.v1.FcmError', errorCode },
                ],
              }
            : {}),
        },
      },
      headers,
    );
  };

  const readBody = (req: http.IncomingMessage): Promise<string> =>
    new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      req.on('data', (c: Buffer) => chunks.push(c));
      req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
      req.on('error', reject);
    });

  const server = http.createServer((req, res) => {
    void (async () => {
      const path = (req.url ?? '').split('?')[0] ?? '';
      if (req.method === 'POST' && path === '/token') {
        oauthRequests++;
        const body = await readBody(req);
        const assertion =
          /(?:^|&)assertion=([^&]+)/.exec(body)?.[1] ??
          (() => {
            try {
              return (JSON.parse(body) as { assertion?: string }).assertion;
            } catch {
              return undefined;
            }
          })();
        if (!assertion) {
          return send(res, 400, {
            error: 'invalid_request',
            error_description: 'Missing assertion',
          });
        }
        const accessToken = `ya29.${randomBytes(24).toString('base64url')}`;
        accessTokens.set(accessToken, now() + ttlMs);
        return send(res, 200, {
          access_token: accessToken,
          expires_in: Math.floor(ttlMs / 1000),
          token_type: 'Bearer',
        });
      }

      const match = SEND_PATH.exec(path);
      if (!match) {
        stats.rejected++;
        return fail(res, 404, 'NOT_FOUND', 'Unknown path');
      }
      if (req.method !== 'POST') {
        stats.rejected++;
        return fail(res, 405, 'METHOD_NOT_ALLOWED', 'Use POST');
      }
      const body = await readBody(req);

      const auth = /^bearer\s+(\S+)$/i.exec(String(req.headers['authorization'] ?? ''));
      const expiry = auth?.[1] ? accessTokens.get(auth[1]) : undefined;
      if (expiry === undefined || expiry <= now()) {
        stats.rejected++;
        return fail(res, 401, 'UNAUTHENTICATED', 'Missing or expired OAuth access token');
      }

      if (bucket) {
        const wait = bucket.take();
        if (wait > 0) {
          rateLimited++;
          return fail(res, 429, 'RESOURCE_EXHAUSTED', 'Quota exceeded', 'QUOTA_EXCEEDED', {
            'retry-after': String(Math.max(1, Math.ceil(wait))),
          });
        }
      }

      let token: string | undefined;
      try {
        const message = (JSON.parse(body) as { message?: { token?: unknown } }).message;
        if (typeof message?.token === 'string' && message.token.length > 0) token = message.token;
      } catch {
        // falls through to the INVALID_ARGUMENT below
      }
      if (!token) {
        stats.rejected++;
        return fail(res, 400, 'INVALID_ARGUMENT', 'message.token is required', 'INVALID_ARGUMENT');
      }

      stats.received++;
      counters.recordAttempt(token);
      const outcome = pickOutcome(errorMix, outcomeRng);
      await sleep(sampleLatency());

      switch (outcome) {
        case 'unregistered':
          stats.unregistered++;
          counters.recordUnregistered(token);
          return fail(res, 404, 'NOT_FOUND', 'Requested entity was not found.', 'UNREGISTERED');
        case 'tooManyRequests':
          stats.tooManyRequests++;
          return fail(res, 429, 'RESOURCE_EXHAUSTED', 'Quota exceeded', 'QUOTA_EXCEEDED', {
            'retry-after': '1',
          });
        case 'serverError':
          stats.serverError++;
          return fail(res, 500, 'INTERNAL', 'Internal error encountered.', 'INTERNAL');
        default:
          stats.ok++;
          counters.recordDelivered(token);
          return send(res, 200, { name: `projects/${match[1]}/messages/${randomUUID()}` });
      }
    })().catch(() => {
      if (!res.headersSent) fail(res, 500, 'INTERNAL', 'Mock failure');
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(options.port ?? 0, options.host ?? '127.0.0.1', resolve);
  });
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://${options.host ?? '127.0.0.1'}:${port}`,
    port,
    counters,
    stats: () => ({ ...stats, rateLimited, oauthRequests }),
    reset: () => {
      counters.reset();
      stats = emptyResponseStats();
      rateLimited = 0;
      oauthRequests = 0;
    },
    setErrorMix: (mix) => {
      errorMix = mix;
    },
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

if (isMain(import.meta.url)) {
  const args = parseArgs(process.argv.slice(2));
  const fcm = await startMockFcm({
    port: numberArg(args, 'port') ?? 8080,
    host: args['host'] ?? '127.0.0.1',
    seed: numberArg(args, 'seed'),
    ratePerSecond: numberArg(args, 'rate'),
    latency: {
      p50Ms: numberArg(args, 'p50') ?? DEFAULT_LATENCY.p50Ms,
      p99Ms: numberArg(args, 'p99') ?? DEFAULT_LATENCY.p99Ms,
    },
  });
  logJson('mock-fcm.listening', { url: fcm.url, port: fcm.port });
  setInterval(() => logJson('mock-fcm.stats', { ...fcm.stats() }), 10_000).unref();
  const stop = () => {
    logJson('mock-fcm.stopped', { ...fcm.stats() });
    void fcm.close().then(() => process.exit(0));
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
}
