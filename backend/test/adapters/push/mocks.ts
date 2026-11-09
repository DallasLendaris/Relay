import { generateKeyPairSync, type KeyObject } from 'node:crypto';
import { createServer as createHttpServer, type Server } from 'node:http';
import {
  createServer as createH2cServer,
  type Http2Server,
  type Http2Session,
  type ServerHttp2Stream,
} from 'node:http2';
import type { AddressInfo } from 'node:net';
import type { NativePushMessage } from '../../../src/scale/contracts.js';

export function makeMessage(
  over: Partial<NativePushMessage> & { token?: string } = {},
): NativePushMessage {
  const { token, ...rest } = over;
  return {
    target: { platform: 'ios', token: token ?? 'ok-token' },
    title: 'Evacuation order',
    body: 'Leave now',
    data: { alertId: 'a1', checkInToken: 'c1', expiresAt: '2030-01-01T00:00:00Z' },
    priority: 'high',
    sound: true,
    collapseKey: 'alert-a1',
    ttlSeconds: 3600,
    category: 'alert-checkin',
    ...rest,
  };
}

export function ecKeys(): { privatePem: string; publicKey: KeyObject } {
  const { privateKey, publicKey } = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privatePem: privateKey.export({ type: 'pkcs8', format: 'pem' }).toString(), publicKey };
}

export function rsaKeys(): { privatePem: string; publicKey: KeyObject } {
  const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  return { privatePem: privateKey.export({ type: 'pkcs8', format: 'pem' }).toString(), publicKey };
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

// ---------- Mock APNs (cleartext HTTP/2) ----------

export interface ApnsRecord {
  headers: Record<string, string | string[] | undefined>;
  path: string;
  body: Record<string, unknown>;
}

export class MockApns {
  readonly server: Http2Server;
  records: ApnsRecord[] = [];
  connections = 0;
  inFlight = 0;
  maxInFlight = 0;
  total = 0;
  /** Delay before answering, ms. */
  delayMs = 0;
  /** Keep request bodies and headers (off for the throughput run). */
  record = true;
  /** Send GOAWAY on the connection after answering the next request. */
  goawayAfterNext = false;
  /** Destroy the connection instead of answering the next request. */
  dropNext = false;
  /** Never answer requests whose token starts with `hang-`. */
  private readonly sessions = new Set<Http2Session>();
  /** Force a response for the next N requests: [status, reason]. */
  forced: Array<[number, string]> = [];
  port = 0;

  constructor() {
    this.server = createH2cServer();
    this.server.on('session', (s) => {
      this.connections++;
      this.sessions.add(s);
      s.on('close', () => this.sessions.delete(s));
    });
    this.server.on('stream', (stream: ServerHttp2Stream, headers) => {
      this.total++;
      this.inFlight++;
      this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
      const path = String(headers[':path']);
      const token = decodeURIComponent(path.replace('/3/device/', ''));
      const chunks: Buffer[] = [];
      stream.on('data', (c: Buffer) => this.record && chunks.push(c));
      stream.on('close', () => {
        this.inFlight--;
      });
      stream.on('error', () => {});
      stream.on('end', () => {
        if (this.record) {
          this.records.push({
            headers,
            path,
            body: JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<string, unknown>,
          });
        }
        if (token.startsWith('hang-')) return;
        const respond = (): void => {
          if (stream.destroyed || stream.closed) return;
          if (this.dropNext) {
            this.dropNext = false;
            stream.session?.destroy();
            return;
          }
          let status = 200;
          let reason = '';
          const forced = this.forced.shift();
          if (forced) [status, reason] = forced;
          else if (token.startsWith('gone-')) [status, reason] = [410, 'Unregistered'];
          else if (token.startsWith('bad-')) [status, reason] = [400, 'BadDeviceToken'];
          else if (token.startsWith('wrongtopic-'))
            [status, reason] = [400, 'DeviceTokenNotForTopic'];
          else if (token.startsWith('badtopic-')) [status, reason] = [400, 'BadTopic'];
          else if (token.startsWith('slow-')) [status, reason] = [429, 'TooManyRequests'];
          else if (token.startsWith('err-')) [status, reason] = [503, 'ServiceUnavailable'];
          else if (token.startsWith('ise-')) [status, reason] = [500, 'InternalServerError'];
          stream.respond({ ':status': status });
          stream.end(reason ? JSON.stringify({ reason }) : undefined);
          if (this.goawayAfterNext) {
            this.goawayAfterNext = false;
            stream.session?.goaway();
          }
        };
        if (this.delayMs > 0) setTimeout(respond, this.delayMs);
        else respond();
      });
    });
  }

  async start(): Promise<string> {
    await new Promise<void>((r) => this.server.listen(0, '127.0.0.1', r));
    this.port = (this.server.address() as AddressInfo).port;
    return `http://127.0.0.1:${this.port}`;
  }

  async stop(): Promise<void> {
    for (const s of this.sessions) s.destroy();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}

// ---------- Mock FCM + OAuth token endpoint ----------

export interface FcmRecord {
  headers: Record<string, string | string[] | undefined>;
  url: string;
  body: { message: Record<string, unknown> };
}

export class MockFcm {
  readonly server: Server;
  records: FcmRecord[] = [];
  tokenRequests: Array<Record<string, string>> = [];
  inFlight = 0;
  maxInFlight = 0;
  total = 0;
  delayMs = 0;
  record = true;
  /** `expires_in` the token endpoint reports. */
  expiresIn = 3600;
  tokenStatus = 200;
  /** Access token the API accepts; changes each time the token endpoint is called. */
  currentAccessToken = '';
  /** Force the status of the next N API responses. */
  forced: Array<{ status: number; body?: unknown; headers?: Record<string, string> }> = [];
  port = 0;

  constructor() {
    this.server = createHttpServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (c: Buffer) => chunks.push(c));
      this.total++;
      this.inFlight++;
      this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
      res.on('close', () => {
        this.inFlight--;
      });
      req.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        if (req.url === '/token') {
          this.tokenRequests.push(Object.fromEntries(new URLSearchParams(text)));
          if (this.tokenStatus !== 200) {
            res.writeHead(this.tokenStatus).end('{}');
            return;
          }
          this.currentAccessToken = `access-${this.tokenRequests.length}`;
          res
            .writeHead(200, { 'content-type': 'application/json' })
            .end(
              JSON.stringify({ access_token: this.currentAccessToken, expires_in: this.expiresIn }),
            );
          return;
        }
        const body = this.record ? (JSON.parse(text) as FcmRecord['body']) : undefined;
        if (body) {
          this.records.push({ headers: req.headers, url: req.url ?? '', body });
        }
        const token = this.record
          ? String(body?.message['token'])
          : (/"token":"([^"]*)"/.exec(text)?.[1] ?? '');
        const respond = (): void => {
          const forced = this.forced.shift();
          if (forced) {
            res
              .writeHead(forced.status, { 'content-type': 'application/json', ...forced.headers })
              .end(JSON.stringify(forced.body ?? {}));
            return;
          }
          if (req.headers.authorization !== `Bearer ${this.currentAccessToken}`) {
            res.writeHead(401).end(JSON.stringify({ error: { status: 'UNAUTHENTICATED' } }));
            return;
          }
          const err = (status: number, st: string, details?: unknown[]): void => {
            res
              .writeHead(status, { 'content-type': 'application/json' })
              .end(JSON.stringify({ error: { code: status, status: st, message: 'x', details } }));
          };
          if (token.startsWith('gone-')) {
            err(404, 'NOT_FOUND', [
              {
                '@type': 'type.googleapis.com/google.firebase.fcm.v1.FcmError',
                errorCode: 'UNREGISTERED',
              },
            ]);
          } else if (token.startsWith('bad-')) {
            err(400, 'INVALID_ARGUMENT', [
              {
                '@type': 'type.googleapis.com/google.rpc.BadRequest',
                fieldViolations: [
                  { field: 'message.token', description: 'not a valid FCM registration token' },
                ],
              },
            ]);
          } else if (token.startsWith('badpayload-')) {
            err(400, 'INVALID_ARGUMENT', [
              { fieldViolations: [{ field: 'message.data', description: 'bad' }] },
            ]);
          } else if (token.startsWith('quota-')) {
            res.writeHead(429, { 'content-type': 'application/json', 'retry-after': '30' }).end(
              JSON.stringify({
                error: {
                  code: 429,
                  status: 'RESOURCE_EXHAUSTED',
                  details: [{ errorCode: 'QUOTA_EXCEEDED' }],
                },
              }),
            );
          } else if (token.startsWith('unavail-')) {
            res.writeHead(503, { 'content-type': 'application/json', 'retry-after': '5' }).end(
              JSON.stringify({
                error: {
                  code: 503,
                  status: 'UNAVAILABLE',
                  details: [{ errorCode: 'UNAVAILABLE' }],
                },
              }),
            );
          } else if (token.startsWith('ise-')) {
            err(500, 'INTERNAL', [{ errorCode: 'INTERNAL' }]);
          } else if (token.startsWith('hang-')) {
            return;
          } else {
            res
              .writeHead(200, { 'content-type': 'application/json' })
              .end('{"name":"projects/p/messages/1"}');
          }
        };
        if (this.delayMs > 0) setTimeout(respond, this.delayMs);
        else respond();
      });
    });
  }

  async start(): Promise<void> {
    await new Promise<void>((r) => this.server.listen(0, '127.0.0.1', r));
    this.port = (this.server.address() as AddressInfo).port;
  }

  get baseUrl(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  get tokenUri(): string {
    return `${this.baseUrl}/token`;
  }

  async stop(): Promise<void> {
    this.server.closeAllConnections();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}
