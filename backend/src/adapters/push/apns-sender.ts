import {
  connect,
  constants,
  type ClientHttp2Session,
  type SecureClientSessionOptions,
} from 'node:http2';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
} from '../../scale/contracts.js';
import { noopLogger, type Logger } from '../../delivery/types.js';
import { errName, signJwt } from './jwt.js';
import { Limiter } from './limiter.js';

export const APNS_PRODUCTION = 'https://api.push.apple.com';
export const APNS_SANDBOX = 'https://api.sandbox.push.apple.com';

/** Apple accepts a provider token that is 20 to 60 minutes old. */
const JWT_REFRESH_MS = 50 * 60 * 1000;

export interface ApnsSenderOptions {
  /** Contents of the .p8 key (PKCS#8 PEM). */
  privateKeyPem: string;
  keyId: string;
  teamId: string;
  /** App bundle id, sent as apns-topic. */
  bundleId: string;
  sandbox?: boolean;
  /** Override the endpoint (tests). `http://` gives cleartext HTTP/2 (h2c), for tests only. */
  authority?: string;
  /** Extra TLS options for the connection (tests). */
  connectOptions?: SecureClientSessionOptions;
  /** HTTP/2 sessions in the pool. Default 4. */
  sessions?: number;
  /** Total concurrent streams across all sessions. Default 500. */
  maxInFlight?: number;
  /** Per-request timeout. Default 10 s. */
  timeoutMs?: number;
  logger?: Logger;
  /** Injectable clock (ms), for token refresh and apns-expiration. */
  now?: () => number;
}

interface PoolSlot {
  session: ClientHttp2Session | null;
  active: number;
}

interface Jwt {
  token: string;
  issuedAt: number;
}

/** Cut to at most `max` UTF-8 bytes without splitting a character. */
export function truncateBytes(text: string, max: number): string {
  if (Buffer.byteLength(text) <= max) return text;
  let out = '';
  for (const ch of text) {
    if (Buffer.byteLength(out + ch) > max) break;
    out += ch;
  }
  return out;
}

const PERMANENT_400 = new Set(['BadDeviceToken', 'DeviceTokenNotForTopic']);

export class ApnsSender implements NativePushSender {
  private readonly authority: string;
  private readonly pool: PoolSlot[];
  private readonly limiter: Limiter;
  private readonly timeoutMs: number;
  private readonly logger: Logger;
  private readonly now: () => number;
  private jwt: Jwt | null = null;
  private rr = 0;

  constructor(private readonly opts: ApnsSenderOptions) {
    const sessions = opts.sessions ?? 4;
    if (!Number.isInteger(sessions) || sessions < 1) throw new RangeError('sessions must be >= 1');
    this.authority = opts.authority ?? (opts.sandbox ? APNS_SANDBOX : APNS_PRODUCTION);
    this.pool = Array.from({ length: sessions }, () => ({ session: null, active: 0 }));
    this.limiter = new Limiter(opts.maxInFlight ?? 500);
    this.timeoutMs = opts.timeoutMs ?? 10_000;
    this.logger = opts.logger ?? noopLogger;
    this.now = opts.now ?? Date.now;
  }

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    return Promise.all(messages.map((m) => this.limiter.run(() => this.sendOne(m))));
  }

  /** Close every session. Call on shutdown. */
  close(): void {
    for (const p of this.pool) {
      p.session?.close();
      p.session = null;
    }
  }

  private providerToken(): Jwt {
    const t = this.now();
    if (!this.jwt || t - this.jwt.issuedAt >= JWT_REFRESH_MS) {
      this.jwt = {
        token: signJwt(
          'ES256',
          { kid: this.opts.keyId },
          { iss: this.opts.teamId, iat: Math.floor(t / 1000) },
          this.opts.privateKeyPem,
        ),
        issuedAt: t,
      };
    }
    return this.jwt;
  }

  /** Force a new token, unless another request already replaced the one that was rejected. */
  private invalidateToken(rejected: Jwt): void {
    if (this.jwt === rejected) this.jwt = null;
  }

  /** Least-loaded slot; ties go round-robin so connections spread evenly. */
  private acquireSlot(): { slot: PoolSlot; session: ClientHttp2Session } {
    let best = this.pool[0] as PoolSlot;
    for (let i = 0; i < this.pool.length; i++) {
      const p = this.pool[(this.rr + i) % this.pool.length] as PoolSlot;
      if (p.active < best.active) best = p;
    }
    this.rr = (this.rr + 1) % this.pool.length;
    let session = best.session;
    if (!session || session.closed || session.destroyed) {
      session = this.openSession(best);
      best.session = session;
    }
    return { slot: best, session };
  }

  private openSession(slot: PoolSlot): ClientHttp2Session {
    const session = connect(this.authority, this.opts.connectOptions);
    const drop = (): void => {
      if (slot.session === session) slot.session = null;
    };
    // Session-level problems must never crash the process; in-flight streams see their own error.
    session.on('error', (err: NodeJS.ErrnoException) => {
      this.logger.warn('apns.session_error', { code: err.code ?? 'unknown' });
      drop();
    });
    session.on('goaway', (code: number) => {
      this.logger.info('apns.goaway', { code });
      // Stop using this session; streams already accepted finish, then it closes.
      drop();
      session.close();
    });
    session.on('close', drop);
    // An idle long-lived connection should not keep the process alive on its own.
    session.unref();
    return session;
  }

  private buildRequest(m: NativePushMessage): { headers: Record<string, string>; body: string } {
    const nowSec = Math.floor(this.now() / 1000);
    const high = m.priority === 'high';
    const headers: Record<string, string> = {
      'apns-topic': this.opts.bundleId,
      'apns-push-type': 'alert',
      'apns-priority': high ? '10' : '5',
      // 0 means "deliver once, do not store".
      'apns-expiration': String(m.ttlSeconds > 0 ? nowSec + Math.floor(m.ttlSeconds) : 0),
      'content-type': 'application/json',
    };
    if (m.collapseKey) headers['apns-collapse-id'] = truncateBytes(m.collapseKey, 64);
    const aps: Record<string, unknown> = {
      alert: { title: m.title, body: m.body },
      category: m.category,
      'thread-id': m.collapseKey,
    };
    if (m.sound) aps['sound'] = 'default';
    if (high) aps['interruption-level'] = 'time-sensitive';
    return { headers, body: JSON.stringify({ ...m.data, aps }) };
  }

  private sendOne(m: NativePushMessage): Promise<NativePushResult> {
    const retry = (error: string): NativePushResult => ({ ok: false, kind: 'retryable', error });
    let jwt: Jwt;
    let built: { headers: Record<string, string>; body: string };
    let acquired: { slot: PoolSlot; session: ClientHttp2Session };
    try {
      jwt = this.providerToken();
      built = this.buildRequest(m);
      acquired = this.acquireSlot();
    } catch (err) {
      return Promise.resolve(retry(`ApnsSetupError: ${errName(err)}`));
    }
    const { slot, session } = acquired;

    return new Promise<NativePushResult>((resolve) => {
      let settled = false;
      let req: ReturnType<ClientHttp2Session['request']> | undefined;
      slot.active++;
      const done = (r: NativePushResult): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        slot.active--;
        resolve(r);
      };
      const timer = setTimeout(() => {
        req?.close(constants.NGHTTP2_CANCEL);
        done(retry('Timeout'));
      }, this.timeoutMs);

      try {
        req = session.request({
          ':method': 'POST',
          ':path': `/3/device/${encodeURIComponent(m.target.token)}`,
          authorization: `bearer ${jwt.token}`,
          ...built.headers,
        });
      } catch (err) {
        done(retry(`ApnsConnectionError: ${errName(err)}`));
        return;
      }

      let status = 0;
      const chunks: Buffer[] = [];
      req.on('response', (h) => {
        status = Number(h[':status']);
      });
      req.on('data', (c: Buffer) => chunks.push(c));
      req.on('error', (err) => done(retry(`ApnsConnectionError: ${errName(err)}`)));
      req.on('close', () => {
        if (!status) {
          done(retry('ApnsConnectionError: stream closed without a response'));
          return;
        }
        let reason = '';
        try {
          const parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')) as { reason?: unknown };
          if (typeof parsed.reason === 'string') reason = parsed.reason;
        } catch {
          // Empty body on 200 and on some errors.
        }
        done(this.mapResponse(status, reason, jwt));
      });
      req.end(built.body);
    });
  }

  private mapResponse(status: number, reason: string, jwt: Jwt): NativePushResult {
    if (status === 200) return { ok: true };
    const error = `Apns${status}${reason ? `:${reason}` : ''}`;
    if (status === 410) return { ok: false, kind: 'permanent', error };
    if (status === 400 && PERMANENT_400.has(reason)) return { ok: false, kind: 'permanent', error };
    if (status === 403) {
      // InvalidProviderToken / ExpiredProviderToken: sign a fresh token for the next attempt.
      this.invalidateToken(jwt);
      this.logger.warn('apns.provider_token_rejected', { reason });
    }
    // 429, 5xx, 403 and anything unexpected (e.g. 400 BadTopic, 413) are not proof that the
    // token is dead, so they stay retryable rather than erasing a good device.
    return { ok: false, kind: 'retryable', error };
  }
}
