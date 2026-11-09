import { Agent as HttpAgent, request as httpRequest, type IncomingHttpHeaders } from 'node:http';
import { Agent as HttpsAgent, request as httpsRequest } from 'node:https';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
} from '../../scale/contracts.js';
import { noopLogger, type Logger } from '../../delivery/types.js';
import { errName, signJwt } from './jwt.js';
import { Limiter } from './limiter.js';

export const FCM_BASE_URL = 'https://fcm.googleapis.com';
const FCM_SCOPE = 'https://www.googleapis.com/auth/firebase.messaging';
const TOKEN_REFRESH_MARGIN_MS = 5 * 60 * 1000;

/** The fields of a Google service-account JSON key that we use. */
export interface ServiceAccount {
  client_email: string;
  private_key: string;
  token_uri?: string;
  project_id?: string;
}

export interface FcmSenderOptions {
  serviceAccount: ServiceAccount;
  /** Defaults to serviceAccount.project_id. */
  projectId?: string;
  /** OAuth2 token endpoint. Defaults to serviceAccount.token_uri. Injectable for tests. */
  tokenUri?: string;
  /** Defaults to https://fcm.googleapis.com. Injectable for tests. */
  baseUrl?: string;
  /** Concurrent requests. Default 200. */
  maxInFlight?: number;
  /** Per-request timeout. Default 10 s. */
  timeoutMs?: number;
  logger?: Logger;
  now?: () => number;
}

interface CachedToken {
  accessToken: string;
  /** Epoch ms after which a new token must be fetched (already minus the safety margin). */
  refreshAt: number;
}

export class FcmSender implements NativePushSender {
  private readonly projectId: string;
  private readonly tokenUri: string;
  private readonly url: string;
  private readonly limiter: Limiter;
  private readonly timeoutMs: number;
  private readonly http: HttpAgent;
  private readonly https: HttpsAgent;
  private readonly logger: Logger;
  private readonly now: () => number;
  private cached: CachedToken | null = null;
  private pendingToken: Promise<CachedToken> | null = null;

  constructor(private readonly opts: FcmSenderOptions) {
    const projectId = opts.projectId ?? opts.serviceAccount.project_id;
    const tokenUri = opts.tokenUri ?? opts.serviceAccount.token_uri;
    if (!projectId) throw new Error('FcmSender needs a projectId');
    if (!tokenUri) throw new Error('FcmSender needs a tokenUri');
    this.projectId = projectId;
    this.tokenUri = tokenUri;
    this.url = `${opts.baseUrl ?? FCM_BASE_URL}/v1/projects/${encodeURIComponent(
      projectId,
    )}/messages:send`;
    this.limiter = new Limiter(opts.maxInFlight ?? 200);
    this.timeoutMs = opts.timeoutMs ?? 10_000;
    // Keep-alive pools sized to the in-flight cap. node:http is several times faster than the
    // global fetch for many small requests, which matters at 1M recipients.
    const maxSockets = opts.maxInFlight ?? 200;
    this.http = new HttpAgent({ keepAlive: true, maxSockets });
    this.https = new HttpsAgent({ keepAlive: true, maxSockets });
    this.logger = opts.logger ?? noopLogger;
    this.now = opts.now ?? Date.now;
  }

  /** Close idle keep-alive connections. Call on shutdown. */
  close(): void {
    this.http.destroy();
    this.https.destroy();
  }

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    if (messages.length === 0) return [];
    let token: CachedToken;
    try {
      token = await this.accessToken();
    } catch (err) {
      const error = `FcmAuthError: ${err instanceof AuthError ? err.message : errName(err)}`;
      this.logger.error('fcm.auth_failed', { error });
      return messages.map((): NativePushResult => ({ ok: false, kind: 'retryable', error }));
    }
    return Promise.all(messages.map((m) => this.limiter.run(() => this.sendOne(m, token))));
  }

  private accessToken(): Promise<CachedToken> {
    const c = this.cached;
    if (c && this.now() < c.refreshAt) return Promise.resolve(c);
    // One refresh at a time; concurrent callers share it.
    this.pendingToken ??= this.fetchAccessToken().finally(() => {
      this.pendingToken = null;
    });
    return this.pendingToken;
  }

  private async fetchAccessToken(): Promise<CachedToken> {
    const sa = this.opts.serviceAccount;
    const iat = Math.floor(this.now() / 1000);
    const assertion = signJwt(
      'RS256',
      {},
      { iss: sa.client_email, scope: FCM_SCOPE, aud: this.tokenUri, iat, exp: iat + 3600 },
      sa.private_key,
    );
    const res = await this.post(
      this.tokenUri,
      { 'content-type': 'application/x-www-form-urlencoded' },
      new URLSearchParams({
        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        assertion,
      }).toString(),
    );
    if (res.status !== 200) throw new AuthError(`token endpoint returned ${res.status}`);
    let json: { access_token?: unknown; expires_in?: unknown } = {};
    try {
      json = JSON.parse(res.text) as typeof json;
    } catch {
      // handled below as malformed
    }
    if (typeof json.access_token !== 'string') throw new AuthError('token response malformed');
    const expiresIn = typeof json.expires_in === 'number' ? json.expires_in : 3600;
    const t = this.now();
    const token = {
      accessToken: json.access_token,
      refreshAt: t + expiresIn * 1000 - TOKEN_REFRESH_MARGIN_MS,
    };
    this.cached = token;
    return token;
  }

  private buildBody(m: NativePushMessage): string {
    const notification: Record<string, unknown> = {
      channel_id: 'alerts',
      click_action: m.category,
    };
    if (m.sound) notification['sound'] = 'default';
    return JSON.stringify({
      message: {
        token: m.target.token,
        notification: { title: m.title, body: m.body },
        data: m.data,
        android: {
          priority: m.priority === 'high' ? 'HIGH' : 'NORMAL',
          ttl: `${Math.max(0, Math.floor(m.ttlSeconds))}s`,
          collapse_key: m.collapseKey,
          notification,
        },
      },
    });
  }

  private async sendOne(m: NativePushMessage, token: CachedToken): Promise<NativePushResult> {
    let res: HttpResult;
    try {
      res = await this.post(
        this.url,
        { authorization: `Bearer ${token.accessToken}`, 'content-type': 'application/json' },
        this.buildBody(m),
      );
    } catch (err) {
      if (err instanceof TimeoutError) return { ok: false, kind: 'retryable', error: 'Timeout' };
      return { ok: false, kind: 'retryable', error: `FcmConnectionError: ${errName(err)}` };
    }
    if (res.status === 200) return { ok: true };
    return this.mapError(res, token);
  }

  private readonly urls = new Map<string, URL>();

  private parsed(url: string): URL {
    let u = this.urls.get(url);
    if (!u) this.urls.set(url, (u = new URL(url)));
    return u;
  }

  /** POST over a keep-alive agent with an overall deadline. Rejects with TimeoutError. */
  private post(url: string, headers: Record<string, string>, body: string): Promise<HttpResult> {
    return new Promise<HttpResult>((resolve, reject) => {
      const u = this.parsed(url);
      const secure = u.protocol === 'https:';
      const make = secure ? httpsRequest : httpRequest;
      const req = make(
        u,
        {
          method: 'POST',
          agent: secure ? this.https : this.http,
          headers: { ...headers, 'content-length': Buffer.byteLength(body) },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c: Buffer) => chunks.push(c));
          res.on('end', () => {
            clearTimeout(timer);
            resolve({
              status: res.statusCode ?? 0,
              headers: res.headers,
              text: Buffer.concat(chunks).toString('utf8'),
            });
          });
          res.on('error', fail);
        },
      );
      const fail = (err: Error): void => {
        clearTimeout(timer);
        reject(err);
      };
      const timer = setTimeout(() => {
        const err = new TimeoutError();
        req.destroy(err);
        fail(err);
      }, this.timeoutMs);
      req.on('error', fail);
      req.end(body);
    });
  }

  private mapError(res: HttpResult, sent: CachedToken): NativePushResult {
    const status = res.status;
    const text = res.text;
    let errBody: {
      status?: string;
      message?: string;
      details?: Array<{
        errorCode?: string;
        fieldViolations?: Array<{ field?: string }>;
      }>;
    } = {};
    try {
      errBody = (JSON.parse(text) as { error?: typeof errBody }).error ?? {};
    } catch {
      // Non-JSON error page from a proxy or load balancer.
    }
    const details = errBody.details ?? [];
    const errorCode =
      details.find((d) => typeof d.errorCode === 'string')?.errorCode ?? errBody.status ?? '';
    // The error message can echo request fields, so only the code is returned, never the text.
    const label = `Fcm${status}${errorCode ? `:${errorCode}` : ''}`;

    // A plain 404 NOT_FOUND can mean a wrong project, so only UNREGISTERED is trusted as a dead token.
    if (status === 404 && errorCode === 'UNREGISTERED') {
      return { ok: false, kind: 'permanent', error: label };
    }
    if (status === 400 && errBody.status === 'INVALID_ARGUMENT') {
      const onToken = details.some((d) =>
        (d.fieldViolations ?? []).some((v) => (v.field ?? '').includes('message.token')),
      );
      if (onToken) return { ok: false, kind: 'permanent', error: label };
    }
    if (status === 401 && this.cached === sent) this.cached = null; // refresh next time

    const ra = res.headers['retry-after'];
    const retryAfter = Array.isArray(ra) ? ra[0] : ra;
    return {
      ok: false,
      kind: 'retryable',
      error: retryAfter ? `${label} retry-after=${retryAfter}` : label,
    };
  }
}

class AuthError extends Error {}

class TimeoutError extends Error {
  constructor() {
    super('timeout');
    this.name = 'TimeoutError';
  }
}

interface HttpResult {
  status: number;
  headers: IncomingHttpHeaders;
  text: string;
}
