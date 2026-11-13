import type { PushMessage, PushSender } from '../../ports.js';
import { redactPushTokens } from '../../delivery/redact.js';
import type { Logger, PolicyPushMessage } from '../../delivery/types.js';

const EXPO_URL = 'https://exp.host/--/api/v2/push/send';
const CHUNK = 100; // Expo's per-request limit
const MAX_RETRIES = 3;
/** Give up on a hung Expo request well inside the Lambda timeout. */
const FETCH_TIMEOUT_MS = 8000;

type Result = { ok: true } | { ok: false; error: string };

interface Ticket {
  status: 'ok' | 'error';
  message?: string;
  details?: { error?: string };
}

export interface ExpoPushSenderOptions {
  fetch?: typeof fetch;
  /** Injectable so tests don't wait. */
  sleep?: (ms: number) => Promise<void>;
  /** Base for exponential backoff. */
  backoffMs?: number;
  /** Optional Expo access token, sent as `Authorization: Bearer`. */
  accessToken?: string;
  /** Per-request timeout. A timeout is a retryable failure. */
  timeoutMs?: number;
  /** Used for the delivery.expo_auth_failed event. */
  logger?: Logger;
}

export class ExpoPushSender implements PushSender {
  private readonly fetchFn: typeof fetch;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly backoffMs: number;
  private readonly accessToken: string | undefined;
  private readonly timeoutMs: number;
  private readonly logger: Logger | undefined;

  constructor(opts: ExpoPushSenderOptions = {}) {
    this.fetchFn = opts.fetch ?? fetch;
    this.sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.backoffMs = opts.backoffMs ?? 500;
    this.accessToken = opts.accessToken;
    this.timeoutMs = opts.timeoutMs ?? FETCH_TIMEOUT_MS;
    this.logger = opts.logger;
  }

  /**
   * Always returns one result per message. A chunk that fails marks only its own messages
   * as failed, so earlier chunks are not re-sent and the caller retries just the failures.
   */
  async send(messages: PushMessage[]): Promise<Result[]> {
    const results: Result[] = [];
    for (let i = 0; i < messages.length; i += CHUNK) {
      const chunk = messages.slice(i, i + CHUNK);
      try {
        results.push(...(await this.sendChunk(chunk)));
      } catch (err) {
        results.push(
          ...chunk.map((): Result => ({
            ok: false,
            error: redactPushTokens(`SendError: ${String(err)}`),
          })),
        );
      }
    }
    return results;
  }

  private async sendChunk(chunk: PushMessage[]): Promise<Result[]> {
    const body = JSON.stringify(
      chunk.map((m) => {
        // Loudness comes from PUSH_POLICY via send-jobs. A message without it is loud.
        const policy = m as PushMessage & Partial<PolicyPushMessage>;
        const sound = 'priority' in policy ? policy.sound : 'default';
        return {
          to: m.to,
          title: m.title,
          body: m.body,
          data: m.data,
          categoryId: m.categoryId,
          priority: policy.priority ?? 'high',
          ...(sound ? { sound } : {}),
          channelId: 'alerts',
        };
      }),
    );
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      accept: 'application/json',
    };
    if (this.accessToken) headers['authorization'] = `Bearer ${this.accessToken}`;
    const failAll = (error: string): Result[] => chunk.map(() => ({ ok: false, error }));

    for (let attempt = 0; ; attempt++) {
      let res: Response;
      try {
        res = await this.fetchFn(EXPO_URL, {
          method: 'POST',
          headers,
          body,
          signal: AbortSignal.timeout(this.timeoutMs),
        });
      } catch (err) {
        // Hung request: don't burn the Lambda's time on in-process retries. SQS retries it.
        if (err instanceof Error && (err.name === 'TimeoutError' || err.name === 'AbortError')) {
          return failAll('Timeout');
        }
        if (attempt < MAX_RETRIES) {
          await this.sleep(this.backoffMs * 2 ** attempt);
          continue;
        }
        return failAll(redactPushTokens(`NetworkError: ${String(err)}`));
      }

      if (res.status === 429 || res.status >= 500) {
        if (attempt < MAX_RETRIES) {
          await this.sleep(this.backoffMs * 2 ** attempt);
          continue;
        }
        return failAll(`HttpError${res.status}`);
      }
      if (res.status === 401 || res.status === 403) {
        // Bad access token: our config problem, not the message's. Retryable once it is fixed.
        // Never log the token itself.
        this.logger?.error('delivery.expo_auth_failed', { status: res.status });
        return failAll(`HttpError${res.status}`);
      }
      if (!res.ok) return failAll(`HttpError${res.status}`); // 4xx: retrying won't help

      // Bad JSON or the wrong shape is a retryable failure, never a throw.
      let tickets: Ticket[];
      try {
        const json = (await res.json()) as { data?: unknown } | null;
        if (!json || !Array.isArray(json.data)) return failAll('BadResponse');
        tickets = json.data as Ticket[];
      } catch {
        return failAll('BadResponse');
      }
      return chunk.map((_, i): Result => {
        const t = tickets[i];
        if (!t || typeof t !== 'object') return { ok: false, error: 'MissingTicket' };
        if (t.status === 'ok') return { ok: true };
        // Expo's message text can echo the push token.
        return {
          ok: false,
          error: t.details?.error ?? redactPushTokens(t.message ?? 'UnknownError'),
        };
      });
    }
  }
}
