import type { PushMessage, PushSender } from '../../ports.js';

const EXPO_URL = 'https://exp.host/--/api/v2/push/send';
const CHUNK = 100; // Expo's per-request limit
const MAX_RETRIES = 3;

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
}

export class ExpoPushSender implements PushSender {
  private readonly fetchFn: typeof fetch;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly backoffMs: number;
  private readonly accessToken: string | undefined;

  constructor(opts: ExpoPushSenderOptions = {}) {
    this.fetchFn = opts.fetch ?? fetch;
    this.sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.backoffMs = opts.backoffMs ?? 500;
    this.accessToken = opts.accessToken;
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
          ...chunk.map((): Result => ({ ok: false, error: `SendError: ${String(err)}` })),
        );
      }
    }
    return results;
  }

  private async sendChunk(chunk: PushMessage[]): Promise<Result[]> {
    const body = JSON.stringify(
      chunk.map((m) => ({
        to: m.to,
        title: m.title,
        body: m.body,
        data: m.data,
        categoryId: m.categoryId,
        priority: 'high',
        sound: 'default',
        channelId: 'alerts',
      })),
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
        });
      } catch (err) {
        if (attempt < MAX_RETRIES) {
          await this.sleep(this.backoffMs * 2 ** attempt);
          continue;
        }
        return failAll(`NetworkError: ${String(err)}`);
      }

      if (res.status === 429 || res.status >= 500) {
        if (attempt < MAX_RETRIES) {
          await this.sleep(this.backoffMs * 2 ** attempt);
          continue;
        }
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
        return { ok: false, error: t.details?.error ?? t.message ?? 'UnknownError' };
      });
    }
  }
}
