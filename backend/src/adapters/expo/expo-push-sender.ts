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
}

export class ExpoPushSender implements PushSender {
  private readonly fetchFn: typeof fetch;
  private readonly sleep: (ms: number) => Promise<void>;
  private readonly backoffMs: number;

  constructor(opts: ExpoPushSenderOptions = {}) {
    this.fetchFn = opts.fetch ?? fetch;
    this.sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
    this.backoffMs = opts.backoffMs ?? 500;
  }

  async send(messages: PushMessage[]): Promise<Result[]> {
    const results: Result[] = [];
    for (let i = 0; i < messages.length; i += CHUNK) {
      results.push(...(await this.sendChunk(messages.slice(i, i + CHUNK))));
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
    const failAll = (error: string): Result[] => chunk.map(() => ({ ok: false, error }));

    for (let attempt = 0; ; attempt++) {
      let res: Response;
      try {
        res = await this.fetchFn(EXPO_URL, {
          method: 'POST',
          headers: { 'content-type': 'application/json', accept: 'application/json' },
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

      const json = (await res.json()) as { data?: Ticket[] };
      const tickets = json.data ?? [];
      return chunk.map((_, i): Result => {
        const t = tickets[i];
        if (!t) return { ok: false, error: 'MissingTicket' };
        if (t.status === 'ok') return { ok: true };
        return { ok: false, error: t.details?.error ?? t.message ?? 'UnknownError' };
      });
    }
  }
}
