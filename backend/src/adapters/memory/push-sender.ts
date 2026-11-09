import type { PushMessage, PushSender } from '../../ports.js';

/** In-memory PushSender. Records messages; tokens listed in `failTokens` fail with that error. */
export class MemoryPushSender implements PushSender {
  readonly sent: PushMessage[] = [];
  readonly failTokens = new Map<string, string>();

  async send(messages: PushMessage[]): Promise<Array<{ ok: true } | { ok: false; error: string }>> {
    return messages.map((m) => {
      const error = this.failTokens.get(m.to);
      if (error) return { ok: false, error };
      this.sent.push(m);
      return { ok: true };
    });
  }
}
