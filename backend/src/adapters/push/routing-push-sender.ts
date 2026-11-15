import type { PushSender } from '../../ports.js';
import type {
  NativePushMessage,
  NativePushResult,
  NativePushSender,
  PushPlatform,
} from '../../scale/contracts.js';
import { redactPushTokens } from '../../delivery/redact.js';
import type { PolicyPushMessage } from '../../delivery/types.js';

/**
 * Wraps the existing Expo sender (development builds) behind the native interface.
 * Expo has no permanent/retryable distinction beyond DeviceNotRegistered.
 */
export class ExpoNativeAdapter implements NativePushSender {
  constructor(private readonly expo: PushSender) {}

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    if (messages.length === 0) return [];
    try {
      const results = await this.expo.send(
        messages.map((m) => ({
          to: m.target.token,
          title: m.title,
          body: m.body,
          data: m.data,
          categoryId: m.category,
          // Loudness rides along as extra fields, as delivery/types.ts documents.
          priority: m.priority,
          ...(m.sound ? { sound: 'default' } : {}),
        })) as Array<Parameters<PushSender['send']>[0][number] & PolicyPushMessage>,
      );
      return messages.map((_, i): NativePushResult => {
        const r = results[i];
        if (!r) return { ok: false, kind: 'retryable', error: 'MissingResult' };
        if (r.ok) return { ok: true };
        return {
          ok: false,
          kind: r.error === 'DeviceNotRegistered' ? 'permanent' : 'retryable',
          error: redactPushTokens(r.error),
        };
      });
    } catch (err) {
      const error = redactPushTokens(`ExpoError: ${err instanceof Error ? err.name : 'Error'}`);
      return messages.map((): NativePushResult => ({ ok: false, kind: 'retryable', error }));
    }
  }
}

/**
 * Splits a message list by target platform, sends each group concurrently, and returns the
 * results in the original order.
 */
export class RoutingPushSender implements NativePushSender {
  constructor(private readonly senders: Record<PushPlatform, NativePushSender>) {}

  async send(messages: NativePushMessage[]): Promise<NativePushResult[]> {
    const groups = new Map<PushPlatform, { index: number[]; msgs: NativePushMessage[] }>();
    const results: NativePushResult[] = new Array<NativePushResult>(messages.length);
    messages.forEach((m, i) => {
      const platform = m.target.platform;
      if (!this.senders[platform]) {
        results[i] = { ok: false, kind: 'permanent', error: 'UnknownPlatform' };
        return;
      }
      let g = groups.get(platform);
      if (!g) groups.set(platform, (g = { index: [], msgs: [] }));
      g.index.push(i);
      g.msgs.push(m);
    });

    await Promise.all(
      [...groups].map(async ([platform, g]) => {
        let out: NativePushResult[];
        try {
          out = await this.senders[platform].send(g.msgs);
        } catch (err) {
          // A sender should not throw, but one that does must not lose the other platforms.
          const error = `SenderError: ${err instanceof Error ? err.name : 'Error'}`;
          out = g.msgs.map((): NativePushResult => ({ ok: false, kind: 'retryable', error }));
        }
        g.index.forEach((orig, j) => {
          results[orig] = out[j] ?? { ok: false, kind: 'retryable', error: 'MissingResult' };
        });
      }),
    );
    return results;
  }
}
