/** Minimal logger shape so tests can pass a fake and Lambdas can pass console. */
export interface Logger {
  info(msg: string, fields?: Record<string, unknown>): void;
  warn(msg: string, fields?: Record<string, unknown>): void;
  error(msg: string, fields?: Record<string, unknown>): void;
}

export const noopLogger: Logger = { info() {}, warn() {}, error() {} };

/** Result for one message: sent, or failed with a reason (Expo error code or message). */
export type SendResult = { ok: true } | { ok: false; error: string };

/**
 * A PushMessage plus the loudness chosen by PUSH_POLICY. The PushSender port only declares
 * PushMessage, so these ride along as extra fields that ExpoPushSender reads.
 */
export interface PolicyPushMessage {
  priority: 'high' | 'normal';
  sound?: 'default';
}
