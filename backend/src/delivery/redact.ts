/** Matches ExponentPushToken[...] and ExpoPushToken[...]. */
const PUSH_TOKEN = /Expo(?:nent)?PushToken\[[^\]]*\]/g;

/** An APNs device token: 64 or more hex characters (32 bytes or more), as one word. */
const APNS_TOKEN = /(?<![0-9A-Za-z])[0-9a-fA-F]{64,}(?![0-9A-Za-z])/g;

/**
 * An FCM registration token: `<id>:APA91b<long url-safe base64>`. The `APA91b` marker is what
 * every legacy FCM token carries; the fallback matches the same shape with another marker
 * (a colon-separated id and a 100+ character body), which is what newer tokens look like.
 */
const FCM_TOKEN = /[A-Za-z0-9_-]{6,}:(?:APA91[A-Za-z0-9_-]{20,}|[A-Za-z0-9_-]{100,})/g;

/**
 * Remove push tokens (Expo, APNs hex, FCM) from any string before it is logged or returned in
 * an error. Over-redacting a long hex string is fine; leaking a token is not.
 */
export const redactPushTokens = (text: string): string =>
  text
    .replace(PUSH_TOKEN, '[redacted-push-token]')
    .replace(FCM_TOKEN, '[redacted-push-token]')
    .replace(APNS_TOKEN, '[redacted-push-token]');

/**
 * What to log about a caught error: its class name and a stable code, never `err.message` or
 * `String(err)` (database and SDK messages can quote rows, tokens or coordinates). The code is
 * the error's own `code` when it is short and plain (ECONNRESET, ThrottlingException, ...),
 * otherwise 'unknown'.
 */
export function errorFields(err: unknown): { error: string; code: string } {
  const name = err instanceof Error && /^[A-Za-z0-9_.]{1,64}$/.test(err.name) ? err.name : 'Error';
  const raw =
    typeof err === 'object' && err !== null ? (err as { code?: unknown }).code : undefined;
  const code = typeof raw === 'string' && /^[A-Za-z0-9_.-]{1,64}$/.test(raw) ? raw : 'unknown';
  return { error: name, code };
}
