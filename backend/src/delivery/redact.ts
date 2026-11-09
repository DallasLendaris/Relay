/** Matches ExponentPushToken[...] and ExpoPushToken[...]. */
const PUSH_TOKEN = /Expo(?:nent)?PushToken\[[^\]]*\]/g;

/** Remove push tokens from any string before it is logged or returned in an error. */
export const redactPushTokens = (text: string): string =>
  text.replace(PUSH_TOKEN, '[redacted-push-token]');
