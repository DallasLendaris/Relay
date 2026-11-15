import type { NativePushMessage } from '../../scale/contracts.js';

/** Text and size limits for push payloads. APNs allows 4096 bytes; FCM 4096 (we stay under at 4000). */
export const PAYLOAD_LIMITS = {
  titleChars: 100,
  bodyChars: 600,
  dataValueChars: 512,
  apnsBytes: 4096,
  fcmBytes: 4000,
} as const;

/** Data keys the app cannot work without; everything else is dropped first when a payload is too big. */
const REQUIRED_DATA_KEYS = new Set(['alertId', 'checkInToken']);

/**
 * Cuts `text` to at most `max` characters, at a word boundary when one exists in the last half,
 * and ends with an ellipsis. Text already short enough is returned unchanged.
 */
export function truncateAtWord(text: string, max: number): string {
  const chars = Array.from(text);
  if (chars.length <= max) return text;
  let cut = chars.slice(0, Math.max(0, max - 1));
  let lastSpace = -1;
  for (let i = cut.length - 1; i >= 0; i--) {
    if (/\s/.test(cut[i] ?? '')) {
      lastSpace = i;
      break;
    }
  }
  if (lastSpace >= max / 2) cut = cut.slice(0, lastSpace);
  return `${cut.join('').trimEnd()}…`;
}

function hardCut(text: string, max: number): string {
  const chars = Array.from(text);
  return chars.length <= max ? text : chars.slice(0, max).join('');
}

/** The message with title, body and data values brought inside the limits. */
export function sanitizeMessage(m: NativePushMessage): NativePushMessage {
  const data: Record<string, string> = {};
  for (const [k, v] of Object.entries(m.data)) {
    data[k] = hardCut(String(v), PAYLOAD_LIMITS.dataValueChars);
  }
  return {
    ...m,
    title: truncateAtWord(m.title, PAYLOAD_LIMITS.titleChars),
    body: truncateAtWord(m.body, PAYLOAD_LIMITS.bodyChars),
    data,
  };
}

/**
 * Builds the wire payload and makes it fit `limitBytes`: first the text limits above; if the
 * result is still too big, the optional data fields are dropped; as a last resort the body is
 * halved. Returns null if nothing fits (the caller reports PayloadTooLarge for the message).
 */
export function fitPayload(
  message: NativePushMessage,
  build: (m: NativePushMessage) => string,
  limitBytes: number,
): string | null {
  let m = sanitizeMessage(message);
  let out = build(m);
  if (Buffer.byteLength(out) <= limitBytes) return out;
  m = {
    ...m,
    data: Object.fromEntries(Object.entries(m.data).filter(([k]) => REQUIRED_DATA_KEYS.has(k))),
  };
  out = build(m);
  while (Buffer.byteLength(out) > limitBytes && Array.from(m.body).length > 40) {
    m = { ...m, body: truncateAtWord(m.body, Math.floor(Array.from(m.body).length / 2)) };
    out = build(m);
  }
  return Buffer.byteLength(out) <= limitBytes ? out : null;
}

export const PAYLOAD_TOO_LARGE = 'PayloadTooLarge';
