import { AlertSchema, relaySeverityFor } from '@relay/shared';
import type { Alert } from '@relay/shared';

export type NormalizeResult =
  { ok: true; alert: Alert } | { ok: false; reason: string; sourceId: string | null };

const URL_PREFIX = 'https://api.weather.gov/alerts/';

type Obj = Record<string, unknown>;

const isObj = (v: unknown): v is Obj => typeof v === 'object' && v !== null && !Array.isArray(v);
const str = (v: unknown): string | null => (typeof v === 'string' && v !== '' ? v : null);
const strList = (v: unknown): string[] =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [];

/** "https://api.weather.gov/alerts/urn:oid:X" and "urn:oid:X" both become "urn:oid:X". */
function bareId(v: unknown): string | null {
  const s = str(v);
  if (s === null) return null;
  return s.startsWith(URL_PREFIX) ? s.slice(URL_PREFIX.length) : s;
}

/** A date must be an ISO 8601 string with an offset; it is kept exactly as given. */
function date(v: unknown): string | null {
  const s = str(v);
  if (s === null || !AlertSchema.shape.sent.safeParse(s).success) return null;
  return Number.isNaN(Date.parse(s)) ? null : s;
}

const STATE = { Alert: 'active', Update: 'updated', Cancel: 'cancelled' } as const;

/**
 * Validates one GeoJSON feature from NWS and converts it to a Relay Alert.
 * Never throws: any problem comes back as { ok: false, reason }.
 */
export function normalizeFeature(feature: unknown, opts: { newId(): string }): NormalizeResult {
  let sourceId: string | null = null;
  const fail = (reason: string): NormalizeResult => ({ ok: false, reason, sourceId });
  try {
    if (!isObj(feature)) return fail('feature is not an object');
    const p = feature['properties'];
    if (!isObj(p)) return fail('missing properties');

    sourceId = bareId(p['id']) ?? bareId(p['@id']) ?? bareId(feature['id']);
    if (sourceId === null) return fail('missing id');

    const status = AlertSchema.shape.status.safeParse(p['status']);
    if (!status.success) return fail(`invalid status ${String(p['status'])}`);
    if (status.data !== 'Actual') return fail(`status ${status.data}`);

    const messageType = AlertSchema.shape.messageType.safeParse(p['messageType']);
    if (!messageType.success) return fail(`invalid messageType ${String(p['messageType'])}`);
    if (messageType.data === 'Ack' || messageType.data === 'Error') {
      return fail(`unsupported messageType ${messageType.data}`);
    }
    const state = STATE[messageType.data];

    const event = str(p['event']);
    if (event === null) return fail('missing event');

    const severity = AlertSchema.shape.capSeverity.safeParse(p['severity']);
    if (!severity.success) return fail(`invalid severity ${String(p['severity'])}`);
    const urgency = AlertSchema.shape.urgency.safeParse(p['urgency']);
    if (!urgency.success) return fail(`invalid urgency ${String(p['urgency'])}`);
    const certainty = AlertSchema.shape.certainty.safeParse(p['certainty']);
    if (!certainty.success) return fail(`invalid certainty ${String(p['certainty'])}`);

    const sent = date(p['sent']);
    if (sent === null) return fail('missing or invalid sent');
    const effective = date(p['effective']);
    if (effective === null) return fail('missing or invalid effective');
    const expires = date(p['expires']);
    if (expires === null) return fail('missing or invalid expires');
    // onset and ends are optional, but if present they must parse.
    const onset = p['onset'] == null ? null : date(p['onset']);
    if (p['onset'] != null && onset === null) return fail('invalid onset');
    const ends = p['ends'] == null ? null : date(p['ends']);
    if (p['ends'] != null && ends === null) return fail('invalid ends');

    const references: Alert['references'] = [];
    if (Array.isArray(p['references'])) {
      for (const r of p['references']) {
        const ref = isObj(r) ? r : {};
        const refId = bareId(ref['identifier']) ?? bareId(ref['@id']);
        const refSent = date(ref['sent']);
        if (refId === null || refSent === null) return fail('invalid reference');
        references.push({ sourceId: refId, sent: refSent });
      }
    }

    const geocode = isObj(p['geocode']) ? p['geocode'] : {};
    const eventCodes = isObj(p['eventCode']) ? p['eventCode'] : {};
    const geometry = feature['geometry'];
    const keepGeometry =
      isObj(geometry) && (geometry['type'] === 'Polygon' || geometry['type'] === 'MultiPolygon');

    const candidate = {
      id: opts.newId(),
      source: 'nws',
      sourceId,
      event,
      eventCode: strList(eventCodes['SAME'])[0] ?? null,
      relaySeverity: relaySeverityFor(event, severity.data),
      capSeverity: severity.data,
      urgency: urgency.data,
      certainty: certainty.data,
      status: status.data,
      messageType: messageType.data,
      state,
      references,
      area: {
        description: str(p['areaDesc']) ?? '',
        geometry: keepGeometry ? geometry : null,
        ugcCodes: strList(geocode['UGC']),
        sameCodes: strList(geocode['SAME']),
        h3Cells: [], // filled later by the matching step
      },
      sent,
      effective,
      onset,
      expires,
      ends,
      official: {
        headline: str(p['headline']),
        description: typeof p['description'] === 'string' ? p['description'] : '',
        instruction: str(p['instruction']),
        senderName: typeof p['senderName'] === 'string' ? p['senderName'] : '',
        language: str(p['language']) ?? 'en-US',
      },
      summaries: [],
      signature: null,
    };
    // Final safety net: nothing leaves this function unless it fits the shared schema.
    const parsed = AlertSchema.safeParse(candidate);
    if (!parsed.success) return fail(`schema: ${parsed.error.issues[0]?.message ?? 'invalid'}`);
    return { ok: true, alert: parsed.data };
  } catch (err) {
    return fail(`unexpected error: ${String(err)}`);
  }
}
