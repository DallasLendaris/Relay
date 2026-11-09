import { z } from 'zod';

/** H3 resolution used everywhere (saved places, alert areas, check-in roll-ups). */
export const H3_RESOLUTION = 7;

export const RelaySeveritySchema = z.enum([
  'emergency',
  'warning',
  'watch',
  'advisory',
  'statement',
]);
export type RelaySeverity = z.infer<typeof RelaySeveritySchema>;

export const CapSeveritySchema = z.enum(['Extreme', 'Severe', 'Moderate', 'Minor', 'Unknown']);
export type CapSeverity = z.infer<typeof CapSeveritySchema>;

export const AlertStateSchema = z.enum(['active', 'updated', 'cancelled', 'expired']);

/** ISO 8601 date-time string. */
const dateTime = z.string().datetime({ offset: true });

const position = z.array(z.number());
const GeoPolygonSchema = z.object({
  type: z.literal('Polygon'),
  coordinates: z.array(z.array(position)),
});
const GeoMultiPolygonSchema = z.object({
  type: z.literal('MultiPolygon'),
  coordinates: z.array(z.array(z.array(position))),
});

export const AlertSchema = z.object({
  id: z.string().uuid(),
  source: z.enum(['nws', 'usgs', 'ipaws']),
  sourceId: z.string(),
  event: z.string(),
  eventCode: z.string().nullable(),
  relaySeverity: RelaySeveritySchema,
  capSeverity: CapSeveritySchema,
  urgency: z.enum(['Immediate', 'Expected', 'Future', 'Past', 'Unknown']),
  certainty: z.enum(['Observed', 'Likely', 'Possible', 'Unlikely', 'Unknown']),
  status: z.enum(['Actual', 'Exercise', 'System', 'Test', 'Draft']),
  messageType: z.enum(['Alert', 'Update', 'Cancel', 'Ack', 'Error']),
  state: AlertStateSchema,
  references: z.array(z.object({ sourceId: z.string(), sent: dateTime })),
  area: z.object({
    description: z.string(),
    geometry: z.union([GeoPolygonSchema, GeoMultiPolygonSchema]).nullable(),
    ugcCodes: z.array(z.string()),
    sameCodes: z.array(z.string()),
    h3Cells: z.array(z.string()),
  }),
  sent: dateTime,
  effective: dateTime,
  onset: dateTime.nullable(),
  expires: dateTime,
  ends: dateTime.nullable(),
  /** Text exactly as published by the agency. */
  official: z.object({
    headline: z.string().nullable(),
    description: z.string(),
    instruction: z.string().nullable(),
    senderName: z.string(),
    language: z.string(),
  }),
  /** Relay-written plain-language summaries. Always label them as a Relay summary in the UI. May be empty. */
  summaries: z.array(z.object({ language: z.string(), whatToDo: z.string(), text: z.string() })),
  /** Set once Relay validates the alert signature. */
  signature: z.string().nullable(),
  /**
   * True for a "particularly dangerous situation" (NWS damage threat CONSIDERABLE on a Tornado or
   * Flash Flood Warning). The tier stays "warning"; the UI and push title add "PDS". Omitted
   * (not false) when it does not apply, so alerts signed before this field existed are unchanged.
   */
  pds: z.boolean().optional(),
});
export type Alert = z.infer<typeof AlertSchema>;

/** NWS impact-based-warning damage threat (parameters.tornadoDamageThreat / flashFloodDamageThreat). */
export type DamageThreat = 'BASE' | 'CONSIDERABLE' | 'CATASTROPHIC';

export interface RelaySeverityOptions {
  /** Damage threat of a Tornado or Flash Flood Warning. CATASTROPHIC is an emergency. */
  damageThreat?: string | null;
  urgency?: Alert['urgency'];
}

const normalizeThreat = (v: string | null | undefined): DamageThreat | null => {
  const t = v?.trim().toUpperCase();
  return t === 'BASE' || t === 'CONSIDERABLE' || t === 'CATASTROPHIC' ? t : null;
};

/** True when the damage threat marks a particularly dangerous situation (PDS). */
export function isPds(event: string, damageThreat?: string | null): boolean {
  return (
    normalizeThreat(damageThreat) === 'CONSIDERABLE' &&
    relaySeverityFor(event, undefined, { damageThreat }) === 'warning'
  );
}

/**
 * Maps an NWS event name to Relay's severity scale.
 * - "Emergency" in the event name is checked first so "Flash Flood Emergency" is not a warning.
 * - NWS sends real Tornado and Flash Flood Emergencies as a Warning with damage threat
 *   CATASTROPHIC, so a warning with that threat becomes an emergency. CONSIDERABLE (PDS) keeps
 *   the warning tier; see isPds.
 * - A statement (normally never pushed) with Immediate urgency and Severe or Extreme CAP severity
 *   is promoted to an advisory so people get a quiet notification.
 */
export function relaySeverityFor(
  event: string,
  capSeverity?: CapSeverity,
  opts: RelaySeverityOptions = {},
): RelaySeverity {
  const name = event.toLowerCase();
  if (name.includes('emergency')) return 'emergency';
  if (name.includes('warning')) {
    return normalizeThreat(opts.damageThreat) === 'CATASTROPHIC' ? 'emergency' : 'warning';
  }
  if (name.includes('watch')) return 'watch';
  if (name.includes('advisory')) return 'advisory';
  if (opts.urgency === 'Immediate' && (capSeverity === 'Severe' || capSeverity === 'Extreme')) {
    return 'advisory';
  }
  return 'statement';
}
