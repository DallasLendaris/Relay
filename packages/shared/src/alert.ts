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
});
export type Alert = z.infer<typeof AlertSchema>;

/**
 * Maps an NWS event name to Relay's severity scale.
 * "Emergency" is checked first so "Flash Flood Emergency" is not mistaken for a warning.
 * capSeverity is accepted for future tuning but does not change the result today.
 */
export function relaySeverityFor(event: string, _capSeverity?: CapSeverity): RelaySeverity {
  const name = event.toLowerCase();
  if (name.includes('emergency')) return 'emergency';
  if (name.includes('warning')) return 'warning';
  if (name.includes('watch')) return 'watch';
  if (name.includes('advisory')) return 'advisory';
  return 'statement';
}
