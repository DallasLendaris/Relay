import { z } from 'zod';
import { LIMITS } from './limits.js';

/**
 * "need_help" is reserved for the Pilot. The demo API rejects it, but the format
 * allows it so the Pilot needs no format change.
 */
export const CheckInStatusSchema = z.enum(['ok', 'need_help']);
export type CheckInStatus = z.infer<typeof CheckInStatusSchema>;

export const CheckInSchema = z.object({
  alertId: z.string().uuid(),
  deviceId: z.string().uuid(),
  status: CheckInStatusSchema,
  checkedInAt: z.string().datetime({ offset: true }),
  token: z.string().max(LIMITS.checkInTokenMax),
  idempotencyKey: z.string().max(LIMITS.idempotencyKeyMax),
  h3Cell: z.string().max(64),
});
export type CheckIn = z.infer<typeof CheckInSchema>;
