/**
 * The PoC path as one scripted run, through the HTTP-shaped API exactly as the phone app uses it:
 *
 *   1. A phone registers (POST /devices) and its push token (PUT /devices/me/push-token).
 *   2. It saves a place inside the alert area (POST /places, by coordinates).
 *   3. A second phone saves a place far outside it (Los Angeles), as a control.
 *   4. The engine polls the feed, stores and signs the alert, maps it to H3 cells, plans delivery,
 *      and the delivery worker sends the push.
 *   5. The first phone taps "I'm okay" with the token from its push (POST /alerts/{id}/checkins).
 *   6. The per-cell total for that alert goes to 1; the control phone got nothing.
 *
 * Used by `npm run simulate` and by the local-mode test. Throws if any step does not hold.
 */
import { randomUUID } from 'node:crypto';
import type { ApiEvent } from '../api/index.js';
import type { LocalRelay } from './relay.js';

/** A point in San José, inside the sample Flash Flood Warning polygon. */
export const INSIDE_POINT = { latitude: 37.35, longitude: -121.9 };
/** Los Angeles: not covered by any alert in the sample. */
export const OUTSIDE_POINT = { latitude: 34.05, longitude: -118.24 };

export interface ScenarioResult {
  alertId: string;
  alertEvent: string;
  pushTitle: string;
  pushesToResident: number;
  h3Cell: string;
  okCount: number;
  pushesToControl: number;
  millisFeedToPush: number;
}

class StepError extends Error {}

export async function runScenario(
  relay: LocalRelay,
  log: (line: string) => void = () => {},
  point = INSIDE_POINT,
): Promise<ScenarioResult> {
  const call = async (method: string, path: string, body?: unknown, credential?: string) => {
    const event: ApiEvent = {
      rawPath: path,
      headers: {
        'content-type': 'application/json',
        ...(credential ? { authorization: `Bearer ${credential}` } : {}),
      },
      body: body === undefined ? null : JSON.stringify(body),
      requestContext: { http: { method } },
    };
    const res = await relay.handle(event);
    const parsed = res.body ? (JSON.parse(res.body) as Record<string, unknown>) : {};
    if (res.statusCode >= 300) {
      throw new StepError(`${method} ${path} returned ${res.statusCode}: ${res.body}`);
    }
    return parsed;
  };
  const check = (ok: boolean, message: string) => {
    if (!ok) throw new StepError(message);
  };

  const phone = async (name: string, where: typeof point) => {
    const device = (await call('POST', '/devices', {})) as { deviceId: string; credential: string };
    const pushToken = `ExponentPushToken[${name}-${randomUUID().slice(0, 8)}]`;
    await call(
      'PUT',
      '/devices/me/push-token',
      { expoPushToken: pushToken, language: 'en' },
      device.credential,
    );
    const place = (await call(
      'POST',
      '/places',
      { label: 'Home', ...where },
      device.credential,
    )) as { h3Cell: string };
    return { ...device, pushToken, h3Cell: place.h3Cell };
  };

  const resident = await phone('resident', point);
  log(`1. Phone registered and saved a place (H3 cell ${resident.h3Cell})`);
  const control = await phone('control', OUTSIDE_POINT);
  log(`2. Control phone saved a place in Los Angeles (H3 cell ${control.h3Cell})`);

  const started = Date.now();
  const run = await relay.pollAndDeliver();
  log(
    `3. Polled the feed: ${run.counts.applied} alerts stored and signed, ` +
      `${run.counts.errors} errors, ${run.queued} delivery jobs`,
  );

  const pushes = relay.sentMessages.filter((m) => m.to === resident.pushToken);
  // One push per alert covering the place, never two for the same alert.
  const perAlert = new Set(pushes.map((m) => String(m.data['alertId'])));
  check(pushes.length >= 1, 'the resident got no push');
  check(perAlert.size === pushes.length, 'the resident got two pushes for the same alert');
  const message = pushes[0];
  if (!message) throw new StepError('no push');
  const millisFeedToPush = Date.now() - started;
  const alertId = String(message.data['alertId']);
  const token = String(message.data['checkInToken']);
  for (const m of pushes) log(`4. Push sent: "${m.title}"`);
  log(`   ${pushes.length} push(es), ${millisFeedToPush} ms from poll to last push`);

  const alert = (await call('GET', `/alerts/${alertId}`, undefined, resident.credential)) as {
    event: string;
    signature?: string;
  };
  check(Boolean(alert.signature), 'stored alert is not signed');

  const pushesToControl = relay.sentMessages.filter((m) => m.to === control.pushToken).length;
  check(pushesToControl === 0, `control phone outside the area got ${pushesToControl} pushes`);
  log('5. Control phone outside the area got no push');

  await call(
    'POST',
    `/alerts/${alertId}/checkins`,
    { status: 'ok', token, idempotencyKey: randomUUID() },
    resident.credential,
  );
  // A second tap is accepted but not counted twice.
  await call(
    'POST',
    `/alerts/${alertId}/checkins`,
    { status: 'ok', token, idempotencyKey: randomUUID() },
    resident.credential,
  );
  const totals = relay.checkInTotals().find((t) => t.alertId === alertId);
  const okCount = totals?.cells.find((c) => c.h3Cell === resident.h3Cell)?.okCount ?? 0;
  check(okCount === 1, `expected 1 check-in in cell ${resident.h3Cell}, got ${okCount}`);
  log(`6. "I'm okay" counted once in cell ${resident.h3Cell} (tapped twice)`);

  return {
    alertId,
    alertEvent: alert.event,
    pushTitle: message.title,
    pushesToResident: pushes.length,
    h3Cell: resident.h3Cell,
    okCount,
    pushesToControl,
    millisFeedToPush,
  };
}
