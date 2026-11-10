/**
 * Synthetic NWS feed for t6: a deterministic mix of California alerts as the api.weather.gov
 * `alerts/active?area=CA` FeatureCollection, plus what the test expects the real pipeline to do
 * with each message.
 *
 * Mix (N features): polygon alerts, zone (UGC) alerts, a few degenerate geometries (rejected, or
 * recovered through their UGC zones), informational statements (never pushed), chains of Alert,
 * Update, Update (the feed lists them shuffled; they must be applied in order) and Alert, Cancel.
 */

export interface FeatureSpec {
  /** Bare id, as the pipeline stores it ("urn:oid:..."). */
  sourceId: string;
  kind:
    | 'polygon'
    | 'zone'
    | 'statement'
    | 'degenerate-rejected'
    | 'degenerate-fallback'
    | 'chain-alert'
    | 'chain-update'
    | 'chain-cancel';
  /** Messages of one chain share this key (the first message's sourceId). */
  chain?: string;
  feature: object;
  /** Epoch ms of `sent` (chains order by this). */
  sentMs: number;
}

export interface FeedModel {
  features: FeatureSpec[];
  /** First sourceId of every chain (Alert, Update, Update) in order, by chain key. */
  chains3: Map<string, string[]>;
  /** Chains ending in a Cancel: [alert sourceId, cancel sourceId]. */
  chainsCancel: Map<string, string[]>;
  /** Alerts that must produce no AlertReady and no stored alert. */
  rejected: string[];
  /** Alerts that are accepted but never pushed (informational). */
  statements: string[];
  /** Standalone alerts that are pushed once (polygon, zone and UGC-fallback). */
  pushedOnce: string[];
  /** Total AlertReady messages expected from one full read. */
  expectedReady: number;
}

const BASE_URL = 'https://api.weather.gov/alerts/';
const iso = (ms: number): string => new Date(ms).toISOString();

/** A deterministic [0,1) sequence. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Zone numbers the mock NWS knows (CAZ001..CAZ080). Anything else answers 404. */
export const KNOWN_ZONES = 80;

/** Square polygon (GeoJSON, counter-clockwise, closed) around a point. */
export function square(lng: number, lat: number, half: number): number[][][] {
  return [
    [
      [lng - half, lat - half],
      [lng + half, lat - half],
      [lng + half, lat + half],
      [lng - half, lat + half],
      [lng - half, lat - half],
    ],
  ];
}

/** Where the mock NWS puts zone number n. */
export function zoneCenter(n: number): { lng: number; lat: number } {
  return { lat: 32.8 + ((n * 37) % 90) / 10, lng: -124 + ((n * 53) % 100) / 10 };
}

export const zoneId = (n: number): string => `CAZ${String(n).padStart(3, '0')}`;

export interface MessageOptions {
  sourceId: string;
  event?: string;
  messageType?: 'Alert' | 'Update' | 'Cancel';
  sentMs: number;
  expiresMs: number;
  geometry: object | null;
  ugc?: string[];
  references?: Array<{ sourceId: string; sentMs: number }>;
  expiredReferences?: string[];
  severity?: string;
  urgency?: string;
  areaDesc?: string;
}

/** One NWS feature, in the shape the normalizer reads. */
export function makeFeature(o: MessageOptions): object {
  const event = o.event ?? 'Flash Flood Warning';
  return {
    id: `${BASE_URL}${o.sourceId}`,
    type: 'Feature',
    geometry: o.geometry,
    properties: {
      '@id': `${BASE_URL}${o.sourceId}`,
      id: o.sourceId,
      areaDesc: o.areaDesc ?? 'California (test)',
      geocode: { UGC: o.ugc ?? [], SAME: [] },
      sent: iso(o.sentMs),
      effective: iso(o.sentMs),
      onset: null,
      expires: iso(o.expiresMs),
      ends: null,
      status: 'Actual',
      messageType: o.messageType ?? 'Alert',
      category: 'Met',
      severity: o.severity ?? 'Severe',
      certainty: 'Likely',
      urgency: o.urgency ?? 'Immediate',
      event,
      sender: 'w-nws.webmaster@noaa.gov',
      senderName: 'NWS Test Office',
      headline: `${event} issued for a test area`,
      description: 'Synthetic alert for the ingest storm test.',
      instruction: 'Move to higher ground.',
      response: 'Avoid',
      parameters: {
        ...(o.expiredReferences && o.expiredReferences.length > 0
          ? { expiredReferences: o.expiredReferences }
          : {}),
      },
      eventCode: { SAME: ['FFW'] },
      references: (o.references ?? []).map((r) => ({
        '@id': `${BASE_URL}${r.sourceId}`,
        identifier: r.sourceId,
        sender: 'w-nws.webmaster@noaa.gov',
        sent: iso(r.sentMs),
      })),
    },
  };
}

export const feedBody = (features: object[]): string =>
  JSON.stringify({ type: 'FeatureCollection', features });

/** The id string NWS lists in `expiredReferences`: "<sender>,<identifier>,<sent>". */
export const expiredRef = (sourceId: string, sentMs: number): string =>
  `w-nws.webmaster@noaa.gov,${sourceId},${iso(sentMs)}`;

/**
 * Builds the model for `n` features. `baseMs` is "now" of the pipeline clock; messages were sent
 * minutes before it and expire hours after it.
 */
export function buildFeed(n: number, baseMs: number, seed = 7): FeedModel {
  const rand = rng(seed);
  const expiresMs = baseMs + 6 * 3600_000;
  const features: FeatureSpec[] = [];
  const chains3 = new Map<string, string[]>();
  const chainsCancel = new Map<string, string[]>();
  const rejected: string[] = [];
  const statements: string[] = [];
  const pushedOnce: string[] = [];
  let serial = 0;
  const nextId = (): string => `urn:oid:2.49.0.1.840.0.t6.${String(++serial).padStart(4, '0')}`;
  const pt = (): { lng: number; lat: number } => ({
    lng: -123.8 + rand() * 8.5,
    lat: 33 + rand() * 8,
  });
  const polygonAt = (half = 0.03): object => {
    const p = pt();
    return { type: 'Polygon', coordinates: square(p.lng, p.lat, half) };
  };
  const zones = (count: number): string[] =>
    Array.from({ length: count }, () => zoneId(1 + Math.floor(rand() * KNOWN_ZONES)));

  const nRejected = Math.max(2, Math.round(n * 0.025));
  const nFallback = Math.max(1, Math.round(n * 0.01));
  const nChains3 = Math.max(2, Math.round(n * 0.075));
  const nChainsCancel = Math.max(1, Math.round(n * 0.025));
  const nZone = Math.max(2, Math.round(n * 0.2));
  const nStatements = Math.max(1, Math.round(n * 0.03));
  const used = nRejected + nFallback + nChains3 * 3 + nChainsCancel * 2 + nZone + nStatements;
  const nPolygon = Math.max(1, n - used);

  const add = (
    kind: FeatureSpec['kind'],
    opts: Omit<MessageOptions, 'expiresMs' | 'sentMs'> & { sentMs?: number },
    chain?: string,
  ): FeatureSpec => {
    const sentMs = opts.sentMs ?? baseMs - (60 + Math.floor(rand() * 600)) * 1000;
    const spec: FeatureSpec = {
      sourceId: opts.sourceId,
      kind,
      ...(chain ? { chain } : {}),
      feature: makeFeature({ ...opts, sentMs, expiresMs }),
      sentMs,
    };
    features.push(spec);
    return spec;
  };

  for (let i = 0; i < nPolygon; i++) {
    const sourceId = nextId();
    add('polygon', { sourceId, geometry: polygonAt() });
    pushedOnce.push(sourceId);
  }
  for (let i = 0; i < nZone; i++) {
    const sourceId = nextId();
    add('zone', { sourceId, geometry: null, ugc: zones(1 + Math.floor(rand() * 3)) });
    pushedOnce.push(sourceId);
  }
  for (let i = 0; i < nStatements; i++) {
    const sourceId = nextId();
    add('statement', {
      sourceId,
      event: 'Special Weather Statement',
      severity: 'Minor',
      urgency: 'Expected',
      geometry: polygonAt(),
    });
    statements.push(sourceId);
  }

  // Degenerate geometries. With no UGC zones to fall back on they are rejected for good.
  const bowtie = {
    type: 'Polygon',
    coordinates: [
      [
        [-120, 36],
        [-119, 37],
        [-119, 36],
        [-120, 37],
        [-120, 36],
      ],
    ],
  };
  const flat = {
    type: 'Polygon',
    coordinates: [
      [
        [-121, 36],
        [-120.5, 36],
        [-120, 36],
        [-121, 36],
      ],
    ],
  };
  const tooFew = {
    type: 'Polygon',
    coordinates: [
      [
        [-121, 36],
        [-120, 36],
        [-121, 36],
      ],
    ],
  };
  const outOfRange = {
    type: 'Polygon',
    coordinates: [
      [
        [-121, 95],
        [-120, 95],
        [-120, 96],
        [-121, 95],
      ],
    ],
  };
  const huge = { type: 'Polygon', coordinates: square(-115, 35, 14) }; // far more than 200,000 cells
  const rejectedGeoms = [bowtie, huge, flat, tooFew, outOfRange];
  for (let i = 0; i < nRejected; i++) {
    const sourceId = nextId();
    add('degenerate-rejected', {
      sourceId,
      geometry: rejectedGeoms[i % rejectedGeoms.length] ?? flat,
    });
    rejected.push(sourceId);
  }
  // A broken polygon with zones listed falls back to the zones.
  const unclosed = {
    type: 'Polygon',
    coordinates: [
      [
        [-121, 36],
        [-120, 36],
        [-120, 37],
        [-121, 37],
      ],
    ],
  };
  for (let i = 0; i < nFallback; i++) {
    const sourceId = nextId();
    add('degenerate-fallback', {
      sourceId,
      geometry: i % 2 === 0 ? flat : unclosed,
      ugc: zones(2),
    });
    pushedOnce.push(sourceId);
  }

  // Chains. Each message lists every earlier one as a reference, as NWS does.
  for (let i = 0; i < nChains3; i++) {
    const ids = [nextId(), nextId(), nextId()] as [string, string, string];
    const geometry = polygonAt(0.04);
    const t0 = baseMs - 3600_000 - i * 1000;
    const sent = [t0, t0 + 600_000, t0 + 1_200_000] as const;
    add('chain-alert', { sourceId: ids[0], geometry, sentMs: sent[0] }, ids[0]);
    add(
      'chain-update',
      {
        sourceId: ids[1],
        messageType: 'Update',
        geometry,
        sentMs: sent[1],
        references: [{ sourceId: ids[0], sentMs: sent[0] }],
        expiredReferences: [expiredRef(ids[0], sent[0])],
      },
      ids[0],
    );
    add(
      'chain-update',
      {
        sourceId: ids[2],
        messageType: 'Update',
        geometry,
        sentMs: sent[2],
        references: [
          { sourceId: ids[0], sentMs: sent[0] },
          { sourceId: ids[1], sentMs: sent[1] },
        ],
        expiredReferences: [expiredRef(ids[1], sent[1])],
      },
      ids[0],
    );
    chains3.set(ids[0], ids);
  }
  for (let i = 0; i < nChainsCancel; i++) {
    const ids = [nextId(), nextId()] as [string, string];
    const geometry = polygonAt(0.04);
    const t0 = baseMs - 3600_000 - i * 1000;
    add('chain-alert', { sourceId: ids[0], geometry, sentMs: t0 }, ids[0]);
    add(
      'chain-cancel',
      {
        sourceId: ids[1],
        messageType: 'Cancel',
        geometry,
        sentMs: t0 + 600_000,
        references: [{ sourceId: ids[0], sentMs: t0 }],
      },
      ids[0],
    );
    chainsCancel.set(ids[0], ids);
  }

  // The feed lists messages in arbitrary order: shuffle (Fisher-Yates, seeded).
  for (let i = features.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [features[i], features[j]] = [features[j] as FeatureSpec, features[i] as FeatureSpec];
  }

  const expectedReady =
    pushedOnce.length +
    chains3.size * 3 +
    chainsCancel.size; /* the Alert; the Cancel plans nothing */
  return { features, chains3, chainsCancel, rejected, statements, pushedOnce, expectedReady };
}
