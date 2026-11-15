/**
 * Pieces shared by the mock APNs and FCM servers: a seeded RNG, the latency model, the injected
 * error mix, per-token counters, a token bucket and a tiny CLI argument parser.
 */
import { generate } from 'selfsigned';

// ---------- Seeded RNG ----------

export type Rng = () => number;

/** mulberry32: small, fast and good enough for load shaping. Returns floats in [0, 1). */
export function createRng(seed: number): Rng {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ---------- Latency ----------

export interface LatencyConfig {
  /** Median response latency in milliseconds. */
  p50Ms: number;
  /** 99th-percentile response latency in milliseconds. */
  p99Ms: number;
}

export const DEFAULT_LATENCY: LatencyConfig = { p50Ms: 20, p99Ms: 150 };

const Z99 = 2.326347874;

export function normal(rng: Rng): number {
  // Box-Muller; 1 - u keeps the log argument above zero.
  const u = 1 - rng();
  const v = rng();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

/**
 * Log-normal latency fitted so that the median is p50Ms and the 99th percentile is p99Ms.
 * Truncated at four times p99 so one sample cannot stall a run.
 */
export function createLatencySampler(config: LatencyConfig, rng: Rng): () => number {
  if (config.p50Ms <= 0 || config.p99Ms <= 0) return () => 0;
  const mu = Math.log(config.p50Ms);
  const sigma = Math.max(0, Math.log(Math.max(config.p99Ms, config.p50Ms) / config.p50Ms) / Z99);
  const cap = config.p99Ms * 4;
  return () => Math.min(cap, Math.exp(mu + sigma * normal(rng)));
}

export function sleep(ms: number): Promise<void> {
  return ms <= 0 ? Promise.resolve() : new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------- Error mix ----------

export interface ErrorMix {
  /** Probability of "token is unregistered" (APNs 410 Unregistered; FCM 404 UNREGISTERED). */
  unregistered: number;
  /** Probability of 429 (too many requests). */
  tooManyRequests: number;
  /** Probability of 500 (server error). */
  serverError: number;
}

export const DEFAULT_ERROR_MIX: ErrorMix = {
  unregistered: 0.005,
  tooManyRequests: 0.002,
  serverError: 0.001,
};

export type Outcome = 'ok' | 'unregistered' | 'tooManyRequests' | 'serverError';

/** Draws exactly one random number per call, so a seeded run gives the same outcome sequence. */
export function pickOutcome(mix: ErrorMix, rng: Rng): Outcome {
  const r = rng();
  if (r < mix.unregistered) return 'unregistered';
  if (r < mix.unregistered + mix.tooManyRequests) return 'tooManyRequests';
  if (r < mix.unregistered + mix.tooManyRequests + mix.serverError) return 'serverError';
  return 'ok';
}

// ---------- Counters ----------

/**
 * Per-token message counters. `delivered` counts accepted (HTTP 200) messages; a token that was
 * answered with 429 or 500 and then retried successfully is still delivered exactly once.
 * `attempts` counts every request that reached the handler with a usable token.
 */
export class TokenCounters {
  /** @param trackAttempts set false to skip the per-token attempt map (saves memory at 1M tokens). */
  constructor(private readonly trackAttempts = true) {}

  readonly attempts = new Map<string, number>();
  readonly delivered = new Map<string, number>();
  /** Tokens answered with "unregistered" (a terminal answer, not a delivery). */
  readonly unregistered = new Map<string, number>();

  recordAttempt(token: string): void {
    if (!this.trackAttempts) return;
    this.attempts.set(token, (this.attempts.get(token) ?? 0) + 1);
  }

  recordDelivered(token: string): void {
    this.delivered.set(token, (this.delivered.get(token) ?? 0) + 1);
  }

  recordUnregistered(token: string): void {
    this.unregistered.set(token, (this.unregistered.get(token) ?? 0) + 1);
  }

  deliveredCount(token: string): number {
    return this.delivered.get(token) ?? 0;
  }

  reset(): void {
    this.attempts.clear();
    this.delivered.clear();
    this.unregistered.clear();
  }
}

export interface ResponseStats {
  /** Requests that reached the handler. */
  received: number;
  ok: number;
  unregistered: number;
  tooManyRequests: number;
  serverError: number;
  /** Validation failures (400, 401, 403, 404 for unknown paths, ...). */
  rejected: number;
}

export function emptyResponseStats(): ResponseStats {
  return { received: 0, ok: 0, unregistered: 0, tooManyRequests: 0, serverError: 0, rejected: 0 };
}

// ---------- Token bucket ----------

export class TokenBucket {
  private tokens: number;
  private last: number;

  constructor(
    private readonly ratePerSecond: number,
    private readonly burst: number,
    private readonly now: () => number = () => performance.now(),
  ) {
    this.tokens = burst;
    this.last = now();
  }

  /** Takes one token. Returns 0 when allowed, otherwise the seconds until one is available. */
  take(): number {
    const t = this.now();
    this.tokens = Math.min(this.burst, this.tokens + ((t - this.last) / 1000) * this.ratePerSecond);
    this.last = t;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return 0;
    }
    return (1 - this.tokens) / this.ratePerSecond;
  }
}

// ---------- TLS ----------

export interface TlsMaterial {
  key: string;
  cert: string;
}

/** Generates a throwaway self-signed certificate for localhost. Never write it to disk or commit it. */
export async function generateSelfSignedCert(): Promise<TlsMaterial> {
  const pems = await generate([{ name: 'commonName', value: 'localhost' }], {
    keySize: 2048,
    algorithm: 'sha256',
    extensions: [
      {
        name: 'subjectAltName',
        altNames: [
          { type: 2, value: 'localhost' },
          { type: 7, ip: '127.0.0.1' },
        ],
      },
    ],
  });
  return { key: pems.private, cert: pems.cert };
}

// ---------- CLI ----------

/** Parses `--name value` and bare `--flag` arguments. */
export function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === undefined || !arg.startsWith('--')) continue;
    const name = arg.slice(2);
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith('--')) {
      out[name] = next;
      i++;
    } else {
      out[name] = 'true';
    }
  }
  return out;
}

export function numberArg(args: Record<string, string>, name: string): number | undefined {
  const v = args[name];
  if (v === undefined) return undefined;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`--${name} must be a number`);
  return n;
}

export function logJson(event: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ level: 'info', event, ...fields }));
}

/** True when this module is the process entry point (works under tsx). */
export function isMain(metaUrl: string): boolean {
  const entry = process.argv[1];
  return entry !== undefined && new URL(metaUrl).pathname === new URL(`file://${entry}`).pathname;
}
