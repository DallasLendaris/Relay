import { createHash, timingSafeEqual } from 'node:crypto';
import { loadSecretString } from '../config/secrets.js';

/** The header CloudFront adds to every request it forwards to the API origin. */
export const ORIGIN_VERIFY_HEADER = 'x-relay-origin-verify';

const TTL_MS = 5 * 60 * 1000;

export interface OriginGuardOptions {
  /** Reads the shared secret (Secrets Manager in AWS). Cached, so not one call per request. */
  loadSecret: () => Promise<string>;
  now?: () => number;
}

/**
 * Rejects requests that did not come through CloudFront (and so skipped the WAF): anyone can
 * call the execute-api URL directly, but only CloudFront knows the secret it adds as a header.
 * The compare is constant time: both sides are hashed to equal length first, so neither the
 * length nor the position of the first difference is observable.
 */
export function createOriginGuard(opts: OriginGuardOptions) {
  const now = opts.now ?? Date.now;
  let cached: { at: number; digest: Buffer } | undefined;

  async function expectedDigest(): Promise<Buffer> {
    if (cached && now() - cached.at < TTL_MS) return cached.digest;
    const secret = (await opts.loadSecret()).trim();
    if (secret === '') throw new Error('origin verify secret is empty');
    cached = { at: now(), digest: createHash('sha256').update(secret).digest() };
    return cached.digest;
  }

  return {
    /** True when the request carries the right header. Throws if the secret cannot be loaded. */
    async allows(headers: Record<string, string | undefined>): Promise<boolean> {
      const expected = await expectedDigest();
      const given = headers[ORIGIN_VERIFY_HEADER];
      // Hash even a missing header so every path does the same work.
      const digest = createHash('sha256')
        .update(typeof given === 'string' ? given : '')
        .digest();
      const match = timingSafeEqual(digest, expected);
      return typeof given === 'string' && given !== '' && match;
    },
  };
}

export type OriginGuard = ReturnType<typeof createOriginGuard>;

/** Production wiring: the secret comes from Secrets Manager (ORIGIN_VERIFY_SECRET_ARN). */
export const secretsManagerOriginGuard = (secretArn: string): OriginGuard =>
  createOriginGuard({ loadSecret: () => loadSecretString(secretArn) });
