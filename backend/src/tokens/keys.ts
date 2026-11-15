import type { TokenKey } from './checkInToken.js';

/**
 * Loads signing keys at runtime. The real implementation will read from AWS
 * Secrets Manager (not written yet; this is only the contract).
 */
export interface LoadTokenKeys {
  (): Promise<{
    /** Key used to sign new tokens. */
    current: TokenKey;
    /** All keys accepted for verification (includes `current`), by key id. */
    verify: Map<string, string>;
  }>;
}
