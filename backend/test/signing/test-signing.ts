import { createMemoryAlertSigning } from '../../src/signing/memory.js';

/** One in-memory ECDSA P-256 alert key for tests (kid "a1"). No AWS. */
export const TEST_SIGNING = createMemoryAlertSigning('a1');
/** A second, unrelated key: what an attacker without kms:Sign could sign with. */
export const ATTACKER_SIGNING = createMemoryAlertSigning('a1');
