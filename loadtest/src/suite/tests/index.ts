// One import and one array entry per line, so merges from parallel branches stay trivial.
import type { SuiteTest } from '../types.js';
import { t1Baseline } from './t1-baseline.js';
import { t5Chaos } from './t5-chaos/index.js';
import { t6Ingest } from './t6-ingest/index.js';

// prettier-ignore
export const SUITE_TESTS: SuiteTest[] = [
  t1Baseline,
  t5Chaos,
  t6Ingest,
];
