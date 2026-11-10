// One import and one array entry per line, so merges from parallel branches stay trivial.
import type { SuiteTest } from '../types.js';
import { t1Baseline } from './t1-baseline.js';
import { t3 } from './t3-checkins/index.js';

// prettier-ignore
export const SUITE_TESTS: SuiteTest[] = [
  t1Baseline,
  t3,
];
