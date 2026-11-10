// One import and one array entry per line, so merges from parallel branches stay trivial.
import type { SuiteTest } from '../types.js';
import { t1Baseline } from './t1-baseline.js';
import { t2 } from './t2-fanout/index.js';
import { t3 } from './t3-checkins/index.js';
import { t4Databases } from './t4-databases/index.js';
import { t7Soak } from './t7-soak/index.js';

// prettier-ignore
export const SUITE_TESTS: SuiteTest[] = [
  t1Baseline,
  t2,
  t3,
  t4Databases,
  t7Soak,
];
