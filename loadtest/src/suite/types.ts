/**
 * Scalability test suite: the shared contract. Each test is one module in ./tests/ that exports a
 * `SuiteTest`. The runner (./runner.ts) runs the selected tests one after another (they compete for
 * the CPU, so never in parallel), writes `<out>/<id>.json` and `<out>/<id>.md` per test, and a
 * combined `<out>/SUMMARY.md`.
 *
 * Built to run natively on the developer's machine (Windows 11 PowerShell, or Linux/macOS) with
 * Node 22: no bash, no POSIX-only APIs, paths via node:path, child processes via child_process.fork
 * or spawn(process.execPath, ...). Heavy work runs in separate processes so all CPU cores are used.
 */

export interface MachineInfo {
  cpuModel: string;
  logicalCpus: number;
  memoryTotalMb: number;
  memoryFreeAtStartMb: number;
  platform: string;
  node: string;
}

export interface SuiteContext {
  /** Directory for this test's results (already created). */
  outDir: string;
  /** Scratch directory for large generated data (seeds), reused between runs; already created. */
  dataDir: string;
  machine: MachineInfo;
  /** Number of worker processes a test should use for CPU-bound parallel work. */
  workers: number;
  /** 'full' = the sizes in the plan; 'smoke' = tiny sizes for CI and a quick check (< 2 min total). */
  scale: 'full' | 'smoke';
  /** Absolute path of the repo root. */
  repoRoot: string;
  /** Docker services, if test 4 started them (null otherwise). */
  services: { dynamoEndpoint: string; postgresUrl: string } | null;
  log(msg: string, fields?: Record<string, unknown>): void;
}

export type Verdict = 'pass' | 'fail' | 'skipped';

export interface Metric {
  name: string;
  value: number;
  unit: string;
  /** Optional threshold the verdict used, for the report. */
  target?: { op: '<=' | '>=' | '=='; value: number };
}

export interface ScenarioResult {
  name: string;
  verdict: Verdict;
  durationMs: number;
  metrics: Metric[];
  /** Correctness checks, e.g. "missed devices == 0". */
  checks: Array<{ name: string; ok: boolean; detail?: string }>;
  notes?: string[];
}

export interface TestResult {
  id: string;
  title: string;
  verdict: Verdict;
  startedAt: string;
  durationMs: number;
  scenarios: ScenarioResult[];
  /** Peak RSS across this test's processes, MB (best effort). */
  peakRssMb?: number;
  error?: string;
}

export interface SuiteTest {
  /** 't1' .. 't7' */
  id: string;
  title: string;
  /** One line shown before the test starts. */
  description: string;
  /** Rough duration at 'full' scale on a 12-core / 32 GB machine, minutes. */
  estimatedMinutes: number;
  requiresDocker?: boolean;
  run(ctx: SuiteContext): Promise<TestResult>;
}
