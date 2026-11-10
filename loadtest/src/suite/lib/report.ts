/** Shared helpers for the per-test markdown reports (`<outDir>/<id>.md`). Optional for tests to use. */
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { MachineInfo, ScenarioResult, SuiteContext, TestResult } from '../types.js';

export const fmt = (n: number, digits = 0): string =>
  n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });

export function machineLine(m: MachineInfo): string {
  return `${m.logicalCpus} logical CPUs (${m.cpuModel}), ${fmt(m.memoryTotalMb)} MB RAM (${fmt(m.memoryFreeAtStartMb)} MB free at start), Node ${m.node}, ${m.platform}`;
}

export function renderScenario(s: ScenarioResult): string {
  const out: string[] = [
    `### ${s.name}: ${s.verdict.toUpperCase()} (${fmt(s.durationMs / 1000, 1)} s)`,
    '',
  ];
  if (s.metrics.length) {
    out.push('| Measure | Value | Target |', '| --- | ---: | --- |');
    for (const m of s.metrics) {
      const t = m.target ? `${m.target.op} ${fmt(m.target.value, 2)}` : '';
      const digits = Number.isInteger(m.value) ? 0 : 2;
      out.push(`| ${m.name} | ${fmt(m.value, digits)} ${m.unit} | ${t} |`);
    }
    out.push('');
  }
  if (s.checks.length) {
    out.push('| Check | Result |', '| --- | --- |');
    for (const c of s.checks) {
      out.push(`| ${c.name} | ${c.ok ? 'ok' : 'FAILED'}${c.detail ? ` (${c.detail})` : ''} |`);
    }
    out.push('');
  }
  for (const n of s.notes ?? []) out.push(`- ${n}`);
  if (s.notes?.length) out.push('');
  return out.join('\n');
}

/** Generic report: title, intro paragraph, every scenario with metrics and checks, machine info. */
export function renderTestReport(ctx: SuiteContext, r: TestResult, intro: string): string {
  const out = [
    `# ${r.id}: ${r.title}`,
    '',
    `Verdict: **${r.verdict.toUpperCase()}**, ${fmt(r.durationMs / 1000, 1)} s, scale ${ctx.scale}.`,
    '',
    intro,
    '',
  ];
  if (r.error) out.push('Error:', '', '```', r.error, '```', '');
  for (const s of r.scenarios) out.push(renderScenario(s));
  if (r.peakRssMb !== undefined) out.push(`Peak RSS across processes: ${fmt(r.peakRssMb)} MB.`, '');
  out.push(`Machine: ${machineLine(ctx.machine)}.`, '');
  return out.join('\n');
}

export function writeTestReport(ctx: SuiteContext, r: TestResult, intro: string): string {
  const file = join(ctx.outDir, `${r.id}.md`);
  writeFileSync(file, renderTestReport(ctx, r, intro));
  return file;
}
