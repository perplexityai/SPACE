// Run the loop against a console.
//
//   npm run loop -- [--base-url URL] [--out DIR] [--only REGEX] [--skip REGEX]
//                   [--no-mutate] [--label NAME] [--compare RUN] [--slow-ms N] [--headed] [--video]
//
// Signs in through the real login page, runs the steps in src/steps.ts, and
// writes <out>/runs/<time>[-label]/ with one screenshot per step and
// summary.json/summary.md; <out>/runs/latest points at the newest run. With
// --compare it exits 1 when this run has an error type the given run does not.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { chromium } from 'playwright';

import { newContext, takeToasts, type Ctx } from './browser.ts';
import { compareRuns, printComparison, readRun, type RunSummary } from './compare.ts';
import { flag, loadConfig, option } from './config.ts';
import { NON_FAILING, Recorder, writeJson, type StepResult } from './recorder.ts';
import { readSeedState } from './seed.ts';
import { steps } from './steps.ts';

async function main(): Promise<number> {
  const args = process.argv.slice(2);
  const config = loadConfig(args);
  const only = new RegExp(option(args, 'only', '.')!);
  const skipPattern = option(args, 'skip');
  const skip = skipPattern ? new RegExp(skipPattern) : null;
  const mutate = !flag(args, 'no-mutate');
  const slowMs = Number(option(args, 'slow-ms', '1500'));
  const label = option(args, 'label');
  const compareWith = option(args, 'compare');
  // Read the comparison run before this run can replace `latest`.
  const before: RunSummary | null = compareWith ? readRun(compareWith) : null;

  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const runDir = path.join(config.outDir, 'runs', label ? `${stamp}-${label.replace(/[^a-z0-9-]+/gi, '-')}` : stamp);
  fs.mkdirSync(runDir, { recursive: true });

  const browser = await chromium.launch({ headless: !flag(args, 'headed') });
  // --video records the whole run as run.webm in the run directory.
  const video = flag(args, 'video');
  const context = await newContext(browser, video ? { videoDir: path.join(runDir, '.video') } : {});
  const page = await context.newPage();
  const rec = new Recorder(page, { slowMs, prefix: config.prefix, outDir: runDir });
  const c: Ctx = {
    page, context, rec, config,
    seed: readSeedState(config),
    runTag: `${config.prefix}-run-${Date.now().toString(36)}`,
    shared: {},
  };

  const results: StepResult[] = [];
  let index = 0;
  for (const s of steps) {
    const key = [s.name, ...s.tags].join(' ');
    const selected = only.test(key) && !(skip && skip.test(key)) && (mutate || !s.mutates);
    // Sign-in always runs, so a filtered run is still signed in.
    if (!selected && !s.tags.includes('auth')) continue;
    if (!selected && s.name === 'logout') continue;
    index += 1;
    rec.begin(s.name);
    const startedAt = Date.now();
    let failure: unknown = null;
    try {
      await s.run(c);
    } catch (err) {
      failure = err;
    }
    // An error toast is a failed check; others are notes.
    for (const toast of await takeToasts(page).catch(() => [])) {
      if (toast.type === 'error' || toast.type === 'warning') rec.check(false, `toast ${toast.type}: ${toast.text}`);
      else rec.note(`toast ${toast.type}: ${toast.text}`);
    }
    const result = await rec.end(index, startedAt, failure);
    results.push(result);
    const bad = result.events.filter((e) => !NON_FAILING.includes(e.kind));
    console.log(`${result.ok ? 'ok  ' : 'FAIL'} ${String(index).padStart(3)} ${s.name} (${result.durationMs} ms)${bad.length ? ` — ${bad.length} event(s)` : ''}`);
    for (const e of bad) console.log(`       ${e.kind}: ${e.message}`);
  }
  const recording = page.video();
  await context.close();
  await browser.close();
  if (recording) {
    fs.renameSync(await recording.path(), path.join(runDir, 'run.webm'));
    fs.rmSync(path.join(runDir, '.video'), { recursive: true, force: true });
  }

  const summary: RunSummary & { mutate: boolean; slowMs: number; failingSteps: string[] } = {
    baseUrl: config.baseUrl,
    startedAt: stamp,
    mutate,
    slowMs,
    steps: results.length,
    failingSteps: results.filter((r) => !r.ok).map((r) => r.name),
    results,
  };
  writeJson(path.join(runDir, 'summary.json'), summary);
  fs.writeFileSync(path.join(runDir, 'summary.md'), markdown(summary, results));
  const latest = path.join(config.outDir, 'runs', 'latest');
  fs.rmSync(latest, { force: true });
  fs.symlinkSync(path.basename(runDir), latest);
  console.log(`\n${results.length} steps, ${summary.failingSteps.length} with events → ${runDir}`);

  if (!before) return 0;
  const comparison = compareRuns(before, summary);
  writeJson(path.join(runDir, 'comparison.json'), { against: compareWith, ...comparison });
  printComparison(comparison);
  return comparison.newTypes.length ? 1 : 0;
}

function markdown(summary: RunSummary, results: StepResult[]): string {
  const lines = [`# Console loop run ${summary.startedAt}`, '', `Console: ${summary.baseUrl}`, ''];
  for (const r of results) {
    lines.push(`## ${r.index}. ${r.name} — ${r.ok ? 'ok' : 'events'}`, '', `URL: ${r.url}  `, `Screenshot: ${r.screenshot ?? 'none'}`, '');
    for (const e of r.events) lines.push(`- **${e.kind}** ${e.message}${e.body ? `\n  \`${e.body.replace(/\s+/g, ' ').slice(0, 300)}\`` : ''}`);
    for (const n of r.notes) lines.push(`- ${n}`);
    lines.push('');
  }
  return lines.join('\n');
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main().then((code) => process.exit(code), (err) => {
    console.error(err);
    process.exit(2);
  });
}
