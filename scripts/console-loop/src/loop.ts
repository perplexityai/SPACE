// Run the loop against a console.
//
//   npm run loop -- [--base-url URL] [--out DIR] [--only NAMES] [--skip NAMES]
//                   [--no-mutate] [--label NAME] [--compare RUN] [--slow-ms N] [--headed] [--video]
//                   [--allow-remote]
//
// Signs in through the real login page, runs the steps in src/steps.ts, and
// writes <out>/runs/<time>[-label]/ with one screenshot per step and
// summary.json/summary.md; <out>/runs/latest points at the newest run. With
// --compare it exits 1 when this run has an error type the given run does not.
// --only and --skip take comma-separated step names or tags, matched exactly.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { chromium } from 'playwright';

import { newContext, takeToasts, type Ctx } from './browser.ts';
import { compareRuns, printComparison, readRun, type RunSummary } from './compare.ts';
import { flag, guardTargets, loadConfig, option } from './config.ts';
import { NON_FAILING, Recorder, writeJson, type StepResult } from './recorder.ts';
import { readSeedState, seedStatePath } from './seed.ts';
import { proveSameStack } from './stack.ts';
import { steps, type Step } from './steps.ts';

// The names in a comma-separated --only or --skip value, each of which must
// be a step's name or tag; null when the option is absent.
export function parseSelection(value: string | undefined, known: readonly Pick<Step, 'name' | 'tags'>[], what: string): Set<string> | null {
  if (value === undefined) return null;
  const names = value.split(',').map((name) => name.trim()).filter(Boolean);
  if (!names.length) throw new Error(`--${what} names no step or tag`);
  const valid = new Set(known.flatMap((s) => [s.name, ...s.tags]));
  const unknown = names.filter((name) => !valid.has(name));
  if (unknown.length) throw new Error(`--${what}: no step or tag is named ${unknown.join(', ')}`);
  return new Set(names);
}

// Whether a step is selected: named by --only (or no --only) and not by --skip.
export function isSelected(s: Pick<Step, 'name' | 'tags'>, only: Set<string> | null, skip: Set<string> | null): boolean {
  const keys = [s.name, ...s.tags];
  return (!only || keys.some((key) => only.has(key))) && !(skip && keys.some((key) => skip.has(key)));
}

async function main(): Promise<number> {
  const args = process.argv.slice(2);
  const config = loadConfig(args);
  const only = parseSelection(option(args, 'only'), steps, 'only');
  const skip = parseSelection(option(args, 'skip'), steps, 'skip');
  const mutate = !flag(args, 'no-mutate');
  const slowMs = Number(option(args, 'slow-ms', '1500'));
  const label = option(args, 'label');
  const compareWith = option(args, 'compare');
  // A state for another prefix is refused on every run.
  const seed = readSeedState(config);
  // The mutating steps create and delete objects: only on this host unless
  // --allow-remote is given, only with a seed state to record them in, and
  // only once the console and the API are shown to be the stack that state
  // was seeded on. Reading-only runs may target any console.
  if (mutate) {
    guardTargets(config, 'loop');
    if (!seed) throw new Error(`refusing to run the mutating steps without a seed state (${seedStatePath(config)}): run \`npm run seed\` first, or pass --no-mutate`);
    await proveSameStack(config, seed.stack, 'loop', seedStatePath(config));
  } else {
    console.log(`[loop] target: console ${config.baseUrl} (read-only: --no-mutate)`);
  }
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
    seed,
    runTag: `${config.prefix}-run-${Date.now().toString(36)}`,
    shared: {},
  };

  const results: StepResult[] = [];
  let index = 0;
  let stopped: string | undefined;
  let videoDeleted: string | undefined;
  try {
    for (const s of steps) {
      const selected = isSelected(s, only, skip) && (mutate || !s.mutates);
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
      // A step that cleared the page because a live API key may have reached
      // it (see the api-key-create-revoke step) ends the run: no later step
      // runs or records anything.
      if (c.shared.secretExposed) {
        stopped = `stopped after ${s.name}: ${c.shared.secretExposed}`;
        console.log(stopped);
        break;
      }
    }
  } finally {
    // Closing the context finishes the recording. One that may show a live
    // key is deleted, not kept, however the run ended.
    const recording = page.video();
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
    const exposed = c.shared.secretExposed;
    if (recording && exposed) {
      fs.rmSync(path.join(runDir, '.video'), { recursive: true, force: true });
      videoDeleted = `deleted the run's video: ${exposed}`;
      console.log(videoDeleted);
    } else if (recording) {
      try {
        fs.renameSync(await recording.path(), path.join(runDir, 'run.webm'));
      } finally {
        fs.rmSync(path.join(runDir, '.video'), { recursive: true, force: true });
      }
    }
  }

  const summary: RunSummary & { mutate: boolean; slowMs: number; failingSteps: string[]; stopped?: string; videoDeleted?: string } = {
    baseUrl: config.baseUrl,
    startedAt: stamp,
    mutate,
    slowMs,
    steps: results.length,
    failingSteps: results.filter((r) => !r.ok).map((r) => r.name),
    stopped,
    videoDeleted,
    results,
  };
  writeJson(path.join(runDir, 'summary.json'), summary);
  fs.writeFileSync(path.join(runDir, 'summary.md'), markdown(summary, results, [stopped, videoDeleted]));
  const latest = path.join(config.outDir, 'runs', 'latest');
  fs.rmSync(latest, { force: true });
  fs.symlinkSync(path.basename(runDir), latest);
  console.log(`\n${results.length} steps, ${summary.failingSteps.length} with events → ${runDir}`);

  if (!before) return stopped ? 1 : 0;
  const comparison = compareRuns(before, summary);
  writeJson(path.join(runDir, 'comparison.json'), { against: compareWith, ...comparison });
  printComparison(comparison);
  return comparison.newTypes.length || stopped ? 1 : 0;
}

function markdown(summary: RunSummary, results: StepResult[], warnings: (string | undefined)[]): string {
  const lines = [`# Console loop run ${summary.startedAt}`, '', `Console: ${summary.baseUrl}`, ''];
  for (const warning of warnings) if (warning) lines.push(`**${warning}**`, '');
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
    console.error(err instanceof Error ? err.message : err);
    process.exit(2);
  });
}
