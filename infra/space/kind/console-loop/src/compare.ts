// Compare two runs by error type.
//
//   npm run compare -- <before-run> <after-run>
//
// A run is a run directory or its summary.json. An error's type is its
// signature without the step name: a known error surfacing on another page
// is not new. Exits 1 when the later run has an error type the earlier one
// does not.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { NON_FAILING, type StepResult } from './recorder.ts';

export interface RunSummary {
  baseUrl: string;
  startedAt: string;
  steps: number;
  results: StepResult[];
}

export interface Comparison {
  newTypes: string[];
  resolvedTypes: string[];
  // `<step>|<type>` signatures only the later run has, including known types
  // on new pages.
  newSignatures: string[];
}

export function readRun(location: string): RunSummary {
  const file = fs.statSync(location).isDirectory() ? path.join(location, 'summary.json') : location;
  return JSON.parse(fs.readFileSync(file, 'utf8')) as RunSummary;
}

function signatures(run: RunSummary): Set<string> {
  return new Set(run.results.flatMap((r) => r.events.filter((e) => !NON_FAILING.includes(e.kind)).map((e) => e.signature)));
}

const typeOf = (signature: string) => signature.slice(signature.indexOf('|') + 1);

export function compareRuns(before: RunSummary, after: RunSummary): Comparison {
  const a = signatures(before);
  const b = signatures(after);
  const typesA = new Set([...a].map(typeOf));
  const typesB = new Set([...b].map(typeOf));
  // Only steps both runs executed can resolve a type.
  const ranBoth = new Set(after.results.map((r) => r.name).filter((name) => before.results.some((r) => r.name === name)));
  return {
    newTypes: [...typesB].filter((t) => !typesA.has(t)).sort(),
    resolvedTypes: [...typesA].filter((t) => !typesB.has(t) && [...a].some((s) => typeOf(s) === t && ranBoth.has(s.slice(0, s.indexOf('|'))))).sort(),
    newSignatures: [...b].filter((s) => !a.has(s)).sort(),
  };
}

export function printComparison(comparison: Comparison): void {
  console.log(`${comparison.newTypes.length} new error type(s), ${comparison.resolvedTypes.length} resolved, ${comparison.newSignatures.length} new step signature(s)`);
  for (const t of comparison.newTypes) console.log(`  NEW      ${t}`);
  for (const t of comparison.resolvedTypes) console.log(`  RESOLVED ${t}`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const [before, after] = process.argv.slice(2);
  if (!before || !after) {
    console.error('usage: npm run compare -- <before-run> <after-run>');
    process.exit(2);
  }
  const comparison = compareRuns(readRun(before), readRun(after));
  printComparison(comparison);
  process.exit(comparison.newTypes.length ? 1 : 0);
}
