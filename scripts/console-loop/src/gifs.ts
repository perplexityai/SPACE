// Cut a run's recording into one GIF per step, with ffmpeg.
//
//   npm run gifs -- RUN [--steps NAMES] [--width N] [--fps N] [--max-seconds N]
//
// RUN is a run directory recorded with --video. Each step's place in
// run.webm is in summary.json (`video`); its GIF is
// <RUN>/gifs/<NNN>-<step>.gif. A step longer than --max-seconds (default 40)
// plays faster, so its GIF lasts that long; the GIF names the speed-up.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { option } from './config.ts';
import type { StepResult } from './recorder.ts';

export interface GifCut {
  step: string;
  file: string;
  fromS: number;
  durationS: number;
  // How many times faster than real time the GIF plays.
  speed: number;
}

export function gifFile(index: number, name: string): string {
  return `${String(index).padStart(3, '0')}-${name.replace(/[^a-z0-9]+/gi, '-').toLowerCase()}.gif`;
}

// The cut for each recorded step `steps` names (every one when null).
export function gifCuts(results: Pick<StepResult, 'index' | 'name' | 'video'>[], steps: Set<string> | null, maxSeconds: number): GifCut[] {
  return results
    .filter((r) => r.video && (!steps || steps.has(r.name)))
    .map((r) => {
      const fromS = Math.max(0, r.video!.fromMs / 1000);
      const durationS = Math.max(0.5, (r.video!.toMs - r.video!.fromMs) / 1000);
      return { step: r.name, file: gifFile(r.index, r.name), fromS, durationS, speed: Math.max(1, Math.ceil(durationS / maxSeconds)) };
    });
}

// ffmpeg's arguments for one cut: the step's span, sped up, at `fps` and
// `width`, through a palette of its own so text stays legible.
export function ffmpegArgs(webm: string, cut: GifCut, out: string, { width, fps }: { width: number; fps: number }): string[] {
  const vf = `setpts=PTS/${cut.speed},fps=${fps},scale=${width}:-1:flags=lanczos,split[a][b];[a]palettegen=stats_mode=diff[p];[b][p]paletteuse=dither=none`;
  return ['-loglevel', 'error', '-y', '-ss', cut.fromS.toFixed(2), '-t', cut.durationS.toFixed(2), '-i', webm, '-vf', vf, out];
}

function main(): void {
  const args = process.argv.slice(2);
  const run = args[0] && !args[0].startsWith('--') ? args[0] : undefined;
  if (!run) throw new Error('usage: npm run gifs -- RUN [--steps NAMES] [--width N] [--fps N] [--max-seconds N]');
  const webm = path.join(run, 'run.webm');
  if (!fs.existsSync(webm)) throw new Error(`${webm} is missing: record the run with --video`);
  const summary = JSON.parse(fs.readFileSync(path.join(run, 'summary.json'), 'utf8')) as { results: StepResult[] };
  const names = option(args, 'steps');
  const steps = names ? new Set(names.split(',').map((name) => name.trim()).filter(Boolean)) : null;
  const width = Number(option(args, 'width', '1280'));
  const fps = Number(option(args, 'fps', '5'));
  const cuts = gifCuts(summary.results, steps, Number(option(args, 'max-seconds', '40')));
  fs.mkdirSync(path.join(run, 'gifs'), { recursive: true });
  for (const cut of cuts) {
    const out = path.join(run, 'gifs', cut.file);
    execFileSync('ffmpeg', ffmpegArgs(webm, cut, out, { width, fps }));
    console.log(`${out} (${cut.durationS.toFixed(0)} s${cut.speed > 1 ? `, ${cut.speed}× speed` : ''})`);
  }
  fs.writeFileSync(path.join(run, 'gifs', 'cuts.json'), JSON.stringify(cuts, null, 2));
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  try {
    main();
  } catch (err) {
    console.error(err instanceof Error ? err.message : err);
    process.exit(2);
  }
}
