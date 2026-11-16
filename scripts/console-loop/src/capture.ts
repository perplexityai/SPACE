// Record a short video (and a GIF, when ffmpeg is installed) of console
// pages, for a pull request's description. Output goes under <out>/media;
// attach it to the PR instead of committing it.
//
//   npm run capture -- --name NAME --route /dashboard/nodes [--route /dashboard/orgs …]
//                      [--click LABEL] [--scroll] [--loopback-hosts] [--base-url URL]
//                      [--width 1280 --height 800]
//
// Signs in as the console account, then opens each route in turn, pausing
// on each, clicking the button LABEL names where the page has one (for an
// action whose result is the point, such as a preview; see `clickTarget`),
// and scrolling it with --scroll. A LABEL that names more than one button
// stops the capture.
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { chromium } from 'playwright';

import { buttonCandidates, clickTarget, settle, showLoopbackHostsOnly } from './browser.ts';
import { flag, loadConfig, option } from './config.ts';

function routes(args: string[]): string[] {
  return args.flatMap((arg, i) => (arg === '--route' && args[i + 1] ? [args[i + 1]] : []));
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const config = loadConfig(args);
  const name = option(args, 'name', 'capture')!.replace(/[^a-z0-9-]+/gi, '-');
  const paths = routes(args);
  if (!paths.length) throw new Error('give at least one --route');
  const width = Number(option(args, 'width', '1280'));
  const height = Number(option(args, 'height', '800'));
  const mediaDir = path.join(config.outDir, 'media');
  const videoDir = path.join(mediaDir, `.video-${name}`);
  fs.mkdirSync(videoDir, { recursive: true });

  const browser = await chromium.launch();
  // Sign in outside the recording, then record only the console pages.
  const signInContext = await browser.newContext();
  const signInPage = await signInContext.newPage();
  await signInPage.goto(`${config.baseUrl}/login`);
  await signInPage.getByLabel('Email').fill(config.email);
  await signInPage.getByLabel('Password').fill(config.password);
  await signInPage.getByRole('button', { name: /sign in|log in|login/i }).click();
  await signInPage.waitForURL(/\/dashboard/, { timeout: 20_000 });
  const storageState = await signInContext.storageState();
  await signInContext.close();

  const context = await browser.newContext({ viewport: { width, height }, storageState, recordVideo: { dir: videoDir, size: { width, height } } });
  if (flag(args, 'loopback-hosts')) await showLoopbackHostsOnly(context);
  const page = await context.newPage();
  for (const route of paths) {
    await page.goto(`${config.baseUrl}${route}`);
    await settle(page, 2000);
    const click = option(args, 'click');
    if (click) {
      const buttons = await buttonCandidates(page);
      const target = clickTarget(buttons.map((button) => button.candidate), click);
      if (target === null) {
        console.log(`click: no button ${JSON.stringify(click)} on ${route}`);
      } else {
        await buttons[target].locator.click();
        await settle(page, 6000);
      }
    }
    if (flag(args, 'scroll')) {
      for (let i = 0; i < 3; i++) {
        await page.mouse.wheel(0, 600);
        await page.waitForTimeout(900);
      }
    }
  }
  const video = page.video();
  await context.close();
  await browser.close();
  const webm = path.join(mediaDir, `${name}.webm`);
  fs.renameSync(await video!.path(), webm);
  fs.rmSync(videoDir, { recursive: true, force: true });
  console.log(`video: ${webm}`);
  try {
    const gif = path.join(mediaDir, `${name}.gif`);
    execFileSync('ffmpeg', ['-loglevel', 'error', '-y', '-i', webm, '-vf', `fps=8,scale=${Math.min(width, 960)}:-1:flags=lanczos`, gif]);
    console.log(`gif:   ${gif}`);
  } catch {
    console.log('gif:   skipped (ffmpeg not found)');
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main().catch((err) => {
    console.error(err instanceof Error ? err.message : err);
    process.exit(2);
  });
}
