// Every setting comes from a command-line flag or the environment, with the
// local Kind stack's defaults (see infra/space/kind/README.md).
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const PACKAGE_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export function flag(args: string[], name: string): boolean {
  return args.includes(`--${name}`);
}

export function option(args: string[], name: string, fallback?: string): string | undefined {
  const index = args.indexOf(`--${name}`);
  if (index >= 0 && index + 1 < args.length) return args[index + 1];
  const inline = args.find((arg) => arg.startsWith(`--${name}=`));
  return inline ? inline.slice(name.length + 3) : fallback;
}

export interface Config {
  // The console under test: the Tilt port-forward, or a `next dev` server.
  baseUrl: string;
  // The public and admin API, used by the seeder and for checks.
  apiUrl: string;
  email: string;
  password: string;
  // Runs, seed state and captured media are written here (git-ignored).
  outDir: string;
  // Names of seeded objects start with this, so cleanup finds only them.
  prefix: string;
}

export function loadConfig(args: string[]): Config {
  const env = process.env;
  return {
    baseUrl: trim(option(args, 'base-url', env.CONSOLE_LOOP_BASE_URL ?? 'http://localhost:3000')!),
    apiUrl: trim(option(args, 'api-url', env.CONSOLE_LOOP_API_URL ?? 'http://localhost:8080')!),
    // The account `scripts/up.sh` provisions; SPACE_CONSOLE_PASSWORD is the
    // same variable the bring-up reads.
    email: option(args, 'email', env.CONSOLE_LOOP_EMAIL ?? 'root@local')!,
    password: option(args, 'password', env.CONSOLE_LOOP_PASSWORD ?? env.SPACE_CONSOLE_PASSWORD ?? 'password')!,
    outDir: path.resolve(option(args, 'out', env.CONSOLE_LOOP_OUT ?? path.join(PACKAGE_DIR, 'out'))!),
    prefix: option(args, 'prefix', env.CONSOLE_LOOP_PREFIX ?? 'loop')!,
  };
}

function trim(url: string): string {
  return url.replace(/\/+$/, '');
}
