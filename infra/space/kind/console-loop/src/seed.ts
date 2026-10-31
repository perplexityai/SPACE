// Seed synthetic state for the loop, or remove it (`--clean`).
//
//   npm run seed [-- --skip-sandboxes]
//   npm run clean
//
// Everything goes through the public and admin APIs as the console account.
// Every object's name starts with the prefix (default `loop`), so seeding is
// rerunnable (existing objects are reused) and cleanup touches nothing else.
// What was created is recorded in <out>/seed.json, which the driver reads.
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { Api, ApiError, type Operation } from './api.ts';
import { flag, loadConfig, option, type Config } from './config.ts';

export interface SeedState {
  prefix: string;
  managingOrgId: string;
  // Password of every seeded user; generated once, kept only in this file.
  userPassword: string;
  orgs: Record<string, string>;
  users: Record<string, { id: string; org: string; role: string }>;
  apiKeys: Record<string, string>;
  sandboxes: Record<string, string>;
  notes: string[];
}

export function seedStatePath(config: Config): string {
  return path.join(config.outDir, 'seed.json');
}

export function readSeedState(config: Config): SeedState | null {
  const file = seedStatePath(config);
  return fs.existsSync(file) ? (JSON.parse(fs.readFileSync(file, 'utf8')) as SeedState) : null;
}

interface Org { id: string; name: string; slug: string; parent_id: string | null }
interface User { id: string; email: string }
interface ApiKey { id: string; name: string }
interface AdminSandbox { id: string; name: string | null; status: string | null; org_id: string | null }

const log = (message: string) => console.log(`[seed] ${message}`);
const publicId = (id: string) => (id.startsWith('sbx_') ? id : `sbx_${id}`);

class Seeder {
  private readonly api: Api;
  private readonly config: Config;
  private readonly template: string;
  private state: SeedState;

  constructor(api: Api, config: Config, template: string) {
    this.api = api;
    this.config = config;
    this.template = template;
    const previous = readSeedState(config);
    this.state = previous && previous.prefix === config.prefix ? previous : {
      prefix: config.prefix,
      managingOrgId: '',
      userPassword: `loop-${randomBytes(12).toString('base64url')}`,
      orgs: {},
      users: {},
      apiKeys: {},
      sandboxes: {},
      notes: [],
    };
  }

  private name(suffix: string): string {
    return `${this.config.prefix}-${suffix}`;
  }

  private note(text: string): void {
    log(`NOTE ${text}`);
    if (!this.state.notes.includes(text)) this.state.notes.push(text);
  }

  save(): void {
    fs.mkdirSync(this.config.outDir, { recursive: true });
    const file = seedStatePath(this.config);
    fs.writeFileSync(`${file}.tmp`, JSON.stringify(this.state, null, 2), { mode: 0o600 });
    fs.renameSync(`${file}.tmp`, file);
  }

  async orgs(): Promise<void> {
    const cluster = await this.api.admin<{ managing_org_id: string }>('GET', '/cluster');
    this.state.managingOrgId = cluster.managing_org_id;
    this.state.orgs.managing = cluster.managing_org_id;
    const bySlug = new Map((await this.api.admin<Org[]>('GET', '/orgs')).map((org) => [org.slug, org]));
    // Two tenant orgs, one with a child org.
    for (const [key, title, parent] of [
      ['acme', 'Acme (synthetic)', null],
      ['globex', 'Globex (synthetic)', null],
      ['globex-research', 'Globex Research (synthetic)', 'globex'],
    ] as const) {
      const slug = this.name(key);
      let org = bySlug.get(slug);
      if (!org) {
        const parentId = parent ? this.state.orgs[parent] : null;
        org = await this.api.admin<Org>('POST', '/orgs', { name: title, slug, parent_id: parentId });
        bySlug.set(slug, org);
        log(`created org ${slug}`);
      }
      this.state.orgs[key] = org.id;
    }
  }

  async users(): Promise<void> {
    for (const [org, local, displayName, role] of [
      ['managing', 'ops-admin', 'Ops Admin', 'organization_admin'],
      ['managing', 'ops-viewer', 'Ops Viewer', 'member'],
      ['acme', 'acme-admin', 'Acme Admin', 'organization_admin'],
      ['acme', 'acme-dev', 'Acme Developer', 'member'],
      ['globex', 'globex-dev', 'Globex Developer', 'member'],
      ['globex-research', 'research-dev', 'Research Developer', 'member'],
    ] as const) {
      const orgId = this.state.orgs[org];
      const email = `${this.name(local)}@example.test`;
      const existing = (await this.api.admin<User[]>('GET', `/orgs/${orgId}/users`)).find((user) => user.email === email);
      const user = existing ?? (await this.api.admin<User>('POST', `/orgs/${orgId}/users`, {
        email, password: this.state.userPassword, display_name: displayName, role,
      }));
      if (!existing) {
        log(`created user ${email}`);
      } else if (!this.state.users[email]) {
        // Reused from an earlier seed whose state is gone: give it the
        // password this state records, so the role steps can sign in.
        await this.api.admin('PATCH', `/users/${user.id}`, { password: this.state.userPassword });
      }
      this.state.users[email] = { id: user.id, org, role };
    }
  }

  // The admin API issues keys only for the signed-in user's own (managing)
  // organization, so tenant orgs get none.
  async apiKeys(): Promise<void> {
    const orgId = this.state.managingOrgId;
    const keys = new Map((await this.api.admin<ApiKey[]>('GET', `/orgs/${orgId}/api-keys`)).map((key) => [key.name, key]));
    for (const [suffix, surfaces] of [['native', ['native']], ['ecw', ['ecw']]] as const) {
      const name = this.name(suffix);
      let key = keys.get(name);
      if (!key) {
        key = (await this.api.admin<{ api_key: ApiKey }>('POST', `/orgs/${orgId}/api-keys`, { name, allowed_surfaces: surfaces })).api_key;
        log(`created API key ${name}`);
      }
      this.state.apiKeys[suffix] = key.id;
    }
  }

  private async adminSandboxes(): Promise<AdminSandbox[]> {
    return this.api.admin<AdminSandbox[]>('GET', '/sandboxes?limit=5000');
  }

  private async waitFor(id: string, states: string[], timeoutMs = 300_000): Promise<string> {
    const deadline = Date.now() + timeoutMs;
    let status = '';
    while (Date.now() < deadline) {
      status = (await this.api.admin<{ status: string | null }>('GET', `/sandboxes/${id}`)).status ?? '';
      if (states.includes(status)) return status;
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
    throw new Error(`${id} is ${status || 'unknown'}, not ${states.join(' or ')}`);
  }

  private async create(name: string, org: string): Promise<string> {
    const live = (await this.adminSandboxes()).find((s) => s.name === name && !['deleted', 'deleting'].includes(s.status ?? ''));
    if (live) return publicId(live.id);
    const sandbox = { template: this.template, runtime: 'fc', name };
    let accepted: { operation_id: string; sandbox_id?: string };
    let op: Operation;
    if (org === 'managing') {
      accepted = await this.api.public('POST', '/sandboxes', sandbox, { idempotent: true, timeoutMs: 120_000 });
      op = await this.api.waitForOperation(accepted.operation_id);
    } else {
      accepted = await this.api.admin('POST', '/sandboxes', { org_id: this.state.orgs[org], sandbox }, { idempotent: true, timeoutMs: 120_000 });
      op = await this.api.waitForOperation(accepted.operation_id, { admin: true });
    }
    if (op.state !== 'succeeded') this.note(`create ${name}: operation ${op.state} ${op.progress_message ?? ''}`);
    log(`created sandbox ${name}`);
    return publicId(accepted.sandbox_id ?? op.target_id ?? '');
  }

  private async act(id: string, action: string): Promise<void> {
    const accepted = await this.api.admin<{ operation_id: string }>('POST', `/sandboxes/${id}:force-${action}`, undefined, { idempotent: true, timeoutMs: 120_000 });
    const op = await this.api.waitForOperation(accepted.operation_id, { admin: true });
    if (op.state !== 'succeeded') this.note(`${action} ${id}: operation ${op.state} ${op.progress_message ?? ''}`);
  }

  // One sandbox per resting state, one in a tenant org, and one deleted (its
  // operations outlive it).
  async sandboxes(): Promise<void> {
    for (const [key, org, action, state] of [
      ['running', 'managing', null, 'started'],
      ['paused', 'managing', 'pause', 'paused'],
      ['suspended', 'managing', 'suspend', 'suspended'],
      ['stopped', 'managing', 'stop', 'stopped'],
      ['acme-running', 'acme', null, 'started'],
    ] as const) {
      try {
        const id = await this.create(this.name(key), org);
        this.state.sandboxes[key] = id;
        this.save();
        const current = (await this.api.admin<{ status: string | null }>('GET', `/sandboxes/${id}`)).status;
        if (current === state) continue;
        await this.waitFor(id, ['started', state]);
        if (action) {
          await this.act(id, action);
          await this.waitFor(id, [state]);
          log(`${this.name(key)} is ${state}`);
        }
      } catch (err) {
        this.note(`sandbox ${key}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    if (!this.state.sandboxes.deleted) {
      try {
        const id = await this.create(this.name('deleted'), 'managing');
        await this.waitFor(id, ['started']);
        await this.act(id, 'delete');
        this.state.sandboxes.deleted = id;
        log(`${this.name('deleted')} created and deleted`);
      } catch (err) {
        this.note(`sandbox deleted: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
  }

  async run(skipSandboxes: boolean): Promise<SeedState> {
    for (const step of [() => this.orgs(), () => this.users(), () => this.apiKeys()]) {
      await step();
      this.save();
    }
    if (!skipSandboxes) await this.sandboxes();
    this.save();
    return this.state;
  }
}

// Delete everything named with the prefix: sandboxes, users, API keys, then
// orgs (children before parents).
async function clean(api: Api, config: Config): Promise<void> {
  const prefix = `${config.prefix}-`;
  const failures: string[] = [];
  const attempt = async (what: string, run: () => Promise<unknown>) => {
    try {
      await run();
      log(`deleted ${what}`);
    } catch (err) {
      failures.push(`${what}: ${err instanceof ApiError ? `${err.status} ${err.body.slice(0, 200)}` : String(err)}`);
    }
  };
  const sandboxes = (await api.admin<AdminSandbox[]>('GET', '/sandboxes?limit=5000'))
    .filter((s) => s.name?.startsWith(prefix) && !['deleted', 'deleting'].includes(s.status ?? ''));
  for (const sandbox of sandboxes) {
    await attempt(`sandbox ${sandbox.name}`, async () => {
      const accepted = await api.admin<{ operation_id: string }>('POST', `/sandboxes/${publicId(sandbox.id)}:force-delete`, undefined, { idempotent: true });
      await api.waitForOperation(accepted.operation_id, { admin: true });
    });
  }
  const cluster = await api.admin<{ managing_org_id: string }>('GET', '/cluster');
  const orgs = await api.admin<Org[]>('GET', '/orgs');
  for (const org of orgs.filter((o) => o.id === cluster.managing_org_id || o.slug.startsWith(prefix))) {
    for (const user of (await api.admin<User[]>('GET', `/orgs/${org.id}/users`)).filter((u) => u.email.startsWith(prefix))) {
      await attempt(`user ${user.email}`, () => api.admin('DELETE', `/users/${user.id}`));
    }
  }
  const keys = await api.admin<ApiKey[]>('GET', `/orgs/${cluster.managing_org_id}/api-keys`);
  for (const key of keys.filter((k) => k.name.startsWith(prefix))) {
    await attempt(`API key ${key.name}`, () => api.admin('DELETE', `/orgs/${cluster.managing_org_id}/api-keys/${key.id}`));
  }
  const ours = orgs.filter((o) => o.slug.startsWith(prefix));
  const depth = (org: Org): number => (org.parent_id && ours.some((o) => o.id === org.parent_id) ? 1 + depth(ours.find((o) => o.id === org.parent_id)!) : 0);
  for (const org of ours.sort((a, b) => depth(b) - depth(a))) {
    await attempt(`org ${org.slug}`, () => api.admin('DELETE', `/orgs/${org.id}`));
  }
  fs.rmSync(seedStatePath(config), { force: true });
  if (failures.length) {
    for (const failure of failures) log(`could not delete ${failure}`);
    process.exitCode = 1;
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const config = loadConfig(args);
  const api = new Api(config);
  await api.signIn(config.email, config.password);
  if (flag(args, 'clean')) return clean(api, config);
  const template = option(args, 'template', process.env.CONSOLE_LOOP_TEMPLATE ?? 'seed:latest')!;
  const state = await new Seeder(api, config, template).run(flag(args, 'skip-sandboxes'));
  log(`seeded under "${state.prefix}"; state in ${path.relative(process.cwd(), seedStatePath(config)) || seedStatePath(config)}`);
  for (const note of state.notes) log(`note: ${note}`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main().catch((err) => {
    console.error(err instanceof Error ? err.message : err);
    process.exit(2);
  });
}
