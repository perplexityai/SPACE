// Seed synthetic state for the loop, or remove it (`--clean`).
//
//   npm run seed [-- --skip-sandboxes]
//   npm run clean
//
// Everything goes through the public and admin APIs as the console account,
// and only against this host unless --allow-remote is given. Every object's
// name starts with the prefix (default `loop`). Seeding is rerunnable: it
// looks each object up by its prefixed name before creating it, so an
// interrupted run leaves nothing a rerun duplicates. Every object the seed or
// the loop creates is recorded in <out>/seed.json as soon as it exists, and
// cleanup deletes only recorded objects.
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { Api, ApiError, type Operation } from './api.ts';
import { flag, guardTargets, loadConfig, option, type Config } from './config.ts';

export type CreatedKind = 'org' | 'user' | 'apiKey' | 'sandbox';

export interface CreatedObject {
  kind: CreatedKind;
  id: string;
  name: string;
  // The org an API key belongs to; the org a sandbox or user was created in.
  orgId?: string;
}

export interface SeedState {
  prefix: string;
  managingOrgId: string;
  // Password of every seeded user; generated once, kept only in this file.
  userPassword: string;
  orgs: Record<string, string>;
  users: Record<string, { id: string; org: string; role: string }>;
  apiKeys: Record<string, string>;
  sandboxes: Record<string, string>;
  // Everything the seed and the loop created, in creation order.
  created: CreatedObject[];
  notes: string[];
}

export function seedStatePath(config: Config): string {
  return path.join(config.outDir, 'seed.json');
}

export function readSeedState(config: Config): SeedState | null {
  const file = seedStatePath(config);
  if (!fs.existsSync(file)) return null;
  const state = JSON.parse(fs.readFileSync(file, 'utf8')) as SeedState;
  state.created ??= [];
  return state;
}

export function writeSeedState(config: Config, state: SeedState): void {
  fs.mkdirSync(config.outDir, { recursive: true });
  const file = seedStatePath(config);
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(state, null, 2), { mode: 0o600 });
  fs.renameSync(`${file}.tmp`, file);
}

// Record an object the loop created, so `npm run clean` can remove it if the
// step that created it did not get to delete it.
export function recordCreated(config: Config, object: CreatedObject): void {
  const state = readSeedState(config);
  if (!state) return;
  if (!state.created.some((o) => o.kind === object.kind && o.id === object.id)) state.created.push(object);
  writeSeedState(config, state);
}

interface Org { id: string; name: string; slug: string; parent_id: string | null }
interface User { id: string; email: string }
interface ApiKey { id: string; name: string }
interface AdminSandbox { id: string; name: string | null; status: string | null; org_id: string | null; resource_type?: string }

// The admin sandbox list has no cursor; past this many rows it cannot prove
// that a sandbox is absent.
const LIST_LIMIT = 5000;

const log = (message: string) => console.log(`[seed] ${message}`);
const publicId = (id: string) => (id.startsWith('sbx_') ? id : `sbx_${id}`);
const isLive = (s: AdminSandbox) => !['deleted', 'deleting'].includes(s.status ?? '');

// The sandboxes of one org whose name is `name`, or a loud failure when the
// org has too many sandboxes to tell.
async function findSandboxesByName(api: Api, orgId: string, name: string): Promise<AdminSandbox[]> {
  const rows = await api.admin<AdminSandbox[]>('GET', `/sandboxes?org_id=${orgId}&limit=${LIST_LIMIT}`);
  const found = rows.filter((s) => s.name === name && isLive(s));
  if (!found.length && rows.filter((s) => s.resource_type === undefined || s.resource_type === 'sandbox').length >= LIST_LIMIT) {
    throw new Error(`org ${orgId} has at least ${LIST_LIMIT} sandboxes, so the admin list cannot show whether ${name} exists`);
  }
  return found;
}

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
      created: [],
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

  private record(object: CreatedObject): void {
    if (!this.state.created.some((o) => o.kind === object.kind && o.id === object.id)) this.state.created.push(object);
    this.save();
  }

  save(): void {
    writeSeedState(this.config, this.state);
  }

  async orgs(): Promise<void> {
    const cluster = await this.api.admin<{ managing_org_id: string }>('GET', '/cluster');
    this.state.managingOrgId = cluster.managing_org_id;
    this.state.orgs.managing = cluster.managing_org_id;
    const bySlug = new Map((await this.api.admin<Org[]>('GET', '/orgs')).map((org) => [org.slug, org]));
    // Two tenant orgs, one with a child org. Display names carry the prefix,
    // so runs under two prefixes on one stack never pick each other's orgs.
    for (const [key, title, parent] of [
      ['acme', 'Acme', null],
      ['globex', 'Globex', null],
      ['globex-research', 'Globex Research', 'globex'],
    ] as const) {
      const slug = this.name(key);
      let org = bySlug.get(slug);
      if (!org) {
        const parentId = parent ? this.state.orgs[parent] : null;
        org = await this.api.admin<Org>('POST', '/orgs', { name: orgDisplayName(this.config.prefix, title), slug, parent_id: parentId });
        bySlug.set(slug, org);
        log(`created org ${slug}`);
      }
      // A reused org carries the prefix, so it is this seed's too (cleanup
      // still refuses it while it holds anything unrecorded).
      this.record({ kind: 'org', id: org.id, name: slug });
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
        this.record({ kind: 'user', id: user.id, name: email, orgId });
        log(`created user ${email}`);
      } else if (!this.state.users[email]) {
        // Reused from an earlier seed whose state is gone: give it the
        // password this state records, so the role steps can sign in.
        await this.api.admin('PATCH', `/users/${user.id}`, { password: this.state.userPassword });
        this.record({ kind: 'user', id: user.id, name: email, orgId });
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
        // The response carries the key's secret; it is not kept.
        key = (await this.api.admin<{ api_key: ApiKey }>('POST', `/orgs/${orgId}/api-keys`, { name, allowed_surfaces: surfaces })).api_key;
        log(`created API key ${name}`);
      }
      this.record({ kind: 'apiKey', id: key.id, name, orgId });
      this.state.apiKeys[suffix] = key.id;
    }
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
    const orgId = this.state.orgs[org];
    const existing = await findSandboxesByName(this.api, orgId, name);
    if (existing.length > 1) this.note(`${existing.length} live sandboxes are named ${name}; using the first`);
    if (existing.length) {
      const id = publicId(existing[0].id);
      this.record({ kind: 'sandbox', id, name, orgId });
      return id;
    }
    const sandbox = { template: this.template, runtime: 'fc', name };
    let accepted: { operation_id: string; sandbox_id?: string };
    let op: Operation;
    if (org === 'managing') {
      accepted = await this.api.public('POST', '/sandboxes', sandbox, { idempotent: true, timeoutMs: 120_000 });
    } else {
      accepted = await this.api.admin('POST', '/sandboxes', { org_id: orgId, sandbox }, { idempotent: true, timeoutMs: 120_000 });
    }
    // Record the sandbox before waiting: an interrupted wait must not leave
    // an unrecorded sandbox behind.
    if (accepted.sandbox_id) this.record({ kind: 'sandbox', id: publicId(accepted.sandbox_id), name, orgId });
    op = await this.api.waitForOperation(accepted.operation_id, { admin: org !== 'managing' });
    const id = publicId(accepted.sandbox_id ?? op.target_id ?? '');
    this.record({ kind: 'sandbox', id, name, orgId });
    if (op.state !== 'succeeded') this.note(`create ${name}: operation ${op.state} ${op.progress_message ?? ''}`);
    log(`created sandbox ${name}`);
    return id;
  }

  private async act(id: string, action: string): Promise<void> {
    const accepted = await this.api.admin<{ operation_id: string }>('POST', `/sandboxes/${id}:force-${action}`, undefined, { idempotent: true, timeoutMs: 120_000 });
    const op = await this.api.waitForOperation(accepted.operation_id, { admin: true });
    if (op.state !== 'succeeded') throw new Error(`${action} ${id}: operation ${op.state} ${op.progress_message ?? ''}`);
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

  // Every recorded fixture must still exist, looked up by its ID.
  async verify(): Promise<void> {
    const missing: string[] = [];
    for (const [key, id] of Object.entries(this.state.orgs)) {
      await this.api.admin('GET', `/orgs/${id}`).catch(() => missing.push(`org ${key}`));
    }
    for (const [email, user] of Object.entries(this.state.users)) {
      await this.api.admin('GET', `/users/${user.id}`).catch(() => missing.push(`user ${email}`));
    }
    for (const [key, id] of Object.entries(this.state.sandboxes)) {
      if (key === 'deleted') continue;
      await this.api.admin('GET', `/sandboxes/${id}`).catch(() => missing.push(`sandbox ${key}`));
    }
    if (missing.length) throw new Error(`seeded fixtures are missing: ${missing.join(', ')}`);
  }

  async run(skipSandboxes: boolean): Promise<SeedState> {
    for (const step of [() => this.orgs(), () => this.users(), () => this.apiKeys()]) {
      await step();
      this.save();
    }
    if (!skipSandboxes) await this.sandboxes();
    this.save();
    await this.verify();
    return this.state;
  }
}

export function orgDisplayName(prefix: string, title: string): string {
  return `${title} (${prefix})`;
}

// Delete what the state recorded: sandboxes, then users and API keys, then
// orgs, children first. An org is deleted only when it holds nothing the
// state did not record; otherwise it is left, and reported.
async function clean(api: Api, config: Config): Promise<number> {
  const state = readSeedState(config);
  if (!state) {
    log(`nothing to clean: no ${seedStatePath(config)}`);
    return 0;
  }
  const recorded = (kind: CreatedKind) => state.created.filter((o) => o.kind === kind);
  const ids = new Set(state.created.map((o) => `${o.kind}:${o.id.replace(/^sbx_/, '')}`));
  const isRecorded = (kind: CreatedKind, id: string) => ids.has(`${kind}:${id.replace(/^sbx_/, '')}`);
  const failures: string[] = [];
  const done = new Set<string>();
  const attempt = async (object: CreatedObject, run: () => Promise<void>) => {
    try {
      await run();
      done.add(`${object.kind}:${object.id}`);
      log(`deleted ${object.kind} ${object.name}`);
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        done.add(`${object.kind}:${object.id}`);
        log(`already gone: ${object.kind} ${object.name}`);
        return;
      }
      failures.push(`${object.kind} ${object.name}: ${err instanceof ApiError ? `${err.status} ${err.body.slice(0, 200)}` : err instanceof Error ? err.message : String(err)}`);
    }
  };

  for (const sandbox of recorded('sandbox')) {
    await attempt(sandbox, async () => {
      const current = await api.admin<AdminSandbox>('GET', `/sandboxes/${publicId(sandbox.id)}`);
      if (!isLive(current)) return;
      const accepted = await api.admin<{ operation_id: string }>('POST', `/sandboxes/${publicId(sandbox.id)}:force-delete`, undefined, { idempotent: true });
      const op = await api.waitForOperation(accepted.operation_id, { admin: true });
      if (op.state !== 'succeeded') throw new Error(`delete operation ${op.state}${op.progress_message ? `: ${op.progress_message}` : ''}`);
    });
  }
  for (const user of recorded('user')) {
    await attempt(user, () => api.admin('DELETE', `/users/${user.id}`));
  }
  for (const key of recorded('apiKey')) {
    await attempt(key, () => api.admin('DELETE', `/orgs/${key.orgId}/api-keys/${key.id}`));
  }

  const orgs = await api.admin<Org[]>('GET', '/orgs');
  const ours = recorded('org');
  const depth = (id: string): number => {
    const parent = orgs.find((o) => o.id === id)?.parent_id;
    return parent ? 1 + depth(parent) : 0;
  };
  for (const org of [...ours].sort((a, b) => depth(b.id) - depth(a.id))) {
    await attempt(org, async () => {
      const foreign: string[] = [];
      for (const child of orgs.filter((o) => o.parent_id === org.id && !done.has(`org:${o.id}`))) foreign.push(`child org ${child.slug}`);
      for (const user of await api.admin<User[]>('GET', `/orgs/${org.id}/users`)) {
        if (!isRecorded('user', user.id) || !done.has(`user:${user.id}`)) foreign.push(`user ${user.email}`);
      }
      for (const key of await api.admin<ApiKey[]>('GET', `/orgs/${org.id}/api-keys`)) foreign.push(`API key ${key.name}`);
      for (const sandbox of (await api.admin<AdminSandbox[]>('GET', `/sandboxes?org_id=${org.id}&limit=${LIST_LIMIT}`)).filter(isLive)) {
        if (sandbox.org_id === org.id) foreign.push(`sandbox ${sandbox.name ?? sandbox.id}`);
      }
      if (foreign.length) throw new Error(`refused: it still holds ${foreign.join(', ')}`);
      await api.admin('DELETE', `/orgs/${org.id}`);
    });
  }

  const remaining = state.created.filter((o) => !done.has(`${o.kind}:${o.id}`));
  if (!remaining.length) {
    fs.rmSync(seedStatePath(config), { force: true });
    log('everything recorded is deleted');
  } else {
    state.created = remaining;
    writeSeedState(config, state);
  }
  for (const failure of failures) log(`NOT deleted: ${failure}`);
  if (failures.length) log(`${failures.length} object(s) not deleted; still recorded in ${seedStatePath(config)}`);
  return failures.length ? 1 : 0;
}

async function main(): Promise<number> {
  const args = process.argv.slice(2);
  const config = loadConfig(args);
  const cleaning = flag(args, 'clean');
  guardTargets(config, cleaning ? 'clean' : 'seed');
  const api = new Api(config);
  await api.signIn(config.email, config.password);
  if (cleaning) return clean(api, config);
  const template = option(args, 'template', process.env.CONSOLE_LOOP_TEMPLATE ?? 'seed:latest')!;
  const state = await new Seeder(api, config, template).run(flag(args, 'skip-sandboxes'));
  log(`seeded under "${state.prefix}"; state in ${seedStatePath(config)}`);
  for (const note of state.notes) log(`note: ${note}`);
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main().then((code) => process.exit(code), (err) => {
    console.error(err instanceof Error ? err.message : err);
    process.exit(2);
  });
}
