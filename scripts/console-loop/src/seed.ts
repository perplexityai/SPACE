// Seed synthetic state for the loop, or remove it (`--clean`).
//
//   npm run seed [-- --skip-sandboxes]
//   npm run clean [-- --delete-orgs]
//
// Everything goes through the public and admin APIs as the console account,
// only against this host unless --allow-remote is given, and only once the
// console and the API have been shown to be one stack (src/stack.ts). Every
// object's name starts with the prefix (default `loop`), and the state lives
// in <out>/seed-<prefix>.json. Each created object is recorded there as soon
// as the API answers, and cleanup deletes only recorded objects. A rerun
// reuses what exists by name. The stack is disposable: when a state is no
// longer usable, delete it and reseed, or recreate the stack.
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { Api, ApiError, TERMINAL_OPERATION_STATES, sleep, type Operation } from './api.ts';
import { flag, guardTargets, loadConfig, option, type Config } from './config.ts';
import { API_KEYS, ORGS, SANDBOXES, USERS, userEmail, verifyFixtures, type Get } from './fixtures.ts';
import { proveSameStack, type StackIdentity } from './stack.ts';

export type CreatedKind = 'org' | 'user' | 'apiKey' | 'sandbox';

// One object the seed or the loop created, recorded once the API answered
// its create.
export interface CreatedObject {
  kind: CreatedKind;
  name: string;
  id: string;
  // The org an API key belongs to; the org a sandbox or user was created in.
  orgId?: string;
  // A sandbox create's operation, from its answer. The answer names the
  // sandbox before the operation may have made it (see readRecordedSandbox).
  createOperation?: { id: string; admin: boolean };
}

export interface SeedState {
  prefix: string;
  // The stack the state was seeded on; see src/stack.ts.
  stack: StackIdentity;
  managingOrgId: string;
  // Password of every seeded user; generated once, kept only in this file.
  userPassword: string;
  // False when the last seed ran with --skip-sandboxes.
  withSandboxes: boolean;
  orgs: Record<string, string>;
  users: Record<string, { id: string; org: string; role: string }>;
  apiKeys: Record<string, string>;
  sandboxes: Record<string, string>;
  // Everything the seed and the loop created, in creation order.
  created: CreatedObject[];
  notes: string[];
}

export function seedStatePath(config: Config): string {
  return path.join(config.outDir, `seed-${config.prefix}.json`);
}

// The way out of a state that cannot be used.
export const resetAdvice = (file: string) => `delete ${file} and run \`npm run seed\` again, or recreate the stack`;

// The state for --prefix, or null when there is none. A state recorded for
// another prefix, or without the stack it was seeded on, is refused.
export function readSeedState(config: Config): SeedState | null {
  const file = seedStatePath(config);
  if (!fs.existsSync(file)) return null;
  let state: SeedState;
  try {
    state = JSON.parse(fs.readFileSync(file, 'utf8')) as SeedState;
  } catch (err) {
    throw new Error(`cannot read ${file} (${err instanceof Error ? err.message : String(err)}): ${resetAdvice(file)}`);
  }
  if (state.prefix !== config.prefix) {
    throw new Error(`refusing ${file}: it records prefix "${state.prefix}", not --prefix "${config.prefix}"`);
  }
  if (!state.stack?.clusterId) throw new Error(`refusing ${file}: it does not record the stack it was seeded on; ${resetAdvice(file)}`);
  state.created ??= [];
  return state;
}

export function writeSeedState(config: Config, state: SeedState): void {
  fs.mkdirSync(config.outDir, { recursive: true });
  const file = seedStatePath(config);
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(state, null, 2), { mode: 0o600 });
  fs.renameSync(`${file}.tmp`, file);
}

// Add an object to the state, or replace the one of that kind and name.
export function upsertCreated(state: SeedState, object: CreatedObject): void {
  const index = state.created.findIndex((o) => o.kind === object.kind && o.name === object.name);
  if (index < 0) state.created.push(object);
  else state.created[index] = object;
}

// Record an object a loop step created, as soon as the API answers, so
// `npm run clean` can remove it if the step that created it did not get to
// delete it.
export function recordCreated(config: Config, object: CreatedObject): void {
  const state = readSeedState(config);
  if (!state) throw new Error(`no seed state at ${seedStatePath(config)} to record ${object.kind} ${object.name} in`);
  upsertCreated(state, object);
  writeSeedState(config, state);
}

interface Org { id: string; name: string; slug: string; parent_id: string | null }
interface User { id: string; email: string }
interface ApiKey { id: string; name: string }
interface AdminSandbox { id: string; name: string | null; status: string | null; org_id: string | null; resource_type?: string }
interface Accepted { operation_id: string; sandbox_id?: string }

// The admin sandbox list has no cursor; past this many rows it cannot prove
// that a sandbox is absent.
const LIST_LIMIT = 5000;
const OPERATION_TIMEOUT_MS = 600_000;
// How long a sandbox whose create was accepted may take to appear.
const PENDING_CREATE_TIMEOUT_MS = 600_000;

let label = 'seed';
const log = (message: string) => console.log(`[${label}] ${message}`);
const describe = (err: unknown) => (err instanceof ApiError ? `${err.status} ${err.body.slice(0, 200)}` : err instanceof Error ? err.message : String(err));
const publicId = (id: string) => (id.startsWith('sbx_') ? id : `sbx_${id}`);
const isLive = (s: AdminSandbox) => !['deleted', 'deleting'].includes(s.status ?? '');

// An admin GET for verifyFixtures, through the API client.
const apiGet = (api: Api): Get => async (p) => {
  try {
    return { status: 200, json: await api.request('GET', p) };
  } catch (err) {
    if (err instanceof ApiError) return { status: err.status, json: null };
    throw err;
  }
};

async function getSandbox(api: Api, id: string): Promise<AdminSandbox | null> {
  try {
    return await api.admin<AdminSandbox>('GET', `/sandboxes/${publicId(id)}`);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

// The state of an operation, or null when the API no longer has it (it
// deletes finished operations after a week).
async function operationState(api: Api, op: { id: string; admin: boolean }): Promise<string | null> {
  try {
    const path = `/operations/${op.id}`;
    return (op.admin ? await api.admin<Operation>('GET', path) : await api.public<Operation>('GET', path)).state;
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

// A recorded sandbox, or null once it is known not to exist. A create's
// answer names the sandbox while its operation can still be queued, before
// the sandbox exists, so a 404 for a sandbox with a recorded create
// operation waits for that operation to finish and reads again. A create
// still unfinished after `timeoutMs` throws, and the record stays.
export async function readRecordedSandbox(
  api: Api,
  entry: CreatedObject,
  { pollMs = 2000, timeoutMs = PENDING_CREATE_TIMEOUT_MS }: { pollMs?: number; timeoutMs?: number } = {},
): Promise<AdminSandbox | null> {
  const current = await getSandbox(api, entry.id);
  const op = entry.createOperation;
  if (current || !op) return current;
  const deadline = Date.now() + timeoutMs;
  let state = await operationState(api, op);
  if (state && !TERMINAL_OPERATION_STATES.includes(state)) log(`waiting for the create operation ${op.id} of ${entry.name} (${state})`);
  while (state && !TERMINAL_OPERATION_STATES.includes(state)) {
    if (Date.now() >= deadline) throw new Error(`${entry.id} does not exist yet, and its create operation ${op.id} is still ${state}; it stays recorded`);
    await sleep(pollMs);
    state = await operationState(api, op);
  }
  return getSandbox(api, entry.id);
}

// The live sandboxes of one org whose name is `name`, or a loud failure
// when the org has too many sandboxes to tell.
async function findSandboxesByName(api: Api, orgId: string, name: string): Promise<AdminSandbox[]> {
  const rows = await api.admin<AdminSandbox[]>('GET', `/sandboxes?org_id=${orgId}&limit=${LIST_LIMIT}`);
  const found = rows.filter((s) => s.name === name && isLive(s));
  if (!found.length && rows.filter((s) => s.resource_type === undefined || s.resource_type === 'sandbox').length >= LIST_LIMIT) {
    throw new Error(`org ${orgId} has at least ${LIST_LIMIT} sandboxes, so the admin list cannot show whether ${name} exists`);
  }
  return found;
}

// Org slugs the seed creates and reuses; any other recorded org is one a
// loop run created.
export const isSeededOrg = (prefix: string, slug: string) => ORGS.some(([key]) => slug === `${prefix}-${key}`);

class Seeder {
  private readonly api: Api;
  private readonly config: Config;
  private readonly template: string;
  private readonly state: SeedState;

  constructor(api: Api, config: Config, template: string, previous: SeedState | null, stack: StackIdentity) {
    this.api = api;
    this.config = config;
    this.template = template;
    this.state = previous ?? {
      prefix: config.prefix,
      stack,
      managingOrgId: '',
      userPassword: `loop-${randomBytes(12).toString('base64url')}`,
      withSandboxes: false,
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

  private record(object: CreatedObject): string {
    upsertCreated(this.state, object);
    this.save();
    return object.id;
  }

  save(): void {
    writeSeedState(this.config, this.state);
  }

  // Orgs, users and API keys are found by slug, email or key name; one that
  // exists carries the prefix, so it is this seed's and is reused.
  async orgs(): Promise<void> {
    const cluster = await this.api.admin<{ managing_org_id: string }>('GET', '/cluster');
    this.state.managingOrgId = cluster.managing_org_id;
    this.state.orgs.managing = cluster.managing_org_id;
    const bySlug = new Map((await this.api.admin<Org[]>('GET', '/orgs')).map((org) => [org.slug, org]));
    // Two tenant orgs, one with a child org. Display names carry the prefix,
    // so runs under two prefixes on one stack never pick each other's orgs.
    for (const [key, title, parent] of ORGS) {
      const slug = this.name(key);
      let org = bySlug.get(slug);
      if (!org) {
        const parentId = parent ? this.state.orgs[parent] : null;
        org = await this.api.admin<Org>('POST', '/orgs', { name: orgDisplayName(this.config.prefix, title), slug, parent_id: parentId });
        bySlug.set(slug, org);
        log(`created org ${slug}`);
      }
      this.state.orgs[key] = this.record({ kind: 'org', id: org.id, name: slug });
    }
  }

  async users(): Promise<void> {
    for (const [org, local, displayName, role] of USERS) {
      const orgId = this.state.orgs[org];
      const email = userEmail(this.config.prefix, local);
      const existing = (await this.api.admin<User[]>('GET', `/orgs/${orgId}/users`)).find((user) => user.email === email);
      if (existing && !this.state.users[email]) {
        // Reused from an earlier seed whose state is gone: give it the
        // password this state records, so the role steps can sign in.
        await this.api.admin('PATCH', `/users/${existing.id}`, { password: this.state.userPassword });
      }
      const user = existing ?? (await this.api.admin<User>('POST', `/orgs/${orgId}/users`, {
        email, password: this.state.userPassword, display_name: displayName, role,
      }));
      if (!existing) log(`created user ${email}`);
      this.record({ kind: 'user', id: user.id, name: email, orgId });
      this.state.users[email] = { id: user.id, org, role };
    }
  }

  // The admin API issues keys only for the signed-in user's own (managing)
  // organization, so tenant orgs get none.
  async apiKeys(): Promise<void> {
    const orgId = this.state.managingOrgId;
    const keys = new Map((await this.api.admin<ApiKey[]>('GET', `/orgs/${orgId}/api-keys`)).map((key) => [key.name, key]));
    for (const [suffix, surfaces] of API_KEYS) {
      const name = this.name(suffix);
      let key = keys.get(name);
      if (!key) {
        // The response carries the key's secret; it is not kept.
        key = (await this.api.admin<{ api_key: ApiKey }>('POST', `/orgs/${orgId}/api-keys`, { name, allowed_surfaces: surfaces })).api_key;
        log(`created API key ${name}`);
      }
      this.state.apiKeys[suffix] = this.record({ kind: 'apiKey', id: key.id, name, orgId });
    }
  }

  private async waitFor(id: string, states: string[], timeoutMs = 300_000): Promise<string> {
    const deadline = Date.now() + timeoutMs;
    let status = '';
    while (Date.now() < deadline) {
      status = (await this.api.admin<{ status: string | null }>('GET', `/sandboxes/${id}`)).status ?? '';
      if (states.includes(status)) return status;
      await sleep(3000);
    }
    throw new Error(`${id} is ${status || 'unknown'}, not ${states.join(' or ')}`);
  }

  // A live sandbox named `name` in `org`: the recorded one, one found by
  // name, or a new one. A recorded create that has not made its sandbox yet
  // is waited for, never replaced.
  private async create(name: string, org: string): Promise<string> {
    const orgId = this.state.orgs[org];
    const recorded = this.state.created.find((o) => o.kind === 'sandbox' && o.name === name);
    if (recorded) {
      const current = await readRecordedSandbox(this.api, recorded);
      if (current && isLive(current)) return recorded.id;
    }
    const existing = await findSandboxesByName(this.api, orgId, name);
    if (existing.length > 1) this.note(`${existing.length} live sandboxes are named ${name}; using the first`);
    if (existing.length) return this.record({ kind: 'sandbox', id: publicId(existing[0].id), name, orgId });

    const admin = org !== 'managing';
    const sandbox = { template: this.template, runtime: 'fc', name };
    const options = { idempotent: true, timeoutMs: 120_000 };
    const accepted = admin
      ? await this.api.admin<Accepted>('POST', '/sandboxes', { org_id: orgId, sandbox }, options)
      : await this.api.public<Accepted>('POST', '/sandboxes', sandbox, options);
    const createOperation = { id: accepted.operation_id, admin };
    if (accepted.sandbox_id) this.record({ kind: 'sandbox', id: publicId(accepted.sandbox_id), name, orgId, createOperation });
    const op = await this.api.waitForOperation(accepted.operation_id, { admin, timeoutMs: OPERATION_TIMEOUT_MS });
    const target = accepted.sandbox_id ?? op.target_id;
    if (target) this.record({ kind: 'sandbox', id: publicId(target), name, orgId, createOperation });
    if (op.state !== 'succeeded' || !target) throw new Error(`create ${name}: operation ${op.state} ${op.progress_message ?? ''}`);
    log(`created sandbox ${name}`);
    return publicId(target);
  }

  private async act(id: string, action: string): Promise<void> {
    const accepted = await this.api.admin<{ operation_id: string }>('POST', `/sandboxes/${id}:force-${action}`, undefined, { idempotent: true, timeoutMs: 120_000 });
    const op = await this.api.waitForOperation(accepted.operation_id, { admin: true, timeoutMs: OPERATION_TIMEOUT_MS });
    if (op.state !== 'succeeded') throw new Error(`${action} ${id}: operation ${op.state} ${op.progress_message ?? ''}`);
  }

  // One sandbox per resting state, one in a tenant org, and one deleted. Any
  // that cannot be set up fails the seed, after the others are tried.
  async sandboxes(): Promise<void> {
    const failures: string[] = [];
    for (const [key, org, action, state] of SANDBOXES) {
      try {
        const name = this.name(key);
        const id = await this.create(name, org);
        this.state.sandboxes[key] = id;
        this.save();
        const current = (await this.api.admin<{ status: string | null }>('GET', `/sandboxes/${id}`)).status;
        if (current === state) continue;
        await this.waitFor(id, ['started', state]);
        if (action) {
          await this.act(id, action);
          await this.waitFor(id, [state]);
          log(`${name} is ${state}`);
        }
      } catch (err) {
        failures.push(`sandbox ${key}: ${describe(err)}`);
      }
    }
    try {
      await this.deletedSandbox();
    } catch (err) {
      failures.push(`sandbox deleted: ${describe(err)}`);
    }
    if (failures.length) throw new Error(`seeding sandboxes failed: ${failures.join('; ')}`);
  }

  private async deletedSandbox(): Promise<void> {
    if (this.state.sandboxes.deleted) return;
    const name = this.name('deleted');
    const id = await this.create(name, 'managing');
    await this.waitFor(id, ['started']);
    await this.act(id, 'delete');
    this.state.sandboxes.deleted = id;
    log(`${name} created and deleted`);
  }

  async run(skipSandboxes: boolean): Promise<SeedState> {
    this.save();
    for (const step of [() => this.orgs(), () => this.users(), () => this.apiKeys()]) {
      await step();
      this.save();
    }
    this.state.withSandboxes = !skipSandboxes;
    if (!skipSandboxes) await this.sandboxes();
    this.save();
    const report = await verifyFixtures(this.state, apiGet(this.api), { statuses: true });
    if (report.problems.length) throw new Error(`seeded fixtures are missing or not at rest: ${report.problems.join(', ')}`);
    log(`verified ${report.ok.length} fixtures by ID`);
    return this.state;
  }
}

export function orgDisplayName(prefix: string, title: string): string {
  return `${title} (${prefix})`;
}

// Delete one recorded sandbox and wait until it is deleted or no longer
// found by ID. An absent sandbox is never deleted: the admin delete of an
// absent target records the operation in the caller's org.
export async function cleanSandbox(api: Api, entry: CreatedObject, wait?: { pollMs?: number; timeoutMs?: number }): Promise<string> {
  let current = await readRecordedSandbox(api, entry, wait);
  if (!current) return 'already gone';
  if (current.status !== 'deleted' && current.status !== 'deleting') {
    const accepted = await api.admin<{ operation_id: string }>('POST', `/sandboxes/${entry.id}:force-delete`, undefined, { idempotent: true });
    const op = await api.waitForOperation(accepted.operation_id, { admin: true, timeoutMs: OPERATION_TIMEOUT_MS });
    current = await getSandbox(api, entry.id);
    // A delete that fails because another delete got there first is fine
    // once the sandbox is going.
    if (op.state !== 'succeeded' && current && current.status !== 'deleted' && current.status !== 'deleting') {
      throw new Error(`delete operation ${op.state}${op.progress_message ? `: ${op.progress_message}` : ''}`);
    }
  }
  const deadline = Date.now() + OPERATION_TIMEOUT_MS;
  if (current?.status === 'deleting') log(`waiting for ${entry.name} (${entry.id}) to finish deleting`);
  while (current?.status === 'deleting') {
    if (Date.now() > deadline) throw new Error(`${entry.id} is still deleting`);
    await sleep(3000);
    current = await getSandbox(api, entry.id);
  }
  if (current && current.status !== 'deleted') throw new Error(`${entry.id} is ${current.status} after its delete`);
  return 'deleted';
}

// Delete what the state recorded: sandboxes, then users and API keys, then
// the orgs loop runs created. The seeded orgs are kept, since a seed reuses
// them by slug, unless `deleteOrgs` is set. Orgs are deleted children first,
// and each is refused while it holds a child org, a user, an API key, or a
// sandbox that is not deleted. Its credentials, credential references,
// master keys, template grants and other cascaded rows are not checked:
// they go with the org (see README.md). A sandbox whose create is still
// pending after `wait` is a failure and stays recorded.
export async function clean(
  api: Api,
  config: Config,
  state: SeedState,
  { deleteOrgs, wait }: { deleteOrgs: boolean; wait?: { pollMs?: number; timeoutMs?: number } },
): Promise<number> {
  const failures: string[] = [];
  const gone = new Set<CreatedObject>();
  const recorded = (kind: CreatedKind) => state.created.filter((o) => o.kind === kind);
  const attempt = async (object: CreatedObject, run: () => Promise<string>) => {
    try {
      log(`${await run()}: ${object.kind} ${object.name}`);
      gone.add(object);
    } catch (err) {
      failures.push(`${object.kind} ${object.name}: ${describe(err)}`);
    }
  };
  // DELETE, where a 404 is gone (these deletes take effect at once).
  const remove = async (apiPath: string): Promise<string> => {
    try {
      await api.admin('DELETE', apiPath);
      return 'deleted';
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) return 'already gone';
      throw err;
    }
  };

  for (const sandbox of recorded('sandbox')) await attempt(sandbox, () => cleanSandbox(api, sandbox, wait));
  for (const user of recorded('user')) await attempt(user, () => remove(`/users/${user.id}`));
  for (const key of recorded('apiKey')) await attempt(key, () => remove(`/orgs/${key.orgId}/api-keys/${key.id}`));

  const orgs = await api.admin<Org[]>('GET', '/orgs');
  const deletedOrgs = new Set<string>();
  const kept: string[] = [];
  const depth = (id: string): number => {
    const parent = orgs.find((o) => o.id === id)?.parent_id;
    return parent ? 1 + depth(parent) : 0;
  };
  for (const org of recorded('org').sort((a, b) => depth(b.id) - depth(a.id))) {
    if (!orgs.some((o) => o.id === org.id)) {
      await attempt(org, async () => 'already gone');
      continue;
    }
    if (!deleteOrgs && isSeededOrg(state.prefix, org.name)) {
      kept.push(org.name);
      continue;
    }
    await attempt(org, async () => {
      const foreign: string[] = [];
      for (const child of orgs.filter((o) => o.parent_id === org.id && !deletedOrgs.has(o.id))) foreign.push(`child org ${child.slug}`);
      for (const user of await api.admin<User[]>('GET', `/orgs/${org.id}/users`)) foreign.push(`user ${user.email}`);
      for (const key of await api.admin<ApiKey[]>('GET', `/orgs/${org.id}/api-keys`)) foreign.push(`API key ${key.name}`);
      for (const sandbox of await api.admin<AdminSandbox[]>('GET', `/sandboxes?org_id=${org.id}&limit=${LIST_LIMIT}`)) {
        if (sandbox.org_id === org.id && sandbox.status !== 'deleted') foreign.push(`sandbox ${sandbox.name ?? sandbox.id} (${sandbox.status})`);
      }
      if (foreign.length) throw new Error(`refused: it still holds ${foreign.join(', ')}`);
      const result = await remove(`/orgs/${org.id}`);
      deletedOrgs.add(org.id);
      return result;
    });
  }

  state.created = state.created.filter((o) => !gone.has(o));
  const live = (kind: CreatedKind) => new Set(recorded(kind).map((o) => o.id));
  const users = live('user');
  const keys = live('apiKey');
  const sandboxes = live('sandbox');
  const orgIds = live('org');
  state.users = Object.fromEntries(Object.entries(state.users).filter(([, user]) => users.has(user.id)));
  state.apiKeys = Object.fromEntries(Object.entries(state.apiKeys).filter(([, id]) => keys.has(id)));
  state.sandboxes = Object.fromEntries(Object.entries(state.sandboxes).filter(([, id]) => sandboxes.has(id)));
  state.orgs = Object.fromEntries(Object.entries(state.orgs).filter(([key, id]) => key === 'managing' || orgIds.has(id)));
  const file = seedStatePath(config);
  if (!state.created.length) {
    fs.rmSync(file, { force: true });
    log('everything recorded is deleted');
  } else {
    writeSeedState(config, state);
  }
  if (kept.length) {
    log(`kept ${kept.length} seeded organization(s), which a seed reuses: ${kept.join(', ')}`);
    log('pass --delete-orgs to delete them; deleting an organization also deletes its credentials, keys and grants (see README.md)');
  }
  for (const failure of failures) log(`NOT deleted: ${failure}`);
  if (failures.length) {
    log(`${failures.length} object(s) not deleted; still recorded in ${file}`);
    log(`rerun \`npm run clean\`; if it keeps failing, ${resetAdvice(file)}`);
  }
  return failures.length ? 1 : 0;
}

async function main(): Promise<number> {
  const args = process.argv.slice(2);
  const config = loadConfig(args);
  const cleaning = flag(args, 'clean');
  const what = cleaning ? 'clean' : 'seed';
  label = what;
  guardTargets(config, what);
  const previous = readSeedState(config);
  if (cleaning && !previous) {
    log(`nothing to clean: no ${seedStatePath(config)}`);
    return 0;
  }
  const { api, identity } = await proveSameStack(config, previous?.stack ?? null, what, seedStatePath(config));
  if (cleaning) return clean(api, config, previous!, { deleteOrgs: flag(args, 'delete-orgs') });
  const template = option(args, 'template', process.env.CONSOLE_LOOP_TEMPLATE ?? 'seed:latest')!;
  try {
    const state = await new Seeder(api, config, template, previous, identity).run(flag(args, 'skip-sandboxes'));
    log(`seeded under "${state.prefix}"; state in ${seedStatePath(config)}`);
    for (const note of state.notes) log(`note: ${note}`);
  } catch (err) {
    throw new Error(`${describe(err)}\nrerun \`npm run seed\`, which reuses what exists; if it keeps failing, ${resetAdvice(seedStatePath(config))}`);
  }
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main().then((code) => process.exit(code), (err) => {
    console.error(err instanceof Error ? err.message : err);
    process.exit(2);
  });
}
