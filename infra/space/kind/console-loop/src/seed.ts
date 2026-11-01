// Seed synthetic state for the loop, or remove it (`--clean`).
//
//   npm run seed [-- --skip-sandboxes]
//   npm run clean [-- --delete-orgs]
//
// Everything goes through the public and admin APIs as the console account,
// only against this host unless --allow-remote is given, and only once the
// console and the API have been shown to be one stack (src/stack.ts). Every
// object's name starts with the prefix (default `loop`), and the state lives
// in <out>/seed-<prefix>.json. Each create is recorded there before it is
// submitted, by name and, for sandboxes, with its idempotency key, and its
// ID and operation are recorded as soon as the API answers, so a rerun after
// an interruption reuses what exists instead of duplicating it. Cleanup
// deletes only recorded objects.
import { randomBytes, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { Api, ApiError, TERMINAL_OPERATION_STATES, sleep, type Operation } from './api.ts';
import { flag, guardTargets, loadConfig, option, type Config } from './config.ts';
import { API_KEYS, ORGS, SANDBOXES, USERS, userEmail, verifyFixtures, type Get } from './fixtures.ts';
import { proveSameStack, type StackIdentity } from './stack.ts';

export type CreatedKind = 'org' | 'user' | 'apiKey' | 'sandbox';

export interface OperationRef {
  id: string;
  // Read from the admin operations route (else the public one).
  admin: boolean;
  action: string;
  // Set once the operation is seen finished, so later runs stop asking:
  // its terminal state, or `gone` when the API no longer has it (it deletes
  // finished operations after a week) and the outcome is unknown.
  outcome?: string;
  // The operation's target, when it was seen finished.
  targetId?: string;
}

// One object the seed or the loop created, or is about to. It is recorded,
// by kind and name, before its create is submitted; `id` is set once the API
// answers.
export interface CreatedObject {
  kind: CreatedKind;
  name: string;
  id?: string;
  // The org an API key belongs to; the org a sandbox or user was created in.
  orgId?: string;
  // Sandbox creates by the seed only: the key and request a rerun resubmits,
  // unchanged, which the API answers with the original operation when the
  // first submit arrived.
  idempotencyKey?: string;
  request?: { admin: boolean; body: unknown };
  // Sandbox creates by the loop: set as the browser's create request leaves,
  // before any answer. A submitted create with no answer and no sandbox of
  // its name is unresolved, and `clean` keeps it.
  submitted?: boolean;
  // Every create, action and delete operation submitted for it.
  operations?: OperationRef[];
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

// The state for --prefix, or null when there is none. A state recorded for
// another prefix, or without the stack it was seeded on, is refused.
export function readSeedState(config: Config): SeedState | null {
  const file = seedStatePath(config);
  if (!fs.existsSync(file)) return null;
  const state = JSON.parse(fs.readFileSync(file, 'utf8')) as SeedState;
  if (state.prefix !== config.prefix) {
    throw new Error(`refusing ${file}: it records prefix "${state.prefix}", not --prefix "${config.prefix}"`);
  }
  if (!state.stack?.clusterId) throw new Error(`refusing ${file}: it does not record the stack it was seeded on`);
  state.created ??= [];
  return state;
}

export function writeSeedState(config: Config, state: SeedState): void {
  fs.mkdirSync(config.outDir, { recursive: true });
  const file = seedStatePath(config);
  fs.writeFileSync(`${file}.tmp`, JSON.stringify(state, null, 2), { mode: 0o600 });
  fs.renameSync(`${file}.tmp`, file);
}

export function findCreated(state: SeedState, kind: CreatedKind, name: string): CreatedObject | undefined {
  return state.created.find((o) => o.kind === kind && o.name === name);
}

// Add an object to the state, or merge into the one of that kind and name:
// given fields replace recorded ones, operations accumulate.
export function upsertCreated(state: SeedState, object: CreatedObject): CreatedObject {
  const entry = findCreated(state, object.kind, object.name);
  if (!entry) {
    state.created.push(object);
    return object;
  }
  const { operations, ...fields } = object;
  for (const [field, value] of Object.entries(fields)) if (value !== undefined) (entry as unknown as Record<string, unknown>)[field] = value;
  for (const op of operations ?? []) addOperation(entry, op);
  return entry;
}

function addOperation(entry: CreatedObject, op: OperationRef): void {
  entry.operations ??= [];
  if (!entry.operations.some((o) => o.id === op.id)) entry.operations.push(op);
}

// Record an object a loop step is creating (before it submits, by name) or
// created (with its ID), so `npm run clean` can remove it if the step that
// created it did not get to delete it.
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

// The live sandboxes of one org whose name is `name` (with `deleting`, also
// those being deleted), or a loud failure when the org has too many
// sandboxes to tell.
async function findSandboxesByName(api: Api, orgId: string, name: string, { deleting = false } = {}): Promise<AdminSandbox[]> {
  const rows = await api.admin<AdminSandbox[]>('GET', `/sandboxes?org_id=${orgId}&limit=${LIST_LIMIT}`);
  const found = rows.filter((s) => s.name === name && (isLive(s) || (deleting && s.status === 'deleting')));
  if (!found.length && rows.filter((s) => s.resource_type === undefined || s.resource_type === 'sandbox').length >= LIST_LIMIT) {
    throw new Error(`org ${orgId} has at least ${LIST_LIMIT} sandboxes, so the admin list cannot show whether ${name} exists`);
  }
  return found;
}

// Read one operation; null when the API no longer has it.
export type ReadOperation = (op: OperationRef) => Promise<Operation | null>;

export const readOperation = (api: Api): ReadOperation => async (op) => {
  try {
    const path = `/operations/${op.id}`;
    return op.admin ? await api.admin<Operation>('GET', path) : await api.public<Operation>('GET', path);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
};

// Wait for every operation recorded for an object to finish, and record
// each outcome as soon as it is seen (`save` writes the state). An operation
// the API no longer has finished long ago, with an outcome nobody can read
// now: it is recorded as `gone`, and callers decide from the object itself.
export async function settleOperations(
  read: ReadOperation,
  entry: CreatedObject | undefined,
  save: () => void,
  { pollMs = 2000, timeoutMs = OPERATION_TIMEOUT_MS }: { pollMs?: number; timeoutMs?: number } = {},
): Promise<void> {
  for (const op of entry?.operations ?? []) {
    if (op.outcome) continue;
    const deadline = Date.now() + timeoutMs;
    let current = await read(op);
    if (current && !TERMINAL_OPERATION_STATES.includes(current.state)) log(`waiting for the ${op.action} operation ${op.id} of ${entry!.name} (${current.state})`);
    while (current && !TERMINAL_OPERATION_STATES.includes(current.state)) {
      if (Date.now() > deadline) throw new Error(`operation ${op.id} is still ${current.state}`);
      await sleep(pollMs);
      current = await read(op);
    }
    op.outcome = current ? current.state : 'gone';
    if (current?.target_id) op.targetId = current.target_id;
    save();
  }
}

// The sandbox a finished create made, as far as the state knows.
const createdTarget = (entry: CreatedObject | undefined): string | undefined =>
  entry?.operations?.find((op) => op.action === 'create' && op.outcome === 'succeeded' && op.targetId)?.targetId;

// What a sandbox create submits next. An unanswered create (a key and
// request recorded, no operation) is resubmitted unchanged, whatever the
// current options: the API refuses the same key with another body. Only a
// create that has resolved, or none, takes a fresh key and the current
// options.
export function nextCreate(
  entry: CreatedObject | undefined,
  fresh: () => { idempotencyKey: string; request: { admin: boolean; body: unknown } },
): { idempotencyKey: string; request: { admin: boolean; body: unknown }; replay: boolean } {
  const answered = entry?.operations?.some((o) => o.action === 'create');
  if (entry?.idempotencyKey && entry.request && !answered) return { idempotencyKey: entry.idempotencyKey, request: entry.request, replay: true };
  return { ...fresh(), replay: false };
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

  private record(object: CreatedObject): CreatedObject {
    const entry = upsertCreated(this.state, object);
    this.save();
    return entry;
  }

  save(): void {
    writeSeedState(this.config, this.state);
  }

  // Orgs, users and API keys have no idempotent create; their names (slug,
  // email, key name) are what a rerun and cleanup find them by, and each is
  // recorded before its create is submitted.
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
        this.record({ kind: 'org', name: slug });
        org = await this.api.admin<Org>('POST', '/orgs', { name: orgDisplayName(this.config.prefix, title), slug, parent_id: parentId });
        bySlug.set(slug, org);
        log(`created org ${slug}`);
      }
      // A reused org carries the prefix, so it is this seed's too.
      this.record({ kind: 'org', id: org.id, name: slug });
      this.state.orgs[key] = org.id;
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
      if (!existing) this.record({ kind: 'user', name: email, orgId });
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
        this.record({ kind: 'apiKey', name, orgId });
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
      await sleep(3000);
    }
    throw new Error(`${id} is ${status || 'unknown'}, not ${states.join(' or ')}`);
  }

  // A live sandbox named `name` in `org`: the recorded one, one found by
  // name, or a new one. A rerun first waits for the operations recorded for
  // it, then resubmits an unanswered create with its recorded key.
  private async create(name: string, org: string): Promise<string> {
    const orgId = this.state.orgs[org];
    const admin = org !== 'managing';
    const entry = findCreated(this.state, 'sandbox', name);
    await settleOperations(readOperation(this.api), entry, () => this.save());
    const target = createdTarget(entry);
    const recordedId = entry?.id ?? (target ? publicId(target) : undefined);
    if (recordedId) {
      const current = await getSandbox(this.api, recordedId);
      if (current && isLive(current)) return this.record({ kind: 'sandbox', id: recordedId, name, orgId }).id!;
    }
    const existing = await findSandboxesByName(this.api, orgId, name);
    if (existing.length > 1) this.note(`${existing.length} live sandboxes are named ${name}; using the first`);
    if (existing.length) return this.record({ kind: 'sandbox', id: publicId(existing[0].id), name, orgId }).id!;

    const { idempotencyKey, request, replay } = nextCreate(entry, () => {
      const sandbox = { template: this.template, runtime: 'fc', name };
      return { idempotencyKey: randomUUID(), request: { admin, body: admin ? { org_id: orgId, sandbox } : sandbox } };
    });
    if (replay) {
      log(`resubmitting the unanswered create of ${name} unchanged, with its recorded key`);
    } else {
      // Every earlier operation has finished and its sandbox is gone.
      const pending = this.record({ kind: 'sandbox', name, orgId, idempotencyKey, request });
      pending.id = undefined;
      pending.operations = [];
      this.save();
    }
    const accepted = await submitCreate(this.api, request, idempotencyKey);
    const surface = request.admin;
    this.record({
      kind: 'sandbox', name, orgId,
      id: accepted.sandbox_id ? publicId(accepted.sandbox_id) : undefined,
      operations: [{ id: accepted.operation_id, admin: surface, action: 'create' }],
    });
    const op = await this.api.waitForOperation(accepted.operation_id, { admin: surface, timeoutMs: OPERATION_TIMEOUT_MS });
    const id = publicId(accepted.sandbox_id ?? op.target_id ?? '');
    const entryNow = this.record({ kind: 'sandbox', id, name, orgId });
    const created = entryNow.operations!.find((o) => o.id === op.id)!;
    created.outcome = op.state;
    if (op.target_id) created.targetId = op.target_id;
    this.save();
    if (op.state !== 'succeeded') throw new Error(`create ${name}: operation ${op.state} ${op.progress_message ?? ''}`);
    log(`created sandbox ${name}`);
    return id;
  }

  private async act(id: string, name: string, action: string): Promise<void> {
    const accepted = await this.api.admin<{ operation_id: string }>('POST', `/sandboxes/${id}:force-${action}`, undefined, { idempotent: true, timeoutMs: 120_000 });
    this.record({ kind: 'sandbox', name, operations: [{ id: accepted.operation_id, admin: true, action }] });
    const op = await this.api.waitForOperation(accepted.operation_id, { admin: true, timeoutMs: OPERATION_TIMEOUT_MS });
    const recorded = findCreated(this.state, 'sandbox', name)!.operations!.find((o) => o.id === op.id)!;
    recorded.outcome = op.state;
    this.save();
    if (op.state !== 'succeeded') throw new Error(`${action} ${id}: operation ${op.state} ${op.progress_message ?? ''}`);
  }

  // One sandbox per resting state, one in a tenant org, and one deleted (its
  // operations outlive it). Any that cannot be set up fails the seed, after
  // the others are tried.
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
          await this.act(id, name, action);
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
    const entry = findCreated(this.state, 'sandbox', name);
    // A rerun after the delete was submitted: wait for it instead of
    // creating another.
    if (entry?.id && entry.operations?.some((o) => o.action === 'delete')) {
      await settleOperations(readOperation(this.api), entry, () => this.save());
      const current = await getSandbox(this.api, entry.id);
      if (!current || !isLive(current)) {
        this.state.sandboxes.deleted = entry.id;
        return;
      }
    }
    const id = await this.create(name, 'managing');
    await this.waitFor(id, ['started']);
    await this.act(id, name, 'delete');
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

function submitCreate(api: Api, request: { admin: boolean; body: unknown }, idempotencyKey: string): Promise<Accepted> {
  return request.admin
    ? api.admin<Accepted>('POST', '/sandboxes', request.body, { idempotencyKey, timeoutMs: 120_000 })
    : api.public<Accepted>('POST', '/sandboxes', request.body, { idempotencyKey, timeoutMs: 120_000 });
}

export function orgDisplayName(prefix: string, title: string): string {
  return `${title} (${prefix})`;
}

// Delete one recorded sandbox. Returns `deleted` once it is gone: every
// operation recorded for it has finished, and it is deleted or no longer
// found by ID. A 404 counts as gone only once nothing recorded for it is
// still running. Returns `never submitted` for a loop create whose request
// never left the browser, and `unresolved` for one that left with no answer
// and no sandbox of its name yet: a create can stay queued, without a
// sandbox, after the loop is gone.
async function cleanSandbox(api: Api, entry: CreatedObject, save: () => void): Promise<'deleted' | 'never submitted' | 'unresolved'> {
  await settleOperations(readOperation(api), entry, save);
  const ids = new Set<string>();
  if (entry.id) ids.add(publicId(entry.id));
  const target = createdTarget(entry);
  if (target) ids.add(publicId(target));
  if (!ids.size && entry.orgId) {
    for (const found of await findSandboxesByName(api, entry.orgId, entry.name, { deleting: true })) ids.add(publicId(found.id));
  }
  const answered = entry.operations?.some((o) => o.action === 'create');
  if (!ids.size && entry.idempotencyKey && entry.request && !answered) {
    // A seed create submitted but never answered. A create still queued has
    // no sandbox to find by name, so resubmit it, unchanged, with its key:
    // the API answers with the original operation if the first submit
    // arrived (or creates it now), and either way the sandbox is then
    // deleted.
    log(`resubmitting the unanswered create of ${entry.name} unchanged, with its key, to learn its operation`);
    const accepted = await submitCreate(api, entry.request, entry.idempotencyKey);
    addOperation(entry, { id: accepted.operation_id, admin: entry.request.admin, action: 'create' });
    if (accepted.sandbox_id) entry.id = publicId(accepted.sandbox_id);
    save();
    await settleOperations(readOperation(api), entry, save);
    const id = accepted.sandbox_id ?? createdTarget(entry);
    if (id) ids.add(publicId(id));
  }
  if (!ids.size) {
    if (answered) return 'deleted'; // its create finished, and nothing of it is left
    return entry.submitted ? 'unresolved' : 'never submitted';
  }
  for (const id of ids) {
    let current = await getSandbox(api, id);
    if (current && current.status !== 'deleted' && current.status !== 'deleting') {
      const accepted = await api.admin<{ operation_id: string }>('POST', `/sandboxes/${id}:force-delete`, undefined, { idempotent: true });
      addOperation(entry, { id: accepted.operation_id, admin: true, action: 'delete' });
      save();
      const op = await api.waitForOperation(accepted.operation_id, { admin: true, timeoutMs: OPERATION_TIMEOUT_MS });
      const recorded = entry.operations!.find((o) => o.id === op.id)!;
      recorded.outcome = op.state;
      save();
      current = await getSandbox(api, id);
      // A delete that fails because another delete got there first is fine
      // once the sandbox is going.
      if (op.state !== 'succeeded' && current && current.status !== 'deleted' && current.status !== 'deleting') {
        throw new Error(`delete operation ${op.state}${op.progress_message ? `: ${op.progress_message}` : ''}`);
      }
    }
    const deadline = Date.now() + OPERATION_TIMEOUT_MS;
    if (current?.status === 'deleting') log(`waiting for ${entry.name} (${id}) to finish deleting`);
    while (current?.status === 'deleting') {
      if (Date.now() > deadline) throw new Error(`${id} is still deleting`);
      await sleep(3000);
      current = await getSandbox(api, id);
    }
    if (current && current.status !== 'deleted') throw new Error(`${id} is ${current.status} after its delete`);
  }
  return 'deleted';
}

// Delete what the state recorded: sandboxes, then users and API keys, then
// the orgs loop runs created. The seeded orgs are kept, since a seed reuses
// them by slug, unless `deleteOrgs` is set. Orgs are deleted children first,
// and each is refused while it holds a child org, a user, an API key, or a
// sandbox that is not deleted. Its credentials, credential references,
// master keys, template grants and other cascaded rows are not checked:
// they go with the org (see README.md). An unresolved sandbox create is
// kept, and reported, unless `forgetUnresolved` is set.
async function clean(api: Api, config: Config, state: SeedState, { deleteOrgs, forgetUnresolved }: { deleteOrgs: boolean; forgetUnresolved: boolean }): Promise<number> {
  const failures: string[] = [];
  const unresolved: string[] = [];
  const gone = new Set<CreatedObject>();
  const save = () => writeSeedState(config, state);
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

  for (const sandbox of recorded('sandbox')) {
    try {
      const result = await cleanSandbox(api, sandbox, save);
      if (result === 'unresolved' && !forgetUnresolved) {
        unresolved.push(sandbox.name);
        log(`unresolved: sandbox ${sandbox.name}: its create left the browser with no answer, and no sandbox has its name yet`);
        continue;
      }
      log(`${result === 'unresolved' ? 'forgotten (--forget-unresolved)' : result}: sandbox ${sandbox.name}`);
      gone.add(sandbox);
    } catch (err) {
      failures.push(`sandbox ${sandbox.name}: ${describe(err)}`);
    }
  }
  for (const user of recorded('user')) {
    await attempt(user, async () => {
      const ids = user.id ? [user.id]
        : (await api.admin<User[]>('GET', `/orgs/${user.orgId}/users`)).filter((u) => u.email === user.name).map((u) => u.id);
      if (!ids.length) return 'never created';
      for (const id of ids) await remove(`/users/${id}`);
      return 'deleted';
    });
  }
  for (const key of recorded('apiKey')) {
    await attempt(key, async () => {
      const ids = key.id ? [key.id]
        : (await api.admin<ApiKey[]>('GET', `/orgs/${key.orgId}/api-keys`)).filter((k) => k.name === key.name).map((k) => k.id);
      if (!ids.length) return 'never created';
      for (const id of ids) await remove(`/orgs/${key.orgId}/api-keys/${id}`);
      return 'deleted';
    });
  }

  const orgs = await api.admin<Org[]>('GET', '/orgs');
  const ours = recorded('org').map((org) => ({ org, id: org.id ?? orgs.find((o) => o.slug === org.name)?.id }));
  const deletedOrgs = new Set<string>();
  const kept: string[] = [];
  const depth = (id: string | undefined): number => {
    const parent = orgs.find((o) => o.id === id)?.parent_id;
    return parent ? 1 + depth(parent) : 0;
  };
  for (const { org, id } of ours.sort((a, b) => depth(b.id) - depth(a.id))) {
    if (!id || !orgs.some((o) => o.id === id)) {
      await attempt(org, async () => (id ? 'already gone' : 'never created'));
      continue;
    }
    if (!deleteOrgs && isSeededOrg(state.prefix, org.name)) {
      kept.push(org.name);
      continue;
    }
    await attempt(org, async () => {
      const foreign: string[] = [];
      for (const child of orgs.filter((o) => o.parent_id === id && !deletedOrgs.has(o.id))) foreign.push(`child org ${child.slug}`);
      for (const user of await api.admin<User[]>('GET', `/orgs/${id}/users`)) foreign.push(`user ${user.email}`);
      for (const key of await api.admin<ApiKey[]>('GET', `/orgs/${id}/api-keys`)) foreign.push(`API key ${key.name}`);
      for (const sandbox of await api.admin<AdminSandbox[]>('GET', `/sandboxes?org_id=${id}&limit=${LIST_LIMIT}`)) {
        if (sandbox.org_id === id && sandbox.status !== 'deleted') foreign.push(`sandbox ${sandbox.name ?? sandbox.id} (${sandbox.status})`);
      }
      if (foreign.length) throw new Error(`refused: it still holds ${foreign.join(', ')}`);
      const result = await remove(`/orgs/${id}`);
      deletedOrgs.add(id);
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
  if (!state.created.length) {
    fs.rmSync(seedStatePath(config), { force: true });
    log('everything recorded is deleted');
  } else {
    save();
  }
  if (kept.length) {
    log(`kept ${kept.length} seeded organization(s), which a seed reuses: ${kept.join(', ')}`);
    log('pass --delete-orgs to delete them; deleting an organization also deletes its credentials, keys and grants (see README.md)');
  }
  for (const failure of failures) log(`NOT deleted: ${failure}`);
  if (failures.length) log(`${failures.length} object(s) not deleted; still recorded in ${seedStatePath(config)}`);
  if (unresolved.length) {
    log(`${unresolved.length} unresolved sandbox create(s), still recorded: ${unresolved.join(', ')}`);
    log('a later clean looks them up by name again; pass --forget-unresolved to drop them');
  }
  return failures.length || unresolved.length ? 1 : 0;
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
  if (cleaning) return clean(api, config, previous!, { deleteOrgs: flag(args, 'delete-orgs'), forgetUnresolved: flag(args, 'forget-unresolved') });
  const template = option(args, 'template', process.env.CONSOLE_LOOP_TEMPLATE ?? 'seed:latest')!;
  const state = await new Seeder(api, config, template, previous, identity).run(flag(args, 'skip-sandboxes'));
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
