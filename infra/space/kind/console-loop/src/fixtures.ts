// The fixtures the seed creates, and the check that every one of them exists.
// The seeder runs the check before it reports success, and the loop's
// `fixtures` step runs it again through the console.
import type { SeedState } from './seed.ts';

// [key, display title, parent key]
export const ORGS = [
  ['acme', 'Acme', null],
  ['globex', 'Globex', null],
  ['globex-research', 'Globex Research', 'globex'],
] as const;

// [org key, local part, display name, role]
export const USERS = [
  ['managing', 'ops-admin', 'Ops Admin', 'organization_admin'],
  ['managing', 'ops-viewer', 'Ops Viewer', 'member'],
  ['acme', 'acme-admin', 'Acme Admin', 'organization_admin'],
  ['acme', 'acme-dev', 'Acme Developer', 'member'],
  ['globex', 'globex-dev', 'Globex Developer', 'member'],
  ['globex-research', 'research-dev', 'Research Developer', 'member'],
] as const;

// [key, allowed surfaces]; all in the managing organization.
export const API_KEYS = [
  ['native', ['native']],
  ['ecw', ['ecw']],
] as const;

// [key, org key, action that brings it to rest, resting status]
export const SANDBOXES = [
  ['running', 'managing', null, 'started'],
  ['paused', 'managing', 'pause', 'paused'],
  ['suspended', 'managing', 'suspend', 'suspended'],
  ['stopped', 'managing', 'stop', 'stopped'],
  ['acme-running', 'acme', null, 'started'],
] as const;

export const userEmail = (prefix: string, local: string) => `${prefix}-${local}@example.test`;

// An admin GET: the API client in the seeder, the page's session in the loop.
export type Get = (path: string) => Promise<{ status: number; json: any }>;

export interface FixtureReport {
  ok: string[];
  problems: string[];
}

// Every fixture the state must hold, looked up by ID. Sandboxes are required
// unless the seed ran with --skip-sandboxes. With `statuses` (the seed), each
// sandbox must also be in its resting status and the deleted one must be
// gone; the loop only notes the statuses, since a stack may move a sandbox on
// later, and leaves the deleted one to its own step, whose 404 it would
// otherwise record.
export async function verifyFixtures(state: SeedState, get: Get, { statuses }: { statuses: boolean }): Promise<FixtureReport> {
  const report: FixtureReport = { ok: [], problems: [] };
  const expect = (ok: boolean, what: string) => (ok ? report.ok : report.problems).push(what);

  for (const key of ['managing', ...ORGS.map(([key]) => key)]) {
    const id = state.orgs[key];
    expect(!!id && (await get(`/admin/api/v1/orgs/${id}`)).status === 200, `org ${key} exists`);
  }
  for (const [, local] of USERS) {
    const address = userEmail(state.prefix, local);
    const user = state.users[address];
    const found = user ? await get(`/admin/api/v1/users/${user.id}`) : null;
    expect(found?.status === 200 && found.json?.email === address, `user ${address} exists`);
  }
  // The org's key list is not paginated: one request shows every key.
  const keys = await get(`/admin/api/v1/orgs/${state.managingOrgId}/api-keys`);
  const keyIds = new Set<string>(keys.status === 200 ? (keys.json ?? []).map((key: { id: string }) => key.id) : []);
  for (const [key] of API_KEYS) {
    const id = state.apiKeys[key];
    expect(!!id && keyIds.has(id), `API key ${key} exists`);
  }
  if (!state.withSandboxes) {
    report.ok.push('sandboxes not seeded (--skip-sandboxes)');
    return report;
  }
  for (const [key, , , resting] of SANDBOXES) {
    const id = state.sandboxes[key];
    const found = id ? await get(`/admin/api/v1/sandboxes/${id}`) : null;
    const status = found?.json?.status ?? null;
    if (found?.status !== 200) expect(false, `sandbox ${key} exists`);
    else if (statuses) expect(status === resting, `sandbox ${key} is ${resting} (${status})`);
    else expect(true, `sandbox ${key} exists (${status})`);
  }
  const deleted = state.sandboxes.deleted;
  if (!statuses) {
    expect(!!deleted, 'sandbox deleted is recorded');
    return report;
  }
  const gone = deleted ? await get(`/admin/api/v1/sandboxes/${deleted}`) : null;
  expect(!!gone && (gone.status === 404 || ['deleted', 'deleting'].includes(gone.json?.status)), 'sandbox deleted is deleted');
  return report;
}
