// The loop's steps. Each visits part of the console as an operator would and
// checks what an operator relies on; the runner records everything else
// (errors, failed and slow requests, screenshots). Steps that create or
// change something are marked `mutates` and clean up after themselves.
import type { Page, Request, Response, Route } from 'playwright';

import {
  SECRET_PLACEHOLDER, adminFetch, carriesSecret, confirmPopover, content, fetchWithoutRedirect, pickSelect, settle, shortLabel,
  signIn, text, visit, waitForStatus, withoutSecret, type Ctx,
} from './browser.ts';
import { TERMINAL_OPERATION_STATES } from './api.ts';
import { verifyFixtures } from './fixtures.ts';
import { orgDisplayName, recordCreated } from './seed.ts';

export interface Step {
  name: string;
  // --only and --skip select steps by exact name or tag.
  tags: string[];
  mutates: boolean;
  run: (c: Ctx) => Promise<void>;
}

export const steps: Step[] = [];
const step = (name: string, tags: string[], run: Step['run'], mutates = false) => steps.push({ name, tags, run, mutates });

const seeded = (c: Ctx) => {
  if (!c.seed) throw new Error('no seed state: run `npm run seed` first, with the same --out and --prefix');
  return c.seed;
};
// The response to the next POST to `pathname` on any origin, or null; start
// waiting before the click that sends it.
const nextPost = (page: Page, pathname: (path: string) => boolean): Promise<Response | null> =>
  page.waitForResponse((res) => res.request().method() === 'POST' && pathname(new URL(res.url()).pathname), { timeout: 30_000 }).catch(() => null);
const jsonOf = async (res: Response | null): Promise<any> => (res?.ok() ? res.json().catch(() => null) : null);
const KEY_SHAPED = /\b[0-9a-f]{64}\b/i;
const sbxId = (id: string) => (id.startsWith('sbx_') ? id : `sbx_${id}`);

const orgId = (c: Ctx, key: string) => seeded(c).orgs[key];
const sandboxId = (c: Ctx, key: string) => seeded(c).sandboxes[key];
const email = (c: Ctx, local: string) => `${c.config.prefix}-${local}@example.test`;
// Seeded org names carry the prefix, so two prefixes on one stack never mix.
const orgName = (c: Ctx, title: string) => orgDisplayName(c.config.prefix, title);

// --------------------------------------------------------------- sign-in
step('login-page', ['auth'], async (c) => {
  await visit(c, '/login');
  c.rec.check(await c.page.getByLabel('Email').isVisible(), 'the login form renders');
});

step('login', ['auth'], async (c) => {
  await c.page.getByLabel('Email').fill(c.config.email);
  await c.page.getByLabel('Password').fill(c.config.password);
  await c.page.getByRole('button', { name: /sign in|log in|login/i }).click();
  await c.page.waitForURL(/\/dashboard/, { timeout: 20_000 });
  await settle(c.page);
  c.rec.check(/\/dashboard/.test(c.page.url()), 'signing in lands on the dashboard');
});

// Every fixture the seed creates must exist, looked up by ID through the
// console, or the steps that use it would fail for the wrong reason (or pass
// on a list that misses it).
step('fixtures', ['auth'], async (c) => {
  if (!c.seed) return c.rec.check(false, 'seed state exists (run `npm run seed` with the same --out and --prefix)');
  const report = await verifyFixtures(c.seed, (apiPath) => adminFetch(c.page, 'GET', apiPath), { statuses: false });
  for (const ok of report.ok) c.rec.check(true, `seeded ${ok}`);
  for (const problem of report.problems) c.rec.check(false, `seeded ${problem}`);
});

// ------------------------------------------------------------ every route
for (const [route, label] of [
  ['/dashboard', 'Overview'],
  ['/dashboard/orgs', 'Organizations'],
  ['/dashboard/users', 'Users'],
  ['/dashboard/api-keys', 'API Keys'],
  ['/dashboard/sandboxes', 'Sandboxes'],
  ['/dashboard/credential-rules', 'Credential Rules'],
  ['/dashboard/templates', 'Templates'],
  ['/dashboard/nodes', 'Nodes'],
] as const) {
  step(`route-${route.split('/').pop()}`, ['routes'], async (c) => {
    await visit(c, route, 2500);
    c.rec.check((await c.page.locator('.ant-menu').innerText()).includes(label), `the menu lists ${label}`);
    c.rec.check((await text(c.page)).length > 0, `${route} renders content`);
  });
}

// ----------------------------------------------------------- organizations
step('orgs-list', ['orgs'], async (c) => {
  await visit(c, '/dashboard/orgs');
  const t = await text(c.page);
  for (const title of ['Acme', 'Globex', 'Globex Research']) {
    c.rec.check(t.includes(orgName(c, title)), `the organizations list shows ${orgName(c, title)}`);
  }
});

for (const key of ['managing', 'acme', 'globex', 'globex-research']) {
  step(`org-detail-${key}`, ['orgs'], async (c) => {
    await visit(c, `/dashboard/orgs/${orgId(c, key)}`, 2500);
    c.rec.check((await text(c.page)).includes('Users'), 'the organization page shows its users');
  });
}

step('org-detail-unknown', ['orgs'], async (c) => {
  await visit(c, '/dashboard/orgs/00000000-0000-0000-0000-00000000dead', 2500);
  c.rec.check(/not found|error|could not/i.test(await text(c.page)), 'an unknown organization shows a not-found or error state');
});

step('org-create-delete', ['orgs', 'flows'], async (c) => {
  await visit(c, '/dashboard/orgs');
  await c.page.getByRole('button', { name: 'Create organization' }).click();
  const modal = c.page.locator('.ant-modal:visible');
  await modal.getByPlaceholder('Name').fill(`${c.runTag} org`);
  await modal.getByPlaceholder('Slug').fill(c.runTag);
  const response = nextPost(c.page, (p) => p === '/admin/api/v1/orgs');
  await modal.getByRole('button', { name: 'Create organization' }).click();
  const created = await jsonOf(await response);
  if (created?.id) recordCreated(c.config, { kind: 'org', id: created.id, name: c.runTag });
  await settle(c.page);
  const row = c.page.locator('tr', { hasText: c.runTag });
  c.rec.check(await row.count() > 0, 'a created organization appears in the list');
  if (!(await row.count())) return;
  await row.locator('button.ant-btn-dangerous').click();
  await confirmPopover(c.page);
  await settle(c.page);
  c.rec.check(await c.page.locator('tr', { hasText: c.runTag }).count() === 0, 'a deleted organization disappears');
}, true);

// ------------------------------------------------------------------- users
step('users-list', ['users'], async (c) => {
  await visit(c, '/dashboard/users');
  c.rec.check((await text(c.page)).includes(c.config.email), "the users list opens on the signed-in admin's organization");
});

step('users-switch-org', ['users'], async (c) => {
  await visit(c, '/dashboard/users');
  await pickSelect(c.page, content(c.page).locator('.ant-select').first(), orgName(c, 'Acme'));
  await settle(c.page);
  c.rec.check((await text(c.page)).includes(email(c, 'acme-dev')), "switching organization lists that organization's users");
});

for (const local of ['ops-admin', 'acme-dev']) {
  step(`user-detail-${local}`, ['users'], async (c) => {
    const user = seeded(c).users[email(c, local)];
    await visit(c, `/dashboard/users/${user.id}`);
    c.rec.check((await text(c.page)).includes(email(c, local)), 'the user page shows the user');
  });
}

step('user-create-role-delete', ['users', 'flows'], async (c) => {
  await visit(c, '/dashboard/users');
  await pickSelect(c.page, content(c.page).locator('.ant-select').first(), orgName(c, 'Acme'));
  await settle(c.page);
  const address = `${c.runTag}@example.test`;
  await c.page.getByPlaceholder('Email').fill(address);
  await c.page.getByPlaceholder('Password').fill(seeded(c).userPassword);
  await c.page.getByPlaceholder('Display Name').fill('Loop Temporary');
  const response = nextPost(c.page, (p) => p === `/admin/api/v1/orgs/${orgId(c, 'acme')}/users`);
  await c.page.getByRole('button', { name: 'Create' }).click();
  const listed = await jsonOf(await response);
  if (listed?.id) recordCreated(c.config, { kind: 'user', id: listed.id, name: address, orgId: orgId(c, 'acme') });
  await settle(c.page);
  const row = c.page.locator('tr', { hasText: address });
  c.rec.check(await row.count() > 0, 'a created user appears in the list');
  if (!(await row.count())) return;
  await pickSelect(c.page, row.locator('.ant-select'), 'Organization Admin');
  const confirm = c.page.locator('.ant-modal-confirm:visible');
  if (await confirm.waitFor({ timeout: 2000 }).then(() => true, () => false)) {
    await confirm.getByRole('button', { name: /Change role|OK/ }).click();
  }
  await settle(c.page);
  const users = await adminFetch(c.page, 'GET', `/admin/api/v1/orgs/${orgId(c, 'acme')}/users`);
  const created = (users.json ?? []).find((u: { email: string }) => u.email === address);
  c.rec.check(created?.role === 'organization_admin', `the role change persists (${created?.role})`);
  await row.locator('button.ant-btn-dangerous').click();
  await confirmPopover(c.page);
  await settle(c.page);
  c.rec.check(await c.page.locator('tr', { hasText: address }).count() === 0, 'a deleted user disappears');
}, true);

// ---------------------------------------------------------------- API keys
step('api-keys-list', ['api-keys'], async (c) => {
  await visit(c, '/dashboard/api-keys');
  c.rec.check((await text(c.page)).includes(`${c.config.prefix}-native`), 'the API keys list shows the seeded key');
});

// The API answers a create with the new key's secret, which the page shows
// once. For this step every POST goes through `page.route`: its answer is
// fetched without following a redirect, any `secret` in it is replaced by a
// placeholder before the page sees it, and a create (a POST whose path ends
// in /api-keys) has its key's ID recorded first. It fails closed: when a
// JSON answer with a `secret` reaches the page some other way, or the dialog
// opens before the route answered the create, the page is cleared before
// any screenshot, the step fails, and the loop deletes a --video recording.
step('api-key-create-revoke', ['api-keys', 'flows'], async (c) => {
  const managing = seeded(c).managingOrgId;
  const routed = new Set<Request>();
  const reads: Promise<void>[] = [];
  const leaks: string[] = [];
  let created: { id?: string; replaced: boolean; refused?: boolean } | null = null;
  const handler = async (route: Route) => {
    const req = route.request();
    if (req.method() !== 'POST') return route.fallback();
    routed.add(req);
    const isCreate = new URL(req.url()).pathname.endsWith('/api-keys');
    const response = await fetchWithoutRedirect(route);
    if (!response) {
      if (isCreate) created = { replaced: false, refused: true };
      return;
    }
    const safe = withoutSecret(await response.text());
    if (isCreate) {
      if (safe.id) recordCreated(c.config, { kind: 'apiKey', id: safe.id, name: c.runTag, orgId: managing });
      created = { id: safe.id, replaced: safe.replaced };
    }
    const headers = { ...response.headers() };
    delete headers['content-length'];
    await route.fulfill({ status: response.status(), headers, body: safe.body });
  };
  const onResponse = (res: Response) => {
    if (routed.has(res.request()) || !(res.headers()['content-type'] ?? '').includes('json')) return;
    reads.push(res.text().then((body) => {
      if (carriesSecret(body)) leaks.push(`${res.request().method()} ${new URL(res.url()).pathname}`);
    }, () => {}));
  };
  // The flag comes first: the runner stops the run on it and deletes a
  // --video recording, even if leaving the page fails.
  const exposed = async (why: string): Promise<never> => {
    c.shared.secretExposed = why;
    await c.page.goto('about:blank').catch(() => {});
    throw new Error(`a live API key may have reached the page (${why}); the page was cleared before any screenshot`);
  };
  await c.page.route('**/*', handler);
  c.page.on('response', onResponse);
  try {
    await visit(c, '/dashboard/api-keys');
    await c.page.getByRole('button', { name: 'Create API key' }).click();
    const modal = c.page.locator('.ant-modal:visible');
    await modal.locator('input').first().fill(c.runTag);
    await modal.getByRole('button', { name: 'Create' }).click();
    const dialog = c.page.locator('.ant-modal:visible', { hasText: 'Copy this key now' });
    const opened = await dialog.waitFor({ timeout: 30_000 }).then(() => true, () => false);
    const answer = created as { id?: string; replaced: boolean; refused?: boolean } | null;
    if (opened && !answer) await exposed('the dialog opened before the route answered the create');
    await Promise.allSettled(reads);
    if (leaks.length) await exposed(`an answer with a secret did not pass the route: ${leaks.join(', ')}`);
    if (answer?.refused) return c.rec.check(false, 'the create is answered without a redirect');
    c.rec.check(!!answer?.id, "the create response carries the new key's ID");
    c.rec.check(!!answer?.replaced, "the create response's secret is replaced before the page sees it");
    if (!opened) return c.rec.check(false, 'the dialog with the new key opens');
    const shown = await dialog.innerText();
    if (KEY_SHAPED.test(shown)) await exposed('the dialog shows something shaped like a key');
    c.rec.check(shown.includes(SECRET_PLACEHOLDER), 'the dialog shows the placeholder, not a key');
    await c.page.waitForTimeout(800);
    await c.page.screenshot({ path: `${c.rec.outDir}/api-key-dialog-placeholder.png` });
    await c.page.getByRole('button', { name: 'Done' }).click();
    const row = c.page.locator('tr', { hasText: c.runTag });
    c.rec.check(await row.count() > 0, 'a created key appears in the list');
    if (!(await row.count())) return;
    await row.getByRole('button', { name: `Revoke ${c.runTag}` }).click();
    await confirmPopover(c.page, /(^|\s)Revoke$/);
    await settle(c.page);
    c.rec.check(await c.page.locator('tr', { hasText: c.runTag }).count() === 0, 'a revoked key disappears');
  } finally {
    c.page.off('response', onResponse);
    await c.page.unroute('**/*', handler);
  }
}, true);

// --------------------------------------------------------------- sandboxes
step('sandboxes-list', ['sandboxes'], async (c) => {
  await visit(c, '/dashboard/sandboxes?age=all', 3000);
  const rows = await content(c.page).locator('tbody tr.ant-table-row').count();
  c.rec.check(rows > 0, `the sandbox list shows sandboxes (${rows} rows)`);
});

step('sandboxes-search-name', ['sandboxes'], async (c) => {
  await visit(c, '/dashboard/sandboxes?age=all', 2000);
  await content(c.page).locator('input[placeholder*="ID"]').first().fill(`${c.config.prefix}-suspended`);
  await c.page.waitForTimeout(800);
  const rows = content(c.page).locator('tbody tr.ant-table-row');
  c.rec.check(await rows.count() === 1, `searching a seeded name finds one row (${await rows.count()})`);
});

step('sandboxes-search-id', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'running');
  await visit(c, '/dashboard/sandboxes?age=all', 2000);
  await content(c.page).locator('input[placeholder*="ID"]').first().fill(id);
  await c.page.waitForTimeout(1500);
  c.rec.check(await content(c.page).locator('tbody tr.ant-table-row', { hasText: shortLabel(id) }).count() === 1, 'searching a full sbx_ ID finds the sandbox');
});

// The search box's text goes to the server's `search` (a name prefix
// ignoring case, or an ID suffix such as the ID column's label), so a match
// beyond the loaded page is found. An API without `search` refuses it with a
// 400 and the console searches as before; the step notes which it saw and
// checks the server's matches only when the server answered.
const searchAnsweredByServer = async (c: Ctx, typed: string): Promise<boolean> => {
  const answered = c.page.waitForResponse((res) => {
    const url = new URL(res.url());
    return url.pathname.endsWith('/admin/api/v1/sandboxes') && url.searchParams.get('search') === typed;
  }, { timeout: 5000 }).then((res) => res.ok(), () => false);
  await content(c.page).locator('input[placeholder*="ID"]').first().fill(typed);
  const server = await answered;
  await c.page.waitForTimeout(1500);
  c.rec.note(server ? `the server answered "${typed}" as search` : `"${typed}" was not answered as search (an API without it); the console searched as before`);
  return server;
};

step('sandboxes-search-label', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'running');
  if (!id) return c.rec.check(false, 'the seed has a running sandbox');
  await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
  const label = shortLabel(id);
  if (await searchAnsweredByServer(c, label)) {
    const rows = content(c.page).locator('tbody tr.ant-table-row');
    c.rec.check(await rows.filter({ hasText: label }).count() === 1, "searching the ID column's label finds the sandbox through the server");
  }
});

step('sandboxes-search-name-case', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'suspended');
  if (!id) return c.rec.check(false, 'the seed has a suspended sandbox');
  await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
  const name = `${c.config.prefix}-suspended`;
  if (await searchAnsweredByServer(c, name.toUpperCase())) {
    const rows = content(c.page).locator('tbody tr.ant-table-row');
    c.rec.check(await rows.count() === 1 && await rows.filter({ hasText: shortLabel(id) }).count() === 1, `searching ${name.toUpperCase()} finds ${name} alone`);
  }
});

step('sandboxes-user-email', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'acme-running');
  if (!id) return c.rec.check(false, 'the seed has an acme-running sandbox');
  const api = await adminFetch(c.page, 'GET', `/admin/api/v1/sandboxes/${id}`);
  const userEmail = api.json?.user_email;
  if (typeof userEmail !== 'string') return c.rec.note('the API reports no user_email; the User column shows the user ID');
  // The org's own list, so the sandbox is on its first page.
  await visit(c, `/dashboard/sandboxes?age=all&page_size=25&org=${orgId(c, 'acme')}`, 2000);
  const row = content(c.page).locator('tbody tr.ant-table-row', { hasText: shortLabel(id) });
  c.rec.check((await row.innerText()).includes(userEmail), "the User column shows the owner's email");
});

step('sandboxes-create-form', ['sandboxes'], async (c) => {
  await visit(c, '/dashboard/sandboxes', 2000);
  const card = c.page.locator('.ant-card', { hasText: 'Create sandbox' });
  const t = await card.innerText();
  c.rec.check(!/gVisor|Cloud Hypervisor|Hyper-V/.test(t), 'the create form offers no runtimes SPACE does not implement');
});

// The Datadog logs button shows only where SPACE_UI_DATADOG_LOGS_URL is set,
// which the loop cannot read; it checks what a shown button links to.
step('sandbox-detail-datadog', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'running');
  if (!id) return c.rec.check(false, 'the seed has a running sandbox');
  await visit(c, `/dashboard/sandboxes/${id}`, 3000);
  const logs = c.page.getByRole('link', { name: 'Datadog logs' });
  if (await logs.count() === 0) return c.rec.note('no Datadog logs button (SPACE_UI_DATADOG_LOGS_URL unset)');
  const href = new URL((await logs.first().getAttribute('href')) ?? '', c.config.baseUrl);
  c.rec.check(href.protocol === 'https:' && (href.searchParams.get('query') ?? '').includes(id.replace(/^sbx_/, '')), 'the Datadog logs button queries the sandbox on an HTTPS URL');
});

for (const key of ['running', 'paused', 'suspended', 'stopped', 'acme-running']) {
  step(`sandbox-detail-${key}`, ['sandboxes'], async (c) => {
    const id = sandboxId(c, key);
    if (!id) return c.rec.check(false, `the seed has a ${key} sandbox`);
    await visit(c, `/dashboard/sandboxes/${id}`, 3000);
    const api = await adminFetch(c.page, 'GET', `/admin/api/v1/sandboxes/${id}`);
    c.rec.check(!/Sandbox not found/.test(await text(c.page)), 'the sandbox page loads');
    c.rec.check(!api.json?.status || (await text(c.page)).includes(api.json.status), `the page shows the API's status (${api.json?.status})`);
  });
}

step('sandbox-detail-deleted', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'deleted');
  if (!id) return c.rec.note('the seed has no deleted sandbox');
  await visit(c, `/dashboard/sandboxes/${id}`, 3000);
  c.rec.check(/not found|deleted|could not/i.test(await text(c.page)), 'a deleted sandbox shows a not-found state');
});

step('sandbox-create', ['sandboxes', 'flows', 'lifecycle'], async (c) => {
  await visit(c, '/dashboard/sandboxes', 3000);
  const card = c.page.locator('.ant-card', { hasText: 'Create sandbox' });
  await card.getByPlaceholder('Name').fill(c.runTag);
  const managing = seeded(c).managingOrgId;
  const response = nextPost(c.page, (p) => p.endsWith('/v1/sandboxes'));
  await card.locator('button[type=submit]').click();
  const answer = await response;
  const accepted = await jsonOf(answer);
  if (typeof accepted?.sandbox_id === 'string') {
    c.shared.sandbox = sbxId(accepted.sandbox_id);
    // The answer can come before the sandbox exists; `clean` waits on the
    // create operation for one it cannot find yet.
    const createOperation = typeof accepted.operation_id === 'string'
      ? { id: accepted.operation_id, admin: new URL(answer!.url()).pathname.startsWith('/admin/') }
      : undefined;
    recordCreated(c.config, { kind: 'sandbox', id: c.shared.sandbox, name: c.runTag, orgId: managing, createOperation });
  }
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline && !c.shared.sandbox) {
    await c.page.waitForTimeout(3000);
    const list = await adminFetch(c.page, 'GET', '/admin/api/v1/sandboxes?limit=5000');
    const row = (list.json ?? []).find((s: { name: string | null }) => s.name === c.runTag);
    if (row) {
      c.shared.sandbox = sbxId(row.id);
      recordCreated(c.config, { kind: 'sandbox', id: c.shared.sandbox, name: c.runTag, orgId: managing });
    }
  }
  c.rec.check(!!c.shared.sandbox, 'creating a sandbox from the form produces one');
}, true);

// A checkpoint stays on the sandbox's node, not in the snapshot catalog: the
// card says so, and lists it under "On the node" only while periodic
// snapshots are on (the node reports its snapshots only then). Runs on the
// sandbox `sandbox-create` made, and turns periodic snapshots off again.
step('sandbox-checkpoint-snapshots', ['sandboxes', 'flows', 'lifecycle'], async (c) => {
  const id = c.shared.sandbox;
  if (!id) return c.rec.check(false, 'a sandbox was created by sandbox-create');
  await visit(c, `/dashboard/sandboxes/${id}`, 2000);
  await waitForStatus(c, id, ['started', 'running'], 120_000);
  // Every read below is bounded by its poll's deadline, so a stalled one
  // cannot hold the step (and its `finally`) past it.
  const read = (method: string, path: string, deadline: number) =>
    adminFetch(c.page, method, path, undefined, { timeoutMs: Math.min(15_000, deadline - Date.now()) });
  const operation = async (action: string) => {
    const deadline = Date.now() + 120_000;
    const accepted = await read('POST', `/admin/api/v1/sandboxes/${id}:${action}`, deadline);
    const opId = accepted.json?.operation_id;
    if (accepted.status !== 202 || typeof opId !== 'string') return `${accepted.status}`;
    let state = 'unfinished';
    while (Date.now() < deadline) {
      state = (await read('GET', `/admin/api/v1/operations/${opId}`, deadline)).json?.state ?? state;
      if (TERMINAL_OPERATION_STATES.includes(state)) return state;
      await c.page.waitForTimeout(2000);
    }
    return `unfinished (${state})`;
  };
  // What follows reads the node against each operation, so one that does not
  // succeed is recorded and ends the step; the `finally` below still turns
  // periodic snapshots off.
  const succeeds = async (action: string, what: string) => {
    const state = await operation(action);
    c.rec.check(state === 'succeeded', `${what} (${state})`);
    if (state !== 'succeeded') throw new Error(`${what}: the operation ended ${state}, so the step stops`);
  };
  // The node's observed periodic state, which decides what On the node can
  // say; the card is read only once it is the one wanted.
  const observedPeriodic = async (want: boolean) => {
    const deadline = Date.now() + 120_000;
    let seen: unknown;
    while (Date.now() < deadline) {
      seen = (await read('GET', `/admin/api/v1/sandboxes/${id}`, deadline)).json?.snapshots_enabled;
      if (seen === want) return true;
      await c.page.waitForTimeout(2000);
    }
    c.rec.note(`snapshots_enabled stayed ${String(seen)}, wanted ${want}`);
    return false;
  };
  // The checkpoint names the node reports, once one is reported.
  const reportedCheckpoints = async () => {
    const deadline = Date.now() + 120_000;
    while (Date.now() < deadline) {
      const debug = await read('GET', `/admin/api/v1/debug/sandbox/${id.replace(/^sbx_/, '')}`, deadline);
      const names: string[] = (debug.json?.snapshots?.inventory ?? [])
        .map((row: { snapshot_id: string }) => row.snapshot_id)
        .filter((name: string) => name.startsWith('f-manual-'));
      if (names.length) return names;
      await c.page.waitForTimeout(3000);
    }
    return [];
  };
  // The card holds the catalog's table, then the node's. The page reloads
  // the node's list every 20 s, so a reading is retried for longer than that.
  const card = () => c.page.locator('.ant-card', { has: c.page.locator('.ant-card-head-title', { hasText: 'Snapshots' }) }).first();
  const tables = () => card().locator('.ant-table-wrapper');
  const shows = async (which: 'catalog' | 'node', want: RegExp) => {
    const deadline = Date.now() + 45_000;
    let last = '';
    while (Date.now() < deadline) {
      if ((await tables().count()) === 2) {
        const table = which === 'catalog' ? tables().first() : tables().last();
        last = (await table.innerText({ timeout: Math.max(1, deadline - Date.now()) }).catch(() => last)).replace(/\s+/g, ' ');
        if (want.test(last)) return true;
      }
      await c.page.waitForTimeout(1500);
    }
    c.rec.note(`the ${which} table showed: ${last.slice(0, 300)}`);
    return false;
  };
  const PERIODIC_OFF = /The node does not report its snapshots while periodic snapshots are off/;
  const POINTS = /No snapshot records\. The node reports \d+ snapshots?, listed under On the node below\.|No snapshots recorded for upload\. The node reports \d+ snapshots?, listed under On the node below\./;
  const escape = (name: string) => name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  await succeeds('force-checkpoint', 'the checkpoint succeeds');
  c.rec.check(await observedPeriodic(false), 'the node reports periodic snapshots off');
  await visit(c, `/dashboard/sandboxes/${id}`, 2000);
  c.rec.check(/A checkpoint stays on the node and is not listed here/.test(await card().innerText({ timeout: 30_000 }).catch(() => '')), 'the card says a checkpoint is not in the catalog');
  c.rec.check(await shows('node', PERIODIC_OFF), 'with periodic snapshots off, On the node says the node does not report them');
  c.rec.check(await shows('catalog', /^.*(No snapshot records|No snapshots recorded for upload)$/), 'the empty catalog makes no claim about the node');

  let off = false;
  try {
    await succeeds('force-enable_snapshots', 'periodic snapshots turn on');
    c.rec.check(await observedPeriodic(true), 'the node reports periodic snapshots on');
    const reported = await reportedCheckpoints();
    const checkpoint = reported[0];
    c.rec.check(!!checkpoint, `the node reports the checkpoint (${checkpoint ?? 'none'})`);
    await visit(c, `/dashboard/sandboxes/${id}`, 2000);
    c.rec.check(await shows('node', new RegExp(escape(checkpoint ?? 'f-manual-'))), 'On the node lists the checkpoint');
    c.rec.check(await shows('catalog', POINTS), 'the empty catalog points at On the node');

    await succeeds('force-disable_snapshots', 'periodic snapshots turn off');
    c.rec.check(await observedPeriodic(false), 'the node reports periodic snapshots off again');
    await visit(c, `/dashboard/sandboxes/${id}`, 2000);
    c.rec.check(await shows('node', PERIODIC_OFF), 'turning periodic snapshots off empties On the node');

    // Hidden, not deleted: with them on again the node reports the same
    // checkpoint.
    await succeeds('force-enable_snapshots', 'periodic snapshots turn on again');
    c.rec.check(await observedPeriodic(true), 'the node reports periodic snapshots on again');
    const reportedAgain = await reportedCheckpoints();
    c.rec.check(!!checkpoint && reportedAgain.includes(checkpoint), `the node reports the same checkpoint again (${reportedAgain.join(', ') || 'none'})`);
    await visit(c, `/dashboard/sandboxes/${id}`, 2000);
    c.rec.check(await shows('node', new RegExp(escape(checkpoint ?? 'f-manual-'))), 'On the node lists the same checkpoint again');

    await succeeds('force-disable_snapshots', 'periodic snapshots turn off again');
    off = true;
  } finally {
    if (!off) {
      const disabled = await operation('force-disable_snapshots');
      c.rec.check(disabled === 'succeeded', `periodic snapshots turn off again (${disabled})`);
    }
  }
}, true);

// A control's accessible name starts with its icon's label.
for (const [action, label, want, confirm] of [
  ['pause', /(^|\s)Pause$/, ['paused'], false],
  ['resume', /(^|\s)Resume$/, ['started', 'running'], false],
  ['suspend', /(^|\s)Suspend$/, ['suspended'], false],
  ['restore', /(^|\s)Restore$/, ['started', 'running'], false],
  ['stop', /(^|\s)Stop$/, ['stopped'], true],
  ['start', /(^|\s)Start$/, ['started', 'running'], false],
] as const) {
  step(`sandbox-${action}`, ['sandboxes', 'flows', 'lifecycle'], async (c) => {
    const id = c.shared.sandbox;
    if (!id) return c.rec.check(false, 'a sandbox was created by sandbox-create');
    await visit(c, `/dashboard/sandboxes/${id}`, 2000);
    await waitForStatus(c, id, ['started', 'running', 'paused', 'suspended', 'stopped'], 120_000);
    const button = c.page.locator('.ant-card', { hasText: 'Controls' }).getByRole('button', { name: label });
    if (!(await button.isEnabled())) return c.rec.check(false, `${action} is enabled`);
    await button.click();
    if (confirm) await confirmPopover(c.page, label);
    const reached = await waitForStatus(c, id, [...want], 240_000);
    c.rec.check((want as readonly string[]).includes(reached), `${action} reaches ${want.join(' or ')} (${reached})`);
  }, true);
}

step('sandbox-delete', ['sandboxes', 'flows', 'lifecycle'], async (c) => {
  const id = c.shared.sandbox;
  if (!id) return;
  await visit(c, `/dashboard/sandboxes/${id}`, 2000);
  await c.page.locator('.ant-card', { hasText: 'Controls' }).getByRole('button', { name: /Delete/ }).click();
  await confirmPopover(c.page);
  await c.page.waitForURL(/\/dashboard\/sandboxes$/, { timeout: 120_000 }).catch(() => {});
  await settle(c.page, 2000);
  c.rec.check(/\/dashboard\/sandboxes$/.test(c.page.url()), 'deleting a sandbox returns to the list');
}, true);

// ---------------------------------------------------- templates and nodes
step('templates', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates', 4000);
  c.rec.check((await text(c.page)).includes('seed'), 'the templates page lists the seed template');
});

step('nodes', ['nodes'], async (c) => {
  await visit(c, '/dashboard/nodes', 4000);
  c.rec.check(/worker/i.test(await text(c.page)), 'the nodes page lists worker nodes');
});

// ------------------------------------------------------------ other roles
for (const [local, label] of [['acme-admin', 'a tenant organization admin'], ['ops-viewer', 'a member of the managing organization']] as const) {
  step(`role-${local}`, ['roles'], async (c) => {
    await signIn(c, email(c, local), seeded(c).userPassword);
    c.rec.check(/\/dashboard/.test(c.page.url()), `${label} can sign in`);
    for (const route of ['/dashboard', '/dashboard/sandboxes']) {
      await visit(c, route, 2000);
      await c.page.screenshot({ path: `${c.rec.outDir}/role-${local}${route.replaceAll('/', '-')}.png`, fullPage: true });
    }
    await signIn(c, c.config.email, c.config.password);
  });
}

step('logout', ['auth'], async (c) => {
  await visit(c, '/dashboard', 1500);
  await c.page.getByRole('button', { name: 'Log out' }).click();
  const confirm = c.page.locator('.ant-modal:visible').getByRole('button', { name: 'Log out' });
  if (await confirm.count()) await confirm.click();
  await c.page.waitForURL(/\/login/, { timeout: 10_000 }).catch(() => {});
  c.rec.check(/\/login/.test(c.page.url()), 'signing out lands on the login page');
});
