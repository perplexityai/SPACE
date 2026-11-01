// The loop's steps. Each visits part of the console as an operator would and
// checks what an operator relies on; the runner records everything else
// (errors, failed and slow requests, screenshots). Steps that create or
// change something are marked `mutates` and clean up after themselves.
import type { Page, Response } from 'playwright';

import { SECRET_PLACEHOLDER, adminFetch, confirmPopover, content, pickSelect, settle, shortLabel, signIn, text, visit, waitForStatus, withoutSecret, type Ctx } from './browser.ts';
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
  recordCreated(c.config, { kind: 'org', name: c.runTag });
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
  recordCreated(c.config, { kind: 'user', name: address, orgId: orgId(c, 'acme') });
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
// once. The step answers that request itself: it records the key's ID from
// the real response, then hands the page the response with the secret
// replaced by a placeholder, so the browser never holds a live key.
step('api-key-create-revoke', ['api-keys', 'flows'], async (c) => {
  const managing = seeded(c).managingOrgId;
  const pattern = '**/admin/api/v1/orgs/*/api-keys';
  let answered: (value: { id?: string; replaced: boolean } | null) => void = () => {};
  const createAnswered = new Promise<{ id?: string; replaced: boolean } | null>((resolve) => { answered = resolve; });
  await c.page.route(pattern, async (route) => {
    if (route.request().method() !== 'POST') return route.fallback();
    const response = await route.fetch();
    const safe = withoutSecret(await response.text());
    if (safe.id) recordCreated(c.config, { kind: 'apiKey', id: safe.id, name: c.runTag, orgId: managing });
    answered({ id: safe.id, replaced: safe.replaced });
    const headers = { ...response.headers() };
    delete headers['content-length'];
    await route.fulfill({ status: response.status(), headers, body: safe.body });
  });
  try {
    await visit(c, '/dashboard/api-keys');
    await c.page.getByRole('button', { name: 'Create API key' }).click();
    const modal = c.page.locator('.ant-modal:visible');
    await modal.locator('input').first().fill(c.runTag);
    recordCreated(c.config, { kind: 'apiKey', name: c.runTag, orgId: managing });
    await modal.getByRole('button', { name: 'Create' }).click();
    const created = await Promise.race([createAnswered, c.page.waitForTimeout(30_000).then(() => null)]);
    c.rec.check(!!created?.id, "the create response carries the new key's ID");
    c.rec.check(!!created?.replaced, "the create response's secret is replaced before the page sees it");
    const dialog = c.page.locator('.ant-modal:visible', { hasText: 'Copy this key now' });
    await dialog.waitFor({ timeout: 10_000 });
    const shown = await dialog.innerText();
    const live = KEY_SHAPED.test(shown);
    c.rec.check(!live && shown.includes(SECRET_PLACEHOLDER), 'the dialog shows the placeholder, not a key');
    if (live) {
      c.rec.note('dialog screenshot skipped: it shows something shaped like a key');
    } else {
      await c.page.waitForTimeout(800);
      await c.page.screenshot({ path: `${c.rec.outDir}/api-key-dialog-placeholder.png` });
    }
    await c.page.getByRole('button', { name: 'Done' }).click();
    const row = c.page.locator('tr', { hasText: c.runTag });
    c.rec.check(await row.count() > 0, 'a created key appears in the list');
    if (!(await row.count())) return;
    await row.getByRole('button', { name: `Revoke ${c.runTag}` }).click();
    await confirmPopover(c.page, /(^|\s)Revoke$/);
    await settle(c.page);
    c.rec.check(await c.page.locator('tr', { hasText: c.runTag }).count() === 0, 'a revoked key disappears');
  } finally {
    await c.page.unroute(pattern);
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
  recordCreated(c.config, { kind: 'sandbox', name: c.runTag, orgId: managing });
  const response = nextPost(c.page, (p) => p.endsWith('/v1/sandboxes'));
  await card.locator('button[type=submit]').click();
  const res = await response;
  const accepted = await jsonOf(res);
  if (accepted?.operation_id) {
    const admin = new URL(res!.url()).pathname.startsWith('/admin/');
    const id = accepted.sandbox_id ? (accepted.sandbox_id.startsWith('sbx_') ? accepted.sandbox_id : `sbx_${accepted.sandbox_id}`) : undefined;
    recordCreated(c.config, { kind: 'sandbox', id, name: c.runTag, orgId: managing, operations: [{ id: accepted.operation_id, admin, action: 'create' }] });
    if (id) c.shared.sandbox = id;
  }
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline && !c.shared.sandbox) {
    await c.page.waitForTimeout(3000);
    const list = await adminFetch(c.page, 'GET', '/admin/api/v1/sandboxes?limit=5000');
    const row = (list.json ?? []).find((s: { name: string | null }) => s.name === c.runTag);
    if (row) {
      c.shared.sandbox = row.id.startsWith('sbx_') ? row.id : `sbx_${row.id}`;
      recordCreated(c.config, { kind: 'sandbox', id: c.shared.sandbox, name: c.runTag, orgId: managing });
    }
  }
  c.rec.check(!!c.shared.sandbox, 'creating a sandbox from the form produces one');
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
