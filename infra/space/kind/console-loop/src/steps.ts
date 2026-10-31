// The loop's steps. Each visits part of the console as an operator would and
// checks what an operator relies on; the runner records everything else
// (errors, failed and slow requests, screenshots). Steps that create or
// change something are marked `mutates` and clean up after themselves.
import { adminFetch, confirmPopover, content, pickSelect, settle, shortLabel, signIn, text, visit, waitForStatus, type Ctx } from './browser.ts';

export interface Step {
  name: string;
  // Tags select steps with --only/--skip (matched against name and tags).
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
const orgId = (c: Ctx, key: string) => seeded(c).orgs[key];
const sandboxId = (c: Ctx, key: string) => seeded(c).sandboxes[key];
const email = (c: Ctx, local: string) => `${c.config.prefix}-${local}@example.test`;

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
  for (const name of ['Acme (synthetic)', 'Globex (synthetic)', 'Globex Research (synthetic)']) {
    c.rec.check(t.includes(name), `the organizations list shows ${name}`);
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
  await modal.getByRole('button', { name: 'Create organization' }).click();
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
  await pickSelect(c.page, content(c.page).locator('.ant-select').first(), 'Acme (synthetic)');
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
  await pickSelect(c.page, content(c.page).locator('.ant-select').first(), 'Acme (synthetic)');
  await settle(c.page);
  const address = `${c.runTag}@example.test`;
  await c.page.getByPlaceholder('Email').fill(address);
  await c.page.getByPlaceholder('Password').fill(seeded(c).userPassword);
  await c.page.getByPlaceholder('Display Name').fill('Loop Temporary');
  await c.page.getByRole('button', { name: 'Create' }).click();
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

step('api-key-create-revoke', ['api-keys', 'flows'], async (c) => {
  await visit(c, '/dashboard/api-keys');
  await c.page.getByRole('button', { name: 'Create API key' }).click();
  const modal = c.page.locator('.ant-modal:visible');
  await modal.locator('input').first().fill(c.runTag);
  await modal.getByRole('button', { name: 'Create' }).click();
  // The secret is shown once; the loop does not keep it.
  await c.page.locator('.ant-modal:visible', { hasText: 'Copy this key now' }).waitFor({ timeout: 10_000 });
  await c.page.getByRole('button', { name: 'Done' }).click();
  const row = c.page.locator('tr', { hasText: c.runTag });
  c.rec.check(await row.count() > 0, 'a created key appears in the list');
  if (!(await row.count())) return;
  await row.getByRole('button', { name: `Revoke ${c.runTag}` }).click();
  await confirmPopover(c.page, 'Revoke');
  await settle(c.page);
  c.rec.check(await c.page.locator('tr', { hasText: c.runTag }).count() === 0, 'a revoked key disappears');
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
  await card.locator('button[type=submit]').click();
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline && !c.shared.sandbox) {
    await c.page.waitForTimeout(3000);
    const list = await adminFetch(c.page, 'GET', '/admin/api/v1/sandboxes?limit=5000');
    const row = (list.json ?? []).find((s: { name: string | null }) => s.name === c.runTag);
    if (row) c.shared.sandbox = row.id.startsWith('sbx_') ? row.id : `sbx_${row.id}`;
  }
  c.rec.check(!!c.shared.sandbox, 'creating a sandbox from the form produces one');
}, true);

for (const [action, label, want, confirm] of [
  ['pause', 'Pause', ['paused'], false],
  ['resume', 'Resume', ['started', 'running'], false],
  ['suspend', 'Suspend', ['suspended'], false],
  ['restore', 'Restore', ['started', 'running'], false],
  ['stop', 'Stop', ['stopped'], true],
  ['start', 'Start', ['started', 'running'], false],
] as const) {
  step(`sandbox-${action}`, ['sandboxes', 'flows', 'lifecycle'], async (c) => {
    const id = c.shared.sandbox;
    if (!id) return c.rec.check(false, 'a sandbox was created by sandbox-create');
    await visit(c, `/dashboard/sandboxes/${id}`, 2000);
    await waitForStatus(c, id, ['started', 'running', 'paused', 'suspended', 'stopped'], 120_000);
    const button = c.page.locator('.ant-card', { hasText: 'Controls' }).getByRole('button', { name: new RegExp(`(^|\\s)${label}$`) });
    if (!(await button.isEnabled())) return c.rec.check(false, `${label} is enabled`);
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
