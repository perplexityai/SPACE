// The loop's steps. Each visits part of the console as an operator would and
// checks what an operator relies on; the runner records everything else
// (errors, failed and slow requests, screenshots). Steps that create or
// change something are marked `mutates` and clean up after themselves.
import type { Page, Request, Response, Route } from 'playwright';

import {
  SECRET_PLACEHOLDER, adminFetch, carriesSecret, confirmPopover, content, fetchWithoutRedirect, pickSelect, settle, shortLabel,
  signIn, text, visit, waitForStatus, withoutSecret, type Ctx,
} from './browser.ts';
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
// A stack with many templates pages them: the search keeps seed on the
// first page.
step('templates', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates?q=seed', 4000);
  c.rec.check((await text(c.page)).includes('seed'), 'the templates page lists the seed template');
});

// The Templates list keeps its search, filters, sort and page in the URL.
// These steps read the `seed` release from the API, so they hold on any
// stack, seeded with more templates or not.
const templateRows = (c: Ctx) => content(c.page).locator('.ant-tabs-tabpane-active tbody tr.ant-table-row');
const templateSearch = (c: Ctx) => content(c.page).getByLabel('Search templates');
const seedRelease = async (c: Ctx): Promise<{ id: string; content_hash: string; tags: string[] } | null> => {
  const releases = await adminFetch(c.page, 'GET', '/admin/api/v1/template-releases');
  return (releases.json ?? []).find((r: { template_name: string; tags: string[] }) => r.template_name === 'seed' && r.tags.includes('latest')) ?? null;
};
const urlParam = (c: Ctx, name: string) => new URL(c.page.url()).searchParams.get(name);
// The open tab's content, without the tab labels.
const activePane = async (c: Ctx) => (await content(c.page).locator('.ant-tabs-tabpane-active').first().innerText()).replace(/\s+/g, ' ');
const activeTab = async (c: Ctx) => (await content(c.page).locator('.ant-tabs-tab-active').first().innerText()).trim();
// The template names on the page: each name cell's first line.
const templateNames = async (c: Ctx) => (await templateRows(c).locator('td:first-child').allInnerTexts()).map((cell) => cell.split('\n')[0].trim());

step('templates-search-name', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates', 3000);
  await templateSearch(c).fill('SEE');
  await c.page.waitForTimeout(500);
  const names = await templateNames(c);
  c.rec.check(names.length >= 1 && names.every((name) => name.toLowerCase().includes('see')), `a name search keeps matching rows only (${names.length})`);
  c.rec.check(urlParam(c, 'q') === 'SEE', 'the search is in the URL');
});

step('templates-search-hash', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates', 2000);
  const release = await seedRelease(c);
  if (!release) return c.rec.check(false, 'the seed template has a latest release');
  const prefix = release.content_hash.replace(/^[a-z0-9]+:/, '').slice(0, 8);
  await templateSearch(c).fill(prefix);
  await c.page.waitForTimeout(500);
  const t = await text(c.page);
  c.rec.check(t.includes(`seed@${prefix}`), `a content hash prefix names the seed release (${prefix})`);
  c.rec.check((await templateRows(c).filter({ hasText: 'seed' }).count()) >= 1, "the release's template is listed");
  await content(c.page).locator('.ant-alert a', { hasText: `seed@${prefix}` }).first().click();
  await c.page.waitForURL(/\/dashboard\/templates\/seed/, { timeout: 10_000 }).catch(() => {});
  await settle(c.page, 2500);
  c.rec.check(urlParam(c, 'tab') === 'releases' && urlParam(c, 'release') === release.id, "the match opens the template's releases with it marked");
  const rows = content(c.page).locator('.ant-tabs-tabpane-active tbody tr.ant-table-row');
  c.rec.check(await rows.count() === 1 && await content(c.page).locator('tr.template-release-highlight').count() === 1, 'the searched release is the one row shown, marked');
  const seedReleases = ((await adminFetch(c.page, 'GET', '/admin/api/v1/template-releases')).json ?? []).filter((r: { template_name: string }) => r.template_name === 'seed');
  if (seedReleases.length < 2) c.rec.note(`seed has ${seedReleases.length} release: one row shows with or without the focus, so this cannot show the focus applied`);
  await content(c.page).getByRole('button', { name: 'Show all releases' }).click();
  await settle(c.page, 1000);
  c.rec.check(urlParam(c, 'release') === null && urlParam(c, 'tab') === 'releases', 'Show all releases drops the release from the URL and stays on the tab');
});

step('templates-search-tag', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates?org=system', 2000);
  await templateSearch(c).fill('latest');
  await c.page.waitForTimeout(500);
  c.rec.check(/release(s)? match(es)? “latest”/.test(await text(c.page)), 'a tag search lists the releases holding it');
  c.rec.check((await templateRows(c).filter({ hasText: 'seed' }).count()) >= 1, 'the seed template, whose release holds latest, is listed');
});

// What the API holds, as an independent oracle for the list's filters.
interface Catalog {
  templates: { name: string; org_id: string | null; pool_current: number | null; pool_target: number | null }[];
  releases: { template_name: string; tags: string[]; warm_pools: { enabled: boolean }[] }[];
  policies: { template_name: string; enabled: boolean }[];
}
const readCatalog = async (c: Ctx): Promise<Catalog> => {
  const list = async (apiPath: string) => {
    const answer = await adminFetch(c.page, 'GET', apiPath);
    return Array.isArray(answer.json) ? answer.json : [];
  };
  return {
    templates: await list('/admin/api/v1/templates'),
    releases: await list('/admin/api/v1/template-releases'),
    policies: await list('/admin/api/v1/warm-pool-policies'),
  };
};
const hasPool = (catalog: Catalog, name: string) => {
  const template = catalog.templates.find((t) => t.name === name);
  return Boolean((template?.pool_current ?? 0) > 0 || (template?.pool_target ?? 0) > 0
    || catalog.releases.some((r) => r.template_name === name && r.warm_pools.some((pool) => pool.enabled))
    || catalog.policies.some((policy) => policy.template_name === name && policy.enabled));
};
// The list's row count, from its pagination total.
const listedCount = async (c: Ctx): Promise<number | null> => {
  const pane = await content(c.page).locator('.ant-tabs-tabpane-active').first().innerText();
  if (/No template matches/.test(pane)) return 0;
  const total = pane.match(/of (\d+) templates/);
  return total ? Number(total[1]) : null;
};
const cellTexts = async (c: Ctx, column: number) => (await templateRows(c).locator(`td:nth-child(${column})`).allInnerTexts()).map((cell) => cell.trim());

// Each filter, from a link: the URL keeps it, the list's count is what the
// API's own records give for it, and each row on the page shows its effect.
// A filter the list ignored would show every template.
for (const [query, label, expected, cell] of [
  ['org=system', 'the system organization', (k: Catalog) => k.templates.filter((t) => t.org_id === null).map((t) => t.name), [2, /^System$/]],
  ['release=published', 'templates with a release', (k: Catalog) => k.templates.filter((t) => k.releases.some((r) => r.template_name === t.name)).map((t) => t.name), [3, /^(?!no release$)/]],
  ['pool=without', 'templates without a warm pool', (k: Catalog) => k.templates.filter((t) => !hasPool(k, t.name)).map((t) => t.name), [5, /No pool|disabled/]],
  ['tag=latest', 'the latest tag', (k: Catalog) => k.templates.filter((t) => k.releases.some((r) => r.template_name === t.name && r.tags.includes('latest'))).map((t) => t.name), null],
] as const) {
  step(`templates-filter-${query.split('=')[0]}`, ['templates'], async (c) => {
    await visit(c, `/dashboard/templates?${query}`, 3000);
    const [param, value] = query.split('=');
    c.rec.check(urlParam(c, param) === value, `the ${param} filter from the link is kept in the URL`);
    const catalog = await readCatalog(c);
    const names = expected(catalog);
    const want = names.length;
    const listed = await listedCount(c);
    c.rec.check(listed === want, `filtering by ${label} lists the ${want} template(s) the API's records give (listed ${listed})`);
    // The first page, sorted by name, is the first of those names: the
    // right count of the wrong templates fails here.
    const page = await templateNames(c);
    const first = [...names].sort((a, b) => a.localeCompare(b)).slice(0, page.length || 1);
    c.rec.check(page.length > 0 === want > 0 && page.every((name, i) => name === first[i]), `the first page lists the expected templates (${page.slice(0, 3).join(', ')}${page.length > 3 ? ', …' : ''})`);
    if (want === catalog.templates.length) c.rec.note(`every template on this stack matches ${query}: the count cannot show whether the filter applied`);
    if (cell) {
      const [column, pattern] = cell;
      const cells = await cellTexts(c, column);
      c.rec.check(cells.every((text) => pattern.test(text)), `each listed row shows ${label} (${cells.length} rows)`);
    }
  });
}

step('templates-filter-build', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates?build=failed', 3000);
  await content(c.page).getByText(/Reading the latest build/).waitFor({ state: 'detached', timeout: 60_000 }).catch(() => {});
  c.rec.check(urlParam(c, 'build') === 'failed', 'the build filter from the link is kept in the URL');
  const catalog = await readCatalog(c);
  let failed = 0;
  let readable = 0;
  for (const template of catalog.templates) {
    const builds = await adminFetch(c.page, 'GET', `/admin/api/v1/templates/${template.name}/builds?limit=1`);
    if (!Array.isArray(builds.json)) continue;
    readable += 1;
    if (['failed', 'publish_failed'].includes(builds.json[0]?.state)) failed += 1;
  }
  if (!readable) return c.rec.note('this API does not list builds: the build filter is disabled');
  const listed = await listedCount(c);
  c.rec.check(listed === failed, `the build filter lists the ${failed} template(s) whose latest build failed (listed ${listed})`);
  if (failed === catalog.templates.length) c.rec.note('every template\'s latest build failed: the count cannot show whether the filter applied');
  const cells = await cellTexts(c, 4);
  c.rec.check(cells.every((text) => /failed/.test(text)), `each listed row's latest build failed (${cells.length} rows)`);
});

step('templates-url-state', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates?q=e&sort=name&order=descend&size=10', 3000);
  await c.page.reload({ waitUntil: 'domcontentloaded' });
  await settle(c.page, 2500);
  c.rec.check((await templateSearch(c).inputValue()) === 'e', 'the search survives a reload');
  const names = await templateNames(c);
  c.rec.check(names.length > 0 && names.every((name, i) => i === 0 || names[i - 1].localeCompare(name) >= 0), 'the sort from the link applies after a reload');
  c.rec.check(urlParam(c, 'sort') === 'name' && urlParam(c, 'order') === 'descend' && urlParam(c, 'size') === '10', 'the sort and page size stay in the URL');
});

step('templates-sort-page', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates?size=10', 3000);
  await content(c.page).locator('th', { hasText: 'Nodes' }).first().click();
  await c.page.waitForTimeout(500);
  c.rec.check(urlParam(c, 'sort') === 'nodes', 'sorting by a column is written to the URL');
  const next = content(c.page).locator('.ant-tabs-tabpane-active .ant-pagination-item-2');
  if (!(await next.count())) return c.rec.note('ten templates or fewer: paging not exercised');
  await next.click();
  await c.page.waitForTimeout(500);
  c.rec.check(urlParam(c, 'page') === '2', 'the page is written to the URL');
  await c.page.reload({ waitUntil: 'domcontentloaded' });
  await settle(c.page, 2500);
  c.rec.check(await content(c.page).locator('.ant-tabs-tabpane-active .ant-pagination-item-active').innerText() === '2', 'the page survives a reload');
});

for (const [tab, label, expect] of [
  ['releases', 'All releases', /template releases/],
  ['policies', 'Warm pool policies', /Rolling Warm Pool Policies/],
  ['nodes', 'Node availability', /template builds|No data/],
  ['spaced', 'spaced versions', /spaced version|Running sandboxes/],
  ['system', 'System templates', /System template rebuild/],
] as const) {
  step(`templates-tab-${tab}`, ['templates'], async (c) => {
    await visit(c, '/dashboard/templates', 2500);
    const tabButton = content(c.page).getByRole('tab', { name: new RegExp(`^${label}`) });
    if (!(await tabButton.count())) return c.rec.note(`no ${label} tab on this stack (substrate or role)`);
    await tabButton.click();
    await settle(c.page, 2500);
    c.rec.check(urlParam(c, 'tab') === tab, `the ${label} tab is in the URL`);
    c.rec.check(expect.test(await activePane(c)), `the ${label} tab renders its content`);
  });
}

step('templates-releases-search', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates?tab=releases&rq=seed', 3000);
  const rows = content(c.page).locator('.ant-tabs-tabpane-active tbody tr.ant-table-row');
  const all = await rows.allInnerTexts();
  c.rec.check(all.length > 0 && all.every((row) => row.includes('seed')), `the releases search from the link keeps matching rows (${all.length})`);
});

for (const [tab, expect] of [
  ['overview', /Default auto lifecycle/],
  ['releases', /template releases/],
  // The rebuild panel and the build history: its table, or its empty state.
  ['builds', /System template rebuild.*(Content hash|No builds recorded|does not list a template's builds)/],
  ['pool', /Release pools|Manage pool|Create pool|This substrate has no warm pools/],
  ['nodes', /template builds|No reachable node/],
  ['sandboxes', /Open the sandboxes of seed/],
] as const) {
  step(`template-detail-${tab}`, ['templates'], async (c) => {
    await visit(c, `/dashboard/templates/seed${tab === 'overview' ? '' : `?tab=${tab}`}`, 3000);
    c.rec.check(expect.test(await activePane(c)), `the seed template's ${tab} section renders`);
  });
}

// A link to another tab of the same page opens that tab: the URL, not the
// tab last clicked, decides what shows; Back returns to the previous tab.
step('template-detail-tab-links', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates/seed', 3000);
  await content(c.page).locator('.ant-tabs-tabpane-active a', { hasText: /\d+ releases?$/ }).first().click();
  await settle(c.page, 1500);
  c.rec.check(urlParam(c, 'tab') === 'releases' && /^Releases/.test(await activeTab(c)), `the Overview's releases link opens the Releases tab (${await activeTab(c)})`);
  await content(c.page).getByRole('tab', { name: /^Builds/ }).click();
  await settle(c.page, 1000);
  await c.page.goBack();
  await settle(c.page, 1500);
  c.rec.check(/^Releases/.test(await activeTab(c)), `Back returns to the Releases tab (${await activeTab(c)})`);
  await visit(c, '/dashboard/templates?tab=releases', 2500);
  await c.page.locator('.ant-menu').getByRole('link', { name: 'Templates' }).click();
  await settle(c.page, 1500);
  c.rec.check(urlParam(c, 'tab') === null && /^Templates/.test(await activeTab(c)), `the menu's Templates link opens the template list (${await activeTab(c)})`);
});

// The system-template rebuild needs no template records: a new cluster has
// none. The list is answered empty here to show the first build's entry.
step('templates-system-first-build', ['templates'], async (c) => {
  const empty = async (route: Route) => route.fulfill({ status: 200, headers: { 'content-type': 'application/json', 'x-console-loop-synthetic': '1' }, body: '[]' });
  await c.page.route(/\/admin\/api\/v1\/(templates|template-releases)$/, empty);
  try {
    await visit(c, '/dashboard/templates?tab=system', 3000);
    const pane = await activePane(c);
    c.rec.check(/not built yet/.test(pane), 'with no templates, the system templates show as not built yet');
    c.rec.check(await content(c.page).getByRole('button', { name: 'Rebuild system templates' }).isEnabled(), 'the rebuild can still be started');
  } finally {
    await c.page.unroute(/\/admin\/api\/v1\/(templates|template-releases)$/, empty);
  }
});

step('templates-row-rebuild', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates?q=seed', 3000);
  const rebuild = templateRows(c).filter({ hasText: 'seed' }).getByRole('button', { name: 'Rebuild' });
  if (!(await rebuild.count())) return c.rec.check(false, 'the seed row offers Rebuild');
  await rebuild.first().click();
  await settle(c.page, 1500);
  c.rec.check(urlParam(c, 'tab') === 'system' && /System template rebuild/.test(await activePane(c)), "the seed row's Rebuild opens the system-template rebuild");
});

step('template-detail-unknown', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates/loop-no-such-template', 3000);
  c.rec.check(/not found/i.test(await text(c.page)), 'an unknown template shows a not-found state');
});

step('template-detail-sandboxes-link', ['templates'], async (c) => {
  await visit(c, '/dashboard/templates/seed?tab=sandboxes', 2500);
  await content(c.page).getByRole('button', { name: /Open the sandboxes of seed/ }).click();
  await c.page.waitForURL(/\/dashboard\/sandboxes/, { timeout: 10_000 }).catch(() => {});
  await settle(c.page, 2500);
  c.rec.check(urlParam(c, 'template') === 'seed', 'the link opens the sandbox list filtered by the template');
});

// Assigns a tag of its own to the seed release through the retag dialog,
// then deletes it from the tag's own control.
step('template-tag-assign-delete', ['templates', 'flows'], async (c) => {
  await visit(c, '/dashboard/templates/seed?tab=releases', 3000);
  const release = await seedRelease(c);
  if (!release) return c.rec.check(false, 'the seed template has a latest release');
  const tag = c.runTag;
  const row = content(c.page).locator('tbody tr.ant-table-row', { hasText: release.content_hash.replace(/^[a-z0-9]+:/, '').slice(0, 12) });
  await row.getByRole('button', { name: 'Assign or move release tag' }).click();
  const modal = c.page.locator('.ant-modal:visible');
  await modal.getByLabel('Tag').fill(tag);
  await modal.getByRole('button', { name: 'OK' }).click();
  await settle(c.page, 2000);
  const after = await seedRelease(c);
  c.rec.check(!!after?.tags.includes(tag), `the tag ${tag} names the seed release`);
  if (!after?.tags.includes(tag)) return;
  await row.getByRole('button', { name: `Delete tag ${tag}` }).click();
  await confirmPopover(c.page);
  await settle(c.page, 2000);
  const cleaned = await seedRelease(c);
  c.rec.check(!cleaned?.tags.includes(tag), 'deleting the tag removes it');
}, true);

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
