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

// ---------------------------------------------------------------- terminal
// The sandbox page's terminal is an emulator on a PTY: keys go to the shell as
// typed, and full-screen programs draw in it. The checks read the rows it
// renders, so each command prints a marker the command line itself does not
// contain (`$((6*7))` prints 42).
const terminalCard = (page: Page) => page.locator('.ant-card', { has: page.locator('.ant-card-head', { hasText: 'Terminal' }) });
const terminalRows = async (page: Page): Promise<string> =>
  (await terminalCard(page).locator('.xterm-rows').innerText().catch(() => '')).replace(/\u00a0/g, ' ');
async function terminalShows(page: Page, want: RegExp | ((rows: string) => boolean), timeoutMs = 10_000): Promise<boolean> {
  const matches = typeof want === 'function' ? want : (rows: string) => want.test(rows);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (matches(await terminalRows(page))) return true;
    await page.waitForTimeout(200);
  }
  return false;
}
const terminalStatus = (page: Page) => terminalCard(page).locator('.ant-card-extra').innerText().catch(() => '');
// The shell is ready for a command once its prompt (a line ending in `$` or
// `#`) is drawn.
const showsPrompt = (rows: string) => /[$#]\s*$/m.test(rows);
async function terminalConnect(c: Ctx): Promise<boolean> {
  await terminalCard(c.page).getByRole('button', { name: /Connect$/ }).click();
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline && !/attached/.test(await terminalStatus(c.page))) await c.page.waitForTimeout(250);
  const attached = /attached/.test(await terminalStatus(c.page));
  c.rec.check(attached, `Connect attaches a session (${(await terminalStatus(c.page)).replace(/\s+/g, ' ')})`);
  if (!attached) return false;
  const ready = await terminalShows(c.page, showsPrompt, 20_000);
  c.rec.check(ready, 'the shell draws its prompt');
  return ready;
}
async function terminalRun(c: Ctx, command: string, marker: RegExp, what: string, timeoutMs = 10_000): Promise<boolean> {
  await c.page.keyboard.type(command);
  await c.page.keyboard.press('Enter');
  const shown = await terminalShows(c.page, marker, timeoutMs);
  c.rec.check(shown, what);
  return shown;
}
const terminalShot = (c: Ctx, name: string) => terminalCard(c.page).screenshot({ path: `${c.rec.outDir}/${name}.png` }).catch(() => {});
const sizeOf = (rows: string, marker: string) => rows.match(new RegExp(`${marker}=(\\d+)x(\\d+)`))?.slice(1).map(Number) ?? null;

// Each session is recorded in the admin audit log, so these steps mutate.
for (const [name, key, ownOrg] of [['own-org', 'running', true], ['other-org', 'acme-running', false]] as const) {
  step(`terminal-${name}`, ['sandboxes', 'terminal'], async (c) => {
    const id = sandboxId(c, key);
    if (!id) return c.rec.check(false, `the seed has a ${key} sandbox`);
    await visit(c, `/dashboard/sandboxes/${id}`, 3000);
    const card = terminalCard(c.page);
    await card.scrollIntoViewIfNeeded();
    c.rec.check(await card.locator('.xterm').count() === 1, 'the Terminal card holds a terminal emulator');
    if (!ownOrg) c.rec.check(/another organization/.test(await card.innerText()), "another organization's sandbox shows the admin-route notice");
    if (!(await terminalConnect(c))) return;
    c.rec.check(await c.page.evaluate(() => !!document.activeElement?.closest('.xterm')), 'the terminal has focus once attached');

    await terminalRun(c, 'ls --color=always / && echo listed-$((6*7))', /listed-42/, 'a command runs and prints');
    const colored = await card.locator('.xterm-rows span[class*="xterm-fg-"], .xterm-rows span[style*="color"]').count();
    c.rec.check(colored > 0, `ls --color draws colored text (${colored} spans)`);
    await c.page.keyboard.press('ArrowUp');
    await c.page.keyboard.press('Enter');
    c.rec.check(await terminalShows(c.page, (rows) => (rows.match(/listed-42/g) ?? []).length >= 2), 'the up arrow recalls the last command');

    await c.page.keyboard.type('ech');
    await c.page.keyboard.press('Tab');
    await terminalRun(c, ' completed-$((7*7))', /completed-49/, 'Tab completes a command name');

    await c.page.keyboard.type('sleep 100');
    await c.page.keyboard.press('Enter');
    await c.page.waitForTimeout(1000);
    await c.page.keyboard.press('Control+C');
    await terminalRun(c, 'echo interrupted-$((8*8))', /interrupted-64/, 'Ctrl-C stops sleep 100');

    if (ownOrg) {
      await c.page.keyboard.type('top');
      await c.page.keyboard.press('Enter');
      c.rec.check(await terminalShows(c.page, /PID/), 'top draws its process table');
      await terminalShot(c, `terminal-${name}-top`);
      await c.page.keyboard.press('q');
      await terminalRun(c, 'clear; echo top-quit-$((9*9))', /top-quit-81/, 'q quits top back to the shell');

      // The seed template has no editor; a template with vi gets the check.
      await terminalRun(c, 'clear; command -v vi >/dev/null && echo vi-$((5*5))-found || echo vi-$((5*5))-none', /vi-25-(found|none)/, 'the shell reports whether vi exists');
      if (/vi-25-found/.test(await terminalRows(c.page))) {
        await c.page.keyboard.type('vi /tmp/console-loop-vi.txt');
        await c.page.keyboard.press('Enter');
        c.rec.check(await terminalShows(c.page, /~\s*\n\s*~/), 'vi opens full screen');
        await c.page.keyboard.type('ityped-in-vi');
        await c.page.keyboard.press('Escape');
        c.rec.check(await terminalShows(c.page, /typed-in-vi/), 'text typed in vi appears');
        await terminalShot(c, `terminal-${name}-vi`);
        await c.page.keyboard.type(':q!');
        await c.page.keyboard.press('Enter');
        await terminalRun(c, 'clear; test ! -e /tmp/console-loop-vi.txt && echo vi-quit-$((10*10))', /vi-quit-100/, ':q! leaves vi without writing');
      } else {
        c.rec.note('the sandbox has no vi; the vi check did not run');
      }

      // The PTY follows the card's size.
      await terminalRun(c, 'echo before=$(tput cols 2>/dev/null || stty size | cut -d" " -f2)x$(tput lines 2>/dev/null || stty size | cut -d" " -f1)', /before=\d+x\d+/, 'the shell reports its size');
      const viewport = c.page.viewportSize();
      await c.page.setViewportSize({ width: 1000, height: viewport?.height ?? 900 });
      await c.page.waitForTimeout(1000);
      await card.scrollIntoViewIfNeeded();
      await c.page.keyboard.press('Enter');
      await terminalRun(c, 'echo after=$(tput cols 2>/dev/null || stty size | cut -d" " -f2)x$(tput lines 2>/dev/null || stty size | cut -d" " -f1)', /after=\d+x\d+/, 'the shell reports its size after the resize');
      const rows = await terminalRows(c.page);
      const [before, after] = [sizeOf(rows, 'before'), sizeOf(rows, 'after')];
      c.rec.check(!!before && !!after && after[0] < before[0], `a narrower window narrows the PTY (${before?.join('x')} → ${after?.join('x')})`);
      if (viewport) await c.page.setViewportSize(viewport);
      await c.page.waitForTimeout(500);
    }
    await terminalShot(c, `terminal-${name}-session`);

    await card.getByRole('button', { name: /Disconnect$/ }).click();
    await c.page.waitForTimeout(500);
    c.rec.check(/closed/.test(await terminalStatus(c.page)), 'Disconnect closes the session');
    // Typed at once, before the new shell has started: the card holds it and
    // sends it when the shell starts.
    await card.getByRole('button', { name: /Connect$/ }).click();
    await terminalRun(c, 'echo reconnected-$((11*11))', /reconnected-121/, 'a second Connect opens a new shell and runs what was typed before it started', 30_000);
    await card.getByRole('button', { name: /Disconnect$/ }).click();
  }, true);
}

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
