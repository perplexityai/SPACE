// The loop's steps. Each visits part of the console as an operator would and
// checks what an operator relies on; the runner records everything else
// (errors, failed and slow requests, screenshots). Steps that create or
// change something are marked `mutates` and clean up after themselves.
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

import type { Locator, Page, Request, Response, Route } from 'playwright';

import {
  SECRET_PLACEHOLDER, adminFetch, carriesSecret, confirmPopover, content, fetchWithoutRedirect, pickSelect, settle, shortLabel,
  signIn, takeToasts, text, visit, waitForStatus, withoutSecret, type Ctx,
} from './browser.ts';
import { Api, ApiError, TERMINAL_OPERATION_STATES, sleep } from './api.ts';
import { verifyFixtures } from './fixtures.ts';
import { SYNTHETIC_HEADER } from './recorder.ts';
import { orgDisplayName, recordCreated } from './seed.ts';
import {
  CONSOLE_PREVIEW_CONCURRENCY, DRAIN_KEEP_ANNOTATION, HOLD_WAIT, KindNodes, bareSandboxId, drainCandidate, drainCountsOf, drainFinished, everyPage,
  panelMismatches, previewAllWaitMs, previewResultsMismatches, previewVerdictIn, previewWaitMs, reportedUnder, rowMismatches,
  type AdminNode, type ApiDrain, type PanelRead, type ResultVerdict,
} from './nodes.ts';
import { DRAIN_V2_SWITCH, KindV2Control, V2_CONTROLLER_ENV, V2_CONTROLLER_KEYS, drainV2Enabled, v1DrainSkip, type Lease } from './drain-v2.ts';

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

// The search box matches as it does on `main`: a case-sensitive `prefix`
// request, and for hex digits and dashes a match within the page as well.
// Text that is also an ID's hex digits (4 to 32, after an optional `sbx_`)
// adds a `search` request of its own, for IDs that end with them (such as
// the ID column's label) or, from 8 digits, start with them; its matches
// beyond the page are listed after it, with the prefix request's, once per
// sandbox. An API without `search` refuses that request with a 400 naming
// the parameter, and the console stops sending it. `server` and `fallback`
// are those two answers to the `search` request; anything else, a timeout
// included, or a `prefix` request left unanswered, is `failed`, and fails
// the step. Other text never sends `search` (`sandboxes-search-name-prefix`).
type SearchAnswer = { kind: 'server' | 'fallback' | 'failed' };
const isListRequest = (url: URL) => url.pathname.endsWith('/admin/api/v1/sandboxes');
const UNKNOWN_SEARCH = 'unknown query parameter "search"';

const typeSearch = async (c: Ctx, typed: string): Promise<SearchAnswer> => {
  const searches: string[] = [];
  const onRequest = (req: Request) => {
    const url = new URL(req.url());
    if (isListRequest(url) && url.searchParams.get('search') === typed) searches.push(url.search);
  };
  const answered = c.page.waitForResponse((res) => {
    const url = new URL(res.url());
    return isListRequest(url) && url.searchParams.get('search') === typed;
  }, { timeout: 10_000 }).catch(() => null);
  const prefixAnswered = c.page.waitForResponse((res) => {
    const url = new URL(res.url());
    return isListRequest(url) && !url.searchParams.has('search') && url.searchParams.get('prefix') === typed;
  }, { timeout: 15_000 }).catch(() => null);
  c.page.on('request', onRequest);
  let answer: SearchAnswer = { kind: 'failed' };
  let res: Response | null = null;
  try {
    await content(c.page).locator('input[placeholder*="ID"]').first().fill(typed);
    res = await answered;
    if (res?.ok()) answer = { kind: 'server' };
    else if (res?.status() === 400 && (await res.json().catch(() => null))?.error === UNKNOWN_SEARCH) answer = { kind: 'fallback' };
    const prefixRes = await prefixAnswered;
    c.rec.check(prefixRes?.ok() === true, `"${typed}" was also sent as prefix, as on main, and answered (${prefixRes ? prefixRes.status() : 'no answer'})`);
    await settle(c.page, 1500);
  } finally {
    c.page.off('request', onRequest);
  }
  c.rec.check(answer.kind !== 'failed', `"${typed}" was answered as search, or refused as an unknown parameter (${res ? res.status() : 'no answer'})`);
  if (answer.kind === 'fallback') {
    c.rec.note(`an API without search refused "${typed}"; the console searched as before`);
    c.rec.check(searches.length === 1, `the refused search was not sent again (${searches.length} sent)`);
  } else if (answer.kind === 'server') {
    c.rec.note(`the server answered "${typed}" as search`);
  }
  return answer;
};

// The ID labels the main table lists, in order.
const listedLabels = async (c: Ctx): Promise<string[]> =>
  (await content(c.page).locator('.ant-table').first().locator('tbody tr.ant-table-row td:first-child').allInnerTexts())
    .map((text) => text.trim());
// The ID labels listed under "More matches beyond this page", if shown.
const beyondLabels = async (c: Ctx): Promise<string[]> =>
  (await content(c.page).locator('xpath=//h5[starts-with(normalize-space(.), "More matches beyond this page")]/..')
    .locator('tbody tr.ant-table-row td:first-child').allInnerTexts())
    .map((text) => text.trim());
const timesListed = (labels: string[], label: string) => labels.filter((shown) => shown === label).length;

// The cursor of the page after `row`, as the admin list encodes one:
// `v1:<created_at in Unix nanoseconds>:<id>`, base64url without padding.
const cursorAfter = (row: { id: string; created_at: string }): string => {
  const [, seconds, fraction = ''] = /^(.*?:\d\d)(?:\.(\d+))?Z$/.exec(row.created_at) ?? [];
  const nanos = BigInt(Date.parse(`${seconds}Z`) / 1000) * 1_000_000_000n + BigInt(fraction.padEnd(9, '0').slice(0, 9) || '0');
  return Buffer.from(`v1:${nanos}:${row.id}`).toString('base64url');
};

// Plays a list whose first page holds one sandbox, so the rest are beyond
// it: the list's first page (no `prefix` or `search`) is cut to the first
// sandbox `keep` accepts, with the cursor after it. With `refuseSearch` it
// also plays an API without `search`, answering any request carrying it
// with that API's 400. The `prefix` and `search` requests reach the server.
const withOneRowFirstPage = async (
  c: Ctx,
  { keep = () => true, refuseSearch = false }: { keep?: (row: { id: string }) => boolean; refuseSearch?: boolean },
  run: () => Promise<void>,
) => {
  const handler = async (route: Route) => {
    const url = new URL(route.request().url());
    if (!isListRequest(url)) return route.continue();
    if (refuseSearch && url.searchParams.has('search')) {
      return route.fulfill({
        status: 400,
        contentType: 'application/json',
        headers: { [SYNTHETIC_HEADER]: '1' },
        body: JSON.stringify({ error: UNKNOWN_SEARCH }),
      });
    }
    if (url.searchParams.get('cursor') !== '' || url.searchParams.has('prefix') || url.searchParams.has('search')) return route.continue();
    const res = await fetchWithoutRedirect(route);
    if (!res) return;
    const body = await res.json().catch(() => null);
    const rows = (body?.sandboxes ?? []).filter((row: { id: string; resource_type?: string }) => row.resource_type === 'sandbox' && keep(row));
    if (!rows.length) return route.fulfill({ response: res });
    return route.fulfill({
      response: res,
      headers: { ...res.headers(), [SYNTHETIC_HEADER]: '1' },
      json: { sandboxes: [rows[0]], next_cursor: cursorAfter(rows[0]) },
    });
  };
  await c.page.route('**/admin/api/v1/sandboxes?*', handler);
  try {
    await run();
  } finally {
    await c.page.unroute('**/admin/api/v1/sandboxes?*', handler);
  }
};

// The ID column's label of a row on the page: matched within the page as on
// `main`, and, as the end of an ID, by the server search, which finds the
// same sandbox, listed once.
step('sandboxes-search-label', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'running');
  if (!id) return c.rec.check(false, 'the seed has a running sandbox');
  await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
  const label = shortLabel(id);
  // The match within the page lists exactly the rows of this page that
  // carry the label.
  const onPage = (await listedLabels(c)).filter((shown) => shown.includes(label));
  await typeSearch(c, label);
  const labels = await listedLabels(c);
  const beyond = await beyondLabels(c);
  c.rec.check(JSON.stringify(labels) === JSON.stringify(onPage), `the page lists its rows that carry the label, as on main (${labels.join(', ')}; expected ${onPage.join(', ')})`);
  c.rec.check(timesListed([...labels, ...beyond], label) === 1, `the sandbox is listed once (page: ${labels.join(', ')}; beyond: ${beyond.join(', ')})`);
});

// A label beyond the page: the first page holds one other sandbox. The
// server search finds it by the end of its ID and lists it after the page,
// once; an API without `search` finds it nowhere, as on `main`.
step('sandboxes-search-label-beyond-page', ['sandboxes'], async (c) => {
  const running = sandboxId(c, 'running');
  const paused = sandboxId(c, 'paused');
  if (!running || !paused) return c.rec.check(false, 'the seed has a running and a paused sandbox');
  await withOneRowFirstPage(c, {}, async () => {
    await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2500);
    const page1 = await listedLabels(c);
    const label = [shortLabel(running), shortLabel(paused)].find((candidate) => !page1.includes(candidate))!;
    const answer = await typeSearch(c, label);
    const labels = await listedLabels(c);
    const beyond = await beyondLabels(c);
    c.rec.check(!labels.includes(label), `the page, which does not hold it, does not list ${label} (${labels.join(', ')})`);
    if (answer.kind === 'server') {
      c.rec.check(timesListed(beyond, label) === 1, `the server search lists ${label} once after the page (${beyond.join(', ')})`);
    } else if (answer.kind === 'fallback') {
      c.rec.check(!beyond.includes(label), `without search, ${label} is found nowhere, as on main (${beyond.join(', ')})`);
    }
  });
});

// An ID start. Its first 8 hex digits, the fewest the server matches an ID
// start with, are matched within the page and sent as `prefix` as on
// `main`, and searched as an ID start too: the sandbox is listed once. The
// dashed first 12, an ID prefix on `main`, list it alone.
step('sandboxes-search-id-start', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'running');
  if (!id) return c.rec.check(false, 'the seed has a running sandbox');
  const uuid = id.replace(/^sbx_/, '');
  const label = shortLabel(id);
  await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
  const start = uuid.slice(0, 8);
  await typeSearch(c, start);
  const all = [...await listedLabels(c), ...await beyondLabels(c)];
  c.rec.check(timesListed(all, label) === 1, `the 8-digit ID start ${start} lists the sandbox once (${all.join(', ')})`);
  c.rec.check(new Set(all).size === all.length, 'no sandbox is listed twice');
  // Again from a fresh page: after a fallback the page sends no `search`
  // until it is reloaded.
  await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
  const dashed = uuid.slice(0, 13);
  await typeSearch(c, dashed);
  const labels = await listedLabels(c);
  const beyond = await beyondLabels(c);
  c.rec.check(labels.length === 1 && labels[0] === label && !beyond.includes(label), `the ID start ${dashed} lists that sandbox alone, once (page: ${labels.join(', ')}; beyond: ${beyond.join(', ')})`);
});

// A name goes to `prefix`, case-sensitive, as on `main`, and never to
// `search`, which matches IDs only. Typed as named, the seeded name lists its
// sandbox alone; in upper case it lists nothing. Hex digits too few for an
// ID search, such as three of a label, are matched within the page as well,
// so a row on the page whose label carries them is listed.
step('sandboxes-search-name-prefix', ['sandboxes'], async (c) => {
  const suspended = sandboxId(c, 'suspended');
  const running = sandboxId(c, 'running');
  if (!suspended || !running) return c.rec.check(false, 'the seed has a suspended and a running sandbox');
  await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
  const runningOnPage = (await listedLabels(c)).includes(shortLabel(running));
  const searched: string[] = [];
  const onRequest = (req: Request) => {
    const url = new URL(req.url());
    if (isListRequest(url) && url.searchParams.has('search')) searched.push(url.searchParams.get('search') ?? '');
  };
  const typePrefix = async (typed: string) => {
    const answered = c.page.waitForResponse((res) => {
      const url = new URL(res.url());
      return isListRequest(url) && url.searchParams.get('prefix') === typed;
    }, { timeout: 10_000 }).catch(() => null);
    await content(c.page).locator('input[placeholder*="ID"]').first().fill(typed);
    const res = await answered;
    await settle(c.page, 1500);
    c.rec.check(res?.ok() === true, `"${typed}" was sent as prefix and answered (${res ? res.status() : 'no answer'})`);
    return listedLabels(c);
  };
  c.page.on('request', onRequest);
  try {
    const name = `${c.config.prefix}-suspended`;
    let labels = await typePrefix(name);
    c.rec.check(labels.length === 1 && labels[0] === shortLabel(suspended), `searching ${name} lists it alone (${labels.join(', ')})`);
    labels = await typePrefix(name.toUpperCase());
    c.rec.check(labels.length === 0, `the name prefix is case-sensitive: ${name.toUpperCase()} lists nothing (${labels.join(', ')})`);
    const piece = shortLabel(running).slice(-3);
    labels = await typePrefix(piece);
    if (runningOnPage) {
      c.rec.check(labels.includes(shortLabel(running)), `"${piece}", too few digits for an ID search, is matched within the page (${labels.join(', ')})`);
    } else {
      c.rec.note('the running sandbox is not on the first page, so the match within the page is not checked');
    }
  } finally {
    c.page.off('request', onRequest);
  }
  c.rec.check(searched.length === 0, `no name or short text was sent as search (${searched.join(', ')})`);
});

// A hex-looking name, such as a test sandbox's `e2e-…`: it is an ID's hex
// digits too, so it is searched as an ID, but it is still found by its name,
// as on `main`. The step creates a sandbox named `e2e-` and six random hex
// digits (no prefix can be hex, so this name alone does not carry it; the
// state records it, so `clean` deletes it if this step does not). Typed, the
// name lists it once: on the page, and with the first page holding another
// sandbox, after the page, found by its `prefix`.
step('sandboxes-search-hex-name', ['sandboxes', 'flows'], async (c) => {
  const name = `e2e-${randomBytes(3).toString('hex')}`;
  await visit(c, '/dashboard/sandboxes', 3000);
  const card = c.page.locator('.ant-card', { hasText: 'Create sandbox' });
  await card.getByPlaceholder('Name').fill(name);
  const managing = seeded(c).managingOrgId;
  const response = nextPost(c.page, (p) => p.endsWith('/v1/sandboxes'));
  await card.locator('button[type=submit]').click();
  const accepted = await jsonOf(await response);
  if (typeof accepted?.sandbox_id !== 'string') return c.rec.check(false, `creating ${name} from the form is accepted`);
  const id = sbxId(accepted.sandbox_id);
  recordCreated(c.config, { kind: 'sandbox', id, name, orgId: managing });
  // The create's answer can come before the row exists.
  const deadline = Date.now() + 60_000;
  let exists = false;
  while (!exists && Date.now() < deadline) {
    exists = (await adminFetch(c.page, 'GET', `/admin/api/v1/sandboxes/${id}`)).status === 200;
    if (!exists) await c.page.waitForTimeout(2000);
  }
  if (!exists) return c.rec.check(false, `${name} appears in the list`);
  const label = shortLabel(id);
  try {
    await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
    await typeSearch(c, name);
    let all = [...await listedLabels(c), ...await beyondLabels(c)];
    c.rec.check(timesListed(all, label) === 1, `${name} is found by its name, once (${all.join(', ')})`);
    await withOneRowFirstPage(c, { keep: (row) => shortLabel(row.id) !== label }, async () => {
      await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2500);
      await typeSearch(c, name);
      const labels = await listedLabels(c);
      const beyond = await beyondLabels(c);
      all = [...labels, ...beyond];
      c.rec.check(!labels.includes(label) && timesListed(beyond, label) === 1, `beyond the page, ${name} is listed once after it, found by its prefix (page: ${labels.join(', ')}; beyond: ${beyond.join(', ')})`);
    });
  } finally {
    await visit(c, `/dashboard/sandboxes/${id}`, 2000);
    await c.page.locator('.ant-card', { hasText: 'Controls' }).getByRole('button', { name: /Delete/ }).click();
    await confirmPopover(c.page);
    await c.page.waitForURL(/\/dashboard\/sandboxes$/, { timeout: 120_000 }).catch(() => {});
    await settle(c.page, 1000);
  }
  c.rec.check(/\/dashboard\/sandboxes$/.test(c.page.url()), `${name} is deleted again`);
}, true);

// The fallback after an API refuses `search` is `main`'s search, on the
// page it was typed on. The step plays an API without `search`, with the
// first page cut to one sandbox so page 2 holds the others. It types the
// label of page 2's first row on page 2: the page stays on page 2, the row
// is listed there, and the refused search is not sent again.
step('sandboxes-search-fallback-page', ['sandboxes'], async (c) => {
  await withOneRowFirstPage(c, { refuseSearch: true }, async () => {
    await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2500);
    await content(c.page).getByRole('button', { name: /Next/ }).first().click();
    await settle(c.page, 2500);
    const page2 = await listedLabels(c);
    if (!page2.length) return c.rec.check(false, 'page 2 lists the seeded sandboxes');
    const known = page2[0];
    const answer = await typeSearch(c, known);
    c.rec.check(answer.kind === 'fallback', 'the played API refused the search');
    const labels = await listedLabels(c);
    c.rec.check(/Page 2/.test(await text(c.page)), 'the fallback stays on page 2');
    c.rec.check(labels.length === 1 && labels[0] === known, `the fallback lists ${known}, the page-2 row typed (${labels.join(', ')})`);
  });
});

// The API reports `user_email` in the same change that adds `search`, so the
// search probe tells the two versions apart: an API without `search` predates
// owner emails and the step skips; one that takes it must report the email,
// and an inconclusive probe fails. The API fetch is only the oracle for the
// browser check; the API contract is tested where the API lives. The seeded
// running sandbox is owned by the password-signed-in admin, a local user, who
// always has an email.
step('sandboxes-user-email', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'running');
  if (!id) return c.rec.check(false, 'the seed has a running sandbox');
  await visit(c, '/dashboard/sandboxes?age=all&page_size=25', 2000);
  const probe = await typeSearch(c, shortLabel(id));
  if (probe.kind === 'fallback') return c.rec.note('the API predates owner emails (it refused search); the User column check is skipped');
  if (probe.kind === 'failed') return c.rec.check(false, "the API's version is known (search answered or refused as unknown), to tell whether it reports owner emails");
  const api = await adminFetch(c.page, 'GET', `/admin/api/v1/sandboxes/${id}`);
  const userEmail = api.json?.user_email;
  const present = typeof userEmail === 'string' && userEmail.length > 0;
  c.rec.check(present, "the local owner's email is present to check the User column");
  if (!present) return;
  // Its full ID, which the list looks up wherever the sandbox is.
  await visit(c, `/dashboard/sandboxes?age=all&id=${id}`, 3000);
  const row = content(c.page).locator('tbody tr.ant-table-row', { hasText: shortLabel(id) });
  c.rec.check((await row.innerText()).includes(userEmail), "the User column shows the owner's email");
});

// The create form has no runtime control: none of its selects offers a
// runtime, open or closed.
step('sandboxes-create-form', ['sandboxes'], async (c) => {
  await visit(c, '/dashboard/sandboxes', 2000);
  const card = c.page.locator('.ant-card', { hasText: 'Create sandbox' });
  const runtimes = /Firecracker|gVisor|Cloud Hypervisor|Hyper-V/;
  c.rec.check(await card.locator('.ant-select', { hasText: runtimes }).count() === 0, 'no select of the create form shows a runtime');
  const selects = card.locator('.ant-select');
  const offered: string[] = [];
  for (let i = 0; i < await selects.count(); i += 1) {
    await selects.nth(i).click();
    await c.page.waitForTimeout(400);
    offered.push(...await c.page.locator('.ant-select-dropdown:visible .ant-select-item-option').allInnerTexts());
    await c.page.keyboard.press('Escape');
  }
  c.rec.check(offered.length > 0, `the create form's selects open (${offered.length} options)`);
  c.rec.check(!offered.some((option) => runtimes.test(option)), 'no select of the create form offers a runtime');
});

// A create in another organization needs an owner from that organization's
// users. The form's Owner select, and what it says when it has none to offer.
const createCard = (c: Ctx) => c.page.locator('.ant-card', { hasText: 'Create sandbox' });
const ownerSelect = (c: Ctx) => createCard(c).locator('.ant-form-item', { hasText: 'Owner' }).locator('.ant-select');
// Waits for the create form's owner line to match `want`, and returns it.
async function ownerLine(c: Ctx, want: RegExp): Promise<string> {
  const deadline = Date.now() + 10_000;
  let line = '';
  while (Date.now() < deadline) {
    line = await createCard(c).innerText();
    if (want.test(line)) break;
    await c.page.waitForTimeout(250);
  }
  return line;
}
// Whether the create form's Create button reaches `enabled` within a few
// seconds: it follows the form's values on the next render.
async function createEnabled(c: Ctx, enabled: boolean): Promise<boolean> {
  const create = createCard(c).locator('button[type=submit]');
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await create.isEnabled() === enabled) return true;
    await c.page.waitForTimeout(200);
  }
  return false;
}
const ownerOptions = async (c: Ctx): Promise<string[]> => {
  await ownerSelect(c).click();
  await c.page.waitForTimeout(400);
  const options = await c.page.locator('.ant-select-dropdown:visible .ant-select-item-option').allInnerTexts();
  await c.page.keyboard.press('Escape');
  return options;
};

// An organization without users, and one whose users cannot be read: the
// step answers each users read itself, and checks that the form says so,
// keeps Create disabled, and sends no create when Enter submits it. Retry
// then reads the real users. It creates nothing: it runs under --no-mutate,
// so every create POST the page sends is recorded and cut off before it
// reaches the API, even from a console whose form submits without an owner.
step('sandboxes-create-owner-states', ['sandboxes'], async (c) => {
  const acme = orgId(c, 'acme');
  const globex = orgId(c, 'globex');
  if (!acme || !globex) return c.rec.check(false, 'the seed has the Acme and Globex organizations');
  const answers = new Map([[acme, { status: 200, body: [] as unknown }], [globex, { status: 500, body: { error: 'synthetic users read failure' } as unknown }]]);
  const handler = async (route: Route) => {
    const id = new URL(route.request().url()).pathname.split('/').at(-2) ?? '';
    const answer = answers.get(id);
    if (!answer || route.request().method() !== 'GET') return route.continue();
    return route.fulfill({ status: answer.status, contentType: 'application/json', headers: { [SYNTHETIC_HEADER]: '1' }, body: JSON.stringify(answer.body) });
  };
  const creates: string[] = [];
  const createUrl = (url: URL) => url.pathname === '/admin/api/v1/sandboxes' || url.pathname === '/api/v1/sandboxes';
  // Aborted, so the recorder takes it as cut off by the step.
  const cutCreate = (route: Route) => {
    if (route.request().method() !== 'POST') return route.fallback();
    creates.push(route.request().url());
    return route.abort('aborted');
  };
  await c.page.route('**/admin/api/v1/orgs/*/users', handler);
  await c.page.route(createUrl, cutCreate);
  try {
    await visit(c, '/dashboard/sandboxes', 2000);
    const card = createCard(c);
    const create = card.locator('button[type=submit]');
    c.rec.check(await ownerSelect(c).count() === 0, 'the own organization has no Owner field');
    c.rec.note(`SYNTHETIC: the users of ${orgName(c, 'Acme')} are answered as none, those of ${orgName(c, 'Globex')} as a 500`);
    await pickSelect(c.page, card.locator('.ant-select').first(), orgName(c, 'Acme'));
    c.rec.check(/create a user first/.test(await ownerLine(c, /create a user first/)), 'an organization without users says a user must be created first');
    c.rec.check(await create.isDisabled(), 'Create is disabled for an organization without users');
    await card.getByPlaceholder('Name').fill(`${c.runTag}-no-owner`);
    await card.getByPlaceholder('Name').press('Enter');
    await pickSelect(c.page, card.locator('.ant-select').first(), orgName(c, 'Globex'));
    c.rec.check(/Could not load/.test(await ownerLine(c, /Could not load/)), 'a failed users read says so');
    c.rec.check(await create.isDisabled(), 'Create is disabled after a failed users read');
    await card.getByPlaceholder('Name').press('Enter');
    await c.page.waitForTimeout(1000);
    await c.page.screenshot({ path: `${c.rec.outDir}/sandboxes-create-owner-failed.png` }).catch(() => {});
    if (creates.length) c.rec.note(`create attempts cut off before the API: ${creates.join(', ')}`);
    c.rec.check(creates.length === 0, `no create is sent without an owner (${creates.length} sent)`);
    answers.clear();
    await card.getByRole('button', { name: 'Retry' }).click();
    c.rec.check(/Choose the user who owns/.test(await ownerLine(c, /Choose the user who owns/)), 'Retry reads the users and asks for an owner');
    const options = await ownerOptions(c);
    c.rec.check(options.some((option) => option.includes(email(c, 'globex-dev'))), `Retry offers the organization's users (${options.join(', ')})`);
    c.rec.check(await create.isDisabled(), 'Create stays disabled until an owner is chosen');
  } finally {
    await c.page.unroute(createUrl, cutCreate);
    await c.page.unroute('**/admin/api/v1/orgs/*/users', handler);
  }
});

// The Datadog logs button shows only where SPACE_UI_DATADOG_LOGS_URL is set on
// the console; the loop reads whether it is set from its own environment, so
// run it with the console's value. Unset, the link must be absent; set, it must
// be shown and link to an HTTPS URL that queries the sandbox.
step('sandbox-detail-datadog', ['sandboxes'], async (c) => {
  const id = sandboxId(c, 'running');
  if (!id) return c.rec.check(false, 'the seed has a running sandbox');
  await visit(c, `/dashboard/sandboxes/${id}`, 3000);
  const logs = c.page.getByRole('link', { name: 'Datadog logs' });
  const shown = await logs.count() > 0;
  if (!c.config.datadogLogsConfigured) return c.rec.check(!shown, 'no Datadog logs link, as SPACE_UI_DATADOG_LOGS_URL is unset');
  c.rec.check(shown, 'the Datadog logs link is shown, as SPACE_UI_DATADOG_LOGS_URL is set');
  if (!shown) return;
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

// A create in a tenant organization through the form: Create waits for an
// owner, choosing another organization clears the owner chosen for the first,
// and the sandbox is owned by the user chosen, Acme Developer, not by Acme
// Admin, the organization's oldest user. The step deletes the sandbox again.
step('sandbox-create-other-org-owner', ['sandboxes', 'flows'], async (c) => {
  const acme = orgId(c, 'acme');
  const owner = seeded(c).users[email(c, 'acme-dev')];
  const oldest = seeded(c).users[email(c, 'acme-admin')];
  if (!acme || !owner || !oldest) return c.rec.check(false, 'the seed has the Acme organization, Acme Admin and Acme Developer');
  await visit(c, '/dashboard/sandboxes', 3000);
  const card = createCard(c);
  const create = card.locator('button[type=submit]');
  const name = `${c.runTag}-owner`;
  await card.getByPlaceholder('Name').fill(name);
  await pickSelect(c.page, card.locator('.ant-select').first(), orgName(c, 'Acme'));
  await ownerLine(c, /Choose the user who owns/);
  c.rec.check(await ownerSelect(c).count() === 1, 'another organization shows an Owner field');
  c.rec.check(await create.isDisabled(), 'Create is disabled until an owner is chosen');
  // Every Acme user's email starts with this; the Owner select of another
  // organization must offer none of them.
  const acmeLocal = `${c.config.prefix}-acme-`;
  const acmeOptions = await ownerOptions(c);
  c.rec.check([email(c, 'acme-admin'), email(c, 'acme-dev')].every((e) => acmeOptions.some((o) => o.includes(e) && o.startsWith(acmeLocal))),
    `the Owner select offers Acme Admin and Acme Developer (${acmeOptions.join(', ')})`);
  await pickSelect(c.page, ownerSelect(c), email(c, 'acme-dev'));
  c.rec.check(await createEnabled(c, true), 'choosing an owner enables Create');

  await pickSelect(c.page, card.locator('.ant-select').first(), orgName(c, 'Globex Research'));
  await ownerLine(c, /Choose the user who owns/);
  c.rec.check(!(await ownerSelect(c).innerText()).includes(email(c, 'acme-dev')), 'choosing another organization clears the owner');
  c.rec.check(await createEnabled(c, false), 'Create is disabled again for the other organization');
  const options = await ownerOptions(c);
  c.rec.check(options.some((o) => o.includes(email(c, 'research-dev'))) && !options.some((o) => o.includes(acmeLocal)),
    `the Owner select offers the other organization's users only (${options.join(', ')})`);

  // Back in Acme the owner chosen there before must be gone too: without the
  // clear it would be Acme Developer again, and Create enabled.
  await pickSelect(c.page, card.locator('.ant-select').first(), orgName(c, 'Acme'));
  c.rec.check(/Choose the user who owns/.test(await ownerLine(c, /Choose the user who owns/)), 'back in Acme, the form asks for an owner again');
  c.rec.check(await createEnabled(c, false), 'back in Acme, Create is disabled until an owner is chosen again');
  await pickSelect(c.page, ownerSelect(c), email(c, 'acme-dev'));
  c.rec.check(await createEnabled(c, true), 'choosing the owner again enables Create');
  await c.page.screenshot({ path: `${c.rec.outDir}/sandbox-create-other-org-owner-chosen.png` }).catch(() => {});
  const response = nextPost(c.page, (p) => p === '/admin/api/v1/sandboxes');
  await create.click();
  const answer = await response;
  const sent = answer?.request().postDataJSON() ?? null;
  c.rec.check(sent?.org_id === acme && sent?.user_id === owner.id, `the create sends Acme and the chosen owner (${JSON.stringify({ org_id: sent?.org_id, user_id: sent?.user_id })})`);
  const accepted = await jsonOf(answer);
  if (typeof accepted?.sandbox_id !== 'string') return c.rec.check(false, `creating ${name} in Acme from the form is accepted`);
  const id = sbxId(accepted.sandbox_id);
  const createOperation = typeof accepted.operation_id === 'string' ? { id: accepted.operation_id, admin: true } : undefined;
  recordCreated(c.config, { kind: 'sandbox', id, name, orgId: acme, createOperation });
  try {
    // The create's answer can come before the row exists; the row is read
    // once its create operation has finished.
    const deadline = Date.now() + 120_000;
    let state = '';
    while (createOperation && !TERMINAL_OPERATION_STATES.includes(state) && Date.now() < deadline) {
      const timeoutMs = Math.min(15_000, deadline - Date.now());
      state = (await adminFetch(c.page, 'GET', `/admin/api/v1/operations/${createOperation.id}`, undefined, { timeoutMs })).json?.state ?? '';
      if (!TERMINAL_OPERATION_STATES.includes(state)) await c.page.waitForTimeout(Math.max(0, Math.min(1000, deadline - Date.now())));
    }
    c.rec.check(state === 'succeeded', `the create operation succeeds (${state || 'unread'})`);
    const read = await adminFetch(c.page, 'GET', `/admin/api/v1/sandboxes/${id}`, undefined, { timeoutMs: 15_000 });
    const row: { org_id?: string; user_id?: string } | null = read.status === 200 ? read.json : null;
    c.rec.check(row?.org_id === acme, `the sandbox is in Acme (${row?.org_id})`);
    c.rec.check(row?.user_id === owner.id, `the sandbox is owned by the chosen user, not the oldest (${row?.user_id}; chosen ${owner.id}, oldest ${oldest.id})`);
  } finally {
    await visit(c, `/dashboard/sandboxes/${id}`, 2000);
    await c.page.locator('.ant-card', { hasText: 'Controls' }).getByRole('button', { name: /Delete/ }).click();
    await confirmPopover(c.page);
    await c.page.waitForURL(/\/dashboard\/sandboxes$/, { timeout: 120_000 }).catch(() => {});
    await settle(c.page, 1000);
  }
  c.rec.check(/\/dashboard\/sandboxes$/.test(c.page.url()), `${name} is deleted again`);
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

// These steps open terminal sessions. The other organization's go through the
// admin route, which records an admin audit row for each, so they mutate.
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
    // Typed right after Connect. If the shell has not started yet the card
    // holds it until it has; the unit tests cover that order.
    await card.getByRole('button', { name: /Connect$/ }).click();
    await terminalRun(c, 'echo reconnected-$((11*11))', /reconnected-121/, 'a second Connect opens a new shell and runs what was typed right after it', 30_000);
    await card.getByRole('button', { name: /Disconnect$/ }).click();
  }, true);
}

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

// ------------------------------------------------------------- node drain
// The node page's Drain section and the drain actions. The flows drain an
// empty Kind worker only (see `drainCandidate`), under the drain-keep override
// the Kind terminator honours, and undrain it again; `npm run clean` does not
// know about drains, so each flow undoes its own in `finally`.

const drainCard = (page: Page) => page.locator('.ant-card', { has: page.locator('.ant-card-head-title', { hasText: /^Drain$/ }) });
const typedModal = (page: Page) => page.locator('.ant-modal:visible', { has: page.getByLabel('Type the node name to confirm') });
// A dialog is captured in the viewport once its opening animation ends: a
// full-page capture can drop a dialog that is still animating.
const shot = async (c: Ctx, name: string, { dialog = false }: { dialog?: boolean } = {}) => {
  if (dialog) await c.page.waitForTimeout(500);
  await c.page.screenshot({ path: `${c.rec.outDir}/${c.rec.stepName()}-${name}.png`, fullPage: !dialog });
};

async function adminNodes(c: Ctx): Promise<AdminNode[]> {
  const res = await adminFetch(c.page, 'GET', '/admin/api/v1/nodes?include_occupancy=true', undefined, { timeoutMs: 30_000 });
  if (res.status !== 200 || !Array.isArray(res.json)) throw new Error(`listing nodes answered ${res.status}`);
  return res.json as AdminNode[];
}

async function adminNode(c: Ctx, id: string): Promise<AdminNode | null> {
  const res = await adminFetch(c.page, 'GET', `/admin/api/v1/nodes/${id}?include_occupancy=false`, undefined, { timeoutMs: 15_000 });
  return res.status === 200 ? (res.json as AdminNode) : null;
}

// Waits until the node's drain phase is one of `want` (null: no drain).
async function waitForPhase(c: Ctx, id: string, want: (string | null)[], timeoutMs: number): Promise<string | null | undefined> {
  const deadline = Date.now() + timeoutMs;
  let phase: string | null | undefined;
  while (Date.now() < deadline) {
    phase = (await adminNode(c, id))?.drain_phase;
    if (phase !== undefined && want.includes(phase)) return phase;
    await c.page.waitForTimeout(1500);
  }
  return phase;
}

// The page also announces a refused terminate in an error toast, which the
// runner would count as a failed check; a refusal is what these steps expect,
// so its toast is checked and noted here instead.
async function expectRefusalToast(c: Ctx, title: string): Promise<void> {
  const toasts = await takeToasts(c.page);
  for (const toast of toasts) {
    if (toast.type === 'error' && toast.text === title) c.rec.note(`expected toast error: ${toast.text}`);
    else c.rec.check(toast.type !== 'error' && toast.type !== 'warning', `toast ${toast.type}: ${toast.text}`);
  }
  c.rec.check(toasts.some((t) => t.type === 'error' && t.text === title), `the refusal is announced: ${title}`);
}

// Waits until the Drain section shows `pattern`, as the page's own poll
// brings it in.
async function waitForSection(c: Ctx, pattern: RegExp, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pattern.test(await drainCard(c.page).innerText().catch(() => ''))) return true;
    await c.page.waitForTimeout(1000);
  }
  return false;
}

// Waits until the Kubernetes Node's `spec.unschedulable` reads `want`.
async function waitForUnschedulable(c: Ctx, kube: KindNodes, name: string, want: boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (kube.unschedulable(name) === want) return true;
    await c.page.waitForTimeout(1000);
  }
  return kube.unschedulable(name) === want;
}

// Cordons and uncordons the worker through the API and checks that the
// cordon shows up on the Node the kube context reads, which shows the context
// and the API are one cluster. The uncordon is sent on every path once the
// cordon has been; if it is refused or does not reach the Node, the step fails
// and the worker may stay cordoned.
async function cordonReachesNode(c: Ctx, node: AdminNode, kube: KindNodes): Promise<boolean> {
  const name = node.k8s_node_name!;
  if (node.cordoned === true || kube.unschedulable(name)) {
    c.rec.check(false, 'the worker is already cordoned');
    return false;
  }
  let reached = false;
  try {
    const cordon = await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${node.id}:cordon`);
    c.rec.note(`probe cordon answered ${cordon.status}`);
    reached = await waitForUnschedulable(c, kube, name, true, 15_000);
  } finally {
    const uncordon = await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${node.id}:uncordon`);
    c.rec.note(`probe uncordon answered ${uncordon.status}`);
  }
  c.rec.check(reached, "the API's cordon reached the worker's Node through the kube context");
  if (!reached) return false;
  const released = await waitForUnschedulable(c, kube, name, false, 15_000);
  c.rec.check(released, "the API's uncordon reached the worker's Node");
  return released;
}

// Picks a worker, proves the kube context reads the API's cluster, claims the
// drain-keep override with a token of its own, refusing a worker that already
// has one, runs `body`, then cleans up with `releaseDrainableWorker`.
// Records why when there is no worker it may drain; that is not a pass.
async function withDrainableWorker(c: Ctx, body: (node: AdminNode, kube: KindNodes) => Promise<void>): Promise<void> {
  const picked = drainCandidate(await adminNodes(c));
  if (!('node' in picked)) {
    c.rec.check(false, `a Kind worker this step may drain exists: ${picked.reason}`);
    return;
  }
  const node = picked.node;
  const name = node.k8s_node_name!;
  const kube = new KindNodes(c.config.kubeContext);
  kube.requireLocalNode(name);
  if (!(await cordonReachesNode(c, node, kube))) return;
  const claim = kube.claimDrainKeep(name);
  if ('held' in claim) {
    c.rec.check(false, `${name} already carries ${DRAIN_KEEP_ANNOTATION}=${claim.held}; it is left as it is and nothing is drained`);
    return;
  }
  c.rec.note(`draining worker ${name} (${node.id}), which holds no sandboxes, under ${DRAIN_KEEP_ANNOTATION}=${claim.token}`);
  try {
    await body(node, kube);
  } finally {
    await releaseDrainableWorker(c, node, kube, claim.token);
  }
}

// Undrains the worker unless the API shows it with no drain phase, then
// removes the override once it does. It runs however the body ended, so it
// records each error as a failed check and goes on where that is safe, and
// never throws: the body's own error stays the step's error.
async function releaseDrainableWorker(c: Ctx, node: AdminNode, kube: KindNodes, token: string): Promise<void> {
  const name = node.k8s_node_name!;
  const failed = (what: string, error: unknown) =>
    c.rec.check(false, `cleanup: ${what} failed: ${error instanceof Error ? error.message.split('\n')[0] : String(error)}`);
  let phase: string | null | undefined;
  try {
    phase = (await adminNode(c, node.id))?.drain_phase;
  } catch (error) {
    failed('reading the node', error);
  }
  // A phase that could not be read may still be a drain.
  if (phase === undefined || phase === 'draining' || phase === 'drained') {
    try {
      const undrain = await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${node.id}:undrain`);
      c.rec.note(`cleanup undrain answered ${undrain.status}`);
    } catch (error) {
      failed('the undrain', error);
    }
  }
  let left: string | null | undefined;
  try {
    left = await waitForPhase(c, node.id, [null], 30_000);
  } catch (error) {
    failed('reading the drain phase', error);
  }
  c.rec.check(left === null, 'the worker is left with no drain phase');
  if (left !== null) {
    c.rec.note(`the drain-keep override ${token} stays on ${name} because ${left === undefined ? 'the API did not report the node' : `the node is still ${left}`}`);
    return;
  }
  try {
    c.rec.check(kube.releaseDrainKeep(name, token), `the drain-keep override on ${name} still carried this claim's token and was removed`);
  } catch (error) {
    failed('removing the drain-keep override', error);
  }
}

step('nodes-heartbeat', ['nodes'], async (c) => {
  await visit(c, '/dashboard/nodes', 4000);
  const tags = content(c.page).locator('td .ant-tag', { hasText: / · seen / });
  const shown = await tags.count();
  c.rec.check(shown > 0, 'the Status column shows a heartbeat tag for each worker and builder');
  const notFresh = await content(c.page).locator('td .ant-tag', { hasText: /^(stale|no status) · seen / }).count();
  c.rec.note(`${notFresh} of ${shown} nodes read stale or without a status snapshot`);
  await tags.first().hover();
  c.rec.check(/stale after: |no status snapshot received/.test(await c.page.locator('.ant-tooltip:visible').innerText().catch(() => '')), "the heartbeat tooltip names the node's stale_after");
  const filter = content(c.page).locator('.ant-select', { has: c.page.getByLabel('Heartbeat') });
  c.rec.check(await filter.count() === 1, 'the list has a heartbeat filter');
  await pickSelect(c.page, filter, 'Stale or no heartbeat');
  await settle(c.page, 800);
  const filtered = await content(c.page).locator('td .ant-tag', { hasText: / · seen / }).count();
  const filteredStale = await content(c.page).locator('td .ant-tag', { hasText: /^(stale|no status) · seen / }).count();
  c.rec.check(filtered === filteredStale && filtered === notFresh, `the stale filter keeps exactly the stale nodes (${filtered} of ${notFresh})`);
  await shot(c, 'stale-filter');
  await pickSelect(c.page, filter, 'Fresh heartbeat');
  await settle(c.page, 800);
  c.rec.check(await content(c.page).locator('td .ant-tag', { hasText: /^(stale|no status) · seen / }).count() === 0, 'the fresh filter shows no stale node');
  await pickSelect(c.page, filter, 'Any heartbeat');
  await settle(c.page, 800);
});

step('node-drain-section', ['nodes'], async (c) => {
  const workers = (await adminNodes(c)).filter((n) => n.role === 'worker');
  if (!workers.length) return c.rec.check(false, 'a worker node exists');
  const node = workers.sort((a, b) => (b.sandboxes_active ?? 0) - (a.sandboxes_active ?? 0))[0];
  await visit(c, `/dashboard/nodes/${node.id}`, 4000);
  const card = drainCard(c.page);
  c.rec.check(await card.count() === 1, "the node page has a Drain section");
  const t = (await card.innerText()).replace(/\s+/g, ' ');
  for (const label of ['Phase', 'Mode', 'Started', 'Last error', 'Heartbeat', 'Kubernetes Node', 'Holding the node', 'History']) {
    c.rec.check(t.includes(label), `the Drain section shows ${label}`);
  }
  c.rec.check(/\d+(s|m \d+s|h \d+m|d \d+h) ago|never/.test(t), 'the heartbeat age is shown');
  c.rec.check(/present|not reported/.test(t), 'whether the Kubernetes Node is present is shown');
  c.rec.check(/admin audit log/.test(t), 'the history says where it comes from and what it leaves out');
  c.rec.check(/updated \S+/.test(t), 'the section says when it was last read');
});

// The holds read for every worker, the most held first: a node in a drain
// phase leads, since that is where an operator reads the panel.
async function workersByHolds(c: Ctx): Promise<{ node: AdminNode; holds: Holds }[]> {
  const workers = (await adminNodes(c)).filter((n) => n.role === 'worker');
  const read: { node: AdminNode; holds: Holds }[] = [];
  for (const node of workers) {
    const res = await adminFetch(c.page, 'GET', `/admin/api/v1/nodes/${node.id}/holds`, undefined, { timeoutMs: 30_000 });
    if (res.status !== 200) throw new Error(`reading what holds ${node.id} answered ${res.status}`);
    read.push({ node, holds: res.json as Holds });
  }
  return read.sort((a, b) => Number(b.node.drain_phase !== null) - Number(a.node.drain_phase !== null) || b.holds.total - a.holds.total);
}

interface Holds {
  residency_evidence: string;
  total: number;
  term_counts: { resident: number; drain_blocker: number; active_resident: number };
  holds: { sandbox_id: string; reason: string; counted_by: { terminate: boolean; drain: boolean; drain_with_termination_blocking_gate: boolean } }[];
}

// The panel's Counted by label for a row: `holdGatesLabel` in the UI's
// `lib/node-drain.ts`.
function holdGatesLabel({ terminate, drain, drain_with_termination_blocking_gate: gated }: Holds['holds'][number]['counted_by']): string {
  if (drain && terminate) return 'drain and terminate';
  if (drain) return 'drain';
  if (terminate && gated) return 'terminate; drain only with the termination-blocking gate on';
  if (terminate) return 'terminate';
  return 'neither';
}

const holdsCard = (page: Page) => page.locator('#node-holds');
const historyPanel = (page: Page) => page.locator('#node-sandboxes');

async function openHistory(c: Ctx): Promise<void> {
  const header = historyPanel(c.page).locator('.ant-collapse-header');
  if ((await header.getAttribute('aria-expanded')) !== 'true') await header.click();
  await historyPanel(c.page).locator('.ant-collapse-panel-active').waitFor({ timeout: 5000 });
  await settle(c.page, 800);
}

// The panel lists every row the holds read returns, each linked, with its
// reason in a sentence and the gates that count it, grouped under the five
// headings; the header says which residency evidence the gates use. The rows
// expected are the ones in the holds answer the page itself received.
step('node-holds-panel', ['nodes'], async (c) => {
  const [first] = await workersByHolds(c);
  if (!first) return c.rec.check(false, 'a worker node exists');
  const { node } = first;
  const pageRead = c.page.waitForResponse((res) => new URL(res.url()).pathname.endsWith(`/nodes/${node.id}/holds`), { timeout: 30_000 }).catch(() => null);
  await visit(c, `/dashboard/nodes/${node.id}`, 4000);
  const holds = (await jsonOf(await pageRead)) as Holds | null;
  if (!holds) return c.rec.check(false, 'the node page read what holds the node');
  c.rec.note(`node ${node.name ?? node.id} (${node.drain_phase ?? 'not draining'}): ${holds.total} holds, ${JSON.stringify(holds.term_counts)}`);
  const card = holdsCard(c.page);
  c.rec.check(await card.count() === 1, 'the node page has a "What holds this node" panel');
  const panel = (await card.innerText()).replace(/\s+/g, ' ');
  const mode = holds.residency_evidence === 'node_census_aware' ? 'node census' : 'rows only';
  c.rec.check(panel.includes(`residency: ${mode}`), `the header shows the residency evidence once (${mode})`);
  await card.locator('.ant-card-extra .ant-tag', { hasText: 'residency:' }).hover();
  c.rec.check(/census absent/.test(await c.page.locator('.ant-tooltip:visible').innerText().catch(() => '')), 'its tooltip says what the mode means for rows the node does not report');
  // The page's `heldByTerms`: rows no term counts do not hold the node.
  const counted = holds.term_counts.resident + holds.term_counts.drain_blocker + holds.term_counts.active_resident > 0;
  const summary = (await drainCard(c.page).innerText()).replace(/\s+/g, ' ');
  if (!counted) {
    c.rec.check(/Nothing holds this node/.test(panel) && /Nothing holds this node/.test(summary), 'a node no term counts reads "Nothing holds this node" in the panel and the Drain section');
  }
  if (!holds.total) {
    c.rec.note('no worker is held: the groups are not exercised');
    return;
  }
  const order = ['Needs you', 'Handled automatically', 'Waiting on users', 'Blocks terminate', 'Listed, not counted'];
  const shown = (await card.locator('[data-hold-group] h5').allInnerTexts()).map((t) => t.trim());
  c.rec.check(shown.length > 0 && shown.every((t, i) => order.indexOf(t) >= 0 && (i === 0 || order.indexOf(t) > order.indexOf(shown[i - 1]))), `the groups come in order (${shown.join(', ')})`);
  const rows = card.locator('[data-hold-group] tbody tr.ant-table-row');
  c.rec.check(await rows.count() === holds.holds.length, `every listed hold is a row (${await rows.count()} of ${holds.holds.length})`);
  for (const hold of holds.holds) {
    // The link carries the sandbox's public ID, which ends in the UUID's hex.
    const row = rows.filter({ has: c.page.locator(`a[href*="${hold.sandbox_id.slice(-8).toLowerCase()}"]`) }).first();
    const cells = (await row.innerText().catch(() => '')).replace(/\s+/g, ' ');
    const gates = holdGatesLabel(hold.counted_by);
    c.rec.check(cells.length > 40 && cells.includes(gates), `${hold.reason} row reads as a sentence counted by ${gates}`);
    const why = (await row.locator('td:nth-child(3)').innerText().catch(() => '')).trim();
    c.rec.check(why.length > 0 && why.endsWith('.'), `${hold.reason} row's Why is a sentence (${why.slice(0, 60)})`);
    const countedBy = (await row.locator('td').last().innerText().catch(() => '')).trim();
    c.rec.check(countedBy === gates, `${hold.reason} row's Counted by is exactly "${gates}" (${countedBy})`);
  }
  if (counted) c.rec.check(/Holding the node .*what holds it/.test(summary), 'the Drain section summarizes the groups and links to the panel');
  const needsUpload = holds.holds.some((h) => h.reason === 'local_only_snapshot');
  const uploadOffered = await card.getByRole('button', { name: 'Upload local snapshots' }).count();
  const uploadOpen = node.drain_phase === 'drained' || node.drain_phase === 'terminating' || (node.drain_phase === 'draining' && node.drain_mode === 'forced');
  c.rec.check(uploadOffered === Number(needsUpload && uploadOpen), `Upload local snapshots sits by the local-only rows exactly when the API accepts it (${uploadOffered})`);
  await card.screenshot({ path: `${c.rec.outDir}/${c.rec.stepName()}-panel.png` }).catch(() => {});
  // The href, not a click: a held row need not have a sandbox the public API
  // can read (a census-absent row has nothing behind it).
  const href = await rows.first().locator('a[href^="/dashboard/sandboxes/sbx_"]').getAttribute('href');
  c.rec.check(href !== null && holds.holds.some((h) => href.endsWith(h.sandbox_id)), "a held sandbox links to the sandbox's page");
});

// The collapsed History lists every row naming the node, newest change
// first, filtered by lifecycle, and by "Failed during this drain" while the
// node drains; each filter is checked against the request the page sends.
step('node-history-filters', ['nodes'], async (c) => {
  const [first] = await workersByHolds(c);
  if (!first) return c.rec.check(false, 'a worker node exists');
  const { node } = first;
  await visit(c, `/dashboard/nodes/${node.id}`, 4000);
  c.rec.check(await historyPanel(c.page).locator('.ant-collapse-panel-active').count() === 0, 'the History starts collapsed');
  const historyRead = (want: (params: URLSearchParams) => boolean) => c.page.waitForResponse((res) => {
    const url = new URL(res.url());
    return url.pathname.endsWith(`/nodes/${node.id}/sandboxes`) && want(url.searchParams);
  }, { timeout: 20_000 }).catch(() => null);
  const newestFirst = (rows: { lifecycle_changed_at: string }[]) => rows.every((r, i) => i === 0 || Date.parse(rows[i - 1].lifecycle_changed_at) >= Date.parse(r.lifecycle_changed_at));
  const bareId = (id: string) => id.replace(/^sbx_/, '').replaceAll('-', '').toLowerCase();
  // The History's rows, in order, are the answer the page just received, and
  // read newest change first through their ids.
  const showsAnswer = async (answer: any, what: string) => {
    await settle(c.page, 800);
    const shown = (await historyPanel(c.page).locator('tbody tr.ant-table-row td:first-child code').allInnerTexts()).map((t) => bareId(t.trim()));
    const rows: { id: string; lifecycle_changed_at: string }[] = Array.isArray(answer?.sandboxes) ? answer.sandboxes : [];
    c.rec.check(answer !== null && shown.join(',') === rows.map((r) => bareId(r.id)).join(','), `${what}: the History shows the answer's sandboxes in its order (${shown.length} of ${rows.length})`);
    const byId = new Map(rows.map((r) => [bareId(r.id), r]));
    const ordered = shown.map((id) => byId.get(id));
    c.rec.check(ordered.every((r) => r !== undefined) && newestFirst(ordered as typeof rows), `${what}: the shown rows run newest change first`);
  };
  await openHistory(c);
  const all = historyRead((p) => p.get('order') === 'lifecycle_changed_desc');
  await c.page.getByRole('button', { name: 'Reload' }).last().click();
  const allJson = await jsonOf(await all);
  c.rec.check(Array.isArray(allJson?.sandboxes) && newestFirst(allJson.sandboxes), `the history is read newest change first (${allJson?.sandboxes?.length ?? 'no'} rows)`);
  await showsAnswer(allJson, 'after Reload');

  const filter = historyPanel(c.page).locator('.ant-select', { has: c.page.getByLabel('Lifecycle filter') });
  const failed = historyRead((p) => p.getAll('lifecycle').join(',') === 'failed');
  // The option list is virtual: type to bring `failed` into it.
  await filter.click();
  await c.page.keyboard.type('failed');
  await c.page.locator('.ant-select-dropdown:visible .ant-select-item-option', { hasText: /^failed$/ }).first().click({ timeout: 5000 });
  await c.page.keyboard.press('Escape');
  const failedJson = await jsonOf(await failed);
  await settle(c.page, 800);
  const tags = await historyPanel(c.page).locator('tbody tr.ant-table-row td:nth-child(2)').allInnerTexts();
  c.rec.check(failedJson !== null && tags.length === failedJson.sandboxes.length && tags.every((t) => t.trim() === 'failed'), `the lifecycle filter keeps only failed rows (${tags.length} of ${failedJson?.sandboxes?.length ?? 'no'})`);
  await shot(c, 'failed-filter');
  await filter.locator('.ant-select-clear').click({ force: true }).catch(() => {});

  const toggle = historyPanel(c.page).getByLabel('Failed during this drain');
  if (!node.drain_started_at) {
    c.rec.check(await toggle.isDisabled(), '"Failed during this drain" is off while the node is not draining');
    return;
  }
  const during = historyRead((p) => p.get('changed_since') === node.drain_started_at && p.getAll('lifecycle').join(',') === 'failed');
  await toggle.click();
  const duringJson = await jsonOf(await during);
  c.rec.check(duringJson !== null, 'the toggle asks for failed rows changed since the drain started');
  const started = Date.parse(node.drain_started_at);
  c.rec.check(Array.isArray(duringJson?.sandboxes) && duringJson.sandboxes.every((r: any) => r.lifecycle === 'failed' && Date.parse(r.lifecycle_changed_at) >= started), `every row failed during the drain (${duringJson?.sandboxes?.length ?? 0})`);
  c.rec.check(await filter.locator('.ant-select-disabled').count() + Number(await filter.evaluate((el) => el.classList.contains('ant-select-disabled'))) > 0, 'the lifecycle filter is replaced while the toggle is on');
  await showsAnswer(duringJson, 'with the toggle on');
  await shot(c, 'failed-during-drain');
  await toggle.click();
});

step('node-forced-drain-undrain', ['nodes', 'flows'], async (c) => {
  const skip = v1DrainSkip(drainV2On(c));
  if (skip) return c.rec.note(skip);
  await withDrainableWorker(c, async (node, kube) => {
    await visit(c, `/dashboard/nodes/${node.id}`, 3000);
    const phrase = node.name ?? node.id;
    await content(c.page).getByRole('button', { name: 'Forced drain' }).click();
    const dialog = typedModal(c.page);
    await dialog.waitFor({ timeout: 5000 });
    const ok = dialog.getByRole('button', { name: 'Forced drain' });
    c.rec.check(/evacuates its sandboxes now/.test(await dialog.innerText()), 'the confirmation says the drain evacuates sandboxes now');
    c.rec.check(await ok.isDisabled(), 'Forced drain is disabled before the name is typed');
    await dialog.getByLabel('Type the node name to confirm').fill(phrase.slice(0, -1));
    c.rec.check(await ok.isDisabled(), 'a partial name does not enable Forced drain');
    await dialog.getByLabel('Type the node name to confirm').fill(phrase);
    c.rec.check(await ok.isEnabled(), 'the exact name enables Forced drain');
    await shot(c, 'confirmation', { dialog: true });
    const sent = nextPost(c.page, (p) => p.endsWith(`/nodes/${node.id}:drain`));
    await ok.click();
    const answer = await sent;
    c.rec.check(answer?.status() === 200, `the forced drain is accepted (${answer?.status()})`);
    c.rec.check(JSON.stringify(answer?.request().postDataJSON()) === '{"mode":"forced"}', 'the request asks for mode forced');
    // An empty worker can pass through draining faster than the page's poll.
    c.rec.check(await waitForSection(c, /Phase\s+(draining|drained)/, 15_000), 'the Drain section shows the drain (draining, or drained already)');
    c.rec.check(/Mode forced/.test((await drainCard(c.page).innerText()).replace(/\s+/g, ' ')), 'the Drain section shows mode forced');
    c.rec.check(!/Engine/.test(await drainCard(c.page).innerText()) && !(await c.page.locator('#drain-progress').count()), 'a v1 forced drain shows no engine and no Drain progress panel');
    c.rec.check(kube.unschedulable(node.k8s_node_name!), 'the Kubernetes Node is cordoned');
    await shot(c, 'draining');
    c.rec.check(await waitForPhase(c, node.id, ['drained'], 120_000) === 'drained', 'the empty worker reaches drained');
    c.rec.check(await waitForSection(c, /Phase\s+drained/, 15_000), "the page's poll shows drained without a reload");
    c.rec.check(/drain/.test(await drainCard(c.page).locator('.ant-table-tbody').innerText()), 'the history lists the drain');
    await shot(c, 'drained');
    await content(c.page).getByRole('button', { name: 'Undrain' }).click();
    const confirm = c.page.locator('.ant-modal-confirm:visible');
    await confirm.waitFor({ timeout: 5000 });
    const undrain = nextPost(c.page, (p) => p.endsWith(`/nodes/${node.id}:undrain`));
    await confirm.getByRole('button', { name: 'Undrain' }).click();
    c.rec.check((await undrain)?.status() === 200, 'the undrain is accepted');
    c.rec.check(await waitForSection(c, /Phase\s+not draining/, 15_000), 'the Drain section shows not draining');
    c.rec.check(!kube.unschedulable(node.k8s_node_name!), 'the Kubernetes Node is schedulable again');
    await waitForSection(c, /undrain/, 20_000);
    c.rec.check(/undrain/.test(await drainCard(c.page).locator('.ant-table-tbody').innerText()), 'the history lists the undrain');
  });
}, true);

step('node-terminate-refused', ['nodes', 'flows'], async (c) => {
  const skip = v1DrainSkip(drainV2On(c));
  if (skip) return c.rec.note(skip);
  await withDrainableWorker(c, async (node) => {
    const phrase = node.name ?? node.id;
    const drained = await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${node.id}:drain`, { mode: 'forced' });
    c.rec.check(drained.status === 200, `the worker is drained through the API (${drained.status})`);
    if (await waitForPhase(c, node.id, ['drained'], 120_000) !== 'drained') return c.rec.check(false, 'the worker reaches drained');
    await visit(c, `/dashboard/nodes/${node.id}`, 3000);
    const terminateUrl = (url: URL) => url.pathname.endsWith(`/nodes/${node.id}:terminate`);

    // 1. The occupancy refusal, answered by the step: nothing is sent. The
    // body has the keys `terminate_conflict_body` emits.
    const synthetic = async (route: Route) => route.fulfill({
      status: 409,
      contentType: 'application/json',
      headers: { [SYNTHETIC_HEADER]: '1' },
      body: JSON.stringify({
        error: 'node still holds sandboxes; terminating would destroy resident sandboxes or strand local-disk snapshots',
        sandboxes_active: 2, sandboxes_drain_blockers: 1, sandboxes_local_snapshots: 1,
      }),
    });
    await c.page.route(terminateUrl, synthetic);
    try {
      await content(c.page).getByRole('button', { name: 'Terminate' }).click();
      const dialog = typedModal(c.page);
      await dialog.waitFor({ timeout: 5000 });
      c.rec.check(/cannot be undrained/.test(await dialog.innerText()), 'the confirmation says a terminating node cannot be undrained');
      c.rec.check(await dialog.getByRole('button', { name: 'Terminate' }).isDisabled(), 'Terminate is disabled before the name is typed');
      await dialog.getByLabel('Type the node name to confirm').fill(phrase);
      await shot(c, 'confirmation', { dialog: true });
      await dialog.getByRole('button', { name: 'Terminate' }).click();
      const alert = drainCard(c.page).locator('.ant-alert-error');
      await alert.waitFor({ timeout: 10_000 });
      const reasons = await alert.locator('li').allInnerTexts();
      c.rec.note('SYNTHETIC: the occupancy refusal is a 409 the step answers itself, with the keys the API emits; no terminate request was sent');
      c.rec.check(reasons.length === 2 && /2 sandboxes still occupy/.test(reasons[0]) && /1 sandbox is held/.test(reasons[1]), 'an occupancy refusal is listed as readable reasons');
      c.rec.check(!/[{}"]/.test(await alert.innerText()), 'the refusal shows no raw JSON');
      await expectRefusalToast(c, 'Terminate refused: the node still holds sandboxes');
      // The confirmation closes once the page has reloaded the node.
      await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
      await shot(c, 'occupancy-refusal');
    } finally {
      await c.page.unroute(terminateUrl, synthetic);
    }

    // 2. The phase gate, from the API: the node is undrained while the
    // confirmation is open, so the terminate the page then sends is refused.
    // A guard aborts the request unless the API reports the node undrained
    // just before the request is forwarded. The API's answer is handed to the
    // page as it came, marked so the recorder does not count the 409 this
    // step expects; the step checks its status.
    let refusedStatus: number | null = null;
    const guard = async (route: Route) => {
      const phase = (await adminNode(c, node.id))?.drain_phase;
      if (phase !== null) {
        c.rec.check(false, `the terminate was not sent: the node is still ${phase}`);
        return route.abort();
      }
      const res = await fetchWithoutRedirect(route);
      if (!res) return;
      refusedStatus = res.status();
      return route.fulfill({ response: res, headers: { ...res.headers(), [SYNTHETIC_HEADER]: '1' } });
    };
    await c.page.route(terminateUrl, guard);
    try {
      await content(c.page).getByRole('button', { name: 'Terminate' }).click();
      const dialog = typedModal(c.page);
      await dialog.waitFor({ timeout: 5000 });
      const undrained = await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${node.id}:undrain`);
      c.rec.check(undrained.status === 200, 'the node is undrained while the confirmation is open');
      if (await waitForPhase(c, node.id, [null], 30_000) !== null) return c.rec.check(false, 'the undrain cleared the phase');
      await dialog.getByLabel('Type the node name to confirm').fill(phrase);
      await dialog.getByRole('button', { name: 'Terminate' }).click();
      const alert = drainCard(c.page).locator('.ant-alert-error', { hasText: 'not drained' });
      await alert.waitFor({ timeout: 15_000 });
      c.rec.check(refusedStatus === 409, `the API refuses the terminate (${refusedStatus})`);
      const reasons = await alert.locator('li').allInnerTexts();
      c.rec.check(reasons.includes('The node is not draining.') && reasons.some((r) => /accepted only from: drained/.test(r)), 'the phase refusal is listed as readable reasons');
      await expectRefusalToast(c, 'Terminate refused: the node is not drained');
      // The confirmation closes once the page has reloaded the node.
      await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
      await shot(c, 'phase-refusal');
    } finally {
      await c.page.unroute(terminateUrl, guard);
    }
  });
}, true);

// A v1 gentle drain, as on main: Drain sends `mode: gentle` and no engine,
// and the page shows no v2 drain.
step('node-gentle-drain-undrain', ['nodes', 'flows'], async (c) => {
  const skip = v1DrainSkip(drainV2On(c));
  if (skip) return c.rec.note(skip);
  await withDrainableWorker(c, async (node, kube) => {
    await visit(c, `/dashboard/nodes/${node.id}`, 3000);
    await exactButton(c.page, 'Drain').click();
    const confirm = c.page.locator('.ant-modal-confirm:visible');
    await confirm.waitFor({ timeout: 5000 });
    c.rec.check(/waits for its sandboxes to leave on their own/.test(await confirm.innerText()), 'the confirmation says a gentle drain waits for sandboxes to leave');
    await shot(c, 'confirmation', { dialog: true });
    const sent = nextPost(c.page, (p) => p.endsWith(`/nodes/${node.id}:drain`));
    await confirm.getByRole('button', { name: 'Drain', exact: true }).click();
    const answer = await sent;
    c.rec.check(answer?.status() === 200, `the gentle drain is accepted (${answer?.status()})`);
    c.rec.check(JSON.stringify(answer?.request().postDataJSON()) === '{"mode":"gentle"}', `the request asks for mode gentle and names no engine (${answer?.request().postData()})`);
    c.rec.check(await waitForSection(c, /Phase\s+(draining|drained)/, 15_000), 'the Drain section shows the drain');
    const section = (await drainCard(c.page).innerText()).replace(/\s+/g, ' ');
    c.rec.check(/Mode gentle/.test(section) && !/Engine/.test(section), 'the Drain section shows mode gentle and no engine');
    c.rec.check(!(await c.page.locator('#drain-progress').count()), 'a v1 drain shows no Drain progress panel');
    c.rec.check(kube.unschedulable(node.k8s_node_name!), 'the Kubernetes Node is cordoned');
    await shot(c, 'draining');
    c.rec.check(await waitForPhase(c, node.id, ['drained'], 180_000) === 'drained', 'the empty worker reaches drained');
    c.rec.check(await waitForSection(c, /Phase\s+drained/, 15_000), "the page's poll shows drained without a reload");
    await shot(c, 'drained');
    await content(c.page).getByRole('button', { name: 'Undrain' }).click();
    const undo = c.page.locator('.ant-modal-confirm:visible');
    await undo.waitFor({ timeout: 5000 });
    const undrain = nextPost(c.page, (p) => p.endsWith(`/nodes/${node.id}:undrain`));
    await undo.getByRole('button', { name: 'Undrain' }).click();
    c.rec.check((await undrain)?.status() === 200, 'the undrain is accepted');
    c.rec.check(await waitForSection(c, /Phase\s+not draining/, 15_000), 'the Drain section shows not draining');
  });
}, true);

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

// ------------------------------------------------------- v2 drain preview
// A preview asks the node what a v2 drain would find and changes nothing on
// it, so these flows create nothing and need no cleanup. Each records every
// admin request the page sends while it runs and fails on a write, and
// checks the node is in the drain phase it started in. They are `flows` for
// the node they act on, but not `mutates`: --no-mutate keeps them.

// The admin writes the page sends from now until `stop()`, and the preview
// reads in flight at most at once.
function watchAdminRequests(page: Page) {
  const writes: string[] = [];
  let inFlight = 0;
  let most = 0;
  const isPreview = (req: Request) => new URL(req.url()).pathname.endsWith('/drain-v2-preview');
  const onRequest = (req: Request) => {
    const path = new URL(req.url()).pathname;
    if (!path.startsWith('/admin/')) return;
    if (!['GET', 'HEAD', 'OPTIONS'].includes(req.method())) writes.push(`${req.method()} ${path}`);
    if (isPreview(req)) most = Math.max(most, ++inFlight);
  };
  const onDone = (req: Request) => {
    if (isPreview(req)) inFlight -= 1;
  };
  page.on('request', onRequest);
  page.on('requestfinished', onDone);
  page.on('requestfailed', onDone);
  return {
    stop: () => {
      page.off('request', onRequest);
      page.off('requestfinished', onDone);
      page.off('requestfailed', onDone);
      return { writes, most };
    },
  };
}

const previewCard = (page: Page) => page.locator('#drain-preview');

// The node a preview request is for, from its path, or null for another request.
const previewedNode = (url: string): string | null => {
  const match = /\/nodes\/([^/]+)\/drain-v2-preview$/.exec(new URL(url).pathname);
  return match ? decodeURIComponent(match[1]) : null;
};

// The console's own verdict for a preview (`previewVerdict` in
// `ui/src/lib/drain-preview.ts`). Loaded at run time: `ui/` is not a module
// package, so the typecheck can't follow a static import of it.
async function consolePreviewVerdict(): Promise<(preview: unknown) => ResultVerdict> {
  const consoleModule = '../../../ui/src/lib/drain-preview.ts';
  return (await import(consoleModule)).previewVerdict;
}

// The verdict each node's preview answered while the page ran, as the
// console reads it: its verdict, or `error` for a preview that failed, the
// console's own timeout among them. Keyed by node id until `stop()`.
function watchPreviewAnswers(page: Page, verdictOf: (preview: unknown) => ResultVerdict) {
  const answers = new Map<string, Promise<ResultVerdict>>();
  const onResponse = (res: Response) => {
    const node = previewedNode(res.url());
    if (node === null) return;
    const verdict = async (): Promise<ResultVerdict> => {
      if (!res.ok()) return 'error';
      try {
        return verdictOf(await res.json());
      } catch {
        return 'error';
      }
    };
    answers.set(node, verdict());
  };
  const onFailed = (req: Request) => {
    const node = previewedNode(req.url());
    if (node !== null) answers.set(node, Promise.resolve('error'));
  };
  page.on('response', onResponse);
  page.on('requestfailed', onFailed);
  return {
    stop: async (): Promise<Map<string, ResultVerdict>> => {
      page.off('response', onResponse);
      page.off('requestfailed', onFailed);
      const settled = new Map<string, ResultVerdict>();
      for (const [node, verdict] of answers) settled.set(node, await verdict);
      return settled;
    },
  };
}
const CHANGES_NOTHING = /A preview changes nothing on the node/;

step('node-drain-preview', ['nodes', 'flows'], async (c) => {
  const workers = (await adminNodes(c)).filter((n) => n.role === 'worker');
  if (!workers.length) return c.rec.check(false, 'a worker node exists');
  const node = workers.sort((a, b) => (b.sandboxes_active ?? 0) - (a.sandboxes_active ?? 0))[0];
  await visit(c, `/dashboard/nodes/${node.id}`, 3000);
  const button = content(c.page).getByRole('button', { name: 'Preview v2 drain' });
  if (!(await button.count())) return c.rec.check(false, 'the node page offers Preview v2 drain');
  await button.hover();
  c.rec.check(CHANGES_NOTHING.test(await c.page.locator('.ant-tooltip:visible').innerText().catch(() => '')), 'the action says a preview changes nothing on the node');
  const watch = watchAdminRequests(c.page);
  const answered = c.page.waitForResponse((res) => new URL(res.url()).pathname.endsWith(`/nodes/${node.id}/drain-v2-preview`), { timeout: previewWaitMs() }).catch(() => null);
  await button.click();
  const answer = await answered;
  c.rec.check(answer?.request().method() === 'GET' && answer.status() === 200, `the preview is a GET the API answers 200 (${answer?.request().method()} ${answer?.status()})`);
  await previewCard(c.page).locator('[data-testid="drain-preview-verdict"]').waitFor({ timeout: 15_000 }).catch(() => {});
  const t = (await previewCard(c.page).innerText().catch(() => '')).replace(/\s+/g, ' ');
  const body = await jsonOf(answer);
  const verdict = previewVerdictIn(await previewCard(c.page).locator('[data-testid="drain-preview-verdict"]').innerText().catch(() => ''));
  const tag = previewVerdictIn(await previewCard(c.page).locator('[data-testid="drain-preview-verdict-tag"]').innerText().catch(() => ''));
  c.rec.check(CHANGES_NOTHING.test(t), 'the panel says a preview changes nothing on the node');
  c.rec.check(verdict !== null, `the panel says whether a drain would finish, wait or block (${verdict})`);
  c.rec.check(tag !== null && tag === verdict, `the verdict is tagged with the same verdict (${tag})`);
  c.rec.check(/Blockers \(\d+\)/.test(t) && /What a drain would do \(\d+\)/.test(t), 'the panel lists the blockers and what a drain would do');
  if (body) {
    c.rec.check(t.includes(`Blockers (${body.blocked.length})`), `the panel counts the ${body.blocked.length} blockers the API reported`);
    c.rec.check(t.includes(`What a drain would do (${body.pending.length})`), `the panel counts the ${body.pending.length} items the API reported`);
    for (const blocked of body.blocked.slice(0, 5)) {
      c.rec.check(t.includes(blocked.reason), `the panel shows the blocker "${blocked.reason.slice(0, 80)}"`);
    }
    c.rec.check(new RegExp(`Already evacuated\\s*${body.evacuated.length}(?!\\d)`).test(t), `the panel counts the ${body.evacuated.length} sandboxes the API reported already evacuated`);
    const running = body.pending.filter((p: { running: boolean }) => p.running).length;
    c.rec.check(new RegExp(`Running\\s*${running}(?!\\d)`).test(t), `the panel counts the ${running} pending sandboxes the API reported running`);
    c.rec.note(`preview of ${node.name ?? node.id}: ${body.evacuated.length} evacuated, ${body.blocked.length} blocked, ${body.pending.length} pending (${body.pending.map((p: { wait: string }) => p.wait).join(', ') || 'none'})`);
  }
  await previewCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
  await shot(c, 'panel');
  const { writes } = watch.stop();
  c.rec.check(writes.length === 0, `the page sent no admin write (${writes.join(', ') || 'none'})`);
  c.rec.check((await adminNode(c, node.id))?.drain_phase === node.drain_phase, 'the node keeps its drain phase');
});

step('nodes-drain-preview-all', ['nodes', 'flows'], async (c) => {
  const workers = (await adminNodes(c)).filter((n) => n.role === 'worker');
  await visit(c, '/dashboard/nodes', 4000);
  const button = content(c.page).getByRole('button', { name: 'Preview v2 drain on all workers' });
  if (!(await button.count())) return c.rec.check(false, 'the nodes list offers Preview v2 drain on all workers');
  const watch = watchAdminRequests(c.page);
  const answers = watchPreviewAnswers(c.page, await consolePreviewVerdict());
  await button.click();
  const card = c.page.locator('#drain-preview-all');
  c.rec.check(await card.waitFor({ timeout: 10_000 }).then(() => true, () => false), 'the results card appears');
  c.rec.check(CHANGES_NOTHING.test(await card.innerText().catch(() => '')), 'the card says a preview changes nothing on the node');
  const total = workers.length;
  const progress = card.locator('[data-testid="drain-preview-progress"]');
  const deadline = Date.now() + previewAllWaitMs(total);
  let shown = '';
  while (Date.now() < deadline) {
    shown = await progress.innerText().catch(() => '');
    if (shown === `${total} of ${total} previewed`) break;
    await c.page.waitForTimeout(500);
  }
  c.rec.check(shown === `${total} of ${total} previewed`, `progress reaches every listed worker (${shown})`);
  await settle(c.page, 800);
  const answered = await answers.stop();
  const expected = new Map(workers.map((w) => [w.id, answered.get(w.id) ?? null] as const));
  // The table pages its rows, so every page is read: each row's node, from
  // its link, and its verdict cell.
  const nextPage = card.locator('.ant-pagination-next');
  const activePage = card.locator('.ant-pagination-item-active');
  let stuck = false;
  const read = () => card.locator('.ant-table-tbody tr.ant-table-row').evaluateAll((trs) => trs.map((tr) => ({
    nodeId: tr.querySelector('a[href^="/dashboard/nodes/"]')?.getAttribute('href')?.slice('/dashboard/nodes/'.length) ?? null,
    verdict: ((tr.querySelectorAll('td')[1] as HTMLElement | undefined)?.innerText ?? '').replace(/\s+/g, ' ').trim(),
  })));
  const turn = async () => {
    if (!(await nextPage.count()) || /\bant-pagination-disabled\b/.test((await nextPage.getAttribute('class')) ?? '')) return false;
    const before = await activePage.innerText().catch(() => '');
    await nextPage.click();
    for (let tries = 0; tries < 40 && (await activePage.innerText().catch(() => before)) === before; tries += 1) {
      await c.page.waitForTimeout(250);
    }
    stuck = (await activePage.innerText().catch(() => before)) === before;
    return !stuck;
  };
  // Every page holds a row, so a table of `total` rows has at most `total`
  // pages; one more is read to see a table with too many.
  const { rows, pages, complete } = await everyPage(read, turn, total + 1);
  c.rec.check(complete && !stuck, `every page of the table could be read (${pages} read${stuck ? ', the next page never showed' : ''})`);
  const mismatches = previewResultsMismatches(expected, rows);
  c.rec.check(
    mismatches.length === 0,
    `across its ${pages} ${pages === 1 ? 'page' : 'pages'} the table has one row per worker, with the verdict its preview answered (${mismatches.slice(0, 5).join('; ') || `${rows.length} of ${total}`})`,
  );
  const { writes, most } = watch.stop();
  c.rec.check(most > 0 && most <= CONSOLE_PREVIEW_CONCURRENCY, `at most ${CONSOLE_PREVIEW_CONCURRENCY} previews were in flight at once (${most})`);
  c.rec.check(writes.length === 0, `the page sent no admin write (${writes.join(', ') || 'none'})`);
  const after = await adminNodes(c);
  c.rec.check(workers.every((w) => after.find((n) => n.id === w.id)?.drain_phase === w.drain_phase), 'every worker keeps its drain phase');
  await shot(c, 'results');
  await card.getByRole('button', { name: 'Close' }).click().catch(() => {});
});

// ------------------------------------------------------------- v2 drains
// The v2 drain flows on a real Kind worker. `nodes-v2-setup` turns the
// cluster controller's v2 loop on (the `aws` terminator against Kind's fake
// EC2) under the smokes' controller Lease, and gives each empty worker a
// fake EC2 instance; `nodes-v2-teardown` puts the controller's config back.
// Every v2 step lists both, so `--only <a v2 step>` runs them around it. A
// flow drains only a worker that holds nothing of anyone else's, under the
// drain-keep override, and undrains it in `finally`, waiting for the v2
// cancel to finish on the node. Where a flow shows an answer the stack can't
// give on demand (the cap, a 503, a stalled drain, an unrecognized state), it
// answers the page itself with the API's own shape, marked synthetic, and
// says so in its notes.

const V2_STEPS = [
  'node-v2-drain-now', 'node-v2-hold-shorten-undrain', 'node-v2-refusals', 'node-v2-states', 'node-drain-progress',
];
// Setup and teardown also carry each flow's name, so selecting a flow runs them.
const V2_TAGS = ['nodes', 'flows', 'drain-v2'];
const V2_AROUND = [...V2_TAGS, ...V2_STEPS];

interface V2Setup {
  control: KindV2Control;
  lease: Lease;
  cordonLease: Lease;
  // node id → its Kubernetes Node, each with a fake instance recorded.
  workers: Map<string, string>;
}
let v2: V2Setup | null = null;

// Whether the bench runs drain v2, read once per run from the controller.
let drainV2Read: boolean | null = null;
function drainV2On(c: Ctx): boolean {
  if (drainV2Read === null) drainV2Read = drainV2Enabled(new KindV2Control(c.config.kubeContext).drainV2Switch());
  return drainV2Read;
}

// Put the controller's standing config, recorded in `file`, back: the keys a
// run writes and its replicas. The config put back.
function putStandingBack(control: KindV2Control, lease: Lease, file: string): Record<string, string | null> {
  const { env: recorded, replicas } = JSON.parse(fs.readFileSync(file, 'utf8'));
  // Only the keys a run writes go back; a record from before the switch
  // moved to Kind's values may also hold it.
  const env = Object.fromEntries(V2_CONTROLLER_KEYS.map((key) => [key, recorded[key] ?? null]));
  if (control.controllerReplicas() !== replicas) control.scaleController(lease, replicas);
  control.setControllerEnv(lease, env);
  return env;
}
const v2StandingPath = (c: Ctx) => path.join(c.config.outDir, 'drain-v2-standing.json');

const progressCard = (page: Page) => page.locator('#drain-progress');

// The page, and beside it the Drain progress panel alone, which a full-page
// capture of a long node page shows far down.
const panelShot = async (c: Ctx, name: string) => {
  await shot(c, name);
  if (await progressCard(c.page).isVisible().catch(() => false)) {
    await progressCard(c.page).screenshot({ path: `${c.rec.outDir}/${c.rec.stepName()}-${name}-panel.png`, timeout: 10_000 }).catch(() => {});
  }
};
// A button by its whole label. antd can leave a button's loading icon, named
// "loading", in its accessible name for a while after a fast answer, though
// it no longer shows; the label alone is matched.
const exactButton = (page: Page, name: string) =>
  content(page).getByRole('button', { name: new RegExp(`^(?:loading )?${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`) });

// The node's preview, whose blockers name what the API refuses a v2 drain
// of it for, the unrecorded instance among them: it reads, and changes
// nothing.
async function v2Refusals(c: Ctx, nodeId: string): Promise<string[] | null> {
  const res = await adminFetch(c.page, 'GET', `/admin/api/v1/nodes/${nodeId}/drain-v2-preview`, undefined, { timeoutMs: 80_000 });
  // A node busy with another preview (503 `node_busy`) is retryable
  // contention: no answer yet.
  if (res.status === 503 && res.json?.reason === 'node_busy') return null;
  if (res.status !== 200) return Array.isArray(res.json?.refusals) ? res.json.refusals : null;
  // Every blocker is node-wide; the API's own refusals come first among them.
  return (res.json?.blocked ?? []).map((b: { reason: string }) => b.reason);
}

// Waits until the API would start a v2 drain of the node: its instance is
// recorded and nothing else node-wide refuses it.
async function waitForV2Ready(c: Ctx, nodeId: string, timeoutMs: number): Promise<string[] | null> {
  const deadline = Date.now() + timeoutMs;
  let refusals: string[] | null = null;
  while (Date.now() < deadline) {
    refusals = await v2Refusals(c, nodeId).catch(() => null);
    if (refusals !== null && !refusals.some((r) => /EC2 instance recorded|Kubernetes nodes only/.test(r))) return refusals;
    await c.page.waitForTimeout(5000);
  }
  return refusals;
}

step('nodes-v2-setup', V2_AROUND, async (c) => {
  if (v2) return c.rec.note('already set up in this run');
  const control = new KindV2Control(c.config.kubeContext);
  const lease = control.acquireLease('retention');
  let cordonLease: Lease | null = null;
  try {
    cordonLease = control.acquireLease('cordon');
    const enabled = control.drainV2Switch();
    c.rec.check(enabled === 'true', `the bench's values turn drain v2 on for the controller (${DRAIN_V2_SWITCH}=${enabled})`);
    // A standing config an interrupted run recorded is the one to put back.
    const file = v2StandingPath(c);
    const standing = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')).env : control.controllerEnv();
    const replicas = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')).replicas : control.controllerReplicas();
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify({ env: standing, replicas }, null, 2));
    c.rec.note(`standing controller config: ${JSON.stringify(standing)}, ${replicas} replicas (kept in ${file})`);
    if (control.controllerReplicas() !== replicas) control.scaleController(lease, replicas);
    const rolled = control.setControllerEnv(lease, V2_CONTROLLER_ENV);
    c.rec.note(rolled ? 'turned the controller\'s aws terminator on and waited for its rollout' : 'the controller\'s aws terminator was already on');
    const env = control.controllerEnv();
    c.rec.check(Object.entries(V2_CONTROLLER_ENV).every(([key, value]) => env[key] === value), `the controller runs the v2 loop with the aws terminator (${JSON.stringify(env)})`);
    // Every empty Kubernetes worker gets a fake instance; one that holds a
    // sandbox is left alone, since re-registering deletes its pods.
    const workers = new Map<string, string>();
    for (const node of (await adminNodes(c)).filter((n) => n.role === 'worker' && n.managed_by === 'k8s' && n.k8s_node_name)) {
      const name = node.k8s_node_name!;
      if (node.drain_phase || node.drain_engine) {
        c.rec.note(`${name} is in a drain (${node.drain_phase ?? node.drain_engine}); left alone`);
        continue;
      }
      if (node.sandboxes_active !== 0 || node.sandboxes_local_snapshots !== 0 || control.sandboxPods(name)) {
        c.rec.note(`${name} holds sandboxes; it keeps its providerID and the v2 flows leave it alone`);
        continue;
      }
      const { instanceId, reRegistered } = control.giveFakeInstance(cordonLease, name);
      c.rec.note(`${name}: fake instance ${instanceId}${reRegistered ? ', re-registered' : ''}`);
      const refusals = await waitForV2Ready(c, node.id, 300_000);
      c.rec.check(refusals !== null && !refusals.some((r) => /EC2 instance recorded/.test(r)), `${name}'s row records its fake instance (${refusals?.join('; ') || 'no node-wide refusal'})`);
      const slotsBy = Date.now() + 120_000;
      while (control.networkSlots(name) === 0 && Date.now() < slotsBy) await c.page.waitForTimeout(2000);
      workers.set(node.id, name);
    }
    c.rec.check(workers.size > 0, `an empty worker is ready for v2 drains (${[...workers.values()].join(', ') || 'none'})`);
    v2 = { control, lease, cordonLease, workers };
  } catch (error) {
    // The standing config goes back while the Leases are still held, so no
    // other run sees the controller this setup changed.
    const file = v2StandingPath(c);
    try {
      if (fs.existsSync(file)) {
        putStandingBack(control, lease, file);
        fs.rmSync(file, { force: true });
      }
    } catch (restoreError) {
      c.rec.note(`putting the controller's standing config back failed: ${restoreError}; ${file} keeps it`);
    }
    if (cordonLease) control.releaseLease(cordonLease);
    control.releaseLease(lease);
    throw error;
  }
}, true);

// The v2-ready worker a flow drains, or null after a failed check. It must
// still hold nothing: a flow never drains anyone else's sandbox.
async function v2Worker(c: Ctx): Promise<{ node: AdminNode; kube: KindNodes } | null> {
  if (!v2) {
    c.rec.check(false, 'nodes-v2-setup prepared a worker for v2 drains');
    return null;
  }
  const nodes = await adminNodes(c);
  const busy = nodes.filter((n) => n.drain_phase || n.drain_engine);
  if (busy.length) {
    c.rec.check(false, `no node is in a drain (${busy.map((n) => `${n.name}: ${n.drain_phase ?? n.drain_engine}`).join(', ')})`);
    return null;
  }
  const node = nodes.find((n) => v2!.workers.has(n.id) && n.sandboxes_active === 0 && n.sandboxes_local_snapshots === 0);
  if (!node) {
    c.rec.check(false, 'a v2-ready worker holds no sandboxes');
    return null;
  }
  const kube = new KindNodes(c.config.kubeContext);
  kube.requireLocalNode(node.k8s_node_name!);
  return { node, kube };
}

// Runs `body` on the v2-ready worker under the drain-keep override, then
// undrains it unless it has no drain phase, and waits until the v2 drain is
// gone from it, its cancel included, before the override is removed.
async function withV2Worker(c: Ctx, body: (node: AdminNode, kube: KindNodes) => Promise<void>): Promise<void> {
  const picked = await v2Worker(c);
  if (!picked) return;
  const { node, kube } = picked;
  const name = node.k8s_node_name!;
  const claim = kube.claimDrainKeep(name);
  if ('held' in claim) return c.rec.check(false, `${name} already carries ${DRAIN_KEEP_ANNOTATION}=${claim.held}; nothing is drained`);
  c.rec.note(`v2-draining worker ${name} (${node.id}) under ${DRAIN_KEEP_ANNOTATION}=${claim.token}`);
  try {
    await body(node, kube);
  } finally {
    // A flow may have scaled the controller down; the cancel needs it.
    try {
      const file = v2StandingPath(c);
      const replicas = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')).replicas : 1;
      if (v2 && v2.control.controllerReplicas() !== replicas) v2.control.scaleController(v2.lease, replicas);
    } catch (error) {
      c.rec.check(false, `cleanup: scaling the controller back failed: ${String(error).split('\n')[0]}`);
    }
    const current = await apiNode(c, node.id).catch(() => null);
    if (!current || current.drain_phase) {
      const undrain = await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${node.id}:undrain`).catch(() => null);
      c.rec.note(`cleanup undrain answered ${undrain?.status}`);
    }
    const gone = await waitForV2Gone(c, node.id, 180_000);
    c.rec.check(gone, 'the worker is left with no drain phase and no v2 drain');
    if (gone) c.rec.check(kube.releaseDrainKeep(name, claim.token), `the drain-keep override on ${name} was this claim's and was removed`);
    else c.rec.note(`the drain-keep override ${claim.token} stays on ${name}`);
  }
}

// The node as the API answers the loop's own client: the v2 flows read the
// node outside the page, so the answers the page receives are the console's
// own (`watchNodeAnswers`).
async function apiNode(c: Ctx, id: string): Promise<AdminNode | null> {
  try {
    return await (await stepApi(c)).admin<AdminNode>('GET', `/nodes/${id}?include_occupancy=false`);
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

async function waitForV2Gone(c: Ctx, id: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const node = await apiNode(c, id).catch(() => null);
    if (node && !node.drain_phase && !node.drain_engine) return true;
    await c.page.waitForTimeout(2000);
  }
  return false;
}

// Waits for an answer of the API's for the node that satisfies `want`.
async function waitForNode(
  c: Ctx, id: string, want: (node: AdminNode) => boolean, timeoutMs: number, everyMs = 1500, seen?: (node: AdminNode) => void,
): Promise<AdminNode | null> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const node = await apiNode(c, id).catch(() => null);
    if (node) seen?.(node);
    if (node && want(node)) return node;
    await c.page.waitForTimeout(everyMs);
  }
  return null;
}

// Waits until the progress panel's text matches `pattern`.
async function waitForPanel(c: Ctx, pattern: RegExp, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pattern.test((await progressCard(c.page).innerText().catch(() => '')).replace(/\s+/g, ' '))) return true;
    await c.page.waitForTimeout(1000);
  }
  return false;
}

// Opens the typed confirmation of the node page's action `label`, answering
// with the dialog.
async function openTyped(c: Ctx, label: string) {
  await exactButton(c.page, label).click();
  const dialog = typedModal(c.page);
  await dialog.waitFor({ timeout: 5000 });
  return dialog;
}

// Picks a hold in an open v2 dialog: a preset by its label, or a custom
// number of seconds.
async function pickHold(dialog: Locator, hold: string | number) {
  if (typeof hold === 'string') {
    await dialog.getByRole('radio', { name: hold, exact: true }).check();
    return;
  }
  await typeHold(dialog, String(hold));
}

// Types `text` as a custom hold.
async function typeHold(dialog: Locator, text: string) {
  await dialog.getByRole('radio', { name: 'Custom', exact: true }).check();
  await dialog.getByLabel('Hold in seconds').fill(text);
}

const okOf = (dialog: Locator, label: string) => dialog.getByRole('button', { name: label, exact: true });

// Sends the open typed dialog's action and answers with the API's answer.
// It clicks once the action is enabled and has settled: a click in the
// same instant the typed name enables it can be lost.
async function confirmTyped(c: Ctx, dialog: Locator, label: string, nodeId: string, action: string, phrase: string): Promise<Response | null> {
  await dialog.getByLabel('Type the node name to confirm').fill(phrase);
  const ok = okOf(dialog, label);
  for (let tries = 0; tries < 20 && !(await ok.isEnabled().catch(() => false)); tries += 1) await c.page.waitForTimeout(250);
  await c.page.waitForTimeout(300);
  const sent = nextPost(c.page, (p) => p.endsWith(`/nodes/${nodeId}:${action}`));
  await ok.click();
  return sent;
}

// The Drain progress panel as an operator reads it.
async function readPanel(page: Page): Promise<PanelRead> {
  const card = progressCard(page);
  const tags = await card.locator('.ant-card-body > .ant-space > .ant-space-item').first().locator('.ant-tag').allInnerTexts().catch(() => []);
  const text = (await card.innerText().catch(() => '')).replace(/\s+/g, ' ');
  const table = (title: string) => card.locator('.ant-table-wrapper', { has: page.locator('.ant-table-title', { hasText: new RegExp(`^${title}$`) }) });
  const rows = async (title: string) => table(title).locator('.ant-table-tbody tr.ant-table-row').evaluateAll((trs) => trs.map((tr) => Array.from(tr.querySelectorAll('td')).map((td) => (td as HTMLElement).innerText.replace(/\s+/g, ' ').trim())));
  // The Sandbox cell is the id, then a `warm-pool member` tag for a pool
  // member; the Wait cell is the wait's tag, then `running` for a running one.
  const pending = (await rows('Pending').catch(() => [] as string[][])).map(([sandbox = '', wait = '', state = '']) => ({
    sandbox: sandbox.split(' ')[0] ?? '',
    pool: /\bwarm-pool member\b/.test(sandbox),
    wait: wait.split(' ')[0] ?? '',
    running: /\brunning\b/.test(wait.split(' ').slice(1).join(' ')),
    state,
  }));
  const evacuated = (await rows('Evacuated').catch(() => [] as string[][])).map(([sandbox, artifact, wasRunning, controller]) => ({ sandbox, artifact, wasRunning, controller }));
  return { tags, text, pending, evacuated };
}

// The node answers the page received, newest last, while `stop()` isn't
// called: what the panel was drawn from.
function watchNodeAnswers(page: Page, nodeId: string) {
  let latest: { at: number; body: any } | null = null;
  const onResponse = async (res: Response) => {
    const url = new URL(res.url());
    if (url.pathname !== `/admin/api/v1/nodes/${nodeId}` || url.searchParams.get('include_occupancy') !== 'false' || !res.ok()) return;
    const body = await res.json().catch(() => null);
    if (body) latest = { at: Date.now(), body };
  };
  page.on('response', onResponse);
  return {
    latest: () => latest,
    stop: () => page.off('response', onResponse),
  };
}

// Checks the panel against the answer the page last rendered it from, once
// the page has drawn it, and the sandboxes' own rows against that answer.
// `reached` says which answer shows the stage: the check waits (up to 30 s)
// until the page has received one after the call, or, with `current`, takes
// the answer the page has already received when it shows the stage.
async function panelMatchesAnswer(
  c: Ctx, answers: ReturnType<typeof watchNodeAnswers>, residents: string[], stage: string,
  reached: (drain: ApiDrain) => boolean = () => true, { current = false }: { current?: boolean } = {},
): Promise<ApiDrain | null> {
  const before = current ? 0 : answers.latest()?.at ?? 0;
  const deadline = Date.now() + 30_000;
  const ready = () => {
    const latest = answers.latest();
    return !!latest && latest.at > before && !!latest.body?.drain && reached(latest.body.drain);
  };
  while (!ready() && Date.now() < deadline) await c.page.waitForTimeout(300);
  if (!ready()) c.rec.check(false, `${stage}: the page received an answer showing the stage within 30 s`);
  await c.page.waitForTimeout(700);
  const answer = answers.latest()?.body;
  const drain = answer?.drain as ApiDrain | undefined;
  if (!drain) {
    c.rec.check(false, `${stage}: the page received the node's drain`);
    return null;
  }
  const mismatches = panelMismatches(await readPanel(c.page), drain, Date.now());
  c.rec.check(mismatches.length === 0, `${stage}: the panel matches the node's report (${mismatches.slice(0, 4).join('; ') || `${drain.evacuated.length} evacuated, ${drain.pending.length} pending`})`);
  const statuses = new Map<string, string | null>();
  for (const id of residents) statuses.set(bareSandboxId(id), await sandboxStatus(c, id));
  const rows = rowMismatches(drain, statuses);
  c.rec.check(rows.length === 0, `${stage}: the sandboxes' rows agree with the report (${rows.slice(0, 3).join('; ') || [...statuses].map(([id, s]) => `${id.slice(-8)} ${s}`).join(', ')})`);
  return drain;
}

// Answers the page's reads of the node with the API's own answer, changed by
// `change`, marked synthetic so the recorder doesn't count them. Returns the
// unroute.
async function overlayNode(c: Ctx, nodeId: string, change: (node: any) => any): Promise<() => Promise<void>> {
  const match = (url: URL) => url.pathname === `/admin/api/v1/nodes/${nodeId}`;
  const handler = async (route: Route) => {
    const res = await fetchWithoutRedirect(route);
    if (!res) return;
    const body = await res.json().catch(() => null);
    return route.fulfill({ status: res.status(), contentType: 'application/json', headers: { [SYNTHETIC_HEADER]: '1' }, body: JSON.stringify(body ? change(body) : body) });
  };
  await c.page.route(match, handler);
  return () => c.page.unroute(match, handler);
}

// Answers the page's next `:action` POST for the node itself, marked
// synthetic: nothing reaches the API.
async function answerAction(c: Ctx, nodeId: string, action: string, status: number, body: unknown, headers: Record<string, string> = {}): Promise<() => Promise<void>> {
  const match = (url: URL) => url.pathname.endsWith(`/nodes/${nodeId}:${action}`);
  const handler = (route: Route) => route.fulfill({ status, contentType: 'application/json', headers: { ...headers, [SYNTHETIC_HEADER]: '1' }, body: JSON.stringify(body) });
  await c.page.route(match, handler);
  return () => c.page.unroute(match, handler);
}

// Sends the page's `:action` POSTs for the node to the API, with the body
// `body` makes of the page's (as it is by default), and hands the page the
// API's answer marked synthetic: a refusal the step provokes is checked by
// the step, not counted by the recorder. `statuses` collects the answers.
async function forwardAction(c: Ctx, nodeId: string, action: string, statuses: number[], body: (sent: any) => any = (sent) => sent): Promise<() => Promise<void>> {
  const match = (url: URL) => url.pathname.endsWith(`/nodes/${nodeId}:${action}`);
  const handler = async (route: Route) => {
    const sent = route.request().postData();
    const res = await route.fetch({ maxRedirects: 0, postData: JSON.stringify(body(sent ? JSON.parse(sent) : undefined)) });
    statuses.push(res.status());
    return route.fulfill({ response: res, headers: { ...res.headers(), [SYNTHETIC_HEADER]: '1' } });
  };
  await c.page.route(match, handler);
  return () => c.page.unroute(match, handler);
}

// The error toast a refusal is also announced with, checked and noted.
async function expectToast(c: Ctx, pattern: RegExp, what: string): Promise<void> {
  const toasts = await takeToasts(c.page);
  for (const toast of toasts) {
    if (toast.type === 'error' && pattern.test(toast.text)) c.rec.note(`expected toast error: ${toast.text}`);
    else c.rec.check(toast.type !== 'error' && toast.type !== 'warning', `toast ${toast.type}: ${toast.text}`);
  }
  c.rec.check(toasts.some((t) => t.type === 'error' && pattern.test(t.text)), `${what} is announced (${toasts.map((t) => t.text).join(' | ') || 'no toast'})`);
}

const refusalAlert = (page: Page, title: RegExp) => drainCard(page).locator('.ant-alert-error', { hasText: title });

// Waits for a refusal alert in the Drain section and returns its reasons.
async function refusalReasons(c: Ctx, title: RegExp): Promise<string[] | null> {
  const alert = refusalAlert(c.page, title);
  if (!(await alert.waitFor({ timeout: 15_000 }).then(() => true, () => false))) return null;
  return alert.locator('li').allInnerTexts();
}

// The page shows no crash: its title and Drain section render, and no page
// error was thrown (the recorder counts those).
async function rendersNode(c: Ctx, what: string): Promise<void> {
  const fine = await drainCard(c.page).isVisible().catch(() => false);
  c.rec.check(fine, `${what}: the node page renders its Drain section`);
  c.rec.check(!/Application error|Something went wrong/i.test(await c.page.locator('body').innerText().catch(() => '')), `${what}: no error page`);
}

step('node-v2-drain-now', V2_TAGS, async (c) => {
  await withV2Worker(c, async (node) => {
    const phrase = node.name ?? node.id;
    await visit(c, `/dashboard/nodes/${node.id}`, 3000);
    await shot(c, '01-before');
    if (!(await exactButton(c.page, 'v2 drain').count())) return c.rec.check(false, 'the node page offers v2 drain on an idle worker');
    const dialog = await openTyped(c, 'v2 drain');
    const said = (await dialog.innerText()).replace(/\s+/g, ' ');
    c.rec.check(/keeps running sandboxes running until the deadline/.test(said), 'the confirmation says running sandboxes run until the deadline');
    c.rec.check(/refused when every slot is held/.test(said), 'the confirmation says a drain is refused at the cap');
    for (const preset of ['Now', '10 minutes', '1 hour', 'Custom']) {
      c.rec.check(await dialog.getByRole('radio', { name: preset, exact: true }).count() === 1, `the hold offers ${preset}`);
    }
    await pickHold(dialog, 'Now');
    c.rec.check(/already passed, so the hold ends at once|in 0s/.test((await dialog.innerText()).replace(/\s+/g, ' ')), 'a hold of Now ends at once');
    c.rec.check(await okOf(dialog, 'v2 drain').isDisabled(), 'v2 drain is disabled before the name is typed');
    await dialog.getByLabel('Type the node name to confirm').fill(phrase);
    await shot(c, '02-confirm-now', { dialog: true });
    const answer = await confirmTyped(c, dialog, 'v2 drain', node.id, 'drain', phrase);
    c.rec.check(answer?.status() === 200, `the v2 drain is accepted (${answer?.status()})`);
    c.rec.check(JSON.stringify(answer?.request().postDataJSON()) === '{"engine":"v2","hold_seconds":0,"require_undrained":true}', `the request asks for engine v2 with hold 0, start-only, and no mode (${answer?.request().postData()})`);
    c.rec.check(await waitForSection(c, /Engine\s+v2/, 15_000), 'the Drain section shows engine v2');
    c.rec.check(await waitForPanel(c, /Drain progress/, 15_000), 'the Drain progress panel appears');
    await panelShot(c, '03-draining');
    c.rec.check(!!(await waitForNode(c, node.id, (n) => n.drain_phase === 'drained', 300_000)), 'the empty worker reaches drained');
    c.rec.check(await waitForSection(c, /Phase\s+drained/, 20_000), "the page's poll shows drained without a reload");
    c.rec.check(await waitForPanel(c, /Finished: everything on the node is durable elsewhere/, 20_000), 'the panel says the drain is finished');
    c.rec.check(await exactButton(c.page, 'Terminate').count() === 1, 'a drained v2 node offers Terminate');
    await panelShot(c, '04-drained');

    // Terminate's refusal: `:terminate` refuses a v2 node only on its phase.
    // The API's own, for a terminate sent after the node is undrained while
    // the confirmation is open.
    let refusedStatus: number | null = null;
    const terminateUrl = (url: URL) => url.pathname.endsWith(`/nodes/${node.id}:terminate`);
    const guard = async (route: Route) => {
      const current = await apiNode(c, node.id);
      if (current?.drain_phase !== null) {
        c.rec.check(false, `the terminate was not sent: the node is still ${current?.drain_phase}`);
        return route.abort();
      }
      const res = await fetchWithoutRedirect(route);
      if (!res) return;
      refusedStatus = res.status();
      return route.fulfill({ response: res, headers: { ...res.headers(), [SYNTHETIC_HEADER]: '1' } });
    };
    await c.page.route(terminateUrl, guard);
    try {
      const confirm = await openTyped(c, 'Terminate');
      // A v2 node can be undrained from terminating until its instance is
      // fenced (`V2_TERMINATE_ENDS` in ui/src/lib/node-actions.ts).
      c.rec.check(/can still be undrained/.test(await confirm.innerText()), 'the terminate confirmation says the node can be undrained until its instance is fenced');
      const undrained = await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${node.id}:undrain`);
      c.rec.check(undrained.status === 200, 'the node is undrained while the confirmation is open');
      if (!(await waitForNode(c, node.id, (n) => n.drain_phase === null, 30_000))) return c.rec.check(false, 'the undrain cleared the phase');
      await confirmTyped(c, confirm, 'Terminate', node.id, 'terminate', phrase);
      const reasons = await refusalReasons(c, /not drained/);
      c.rec.check(refusedStatus === 409, `the API refuses the terminate (${refusedStatus})`);
      c.rec.check(reasons !== null && reasons.some((r) => /accepted only from: drained/.test(r)), `the phase refusal is listed as reasons (${reasons?.join(' | ')})`);
      await expectToast(c, /Terminate refused: the node is not drained/, 'the refusal');
      await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
      await shot(c, '05-refused-phase');
    } finally {
      await c.page.unroute(terminateUrl, guard);
    }
    c.rec.check(await waitForV2Gone(c, node.id, 120_000), 'the undrain\'s cancel finishes on the node');
    c.rec.check(await waitForSection(c, /Phase\s+not draining/, 20_000), 'the Drain section shows not draining');
    c.rec.check(await progressCard(c.page).waitFor({ state: 'detached', timeout: 15_000 }).then(() => true, () => false), 'the Drain progress panel is gone once the cancel finished');
    await shot(c, '06-undrained');
  });
}, true);

step('node-v2-hold-shorten-undrain', V2_TAGS, async (c) => {
  await withV2Worker(c, async (node) => {
    const phrase = node.name ?? node.id;
    const answers = watchNodeAnswers(c.page, node.id);
    let residents: string[] = [];
    try {
      // A running sandbox, so the drain holds until its deadline: a drain of
      // an empty node is done at once.
      residents = await createResidents(c, node, [['held', null]]);
      await visit(c, `/dashboard/nodes/${node.id}`, 3000);
      // 1. A start with a custom hold of N seconds.
      c.rec.note('stage 1');
      const dialog = await openTyped(c, 'v2 drain');
      await pickHold(dialog, 600);
      c.rec.check(/Deadline .* \(in (9|10)m\)/.test((await dialog.innerText()).replace(/\s+/g, ' ')), 'a custom hold of 600 s puts the deadline 10 minutes from now');
      await typeHold(dialog, '1.5');
      c.rec.check(await okOf(dialog, 'v2 drain').isDisabled(), 'a hold that is no whole number of seconds holds v2 drain');
      c.rec.check(/A whole number of seconds/.test(await dialog.innerText()), 'the dialog says what a hold must be');
      await pickHold(dialog, 600);
      await dialog.getByLabel('Type the node name to confirm').fill(phrase);
      await shot(c, '01-confirm-hold', { dialog: true });
      const started = await confirmTyped(c, dialog, 'v2 drain', node.id, 'drain', phrase);
      c.rec.check(started?.status() === 200, `the v2 drain is accepted (${started?.status()})`);
      c.rec.check(JSON.stringify(started?.request().postDataJSON()) === '{"engine":"v2","hold_seconds":600,"require_undrained":true}', `the request asks for engine v2 with hold 600, start-only (${started?.request().postData()})`);
      // The API doesn't show the node's seal: the controller storing a report
      // under the drain's generation is what shows the node answered it.
      const generation = (await waitForNode(c, node.id, (n) => n.drain_engine === 'v2' && !!n.drain, 30_000))?.drain?.generation;
      c.rec.check(generation !== undefined, `the drain records its generation (${generation})`);
      const reported = await waitForNode(c, node.id, (n) => reportedUnder(n.drain, generation), 120_000);
      c.rec.check(!!reported, 'the controller stores the node\'s report under the drain\'s generation');
      c.rec.check(await waitForPanel(c, /deadline in (9|10)m/, 20_000), 'the panel counts down to the deadline');
      await panelMatchesAnswer(c, answers, residents, 'holding', (d) => reportedUnder(d, generation) && d.pending.some((p) => p.wait === HOLD_WAIT));
      await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
      await panelShot(c, '02-holding');

      // 2. The nodes list's cell.
      c.rec.note('stage 2');
      await visit(c, '/dashboard/nodes', 3000);
      const row = content(c.page).locator('tr', { has: c.page.locator(`a[href="/dashboard/nodes/${node.id}"]`) });
      const cell = (await row.innerText().catch(() => '')).replace(/\s+/g, ' ');
      // Counted as the panel counts, from the answer: the worker may also
      // hold an earlier run's durable sandbox, which the report lists
      // evacuated.
      const listed = (await apiNode(c, node.id))?.drain;
      const counts = listed ? drainCountsOf(listed) : null;
      const progress = counts ? `${counts.evacuated}/${counts.total} evacuated` : 'no drain answer';
      c.rec.check(/draining · v2/.test(cell) && cell.includes(`${progress} · deadline`) && /deadline (9|10)m/.test(cell), `the nodes list shows the drain in one line, ${progress} (${cell.slice(0, 160)})`);
      await shot(c, '03-nodes-list');
      await visit(c, `/dashboard/nodes/${node.id}`, 3000);

      // 3. A preview of the drained node: the API answers a node a v2 drain
      // holds with a 409 and asks the node nothing. The step passes the
      // API's answer on, marked synthetic so the recorder doesn't count the
      // 409 it expects.
      c.rec.note('stage 3');
      const previewUrl = (url: URL) => url.pathname.endsWith(`/nodes/${node.id}/drain-v2-preview`);
      let previewed: { status: number; body: any } | null = null;
      const passOn = async (route: Route) => {
        const res = await fetchWithoutRedirect(route);
        if (!res) return;
        const text = await res.text().catch(() => '');
        previewed = { status: res.status(), body: (() => { try { return JSON.parse(text); } catch { return null; } })() };
        return route.fulfill({ status: res.status(), headers: { ...res.headers(), [SYNTHETIC_HEADER]: '1' }, body: text });
      };
      await c.page.route(previewUrl, passOn);
      try {
        await content(c.page).getByRole('button', { name: 'Preview v2 drain' }).click();
        const refused = previewCard(c.page).locator('.ant-alert', { hasText: 'Could not preview this node (v2 drain in progress)' });
        c.rec.check(await refused.waitFor({ timeout: previewWaitMs() }).then(() => true, () => false), 'the preview panel says a v2 drain holds the node');
        const got = previewed as { status: number; body: any } | null;
        c.rec.check(got?.status === 409 && got.body?.reason === 'drain_v2_in_progress', `the API answers the preview of a node a v2 drain holds with 409 drain_v2_in_progress (${got?.status} ${got?.body?.reason})`);
        await previewCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
        await shot(c, '04-preview-draining');
      } finally {
        await c.page.unroute(previewUrl, passOn);
      }

      // 4. A longer hold: the deadline only moves earlier.
      c.rec.note('stage 4');
      const shorten = await openTyped(c, 'Shorten hold');
      c.rec.check(/only moves earlier|can't be moved later/.test((await shorten.innerText()).replace(/\s+/g, ' ')), 'the dialog says a deadline moves only earlier');
      await pickHold(shorten, '1 hour');
      await shorten.getByLabel('Type the node name to confirm').fill(phrase);
      const longerHeld = await okOf(shorten, 'Shorten hold').isDisabled();
      c.rec.check(longerHeld, 'a hold that would end the drain later is caught before it is sent');
      c.rec.check(/a deadline only moves earlier/.test(await shorten.innerText()), 'the dialog says why');
      await shot(c, '05-shorten-longer', { dialog: true });
      if (!longerHeld) {
        // A console that sends it shows what the API answers.
        const sent = nextPost(c.page, (p) => p.endsWith(`/nodes/${node.id}:drain`));
        await okOf(shorten, 'Shorten hold').click();
        c.rec.note(`the longer hold was sent and answered ${(await sent)?.status()}`);
        await expectToast(c, /Shorten refused/, 'the refusal');
        await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
        await shot(c, '05b-shorten-longer-sent');
      } else {
        await shorten.getByRole('button', { name: 'Cancel' }).click();
      }

      // 5. A shorten: 600 s to 300 s from the drain's start.
      c.rec.note('stage 5');
      const shorter = await openTyped(c, 'Shorten hold');
      await pickHold(shorter, 300);
      c.rec.check(await okOf(shorter, 'Shorten hold').isDisabled(), 'Shorten hold is disabled before the name is typed');
      await shorter.getByLabel('Type the node name to confirm').fill(phrase);
      c.rec.check(await okOf(shorter, 'Shorten hold').isEnabled(), 'a shorter hold can be sent');
      await shot(c, '06a-shorten-confirm', { dialog: true });
      const shortened = await confirmTyped(c, shorter, 'Shorten hold', node.id, 'drain', phrase);
      c.rec.check(shortened?.status() === 200 && JSON.stringify(shortened.request().postDataJSON()) === '{"hold_seconds":300}', `the shorten sends hold 300, naming no engine, and is accepted (${shortened?.status()} ${shortened?.request().postData()})`);
      const moved = await waitForNode(c, node.id, (n) => !!n.drain?.deadline && !!n.drain_started_at && Math.abs(Date.parse(n.drain.deadline) - (Date.parse(n.drain_started_at) + 300_000)) <= 1500, 30_000);
      c.rec.check(!!moved, `the deadline moves to the drain's start plus 300 s (${moved?.drain?.deadline})`);
      c.rec.check(await waitForPanel(c, /deadline in [0-4]m/, 20_000), 'the panel counts down to the earlier deadline');
      await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
      await panelShot(c, '06b-shortened');

      // 6. The API's own 409: another operator shortened the drain while
      // this page read the node as it was. The page's reads are answered
      // with the node as last read (synthetic, stale), so the console can't
      // know; the shorten goes to the API, which refuses it.
      c.rec.note('stage 6');
      const stale = answers.latest()?.body;
      const unstale = await overlayNode(c, node.id, () => stale);
      const staleAnswers: number[] = [];
      const unforward = await forwardAction(c, node.id, 'drain', staleAnswers);
      try {
        const again = await openTyped(c, 'Shorten hold');
        await pickHold(again, 200);
        // Another client: the loop's own, outside the page.
        const elsewhere = await (await stepApi(c)).admin<AdminNode>('POST', `/nodes/${node.id}:drain`, { engine: 'v2', hold_seconds: 120 }, { idempotent: true }).then(() => 200, (error) => (error instanceof ApiError ? error.status : 0));
        c.rec.check(elsewhere === 200, `another client shortens the hold to 120 s (${elsewhere})`);
        await confirmTyped(c, again, 'Shorten hold', node.id, 'drain', phrase);
        c.rec.check(staleAnswers.length === 1 && staleAnswers[0] === 409, `the API refuses a shorten that is now longer (${staleAnswers.join(', ')})`);
        c.rec.note('SYNTHETIC: the page read the node as it was before the other client\'s shorten (its reads answered with that answer); the 409 is the API\'s');
        const reasons = await refusalReasons(c, /Shorten refused/);
        c.rec.check(reasons !== null && reasons.some((r) => /only moves earlier/.test(r)), `the refusal is listed (${reasons?.join(' | ')})`);
        await expectToast(c, /Shorten refused/, 'the refusal');
        await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
        await shot(c, '07-shorten-refused');
      } finally {
        await unforward();
        await unstale();
      }
      await drainCard(c.page).locator('.ant-alert-error .ant-alert-close-icon').click({ timeout: 3000 }).catch(() => {});

      // 7. `mode` with v2: the step adds `mode: forced` to the End hold now
      // the page sends, and the API refuses it.
      c.rec.note('stage 7');
      const drainUrl = (url: URL) => url.pathname.endsWith(`/nodes/${node.id}:drain`);
      const modeAnswers: number[] = [];
      const unmode = await forwardAction(c, node.id, 'drain', modeAnswers, (sent) => ({ ...(sent ?? {}), mode: 'forced' }));
      try {
        const end = await openTyped(c, 'End hold now');
        c.rec.check(/Moves this v2 drain's deadline to now/.test(await end.innerText()), 'End hold now says it moves the deadline to now');
        await shot(c, '08-end-hold-confirm', { dialog: true });
        await confirmTyped(c, end, 'End hold now', node.id, 'drain', phrase);
        const reasons = await refusalReasons(c, /takes no mode/);
        c.rec.note('the step added mode forced to the request the page sent; the 400 is the API\'s');
        c.rec.check(reasons !== null, `a v2 drain with a mode is refused as such (${reasons?.join(' | ')})`);
        await expectToast(c, /takes no mode/, 'the refusal');
        await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
        await shot(c, '09-mode-refused');
      } finally {
        await unmode();
      }
      c.rec.check(modeAnswers[0] === 400, `the API answers mode with v2 with a 400 (${modeAnswers.join(', ')})`);
      await drainCard(c.page).locator('.ant-alert-error .ant-alert-close-icon').click({ timeout: 3000 }).catch(() => {});

      // 8. A 503 and a network error, answered by the step.
      c.rec.note('stage 8');
      for (const [name, how] of [['10-unavailable', 503], ['11-network-error', 0]] as const) {
        const match = drainUrl;
        const handler = (route: Route) => (how === 0
          ? route.abort('aborted')
          : route.fulfill({ status: 503, contentType: 'application/json', headers: { 'retry-after': '5', [SYNTHETIC_HEADER]: '1' }, body: JSON.stringify({ error: 'service temporarily unavailable; retry shortly' }) }));
        await c.page.route(match, handler);
        try {
          const dlg = await openTyped(c, 'Shorten hold');
          await pickHold(dlg, 60);
          await confirmTyped(c, dlg, 'Shorten hold', node.id, 'drain', phrase);
          await c.page.waitForTimeout(1500);
          c.rec.note(`SYNTHETIC: ${how === 0 ? 'the request is cut off before it reaches the API' : "a 503 with Retry-After, answered by the step"}`);
          await expectToast(c, how === 0 ? /fetch|network|failed/i : /unavailable|retry shortly/i, how === 0 ? 'the network error' : 'the 503');
          await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
          c.rec.check(await waitForPanel(c, /Drain progress/, 5000), 'the panel stays');
          await shot(c, name);
        } finally {
          await c.page.unroute(match, handler);
        }
      }
      c.rec.check(!!(await apiNode(c, node.id))?.drain_phase, 'the drain is untouched by them');

      // 9. Stale data while polling: the page's node reads fail for a while.
      c.rec.note('stage 9');
      const nodeUrl = (url: URL) => url.pathname === `/admin/api/v1/nodes/${node.id}`;
      // Aborted, so the recorder takes them as cut off by the step.
      const cut = (route: Route) => route.abort('aborted');
      await c.page.route(nodeUrl, cut);
      try {
        const warned = await c.page.locator('.ant-alert-warning', { hasText: 'Could not refresh the node' }).waitFor({ timeout: 15_000 }).then(() => true, () => false);
        c.rec.check(warned, 'a failed poll says the node may be stale');
        c.rec.check(await progressCard(c.page).isVisible(), 'the panel keeps the last answer while the reads fail');
        c.rec.note('SYNTHETIC: the page\'s node reads are cut off by the step for one poll');
        await shot(c, '12-stale-poll');
      } finally {
        await c.page.unroute(nodeUrl, cut);
      }
      c.rec.check(await c.page.locator('.ant-alert-warning', { hasText: 'Could not refresh the node' }).waitFor({ state: 'hidden', timeout: 15_000 }).then(() => true, () => false), 'the warning clears once a read answers');

      // 10. End hold now, then undrain mid-drain, with the controller down,
      // so the node's state stays where the API left it: past the deadline,
      // then cancelling until the controller runs again.
      c.rec.note('stage 10');
      v2!.control.scaleController(v2!.lease, 0);
      c.rec.note('scaled the cluster controller to 0');
      const end = await openTyped(c, 'End hold now');
      const ended = await confirmTyped(c, end, 'End hold now', node.id, 'drain', phrase);
      c.rec.check(ended?.status() === 200 && JSON.stringify(ended.request().postDataJSON()) === '{"hold_seconds":0}', `End hold now sends hold 0, naming no engine, and is accepted (${ended?.status()} ${ended?.request().postData()})`);
      c.rec.check(await waitForPanel(c, /past the deadline/, 20_000), 'the panel says the drain is past its deadline');
      await panelShot(c, '13-past-deadline');
      await exactButton(c.page, 'Undrain').click();
      const confirm = c.page.locator('.ant-modal-confirm:visible');
      await confirm.waitFor({ timeout: 5000 });
      const undrain = nextPost(c.page, (p) => p.endsWith(`/nodes/${node.id}:undrain`));
      await confirm.getByRole('button', { name: 'Undrain' }).click();
      c.rec.check((await undrain)?.status() === 200, 'the undrain is accepted mid-drain');
      c.rec.check(await waitForPanel(c, /Undrained: cancelling the drain on the node/, 20_000), 'the panel says the cancel is in progress');
      c.rec.check(await waitForSection(c, /Phase\s+not draining.*Engine\s+v2/s, 15_000), 'the Drain section shows no phase, under engine v2');
      const offered = { drain: await exactButton(c.page, 'Drain').count(), forced: await exactButton(c.page, 'Forced drain').count(), v2: await exactButton(c.page, 'v2 drain').count() };
      // The API refuses every drain until the cancel finishes ("undrain still
      // finishing"), so the page offers none.
      c.rec.check(offered.drain === 0 && offered.forced === 0 && offered.v2 === 0, `while the cancel is pending no drain is offered (${JSON.stringify(offered)})`);
      await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
      await panelShot(c, '14-cancelling');
      await visit(c, '/dashboard/nodes', 3000);
      const cancelCell = (await content(c.page).locator('tr', { has: c.page.locator(`a[href="/dashboard/nodes/${node.id}"]`) }).innerText().catch(() => '')).replace(/\s+/g, ' ');
      c.rec.check(/cancelling on the node/.test(cancelCell), `the nodes list says the cancel is in progress (${cancelCell.slice(0, 120)})`);
      await shot(c, '15-nodes-list-cancelling');
      await visit(c, `/dashboard/nodes/${node.id}`, 2000);
      const file = v2StandingPath(c);
      v2!.control.scaleController(v2!.lease, JSON.parse(fs.readFileSync(file, 'utf8')).replicas);
      c.rec.note('scaled the cluster controller back');
      c.rec.check(await waitForV2Gone(c, node.id, 180_000), 'the cancel finishes once the controller runs');
      c.rec.check(await waitForSection(c, /Phase\s+not draining/, 20_000) && await progressCard(c.page).waitFor({ state: 'detached', timeout: 15_000 }).then(() => true, () => false), 'the page shows no drain');
      await shot(c, '16-let-go');
    } finally {
      answers.stop();
      if (residents.length) await deleteResidents(c, residents);
    }
  });
}, true);

step('node-v2-refusals', V2_TAGS, async (c) => {
  const picked = await v2Worker(c);
  if (!picked) return;
  const { node } = picked;
  const phrase = node.name ?? node.id;
  await visit(c, `/dashboard/nodes/${node.id}`, 3000);
  // The cap and the stale heartbeat: the API's 409s for a v2 start
  // (`v2_start_refusal`), answered by the step; nothing reaches the API.
  const cases: [string, number, unknown, Record<string, string>, RegExp, RegExp[]][] = [
    ['01-cap', 409, {
      error: "4 nodes' v2 drains hold every evacuation slot (SPACE_DRAIN_V2_MAX_EVACUATING_NODES is 4); retry once one reaches drained, or free a slot: shorten a drain in flight with hold_seconds 0, or undrain one",
      code: 'drain_v2_evacuation_cap', evacuating: 4, max_evacuating: 4,
    }, { 'retry-after': '60' }, /every evacuation slot is held/, [/4 of 4 evacuation slots are held/, /Retry after 60 s/]],
    ['02-heartbeat-stale', 409, {
      error: "the node's heartbeat is stale: a v2 drain starts only on a node that answers, since its node does the drain",
      code: 'drain_v2_node_heartbeat_stale',
    }, {}, /heartbeat is stale/, [/starts only on a node that answers/]],
    ['03-switched-off', 400, { error: 'drain engine v2 is not enabled on this cluster' }, {}, /switched off/, [/SPACE_DRAIN_V2_ENABLED/]],
  ];
  for (const [name, status, body, headers, title, reasons] of cases) {
    c.rec.note(`stage ${name}`);
    const unroute = await answerAction(c, node.id, 'drain', status, body, headers);
    try {
      const dialog = await openTyped(c, 'v2 drain');
      await pickHold(dialog, '10 minutes');
      await confirmTyped(c, dialog, 'v2 drain', node.id, 'drain', phrase);
      const shown = await refusalReasons(c, title);
      c.rec.note(`SYNTHETIC: ${name} is the API's ${status} for a v2 start, answered by the step`);
      c.rec.check(shown !== null && reasons.every((r) => shown.some((s) => r.test(s))), `${name}: the refusal is listed with what the API said (${shown?.join(' | ') ?? 'none'})`);
      await expectToast(c, title, 'the refusal');
      await typedModal(c.page).waitFor({ state: 'hidden', timeout: 15_000 }).catch(() => {});
      await shot(c, name);
      await drainCard(c.page).locator('.ant-alert-error .ant-alert-close-icon').click({ timeout: 3000 }).catch(() => {});
    } finally {
      await unroute();
    }
  }
  c.rec.check(!(await apiNode(c, node.id))?.drain_phase, 'the node was never drained');
});

step('node-v2-states', V2_TAGS, async (c) => {
  const picked = await v2Worker(c);
  if (!picked) return;
  const { node } = picked;
  const now = Date.now();
  const at = (ms: number) => new Date(now + ms).toISOString();
  const report = (over: Record<string, unknown> = {}) => ({
    generation: 1_763_553_600_000_000, deadline: at(-60_000), cancelling: false, fenced: false, reported_at: at(-5_000), evacuated_grew_at: at(-60_000),
    evacuated_total: [], evacuated: [], pending: [], blocked: [], ...over,
  });
  // Each is the API's answer for the node with its drain fields changed;
  // the page renders it, and must neither crash nor misread it.
  const states: [string, (n: any) => any, RegExp][] = [
    ['01-terminating', (n) => ({ ...n, drain_phase: 'terminating', drain_engine: 'v2', drain_started_at: at(-600_000), drain: report({ fenced: true }) }), /Finished: everything on the node is durable elsewhere.*instance fenced for termination/],
    ['02-unreadable', (n) => ({ ...n, drain_phase: 'draining', drain_engine: 'v2', drain_started_at: at(-600_000), drain: undefined }), /can't be shown|Drain/],
    ['03-unknown-phase-engine', (n) => ({ ...n, drain_phase: 'quiescing', drain_engine: 'v3', drain_started_at: at(-600_000) }), /quiescing/],
    ['04-malformed-report', (n) => ({ ...n, drain_phase: 'draining', drain_engine: 'v2', drain_started_at: at(-600_000), drain: { generation: 7, deadline: at(60_000), pending: [{ sandbox_id: '01a1f4900000700080000000000000bb', wait: 'reticulate' }] } }), /deadline in|Drain progress/],
    ['05-unknown-entries', (n) => ({ ...n, drain_phase: 'draining', drain_engine: 'v2', drain_started_at: at(-600_000), drain: report({
      evacuated: [{ sandbox_id: '01a1f4900000700080000000000000cc', artifact_kind: 'unknown', was_running: false, recorded: true }],
      pending: [{ sandbox_id: '01a1f4900000700080000000000000dd', wait: 'reticulate', running: false, pool_member: false, reason: 'a newer node reports it', since: at(-120_000) }],
    }) }), /reticulate/],
    // `:terminate` refuses a v2 node only on its phase; the terminating
    // tick's fresh check, refused, returns it to drained with why.
    ['06-terminate-refused', (n) => ({ ...n, drain_phase: 'drained', drain_engine: 'v2', drain_started_at: at(-600_000), drain_last_error: 'terminate refused: the node reports 1 pending, 0 blocked: synthetic', drain: report() }), /The last terminate was refused: the node is back in drained/],
  ];
  for (const [name, change, shows] of states) {
    const unroute = await overlayNode(c, node.id, change);
    try {
      await visit(c, `/dashboard/nodes/${node.id}`, 3000);
      c.rec.note(`SYNTHETIC: ${name}: the node's answer with its drain fields changed by the step`);
      await rendersNode(c, name);
      c.rec.check(shows.test((await content(c.page).innerText().catch(() => '')).replace(/\s+/g, ' ')), `${name}: the page shows the state (${shows})`);
      if (name === '02-unreadable') c.rec.check(await progressCard(c.page).filter({ hasText: "can't be shown" }).count() === 1, `${name}: the page says the drain's progress can't be read`);
      if (name === '03-unknown-phase-engine') {
        const section = (await drainCard(c.page).innerText()).replace(/\s+/g, ' ');
        c.rec.check(/Engine v3/.test(section) && !/Mode gentle/.test(section), `${name}: the Drain section names the engine rather than reading it as a v1 gentle drain (${section.slice(0, 120)})`);
      }
      await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
      await panelShot(c, name);
    } finally {
      await unroute();
    }
  }
  // The nodes list with an unrecognized and a malformed v2 drain.
  const listUrl = (url: URL) => url.pathname === '/admin/api/v1/nodes';
  const list = async (route: Route) => {
    const res = await fetchWithoutRedirect(route);
    if (!res) return;
    const body = await res.json().catch(() => null);
    const changed = Array.isArray(body) ? body.map((n: any) => (n.id === node.id ? { ...n, drain_phase: 'draining', drain_engine: 'v2', drain_started_at: at(-600_000), drain: { generation: 7, deadline: at(60_000) } } : n)) : body;
    return route.fulfill({ status: res.status(), contentType: 'application/json', headers: { [SYNTHETIC_HEADER]: '1' }, body: JSON.stringify(changed) });
  };
  await c.page.route(listUrl, list);
  try {
    await visit(c, '/dashboard/nodes', 3000);
    c.rec.note('SYNTHETIC: the nodes list with a v2 drain whose report has no entries, answered by the step');
    c.rec.check(await content(c.page).locator('tr', { has: c.page.locator(`a[href="/dashboard/nodes/${node.id}"]`) }).count() === 1, 'the nodes list renders the node');
    await shot(c, '07-nodes-list-malformed');
  } finally {
    await c.page.unroute(listUrl, list);
  }
  // Let go: a terminated v2 node's row is deleted, so its page reads it gone.
  const gone = async (route: Route) => route.fulfill({ status: 404, contentType: 'application/json', headers: { [SYNTHETIC_HEADER]: '1' }, body: JSON.stringify({ error: 'Node not found' }) });
  const nodeUrl = (url: URL) => url.pathname === `/admin/api/v1/nodes/${node.id}`;
  await c.page.route(nodeUrl, gone);
  try {
    await visit(c, `/dashboard/nodes/${node.id}`, 3000);
    c.rec.note('SYNTHETIC: the node read answers 404, as for a v2 node the drain let go and deleted');
    c.rec.check(/Node not found/.test(await content(c.page).innerText()), 'a let-go node\'s page says the node is not found');
    await shot(c, '08-let-go');
  } finally {
    await c.page.unroute(nodeUrl, gone);
  }
});

// The loop's own API client, signed in as the console account: the steps'
// reads and writes of the residents go through it, outside the page, so a
// read that finds a sandbox gone is never counted against the console.
let apiClient: Api | null = null;
async function stepApi(c: Ctx): Promise<Api> {
  if (!apiClient) {
    const api = new Api(c.config);
    await api.signIn(c.config.email, c.config.password);
    apiClient = api;
  }
  return apiClient;
}

// A sandbox's admin status, or null once it is gone.
async function sandboxStatus(c: Ctx, id: string): Promise<string | null> {
  try {
    return (await (await stepApi(c)).admin<{ status: string | null }>('GET', `/sandboxes/${id}`)).status;
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

async function waitForSandbox(c: Ctx, id: string, want: (string | null)[], timeoutMs: number): Promise<string | null> {
  const deadline = Date.now() + timeoutMs;
  let status: string | null = null;
  while (Date.now() < deadline) {
    status = await sandboxStatus(c, id);
    if (want.includes(status)) return status;
    await c.page.waitForTimeout(2000);
  }
  return status;
}

// The residents of a v2 drain: by default running, running and paused
// sandboxes in the managing organization, all on `node`, which the step pins
// them to by cordoning every other worker while it creates them. A v2 drain
// refuses a node that holds a stopped sandbox, so none is stopped.
const ALL_RESIDENTS = [['run-a', null], ['run-b', null], ['paused', 'pause']] as const;

async function createResidents(c: Ctx, node: AdminNode, kinds: readonly (readonly [string, 'pause' | null])[] = ALL_RESIDENTS): Promise<string[]> {
  const api = await stepApi(c);
  const others = (await adminNodes(c)).filter((n) => n.role === 'worker' && n.id !== node.id);
  const managing = seeded(c).managingOrgId;
  const ids: string[] = [];
  for (const other of others) c.rec.check((await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${other.id}:cordon`)).status === 200, `${other.name} is cordoned while the residents are placed`);
  try {
    for (const [suffix] of kinds) {
      const name = `${c.runTag}-v2-${suffix}`;
      const accepted = await api.admin<{ operation_id: string; sandbox_id?: string }>('POST', '/sandboxes', { org_id: managing, sandbox: { template: 'seed:latest', runtime: 'fc', name } }, { idempotent: true, timeoutMs: 120_000 });
      if (accepted.sandbox_id) recordCreated(c.config, { kind: 'sandbox', id: sbxId(accepted.sandbox_id), name, orgId: managing, createOperation: { id: accepted.operation_id, admin: true } });
      // The answer names the sandbox before its operation has made it.
      const op = await api.waitForOperation(accepted.operation_id, { admin: true, timeoutMs: 300_000 });
      const target = accepted.sandbox_id ?? op.target_id;
      c.rec.check(op.state === 'succeeded' && !!target, `the create of ${name} succeeds (${op.state})`);
      if (!target) continue;
      recordCreated(c.config, { kind: 'sandbox', id: sbxId(target), name, orgId: managing });
      ids.push(sbxId(target));
    }
    for (const id of ids) c.rec.check(await waitForSandbox(c, id, ['started'], 300_000) === 'started', `${id} starts`);
  } finally {
    for (const other of others) await adminFetch(c.page, 'POST', `/admin/api/v1/nodes/${other.id}:uncordon`).catch(() => null);
  }
  for (const id of ids) {
    const row = await api.admin<{ active_node_id: string | null; active_node?: { name: string } }>('GET', `/sandboxes/${id}`);
    c.rec.check(row.active_node_id === node.id, `${id} runs on ${node.name} (${row.active_node?.name})`);
  }
  for (const [index, [, action]] of kinds.entries()) {
    if (!action || !ids[index]) continue;
    const state = 'paused';
    const accepted = await api.admin<{ operation_id: string }>('POST', `/sandboxes/${ids[index]}:force-${action}`, undefined, { idempotent: true, timeoutMs: 120_000 });
    const op = await api.waitForOperation(accepted.operation_id, { admin: true, timeoutMs: 300_000 });
    c.rec.check(op.state === 'succeeded', `${action} ${ids[index]} succeeds (${op.state})`);
    c.rec.check(await waitForSandbox(c, ids[index], [state], 300_000) === state, `${ids[index]} is ${state}`);
  }
  return ids;
}

// Deletes the residents and waits for them to go.
async function deleteResidents(c: Ctx, ids: string[]): Promise<void> {
  const api = await stepApi(c);
  for (const id of ids) {
    try {
      const accepted = await api.admin<{ operation_id: string }>('POST', `/sandboxes/${id}:force-delete`, undefined, { idempotent: true, timeoutMs: 120_000 });
      const op = await api.waitForOperation(accepted.operation_id, { admin: true, timeoutMs: 300_000 });
      c.rec.note(`cleanup: delete ${id}: ${op.state}`);
    } catch (error) {
      c.rec.check(false, `cleanup: deleting ${id} failed: ${String(error).split('\n')[0]}`);
    }
  }
  for (const id of ids) {
    const status = await waitForSandbox(c, id, ['deleted', null], 300_000);
    c.rec.check(status === 'deleted' || status === null, `cleanup: ${id} is deleted (${status})`);
  }
}

const HOLD_SECONDS = 150;

step('node-drain-progress', V2_TAGS, async (c) => {
  await withV2Worker(c, async (node) => {
    const phrase = node.name ?? node.id;
    let residents: string[] = [];
    const answers = watchNodeAnswers(c.page, node.id);
    // Every report the loop reads, and each resident's evacuated entry the
    // first time a report lists it: the node's report lists only what is
    // still on the node, so a sandbox the drain has released drops out.
    const timeline: { at: string; phase: string | null; drain: ApiDrain | undefined }[] = [];
    const firstEvacuated = new Map<string, ApiDrain['evacuated'][number]>();
    const seen = (n: AdminNode) => {
      const last = timeline.at(-1);
      if (!last || JSON.stringify(last.drain) !== JSON.stringify(n.drain) || last.phase !== n.drain_phase) {
        timeline.push({ at: new Date().toISOString(), phase: n.drain_phase, drain: n.drain });
      }
      for (const entry of n.drain?.evacuated ?? []) if (!firstEvacuated.has(bareSandboxId(entry.sandbox_id))) firstEvacuated.set(bareSandboxId(entry.sandbox_id), entry);
    };
    // The loop reads the node every 2 s for the whole flow.
    let polling = true;
    const poll = (async () => {
      while (polling) {
        const n = await apiNode(c, node.id).catch(() => null);
        if (n) seen(n);
        await sleep(2000);
      }
    })();
    try {
      residents = await createResidents(c, node);
      const [runA, runB, paused] = residents;
      await visit(c, `/dashboard/nodes/${node.id}`, 3000);
      await shot(c, '01-before');
      const dialog = await openTyped(c, 'v2 drain');
      await pickHold(dialog, HOLD_SECONDS);
      await dialog.getByLabel('Type the node name to confirm').fill(phrase);
      await shot(c, '02-confirm', { dialog: true });
      const started = await confirmTyped(c, dialog, 'v2 drain', node.id, 'drain', phrase);
      c.rec.check(started?.status() === 200, `the v2 drain is accepted (${started?.status()})`);
      const startedAt = Date.now();
      const generation = (await waitForNode(c, node.id, (n) => n.drain_engine === 'v2' && !!n.drain, 30_000))?.drain?.generation;
      c.rec.check(generation !== undefined, `the drain records its generation (${generation})`);

      // Before the deadline: the running sandboxes are held and the paused
      // one suspended at once. A report
      // stored under the drain's generation is what shows the node answered.
      const holds = (d: ApiDrain) => reportedUnder(d, generation)
        && [runA, runB].every((id) => d.pending.some((p) => p.wait === HOLD_WAIT && p.running && bareSandboxId(p.sandbox_id) === bareSandboxId(id)));
      const holding = await waitForNode(c, node.id, (n) => !!n.drain && holds(n.drain), 120_000);
      c.rec.check(!!holding, 'the node holds both running sandboxes until the deadline, each pending hold and running');
      await panelMatchesAnswer(c, answers, residents, 'holding', holds);
      c.rec.check(/running until the deadline/.test(await progressCard(c.page).innerText()), 'a held sandbox reads "running until the deadline"');
      await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
      await panelShot(c, '03-holding');
      const early = await waitForNode(c, node.id, (n) => !!n.drain?.evacuated.some((e) => bareSandboxId(e.sandbox_id) === bareSandboxId(paused)), 90_000);
      c.rec.check(!!early, 'the paused sandbox is evacuated before the deadline');
      const earlyDone = (d: ApiDrain) => d.evacuated.some((e) => bareSandboxId(e.sandbox_id) === bareSandboxId(paused));
      const earlyDrain = await panelMatchesAnswer(c, answers, residents, 'evacuated before the deadline', earlyDone);
      if (earlyDrain) {
        const kind = (id: string) => earlyDrain.evacuated.find((e) => bareSandboxId(e.sandbox_id) === bareSandboxId(id));
        c.rec.check(kind(paused)?.artifact_kind === 'full' && kind(paused)?.was_running === false, 'the paused sandbox is a snapshot that was not running');
      }
      await panelShot(c, '04-evacuated-early');

      // The nodes list's cell.
      await visit(c, '/dashboard/nodes', 2500);
      const cell = (await content(c.page).locator('tr', { has: c.page.locator(`a[href="/dashboard/nodes/${node.id}"]`) }).innerText().catch(() => '')).replace(/\s+/g, ' ');
      c.rec.check(/[0-9]+\/3 evacuated · deadline \d+(s|m)/.test(cell), `the nodes list reads evacuated of 3 and the deadline (${cell.slice(0, 160)})`);
      await shot(c, '05-nodes-list');
      await visit(c, `/dashboard/nodes/${node.id}`, 2500);

      // A warm-pool member, stalled and cancelling: the node's own answer
      // with the fields that make each set by the step, as the API reports
      // them. Stalled is past the deadline with nothing evacuated for 15
      // minutes since `evacuated_grew_at`.
      const POOL_MEMBER = '01a1f4900000700080000000000000ee';
      for (const [name, change, pattern] of [
        ['06-pool-member', (d: any) => ({ ...d, pending: [...d.pending, { sandbox_id: POOL_MEMBER, wait: 'external', running: false, pool_member: true, since: null }] }), /warm-pool member.*\(1 warm-pool\)|\(1 warm-pool\).*warm-pool member/],
        ['07-stalled', (d: any) => ({ ...d, deadline: new Date(Date.now() - 20 * 60_000).toISOString(), evacuated_grew_at: new Date(Date.now() - 16 * 60_000).toISOString() }), /stalled since/],
        ['08-cancelling', (d: any) => ({ ...d, cancelling: true }), /Undrained: cancelling the drain on the node/],
      ] as const) {
        const unroute = await overlayNode(c, node.id, (n) => (n.drain ? {
          ...n,
          drain: change(n.drain),
          ...(name === '07-stalled' ? { drain_last_error: 'the drain is stalled: synthetic' } : {}),
          ...(name === '08-cancelling' ? { drain_phase: null } : {}),
        } : n));
        try {
          c.rec.note(`SYNTHETIC: ${name}: the node's own answer with one field set by the step`);
          c.rec.check(await waitForPanel(c, pattern, 15_000), `${name}: the panel shows it (${pattern})`);
          if (name === '06-pool-member') {
            const row = (await readPanel(c.page)).pending.find((r) => bareSandboxId(r.sandbox) === POOL_MEMBER);
            c.rec.check(!!row?.pool && row.wait === 'external' && !row.running, `${name}: the member's row is labelled a warm-pool member, with its wait and not running (${JSON.stringify(row)})`);
          }
          if (name === '07-stalled') {
            const listUrl = (url: URL) => url.pathname === '/admin/api/v1/nodes';
            const list = async (route: Route) => {
              const res = await fetchWithoutRedirect(route);
              if (!res) return;
              const body = await res.json().catch(() => null);
              const changed = Array.isArray(body) ? body.map((n: any) => (n.id === node.id && n.drain ? { ...n, drain: change(n.drain) } : n)) : body;
              return route.fulfill({ status: res.status(), contentType: 'application/json', headers: { [SYNTHETIC_HEADER]: '1' }, body: JSON.stringify(changed) });
            };
            await c.page.route(listUrl, list);
            await visit(c, '/dashboard/nodes', 2000);
            c.rec.check(/stalled/.test(await content(c.page).locator('tr', { has: c.page.locator(`a[href="/dashboard/nodes/${node.id}"]`) }).innerText().catch(() => '')), 'the nodes list marks the drain stalled');
            await shot(c, '07b-nodes-list-stalled');
            await c.page.unroute(listUrl, list);
            await visit(c, `/dashboard/nodes/${node.id}`, 2000);
            await waitForPanel(c, pattern, 10_000);
          }
          await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
          await panelShot(c, name);
        } finally {
          await unroute();
        }
      }

      // Past the deadline: the node suspends what it held.
      const due = startedAt + HOLD_SECONDS * 1000 - Date.now();
      if (due > 0) await c.page.waitForTimeout(due);
      const suspending = await waitForNode(c, node.id, (n) => !!n.drain && n.drain.deadline !== null && Date.now() >= Date.parse(n.drain.deadline) && n.drain.pending.some((p) => p.wait !== HOLD_WAIT), 60_000, 1000);
      // The node lists the held sandboxes pending, moving, for a few seconds;
      // the page polls every 5 s, so its own answers may miss them.
      const pendingSuspends = (d: ApiDrain) => !!d.deadline && Date.now() >= Date.parse(d.deadline) && d.pending.some((p) => p.wait !== HOLD_WAIT);
      const pageCaught = async () => {
        const by = Date.now() + 15_000;
        while (Date.now() < by) {
          const latest = answers.latest()?.body?.drain;
          if (latest && pendingSuspends(latest)) return true;
          await c.page.waitForTimeout(250);
        }
        return false;
      };
      if (suspending && await pageCaught()) {
        await panelMatchesAnswer(c, answers, residents, 'past the deadline, pending suspends', pendingSuspends, { current: true });
        await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
        await panelShot(c, '09-past-deadline');
      } else {
        c.rec.note(`the page's poll did not catch the pending suspends past the deadline (${suspending ? 'the loop read them' : 'no read listed them'}); the panel is checked against the next answer`);
        await panelMatchesAnswer(c, answers, residents, 'past the deadline');
        await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
        await panelShot(c, '09-past-deadline');
      }
      // Each resident is reported evacuated, as what it was, by some report:
      // a report lists only what is still on the node.
      const reported = (id: string) => firstEvacuated.get(bareSandboxId(id));
      c.rec.check(!!(await waitForNode(c, node.id, () => residents.every((id) => reported(id)), 300_000, 2000)), 'every resident is reported evacuated');
      c.rec.check([runA, runB].every((id) => reported(id)?.artifact_kind === 'full' && reported(id)?.was_running === true), 'the held sandboxes are reported as snapshots that were running');
      c.rec.check(reported(paused)?.artifact_kind === 'full' && reported(paused)?.was_running === false, 'the paused sandbox is reported as a snapshot that was not running');
      const heldOut = (d: ApiDrain) => [runA, runB].some((id) => d.evacuated.some((e) => bareSandboxId(e.sandbox_id) === bareSandboxId(id)));
      if (answers.latest()?.body?.drain && heldOut(answers.latest()!.body.drain)) {
        await panelMatchesAnswer(c, answers, residents, 'evacuated', heldOut);
        await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
        await panelShot(c, '10-evacuated');
      } else {
        await panelMatchesAnswer(c, answers, residents, 'evacuated');
        await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
        await panelShot(c, '10-evacuated');
      }
      c.rec.check(!!(await waitForNode(c, node.id, (n) => n.drain_phase === 'drained', 300_000)), 'the drain reaches drained');
      await waitForPanel(c, /Finished: everything on the node is durable elsewhere/, 20_000);
      const final = await panelMatchesAnswer(c, answers, residents, 'drained', drainFinished);
      c.rec.check(await waitForSection(c, /Phase\s+drained/, 15_000), 'the Drain section shows drained');
      await progressCard(c.page).scrollIntoViewIfNeeded().catch(() => {});
      await panelShot(c, '11-drained');
      // A sandbox the drain released drops out of the node's report, and
      // stays in `evacuated_total`, which the panel counts from.
      if (final) {
        const total = new Set(final.evacuated_total.map(bareSandboxId));
        const missing = residents.filter((id) => !total.has(bareSandboxId(id)) && !final.evacuated.some((e) => bareSandboxId(e.sandbox_id) === bareSandboxId(id)));
        c.rec.check(missing.length === 0, `the drained drain counts every resident evacuated, a released one through evacuated_total (${final.evacuated.length} in the report, ${final.evacuated_total.length} recorded${missing.length ? `; missing ${missing.join(', ')}` : ''})`);
        const counts = drainCountsOf(final);
        c.rec.check((await progressCard(c.page).innerText().catch(() => '')).includes(`${counts.evacuated}/${counts.total} evacuated`), `the panel reads ${counts.evacuated}/${counts.total} evacuated`);
      }
      await visit(c, '/dashboard/nodes', 2500);
      const done = (await content(c.page).locator('tr', { has: c.page.locator(`a[href="/dashboard/nodes/${node.id}"]`) }).innerText().catch(() => '')).replace(/\s+/g, ' ');
      const finalCounts = final ? drainCountsOf(final) : null;
      const listed = finalCounts ? `${finalCounts.evacuated}/${finalCounts.total} evacuated · finished` : 'evacuated · finished';
      c.rec.check(done.includes(listed), `the nodes list reads "${listed}", as the report says (${done.slice(0, 160)})`);
      await shot(c, '12-nodes-list-done');
    } finally {
      answers.stop();
      polling = false;
      await poll;
      fs.writeFileSync(path.join(c.rec.outDir, 'node-drain-progress-timeline.json'), JSON.stringify(timeline, null, 2));
      // The drain is undrained by withV2Worker; the residents go first, so
      // the worker is empty again for the next run.
      if (residents.length) await deleteResidents(c, residents);
    }
  });
}, true);

step('nodes-v2-teardown', V2_AROUND, async (c) => {
  if (!v2) return c.rec.note('nodes-v2-setup did not run in this run');
  const { control, lease, cordonLease } = v2;
  try {
    const file = v2StandingPath(c);
    const env = putStandingBack(control, lease, file);
    const now = control.controllerEnv();
    c.rec.check(JSON.stringify(now) === JSON.stringify(env), `the controller's standing config is back (${JSON.stringify(now)})`);
    fs.rmSync(file, { force: true });
    c.rec.note('the workers keep their fake EC2 instances, as the v2 smokes leave them');
  } finally {
    control.releaseLease(cordonLease);
    control.releaseLease(lease);
    v2 = null;
  }
}, true);

step('logout', ['auth'], async (c) => {
  await visit(c, '/dashboard', 1500);
  await c.page.getByRole('button', { name: 'Log out' }).click();
  const confirm = c.page.locator('.ant-modal:visible').getByRole('button', { name: 'Log out' });
  if (await confirm.count()) await confirm.click();
  await c.page.waitForURL(/\/login/, { timeout: 10_000 }).catch(() => {});
  c.rec.check(/\/login/.test(c.page.url()), 'signing out lands on the login page');
});
