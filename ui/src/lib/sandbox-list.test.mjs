import assert from 'node:assert/strict';
import { mock, test } from 'node:test';
import {
  FIRST_PAGE,
  ListRequests,
  ListSelection,
  isTemplateName,
  NO_FILTERS,
  continuationFor,
  createdBounds,
  listRequestQuery,
  listScope,
  listSections,
  loadedRowFilter,
  nextCursorFor,
  nextPage,
  nodeReportedPlacement,
  pageCursor,
  parseListLink,
  parsePageSize,
  parseSandboxListResponse,
  previousPage,
  sameScope,
  sandboxListQuery,
  scopeChangePlan,
  splitNodeReported,
  templateFilterOptions,
} from './sandbox-list.ts';

const sandbox = { id: 'sbx_01a19089-216f-751e-9f56-1906865f5c23', resource_type: 'sandbox' };

test('a page object is a paged answer, and a bare array is a server without paging', () => {
  assert.deepEqual(parseSandboxListResponse({ sandboxes: [sandbox], next_cursor: 'abc' }), {
    mode: 'paged', sandboxes: [sandbox], nextCursor: 'abc',
  });
  // The last page.
  assert.deepEqual(parseSandboxListResponse({ sandboxes: [], next_cursor: null }), {
    mode: 'paged', sandboxes: [], nextCursor: null,
  });
  assert.deepEqual(parseSandboxListResponse({ sandboxes: [sandbox] }).nextCursor, null);
  // An empty cursor would reread the first page, so it ends the walk.
  assert.equal(parseSandboxListResponse({ sandboxes: [], next_cursor: '' }).nextCursor, null);
  assert.deepEqual(parseSandboxListResponse([sandbox]), { mode: 'sampled', sandboxes: [sandbox] });
  assert.deepEqual(parseSandboxListResponse([]), { mode: 'sampled', sandboxes: [] });
});

test('any other answer is refused rather than shown as an empty list', () => {
  for (const body of [null, undefined, 'text', 7, {}, { error: 'boom' }, { sandboxes: {} }, { sandboxes: [], next_cursor: 3 }]) {
    assert.throws(() => parseSandboxListResponse(body), /unrecognized sandbox list response/, JSON.stringify(body));
  }
});

const parse = (query) => Object.fromEntries(new URLSearchParams(query));
const NOW = Date.parse('2026-11-03T12:00:00.000Z');
const HOUR = 60 * 60 * 1000;

test('every set filter maps to its list parameter on a paged request', () => {
  const query = sandboxListQuery('paged', {
    org: '0193a000-0000-7000-8000-000000000001',
    lifecycles: ['started', 'deleted'],
    template: 'seed',
    userId: '0193a000-0000-7000-8000-000000000002',
    createdWithin: '30d',
    olderThan: '24h',
    prefix: '  Build-  ',
  }, { limit: 50, cursor: 'djE6MTo', now: NOW });
  assert.deepEqual(parse(query), {
    limit: '50',
    cursor: 'djE6MTo',
    org_id: '0193a000-0000-7000-8000-000000000001',
    lifecycle: 'started,deleted',
    template: 'seed',
    user_id: '0193a000-0000-7000-8000-000000000002',
    created_after: '2026-10-04T12:00:00.000Z',
    created_before: '2026-11-02T12:00:00.000Z',
    // Trimmed, case kept: a name prefix is case-sensitive.
    prefix: 'Build-',
  });
});

test('the first page sends an empty cursor, and unset filters send nothing', () => {
  assert.equal(sandboxListQuery('paged', NO_FILTERS, { limit: 25, now: NOW }), 'limit=25&cursor=');
  assert.deepEqual(parse(sandboxListQuery('paged', { ...NO_FILTERS, prefix: ' \0 ' }, { limit: 25, now: NOW })), { limit: '25', cursor: '' });
});

test('a sampled request is the first page at the sample size, with the org only', () => {
  const filters = { ...NO_FILTERS, org: '0193a000-0000-7000-8000-000000000001', template: 'seed', lifecycles: ['started'], prefix: 'a' };
  // A server without paging ignores the cursor and answers the sample; one
  // that pages answers a page, which is the probe.
  assert.deepEqual(parse(sandboxListQuery('sampled', filters, { limit: 1000, cursor: 'ignored', now: NOW })), {
    limit: '1000',
    cursor: '',
    org_id: '0193a000-0000-7000-8000-000000000001',
  });
});

test('a relative created bound is computed when each request is sent, and does not change the scope', () => {
  const filters = { ...NO_FILTERS, createdWithin: '1h' };
  mock.timers.enable({ apis: ['Date'], now: NOW });
  let scope;
  try {
    scope = listScope('paged', filters, { limit: 50, cursor: '' });
    // The same view a tab's age later: a poll rereads it rather than
    // starting a new view on page 1.
    mock.timers.tick(3 * HOUR);
    assert.ok(sameScope(scope, listScope('paged', { ...filters }, { limit: 50, cursor: '' })));
  } finally {
    mock.timers.reset();
  }
  assert.equal(parse(listRequestQuery(scope, NOW)).created_after, '2026-11-03T11:00:00.000Z');
  assert.equal(parse(listRequestQuery(scope, NOW + 3 * HOUR)).created_after, '2026-11-03T14:00:00.000Z');
  const older = listScope('paged', { ...NO_FILTERS, olderThan: '24h' }, { limit: 50, cursor: '' });
  assert.equal(parse(listRequestQuery(older, NOW + 24 * HOUR)).created_before, '2026-11-03T12:00:00.000Z');
  // Another window, page or size is another scope.
  assert.ok(!sameScope(scope, listScope('paged', { ...NO_FILTERS, createdWithin: '24h' }, { limit: 50, cursor: '' })));
  assert.ok(!sameScope(scope, listScope('paged', filters, { limit: 50, cursor: 'c1' })));
  assert.ok(!sameScope(scope, listScope('paged', filters, { limit: 25, cursor: '' })));
});

test('a sampled scope is its size and org: the filters it does not send do not change it', () => {
  const org = '0193a000-0000-7000-8000-000000000001';
  const a = listScope('sampled', { ...NO_FILTERS, org }, { limit: 1000, cursor: '' });
  const b = listScope('sampled', { ...NO_FILTERS, org, template: 'seed', createdWithin: '1h' }, { limit: 1000, cursor: 'c1' });
  assert.ok(sameScope(a, b));
  assert.ok(!sameScope(a, listScope('paged', { ...NO_FILTERS, org }, { limit: 1000, cursor: '' })));
  assert.ok(!sameScope(a, null));
});

const USER = '0193a000-0000-7000-8000-000000000002';
const row = (fields) => ({
  template: 'seed', status: 'started', user_id: USER, active_node_id: 'node-a', created_at: '2026-11-03T11:30:00.000Z', name: 'build-a', ...fields,
});
const criteria = (fields) => ({
  matchesSearch: null, template: null, statuses: [], statusKey: (status) => status || 'unknown', userId: null,
  node: 'any', createdWithin: null, olderThan: null, now: NOW, ...fields,
});

test('a paged list does not refilter what the server applied, but filters the node and a page search', () => {
  // Every row of a page matched the filters the server was sent; a row with
  // another template shows the one its node reports, and stays.
  const applied = criteria({ template: 'other', statuses: ['stopped'], userId: '0193a000-0000-7000-8000-000000000003', createdWithin: '1h', olderThan: '1h' });
  assert.equal(loadedRowFilter('paged', applied)(row({ created_at: null })), true);
  // The server has no node filter.
  assert.equal(loadedRowFilter('paged', criteria({ node: 'unassigned' }))(row({})), false);
  assert.equal(loadedRowFilter('paged', criteria({ node: 'assigned' }))(row({ active_node_id: null })), false);
  // A search the server could not answer.
  const search = criteria({ matchesSearch: (r) => r.name.includes('cache') });
  assert.equal(loadedRowFilter('paged', search)(row({})), false);
  assert.equal(loadedRowFilter('paged', search)(row({ name: 'build-cache' })), true);
});

test('a sampled list filters every loaded row on everything but the org', () => {
  const keep = (fields, rowFields = {}) => loadedRowFilter('sampled', criteria(fields))(row(rowFields));
  assert.equal(keep({}), true);
  assert.equal(keep({ template: 'other' }), false);
  assert.equal(keep({ statuses: ['stopped'] }), false);
  assert.equal(keep({ statuses: ['unknown'] }, { status: null }), true);
  assert.equal(keep({ userId: '0193a000-0000-7000-8000-000000000003' }), false);
  assert.equal(keep({ node: 'unassigned' }), false);
  // Within the last hour: 11:30 at 12:00 is, and a row with no time is not.
  assert.equal(keep({ createdWithin: '1h' }), true);
  assert.equal(keep({ createdWithin: '1h' }, { created_at: '2026-11-03T10:59:00.000Z' }), false);
  assert.equal(keep({ createdWithin: '1h' }, { created_at: null }), false);
  assert.equal(keep({ olderThan: '1h' }), false);
  assert.equal(keep({ olderThan: '1h' }, { created_at: '2026-11-03T10:00:00.000Z' }), true);
});

const scopeOf = (filters, cursor = '') => listScope('paged', { ...NO_FILTERS, ...filters }, { limit: 50, cursor });

test('an answer for a filter undone before it arrived is dropped, and the rows on screen are kept', () => {
  const requests = new ListRequests();
  const a = scopeOf({ template: 'alpha' });
  const b = scopeOf({ template: 'beta' });
  // A is on screen.
  let shown = a;
  // B is selected and requested.
  requests.invalidate();
  assert.equal(scopeChangePlan(shown, b), 'load');
  const forB = requests.begin(b);
  // A is selected again before B answers: nothing is reloaded...
  requests.invalidate();
  assert.equal(scopeChangePlan(shown, a), 'keep');
  // ...and B's answer, still the newest request, does not apply under A.
  assert.equal(requests.applies(forB, a), false);
  // Nor would it if B were selected again: it was sent before the change.
  assert.equal(requests.applies(forB, b), false);
  // A poll of A begun before the change is dropped too; the next one applies.
  const pollA = requests.begin(a);
  requests.invalidate();
  assert.equal(requests.applies(pollA, a), false);
  const nextPoll = requests.begin(a);
  assert.equal(requests.applies(nextPoll, a), true);
  shown = null;
  assert.equal(scopeChangePlan(shown, a), 'load');
});

test('a newer request for the same scope supersedes an older one', () => {
  const requests = new ListRequests();
  const a = scopeOf({});
  const first = requests.begin(a);
  const second = requests.begin(a);
  assert.equal(requests.applies(first, a), false);
  assert.equal(requests.applies(second, a), true);
});

test('Next takes the cursor only while the page that returned it is selected', () => {
  const page1 = scopeOf({ template: 'alpha' });
  const fromPage1 = continuationFor(page1, 'c1');
  assert.equal(nextCursorFor(fromPage1, page1), 'c1');
  // Next was clicked: page 2 is selected and loading, so page 1's cursor
  // does not lead anywhere from it.
  const page2 = scopeOf({ template: 'alpha' }, 'c1');
  assert.equal(nextCursorFor(fromPage1, page2), null);
  // A failed load is in 'Next follows the page shown, ...' below.
  assert.equal(nextCursorFor(continuationFor(page2, 'c2'), page2), 'c2');
  // Another filter on the same page position is another scope.
  assert.equal(nextCursorFor(fromPage1, scopeOf({ template: 'beta' })), null);
  // The last page has no continuation.
  assert.equal(continuationFor(page2, null), null);
});

// A list read that answers `answers` in turn, and records the scopes sent.
// An Error answer is thrown; a request past the last answer fails the test.
function reads(...answers) {
  const sent = [];
  const read = async (scope) => {
    sent.push(scope);
    if (!answers.length) throw new Error(`unexpected request ${sent.length}`);
    const answer = answers.shift();
    if (answer instanceof Error) throw answer;
    return answer;
  };
  return { sent, read };
}

// A read that answers when `answer` is called.
function pendingRead() {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { read: () => promise, answer: (value) => resolve(value) };
}

const page = (nextCursor, rows = [sandbox]) => ({ mode: 'paged', sandboxes: rows, nextCursor });
const newSelection = (filters = {}) =>
  new ListSelection({ filters: { ...NO_FILTERS, ...filters }, pageSize: 50, sampleSize: 1000 });

test('Next follows the page shown, and a failed load leaves it off until a page answers', async () => {
  const selection = newSelection({ template: 'alpha' });
  assert.equal((await selection.load(selection.selected, reads(page('c1')).read)).kind, 'shown');
  assert.equal(selection.nextCursor(), 'c1');
  // Next selects page 2, and page 1's cursor does not lead on from it.
  assert.equal(selection.goTo(selection.nextPosition()), true);
  assert.equal(selection.selected.cursor, 'c1');
  assert.equal(selection.nextCursor(), null);
  // Page 2 fails. The rows on screen are page 1's, so they are cleared, and
  // Next stays off.
  const failed = await selection.load(selection.selected, reads(new Error('unavailable')).read);
  assert.equal(failed.kind, 'failed');
  assert.equal(failed.kept, false);
  assert.equal(selection.shown, null);
  assert.equal(selection.nextPosition(), null);
  // The retry answers, with a page after it.
  assert.equal((await selection.load(selection.selected, reads(page('c2')).read)).kind, 'shown');
  assert.equal(selection.nextCursor(), 'c2');
  // A refresh of page 2 fails: its rows stay on screen, and Next is off
  // until a page answers again.
  const refreshFailed = await selection.load(selection.selected, reads(new Error('unavailable')).read);
  assert.equal(refreshFailed.kind, 'failed');
  assert.equal(refreshFailed.kept, true);
  assert.equal(selection.nextCursor(), null);
  assert.equal(selection.nextPosition(), null);
  // Previous goes back by the visited cursor.
  assert.equal(selection.goTo(selection.previousPosition()), true);
  assert.equal(selection.selected.cursor, '');
  assert.equal(selection.goTo(selection.previousPosition()), false);
});

test('an answer begun before Previous or Next is dropped, even when its page is selected again', async () => {
  const selection = newSelection();
  await selection.load(selection.selected, reads(page('c1')).read);
  selection.goTo(selection.nextPosition());
  const pageTwo = selection.selected;
  const slow = pendingRead();
  const inFlight = selection.load(pageTwo, slow.read);
  // Previous, then page 2 again, before page 2 answered.
  selection.goTo(selection.previousPosition());
  selection.goTo({ cursors: ['', 'c1'], index: 1 });
  assert.equal(sameScope(selection.selected, pageTwo), true);
  slow.answer(page(null));
  assert.deepEqual(await inFlight, { kind: 'skipped' });
  // A load the selection moved away from before it started is skipped too.
  selection.goTo(selection.previousPosition());
  assert.deepEqual(await selection.load(pageTwo, reads().read), { kind: 'skipped' });
});

test('a filter undone before its answer arrived keeps the rows on screen and drops the answer', async () => {
  const selection = newSelection({ template: 'alpha' });
  await selection.load(selection.selected, reads(page(null)).read);
  const sizes = { pageSize: 50, sampleSize: 1000 };
  assert.equal(selection.select({ filters: { ...NO_FILTERS, template: 'beta' }, ...sizes }), 'load');
  const slow = pendingRead();
  const forBeta = selection.load(selection.selected, slow.read);
  assert.equal(selection.select({ filters: { ...NO_FILTERS, template: 'alpha' }, ...sizes }), 'keep');
  // Beta again, before its first answer arrived: that answer was sent
  // before the change and is dropped.
  assert.equal(selection.select({ filters: { ...NO_FILTERS, template: 'beta' }, ...sizes }), 'load');
  slow.answer(page(null));
  assert.deepEqual(await forBeta, { kind: 'skipped' });
  // A change the request does not carry changes nothing.
  assert.equal(selection.select({ filters: { ...NO_FILTERS, template: 'beta' }, ...sizes, sampleSize: 100 }), 'unchanged');
});

const sampledAnswer = { mode: 'sampled', sandboxes: [] };
const sample = (rows = [sandbox]) => ({ mode: 'sampled', sandboxes: rows });

test('the first answer decides the list shape: a bare array switches to sampling and reads the sample', async () => {
  const org = '0193a000-0000-7000-8000-000000000001';
  const selection = newSelection({ org, template: 'alpha', prefix: 'build' });
  // Its rows were asked for as a page, filtered; the server ignored the
  // filters, and the sample is another size.
  const first = reads(sampledAnswer, sample());
  const result = await selection.load(selection.selected, first.read);
  assert.equal(result.kind, 'shown');
  assert.equal(result.scope.mode, 'sampled');
  assert.equal(selection.mode, 'sampled');
  assert.deepEqual(first.sent.map((scope) => Object.fromEntries(new URLSearchParams(listRequestQuery(scope, 0)))), [
    { limit: '50', cursor: '', org_id: org, template: 'alpha', prefix: 'build' },
    { limit: '1000', cursor: '', org_id: org },
  ]);
  // A sample answers the next refresh too.
  assert.equal((await selection.load(selection.selected, reads(sample()).read)).kind, 'shown');
});

// After `stopped`, no load reads anything: `reads()` fails on any request.
async function assertStaysStopped(selection, kept) {
  assert.equal(selection.stopped, true);
  assert.equal(selection.nextPosition(), null);
  assert.deepEqual(await selection.load(selection.selected, reads().read), { kind: 'stopped', kept });
}

test('a shape change on a refresh of the shown scope stops the list and keeps its rows', async () => {
  // A page view that pages, then a refresh answered by a server without paging.
  const paged = newSelection({ template: 'alpha' });
  await paged.load(paged.selected, reads(page('c1')).read);
  const pageOne = paged.selected;
  const refresh = reads(sampledAnswer);
  assert.deepEqual(await paged.load(pageOne, refresh.read), { kind: 'stopped', kept: true });
  // No sample is read, and the list stays as it was, without Next.
  assert.equal(refresh.sent.length, 1);
  assert.equal(paged.mode, 'paged');
  assert.equal(sameScope(paged.shown, pageOne), true);
  await assertStaysStopped(paged, true);

  // A page view that samples, then a refresh answered by a server that pages.
  const sampled = newSelection();
  await sampled.load(sampled.selected, reads(sampledAnswer, sample()).read);
  const sampleScope = sampled.selected;
  const probe = reads(page('c1'));
  assert.deepEqual(await sampled.load(sampleScope, probe.read), { kind: 'stopped', kept: true });
  assert.equal(probe.sent.length, 1);
  assert.equal(sampled.mode, 'sampled');
  assert.equal(sameScope(sampled.shown, sampleScope), true);
  await assertStaysStopped(sampled, true);
});

test('a shape change after a filter change stops the list and clears the other scope\'s rows', async () => {
  const sizes = { pageSize: 50, sampleSize: 1000 };
  const orgA = { ...NO_FILTERS, org: '0193a000-0000-7000-8000-000000000001' };
  const orgB = { ...NO_FILTERS, org: '0193a000-0000-7000-8000-000000000003' };
  const selection = newSelection(orgA);
  await selection.load(selection.selected, reads(page('c1')).read);
  // B is selected while A's page is shown, and answered by a bare array.
  assert.equal(selection.select({ filters: orgB, ...sizes }), 'load');
  const forB = reads(sampledAnswer);
  assert.deepEqual(await selection.load(selection.selected, forB.read), { kind: 'stopped', kept: false });
  assert.equal(forB.sent.length, 1);
  assert.equal(selection.shown, null);
  await assertStaysStopped(selection, false);
  // A again: its rows were cleared, so there is nothing to keep, and nothing is read.
  assert.equal(selection.select({ filters: orgA, ...sizes }), 'load');
  await assertStaysStopped(selection, false);

  // A sampled list, whose sample request a server that pages answers after
  // an org change.
  const sampled = newSelection(orgA);
  await sampled.load(sampled.selected, reads(sampledAnswer, sample()).read);
  assert.equal(sampled.select({ filters: orgB, ...sizes }), 'load');
  assert.deepEqual(await sampled.load(sampled.selected, reads(page(null)).read), { kind: 'stopped', kept: false });
  await assertStaysStopped(sampled, false);
});

test('a shape change after Next stops the list and clears the previous page', async () => {
  const selection = newSelection({ template: 'alpha' });
  await selection.load(selection.selected, reads(page('c1')).read);
  assert.equal(selection.goTo(selection.nextPosition()), true);
  const pageTwo = reads(sampledAnswer);
  assert.deepEqual(await selection.load(selection.selected, pageTwo.read), { kind: 'stopped', kept: false });
  assert.equal(pageTwo.sent.length, 1);
  assert.equal(selection.shown, null);
  assert.equal(selection.mode, 'paged');
  await assertStaysStopped(selection, false);
  // Previous does not bring page 1's rows back without a read.
  assert.equal(selection.goTo(selection.previousPosition()), true);
  await assertStaysStopped(selection, false);
});

test('a filter change always restarts paging at the first page', async () => {
  const sizes = { pageSize: 50, sampleSize: 1000 };
  const selection = newSelection({ template: 'alpha' });
  await selection.load(selection.selected, reads(page('c1')).read);
  selection.goTo(selection.nextPosition());
  await selection.load(selection.selected, reads(page('c2')).read);
  // A refresh of page 2 is in flight when the template changes.
  const slow = pendingRead();
  const refresh = selection.load(selection.selected, slow.read);
  assert.equal(selection.select({ filters: { ...NO_FILTERS, template: 'beta' }, ...sizes }), 'load');
  assert.deepEqual(selection.position, FIRST_PAGE);
  assert.equal(selection.selected.cursor, '');
  assert.equal(selection.nextCursor(), null);
  slow.answer(page('c3'));
  assert.deepEqual(await refresh, { kind: 'skipped' });
  // Beta's first page is read with no cursor, and Next follows its answer.
  const beta = reads(page('b1'));
  assert.equal((await selection.load(selection.selected, beta.read)).kind, 'shown');
  assert.equal(new URLSearchParams(listRequestQuery(beta.sent[0], 0)).get('cursor'), '');
  assert.equal(selection.nextCursor(), 'b1');
  // A page size change does too.
  selection.goTo(selection.nextPosition());
  assert.equal(selection.select({ filters: { ...NO_FILTERS, template: 'beta' }, pageSize: 100, sampleSize: 1000 }), 'load');
  assert.deepEqual(selection.position, FIRST_PAGE);

  // On a sampled list, a filter its request does not send changes no request,
  // while the sample is still on its way, and the sample is shown.
  const sampled = newSelection({ template: 'alpha' });
  const deciding = pendingRead();
  let sampleRequest = null;
  const first = sampled.load(sampled.selected, async (scope) => {
    if (scope.mode === 'paged') return sampledAnswer;
    sampleRequest = scope;
    return deciding.read();
  });
  await new Promise((done) => setImmediate(done));
  assert.equal(sampled.mode, 'sampled');
  assert.equal(sampled.select({ filters: { ...NO_FILTERS, template: 'beta' }, ...sizes }), 'unchanged');
  assert.deepEqual(sampled.position, FIRST_PAGE);
  deciding.answer(sample());
  const shown = await first;
  assert.equal(shown.kind, 'shown');
  assert.equal(sameScope(shown.scope, sampleRequest), true);
  assert.equal(sampled.selected.cursor, '');
});

test('a full ID shows the row it finds in the main table, whatever its kind', () => {
  const slot = { id: 'sbx_0193a000-0000-7000-8000-00000000000a', resource_type: 'pool_slot' };
  const observed = { id: 'sbx_0193a000-0000-7000-8000-00000000000b', resource_type: 'observed_local' };
  for (const scope of [scopeOf({ template: 'alpha' }), scopeOf({}, 'c1'), scopeOf({})]) {
    for (const row of [slot, observed, sandbox]) {
      assert.deepEqual(listSections('paged', [row], true), { sandboxes: [row], nodeReported: [] }, scope.key);
    }
    assert.equal(nodeReportedPlacement(scope, true), 'looked-up', scope.key);
  }
  // Without the lookup a page splits its rows, and a filtered page or a
  // later one does not show the node-reported section.
  assert.deepEqual(listSections('paged', [sandbox, slot], false), { sandboxes: [sandbox], nodeReported: [slot] });
  assert.equal(nodeReportedPlacement(scopeOf({ template: 'alpha' }), false), 'filtered');
  assert.equal(nodeReportedPlacement(scopeOf({}, 'c1'), false), 'first-page');
  // A sample is one table.
  assert.deepEqual(listSections('sampled', [sandbox, slot], false), { sandboxes: [sandbox, slot], nodeReported: [] });
});

test('pages are walked forward by next_cursor and back by the visited cursors', () => {
  assert.equal(pageCursor(FIRST_PAGE), '');
  assert.equal(previousPage(FIRST_PAGE), null);
  assert.equal(nextPage(FIRST_PAGE, null), null);
  const second = nextPage(FIRST_PAGE, 'c1');
  const third = nextPage(second, 'c2');
  assert.deepEqual(third, { cursors: ['', 'c1', 'c2'], index: 2 });
  assert.equal(pageCursor(third), 'c2');
  const back = previousPage(third);
  assert.equal(pageCursor(back), 'c1');
  // Going forward again from a revisited page takes that page's own cursor,
  // which a refresh may have changed.
  assert.deepEqual(nextPage(back, 'c2b'), { cursors: ['', 'c1', 'c2b'], index: 2 });
});

test('the page size is one of the offered sizes', () => {
  assert.equal(parsePageSize('100'), 100);
  assert.equal(parsePageSize(null), 50);
  assert.equal(parsePageSize('7'), 50);
  assert.equal(parsePageSize('5000'), 50);
});

test('created bounds are fixed at one instant, and an empty range keeps only the window', () => {
  const now = Date.parse('2026-11-03T12:00:00.000Z');
  const hour = 60 * 60 * 1000;
  assert.deepEqual(createdBounds(null, null, now), { createdAfter: null, createdBefore: null });
  assert.deepEqual(createdBounds(24 * hour, hour, now), {
    createdAfter: '2026-11-02T12:00:00.000Z',
    createdBefore: '2026-11-03T11:00:00.000Z',
  });
  assert.deepEqual(createdBounds(null, 7 * 24 * hour, now), { createdAfter: null, createdBefore: '2026-10-27T12:00:00.000Z' });
  assert.deepEqual(createdBounds(hour, 24 * hour, now), { createdAfter: '2026-11-03T11:00:00.000Z', createdBefore: null });
  assert.deepEqual(createdBounds(hour, hour, now), { createdAfter: '2026-11-03T11:00:00.000Z', createdBefore: null });
});

const link = (query) => parseListLink(new URLSearchParams(query));

test('a link sets each filter the list can apply', () => {
  assert.deepEqual(link('template=seed&status=Started,deleted,started&user=0193A000-0000-7000-8000-000000000002&node=assigned&age=24h&older=1h&page_size=100'), {
    template: 'seed',
    statuses: ['started', 'deleted'],
    user: '0193a000-0000-7000-8000-000000000002',
    node: 'assigned',
    age: '24h',
    older: '1h',
    pageSize: 100,
    ignored: [],
  });
  assert.deepEqual(link(''), {
    template: null, statuses: [], user: null, node: 'any', age: 'all', older: 'any', pageSize: 50, ignored: [],
  });
  // `older` is kept with no window, and an explicit default is no notice.
  assert.equal(link('older=7d').older, '7d');
  assert.deepEqual(link('age=all&older=any&node=any').ignored, []);
});

test('a link\'s filters the list cannot apply are left out and listed', () => {
  const { older, age, ignored } = link('age=1h&older=24h');
  // Shown as set, "older than 24h" within the last hour would have been
  // dropped from the request; it is not shown at all.
  assert.equal(age, '1h');
  assert.equal(older, 'any');
  assert.deepEqual(ignored, [{ param: 'older', value: '24h', reason: 'leaves no time inside the 1h window' }]);
  assert.deepEqual(link('age=1h&older=1h').older, 'any');
  const bad = link('template=Bad name,seed&user=someone&node=nearby&age=2h&older=2d&page_size=7');
  assert.equal(bad.template, null);
  assert.equal(bad.user, null);
  assert.equal(bad.node, 'any');
  assert.equal(bad.age, 'all');
  assert.equal(bad.older, 'any');
  assert.equal(bad.pageSize, 50);
  assert.deepEqual(bad.ignored.map((entry) => entry.param), ['template', 'template', 'user', 'node', 'age', 'older', 'page_size']);
  // A template the server would refuse is never sent.
  assert.equal(link('template=../etc').template, null);
});

test('template names follow the server\'s rules', () => {
  for (const name of ['seed', 'Python-3.12', 'a_b', 'x'.repeat(255)]) assert.equal(isTemplateName(name), true, name);
  for (const name of ['', '.', '..', 'a b', 'a/b', 'x'.repeat(256), 'é']) assert.equal(isTemplateName(name), false, name);
});

test('the node-reported rows are on the first page of the list with no filter', () => {
  assert.equal(nodeReportedPlacement(listScope('paged', NO_FILTERS, { limit: 50, cursor: '' }), false), 'here');
  assert.equal(nodeReportedPlacement(listScope('paged', NO_FILTERS, { limit: 50, cursor: 'c1' }), false), 'first-page');
  // A blank prefix is not sent, so it is no filter.
  assert.equal(nodeReportedPlacement(listScope('paged', { ...NO_FILTERS, prefix: '  ' }, { limit: 50, cursor: '' }), false), 'here');
  for (const filter of [
    { org: '0193a000-0000-7000-8000-000000000001' }, { lifecycles: ['started'] }, { template: 'seed' },
    { userId: '0193a000-0000-7000-8000-000000000002' }, { createdWithin: '1h' }, { olderThan: '1h' }, { prefix: 'build' },
  ]) {
    assert.equal(nodeReportedPlacement(listScope('paged', { ...NO_FILTERS, ...filter }, { limit: 50, cursor: '' }), false), 'filtered', JSON.stringify(filter));
  }
});

test('node-reported rows are split from the page\'s sandboxes, in order', () => {
  const rows = [
    { id: 'a', resource_type: 'sandbox' }, { id: 'p', resource_type: 'pool_slot' },
    { id: 'b', resource_type: 'sandbox' }, { id: 'o', resource_type: 'observed_local' },
  ];
  const { sandboxes, nodeReported } = splitNodeReported(rows);
  assert.deepEqual(sandboxes.map((row) => row.id), ['a', 'b']);
  assert.deepEqual(nodeReported.map((row) => row.id), ['p', 'o']);
});

test('template options are the admin template list, with the loaded and selected ones', () => {
  const options = templateFilterOptions({
    admin: ['seed', 'python'], loaded: ['seed', 'retired', null], own: ['mine'], selected: 'linked', typed: '',
  });
  assert.deepEqual(options, { names: ['linked', 'python', 'retired', 'seed'], typed: null, partial: false });
});

test('without the admin template list the options are partial, and a typed name can be chosen', () => {
  const options = templateFilterOptions({ admin: null, loaded: ['seed'], own: ['mine'], selected: null, typed: ' other-org-template ' });
  assert.deepEqual(options, { names: ['mine', 'seed'], typed: 'other-org-template', partial: true });
  // Not one the server would refuse, nor one already listed.
  assert.equal(templateFilterOptions({ admin: null, loaded: [], own: [], selected: null, typed: 'two words' }).typed, null);
  assert.equal(templateFilterOptions({ admin: null, loaded: ['seed'], own: [], selected: null, typed: 'seed' }).typed, null);
});
