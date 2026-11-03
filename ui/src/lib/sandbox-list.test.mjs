import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  FIRST_PAGE,
  ListRequests,
  isTemplateName,
  NO_FILTERS,
  continuationFor,
  createdBounds,
  listAnswerPlan,
  listRequestQuery,
  listScope,
  loadedRowFilter,
  nextCursorFor,
  nextPage,
  pageCursor,
  parseListLink,
  parsePageSize,
  parseSandboxListResponse,
  previousPage,
  sameScope,
  sandboxListQuery,
  scopeChangePlan,
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
  const scope = listScope('paged', filters, { limit: 50, cursor: '' });
  // The same view a tab's age later: a poll rereads it rather than starting
  // a new view on page 1.
  assert.ok(sameScope(scope, listScope('paged', { ...filters }, { limit: 50, cursor: '' })));
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

const sampledAnswer = { mode: 'sampled', sandboxes: [] };
const pagedAnswer = { mode: 'paged', sandboxes: [], nextCursor: null };

test('an answer in the shape asked for is shown', () => {
  assert.deepEqual(listAnswerPlan('paged', pagedAnswer), { mode: 'paged', reread: false });
  assert.deepEqual(listAnswerPlan('sampled', sampledAnswer), { mode: 'sampled', reread: false });
});

test('a bare array answering a page request switches to sampling and reads the sample', () => {
  // Its rows were asked for as a page, filtered; the server ignored the
  // filters, and the sample is another size.
  assert.deepEqual(listAnswerPlan('paged', sampledAnswer), { mode: 'sampled', reread: true });
});

test('a sampled tab pages again on the first refresh a paging server answers', () => {
  // A rolling deploy: the first load reached a server without paging...
  let mode = 'paged';
  let plan = listAnswerPlan(mode, sampledAnswer);
  assert.deepEqual(plan, { mode: 'sampled', reread: true });
  mode = plan.mode;
  plan = listAnswerPlan(mode, sampledAnswer);
  assert.deepEqual(plan, { mode: 'sampled', reread: false });
  // ...and a later refresh, sent in the same form, reaches one that pages.
  // Its answer is a page of the unfiltered list at the sample size, so the
  // first page is read again with the page size and the filters.
  plan = listAnswerPlan(mode, pagedAnswer);
  assert.deepEqual(plan, { mode: 'paged', reread: true });
  mode = plan.mode;
  assert.deepEqual(listAnswerPlan(mode, pagedAnswer), { mode: 'paged', reread: false });
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
  // Page 2 failed: the console clears the continuation, and Next stays off
  // until a page answers.
  assert.equal(nextCursorFor(null, page2), null);
  assert.equal(nextCursorFor(continuationFor(page2, 'c2'), page2), 'c2');
  // Another filter on the same page position is another scope.
  assert.equal(nextCursorFor(fromPage1, scopeOf({ template: 'beta' })), null);
  // The last page has no continuation.
  assert.equal(continuationFor(page2, null), null);
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
