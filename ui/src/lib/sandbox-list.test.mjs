import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  FIRST_PAGE,
  NO_FILTERS,
  clientFilters,
  createdBounds,
  listAnswerPlan,
  listRequestQuery,
  listScope,
  nextPage,
  pageCursor,
  parsePageSize,
  parseSandboxListResponse,
  previousPage,
  sameScope,
  sandboxListQuery,
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

test('a sampled request sends what the list took before paging: the size and the org only', () => {
  const filters = { ...NO_FILTERS, org: '0193a000-0000-7000-8000-000000000001', template: 'seed', lifecycles: ['started'], prefix: 'a' };
  assert.deepEqual(parse(sandboxListQuery('sampled', filters, { limit: 1000, cursor: 'ignored', now: NOW })), {
    limit: '1000',
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

test('a paged list leaves only the node filter to the console; a sampled one filters everything but the org', () => {
  assert.deepEqual(clientFilters('paged'), { text: false, template: false, status: false, user: false, created: false, node: true });
  assert.deepEqual(clientFilters('sampled'), { text: true, template: true, status: true, user: true, created: true, node: true });
});

test('a bare array answering a paged request switches to sampling and reads the sample size', () => {
  const sampled = { mode: 'sampled', sandboxes: [] };
  const paged = { mode: 'paged', sandboxes: [], nextCursor: null };
  const sizes = { pageSize: 50, sampleLimit: 1000 };
  assert.deepEqual(listAnswerPlan('paged', sampled, sizes), { mode: 'sampled', reloadSample: true });
  // The answer already holds the sample the console wanted.
  assert.deepEqual(listAnswerPlan('paged', sampled, { pageSize: 100, sampleLimit: 100 }), { mode: 'sampled', reloadSample: false });
  // A sampled request was already the sample.
  assert.deepEqual(listAnswerPlan('sampled', sampled, sizes), { mode: 'sampled', reloadSample: false });
  assert.deepEqual(listAnswerPlan('paged', paged, sizes), { mode: 'paged', reloadSample: false });
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
