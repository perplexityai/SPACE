import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  FIRST_PAGE,
  NO_FILTERS,
  clientFilters,
  createdBounds,
  listAnswerPlan,
  nextPage,
  pageCursor,
  parsePageSize,
  parseSandboxListResponse,
  previousPage,
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

test('every set filter maps to its list parameter on a paged request', () => {
  const query = sandboxListQuery('paged', {
    org: '0193a000-0000-7000-8000-000000000001',
    lifecycles: ['started', 'deleted'],
    template: 'seed',
    userId: '0193a000-0000-7000-8000-000000000002',
    createdAfter: '2026-10-01T00:00:00.000Z',
    createdBefore: '2026-11-01T00:00:00.000Z',
    prefix: '  Build-  ',
  }, { limit: 50, cursor: 'djE6MTo' });
  assert.deepEqual(parse(query), {
    limit: '50',
    cursor: 'djE6MTo',
    org_id: '0193a000-0000-7000-8000-000000000001',
    lifecycle: 'started,deleted',
    template: 'seed',
    user_id: '0193a000-0000-7000-8000-000000000002',
    created_after: '2026-10-01T00:00:00.000Z',
    created_before: '2026-11-01T00:00:00.000Z',
    // Trimmed, case kept: a name prefix is case-sensitive.
    prefix: 'Build-',
  });
});

test('the first page sends an empty cursor, and unset filters send nothing', () => {
  assert.equal(sandboxListQuery('paged', NO_FILTERS, { limit: 25 }), 'limit=25&cursor=');
  assert.deepEqual(parse(sandboxListQuery('paged', { ...NO_FILTERS, prefix: ' \0 ' }, { limit: 25 })), { limit: '25', cursor: '' });
});

test('a sampled request sends what the list took before paging: the size and the org only', () => {
  const filters = { ...NO_FILTERS, org: '0193a000-0000-7000-8000-000000000001', template: 'seed', lifecycles: ['started'], prefix: 'a' };
  assert.deepEqual(parse(sandboxListQuery('sampled', filters, { limit: 1000, cursor: 'ignored' })), {
    limit: '1000',
    org_id: '0193a000-0000-7000-8000-000000000001',
  });
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
