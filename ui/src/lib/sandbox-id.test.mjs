import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  DEFAULT_LIST_LIMIT,
  MAX_LIST_LIMIT,
  listIsCapped,
  matchesSandboxQuery,
  parseListLimit,
  parseSandboxRef,
  publicSandboxIdFromAny,
  safeDecodeURIComponent,
  sandboxLabelOrId,
  sandboxQuery,
  sandboxRowCount,
  sandboxSearch,
} from './sandbox-id.ts';

const uuid = '01a19089-216f-751e-9f56-1906865f5c23';
const publicId = `sbx_${uuid}`;

for (const [name, input] of [
  ['public ID', publicId],
  ['bare UUID', uuid],
  ['undashed UUID', uuid.replaceAll('-', '')],
  ['upper case', publicId.toUpperCase().replace('SBX_', 'sbx_')],
  ['upper-case prefix', `SBX_${uuid}`],
  ['log tag', `sandbox:${uuid}`],
  ['pod name', `space-sbx-${uuid.replaceAll('-', '')}-g1`],
  ['pod name of a later generation', `space-sbx-${uuid.replaceAll('-', '')}-g12`],
  ['pod name without generation', `space-sbx-${uuid.replaceAll('-', '')}`],
  ['surrounding whitespace and quotes', `  "${publicId}" `],
]) {
  test(`parseSandboxRef accepts a ${name}`, () => {
    assert.deepEqual(parseSandboxRef(input), { uuid, publicId });
  });
}

for (const [name, input] of [
  ['empty', ''],
  ['name', 'uiaudit-running'],
  ['short label', '865f5c23'],
  ['truncated UUID', uuid.slice(0, 30)],
  ['prefixed garbage', 'sbx_not-a-uuid'],
  ['other pod', `space-net-${uuid.replaceAll('-', '')}-g1`],
]) {
  test(`parseSandboxRef rejects a ${name}`, () => {
    assert.equal(parseSandboxRef(input), null);
  });
}

test('publicSandboxIdFromAny canonicalizes a bare UUID, a pod name and a public ID', () => {
  assert.equal(publicSandboxIdFromAny(uuid), publicId);
  assert.equal(publicSandboxIdFromAny(`space-sbx-${uuid.replaceAll('-', '')}-g3`), publicId);
  assert.equal(publicSandboxIdFromAny(publicId), publicId);
});

test('publicSandboxIdFromAny prefixes an unparseable ID unchanged', () => {
  assert.equal(publicSandboxIdFromAny('not-a-uuid'), 'sbx_not-a-uuid');
  assert.equal(publicSandboxIdFromAny('sbx_not-a-uuid'), 'sbx_not-a-uuid');
});

test('safeDecodeURIComponent decodes valid escapes and keeps malformed input', () => {
  assert.equal(safeDecodeURIComponent('sandbox%3A' + uuid), `sandbox:${uuid}`);
  assert.equal(safeDecodeURIComponent('%E0%A4%A'), '%E0%A4%A');
});

const searchUuid = '01a19089-394a-70ba-ae1d-72e09b10b406';
const sandbox = { id: `sbx_${searchUuid}`, name: 'build-cache' };
const other = { id: 'sbx_01a19089-216f-751e-9f56-1906865f5c23', name: 'build-runner' };

test('a full identifier in any form is an exact lookup', () => {
  for (const input of [sandbox.id, searchUuid, `space-sbx-${searchUuid.replaceAll('-', '')}-g1`]) {
    const query = sandboxQuery(input);
    assert.equal(query.kind, 'ref');
    assert.equal(matchesSandboxQuery(query, sandbox), true);
    assert.equal(matchesSandboxQuery(query, other), false);
  }
});

test('an exact lookup matches rows that carry bare UUIDs', () => {
  assert.equal(matchesSandboxQuery(sandboxQuery(sandbox.id), { id: searchUuid }), true);
});

test('partial text matches ID fragments and names', () => {
  assert.equal(matchesSandboxQuery(sandboxQuery('72e09b10'), sandbox), true);
  assert.equal(matchesSandboxQuery(sandboxQuery('ae1d-72e0'), sandbox), true);
  assert.equal(matchesSandboxQuery(sandboxQuery('Build'), sandbox), true);
  assert.equal(matchesSandboxQuery(sandboxQuery('cache'), other), false);
});

test('on a paging server, the ID column\'s label is matched within the page, ignoring case', () => {
  const label = sandboxLabelOrId(sandbox.id);
  assert.equal(label, '9b10b406');
  for (const input of [label, label.toUpperCase(), 'ae1d-72e0', 'AE1D72E0', '1906']) {
    const search = sandboxSearch(input, 'paged');
    assert.equal(search.kind, 'fragment', input);
    assert.equal(matchesSandboxQuery(search.query, sandbox), input !== '1906', input);
  }
  // Names that contain the text match too, as they did before paging.
  const cafe = sandboxSearch('CAFE', 'paged');
  assert.equal(cafe.kind, 'fragment');
  assert.equal(matchesSandboxQuery(cafe.query, { id: other.id, name: 'my-cafe' }), true);
});

test('on a paging server, hex digits and dashes are also sent as a prefix, so such names are found beyond the page', () => {
  // Names made of hex digits and dashes, which the server matches by prefix
  // as typed; it reads the same text as the start of an ID too.
  for (const name of ['e2e-1', 'cafe', '1234', 'CAFE', 'ae1d-72e0']) {
    const search = sandboxSearch(` ${name} `, 'paged');
    assert.equal(search.kind, 'fragment', name);
    assert.equal(search.prefix, name, name);
    // A row so named on the loaded page matches there as well.
    assert.equal(matchesSandboxQuery(search.query, { id: other.id, name }), true, name);
  }
  // A dashed ID start is a prefix alone, as before.
  assert.deepEqual(sandboxSearch('01a19089-394a', 'paged'), { kind: 'prefix', prefix: '01a19089-394a' });
  // Hex longer than any name is matched within the page only.
  assert.equal(sandboxSearch('a'.repeat(257), 'paged').kind, 'page');
  // A sampled list sends no prefix.
  assert.equal(sandboxSearch('e2e-1', 'sampled').kind, 'page');
});

test('on a paging server, a name or ID prefix goes to the server, case kept for names', () => {
  assert.deepEqual(sandboxSearch('  Build-  ', 'paged'), { kind: 'prefix', prefix: 'Build-' });
  assert.deepEqual(sandboxSearch('build cache', 'paged'), { kind: 'prefix', prefix: 'build cache' });
  // The start of an ID the server reads: `sbx_` and hex, or the UUID's
  // dashes where it has them. Hex digits match the server in either case.
  assert.deepEqual(sandboxSearch('01a19089-394a', 'paged'), { kind: 'prefix', prefix: '01a19089-394a' });
  assert.deepEqual(sandboxSearch('01A19089-394A', 'paged'), { kind: 'prefix', prefix: '01A19089-394A' });
  assert.deepEqual(sandboxSearch('sbx_01a19089-394a-70', 'paged'), { kind: 'prefix', prefix: 'sbx_01a19089-394a-70' });
  // `sbx_` alone, or with what no ID starts with, is a name prefix.
  assert.deepEqual(sandboxSearch('sbx_', 'paged'), { kind: 'prefix', prefix: 'sbx_' });
  assert.deepEqual(sandboxSearch('sbx_x1', 'paged'), { kind: 'prefix', prefix: 'sbx_x1' });
});

test('a mixed-case name that looks like an ID prefix is sent as typed', () => {
  // The server matches names case-sensitively and folds the hex of the ID
  // range itself, so lowercasing would lose `sbx_AB-worker`.
  for (const typed of ['sbx_AB', 'sbx_Ab12', 'sbx_AB-worker', 'sbx_01A19089-394A', 'AbCd1234-Ef']) {
    assert.deepEqual(sandboxSearch(typed, 'paged'), { kind: 'prefix', prefix: typed }, typed);
  }
  // `SBX_` is the console's alias for the ID's `sbx_` (see `parseSandboxRef`),
  // which the server reads only in lower case; the rest keeps its case.
  assert.deepEqual(sandboxSearch('SBX_01A19089', 'paged'), { kind: 'prefix', prefix: 'sbx_01A19089' });
  assert.deepEqual(sandboxSearch('Sbx_ab', 'paged'), { kind: 'prefix', prefix: 'sbx_ab' });
  // Behind it, what no ID starts with is a name prefix, as typed.
  assert.deepEqual(sandboxSearch('SBX_AB-worker', 'paged'), { kind: 'prefix', prefix: 'SBX_AB-worker' });
});

test('on a paging server, text no name can start with is matched within the page', () => {
  assert.equal(sandboxSearch('n'.repeat(257), 'paged').kind, 'page');
  assert.equal(sandboxSearch('é'.repeat(129), 'paged').kind, 'page');
  assert.equal(sandboxSearch('n'.repeat(256), 'paged').kind, 'prefix');
  assert.equal(sandboxSearch('build\0', 'paged').kind, 'page');
});

test('a full identifier is a lookup on either list, and a sampled list matches all other text within it', () => {
  for (const mode of ['paged', 'sampled']) {
    assert.equal(sandboxSearch(sandbox.id, mode).kind, 'ref');
    assert.equal(sandboxSearch(` ${searchUuid.toUpperCase()} `, mode).kind, 'ref');
    assert.equal(sandboxSearch('  ', mode).kind, 'empty');
  }
  for (const input of ['Build', 'sbx_01a19089', '01a19089-394a', '9b10b406']) {
    const search = sandboxSearch(input, 'sampled');
    assert.equal(search.kind, 'page', input);
    assert.equal(matchesSandboxQuery(search.query, sandbox), true, input);
  }
});

test('an empty query matches everything', () => {
  assert.equal(sandboxQuery('  ').kind, 'empty');
  assert.equal(matchesSandboxQuery(sandboxQuery(''), other), true);
});

test('parseListLimit clamps to the admin API range', () => {
  assert.equal(parseListLimit(null), DEFAULT_LIST_LIMIT);
  assert.equal(parseListLimit('abc'), DEFAULT_LIST_LIMIT);
  assert.equal(parseListLimit('0'), DEFAULT_LIST_LIMIT);
  assert.equal(parseListLimit('3'), 3);
  assert.equal(parseListLimit('999999'), MAX_LIST_LIMIT);
});

test('only sandbox rows count against the list limit', () => {
  const rows = [
    { resource_type: 'sandbox' },
    { resource_type: 'sandbox' },
    { resource_type: 'pool_slot' },
    { resource_type: 'observed_local' },
  ];
  assert.equal(sandboxRowCount(rows), 2);
  assert.equal(listIsCapped(rows, 2), true);
  assert.equal(listIsCapped(rows, 3), false);
});
