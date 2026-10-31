import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  DEFAULT_LIST_LIMIT,
  MAX_LIST_LIMIT,
  matchesSandboxQuery,
  parseListLimit,
  parseSandboxRef,
  publicSandboxIdFromAny,
  safeDecodeURIComponent,
  sandboxQuery,
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
