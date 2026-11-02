import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mergeNewest, newerFirst, sandboxTargetForms } from './operation-list.ts';

const op = (id, created_at) => ({ id, created_at });

test('operations sort by their full fractional seconds, newest first', () => {
  const rows = [
    op('a', '2026-01-02T03:04:00.1234Z'),
    op('b', '2026-01-02T03:04:00.12346Z'),
    op('c', '2026-01-02T03:04:00.12345Z'),
    op('d', '2026-01-02T03:04:00.123451Z'),
  ];
  assert.deepEqual([...rows].sort(newerFirst).map((row) => row.id), ['b', 'd', 'c', 'a']);
});

test('a timestamp without a fraction sorts as a zero fraction', () => {
  const rows = [
    op('whole', '2026-01-02T03:04:00Z'),
    op('later', '2026-01-02T03:04:00.000001Z'),
    op('earlier', '2026-01-02T03:03:59.999999999Z'),
    op('next', '2026-01-02T03:04:01Z'),
  ];
  assert.deepEqual([...rows].sort(newerFirst).map((row) => row.id), ['next', 'later', 'whole', 'earlier']);
});

test('the operation ID breaks only exact ties', () => {
  const rows = [op('a', '2026-01-02T03:04:00.5Z'), op('b', '2026-01-02T03:04:00.500Z')];
  assert.deepEqual([...rows].sort(newerFirst).map((row) => row.id), ['b', 'a']);
});

test('merged pages hold each operation once, newest first, up to the limit', () => {
  const merged = mergeNewest([
    [op('x', '2026-01-02T03:04:00.2Z'), op('y', '2026-01-02T03:04:00.1Z')],
    [op('x', '2026-01-02T03:04:00.2Z'), op('z', '2026-01-02T03:04:00.15Z')],
  ], 2);
  assert.deepEqual(merged.map((row) => row.id), ['x', 'z']);
});

test('a sandbox ID or UUID in any accepted form names both recorded forms', () => {
  const uuid = '0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b';
  const both = [`sbx_${uuid}`, uuid];
  assert.deepEqual(sandboxTargetForms(`sbx_${uuid}`), both);
  assert.deepEqual(sandboxTargetForms(uuid), both);
  assert.deepEqual(sandboxTargetForms(uuid.toUpperCase()), both);
  assert.deepEqual(sandboxTargetForms(uuid.replaceAll('-', '')), both);
  assert.deepEqual(sandboxTargetForms(` SBX_${uuid.toUpperCase()} `), both);
});

test('a target that is not a sandbox UUID is searched as typed', () => {
  assert.equal(sandboxTargetForms('tpl_seed'), null);
  assert.equal(sandboxTargetForms('0190a1b2'), null);
  assert.equal(sandboxTargetForms('sbx_0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5'), null);
});
