import assert from 'node:assert/strict';
import { test } from 'node:test';
import { LATEST_TAG, currentTagHolder, shortHash, tagHolder, tagHolderChange, tagHolders } from './template-format.ts';

const HEX = '0123456789abcdef'.repeat(4);

test('shortHash keeps 12 hex characters whatever the algorithm prefix', () => {
  assert.equal(shortHash(`blake3:${HEX}`), '0123456789ab');
  assert.equal(shortHash(`sha256:${HEX}`), '0123456789ab');
  assert.equal(shortHash(`sha3256:${HEX}`), '0123456789ab');
});

test('shortHash handles a digest with no prefix', () => {
  assert.equal(shortHash(HEX), '0123456789ab');
});

test('shortHash falls back when there is no digest', () => {
  assert.equal(shortHash(null), LATEST_TAG);
  assert.equal(shortHash(undefined, 'none'), 'none');
  assert.equal(shortHash('', ''), '');
});

test("the tag's current release is the one of that template holding it, across runtimes", () => {
  const releases = [
    { id: 'r1', template_name: 'seed', tags: ['latest', 'prod'] },
    { id: 'r2', template_name: 'seed', tags: [] },
    { id: 'r3', template_name: 'other', tags: ['canary'] },
  ];
  assert.equal(currentTagHolder(releases, 'seed', 'prod'), 'r1');
  // A tag only another template holds does not exist for this one.
  assert.equal(currentTagHolder(releases, 'seed', 'canary'), null);
  assert.equal(currentTagHolder(releases, 'other', 'canary'), 'r3');
});

test("a tag dialog keeps the holders it opened with; a later read shows how the typed tag moved", () => {
  const opened = tagHolders([
    { id: 'r1', template_name: 'seed', tags: ['latest', 'prod'] },
    { id: 'r2', template_name: 'seed', tags: ['canary'] },
    { id: 'r3', template_name: 'other', tags: ['prod'] },
  ], 'seed');
  assert.deepEqual(opened, { latest: 'r1', prod: 'r1', canary: 'r2' });
  // The precondition is what the operator saw, not what a refresh read later.
  assert.equal(tagHolder(opened, 'prod'), 'r1');
  assert.equal(tagHolder(opened, 'staging'), null);
  // Inherited names are not tags.
  assert.equal(tagHolder(opened, 'constructor'), null);
  const refreshed = tagHolders([
    { id: 'r1', template_name: 'seed', tags: ['latest'] },
    { id: 'r2', template_name: 'seed', tags: ['canary', 'prod'] },
    { id: 'r4', template_name: 'seed', tags: ['staging'] },
  ], 'seed');
  assert.deepEqual(tagHolderChange(opened, refreshed, 'prod'), { from: 'r1', to: 'r2' });
  assert.deepEqual(tagHolderChange(opened, refreshed, 'staging'), { from: null, to: 'r4' });
  assert.equal(tagHolderChange(opened, refreshed, 'latest'), null);
  assert.equal(tagHolderChange(opened, refreshed, 'unused'), null);
  // A tag deleted meanwhile moved to nowhere.
  assert.deepEqual(tagHolderChange(refreshed, {}, 'staging'), { from: 'r4', to: null });
});
