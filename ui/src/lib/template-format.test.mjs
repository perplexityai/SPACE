import assert from 'node:assert/strict';
import { test } from 'node:test';
import { LATEST_TAG, currentTagHolder, shortHash, tagMoveFailureMessage } from './template-format.ts';

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

test('a refused precondition says the tag moved; other failures keep the API message', () => {
  const error = (status, message) => Object.assign(new Error(message), { status });
  assert.match(tagMoveFailureMessage(error(412, 'tag does not point at expected_current_release_id')), /moved since this page loaded/);
  assert.equal(tagMoveFailureMessage(error(404, 'Template release not found')), 'Template release not found');
});
