import assert from 'node:assert/strict';
import { test } from 'node:test';
import { LATEST_TAG, shortHash } from './template-format.ts';

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
