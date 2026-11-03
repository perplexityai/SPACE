import assert from 'node:assert/strict';
import { test } from 'node:test';
import { countLabel, nodeReadFailure } from './node-detail.ts';

test('a 403 or 404 replaces the page even after a successful load', () => {
  for (const loaded of [false, true]) {
    assert.equal(nodeReadFailure(403, loaded), 'replace');
    assert.equal(nodeReadFailure(404, loaded), 'replace');
  }
});

test('other failures replace the page only before the first load', () => {
  for (const status of [null, 500, 502, 503]) {
    assert.equal(nodeReadFailure(status, false), 'replace');
    assert.equal(nodeReadFailure(status, true), 'stale');
  }
});

test('a count names why it is missing', () => {
  assert.equal(countLabel(null, true), 'could not count');
  assert.equal(countLabel(undefined, true), 'could not count');
  assert.equal(countLabel(null, false), 'not computed');
});

test('a counted value is marked stale once a later count fails', () => {
  assert.equal(countLabel(0, false), '0');
  assert.equal(countLabel(7, false), '7');
  assert.equal(countLabel(7, true), '7 (stale: the last count failed)');
});
