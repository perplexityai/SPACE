import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  ROLLBACK_NOT_ENABLED,
  forgetRollbackRefusal,
  isHiddenOperationError,
  rememberRollbackRefusal,
  rememberedRollbackRefusal,
  rollbackGateRefusal,
} from './hidden-operations.ts';

function apiError(status, message) {
  return Object.assign(new Error(message), { status });
}

for (const [name, err, expected] of [
  ['a hidden route', apiError(404, 'route not found'), true],
  ['a hidden sandbox action', apiError(400, 'invalid sandbox action'), true],
  ['a sandbox the caller cannot see', apiError(404, 'sandbox not found'), false],
  ['a rejected rollback request', apiError(400, 'snapshot_name is required'), false],
  ['a server error', apiError(500, 'route not found'), false],
  ['a network failure', new TypeError('Failed to fetch'), false],
  ['a non-error', 'route not found', false],
  ['nothing', null, false],
]) {
  test(`isHiddenOperationError: ${name}`, () => {
    assert.equal(isHiddenOperationError(err), expected);
  });
}

const GATE = "rollback is not enabled for the sandbox's organization";

test('a rollback gate refusal is read from each route as it answers it', () => {
  assert.equal(rollbackGateRefusal(apiError(404, 'route not found'), 'public'), ROLLBACK_NOT_ENABLED);
  assert.equal(rollbackGateRefusal(apiError(400, 'invalid sandbox action'), 'public'), ROLLBACK_NOT_ENABLED);
  assert.equal(rollbackGateRefusal(apiError(404, GATE), 'admin'), `Snapshot rollback is refused: ${GATE}.`);
  // Anything else is a failure, not the gate.
  for (const [err, via] of [
    [apiError(404, 'Sandbox not found'), 'admin'],
    [apiError(404, 'route not found'), 'admin'],
    [apiError(400, GATE), 'admin'],
    [apiError(404, GATE), 'public'],
    [apiError(500, 'Internal server error'), 'public'],
    [new TypeError('Failed to fetch'), 'admin'],
    [null, 'admin'],
  ]) {
    assert.equal(rollbackGateRefusal(err, via), null, `${via}: ${err?.message}`);
  }
});

const fakeStorage = () => {
  const values = new Map();
  return { getItem: (key) => values.get(key) ?? null, setItem: (key, value) => values.set(key, String(value)), values };
};

test('a refusal is remembered for its organization for the session, until the operator tries again', () => {
  const storage = fakeStorage();
  assert.equal(rememberedRollbackRefusal('org-a', storage), null);
  rememberRollbackRefusal('org-a', 'refused A', storage);
  rememberRollbackRefusal('org-b', 'refused B', storage);
  assert.equal(rememberedRollbackRefusal('org-a', storage), 'refused A');
  assert.equal(rememberedRollbackRefusal('org-b', storage), 'refused B');
  assert.equal(rememberedRollbackRefusal('org-c', storage), null);
  forgetRollbackRefusal('org-a', storage);
  assert.equal(rememberedRollbackRefusal('org-a', storage), null);
  assert.equal(rememberedRollbackRefusal('org-b', storage), 'refused B');
  // No organization, nothing remembered.
  rememberRollbackRefusal(null, 'refused', storage);
  assert.equal(rememberedRollbackRefusal(null, storage), null);
});

test('a stored value this page did not write is ignored, and storage that refuses writes keeps refusals in memory', () => {
  const storage = fakeStorage();
  storage.setItem('space:rollback-refusals:v1', 'not json');
  assert.equal(rememberedRollbackRefusal('org-a', storage), null);
  storage.setItem('space:rollback-refusals:v1', JSON.stringify({ 'org-a': 7 }));
  assert.equal(rememberedRollbackRefusal('org-a', storage), null);
  const full = { getItem: () => null, setItem: () => { throw new Error('QuotaExceededError'); } };
  assert.doesNotThrow(() => rememberRollbackRefusal('org-a', 'refused A', full));
  // Without a browser, the default storage is memory.
  rememberRollbackRefusal('org-memory', 'refused M');
  assert.equal(rememberedRollbackRefusal('org-memory'), 'refused M');
  forgetRollbackRefusal('org-memory');
  assert.equal(rememberedRollbackRefusal('org-memory'), null);
});
