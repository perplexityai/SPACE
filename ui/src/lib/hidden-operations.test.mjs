import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  ROLLBACK_NOT_ENABLED,
  createRollbackRefusals,
  isHiddenOperationError,
  rollbackGateRefusal,
  rollbackRefusals,
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
const KEY = 'space:rollback-refusals:v1';

test('a refusal is remembered for its organization for the session, until the operator tries again', () => {
  const storage = fakeStorage();
  const refusals = createRollbackRefusals(storage);
  assert.equal(refusals.get('org-a'), null);
  refusals.remember('org-a', 'refused A');
  refusals.remember('org-b', 'refused B');
  assert.equal(refusals.get('org-a'), 'refused A');
  assert.equal(refusals.get('org-b'), 'refused B');
  assert.equal(refusals.get('org-c'), null);
  refusals.forget('org-a');
  assert.equal(refusals.get('org-a'), null);
  assert.equal(refusals.get('org-b'), 'refused B');
  // The tab's next page load reads what this one stored.
  assert.equal(createRollbackRefusals(storage).get('org-b'), 'refused B');
  assert.equal(createRollbackRefusals(storage).get('org-a'), null);
  // No organization, nothing remembered.
  refusals.remember(null, 'refused');
  assert.equal(refusals.get(null), null);
});

test("a refusal or a Try again in one panel reaches every panel of that organization, and only those", () => {
  const refusals = createRollbackRefusals(fakeStorage());
  const calls = { a1: 0, a2: 0, b: 0 };
  // Two panels of org-a (the sandbox page and a list row), one of org-b.
  const stopA1 = refusals.subscribe('org-a', () => { calls.a1 += 1; });
  refusals.subscribe('org-a', () => { calls.a2 += 1; });
  refusals.subscribe('org-b', () => { calls.b += 1; });
  refusals.remember('org-a', 'refused A');
  assert.deepEqual(calls, { a1: 1, a2: 1, b: 0 });
  // Each panel reads the shared refusal when notified, and before it sends.
  assert.equal(refusals.get('org-a'), 'refused A');
  // The same refusal again changes nothing.
  refusals.remember('org-a', 'refused A');
  assert.deepEqual(calls, { a1: 1, a2: 1, b: 0 });
  refusals.forget('org-a');
  assert.deepEqual(calls, { a1: 2, a2: 2, b: 0 });
  assert.equal(refusals.get('org-a'), null);
  // A closed panel is no longer notified.
  stopA1();
  refusals.remember('org-a', 'refused again');
  assert.deepEqual(calls, { a1: 2, a2: 3, b: 0 });
});

test('a stored value this page did not write is ignored, and storage that refuses writes keeps refusals in memory', () => {
  const storage = fakeStorage();
  storage.setItem(KEY, 'not json');
  assert.equal(createRollbackRefusals(storage).get('org-a'), null);
  storage.setItem(KEY, JSON.stringify({ 'org-a': 7, 'org-b': 'refused B' }));
  assert.equal(createRollbackRefusals(storage).get('org-a'), null);
  assert.equal(createRollbackRefusals(storage).get('org-b'), 'refused B');
  // Storage that refuses every write: the refusal still holds in this tab.
  const full = { getItem: () => null, setItem: () => { throw new Error('QuotaExceededError'); } };
  const refusals = createRollbackRefusals(full);
  assert.doesNotThrow(() => refusals.remember('org-a', 'refused A'));
  assert.equal(refusals.get('org-a'), 'refused A');
  refusals.forget('org-a');
  assert.equal(refusals.get('org-a'), null);
  // Without a browser there is no storage, and the tab's store is memory.
  rollbackRefusals().remember('org-memory', 'refused M');
  assert.equal(rollbackRefusals().get('org-memory'), 'refused M');
  rollbackRefusals().forget('org-memory');
  assert.equal(rollbackRefusals().get('org-memory'), null);
});
