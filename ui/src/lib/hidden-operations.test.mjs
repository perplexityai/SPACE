import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isHiddenOperationError } from './hidden-operations.ts';

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
