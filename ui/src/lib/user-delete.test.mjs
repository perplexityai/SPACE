import assert from 'node:assert/strict';
import { test } from 'node:test';
import { USER_DELETE_CONSEQUENCES, userDeleteFailureMessage } from './user-delete.ts';

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

test('the confirmation says what deleting a user does', () => {
  assert.match(USER_DELETE_CONSEQUENCES, /sessions end/);
  assert.match(USER_DELETE_CONSEQUENCES, /without a creator/);
  assert.match(USER_DELETE_CONSEQUENCES, /refused while they own sandboxes/);
});

test('a refused delete explains the sandbox ownership instead of a raw server error', () => {
  for (const status of [500, 409]) {
    const text = userDeleteFailureMessage(new ApiError(status, 'internal server error'));
    assert.match(text, /owns sandboxes/);
    assert.doesNotMatch(text, /internal server error/);
  }
});

test('other failures keep the API message', () => {
  assert.equal(userDeleteFailureMessage(new ApiError(403, 'Insufficient permissions')), 'Insufficient permissions');
});
