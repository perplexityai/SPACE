import assert from 'node:assert/strict';
import { test } from 'node:test';
import { USER_DELETE_CONSEQUENCES, passwordTooLong, userDeleteFailureMessage } from './user-delete.ts';

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

test('the confirmation says what deleting a user does, including keys that keep working', () => {
  assert.match(USER_DELETE_CONSEQUENCES, /sessions end/);
  assert.match(USER_DELETE_CONSEQUENCES, /not revoked and keep authenticating/);
  assert.match(USER_DELETE_CONSEQUENCES, /refused while they own sandboxes/);
});

test('a 409 explains the sandbox ownership', () => {
  assert.match(userDeleteFailureMessage(new ApiError(409, 'conflict')), /owns sandboxes/);
});

test('a 500 explains the ownership only when the page shows the user owns sandboxes', () => {
  assert.match(userDeleteFailureMessage(new ApiError(500, 'Internal server error'), { ownsSandboxes: true }), /owns sandboxes/);
  assert.equal(userDeleteFailureMessage(new ApiError(500, 'Internal server error')), 'Internal server error');
});

test('other failures, including a retryable 503, keep the API message', () => {
  assert.equal(userDeleteFailureMessage(new ApiError(503, 'database busy, retry'), { ownsSandboxes: true }), 'database busy, retry');
  assert.equal(userDeleteFailureMessage(new ApiError(403, 'Insufficient permissions')), 'Insufficient permissions');
});

test('the password limit counts bytes, not characters', () => {
  assert.equal(passwordTooLong('a'.repeat(72)), false);
  assert.equal(passwordTooLong('a'.repeat(73)), true);
  // 24 three-byte characters are 72 bytes; 25 are 75.
  assert.equal(passwordTooLong('€'.repeat(24)), false);
  assert.equal(passwordTooLong('€'.repeat(25)), true);
});
