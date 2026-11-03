import assert from 'node:assert/strict';
import { test } from 'node:test';
import { USER_DELETE_CONSEQUENCES, passwordTooLong, userDeleteFailureMessage } from './user-delete.ts';

class ApiError extends Error {
  constructor(status, message, body = null) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

// The 409 a release that reports the ownership refusal answers.
const ownership = (reason, extra = {}) => {
  const error = `synthetic API text for ${reason}`;
  return new ApiError(409, error, { error, reason, ...extra });
};

test('the confirmation says what deleting a user does, including keys that keep working', () => {
  assert.match(USER_DELETE_CONSEQUENCES, /sessions end/);
  assert.match(USER_DELETE_CONSEQUENCES, /not revoked and keep authenticating/);
  assert.match(USER_DELETE_CONSEQUENCES, /refused while they own sandboxes/);
});

test('each ownership reason says what still blocks the delete', () => {
  assert.match(userDeleteFailureMessage(ownership('user_owns_sandboxes')), /still owns sandboxes/);
  assert.match(userDeleteFailureMessage(ownership('user_owns_archived_sandboxes')), /archived sandboxes\. They must be purged/);
  const retained = userDeleteFailureMessage(ownership('user_owns_retained_deleted_sandboxes', { retained_until: '2026-11-10T12:00:00.123456Z' }));
  assert.match(retained, /sandboxes are deleted, but their records are kept until about /);
  assert.ok(retained.includes(new Date('2026-11-10T12:00:00.123456Z').toLocaleString()), retained);
  // Without a readable time it still says why, and nothing prints "Invalid Date".
  const unknownTime = userDeleteFailureMessage(ownership('user_owns_retained_deleted_sandboxes', { retained_until: 'soon' }));
  assert.match(unknownTime, /still kept for retention/);
  assert.doesNotMatch(unknownTime, /Invalid Date/);
});

test('another 409 keeps the API message', () => {
  const refusal = 'cannot modify your own role or delete yourself';
  assert.equal(userDeleteFailureMessage(new ApiError(409, refusal, { error: refusal, reason: 'self_mutation' })), refusal);
  assert.equal(userDeleteFailureMessage(new ApiError(409, refusal, { error: refusal })), refusal);
});

test('a 500 from a release without the refusal reads as a short generic message', () => {
  const message = userDeleteFailureMessage(new ApiError(500, 'internal server error', { error: 'internal server error' }));
  assert.match(message, /could not be deleted/);
  assert.doesNotMatch(message, /internal server error/);
});

test('other failures, including a retryable 503, keep the API message', () => {
  assert.equal(userDeleteFailureMessage(new ApiError(503, 'database busy, retry')), 'database busy, retry');
  assert.equal(userDeleteFailureMessage(new ApiError(403, 'Insufficient permissions')), 'Insufficient permissions');
});

test('the password limit counts bytes, not characters', () => {
  assert.equal(passwordTooLong('a'.repeat(72)), false);
  assert.equal(passwordTooLong('a'.repeat(73)), true);
  // 24 three-byte characters are 72 bytes; 25 are 75.
  assert.equal(passwordTooLong('€'.repeat(24)), false);
  assert.equal(passwordTooLong('€'.repeat(25)), true);
});
