import assert from 'node:assert/strict';
import { test } from 'node:test';
import { loadFailureFrom, loadFailureView } from './load-failure.ts';

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

test('only a 404 is reported as not found', () => {
  assert.deepEqual(loadFailureView({ status: 404, message: 'resource not found' }, 'organization'), {
    title: 'Organization not found',
    retry: false,
  });
});

test('a 500, a 403 or a network failure is a load failure with retry', () => {
  for (const status of [500, 503, 403, null]) {
    assert.deepEqual(loadFailureView({ status, message: 'x' }, 'user'), { title: 'Could not load the user', retry: true });
  }
});

test('loadFailureFrom keeps the API status and message', () => {
  assert.deepEqual(loadFailureFrom(new ApiError(404, 'resource not found'), 'fallback'), { status: 404, message: 'resource not found' });
  assert.deepEqual(loadFailureFrom(new TypeError('Failed to fetch'), 'fallback'), { status: null, message: 'Failed to fetch' });
  assert.deepEqual(loadFailureFrom('weird', 'fallback'), { status: null, message: 'fallback' });
});
