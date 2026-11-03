import assert from 'node:assert/strict';
import { test } from 'node:test';
import { brandingRefusal } from './org-branding.ts';

const apiError = (status, body) => Object.assign(new Error(body?.error ?? 'Request failed'), { status, body });

test('a branding refusal carries the API message and the field it names', () => {
  assert.deepEqual(
    brandingRefusal(apiError(400, { error: 'theme colors must be #rgb or #rrggbb hex colors', field: 'theme_primary' })),
    { message: 'theme colors must be #rgb or #rrggbb hex colors', field: 'theme_primary' },
  );
  // A 400 without a known field still shows its message.
  assert.deepEqual(brandingRefusal(apiError(400, { error: 'bad request' })), { message: 'bad request', field: null });
  assert.deepEqual(brandingRefusal(apiError(400, { error: 'x', field: 'name' })), { message: 'x', field: null });
});

test('other failures are not branding refusals', () => {
  assert.equal(brandingRefusal(apiError(500, { error: 'internal server error' })), null);
  assert.equal(brandingRefusal(new TypeError('Failed to fetch')), null);
});
