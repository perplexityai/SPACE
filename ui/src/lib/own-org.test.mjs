import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isOwnOrg } from './own-org.ts';

test("a sandbox is the operator's own unless both organizations are known and differ", () => {
  assert.equal(isOwnOrg('org-a', 'org-a'), true);
  assert.equal(isOwnOrg('org-a', 'org-b'), false);
  // Unknown on either side: the public routes, as before the admin routes.
  assert.equal(isOwnOrg(null, 'org-b'), true);
  assert.equal(isOwnOrg(undefined, 'org-b'), true);
  assert.equal(isOwnOrg('org-a', null), true);
  assert.equal(isOwnOrg('org-a', ''), true);
});
