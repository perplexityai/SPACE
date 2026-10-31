import assert from 'node:assert/strict';
import { test } from 'node:test';
import { roleChangeConfirmation } from './userRoles.ts';

test('role-change text promises console access only for the managing org', () => {
  assert.match(roleChangeConfirmation('a@example.test', 'organization_admin', { inManagingOrg: true }).content, /operate it from this console/);
  assert.match(roleChangeConfirmation('a@example.test', 'organization_admin', { inManagingOrg: false }).content, /does not give access/);
});

test('role-change text says console access is unknown when the cluster did not load', () => {
  for (const role of ['organization_admin', 'member']) {
    const { content } = roleChangeConfirmation('a@example.test', role, { inManagingOrg: null, currentRole: 'organization_admin' });
    assert.match(content, /could not be determined/);
    assert.doesNotMatch(content, /does not give access|operate it from this console/);
  }
});
