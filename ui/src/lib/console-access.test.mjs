import assert from 'node:assert/strict';
import { test } from 'node:test';
import { consoleAccess } from './console-access.ts';

const cluster = { managing_org_id: 'org-managing' };

test('an organization admin of the managing org is an operator', () => {
  assert.equal(consoleAccess({ role: 'organization_admin', org_id: 'org-managing' }, cluster), 'operator');
});

test('an organization admin of another org is not an operator', () => {
  assert.equal(consoleAccess({ role: 'organization_admin', org_id: 'org-tenant' }, cluster), 'not-operator');
});

test('a member is not an operator, even in the managing org', () => {
  assert.equal(consoleAccess({ role: 'member', org_id: 'org-managing' }, cluster), 'not-operator');
  assert.equal(consoleAccess({ role: 'member', org_id: 'org-managing' }, null), 'not-operator');
});

test('an organization admin whose cluster record failed to load is undecided', () => {
  assert.equal(consoleAccess({ role: 'organization_admin', org_id: 'org-managing' }, null), 'cluster-unknown');
});
