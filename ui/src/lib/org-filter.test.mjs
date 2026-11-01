import assert from 'node:assert/strict';
import { test } from 'node:test';
import { lookupScopeText, orgOptionLabels, parseOrgParam, scopedRefreshKey } from './org-filter.ts';

test('parseOrgParam keeps a UUID and drops anything else', () => {
  assert.equal(parseOrgParam('01A19089-216F-751E-9F56-1906865F5C23'), '01a19089-216f-751e-9f56-1906865f5c23');
  assert.equal(parseOrgParam(null), null);
  assert.equal(parseOrgParam(''), null);
  assert.equal(parseOrgParam('acme'), null);
  assert.equal(parseOrgParam('01a19089-216f-751e-9f56'), null);
});

test('orgOptionLabels adds the slug only where a name repeats', () => {
  const labels = orgOptionLabels([
    { id: 'a', name: 'Acme', slug: 'acme' },
    { id: 'b', name: 'Acme', slug: 'acme-eu' },
    { id: 'c', name: 'Globex', slug: 'globex' },
  ]);
  assert.deepEqual(labels.map((o) => o.label), ['Acme (acme)', 'Acme (acme-eu)', 'Globex']);
});

test('each list scope keeps its own refresh key', () => {
  const key = (limit, org) => scopedRefreshKey('dashboard:sandboxes', limit, 1000, org);
  assert.equal(key(1000, null), 'dashboard:sandboxes');
  assert.equal(key(100, null), 'dashboard:sandboxes:limit-100');
  assert.equal(key(5000, null), 'dashboard:sandboxes:limit-5000');
  assert.equal(key(1000, 'org-a'), 'dashboard:sandboxes:org-org-a:limit-1000');
  assert.notEqual(key(1000, 'org-a'), key(1000, 'org-b'));
  assert.notEqual(key(1000, 'org-a'), key(5000, 'org-a'));
});

test('the lookup notice says whose sandbox it is under an org filter', () => {
  const names = new Map([['a', 'Acme'], ['b', 'Globex']]);
  const name = (id) => names.get(id);
  assert.equal(lookupScopeText({ org_id: 'a' }, null, name, 1000), 'It is not among the newest 1000 sandboxes loaded below.');
  assert.equal(lookupScopeText({ org_id: 'a' }, 'a', name, 1000), 'It is not among the newest 1000 sandboxes of Acme loaded below.');
  assert.equal(lookupScopeText({ org_id: 'b' }, 'a', name, 1000), 'It belongs to Globex, not Acme.');
  assert.equal(lookupScopeText({ org_id: 'c' }, 'a', name, 1000), 'It belongs to another organization, not Acme.');
  assert.equal(lookupScopeText({ org_id: null }, 'a', name, 1000), 'It belongs to no organization, not Acme.');
});
