import assert from 'node:assert/strict';
import { test } from 'node:test';
import { orgOptionLabels, parseOrgParam } from './org-filter.ts';

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
