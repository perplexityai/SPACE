import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  canCreateWithOwner,
  crossOrgCreateTarget,
  ownerOptionLabel,
  sandboxOwner,
  sandboxOwnerText,
} from './sandbox-owner.ts';

const ada = { id: 'user-ada', email: 'ada@example.test', display_name: 'Ada' };
const bob = { id: 'user-bob', email: 'bob@example.test', display_name: '' };

test('only a chosen organization other than the own one is a cross-organization create', () => {
  assert.equal(crossOrgCreateTarget('org-b', 'org-a'), 'org-b');
  assert.equal(crossOrgCreateTarget('org-a', 'org-a'), null);
  assert.equal(crossOrgCreateTarget(undefined, 'org-a'), null);
  assert.equal(crossOrgCreateTarget(null, 'org-a'), null);
  assert.equal(crossOrgCreateTarget('', 'org-a'), null);
  // Before sign-in has loaded the own organization, a chosen one is another's.
  assert.equal(crossOrgCreateTarget('org-b', null), 'org-b');
});

test('an own-organization create has no owner to choose and may be created', () => {
  const owner = sandboxOwner({ targetOrgId: null, loaded: null, chosenUserId: 'user-ada' });
  assert.deepEqual(owner, { kind: 'own-org' });
  assert.equal(canCreateWithOwner(owner), true);
  assert.equal(sandboxOwnerText(owner), null);
});

test("another organization's create waits for that organization's users", () => {
  for (const loaded of [null, { orgId: 'org-c', users: [ada], error: null }]) {
    const owner = sandboxOwner({ targetOrgId: 'org-b', loaded, chosenUserId: 'user-ada' });
    assert.deepEqual(owner, { kind: 'loading' });
    assert.equal(canCreateWithOwner(owner), false);
    assert.match(sandboxOwnerText(owner), /Loading/);
  }
});

test('a failed users read keeps Create disabled and says why', () => {
  const owner = sandboxOwner({ targetOrgId: 'org-b', loaded: { orgId: 'org-b', users: null, error: 'HTTP 500' } });
  assert.deepEqual(owner, { kind: 'failed', error: 'HTTP 500' });
  assert.equal(canCreateWithOwner(owner), false);
  assert.match(sandboxOwnerText(owner), /Could not load .*HTTP 500.*retry/);
});

test('an organization without users needs one created first', () => {
  const owner = sandboxOwner({ targetOrgId: 'org-b', loaded: { orgId: 'org-b', users: [], error: null } });
  assert.deepEqual(owner, { kind: 'no-users' });
  assert.equal(canCreateWithOwner(owner), false);
  assert.match(sandboxOwnerText(owner), /create a user first/);
});

test('Create waits for an owner of the target organization', () => {
  const loaded = { orgId: 'org-b', users: [ada, bob], error: null };
  for (const chosenUserId of [undefined, null, '', 'user-of-another-org']) {
    const owner = sandboxOwner({ targetOrgId: 'org-b', loaded, chosenUserId });
    assert.equal(owner.kind, 'unchosen');
    assert.equal(canCreateWithOwner(owner), false);
    assert.match(sandboxOwnerText(owner), /Choose the user/);
  }
  const owner = sandboxOwner({ targetOrgId: 'org-b', loaded, chosenUserId: 'user-bob' });
  assert.equal(owner.kind, 'chosen');
  assert.equal(owner.user, bob);
  assert.equal(owner.orgId, 'org-b');
  assert.equal(canCreateWithOwner(owner), true);
  assert.equal(sandboxOwnerText(owner), null);
});

test('an owner option names the email, and the display name when there is one', () => {
  assert.equal(ownerOptionLabel(ada), 'ada@example.test (Ada)');
  assert.equal(ownerOptionLabel(bob), 'bob@example.test');
  assert.equal(ownerOptionLabel({ id: 'x', email: 'x@example.test', display_name: null }), 'x@example.test');
});
