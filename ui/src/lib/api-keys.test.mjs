import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  KEY_CROSS_ORG_UNSUPPORTED,
  LAST_USED_CAVEAT,
  canCreateKey,
  crossOrgKeyRefused,
  createKeyBody,
  isCrossOrgKeyRefusal,
  keyActorText,
  keyActsAs,
  keyActsAsText,
  keyActsAsUnknownText,
  oldestUser,
} from './api-keys.ts';

const OWN_ORG = 'org-own';
const OTHER_ORG = 'org-other';
const caller = { id: 'u-caller', email: 'operator@example.test', org_id: OWN_ORG, created_at: '2026-01-05T00:00:00Z' };
const ownUsers = [
  { id: 'u-first', email: 'first@example.test', created_at: '2026-01-01T00:00:00Z' },
  caller,
];
const otherUsers = [
  { id: 'u-b', email: 'b@example.test', created_at: '2026-02-01T00:00:00Z' },
  { id: 'u-a', email: 'a@example.test', created_at: '2026-03-01T00:00:00Z' },
];

test('a chosen user is who the key acts as, in any organization', () => {
  assert.deepEqual(keyActsAs({ orgId: OTHER_ORG, caller, users: otherUsers, chosenUserId: 'u-a' }), { kind: 'chosen', user: otherUsers[1] });
  assert.deepEqual(keyActsAs({ orgId: OWN_ORG, caller, users: ownUsers, chosenUserId: 'u-first' }), { kind: 'chosen', user: ownUsers[0] });
});

test("without a choice, a key for the caller's own organization acts as the caller, not its oldest user", () => {
  const actsAs = keyActsAs({ orgId: OWN_ORG, caller, users: ownUsers, chosenUserId: undefined });
  assert.equal(actsAs.kind, 'caller');
  assert.equal(actsAs.user.id, caller.id);
  assert.match(keyActsAsText(actsAs), /act as you \(operator@example\.test\)/);
});

test("without a choice, a key for another organization acts as that organization's oldest user", () => {
  const actsAs = keyActsAs({ orgId: OTHER_ORG, caller, users: otherUsers, chosenUserId: null });
  assert.deepEqual(actsAs, { kind: 'oldest', user: otherUsers[0] });
  assert.match(keyActsAsText(actsAs), /b@example\.test, the organization's oldest user/);
  // A choice that names nobody in the organization is no choice.
  assert.equal(keyActsAs({ orgId: OTHER_ORG, caller, users: otherUsers, chosenUserId: 'u-gone' }).kind, 'oldest');
});

test('an organization without users has no one for the key to act as', () => {
  const actsAs = keyActsAs({ orgId: OTHER_ORG, caller, users: [], chosenUserId: null });
  assert.deepEqual(actsAs, { kind: 'no-users' });
  assert.match(keyActsAsText(actsAs), /no users/);
});

test('the oldest user is ordered by creation time, then ID, as the API orders them', () => {
  const at = '2026-01-01T00:00:00Z';
  assert.equal(oldestUser([{ id: 'b', created_at: at }, { id: 'a', created_at: at }]).id, 'a');
  // Microseconds order, below the millisecond `Date.parse` keeps.
  assert.equal(oldestUser([{ id: 'a', created_at: '2026-01-01T00:00:00.167376Z' }, { id: 'b', created_at: '2026-01-01T00:00:00.167375Z' }]).id, 'b');
  assert.equal(oldestUser([{ id: 'a', created_at: '2026-01-01T00:00:00.5Z' }, { id: 'b', created_at: '2026-01-01T00:00:00.45Z' }]).id, 'b');
  assert.equal(oldestUser([]), null);
});

test('user_id is sent only when the operator picked a user', () => {
  assert.deepEqual(createKeyBody({ name: 'ci', allowed_surfaces: ['ecw'] }), { name: 'ci', allowed_surfaces: ['ecw'] });
  assert.deepEqual(createKeyBody({ name: 'ci', allowed_surfaces: ['ecw'], user_id: null }), { name: 'ci', allowed_surfaces: ['ecw'] });
  assert.deepEqual(createKeyBody({ name: 'ci', allowed_surfaces: ['native'], user_id: 'u-a' }), { name: 'ci', allowed_surfaces: ['native'], user_id: 'u-a' });
});

test("a key whose user was deleted says it acts as the organization's oldest user, not that it is unassigned", () => {
  assert.deepEqual(keyActorText({ created_by: 'u-a', created_by_email: 'a@example.test' }), { text: 'a@example.test', recorded: true });
  const orphan = keyActorText({ created_by: null, created_by_email: null });
  assert.equal(orphan.recorded, false);
  assert.equal(orphan.text, "No recorded user; acts as the organization's oldest user");
  assert.doesNotMatch(orphan.text, /Unassigned/);
});

test("Create waits for the organization's users, and after a failed read says who the key acts as", () => {
  assert.equal(canCreateKey({ usersLoading: true, ownOrg: true, crossOrgUnsupported: false }), false);
  assert.equal(canCreateKey({ usersLoading: false, ownOrg: false, crossOrgUnsupported: false }), true);
  assert.match(keyActsAsUnknownText({ ownOrg: false, usersError: null }), /Loading/);
  assert.match(keyActsAsUnknownText({ ownOrg: true, usersError: 'synthetic failure' }), /\(synthetic failure\).*act as you\./);
  assert.match(keyActsAsUnknownText({ ownOrg: false, usersError: 'synthetic failure' }), /oldest user, who could not be read/);
});

test('a release that creates keys only for your own organization refuses another with a 403, and Create stops offering it there', () => {
  const forbidden = Object.assign(new Error('API keys can only be created for your organization'), { status: 403 });
  assert.equal(isCrossOrgKeyRefusal(forbidden, false), true);
  // In the own organization a 403 is some other refusal.
  assert.equal(isCrossOrgKeyRefusal(forbidden, true), false);
  assert.equal(isCrossOrgKeyRefusal(Object.assign(new Error('x'), { status: 409 }), false), false);
  assert.match(KEY_CROSS_ORG_UNSUPPORTED, /only for your own organization/);
  assert.equal(canCreateKey({ usersLoading: false, ownOrg: false, crossOrgUnsupported: true }), false);
});

test("one organization's 403, from one old pod during a rollout, disables Create for that organization only", () => {
  const allowed = (refusedFor, orgId) => canCreateKey({
    usersLoading: false,
    ownOrg: false,
    crossOrgUnsupported: crossOrgKeyRefused(refusedFor, orgId),
  });
  assert.equal(allowed('org-b', 'org-b'), false);
  assert.equal(allowed('org-b', 'org-c'), true);
  // Try again, or picking an organization, forgets it.
  assert.equal(allowed(null, 'org-b'), true);
  assert.equal(crossOrgKeyRefused(null, undefined), false);
});

test('the last-used caveat names what is not recorded and why an older key shows Never', () => {
  assert.match(LAST_USED_CAVEAT, /at most once every five minutes/);
  assert.match(LAST_USED_CAVEAT, /Data-plane calls \(ECW envd/);
  assert.match(LAST_USED_CAVEAT, /shows "Never", even if it was used before/);
});
