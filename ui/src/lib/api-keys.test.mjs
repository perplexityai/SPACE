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

test('without a choice, a key for another organization acts as nobody until a user is chosen', () => {
  const actsAs = keyActsAs({ orgId: OTHER_ORG, caller, users: otherUsers, chosenUserId: null });
  assert.deepEqual(actsAs, { kind: 'unchosen' });
  assert.equal(keyActsAsText(actsAs), 'Choose the user the key acts as.');
  assert.doesNotMatch(keyActsAsText(actsAs), /oldest|default/);
  // A choice that names nobody in the organization is no choice.
  assert.equal(keyActsAs({ orgId: OTHER_ORG, caller, users: otherUsers, chosenUserId: 'u-gone' }).kind, 'unchosen');
});

test("Create for another organization's key waits for a chosen user; the own organization's needs none", () => {
  assert.equal(canCreateKey({ usersLoading: false, ownOrg: false, userChosen: false, crossOrgUnsupported: false }), false);
  assert.equal(canCreateKey({ usersLoading: false, ownOrg: false, userChosen: true, crossOrgUnsupported: false }), true);
  assert.equal(canCreateKey({ usersLoading: false, ownOrg: true, userChosen: false, crossOrgUnsupported: false }), true);
});

test('an organization without users has no one for the key to act as', () => {
  const actsAs = keyActsAs({ orgId: OTHER_ORG, caller, users: [], chosenUserId: null });
  assert.deepEqual(actsAs, { kind: 'no-users' });
  assert.match(keyActsAsText(actsAs), /no users/);
});

test("user_id is sent only when the operator picked a user, so an own-organization key without one sends none", () => {
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
  assert.equal(canCreateKey({ usersLoading: true, ownOrg: true, userChosen: false, crossOrgUnsupported: false }), false);
  assert.match(keyActsAsUnknownText({ ownOrg: false, usersError: null }), /Loading/);
  assert.match(keyActsAsUnknownText({ ownOrg: true, usersError: 'synthetic failure' }), /\(synthetic failure\).*act as you\./);
  // Another organization's key needs a chosen user, which only a retry offers.
  const unread = keyActsAsUnknownText({ ownOrg: false, usersError: 'synthetic failure' });
  assert.match(unread, /\(synthetic failure\).*Retry to choose the user the key acts as\./);
  assert.doesNotMatch(unread, /oldest/);
});

test('a release that creates keys only for your own organization refuses another with a 403, and Create stops offering it there', () => {
  const forbidden = Object.assign(new Error('API keys can only be created for your organization'), { status: 403 });
  assert.equal(isCrossOrgKeyRefusal(forbidden, false), true);
  // In the own organization a 403 is some other refusal.
  assert.equal(isCrossOrgKeyRefusal(forbidden, true), false);
  assert.equal(isCrossOrgKeyRefusal(Object.assign(new Error('x'), { status: 409 }), false), false);
  assert.match(KEY_CROSS_ORG_UNSUPPORTED, /only for your own organization/);
  assert.equal(canCreateKey({ usersLoading: false, ownOrg: false, userChosen: true, crossOrgUnsupported: true }), false);
});

test("that release refuses another organization's key, which carries user_id, as a 422 first; the own organization's 422 is the user choice", () => {
  const unknownField = Object.assign(new Error('unknown field `user_id`'), { status: 422 });
  assert.equal(isCrossOrgKeyRefusal(unknownField, false, true), true);
  assert.equal(isCrossOrgKeyRefusal(unknownField, true, true), false);
});

test("a release that requires user_id answers its 400 as a message, not as a cross-organization refusal", () => {
  const required = Object.assign(new Error("user_id is required for another organization's key"), { status: 400 });
  assert.equal(isCrossOrgKeyRefusal(required, false, false), false);
});

test("one organization's 403, from one old pod during a rollout, disables Create for that organization only", () => {
  const allowed = (refusedFor, orgId) => canCreateKey({
    usersLoading: false,
    ownOrg: false,
    userChosen: true,
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
