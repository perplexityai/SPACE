// Who a new API key acts as. The admin API stores the key's user as its
// `created_by` and resolves the key's actor from it on every request:
// - a chosen `user_id` (a user of the key's organization);
// - without one, the caller, for a key in the caller's own organization;
// - otherwise the organization's oldest user (`ORDER BY created_at, id`).
// Releases before user choice refuse `user_id` with a 422 and create keys
// only for the caller's organization, acting as the caller.

export interface KeyUserCandidate {
  id: string;
  email: string;
  display_name?: string | null;
  created_at: string;
}

export type KeyActsAs =
  | { kind: 'chosen'; user: KeyUserCandidate }
  | { kind: 'caller'; user: KeyUserCandidate }
  | { kind: 'oldest'; user: KeyUserCandidate }
  | { kind: 'no-users' };

// Whole seconds and the fraction apart: `Date.parse` keeps milliseconds only,
// while the API's timestamps carry microseconds.
function timestampOrder(value: string): [number, string] {
  const match = /^(.*?)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)?$/.exec(value);
  if (!match) return [Date.parse(value), ''];
  return [Date.parse(`${match[1]}${match[3] ?? 'Z'}`), (match[2] ?? '').padEnd(9, '0')];
}

function olderThan(a: KeyUserCandidate, b: KeyUserCandidate): boolean {
  const [aSeconds, aFraction] = timestampOrder(a.created_at);
  const [bSeconds, bFraction] = timestampOrder(b.created_at);
  if (aSeconds !== bSeconds) return aSeconds < bSeconds;
  if (aFraction !== bFraction) return aFraction < bFraction;
  return a.id < b.id;
}

export function oldestUser<T extends KeyUserCandidate>(users: T[]): T | null {
  let oldest: T | null = null;
  for (const user of users) {
    if (oldest === null || olderThan(user, oldest)) oldest = user;
  }
  return oldest;
}

export function keyActsAs({
  orgId,
  caller,
  users,
  chosenUserId,
}: {
  orgId: string;
  caller: KeyUserCandidate & { org_id: string };
  users: KeyUserCandidate[];
  chosenUserId?: string | null;
}): KeyActsAs {
  if (chosenUserId) {
    const user = users.find((candidate) => candidate.id === chosenUserId);
    if (user) return { kind: 'chosen', user };
  }
  if (orgId === caller.org_id) {
    return { kind: 'caller', user: users.find((candidate) => candidate.id === caller.id) ?? caller };
  }
  const oldest = oldestUser(users);
  return oldest ? { kind: 'oldest', user: oldest } : { kind: 'no-users' };
}

export function keyActsAsText(actsAs: KeyActsAs): string {
  switch (actsAs.kind) {
    case 'chosen':
      return `The key will act as ${actsAs.user.email}.`;
    case 'caller':
      return `The key will act as you (${actsAs.user.email}).`;
    case 'oldest':
      return `The key will act as ${actsAs.user.email}, the organization's oldest user.`;
    case 'no-users':
      return 'The organization has no users for the key to act as; create a user first.';
  }
}

// What the create dialog says about the key's user while the organization's
// users are not known. Create waits while they load; after a failed read it
// is allowed without a choice, and the key acts as the default user.
export function keyActsAsUnknownText({ ownOrg, usersError }: { ownOrg: boolean; usersError: string | null }): string {
  if (usersError === null) return "Loading the organization's users…";
  return `Could not load the organization's users (${usersError}), so none can be chosen. ` + (ownOrg
    ? 'The key will act as you.'
    : "The key will act as the organization's oldest user, who could not be read; retry to see who that is.");
}

// Whether Create may be pressed: not while the users a key could act as are
// still loading, and not for another organization once this API release has
// refused that (`isCrossOrgKeyRefusal`).
export function canCreateKey({
  usersLoading,
  ownOrg,
  crossOrgUnsupported,
}: {
  usersLoading: boolean;
  ownOrg: boolean;
  crossOrgUnsupported: boolean;
}): boolean {
  return !usersLoading && (ownOrg || !crossOrgUnsupported);
}

// Releases before cross-organization keys answer a create for any other
// organization with a 403 ("API keys can only be created for your
// organization").
export function isCrossOrgKeyRefusal(err: unknown, ownOrg: boolean): boolean {
  return !ownOrg && typeof err === 'object' && err !== null && (err as { status?: unknown }).status === 403;
}

export const KEY_CROSS_ORG_UNSUPPORTED = 'This API version creates keys only for your own organization.';

// Who an existing key acts as, from its `created_by`: that user, or, once the
// user was deleted (`created_by` is set to NULL), the organization's oldest
// user, whom every release resolves such a key to.
export function keyActorText(key: { created_by: string | null; created_by_email: string | null }): {
  text: string;
  recorded: boolean;
} {
  if (key.created_by_email) return { text: key.created_by_email, recorded: true };
  if (key.created_by) return { text: key.created_by, recorded: true };
  return { text: "No recorded user; acts as the organization's oldest user", recorded: false };
}

export interface CreateKeyValues {
  name: string;
  allowed_surfaces: string[];
  user_id?: string | null;
}

// `user_id` only when the operator picked one, so a release that does not
// know the field still creates keys that need no choice.
export function createKeyBody(values: CreateKeyValues): { name: string; allowed_surfaces: string[]; user_id?: string } {
  const body: { name: string; allowed_surfaces: string[]; user_id?: string } = {
    name: values.name,
    allowed_surfaces: values.allowed_surfaces,
  };
  if (values.user_id) body.user_id = values.user_id;
  return body;
}

// What the console says when a release refuses `user_id` (see
// `isUnknownFieldRejection`).
export const KEY_USER_CHOICE_UNSUPPORTED =
  "This API release cannot choose the key's user. Clear the user to create a key that acts as you, in your own organization.";

export const LAST_USED_CAVEAT =
  'Recorded when the key authenticates a SPACE or E2B-compatible API request, at most once every five minutes. ' +
  'Data-plane calls (ECW envd: terminals, commands and files inside the sandbox) are not recorded. A key not used ' +
  'since the API began recording use shows "Never", even if it was used before.';
