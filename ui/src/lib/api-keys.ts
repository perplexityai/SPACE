// Who a new API key acts as. The admin API stores the key's user as its
// `created_by` and resolves the key's actor from it on every request:
// - a chosen `user_id` (a user of the key's organization);
// - without one, the caller, for a key in the caller's own organization.
// A key for another organization without one answers 400 ("user_id is
// required for another organization's key"), so the console sends none only
// for the caller's own. Releases before user choice refuse `user_id` with a
// 422 and create keys only for the caller's organization, acting as the
// caller.

export interface KeyUserCandidate {
  id: string;
  email: string;
  display_name?: string | null;
  created_at: string;
}

export type KeyActsAs =
  | { kind: 'chosen'; user: KeyUserCandidate }
  | { kind: 'caller'; user: KeyUserCandidate }
  | { kind: 'unchosen' }
  | { kind: 'no-users' };

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
  return users.length > 0 ? { kind: 'unchosen' } : { kind: 'no-users' };
}

export function keyActsAsText(actsAs: KeyActsAs): string {
  switch (actsAs.kind) {
    case 'chosen':
      return `The key will act as ${actsAs.user.email}.`;
    case 'caller':
      return `The key will act as you (${actsAs.user.email}).`;
    case 'unchosen':
      return 'Choose the user the key acts as.';
    case 'no-users':
      return 'The organization has no users for the key to act as; create a user first.';
  }
}

// What the create dialog says about the key's user while the organization's
// users are not known. Create waits while they load; after a failed read a
// key for the own organization is allowed without a choice and acts as the
// caller, while another organization's waits for a retry to offer its users.
export function keyActsAsUnknownText({ ownOrg, usersError }: { ownOrg: boolean; usersError: string | null }): string {
  if (usersError === null) return "Loading the organization's users…";
  return `Could not load the organization's users (${usersError}), so none can be chosen. ` + (ownOrg
    ? 'The key will act as you.'
    : 'Retry to choose the user the key acts as.');
}

// Whether Create may be pressed: not while the users a key could act as are
// still loading; for another organization, only once a user is chosen, and
// not once this API release has refused that organization
// (`isCrossOrgKeyRefusal`).
export function canCreateKey({
  usersLoading,
  ownOrg,
  userChosen,
  crossOrgUnsupported,
}: {
  usersLoading: boolean;
  ownOrg: boolean;
  userChosen: boolean;
  crossOrgUnsupported: boolean;
}): boolean {
  return !usersLoading && (ownOrg || (userChosen && !crossOrgUnsupported));
}

// Releases before cross-organization keys answer a create for any other
// organization with a 403 ("API keys can only be created for your
// organization"). They also predate `user_id` and refuse a body with it as a
// 422 before that check (`userIdRefused`, from `isUnknownFieldRejection`), so
// another organization's key, which always sends `user_id`, may meet either.
export function isCrossOrgKeyRefusal(err: unknown, ownOrg: boolean, userIdRefused = false): boolean {
  if (ownOrg) return false;
  return userIdRefused || (typeof err === 'object' && err !== null && (err as { status?: unknown }).status === 403);
}

export const KEY_CROSS_ORG_UNSUPPORTED = 'This API version creates keys only for your own organization.';

// Whether that 403 holds for `orgId`. It is one API pod's answer for one
// organization: during a rollout another pod may create the key. So it
// disables Create for the organization it was answered for only, and the page
// forgets it when the operator tries again or picks an organization.
export function crossOrgKeyRefused(refusedFor: string | null, orgId: string | null | undefined): boolean {
  return Boolean(orgId) && refusedFor === orgId;
}

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
// know the field still creates own-organization keys, which need no choice.
export function createKeyBody(values: CreateKeyValues): { name: string; allowed_surfaces: string[]; user_id?: string } {
  const body: { name: string; allowed_surfaces: string[]; user_id?: string } = {
    name: values.name,
    allowed_surfaces: values.allowed_surfaces,
  };
  if (values.user_id) body.user_id = values.user_id;
  return body;
}

// What the console says when a release refuses `user_id` for a key of the
// own organization (see `isUnknownFieldRejection`).
export const KEY_USER_CHOICE_UNSUPPORTED =
  "This API release cannot choose the key's user. Clear the user to create a key that acts as you, in your own organization.";

export const LAST_USED_CAVEAT =
  'Recorded when the key authenticates a SPACE or E2B-compatible API request, at most once every five minutes. ' +
  'Data-plane calls (ECW envd: terminals, commands and files inside the sandbox) are not recorded. A key not used ' +
  'since the API began recording use shows "Never", even if it was used before.';
