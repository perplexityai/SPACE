// The owner of a sandbox the console creates in another organization. The
// admin create route takes the owner as `user_id`, a user of the target
// organization; the console always sends one, chosen by the operator, so the
// sandbox is attributed to someone the operator picked. A create in the
// operator's own organization goes through the public route, which records
// the signed-in operator as the owner, and has no owner to choose.

export interface OwnerCandidate {
  id: string;
  email: string;
  display_name?: string | null;
}

// One answer of `GET /orgs/{org_id}/users`, kept with the organization it was
// read for, so a list read for an earlier organization is never offered.
export interface OwnerUsers {
  orgId: string;
  users: OwnerCandidate[] | null;
  error: string | null;
}

export type SandboxOwner =
  | { kind: 'own-org' }
  | { kind: 'loading' }
  | { kind: 'failed'; error: string }
  | { kind: 'no-users' }
  | { kind: 'unchosen'; users: OwnerCandidate[] }
  | { kind: 'chosen'; orgId: string; users: OwnerCandidate[]; user: OwnerCandidate };

// The organization a create goes to through the admin route, or null for the
// public route: a chosen organization other than the operator's own. Before
// sign-in has loaded the operator's organization, any chosen one counts as
// another's, so a create never lands in the wrong organization.
export function crossOrgCreateTarget(
  chosenOrgId: string | null | undefined,
  signedInOrgId: string | null | undefined,
): string | null {
  return chosenOrgId && chosenOrgId !== signedInOrgId ? chosenOrgId : null;
}

// What the create form knows about the owner of a create in `targetOrgId`.
// A chosen user counts only when the list read for that organization has them.
export function sandboxOwner({
  targetOrgId,
  loaded,
  chosenUserId,
}: {
  targetOrgId: string | null;
  loaded: OwnerUsers | null;
  chosenUserId?: string | null;
}): SandboxOwner {
  if (!targetOrgId) return { kind: 'own-org' };
  if (!loaded || loaded.orgId !== targetOrgId) return { kind: 'loading' };
  if (loaded.error !== null || loaded.users === null) {
    return { kind: 'failed', error: loaded.error ?? 'no users were read' };
  }
  const { users } = loaded;
  if (users.length === 0) return { kind: 'no-users' };
  const user = chosenUserId ? users.find((candidate) => candidate.id === chosenUserId) : undefined;
  return user ? { kind: 'chosen', orgId: targetOrgId, users, user } : { kind: 'unchosen', users };
}

// Create may be pressed for the own organization, and for another only once
// an owner of that organization is chosen.
export function canCreateWithOwner(owner: SandboxOwner): owner is Extract<SandboxOwner, { kind: 'own-org' | 'chosen' }> {
  return owner.kind === 'own-org' || owner.kind === 'chosen';
}

// The line beside the Owner select; null where it needs none.
export function sandboxOwnerText(owner: SandboxOwner): string | null {
  switch (owner.kind) {
    case 'own-org':
    case 'chosen':
      return null;
    case 'loading':
      return "Loading the organization's users…";
    case 'failed':
      return `Could not load the organization's users (${owner.error}); retry to choose an owner.`;
    case 'no-users':
      return 'The organization has no users to own the sandbox; create a user first.';
    case 'unchosen':
      return 'Choose the user who owns the sandbox.';
  }
}

export function ownerOptionLabel(user: OwnerCandidate): string {
  return user.display_name ? `${user.email} (${user.display_name})` : user.email;
}
