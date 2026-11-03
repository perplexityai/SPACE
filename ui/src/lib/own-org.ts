// Whether a sandbox is in the signed-in operator's own organization, which
// decides the route the console reads and acts on it through: the public
// routes serve only that organization; the admin routes serve any, and some
// of their reads are audited. An organization not known on either side (the
// operator's before sign-in has loaded, a sandbox row without one) counts as
// the operator's own, the route the console used before the admin routes.
export function isOwnOrg(
  signedInOrgId: string | null | undefined,
  sandboxOrgId: string | null | undefined,
): boolean {
  return !signedInOrgId || !sandboxOrgId || signedInOrgId === sandboxOrgId;
}
