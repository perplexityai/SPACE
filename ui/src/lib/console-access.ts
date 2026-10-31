import type { Cluster, User } from './types';

// Who may use the console. Every page reads the admin API, which answers only
// organization admins of the cluster's managing organization. Deciding that
// needs the cluster record; when it failed to load, an organization admin is
// neither admitted nor turned away.
export type ConsoleAccess = 'operator' | 'not-operator' | 'cluster-unknown';

export function consoleAccess(
  user: Pick<User, 'role' | 'org_id'>,
  cluster: Pick<Cluster, 'managing_org_id'> | null,
): ConsoleAccess {
  if (user.role !== 'organization_admin') return 'not-operator';
  if (!cluster) return 'cluster-unknown';
  return user.org_id === cluster.managing_org_id ? 'operator' : 'not-operator';
}
