import { Cluster, User, UserRole } from './types';

export const roleLabels: Record<UserRole, string> = {
  organization_admin: 'Organization Admin',
  member: 'Member',
};

export const roleColors: Record<UserRole, string> = {
  organization_admin: 'blue',
  member: 'default',
};

const roleOptions: Array<{ value: UserRole; label: string }> = [
  { value: 'member', label: roleLabels.member },
  { value: 'organization_admin', label: roleLabels.organization_admin },
];

export function isManagingOrgAdmin(actor: User | null, cluster: Cluster | null): boolean {
  return actor?.role === 'organization_admin' && actor.org_id === cluster?.managing_org_id;
}

export function assignableRoleOptions(
  actor: User | null,
  cluster: Cluster | null,
): Array<{ value: UserRole; label: string }> {
  if (isManagingOrgAdmin(actor, cluster)) return roleOptions;
  if (actor?.role === 'organization_admin') return roleOptions;
  return [];
}

export function canManageUserRole(
  actor: User | null,
  target: User,
  cluster: Cluster | null,
  visibleOrgIds: string[] = [],
): boolean {
  if (!actor || actor.role === 'member') return false;
  if (actor.id === target.id) return false;
  if (isManagingOrgAdmin(actor, cluster)) return true;
  return actor.role === 'organization_admin' && visibleOrgIds.includes(target.org_id);
}

export function canDeleteUser(
  actor: User | null,
  target: User,
  cluster: Cluster | null,
  visibleOrgIds: string[] = [],
): boolean {
  return canManageUserRole(actor, target, cluster, visibleOrgIds);
}

// The confirmation shown before a role change; granting organization admin
// in the managing organization grants operator access to this console.
// `inManagingOrg`: the user belongs to the organization that manages the
// cluster, whose organization admins are the only users of this console.
export function roleChangeConfirmation(
  email: string,
  role: UserRole,
  { inManagingOrg = false, currentRole }: { inManagingOrg?: boolean; currentRole?: UserRole } = {},
) {
  let content: string;
  if (role === 'organization_admin') {
    content = inManagingOrg
      ? 'Organization admins manage their organization. This user is in the organization that manages the cluster, so they can also operate it from this console.'
      : 'Organization admins manage their organization. The role does not give access to this admin console, which only admins of the organization that manages the cluster can use.';
  } else if (currentRole === undefined || currentRole === 'organization_admin') {
    content = inManagingOrg
      ? 'They lose organization admin rights and access to this admin console.'
      : 'They lose organization admin rights.';
  } else {
    content = `Their role becomes ${roleLabels[role]}.`;
  }
  return { title: `Make ${email} ${roleLabels[role]}?`, content, okText: 'Change role' };
}
