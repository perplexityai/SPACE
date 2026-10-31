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
export function roleChangeConfirmation(email: string, role: UserRole) {
  return {
    title: `Make ${email} ${roleLabels[role]}?`,
    content:
      role === 'organization_admin'
        ? 'Organization admins manage their organization; in the organization that manages the cluster they can also operate it from this console.'
        : 'They lose organization admin rights.',
    okText: 'Change role',
  };
}
