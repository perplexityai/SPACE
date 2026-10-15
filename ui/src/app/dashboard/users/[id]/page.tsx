'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { AdminUser, Organization, UserRole } from '@/lib/types';
import { assignableRoleOptions, canManageUserRole, roleColors, roleLabels } from '@/lib/userRoles';
import { Typography, Descriptions, Select, Tag, App } from 'antd';

const { Title } = Typography;

export default function UserDetailPage() {
  const { message } = App.useApp();
  const { id } = useParams<{ id: string }>();
  const { user: currentUser, cluster } = useAuth();
  const [user, setUser] = useState<AdminUser | null>(null);
  const [org, setOrg] = useState<Organization | null>(null);
  const [visibleOrgIds, setVisibleOrgIds] = useState<string[]>([]);
  const [updatingRole, setUpdatingRole] = useState(false);

  useEffect(() => {
    adminApi.get<AdminUser>(`/v1/users/${id}`).then((u) => {
      setUser(u);
      adminApi.get<Organization>(`/v1/orgs/${u.org_id}`).then(setOrg).catch(() => {});
    });
    adminApi.get<Organization[]>('/v1/orgs')
      .then((orgs) => setVisibleOrgIds(orgs.map((org) => org.id)))
      .catch(() => setVisibleOrgIds([]));
  }, [id]);

  if (!user) return null;

  const roleOptions = assignableRoleOptions(currentUser, cluster);
  const canUpdateRole = canManageUserRole(currentUser, user, cluster, visibleOrgIds) && roleOptions.length > 0;

  const handleRoleChange = async (role: UserRole) => {
    if (user.role === role) return;

    setUpdatingRole(true);
    try {
      const updated = await adminApi.put<AdminUser>(`/v1/users/${user.id}/role`, { role });
      setUser(updated);
      message.success('Role updated');
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setUpdatingRole(false);
    }
  };

  return (
    <div>
      <Title level={3}>{user.display_name}</Title>
      <Descriptions size="small" column={2} style={{ marginBottom: 24 }}>
        <Descriptions.Item label="Email">{user.email}</Descriptions.Item>
        <Descriptions.Item label="Role">
          {canUpdateRole ? (
            <Select<UserRole>
              size="small"
              value={user.role}
              options={roleOptions}
              loading={updatingRole}
              disabled={updatingRole}
              style={{ width: 180 }}
              onChange={handleRoleChange}
            />
          ) : (
            <Tag color={roleColors[user.role]}>{roleLabels[user.role]}</Tag>
          )}
        </Descriptions.Item>
        <Descriptions.Item label="Organization">{org?.name ?? user.org_id}</Descriptions.Item>
        <Descriptions.Item label="Password login">
          <Tag color={user.password_login_configured ? 'green' : 'default'}>
            {user.password_login_configured ? 'Configured' : 'Not configured'}
          </Tag>
        </Descriptions.Item>
        <Descriptions.Item label="ID" contentStyle={{ fontFamily: 'monospace', fontSize: 12 }}>{user.id}</Descriptions.Item>
      </Descriptions>
    </div>
  );
}
