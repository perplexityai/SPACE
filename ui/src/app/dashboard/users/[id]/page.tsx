'use client';

import { useEffect, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import Link from 'next/link';
import { adminApi } from '@/lib/api';
import { loadFailureFrom, type LoadFailure } from '@/lib/load-failure';
import LoadFailureResult from '@/components/load-failure-result';
import { useAuth } from '@/lib/auth';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
import { AdminUser, ApiKeyMetadata, Organization, Sandbox, UserRole } from '@/lib/types';
import { assignableRoleOptions, canDeleteUser, canManageUserRole, roleChangeConfirmation, roleColors, roleLabels } from '@/lib/userRoles';
import { Typography, Descriptions, Select, Tag, App, Button, Card, Form, Input, Modal, Popconfirm, Space, Table, Tooltip } from 'antd';
import { DeleteOutlined, EditOutlined, KeyOutlined } from '@ant-design/icons';
import { USER_DELETE_CONSEQUENCES, USER_OWNS_SANDBOXES, userDeleteFailureMessage } from '@/lib/user-delete';

const { Title, Text } = Typography;

type EditValues = { display_name: string; email: string };
type PasswordValues = { password: string; confirm: string };

export default function UserDetailPage() {
  const { message, modal } = App.useApp();
  const router = useRouter();
  const { id } = useParams<{ id: string }>();
  const { user: currentUser, cluster } = useAuth();
  const [user, setUser] = useState<AdminUser | null>(null);
  const [org, setOrg] = useState<Organization | null>(null);
  const [visibleOrgIds, setVisibleOrgIds] = useState<string[]>([]);
  const [updatingRole, setUpdatingRole] = useState(false);
  const [loadError, setLoadError] = useState<LoadFailure | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [sandboxes, setSandboxes] = useState<Sandbox[] | null>(null);
  const [keys, setKeys] = useState<ApiKeyMetadata[] | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [passwordOpen, setPasswordOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [editForm] = Form.useForm<EditValues>();
  const [passwordForm] = Form.useForm<PasswordValues>();

  useEffect(() => {
    setLoadError(null);
    adminApi.get<AdminUser>(`/v1/users/${id}`).then((u) => {
      setUser(u);
      adminApi.get<Organization>(`/v1/orgs/${u.org_id}`).then(setOrg).catch(() => {});
      // The newest sandboxes of the user's org, narrowed to this user.
      adminApi.get<Sandbox[]>(`/v1/sandboxes?org_id=${u.org_id}&limit=1000`)
        .then((rows) => setSandboxes(rows.filter((sandbox) => sandbox.user_id === u.id)))
        .catch(() => setSandboxes(null));
      adminApi.get<ApiKeyMetadata[]>(`/v1/orgs/${u.org_id}/api-keys`)
        .then((rows) => setKeys(rows.filter((key) => key.created_by === u.id)))
        .catch(() => setKeys(null));
    }).catch((err: unknown) => setLoadError(loadFailureFrom(err, 'Failed to load user')));
    adminApi.get<Organization[]>('/v1/orgs')
      .then((orgs) => setVisibleOrgIds(orgs.map((org) => org.id)))
      .catch(() => setVisibleOrgIds([]));
  }, [id, attempt]);

  if (loadError) {
    return (
      <LoadFailureResult
        failure={loadError}
        noun="user"
        id={id}
        backHref="/dashboard/users"
        backLabel="Users"
        onRetry={() => setAttempt((value) => value + 1)}
      />
    );
  }

  if (!user) return null;

  const roleOptions = assignableRoleOptions(currentUser, cluster);
  const canUpdateRole = canManageUserRole(currentUser, user, cluster, visibleOrgIds) && roleOptions.length > 0;
  const canEdit = canManageUserRole(currentUser, user, cluster, visibleOrgIds);
  const canDelete = canDeleteUser(currentUser, user, cluster, visibleOrgIds);

  const applyRole = async (role: UserRole) => {
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

  const handleRoleChange = (role: UserRole) => {
    if (user.role === role) return;
    modal.confirm({ ...roleChangeConfirmation(user.email, role), onOk: () => applyRole(role) });
  };

  const patchUser = async (body: Partial<{ email: string; display_name: string; password: string }>, done: string) => {
    setSaving(true);
    try {
      const updated = await adminApi.patch<AdminUser>(`/v1/users/${user.id}`, body);
      setUser({ ...user, ...updated });
      message.success(done);
      return true;
    } catch (err: any) {
      message.error(err.message);
      return false;
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    try {
      await adminApi.delete(`/v1/users/${user.id}`);
      message.success('User deleted');
      router.push('/dashboard/users');
    } catch (err: unknown) {
      message.error(userDeleteFailureMessage(err));
    }
  };

  // The list covers the org's newest 1000 sandboxes, so a refused delete is
  // still possible when it is empty; the handler reports that too.
  const ownsSandboxes = (sandboxes?.length ?? 0) > 0;

  return (
    <div>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <Title level={3}>{user.display_name}</Title>
        {canEdit ? (
          <Space>
            <Button icon={<EditOutlined />} onClick={() => { editForm.setFieldsValue({ display_name: user.display_name, email: user.email }); setEditOpen(true); }}>
              Edit
            </Button>
            <Button icon={<KeyOutlined />} onClick={() => { passwordForm.resetFields(); setPasswordOpen(true); }}>
              Set password
            </Button>
            {canDelete ? (
              ownsSandboxes ? (
                <Tooltip title={USER_OWNS_SANDBOXES}>
                  <Button danger icon={<DeleteOutlined />} disabled>Delete</Button>
                </Tooltip>
              ) : (
                <Popconfirm
                  title="Delete this user?"
                  description={USER_DELETE_CONSEQUENCES}
                  okText="Delete"
                  okButtonProps={{ danger: true }}
                  onConfirm={handleDelete}
                >
                  <Button danger icon={<DeleteOutlined />}>Delete</Button>
                </Popconfirm>
              )
            ) : null}
          </Space>
        ) : null}
      </Space>
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
        <Descriptions.Item label="Organization">
          <Link href={`/dashboard/orgs/${user.org_id}`}>{org?.name ?? user.org_id}</Link>
        </Descriptions.Item>
        <Descriptions.Item label="Password login">
          <Tag color={user.password_login_configured ? 'green' : 'default'}>
            {user.password_login_configured ? 'Configured' : 'Not configured'}
          </Tag>
        </Descriptions.Item>
        <Descriptions.Item label="ID" contentStyle={{ fontFamily: 'monospace', fontSize: 12 }}>{user.id}</Descriptions.Item>
      </Descriptions>

      <Card
        size="small"
        title="Sandboxes"
        style={{ marginBottom: 16 }}
        extra={<Link href={`/dashboard/sandboxes?org=${user.org_id}`}>All of the organization&apos;s sandboxes</Link>}
      >
        <Table
          size="small"
          rowKey="id"
          dataSource={sandboxes ?? []}
          loading={sandboxes === null}
          pagination={false}
          locale={{ emptyText: 'No sandboxes owned by this user among the organization\'s newest 1000' }}
          columns={[
            { title: 'Sandbox', dataIndex: 'id', render: (sid: string, row) => <Link href={`/dashboard/sandboxes/${publicSandboxIdFromAny(sid)}`}>{row.name || publicSandboxIdFromAny(sid)}</Link> },
            { title: 'State', dataIndex: 'status', width: 120, render: (v: string | null) => <Tag>{v || 'unknown'}</Tag> },
            { title: 'Template', dataIndex: 'template', width: 160, render: (v: string | null) => v || '—' },
            { title: 'Created', dataIndex: 'created_at', width: 200, render: (v: string | null) => (v ? new Date(v).toLocaleString() : '—') },
          ]}
        />
      </Card>

      <Card size="small" title="API keys created by this user">
        <Table
          size="small"
          rowKey="id"
          dataSource={keys ?? []}
          loading={keys === null}
          pagination={false}
          locale={{ emptyText: 'None' }}
          columns={[
            { title: 'Name', dataIndex: 'name' },
            { title: 'Key ID', dataIndex: 'id', render: (v: string) => <Text code copyable>{v}</Text> },
            { title: 'Created', dataIndex: 'created_at', width: 200, render: (v: string) => new Date(v).toLocaleString() },
          ]}
        />
      </Card>

      <Modal
        title="Edit user"
        open={editOpen}
        okText="Save"
        confirmLoading={saving}
        onOk={() => editForm.submit()}
        onCancel={() => setEditOpen(false)}
        destroyOnHidden
      >
        <Form
          form={editForm}
          layout="vertical"
          preserve={false}
          onFinish={async (values) => {
            if (await patchUser({ display_name: values.display_name, email: values.email }, 'User updated')) setEditOpen(false);
          }}
        >
          <Form.Item name="display_name" label="Display name" rules={[{ required: true }]}>
            <Input autoComplete="off" />
          </Form.Item>
          <Form.Item name="email" label="Email" rules={[{ required: true, type: 'email' }]}>
            <Input autoComplete="off" />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={`Set password for ${user.email}`}
        open={passwordOpen}
        okText="Set password"
        confirmLoading={saving}
        onOk={() => passwordForm.submit()}
        onCancel={() => setPasswordOpen(false)}
        destroyOnHidden
      >
        <Form
          form={passwordForm}
          layout="vertical"
          preserve={false}
          onFinish={async (values) => {
            if (await patchUser({ password: values.password }, 'Password set')) setPasswordOpen(false);
          }}
        >
          <Form.Item name="password" label="New password" rules={[{ required: true }, { max: 72, message: 'At most 72 characters' }]}>
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Form.Item
            name="confirm"
            label="Repeat password"
            dependencies={['password']}
            rules={[
              { required: true },
              ({ getFieldValue }) => ({
                validator: (_, value) =>
                  !value || value === getFieldValue('password') ? Promise.resolve() : Promise.reject(new Error('Passwords differ')),
              }),
            ]}
          >
            <Input.Password autoComplete="new-password" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
