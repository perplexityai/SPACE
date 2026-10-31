'use client';

import { useEffect, useRef, useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import Link from 'next/link';
import { adminApi } from '@/lib/api';
import { loadFailureFrom, type LoadFailure } from '@/lib/load-failure';
import LoadFailureResult from '@/components/load-failure-result';
import { useAuth } from '@/lib/auth';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
import { setSignInNotice } from '@/lib/sign-in-notice';
import { AdminUser, ApiKeyMetadata, Organization, Sandbox, UserRole } from '@/lib/types';
import { assignableRoleOptions, canDeleteUser, canManageUserRole, roleChangeConfirmation, roleColors, roleLabels } from '@/lib/userRoles';
import { Typography, Descriptions, Select, Tag, App, Alert, Button, Card, Form, Input, Modal, Popconfirm, Space, Table, Tooltip } from 'antd';
import { DeleteOutlined, EditOutlined, KeyOutlined, ReloadOutlined } from '@ant-design/icons';
import { MAX_PASSWORD_BYTES, USER_DELETE_CONSEQUENCES, USER_OWNS_SANDBOXES, passwordTooLong, userDeleteFailureMessage } from '@/lib/user-delete';

const { Title, Text } = Typography;

type EditValues = { display_name: string; email: string };
type PasswordValues = { password: string; confirm: string };

// The org's sandboxes are read from the admin list, newest first; the same
// size the org page reads.
const ORG_SANDBOX_LIMIT = 5000;
// Seeded accounts such as `root@local` have no dot in their domain, which a
// strict email rule rejects; the API decides what an email is.
const LOOSE_EMAIL = /^[^@\s]+@[^@\s]+$/;

export default function UserDetailPage() {
  const { message, modal } = App.useApp();
  const router = useRouter();
  const { id } = useParams<{ id: string }>();
  const { user: currentUser, cluster, authMode } = useAuth();
  const [user, setUser] = useState<AdminUser | null>(null);
  const [org, setOrg] = useState<Organization | null>(null);
  const [visibleOrgIds, setVisibleOrgIds] = useState<string[]>([]);
  const [updatingRole, setUpdatingRole] = useState(false);
  const [loadError, setLoadError] = useState<LoadFailure | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [sandboxes, setSandboxes] = useState<Sandbox[] | null>(null);
  const [sandboxesError, setSandboxesError] = useState<string | null>(null);
  const [keys, setKeys] = useState<ApiKeyMetadata[] | null>(null);
  const [keysError, setKeysError] = useState<string | null>(null);
  const [editOpen, setEditOpen] = useState(false);
  const [passwordOpen, setPasswordOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [editForm] = Form.useForm<EditValues>();
  // Saves answer with the user as that request saw it; a profile save and a
  // role save in flight together could leave the older answer on screen.
  // Each save instead reloads the user, and only the newest reload applies.
  // When that reload fails, the save's own answer applies instead (under the
  // same check), so a later Edit does not send back what the save replaced.
  const userRequest = useRef(0);
  const [reloadError, setReloadError] = useState<string | null>(null);
  const reloadUser = async (saved?: AdminUser) => {
    const request = ++userRequest.current;
    try {
      const fresh = await adminApi.get<AdminUser>(`/v1/users/${id}`);
      if (request !== userRequest.current) return;
      setUser(fresh);
      setReloadError(null);
    } catch (err: unknown) {
      if (request !== userRequest.current) return;
      if (saved) setUser(saved);
      setReloadError(errorText(err, 'Failed to load user'));
    }
  };
  const [passwordForm] = Form.useForm<PasswordValues>();

  const errorText = (err: unknown, fallback: string) => (err instanceof Error && err.message ? err.message : fallback);

  // The newest sandboxes of the user's org, narrowed to this user.
  const loadSandboxes = (u: AdminUser) => {
    setSandboxes(null);
    setSandboxesError(null);
    adminApi.get<Sandbox[]>(`/v1/sandboxes?org_id=${u.org_id}&limit=${ORG_SANDBOX_LIMIT}`)
      .then((rows) => setSandboxes(rows.filter((sandbox) => sandbox.user_id === u.id)))
      .catch((err: unknown) => setSandboxesError(errorText(err, 'Failed to load sandboxes')));
  };

  const loadKeys = (u: AdminUser) => {
    setKeys(null);
    setKeysError(null);
    adminApi.get<ApiKeyMetadata[]>(`/v1/orgs/${u.org_id}/api-keys`)
      .then((rows) => setKeys(rows.filter((key) => key.created_by === u.id)))
      .catch((err: unknown) => setKeysError(errorText(err, 'Failed to load API keys')));
  };

  useEffect(() => {
    setLoadError(null);
    adminApi.get<AdminUser>(`/v1/users/${id}`).then((u) => {
      setUser(u);
      adminApi.get<Organization>(`/v1/orgs/${u.org_id}`).then(setOrg).catch(() => {});
      loadSandboxes(u);
      loadKeys(u);
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
  const isSelf = currentUser?.id === user.id;
  // The API lets anyone edit their own profile and password; only a role
  // change and a delete refuse a self-target.
  const canUpdateRole = !isSelf && canManageUserRole(currentUser, user, cluster, visibleOrgIds) && roleOptions.length > 0;
  const canEdit = isSelf || canManageUserRole(currentUser, user, cluster, visibleOrgIds);
  const canDelete = !isSelf && canDeleteUser(currentUser, user, cluster, visibleOrgIds);
  // External sign-in has no local passwords. The API also refuses one for a
  // federated user, which its user response does not mark; that refusal is
  // shown as the API's message.
  const canSetPassword = canEdit && authMode?.mode !== 'external';
  // `null` when the cluster record did not load: console access is unknown.
  const inManagingOrg = cluster ? user.org_id === cluster.managing_org_id : null;
  // External sign-in matches the provider's email exactly against the user's
  // email, so changing your own would lock you out; only another
  // administrator of the managing organization could change it back.
  const emailLocked = isSelf && authMode?.mode === 'external';

  const applyRole = async (role: UserRole) => {
    setUpdatingRole(true);
    try {
      const saved = await adminApi.put<AdminUser>(`/v1/users/${user.id}/role`, { role });
      message.success('Role updated');
      await reloadUser(saved);
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setUpdatingRole(false);
    }
  };

  const handleRoleChange = (role: UserRole) => {
    if (user.role === role) return;
    modal.confirm({ ...roleChangeConfirmation(user.email, role, { inManagingOrg, currentRole: user.role }), onOk: () => applyRole(role) });
  };

  const patchUser = async (body: Partial<{ email: string; display_name: string; password: string }>, done: string) => {
    setSaving(true);
    try {
      const saved = await adminApi.patch<AdminUser>(`/v1/users/${user.id}`, body);
      if (body.password !== undefined && isSelf) {
        // Setting a password ends every session of that user, this one too.
        setSignInNotice('Your password was changed and your sessions ended. Sign in with the new password.');
        localStorage.removeItem('token');
        window.location.assign('/login');
        return true;
      }
      message.success(done);
      await reloadUser(saved);
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
      message.error(userDeleteFailureMessage(err, { ownsSandboxes }));
    }
  };

  // The list covers the org's newest 5000 sandboxes, so a refused delete is
  // still possible when it is empty; the handler reports that too.
  const ownsSandboxes = (sandboxes?.length ?? 0) > 0;

  return (
    <div>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <Title level={3}>{user.display_name}</Title>
        {canEdit ? (
          <Space>
            <Button icon={<EditOutlined />} onClick={() => setEditOpen(true)}>
              Edit
            </Button>
            {canSetPassword ? (
              <Button icon={<KeyOutlined />} onClick={() => { passwordForm.resetFields(); setPasswordOpen(true); }}>
                Set password
              </Button>
            ) : null}
            {canDelete && sandboxesError ? <Text type="secondary">sandbox list unavailable</Text> : null}
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
      {reloadError ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message={`Saved; the page could not reload the user: ${reloadError}`}
          action={<Button size="small" icon={<ReloadOutlined />} onClick={() => void reloadUser()}>Retry</Button>}
        />
      ) : null}
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
        extra={<Link href={`/dashboard/sandboxes?org=${user.org_id}`}>The organization&apos;s sandbox list</Link>}
      >
        {sandboxesError ? (
          <Alert
            type="error"
            showIcon
            title={`Could not load sandboxes: ${sandboxesError}`}
            action={<Button size="small" icon={<ReloadOutlined />} onClick={() => loadSandboxes(user)}>Retry</Button>}
            style={{ marginBottom: 8 }}
          />
        ) : null}
        <Table
          size="small"
          rowKey="id"
          dataSource={sandboxes ?? []}
          loading={sandboxes === null && !sandboxesError}
          pagination={false}
          locale={{ emptyText: sandboxesError ? 'Not loaded' : `No sandboxes owned by this user among the organization's newest ${ORG_SANDBOX_LIMIT}` }}
          columns={[
            { title: 'Sandbox', dataIndex: 'id', render: (sid: string, row) => <Link href={`/dashboard/sandboxes/${publicSandboxIdFromAny(sid)}`}>{row.name || publicSandboxIdFromAny(sid)}</Link> },
            { title: 'State', dataIndex: 'status', width: 120, render: (v: string | null) => <Tag>{v || 'unknown'}</Tag> },
            { title: 'Template', dataIndex: 'template', width: 160, render: (v: string | null) => v || '—' },
            { title: 'Created', dataIndex: 'created_at', width: 200, render: (v: string | null) => (v ? new Date(v).toLocaleString() : '—') },
          ]}
        />
      </Card>

      <Card size="small" title="API keys created by this user">
        {keysError ? (
          <Alert
            type="error"
            showIcon
            title={`Could not load API keys: ${keysError}`}
            action={<Button size="small" icon={<ReloadOutlined />} onClick={() => loadKeys(user)}>Retry</Button>}
            style={{ marginBottom: 8 }}
          />
        ) : null}
        <Table
          size="small"
          rowKey="id"
          dataSource={keys ?? []}
          loading={keys === null && !keysError}
          pagination={false}
          locale={{ emptyText: keysError ? 'Not loaded' : 'None' }}
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
          // The dialog mounts its form when it opens, so the current values
          // are initial values; setting fields before that is lost.
          initialValues={{ display_name: user.display_name, email: user.email }}
          onFinish={async (values) => {
            const body = emailLocked ? { display_name: values.display_name } : { display_name: values.display_name, email: values.email };
            if (await patchUser(body, 'User updated')) setEditOpen(false);
          }}
        >
          <Form.Item name="display_name" label="Display name" rules={[{ required: true }]}>
            <Input autoComplete="off" />
          </Form.Item>
          <Form.Item
            name="email"
            label="Email"
            rules={emailLocked ? [] : [{ required: true }, { pattern: LOOSE_EMAIL, message: 'Enter an email address' }]}
            extra={emailLocked ? 'Your email is how external sign-in identifies you. Changing it here would lock you out, so it cannot be edited from your own page.' : undefined}
          >
            <Input autoComplete="off" disabled={emailLocked} />
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
        <Typography.Paragraph type="secondary">
          {isSelf
            ? 'Setting a password ends all of your sessions, including this one: you will sign in again with the new password.'
            : "Setting a password ends all of this user's sessions. Your own session is not affected."}
        </Typography.Paragraph>
        <Form
          form={passwordForm}
          layout="vertical"
          preserve={false}
          onFinish={async (values) => {
            if (await patchUser({ password: values.password }, 'Password set')) setPasswordOpen(false);
          }}
        >
          <Form.Item
            name="password"
            label="New password"
            rules={[
              { required: true },
              {
                validator: (_, value: string | undefined) =>
                  value && passwordTooLong(value) ? Promise.reject(new Error(`At most ${MAX_PASSWORD_BYTES} bytes`)) : Promise.resolve(),
              },
            ]}
          >
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
