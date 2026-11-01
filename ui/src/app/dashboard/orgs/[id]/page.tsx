'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import { adminApi } from '@/lib/api';
import { loadFailureFrom, type LoadFailure } from '@/lib/load-failure';
import LoadFailureResult from '@/components/load-failure-result';
import { useAuth } from '@/lib/auth';
import { useCuriosityTheme } from '@/lib/theme';
import { ApiKeyMetadata, Organization, OrgMasterKey, Sandbox, User } from '@/lib/types';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
import Link from 'next/link';
import { Alert, Card, Table, Typography, Form, Input, Button, App, Descriptions, Space, Switch, Tag } from 'antd';
import { LockOutlined, SaveOutlined } from '@ant-design/icons';
import { roleLabels } from '@/lib/userRoles';
import type { ColumnsType } from 'antd/es/table';

const { Title } = Typography;

// What the BYOK flag does, on every path that reads it: a new sandbox that is
// not a fork is encrypted, and then never adopts a warm-pool member, only when
// the org flag and the cluster's SPACE_BYOK_VOLUME_ENCRYPTION are both on; with
// the flag on, create admission never claims a warm-pool slot
// (`controllers/sandbox/queries.rs`); a fork keeps its source's encryption and
// needs a source whose encryption matches the flag
// (`fork_operation_executor.rs`); the flag is shared with credential
// encryption; a sandbox's encryption is fixed at create.
const BYOK_VOLUME_SWITCH = 'SPACE_BYOK_VOLUME_ENCRYPTION';
const BYOK_SHARED = 'The same flag is shared with credential encryption.';
const BYOK_EXISTING = 'Existing sandboxes keep the encryption they were created with.';
const BYOK_ENABLE_TEXT = `New sandboxes in this organization run on encrypted volumes keyed through the external provider only where the cluster also enables BYOK volume encryption (${BYOK_VOLUME_SWITCH}); those never come from a warm pool. A fork keeps its source's encryption: forks of the organization's encrypted sandboxes stay encrypted whatever that setting, and forks of its unencrypted sandboxes are refused. Whatever that setting, new sandboxes are not given a warm-pool slot at create admission. ${BYOK_SHARED} ${BYOK_EXISTING}`;
const BYOK_DISABLE_TEXT = `New sandboxes in this organization are created without volume encryption, and forks of its encrypted sandboxes are refused. ${BYOK_SHARED} ${BYOK_EXISTING} Encrypted ones can still start and restore.`;
const BYOK_CARD_TEXT = `When enabled, and the cluster enables BYOK volume encryption (${BYOK_VOLUME_SWITCH}), newly created sandboxes that are not forks run on encrypted volumes keyed through the configured external provider and never come from a warm pool. A fork keeps its source's encryption. With the flag on, whatever that setting, new sandboxes are not given a warm-pool slot at create admission and a fork's source must be encrypted; with it off, a fork's source must be unencrypted. ${BYOK_SHARED} ${BYOK_EXISTING}`;
const ORG_SANDBOX_LIMIT = 5000;

export default function OrgDetailPage() {
  const { message, modal } = App.useApp();
  const { primaryColor: defaultPrimaryColor } = useCuriosityTheme();
  const { id } = useParams<{ id: string }>();
  const { org: authOrg, refreshOrg } = useAuth();
  const [org, setOrg] = useState<Organization | null>(null);
  const [users, setUsers] = useState<User[]>([]);
  // `null` while loading. Only this org's sandbox rows: the org-scoped admin
  // list can also carry rowless node-reported rows (warm-pool slots,
  // observed-local sandboxes) that belong to no org.
  const [sandboxes, setSandboxes] = useState<Sandbox[] | null>(null);
  const [sandboxesError, setSandboxesError] = useState<string | null>(null);
  const [sandboxesCapped, setSandboxesCapped] = useState(false);
  const [masterKey, setMasterKey] = useState<OrgMasterKey | null>(null);
  const [apiKeys, setApiKeys] = useState<ApiKeyMetadata[] | null>(null);
  const [apiKeysError, setApiKeysError] = useState<string | null>(null);
  const [byokSaving, setByokSaving] = useState(false);
  const [loadError, setLoadError] = useState<LoadFailure | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [saving, setSaving] = useState(false);
  const [form] = Form.useForm();

  const errorText = (err: unknown, fallback: string) => (err instanceof Error && err.message ? err.message : fallback);

  const loadSandboxes = () => {
    setSandboxes(null);
    setSandboxesError(null);
    setSandboxesCapped(false);
    adminApi.get<Sandbox[]>(`/v1/sandboxes?org_id=${id}&limit=${ORG_SANDBOX_LIMIT}`)
      .then((rows) => {
        const own = rows.filter((sandbox) => sandbox.resource_type === 'sandbox' && sandbox.org_id === id);
        setSandboxes(own);
        // The API's limit counts only database rows, all of which belong to
        // this org; it appends node-reported rows beyond the limit.
        setSandboxesCapped(own.length >= ORG_SANDBOX_LIMIT);
      })
      .catch((err: unknown) => setSandboxesError(errorText(err, 'Failed to load sandboxes')));
  };

  const loadApiKeys = () => {
    setApiKeys(null);
    setApiKeysError(null);
    adminApi.get<ApiKeyMetadata[]>(`/v1/orgs/${id}/api-keys`)
      .then(setApiKeys)
      .catch((err: unknown) => setApiKeysError(errorText(err, 'Failed to load API keys')));
  };

  useEffect(() => {
    setLoadError(null);
    adminApi.get<Organization>(`/v1/orgs/${id}`).then((o) => {
      setOrg(o);
      // Only for an organization that exists; an unknown ID shows not found.
      loadApiKeys();
      form.setFieldsValue({
        theme_primary: o.theme_primary || '',
        theme_secondary: o.theme_secondary || '',
        logo_url: o.logo_url || '',
      });
    }).catch((err: unknown) => setLoadError(loadFailureFrom(err, 'Failed to load organization')));
    adminApi.get<User[]>(`/v1/orgs/${id}/users`).then(setUsers).catch(() => {});
    loadSandboxes();
    adminApi.get<OrgMasterKey | null>(`/v1/orgs/${id}/master-key`).then(setMasterKey).catch(() => {});
  }, [id, attempt]);

  // Counts over the loaded rows: when the list is capped they cover only the
  // newest ORG_SANDBOX_LIMIT, so they are lower bounds.
  const sandboxCountByUser = (userId: string) => {
    if (!sandboxes) return null;
    const count = sandboxes.filter((sandbox) => sandbox.user_id === userId).length;
    return sandboxesCapped ? `≥${count}` : String(count);
  };
  const sandboxTotal = sandboxesError
    ? 'not loaded'
    : sandboxes === null ? '…' : `${sandboxes.length}${sandboxesCapped ? '+' : ''}`;

  const handleSave = async (values: any) => {
    setSaving(true);
    try {
      const updated = await adminApi.patch<Organization>(`/v1/orgs/${id}`, {
        theme_primary: values.theme_primary || null,
        theme_secondary: values.theme_secondary || null,
        logo_url: values.logo_url || null,
      });
      setOrg(updated);
      if (authOrg && updated.id === authOrg.id) {
        refreshOrg(updated);
      }
      message.success('Branding saved');
    } catch (err) {
      message.error(err instanceof Error ? err.message : 'Failed to save branding');
    } finally {
      setSaving(false);
    }
  };

  // BYOK decides how new sandboxes in the org are created; confirm first. The
  // texts follow the create and fork paths (see the constants above): a new
  // sandbox that is not a fork is encrypted only when the org flag and the
  // cluster's SPACE_BYOK_VOLUME_ENCRYPTION are both on, a fork keeps its
  // source's encryption, the flag is shared with credential encryption, and a
  // sandbox's encryption stamp never changes.
  const confirmByokToggle = (checked: boolean) => {
    modal.confirm({
      title: checked ? `Enable BYOK encryption for ${org?.name}?` : `Disable BYOK encryption for ${org?.name}?`,
      content: checked
        ? BYOK_ENABLE_TEXT
        : BYOK_DISABLE_TEXT,
      okText: checked ? 'Enable BYOK' : 'Disable BYOK',
      onOk: () => handleByokToggle(checked),
    });
  };

  const handleByokToggle = async (checked: boolean) => {
    setByokSaving(true);
    try {
      const updated = await adminApi.patch<Organization>(`/v1/orgs/${id}`, {
        byok_enabled: checked,
      });
      setOrg(updated);
      message.success(checked ? 'BYOK enabled' : 'BYOK disabled');
    } catch (err) {
      message.error(err instanceof Error ? err.message : 'Failed to update BYOK setting');
    } finally {
      setByokSaving(false);
    }
  };

  const userColumns: ColumnsType<User> = [
    { title: 'Name', dataIndex: 'display_name', render: (name: string, record) => <Link href={`/dashboard/users/${record.id}`}>{name}</Link> },
    { title: 'Email', dataIndex: 'email' },
    { title: 'Role', dataIndex: 'role', render: (role: User['role']) => roleLabels[role] ?? role },
    {
      title: 'Sandboxes',
      align: 'right',
      render: (_, record) => sandboxCountByUser(record.id) ?? (sandboxesError ? 'not loaded' : '…'),
    },
  ];

  if (loadError) {
    return (
      <LoadFailureResult
        failure={loadError}
        noun="organization"
        id={id}
        backHref="/dashboard/orgs"
        backLabel="Organizations"
        onRetry={() => setAttempt((value) => value + 1)}
      />
    );
  }

  if (!org) return null;

  return (
    <div>
      <Title level={3}>{org.name}</Title>
      <Descriptions size="small" column={2} style={{ marginBottom: 24 }}>
        <Descriptions.Item label="Slug">{org.slug}</Descriptions.Item>
        <Descriptions.Item label="ID" contentStyle={{ fontFamily: 'monospace', fontSize: 12 }}>{org.id}</Descriptions.Item>
      </Descriptions>

      <Card title="Users" size="small" style={{ marginBottom: 24 }}>
        <Table
          columns={userColumns}
          dataSource={users}
          rowKey="id"
          size="small"
          pagination={false}
          summary={() => (
            <Table.Summary.Row>
              <Table.Summary.Cell index={0} colSpan={4}>
                <strong>Total ({users.length} users, {sandboxTotal} sandboxes)</strong>
              </Table.Summary.Cell>
            </Table.Summary.Row>
          )}
        />
      </Card>

      <Card
        title={`Sandboxes (${sandboxTotal})`}
        size="small"
        style={{ marginBottom: 24 }}
        extra={<Link href={`/dashboard/sandboxes?org=${org.id}`}>Open in Sandboxes</Link>}
      >
        {sandboxesError ? (
          <Alert
            type="error"
            showIcon
            title={`Could not load sandboxes: ${sandboxesError}`}
            action={<Button size="small" onClick={loadSandboxes}>Retry</Button>}
            style={{ marginBottom: 8 }}
          />
        ) : null}
        {sandboxesCapped ? (
          <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
            The newest {ORG_SANDBOX_LIMIT} are listed; the organization has at least that many. The users&apos; sandbox counts cover only these.
          </Typography.Text>
        ) : null}
        <Table
          size="small"
          rowKey="id"
          dataSource={sandboxes ?? []}
          loading={sandboxes === null && !sandboxesError}
          pagination={{ pageSize: 10, hideOnSinglePage: true }}
          locale={{ emptyText: sandboxesError ? 'Not loaded' : 'No sandboxes' }}
          columns={[
            {
              title: 'Sandbox',
              dataIndex: 'id',
              render: (sid: string, row) => (
                <Link href={`/dashboard/sandboxes/${publicSandboxIdFromAny(sid)}`}>{row.name || publicSandboxIdFromAny(sid)}</Link>
              ),
            },
            { title: 'State', dataIndex: 'status', width: 120, render: (v: string | null) => <Tag>{v || 'unknown'}</Tag> },
            { title: 'Template', dataIndex: 'template', width: 160, render: (v: string | null) => v || '—' },
            { title: 'Created', dataIndex: 'created_at', width: 200, render: (v: string | null) => (v ? new Date(v).toLocaleString() : '—') },
          ]}
        />
      </Card>

      <Card
        title={`API keys (${apiKeys ? apiKeys.length : apiKeysError ? 'not loaded' : '…'})`}
        size="small"
        style={{ marginBottom: 24 }}
        extra={<Link href={`/dashboard/api-keys?org=${org.id}`}>Manage API keys</Link>}
      >
        {apiKeysError ? (
          <Alert
            type="error"
            showIcon
            title={`Could not load API keys: ${apiKeysError}`}
            action={<Button size="small" onClick={loadApiKeys}>Retry</Button>}
            style={{ marginBottom: 8 }}
          />
        ) : null}
        <Table
          size="small"
          rowKey="id"
          dataSource={apiKeys ?? []}
          loading={apiKeys === null && !apiKeysError}
          pagination={false}
          locale={{ emptyText: apiKeysError ? 'Not loaded' : 'No API keys' }}
          columns={[
            { title: 'Name', dataIndex: 'name' },
            { title: 'Key ID', dataIndex: 'id', render: (v: string) => <Typography.Text code copyable>{v}</Typography.Text> },
            { title: 'Allowed APIs', dataIndex: 'allowed_surfaces', render: (v: string[]) => v.map((surface) => <Tag key={surface}>{surface === 'native' ? 'SPACE' : 'E2B-compatible'}</Tag>) },
            { title: 'Creator', dataIndex: 'created_by_email', render: (v: string | null) => v || '—' },
            { title: 'Created', dataIndex: 'created_at', width: 200, render: (v: string) => new Date(v).toLocaleString() },
          ]}
        />
      </Card>

      <Card title="Branding" size="small">
        <Form form={form} layout="vertical" onFinish={handleSave} style={{ maxWidth: 500 }}>
          <Form.Item label="Primary Color" name="theme_primary">
            <Input placeholder={defaultPrimaryColor} addonBefore={
              <input
                type="color"
                value={form.getFieldValue('theme_primary') || defaultPrimaryColor}
                onChange={(e) => form.setFieldsValue({ theme_primary: e.target.value })}
                style={{ width: 24, height: 24, border: 'none', padding: 0, cursor: 'pointer' }}
              />
            } />
          </Form.Item>
          <Form.Item label="Secondary Color" name="theme_secondary">
            <Input placeholder="#f5f5f5" addonBefore={
              <input
                type="color"
                value={form.getFieldValue('theme_secondary') || '#f5f5f5'}
                onChange={(e) => form.setFieldsValue({ theme_secondary: e.target.value })}
                style={{ width: 24, height: 24, border: 'none', padding: 0, cursor: 'pointer' }}
              />
            } />
          </Form.Item>
          <Form.Item label="Logo URL" name="logo_url">
            <Input placeholder="https://example.com/logo.png" />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" icon={<SaveOutlined />} loading={saving}>
              Save Branding
            </Button>
          </Form.Item>
        </Form>

        {org.logo_url && (
          <div style={{ marginTop: 16 }}>
            <Typography.Text type="secondary">Logo Preview</Typography.Text>
            <br />
            <img src={org.logo_url} alt="Logo preview" style={{ maxWidth: 200, marginTop: 8 }} />
          </div>
        )}
      </Card>

      <Card
        title={
          <Space size={8}>
            <LockOutlined />
            BYOK Encryption
            {org.byok_enabled && <Tag color="geekblue">enabled</Tag>}
          </Space>
        }
        size="small"
        style={{ marginTop: 24 }}
      >
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Space size={12}>
            <Switch
              checked={org.byok_enabled}
              loading={byokSaving}
              onChange={confirmByokToggle}
            />
            <Typography.Text>
              Encrypt this organization&apos;s sandboxes with its own key
            </Typography.Text>
          </Space>
          <Typography.Text type="secondary">{BYOK_CARD_TEXT}</Typography.Text>

          {masterKey ? (
            <Descriptions size="small" column={1}>
              <Descriptions.Item label="Status">Provisioned</Descriptions.Item>
              <Descriptions.Item label="Provider">{masterKey.provider}</Descriptions.Item>
              <Descriptions.Item label="Since">{masterKey.created_at}</Descriptions.Item>
            </Descriptions>
          ) : (
            <Typography.Text type="secondary">
              No branching key has been provisioned yet. The Credentials
              Authority provisions one on first encrypted use.
            </Typography.Text>
          )}
        </Space>
      </Card>
    </div>
  );
}
