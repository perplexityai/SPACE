'use client';

import { Suspense, useEffect, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import {
  Alert,
  Button,
  Card,
  Checkbox,
  Form,
  Input,
  Modal,
  Popconfirm,
  Select,
  Space,
  Switch,
  Table,
  Typography,
  App,
} from 'antd';
import { CopyOutlined, DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { isUnknownFieldRejection } from '@/lib/admin-fallback';
import {
  KEY_CROSS_ORG_UNSUPPORTED,
  KEY_USER_CHOICE_UNSUPPORTED,
  canCreateKey,
  createKeyBody,
  crossOrgKeyRefused,
  isCrossOrgKeyRefusal,
  keyActorText,
  keyActsAs,
  keyActsAsText,
  keyActsAsUnknownText,
} from '@/lib/api-keys';
import { apiKeyColumns } from '@/components/api-key-columns';
import type {
  AdminUser,
  ApiKeyMetadata,
  ApiSurface,
  CreatedApiKey,
  Organization,
} from '@/lib/types';

const { Paragraph, Text, Title } = Typography;

type CreateApiKeyValues = {
  name: string;
  allowed_surfaces: ApiSurface[];
  user_id?: string | null;
};

// `useSearchParams` needs a Suspense boundary in the app router.
export default function ApiKeysPage() {
  return (
    <Suspense fallback={null}>
      <ApiKeysPageContent />
    </Suspense>
  );
}

function ApiKeysPageContent() {
  // `?org=<id>` opens a given organization's keys (linked from its page).
  const requestedOrgId = useSearchParams().get('org');
  const { message } = App.useApp();
  const { org: signedInOrg, user: signedInUser } = useAuth();
  const [organizations, setOrganizations] = useState<Organization[]>([]);
  const [orgId, setOrgId] = useState<string>();
  const [keys, setKeys] = useState<ApiKeyMetadata[]>([]);
  const [loading, setLoading] = useState(false);
  const [includeRevoked, setIncludeRevoked] = useState(false);
  const [reloadKeys, setReloadKeys] = useState(0);
  // The selected organization's users, whom a new key can act as.
  const [orgUsers, setOrgUsers] = useState<AdminUser[] | null>(null);
  const [orgUsersError, setOrgUsersError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  // The created key with the organization and user it was created for, as
  // the dialog that shows its secret names them.
  const [created, setCreated] = useState<{ key: CreatedApiKey; orgName: string; actsAs: string } | null>(null);
  // The organization a create was refused for with the 403 a release that
  // creates keys only for the operator's own answers (`crossOrgKeyRefused`).
  const [crossOrgRefusedFor, setCrossOrgRefusedFor] = useState<string | null>(null);
  // Picking an organization asks the API afresh, also for the refused one.
  useEffect(() => setCrossOrgRefusedFor(null), [orgId]);
  const [reloadUsers, setReloadUsers] = useState(0);
  const [form] = Form.useForm<CreateApiKeyValues>();
  const chosenUserId = Form.useWatch('user_id', form);

  useEffect(() => {
    let active = true;
    adminApi
      .get<Organization[]>('/v1/orgs')
      .then((loaded) => {
        if (!active) return;
        setOrganizations(loaded);
        const preferred =
          loaded.find((candidate) => candidate.id === requestedOrgId)
          || loaded.find((candidate) => candidate.id === signedInOrg?.id)
          || loaded[0];
        setOrgId(preferred?.id);
      })
      .catch((error) => {
        if (active) {
          message.error(error instanceof Error ? error.message : 'Failed to load organizations');
        }
      });
    return () => {
      active = false;
    };
  }, [signedInOrg?.id, requestedOrgId]);

  useEffect(() => {
    if (!orgId) {
      setKeys([]);
      setLoading(false);
      return;
    }
    let active = true;
    setKeys([]);
    setLoading(true);
    // Releases before revocation records ignore `include_revoked` and list
    // only live keys.
    adminApi
      .get<ApiKeyMetadata[]>(`/v1/orgs/${orgId}/api-keys${includeRevoked ? '?include_revoked=true' : ''}`)
      .then((loadedKeys) => {
        if (active) setKeys(loadedKeys);
      })
      .catch((error) => {
        if (active) {
          setKeys([]);
          message.error(error instanceof Error ? error.message : 'Failed to load API keys');
        }
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, [orgId, includeRevoked, reloadKeys]);

  useEffect(() => {
    setOrgUsers(null);
    setOrgUsersError(null);
    if (!orgId) return;
    let active = true;
    adminApi
      .get<AdminUser[]>(`/v1/orgs/${orgId}/users`)
      .then((users) => {
        if (active) setOrgUsers(users);
      })
      .catch((error) => {
        if (active) setOrgUsersError(error instanceof Error ? error.message : 'Failed to load users');
      });
    return () => {
      active = false;
    };
  }, [orgId, reloadUsers]);

  const ownOrg = !!orgId && orgId === signedInOrg?.id;
  const orgName = organizations.find((organization) => organization.id === orgId)?.name ?? 'this organization';
  const actsAs = orgId && signedInUser && orgUsers
    ? keyActsAs({ orgId, caller: signedInUser, users: orgUsers, chosenUserId })
    : null;
  const usersLoading = !!orgId && orgUsers === null && orgUsersError === null;
  const crossOrgUnsupported = crossOrgKeyRefused(crossOrgRefusedFor, orgId);
  const createAllowed = canCreateKey({ usersLoading, ownOrg, userChosen: actsAs?.kind === 'chosen', crossOrgUnsupported });
  const actsAsExtra = !ownOrg && crossOrgUnsupported
    ? KEY_CROSS_ORG_UNSUPPORTED
    : actsAs
      ? keyActsAsText(actsAs)
      : keyActsAsUnknownText({ ownOrg, usersError: orgUsersError });

  const openCreate = () => {
    form.resetFields();
    form.setFieldsValue({ allowed_surfaces: ['ecw'] });
    setCreateOpen(true);
  };

  const createKey = async (values: CreateApiKeyValues) => {
    if (!orgId) return;
    setCreating(true);
    try {
      const result = await adminApi.post<CreatedApiKey>(`/v1/orgs/${orgId}/api-keys`, createKeyBody(values));
      setKeys((current) => [result.api_key, ...current]);
      setCreateOpen(false);
      // The key's user as the API stored it; the preview only when an older
      // release does not say.
      const stored = keyActorText(result.api_key);
      setCreated({
        key: result,
        orgName,
        actsAs: stored.recorded ? stored.text : actsAs && 'user' in actsAs ? actsAs.user.email : stored.text,
      });
      message.success('API key created');
    } catch (error) {
      const userIdRefused = isUnknownFieldRejection(error, 'user_id');
      if (isCrossOrgKeyRefusal(error, ownOrg, userIdRefused)) {
        setCrossOrgRefusedFor(orgId);
        message.error(KEY_CROSS_ORG_UNSUPPORTED);
      } else if (userIdRefused) {
        message.error(KEY_USER_CHOICE_UNSUPPORTED);
      } else {
        message.error(error instanceof Error ? error.message : 'Failed to create API key');
      }
    } finally {
      setCreating(false);
    }
  };

  const revokeKey = async (key: ApiKeyMetadata) => {
    try {
      await adminApi.delete<void>(`/v1/orgs/${key.org_id}/api-keys/${key.id}`);
      // Read again: with revoked keys shown, the row stays, marked revoked.
      setKeys((current) => current.filter((candidate) => candidate.id !== key.id));
      setReloadKeys((value) => value + 1);
      message.success('API key revoked');
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to revoke API key');
    }
  };

  const copySecret = async () => {
    if (!created) return;
    try {
      await navigator.clipboard.writeText(created.key.secret);
      message.success('API key copied');
    } catch {
      message.error('Could not copy the API key');
    }
  };

  const columns: ColumnsType<ApiKeyMetadata> = [
    ...apiKeyColumns(),
    {
      title: '',
      key: 'actions',
      align: 'right',
      render: (_, key) => (key.revoked_at ? null : (
        <Popconfirm
          title="Revoke this API key?"
          description="Clients using it will immediately lose access."
          okText="Revoke"
          okButtonProps={{ danger: true }}
          onConfirm={() => revokeKey(key)}
        >
          <Button danger type="text" icon={<DeleteOutlined />} aria-label={`Revoke ${key.name}`} />
        </Popconfirm>
      )),
    },
  ];

  return (
    <div>
      <Space align="start" style={{ width: '100%', justifyContent: 'space-between', marginBottom: 24 }}>
        <div>
          <Title level={3} style={{ marginBottom: 4 }}>API Keys</Title>
          <Text type="secondary">Organization-scoped credentials for the SPACE and E2B-compatible APIs.</Text>
        </div>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={openCreate}
          disabled={!orgId}
        >
          Create API key
        </Button>
      </Space>

      <Card>
        <Space orientation="vertical" size={16} style={{ width: '100%' }}>
          <Space wrap size={16}>
            <Select
              value={orgId}
              onChange={setOrgId}
              style={{ width: 360, maxWidth: '100%' }}
              placeholder="Select an organization"
              options={organizations.map((organization) => ({
                label: organization.id === signedInOrg?.id ? `${organization.name} (mine)` : organization.name,
                value: organization.id,
              }))}
            />
            <Space size={8}>
              <Switch size="small" checked={includeRevoked} onChange={setIncludeRevoked} aria-label="Show revoked keys" />
              <Text>Show revoked keys</Text>
            </Space>
          </Space>
          <Table
            rowKey="id"
            columns={columns}
            dataSource={keys}
            loading={loading}
            pagination={false}
          />
        </Space>
      </Card>

      <Modal
        title={`Create an API key for ${orgName}`}
        open={createOpen}
        okText="Create"
        okButtonProps={{ disabled: !createAllowed }}
        confirmLoading={creating}
        onOk={() => form.submit()}
        onCancel={() => setCreateOpen(false)}
        destroyOnHidden
      >
        <Form form={form} layout="vertical" onFinish={createKey} preserve={false}>
          <Form.Item
            name="name"
            label="Name"
            rules={[{ required: true }, { max: 128 }]}
          >
            <Input placeholder="CI production" autoComplete="off" />
          </Form.Item>
          <Form.Item
            name="allowed_surfaces"
            label="Allowed APIs"
            rules={[{ required: true, message: 'Select at least one API' }]}
          >
            <Checkbox.Group
              options={[
                { label: 'E2B-compatible API', value: 'ecw' },
                { label: 'SPACE API', value: 'native' },
              ]}
            />
          </Form.Item>
          <Form.Item
            name="user_id"
            label="Acts as"
            // The API picks no user for another organization's key.
            rules={ownOrg ? [] : [{ required: true, message: 'Choose the user the key acts as' }]}
            extra={(
              <Space size={4} wrap>
                <span>{actsAsExtra}</span>
                {orgUsersError ? (
                  <Button type="link" size="small" style={{ padding: 0 }} onClick={() => setReloadUsers((value) => value + 1)}>
                    Retry
                  </Button>
                ) : null}
                {crossOrgUnsupported ? (
                  <Button type="link" size="small" style={{ padding: 0 }} onClick={() => setCrossOrgRefusedFor(null)}>
                    Try again
                  </Button>
                ) : null}
              </Space>
            )}
          >
            <Select
              allowClear
              showSearch
              optionFilterProp="label"
              loading={usersLoading}
              placeholder={ownOrg ? 'You (default)' : 'Choose a user'}
              options={(orgUsers ?? []).map((user) => ({
                label: user.display_name ? `${user.email} (${user.display_name})` : user.email,
                value: user.id,
              }))}
            />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={created ? `API key created for ${created.orgName}` : 'API key created'}
        open={created !== null}
        okText="Done"
        cancelButtonProps={{ style: { display: 'none' } }}
        onOk={() => setCreated(null)}
        onCancel={() => setCreated(null)}
        closable={false}
        mask={{ closable: false }}
      >
        <Alert
          type="warning"
          showIcon
          title="Copy this key now"
          description="SPACE stores only its hash. The key cannot be displayed again after this dialog closes."
          style={{ marginBottom: 16 }}
        />
        <Paragraph>
          <Text strong>{created?.key.api_key.name}</Text> acts as {created?.actsAs} in {created?.orgName}.
        </Paragraph>
        <Paragraph code style={{ wordBreak: 'break-all' }}>
          {created?.key.secret}
        </Paragraph>
        <Button icon={<CopyOutlined />} onClick={copySecret}>Copy API key</Button>
      </Modal>
    </div>
  );
}
