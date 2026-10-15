'use client';

import { useEffect, useState } from 'react';
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
  Table,
  Tag,
  Typography,
  App,
} from 'antd';
import { CopyOutlined, DeleteOutlined, PlusOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import type {
  ApiKeyMetadata,
  ApiSurface,
  CreatedApiKey,
  Organization,
} from '@/lib/types';

const { Paragraph, Text, Title } = Typography;

type CreateApiKeyValues = {
  name: string;
  allowed_surfaces: ApiSurface[];
};

function formatDate(value: string | null): string {
  return value ? new Date(value).toLocaleString() : 'Never';
}

export default function ApiKeysPage() {
  const { message } = App.useApp();
  const { org: signedInOrg } = useAuth();
  const [organizations, setOrganizations] = useState<Organization[]>([]);
  const [orgId, setOrgId] = useState<string>();
  const [keys, setKeys] = useState<ApiKeyMetadata[]>([]);
  const [loading, setLoading] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [created, setCreated] = useState<CreatedApiKey | null>(null);
  const [form] = Form.useForm<CreateApiKeyValues>();

  useEffect(() => {
    let active = true;
    adminApi
      .get<Organization[]>('/v1/orgs')
      .then((loaded) => {
        if (!active) return;
        setOrganizations(loaded);
        const preferred = loaded.find((candidate) => candidate.id === signedInOrg?.id) || loaded[0];
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
  }, [signedInOrg?.id]);

  useEffect(() => {
    if (!orgId) {
      setKeys([]);
      setLoading(false);
      return;
    }
    let active = true;
    setKeys([]);
    setLoading(true);
    adminApi
      .get<ApiKeyMetadata[]>(`/v1/orgs/${orgId}/api-keys`)
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
  }, [orgId]);

  const openCreate = () => {
    form.resetFields();
    form.setFieldsValue({ allowed_surfaces: ['ecw'] });
    setCreateOpen(true);
  };

  const createKey = async (values: CreateApiKeyValues) => {
    if (!orgId) return;
    setCreating(true);
    try {
      const result = await adminApi.post<CreatedApiKey>(`/v1/orgs/${orgId}/api-keys`, values);
      setKeys((current) => [result.api_key, ...current]);
      setCreateOpen(false);
      setCreated(result);
      message.success('API key created');
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to create API key');
    } finally {
      setCreating(false);
    }
  };

  const revokeKey = async (key: ApiKeyMetadata) => {
    try {
      await adminApi.delete<void>(`/v1/orgs/${key.org_id}/api-keys/${key.id}`);
      setKeys((current) => current.filter((candidate) => candidate.id !== key.id));
      message.success('API key revoked');
    } catch (error) {
      message.error(error instanceof Error ? error.message : 'Failed to revoke API key');
    }
  };

  const copySecret = async () => {
    if (!created) return;
    try {
      await navigator.clipboard.writeText(created.secret);
      message.success('API key copied');
    } catch {
      message.error('Could not copy the API key');
    }
  };

  const columns: ColumnsType<ApiKeyMetadata> = [
    { title: 'Name', dataIndex: 'name' },
    {
      title: 'Key ID',
      dataIndex: 'id',
      render: (id: string) => <Text code copyable>{id}</Text>,
    },
    {
      title: 'Allowed APIs',
      dataIndex: 'allowed_surfaces',
      render: (surfaces: ApiSurface[]) => (
        <Space size={4} wrap>
          {surfaces.map((surface) => (
            <Tag key={surface} color={surface === 'native' ? 'blue' : 'purple'}>
              {surface === 'native' ? 'SPACE' : 'E2B-compatible'}
            </Tag>
          ))}
        </Space>
      ),
    },
    {
      title: 'Creator',
      dataIndex: 'created_by_email',
      render: (email: string | null) => email || <Text type="secondary">Unassigned</Text>,
    },
    { title: 'Created', dataIndex: 'created_at', render: formatDate },
    {
      title: '',
      key: 'actions',
      align: 'right',
      render: (_, key) => (
        <Popconfirm
          title="Revoke this API key?"
          description="Clients using it will immediately lose access."
          okText="Revoke"
          okButtonProps={{ danger: true }}
          onConfirm={() => revokeKey(key)}
        >
          <Button danger type="text" icon={<DeleteOutlined />} aria-label={`Revoke ${key.name}`} />
        </Popconfirm>
      ),
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
          disabled={!orgId || orgId !== signedInOrg?.id}
        >
          Create API key
        </Button>
      </Space>

      <Card>
        <Space orientation="vertical" size={16} style={{ width: '100%' }}>
          <Select
            value={orgId}
            onChange={setOrgId}
            style={{ width: 360, maxWidth: '100%' }}
            placeholder="Select an organization"
            options={organizations.map((organization) => ({
              label: organization.name,
              value: organization.id,
            }))}
          />
          {orgId && signedInOrg && orgId !== signedInOrg.id && (
            <Alert
              type="info"
              showIcon
              message="API keys can only be created for your organization. You can still view and revoke this organization's keys."
            />
          )}
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
        title="Create API key"
        open={createOpen}
        okText="Create"
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
        </Form>
      </Modal>

      <Modal
        title="API key created"
        open={created !== null}
        okText="Done"
        cancelButtonProps={{ style: { display: 'none' } }}
        onOk={() => setCreated(null)}
        onCancel={() => setCreated(null)}
        closable={false}
        maskClosable={false}
      >
        <Alert
          type="warning"
          showIcon
          message="Copy this key now"
          description="SPACE stores only its hash. The key cannot be displayed again after this dialog closes."
          style={{ marginBottom: 16 }}
        />
        <Paragraph code style={{ wordBreak: 'break-all' }}>
          {created?.secret}
        </Paragraph>
        <Button icon={<CopyOutlined />} onClick={copySecret}>Copy API key</Button>
      </Modal>
    </div>
  );
}
