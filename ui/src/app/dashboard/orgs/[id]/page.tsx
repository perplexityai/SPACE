'use client';

import { useEffect, useState } from 'react';
import { useParams } from 'next/navigation';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { useCuriosityTheme } from '@/lib/theme';
import { Organization, OrgMasterKey, Sandbox, User } from '@/lib/types';
import { Card, Table, Typography, Form, Input, Button, App, Descriptions, Space, Switch, Tag } from 'antd';
import { LockOutlined, SaveOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

const { Title } = Typography;

export default function OrgDetailPage() {
  const { message } = App.useApp();
  const { primaryColor: defaultPrimaryColor } = useCuriosityTheme();
  const { id } = useParams<{ id: string }>();
  const { org: authOrg, refreshOrg } = useAuth();
  const [org, setOrg] = useState<Organization | null>(null);
  const [users, setUsers] = useState<User[]>([]);
  const [sandboxes, setSandboxes] = useState<Sandbox[]>([]);
  const [masterKey, setMasterKey] = useState<OrgMasterKey | null>(null);
  const [byokSaving, setByokSaving] = useState(false);
  const [form] = Form.useForm();

  useEffect(() => {
    adminApi.get<Organization>(`/v1/orgs/${id}`).then((o) => {
      setOrg(o);
      form.setFieldsValue({
        theme_primary: o.theme_primary || '',
        theme_secondary: o.theme_secondary || '',
        logo_url: o.logo_url || '',
      });
    });
    adminApi.get<User[]>(`/v1/orgs/${id}/users`).then(setUsers).catch(() => {});
    adminApi.get<Sandbox[]>(`/v1/sandboxes?org_id=${id}&limit=5000`).then(setSandboxes).catch(() => {});
    adminApi.get<OrgMasterKey | null>(`/v1/orgs/${id}/master-key`).then(setMasterKey).catch(() => {});
  }, [id]);

  const sandboxCountByUser = (userId: string) =>
    sandboxes.filter((sandbox) => sandbox.user_id === userId).length;

  const handleSave = async (values: any) => {
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
    { title: 'Name', dataIndex: 'display_name' },
    { title: 'Email', dataIndex: 'email' },
    { title: 'Role', dataIndex: 'role' },
    {
      title: 'Sandboxes',
      align: 'right',
      render: (_, record) => sandboxCountByUser(record.id),
    },
  ];

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
                <strong>Total ({users.length} users, {sandboxes.length} sandboxes)</strong>
              </Table.Summary.Cell>
            </Table.Summary.Row>
          )}
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
            <Button type="primary" htmlType="submit" icon={<SaveOutlined />}>
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
              onChange={handleByokToggle}
            />
            <Typography.Text>
              Encrypt this organization&apos;s sandboxes with its own key
            </Typography.Text>
          </Space>
          <Typography.Text type="secondary">
            When enabled, newly created sandboxes run on encrypted volumes keyed
            through the configured external provider and bypass warm pools.
            Existing sandboxes keep the encryption state they were created with.
          </Typography.Text>

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
