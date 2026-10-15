'use client';

import { useEffect, useState } from 'react';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { Cluster, Organization } from '@/lib/types';
import { isManagingOrgAdmin } from '@/lib/userRoles';
import Link from 'next/link';
import { Table, Button, Form, Input, Select, Typography, Space, Popconfirm, App, Card, Tag } from 'antd';
import { PlusOutlined, DeleteOutlined, ClusterOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

const { Title } = Typography;

export default function OrgsPage() {
  const { message } = App.useApp();
  const { user, cluster, refreshCluster } = useAuth();
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(false);
  const [updatingManagingOrgId, setUpdatingManagingOrgId] = useState<string | null>(null);
  const [form] = Form.useForm();

  const load = () => {
    setLoading(true);
    adminApi
      .get<Organization[]>('/v1/orgs')
      .then(setOrgs)
      .catch((err: any) => message.error(err.message))
      .finally(() => setLoading(false));
  };

  useEffect(load, []);

  const handleCreate = async (values: { name: string; slug: string; parent_id?: string }) => {
    try {
      await adminApi.post('/v1/orgs', { ...values, parent_id: values.parent_id || null });
      form.resetFields();
      load();
      message.success('Organization created');
    } catch (err: any) {
      message.error(err.message);
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await adminApi.delete(`/v1/orgs/${id}`);
      load();
      message.success('Organization deleted');
    } catch (err: any) {
      message.error(err.message);
    }
  };

  const handleSetManagingOrg = async (id: string) => {
    setUpdatingManagingOrgId(id);
    try {
      const updated = await adminApi.patch<Cluster>('/v1/cluster', { managing_org_id: id });
      refreshCluster(updated);
      message.success('Cluster manager updated');
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setUpdatingManagingOrgId(null);
    }
  };

  const managingOrgAdmin = isManagingOrgAdmin(user, cluster);
  const columns: ColumnsType<Organization> = [
    {
      title: 'Name',
      dataIndex: 'name',
      render: (name: string, record) => (
        <Space size={8}>
          <Link href={`/dashboard/orgs/${record.id}`}>{name}</Link>
          {record.id === cluster?.managing_org_id ? <Tag color="gold">Cluster manager</Tag> : null}
        </Space>
      ),
    },
    { title: 'Slug', dataIndex: 'slug' },
    {
      title: 'Parent',
      dataIndex: 'parent_id',
      render: (parentId: string | null) =>
        parentId ? orgs.find((o) => o.id === parentId)?.name || parentId.slice(0, 8) : '—',
    },
    {
      title: 'Actions',
      width: 240,
      render: (_, record) => {
        const isManagingOrg = record.id === cluster?.managing_org_id;
        const canSetManagingOrg = managingOrgAdmin && !isManagingOrg;
        const canDelete = managingOrgAdmin && !isManagingOrg;
        return (
          <Space size={4}>
            <Popconfirm
              title="Make this organization the cluster manager?"
              onConfirm={() => handleSetManagingOrg(record.id)}
              disabled={!canSetManagingOrg}
            >
              <Button
                size="small"
                icon={<ClusterOutlined />}
                loading={updatingManagingOrgId === record.id}
                disabled={!canSetManagingOrg || updatingManagingOrgId !== null}
              >
                Make manager
              </Button>
            </Popconfirm>
            <Popconfirm title="Delete this organization?" onConfirm={() => handleDelete(record.id)} disabled={!canDelete}>
              <Button type="text" danger icon={<DeleteOutlined />} size="small" disabled={!canDelete} />
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  return (
    <div>
      <Title level={3}>Organizations</Title>

      <Card size="small" style={{ marginBottom: 24 }}>
        <Form form={form} layout="inline" onFinish={handleCreate}>
          <Form.Item name="name" rules={[{ required: true, message: 'Name required' }]}>
            <Input placeholder="Name" />
          </Form.Item>
          <Form.Item name="slug" rules={[{ required: true, message: 'Slug required' }]}>
            <Input placeholder="Slug" />
          </Form.Item>
          <Form.Item
            name="parent_id"
            rules={managingOrgAdmin ? [] : [{ required: true, message: 'Parent required' }]}
          >
            <Select
              placeholder={managingOrgAdmin ? 'Parent (optional)' : 'Parent'}
              allowClear={managingOrgAdmin}
              style={{ width: 200 }}
            >
              {orgs.map((o) => (
                <Select.Option key={o.id} value={o.id}>
                  {o.name} ({o.slug})
                </Select.Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" icon={<PlusOutlined />}>
              Create
            </Button>
          </Form.Item>
        </Form>
      </Card>

      <Table
        columns={columns}
        dataSource={orgs}
        rowKey="id"
        loading={loading}
        size="middle"
        pagination={false}
      />
    </div>
  );
}
