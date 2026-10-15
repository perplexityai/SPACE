'use client';

import { useEffect, useState } from 'react';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { AdminUser, Organization, UserRole } from '@/lib/types';
import { Table, Button, Form, Input, Select, Typography, Popconfirm, App, Card, Space, Tag } from 'antd';
import { assignableRoleOptions, canDeleteUser, canManageUserRole, roleColors, roleLabels } from '@/lib/userRoles';
import { PlusOutlined, DeleteOutlined } from '@ant-design/icons';
import Link from 'next/link';
import type { ColumnsType } from 'antd/es/table';

const { Title } = Typography;

export default function UsersPage() {
  const { message } = App.useApp();
  const { user: currentUser, cluster } = useAuth();
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [selectedOrgId, setSelectedOrgId] = useState('');
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [loading, setLoading] = useState(false);
  const [updatingRoleId, setUpdatingRoleId] = useState<string | null>(null);
  const [form] = Form.useForm();
  const createRoleOptions = assignableRoleOptions(currentUser, cluster);
  const visibleOrgIds = orgs.map((org) => org.id);

  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then((orgs) => {
      setOrgs(orgs);
      if (orgs.length > 0 && !selectedOrgId) {
        setSelectedOrgId(orgs[0].id);
      }
    }).catch((err: any) => message.error(err.message));
  }, []);

  useEffect(() => {
    if (selectedOrgId) {
      setLoading(true);
      adminApi
        .get<AdminUser[]>(`/v1/orgs/${selectedOrgId}/users`)
        .then(setUsers)
        .catch((err: any) => {
          setUsers([]);
          message.error(err.message);
        })
        .finally(() => setLoading(false));
    }
  }, [selectedOrgId]);

  const handleCreate = async (values: { email: string; password: string; display_name: string; role: UserRole }) => {
    try {
      await adminApi.post(`/v1/orgs/${selectedOrgId}/users`, values);
      form.resetFields();
      const updated = await adminApi.get<AdminUser[]>(`/v1/orgs/${selectedOrgId}/users`);
      setUsers(updated);
      message.success('User created');
    } catch (err: any) {
      message.error(err.message);
    }
  };

  const handleRoleChange = async (target: AdminUser, role: UserRole) => {
    if (target.role === role) return;

    setUpdatingRoleId(target.id);
    try {
      const updated = await adminApi.put<AdminUser>(`/v1/users/${target.id}/role`, { role });
      setUsers((current) => current.map((user) => (user.id === updated.id ? updated : user)));
      message.success('Role updated');
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setUpdatingRoleId(null);
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await adminApi.delete(`/v1/users/${id}`);
      setUsers(users.filter((u) => u.id !== id));
      message.success('User deleted');
    } catch (err: any) {
      message.error(err.message);
    }
  };

  const columns: ColumnsType<AdminUser> = [
    {
      title: 'Email',
      dataIndex: 'email',
      render: (email: string, record: AdminUser) => (
        <Link href={`/dashboard/users/${record.id}`}>{email}</Link>
      ),
    },
    {
      title: 'Name',
      dataIndex: 'display_name',
      render: (name: string, record: AdminUser) => (
        <Link href={`/dashboard/users/${record.id}`}>{name}</Link>
      ),
    },
    {
      title: 'Role',
      dataIndex: 'role',
      width: 180,
      render: (role: UserRole, record: AdminUser) => {
        const options = assignableRoleOptions(currentUser, cluster);
        if (!canManageUserRole(currentUser, record, cluster, visibleOrgIds) || options.length === 0) {
          return <Tag color={roleColors[role]}>{roleLabels[role]}</Tag>;
        }

        return (
          <Select<UserRole>
            size="small"
            value={role}
            options={options}
            loading={updatingRoleId === record.id}
            disabled={updatingRoleId !== null}
            style={{ width: 180 }}
            onChange={(nextRole) => handleRoleChange(record, nextRole)}
          />
        );
      },
    },
    {
      title: 'Password login',
      dataIndex: 'password_login_configured',
      width: 150,
      render: (configured: boolean) => (
        <Tag color={configured ? 'green' : 'default'}>
          {configured ? 'Configured' : 'Not configured'}
        </Tag>
      ),
    },
    {
      title: 'Actions',
      width: 100,
      render: (_, record) => {
        const canDelete = canDeleteUser(currentUser, record, cluster, visibleOrgIds);
        return (
          <Popconfirm
            title="Delete this user?"
            onConfirm={() => handleDelete(record.id)}
            disabled={!canDelete}
          >
            <Button
              type="text"
              danger
              icon={<DeleteOutlined />}
              size="small"
              disabled={!canDelete}
            />
          </Popconfirm>
        );
      },
    },
  ];

  return (
    <div>
      <Title level={3}>Users</Title>

      <Space style={{ marginBottom: 16 }}>
        <Typography.Text>Organization:</Typography.Text>
        <Select
          value={selectedOrgId}
          onChange={setSelectedOrgId}
          style={{ width: 300 }}
        >
          {orgs.map((o) => (
            <Select.Option key={o.id} value={o.id}>
              {o.name} ({o.slug})
            </Select.Option>
          ))}
        </Select>
      </Space>

      <Card size="small" style={{ marginBottom: 24 }}>
        <Form form={form} layout="inline" onFinish={handleCreate} initialValues={{ role: 'member' }}>
          <Form.Item name="email" rules={[{ required: true }]}>
            <Input placeholder="Email" />
          </Form.Item>
          <Form.Item name="password" rules={[{ required: true }]}>
            <Input.Password placeholder="Password" />
          </Form.Item>
          <Form.Item name="display_name" rules={[{ required: true }]}>
            <Input placeholder="Display Name" />
          </Form.Item>
          <Form.Item name="role">
            <Select style={{ width: 180 }}>
              {createRoleOptions.map((option) => (
                <Select.Option key={option.value} value={option.value}>{option.label}</Select.Option>
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
        dataSource={users}
        rowKey="id"
        loading={loading}
        size="middle"
        pagination={false}
      />

    </div>
  );
}
