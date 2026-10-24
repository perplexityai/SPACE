'use client';

import { useEffect, useId, useRef, useState } from 'react';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { AdminUser, Organization, UserRole } from '@/lib/types';
import { Table, Button, Form, Input, Select, Typography, Popconfirm, App, Modal, Space, Tag } from 'antd';
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
  const [usersRevision, setUsersRevision] = useState(0);
  const [createOrg, setCreateOrg] = useState<Organization | null>(null);
  const [creating, setCreating] = useState(false);
  const createPending = useRef(false);
  const [updatingRoleId, setUpdatingRoleId] = useState<string | null>(null);
  const [form] = Form.useForm();
  const createFormId = useId();
  const createRoleOptions = assignableRoleOptions(currentUser, cluster);
  const visibleOrgIds = orgs.map((org) => org.id);
  const selectedOrg = orgs.find((org) => org.id === selectedOrgId);

  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then((orgs) => {
      setOrgs(orgs);
      if (orgs.length > 0 && !selectedOrgId) {
        setSelectedOrgId(orgs[0].id);
      }
    }).catch((err: any) => message.error(err.message));
  }, []);

  useEffect(() => {
    let active = true;
    if (selectedOrgId) {
      setLoading(true);
      adminApi
        .get<AdminUser[]>(`/v1/orgs/${selectedOrgId}/users`)
        .then((updated) => {
          if (active) setUsers(updated);
        })
        .catch((err: any) => {
          if (!active) return;
          setUsers([]);
          message.error(`Failed to load users: ${err.message}`);
        })
        .finally(() => {
          if (active) setLoading(false);
        });
    }
    return () => { active = false; };
  }, [selectedOrgId, usersRevision]);

  const closeCreate = () => {
    if (createPending.current) return;
    setCreateOrg(null);
    form.resetFields();
  };

  const handleCreate = async (values: { email: string; password: string; display_name: string; role: UserRole }) => {
    if (!createOrg || createPending.current) return;
    createPending.current = true;
    setCreating(true);
    try {
      await adminApi.post(`/v1/orgs/${createOrg.id}/users`, values);
      setCreateOrg(null);
      form.resetFields();
      message.success('User created');
      setUsersRevision((revision) => revision + 1);
    } catch (err: any) {
      message.error(err.message);
    } finally {
      createPending.current = false;
      setCreating(false);
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
      <Space align="center" wrap style={{ width: '100%', justifyContent: 'space-between', marginBottom: 24 }}>
        <Title level={3} style={{ margin: 0 }}>Users</Title>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          disabled={!selectedOrg || creating}
          onClick={() => {
            if (!selectedOrg || createPending.current) return;
            form.resetFields();
            setCreateOrg(selectedOrg);
          }}
        >
          Create user
        </Button>
      </Space>

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

      <Modal
        title="Create user"
        open={!!createOrg}
        onCancel={closeCreate}
        okText="Create user"
        okButtonProps={{ htmlType: 'submit', form: createFormId }}
        confirmLoading={creating}
        cancelButtonProps={{ disabled: creating }}
        closable={!creating}
        mask={{ closable: !creating }}
        keyboard={!creating}
        destroyOnHidden
      >
        <Typography.Paragraph>
          Organization: <Typography.Text strong>{createOrg?.name} ({createOrg?.slug})</Typography.Text>
        </Typography.Paragraph>
        <Form
          id={createFormId}
          form={form}
          layout="vertical"
          onFinish={handleCreate}
          initialValues={{ role: 'member' }}
          disabled={creating}
          preserve={false}
          autoComplete="off"
        >
          <Form.Item label="Email" name="email" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item label="Password" name="password" rules={[{ required: true }]}>
            <Input.Password autoComplete="new-password" />
          </Form.Item>
          <Form.Item label="Display name" name="display_name" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item label="Role" name="role">
            <Select>
              {createRoleOptions.map((option) => (
                <Select.Option key={option.value} value={option.value}>{option.label}</Select.Option>
              ))}
            </Select>
          </Form.Item>
        </Form>
      </Modal>

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
