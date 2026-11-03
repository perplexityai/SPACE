'use client';

import { Space, Tag, Tooltip, Typography } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { LAST_USED_CAVEAT } from '@/lib/api-keys';
import type { ApiKeyMetadata, ApiSurface } from '@/lib/types';

const { Text } = Typography;

function formatDate(value: string): string {
  return new Date(value).toLocaleString();
}

// The columns the API keys page and an organization's key card share. A
// release without usage or revocation records sends neither field, and those
// cells stay empty.
export function apiKeyColumns(): ColumnsType<ApiKeyMetadata> {
  return [
    {
      title: 'Name',
      dataIndex: 'name',
      render: (name: string, key) => (
        <Space size={6}>
          <span>{name}</span>
          {key.revoked_at ? (
            <Tooltip title={`Revoked ${formatDate(key.revoked_at)}`}>
              <Tag color="red">revoked</Tag>
            </Tooltip>
          ) : null}
        </Space>
      ),
    },
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
      title: 'Acts as',
      dataIndex: 'created_by_email',
      render: (email: string | null) => email || <Text type="secondary">Unassigned</Text>,
    },
    { title: 'Created', dataIndex: 'created_at', render: formatDate },
    {
      title: (
        <Tooltip title={LAST_USED_CAVEAT}>
          <Space size={4}>Last used <InfoCircleOutlined /></Space>
        </Tooltip>
      ),
      dataIndex: 'last_used_at',
      render: (value: string | null | undefined) => {
        if (value === undefined) return null;
        return value ? formatDate(value) : <Text type="secondary">Never</Text>;
      },
    },
    {
      title: 'Revoked',
      dataIndex: 'revoked_at',
      render: (value: string | null | undefined) => (value ? formatDate(value) : null),
    },
  ];
}
