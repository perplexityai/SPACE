'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { Alert, App, Button, Input, Select, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { adminApi } from '@/lib/api';
import { tablePagination } from '@/lib/table-pagination';
import type { Organization, PublicOperationRecord, User } from '@/lib/types';

const { Title, Text } = Typography;

// `GET /admin/api/v1/operations` (newest first, at most 250 rows).
interface AdminOperation extends PublicOperationRecord {
  org_id: string | null;
  actor_user_id: string | null;
  api_product: string | null;
  target_type: string | null;
  target_id: string | null;
  attempts: number;
}

const STATES = ['queued', 'running', 'succeeded', 'failed', 'canceled', 'expired'];
const STATE_COLORS: Record<string, string> = {
  succeeded: 'green', failed: 'red', canceled: 'default', expired: 'orange', running: 'processing', queued: 'processing',
};

function targetLink(op: AdminOperation) {
  if (!op.target_id) return '—';
  if (op.target_type === 'sandbox') return <Link href={`/dashboard/sandboxes/${op.target_id}`}><code>{op.target_id}</code></Link>;
  return <code>{op.target_type ? `${op.target_type}:` : ''}{op.target_id}</code>;
}

function failure(op: AdminOperation): string | null {
  if (op.terminal_error === null || op.terminal_error === undefined) return null;
  const text = typeof op.terminal_error === 'string' ? op.terminal_error : JSON.stringify(op.terminal_error);
  return op.terminal_error_code ? `${op.terminal_error_code}: ${text}` : text;
}

export default function OperationsPage() {
  const { message } = App.useApp();
  const [operations, setOperations] = useState<AdminOperation[]>([]);
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [state, setState] = useState<string | undefined>();
  const [orgId, setOrgId] = useState<string | undefined>();
  const [kind, setKind] = useState('');
  const [targetId, setTargetId] = useState('');
  const [limit, setLimit] = useState(100);

  const load = () => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (state) params.set('state', state);
    if (orgId) params.set('org_id', orgId);
    if (kind.trim()) params.set('kind', kind.trim());
    if (targetId.trim()) params.set('target_id', targetId.trim());
    setLoading(true);
    adminApi
      .get<AdminOperation[]>(`/v1/operations?${params}`)
      .then((rows) => {
        setOperations(rows);
        setError(null);
      })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : 'Failed to load operations'))
      .finally(() => setLoading(false));
  };

  useEffect(load, [state, orgId, limit]);
  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then(setOrgs).catch((err: unknown) => message.error(err instanceof Error ? err.message : 'Failed to load organizations'));
  }, []);

  const orgNames = useMemo(() => new Map(orgs.map((org) => [org.id, org.name])), [orgs]);

  // Resolve each distinct actor to an email once.
  const [actorEmails, setActorEmails] = useState<Record<string, string>>({});
  useEffect(() => {
    const missing = [...new Set(operations.map((op) => op.actor_user_id).filter((v): v is string => !!v))]
      .filter((userId) => !(userId in actorEmails));
    if (missing.length === 0) return;
    void Promise.all(
      missing.map((userId) =>
        adminApi.get<User>(`/v1/users/${userId}`).then((u) => [userId, u.email] as const).catch(() => [userId, userId] as const),
      ),
    ).then((pairs) => setActorEmails((current) => ({ ...current, ...Object.fromEntries(pairs) })));
  }, [operations]);

  const columns: ColumnsType<AdminOperation> = [
    {
      title: 'Requested', dataIndex: 'created_at', width: 180,
      render: (v: string) => <Tooltip title={v}>{new Date(v).toLocaleString()}</Tooltip>,
    },
    { title: 'Operation', dataIndex: 'kind', width: 180, render: (v: string) => <code>{v}</code> },
    { title: 'State', dataIndex: 'state', width: 100, render: (v: string) => <Tag color={STATE_COLORS[v] ?? 'default'}>{v}</Tag> },
    { title: 'Target', render: (_, op) => targetLink(op) },
    {
      title: 'Org', dataIndex: 'org_id', width: 150,
      render: (v: string | null) => (v ? <Link href={`/dashboard/orgs/${v}`}>{orgNames.get(v) ?? v}</Link> : '—'),
    },
    { title: 'Via', dataIndex: 'api_product', width: 80, render: (v: string | null) => v || '—' },
    {
      title: <Tooltip title="The user the operation was recorded for. Creates through the admin API record the sandbox owner.">Recorded for</Tooltip>,
      dataIndex: 'actor_user_id', width: 220,
      render: (v: string | null) => (v ? <Link href={`/dashboard/users/${v}`}>{actorEmails[v] ?? v}</Link> : '—'),
    },
    { title: 'Attempts', dataIndex: 'attempts', width: 85 },
    {
      title: 'Result',
      render: (_, op) => {
        const failed = failure(op);
        return failed ? <Text type="danger">{failed}</Text> : op.progress_message || '—';
      },
    },
  ];

  return (
    <div>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <div>
          <Title level={3} style={{ marginBottom: 4 }}>Operations</Title>
          <Text type="secondary">Lifecycle operations across every organization, newest first.</Text>
        </div>
        <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>Refresh</Button>
      </Space>
      <Space wrap style={{ margin: '16px 0' }}>
        <Select allowClear placeholder="State" value={state} onChange={setState} style={{ width: 150 }} options={STATES.map((s) => ({ value: s, label: s }))} />
        <Select
          allowClear
          placeholder="Organization"
          value={orgId}
          onChange={setOrgId}
          style={{ width: 200 }}
          options={orgs.map((org) => ({ value: org.id, label: org.name }))}
        />
        <Input allowClear placeholder="Kind, e.g. suspend_sandbox" value={kind} onChange={(e) => setKind(e.target.value)} onPressEnter={load} style={{ width: 220 }} />
        <Input allowClear placeholder="Target ID, e.g. sbx_…" value={targetId} onChange={(e) => setTargetId(e.target.value)} onPressEnter={load} style={{ width: 320 }} />
        <Select value={limit} onChange={setLimit} style={{ width: 140 }} options={[50, 100, 250].map((n) => ({ value: n, label: `Newest ${n}` }))} />
      </Space>
      {error ? <Alert type="error" showIcon title={error} style={{ marginBottom: 12 }} /> : null}
      <Table
        columns={columns}
        dataSource={operations}
        rowKey="id"
        size="small"
        loading={loading}
        scroll={{ x: 1300 }}
        pagination={tablePagination('operations')}
        locale={{ emptyText: 'No operations match these filters' }}
      />
    </div>
  );
}
