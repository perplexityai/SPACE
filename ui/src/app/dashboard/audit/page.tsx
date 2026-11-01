'use client';

import { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { Alert, Button, Input, Select, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { adminApi } from '@/lib/api';
import { tablePagination } from '@/lib/table-pagination';

const { Title, Text } = Typography;

// `GET /admin/api/v1/audit-events`: one row per admin API request other than
// GET, HEAD and OPTIONS that passed the operator check, whatever its status,
// newest first, at most 250 (`admin_request_should_persist_audit_event`).
interface AuditEvent {
  id: string;
  actor_user_id: string | null;
  actor_org_id: string | null;
  actor_email: string | null;
  method: string;
  path: string;
  status_code: number;
  success: boolean;
  duration_ms: number;
  created_at: string;
}

export default function AuditPage() {
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [limit, setLimit] = useState(100);
  const [query, setQuery] = useState('');
  // Only the newest request may write its answer.
  const request = useRef(0);

  const load = () => {
    const current = ++request.current;
    setLoading(true);
    adminApi
      .get<{ events: AuditEvent[] }>(`/v1/audit-events?limit=${limit}`)
      .then((body) => {
        if (current !== request.current) return;
        setEvents(body.events);
        setError(null);
      })
      .catch((err: unknown) => {
        if (current !== request.current) return;
        setError(err instanceof Error ? err.message : 'Failed to load audit events');
      })
      .finally(() => {
        if (current === request.current) setLoading(false);
      });
  };

  useEffect(load, [limit]);

  // The API has no filters, so search the loaded page.
  const needle = query.trim().toLowerCase();
  const rows = needle
    ? events.filter((e) => [e.actor_email, e.method, e.path, String(e.status_code)].some((v) => v?.toLowerCase().includes(needle)))
    : events;

  const columns: ColumnsType<AuditEvent> = [
    { title: 'When', dataIndex: 'created_at', width: 180, render: (v: string) => <Tooltip title={v}>{new Date(v).toLocaleString()}</Tooltip> },
    {
      title: 'Actor', dataIndex: 'actor_email', width: 220,
      render: (v: string | null, e) => (e.actor_user_id ? <Link href={`/dashboard/users/${e.actor_user_id}`}>{v || e.actor_user_id}</Link> : v || '—'),
    },
    { title: 'Method', dataIndex: 'method', width: 90, render: (v: string) => <Tag>{v}</Tag> },
    { title: 'Path', dataIndex: 'path', render: (v: string) => <code>{v}</code> },
    {
      title: 'Result', dataIndex: 'status_code', width: 100,
      render: (v: number, e) => <Tag color={e.success ? 'green' : 'red'}>{v}</Tag>,
    },
    { title: 'Duration', dataIndex: 'duration_ms', width: 100, render: (v: number) => `${v} ms` },
  ];

  return (
    <div>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <div>
          <Title level={3} style={{ marginBottom: 4 }}>Audit log</Title>
          <Text type="secondary">
            Every admin API request other than reads (GET, HEAD, OPTIONS) made by a signed-in operator, including refused
            ones, newest first. Console actions sent through the public API appear under Operations.
          </Text>
        </div>
        <Button icon={<ReloadOutlined />} onClick={load} loading={loading}>Refresh</Button>
      </Space>
      <Space wrap style={{ margin: '16px 0' }}>
        <Input allowClear placeholder="Search actor, method, path or status" value={query} onChange={(e) => setQuery(e.target.value)} style={{ width: 360 }} />
        <Select value={limit} onChange={setLimit} style={{ width: 140 }} options={[50, 100, 250].map((n) => ({ value: n, label: `Newest ${n}` }))} />
      </Space>
      {error ? (
        <Alert
          type="error"
          showIcon
          title={events.length ? `${error}. The rows below are from the previous load.` : error}
          style={{ marginBottom: 12 }}
        />
      ) : null}
      <Table
        columns={columns}
        dataSource={rows}
        rowKey="id"
        size="small"
        loading={loading}
        pagination={tablePagination('events')}
        // "Not loaded" only when there is nothing to show; rows kept from the
        // previous load can still fail to match the search.
        locale={{ emptyText: error && events.length === 0 ? 'Not loaded' : needle ? 'No events match' : 'No audit events' }}
      />
    </div>
  );
}
