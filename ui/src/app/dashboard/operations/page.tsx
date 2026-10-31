'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { Alert, App, Button, Input, Select, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { InfoCircleOutlined, ReloadOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { adminApi } from '@/lib/api';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
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

const ACTOR_LOOKUPS = 6;
const SBX_TARGET = /^sbx_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/i;

// Rows read under two target IDs, one per operation, newest first.
function mergeNewest(pages: AdminOperation[][], limit: number): AdminOperation[] {
  const byId = new Map<string, AdminOperation>();
  for (const op of pages.flat()) byId.set(op.id, op);
  return [...byId.values()]
    .sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || b.id.localeCompare(a.id))
    .slice(0, limit);
}

const STATES = ['queued', 'running', 'succeeded', 'failed', 'canceled', 'expired'];
const STATE_COLORS: Record<string, string> = {
  succeeded: 'green', failed: 'red', canceled: 'default', expired: 'orange', running: 'processing', queued: 'processing',
};

function targetLink(op: AdminOperation) {
  if (!op.target_id) return '—';
  if (op.target_type === 'sandbox') {
    const link = <Link href={`/dashboard/sandboxes/${publicSandboxIdFromAny(op.target_id)}`}><code>{op.target_id}</code></Link>;
    // A fork records its source sandbox by the bare UUID, not the sbx_ ID.
    return op.target_id.startsWith('sbx_') ? link : (
      <Tooltip title="Forks are listed under their source sandbox's bare UUID; filter by that UUID to find them.">{link}</Tooltip>
    );
  }
  return <code>{op.target_type ? `${op.target_type}:` : ''}{op.target_id}</code>;
}

// The terminal error's message for the cell; the full value goes in a tooltip.
function failureSummary(op: AdminOperation): { text: string; full: string } | null {
  const value = op.terminal_error;
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return { text: value, full: value };
  const message = typeof (value as { message?: unknown }).message === 'string' ? (value as { message: string }).message : null;
  const full = JSON.stringify(value, null, 2);
  return { text: message ?? JSON.stringify(value), full };
}

// Canceled is an outcome someone chose, not a fault.
const FAULT_STATES = new Set(['failed', 'expired']);

function resultCell(op: AdminOperation) {
  const failed = failureSummary(op);
  const text = op.progress_message || failed?.text || '—';
  const fault = FAULT_STATES.has(op.state);
  const body = fault ? <Text type="danger">{text}</Text> : <span>{text}</span>;
  return (
    <Space size={4}>
      {op.terminal_error_code ? <Tag color={fault ? 'red' : 'default'} style={{ marginInlineEnd: 0 }}>{op.terminal_error_code}</Tag> : null}
      {failed ? <Tooltip title={<pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{failed.full}</pre>}>{body}</Tooltip> : body}
    </Space>
  );
}

export default function OperationsPage() {
  const { message } = App.useApp();
  const [operations, setOperations] = useState<AdminOperation[]>([]);
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [state, setState] = useState<string | undefined>();
  const [orgId, setOrgId] = useState<string | undefined>();
  // What is typed, and what the loaded rows were filtered by: text filters
  // apply on Enter and on clear, so a half-typed value never looks applied.
  const [kindInput, setKindInput] = useState('');
  const [targetInput, setTargetInput] = useState('');
  const [kind, setKind] = useState('');
  const [targetId, setTargetId] = useState('');
  const [limit, setLimit] = useState(100);
  // Only the newest request may write its answer.
  const request = useRef(0);
  // Changing another filter applies whatever is typed in the text boxes too,
  // so a box never shows a filter the rows do not have.
  const withTypedFilters = <V,>(set: (value: V) => void) => (value: V) => {
    setKind(kindInput.trim());
    setTargetId(targetInput.trim());
    set(value);
  };

  const load = () => {
    const current = ++request.current;
    const query = (target: string | null) => {
      const params = new URLSearchParams({ limit: String(limit) });
      if (state) params.set('state', state);
      if (orgId) params.set('org_id', orgId);
      if (kind.trim()) params.set('kind', kind.trim());
      if (target) params.set('target_id', target);
      return adminApi.get<AdminOperation[]>(`/v1/operations?${params}`);
    };
    // A fork records its source sandbox by the bare UUID, so an `sbx_` target
    // also reads that form, and the two answers are merged.
    const target = targetId.trim() || null;
    const bare = target ? SBX_TARGET.exec(target)?.[1] ?? null : null;
    setLoading(true);
    (bare ? Promise.all([query(target), query(bare)]).then(([a, b]) => mergeNewest([a, b], limit)) : query(target))
      .then((rows) => {
        if (current !== request.current) return;
        setOperations(rows);
        setError(null);
      })
      .catch((err: unknown) => {
        if (current !== request.current) return;
        setError(err instanceof Error ? err.message : 'Failed to load operations');
      })
      .finally(() => {
        if (current === request.current) setLoading(false);
      });
  };

  useEffect(load, [state, orgId, kind, targetId, limit]);
  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then(setOrgs).catch((err: unknown) => message.error(err instanceof Error ? err.message : 'Failed to load organizations'));
  }, []);

  const orgNames = useMemo(() => new Map(orgs.map((org) => [org.id, org.name])), [orgs]);

  // Resolve each distinct actor to an email. One queue per page keeps at
  // most ACTOR_LOOKUPS lookups in flight across loads; leaving the page
  // cancels them. A failed lookup is not remembered: the next load that
  // lists the actor tries again.
  const [actorEmails, setActorEmails] = useState<Record<string, string>>({});
  const actorQueue = useRef<string[]>([]);
  const actorPending = useRef(new Set<string>());
  const actorInFlight = useRef(0);
  const actorAbort = useRef<AbortController | null>(null);
  useEffect(() => {
    actorAbort.current = new AbortController();
    return () => actorAbort.current?.abort();
  }, []);
  const pumpActors = () => {
    const signal = actorAbort.current?.signal;
    while (signal && !signal.aborted && actorInFlight.current < ACTOR_LOOKUPS && actorQueue.current.length) {
      const userId = actorQueue.current.shift()!;
      actorInFlight.current += 1;
      adminApi
        .get<User>(`/v1/users/${userId}`, { signal })
        .then((u) => { if (!signal.aborted) setActorEmails((current) => ({ ...current, [userId]: u.email })); })
        .catch(() => {})
        .finally(() => {
          actorInFlight.current -= 1;
          actorPending.current.delete(userId);
          pumpActors();
        });
    }
  };
  useEffect(() => {
    const missing = [...new Set(operations.map((op) => op.actor_user_id).filter((v): v is string => !!v))]
      .filter((userId) => !(userId in actorEmails) && !actorPending.current.has(userId));
    for (const userId of missing) {
      actorPending.current.add(userId);
      actorQueue.current.push(userId);
    }
    pumpActors();
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
    { title: 'Result', render: (_, op) => resultCell(op) },
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
        <Select allowClear placeholder="State" value={state} onChange={withTypedFilters(setState)} style={{ width: 150 }} options={STATES.map((s) => ({ value: s, label: s }))} />
        <Select
          allowClear
          placeholder="Organization"
          value={orgId}
          onChange={withTypedFilters(setOrgId)}
          style={{ width: 200 }}
          options={orgs.map((org) => ({ value: org.id, label: org.name }))}
        />
        <Input
          allowClear
          placeholder="Kind, e.g. suspend_sandbox"
          value={kindInput}
          onChange={(e) => {
            setKindInput(e.target.value);
            if (!e.target.value) setKind('');
          }}
          onPressEnter={() => setKind(kindInput.trim())}
          style={{ width: 220 }}
          aria-label="Kind"
        />
        <Input
          allowClear
          placeholder="Target ID, e.g. sbx_… (Enter to apply)"
          title="Forks are listed under their source sandbox's bare UUID; an sbx_ ID also finds them."
          suffix={
            <Tooltip title="Forks are listed under their source sandbox's bare UUID. An sbx_ ID here also searches that UUID, so its forks are included.">
              <InfoCircleOutlined style={{ color: 'rgba(0,0,0,0.45)' }} />
            </Tooltip>
          }
          value={targetInput}
          onChange={(e) => {
            setTargetInput(e.target.value);
            if (!e.target.value) setTargetId('');
          }}
          onPressEnter={() => setTargetId(targetInput.trim())}
          style={{ width: 320 }}
          aria-label="Target ID"
        />
        <Select value={limit} onChange={withTypedFilters(setLimit)} style={{ width: 140 }} options={[50, 100, 250].map((n) => ({ value: n, label: `Newest ${n}` }))} />
      </Space>
      {error ? (
        <Alert
          type="error"
          showIcon
          title={operations.length ? `${error}. The rows below are from the previous filters.` : error}
          style={{ marginBottom: 12 }}
        />
      ) : null}
      <Table
        columns={columns}
        dataSource={operations}
        rowKey="id"
        size="small"
        loading={loading}
        scroll={{ x: 1300 }}
        pagination={tablePagination('operations')}
        locale={{ emptyText: error ? 'Not loaded' : 'No operations match these filters' }}
      />
    </div>
  );
}
