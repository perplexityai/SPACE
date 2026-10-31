'use client';

// Cards built from the admin debug route and the operations log: where the
// sandbox runs and ran, what happened to it, and what it was given.

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { Alert, Card, Descriptions, Space, Table, Tag, Tooltip, Typography } from 'antd';
import {
  ClusterOutlined,
  DatabaseOutlined,
  HistoryOutlined,
  ProfileOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { adminApi } from '@/lib/api';
import type { AdminOperationRecord } from '@/lib/public-api';
import {
  formatAge,
  formatBytes,
  formatMetadataValue,
  sectionData,
  sectionError,
  sortedTimeline,
  summarizeTimelineDetail,
  type DebugPod,
  type DebugPort,
  type DebugTimelineEvent,
  type SandboxDebug,
} from '@/lib/sandbox-debug';
import { tablePagination } from '@/lib/table-pagination';
import type { AdminUser, Organization } from '@/lib/types';

const { Text } = Typography;

function When({ value, nowMs }: { value: string | null | undefined; nowMs: number }) {
  if (!value) return <Text type="secondary">—</Text>;
  return (
    <Tooltip title={new Date(value).toLocaleString()}>
      <span>{formatAge(value, nowMs)} ago</span>
    </Tooltip>
  );
}

function SectionError({ error }: { error: string | null }) {
  return error ? <Alert type="warning" showIcon title={`Could not load: ${error}`} style={{ marginBottom: 8 }} /> : null;
}

export function OrgLink({ orgId }: { orgId: string | null }) {
  const [org, setOrg] = useState<Organization | null>(null);
  useEffect(() => {
    setOrg(null);
    if (!orgId) return;
    adminApi.get<Organization>(`/v1/orgs/${orgId}`).then(setOrg).catch(() => setOrg(null));
  }, [orgId]);
  if (!orgId) return <Text type="secondary">—</Text>;
  return (
    <Link href={`/dashboard/orgs/${orgId}`}>
      {org ? org.name : <code>{orgId}</code>}
    </Link>
  );
}

export function UserLink({ userId }: { userId: string | null }) {
  const [user, setUser] = useState<AdminUser | null>(null);
  useEffect(() => {
    setUser(null);
    if (!userId) return;
    adminApi.get<AdminUser>(`/v1/users/${userId}`).then(setUser).catch(() => setUser(null));
  }, [userId]);
  if (!userId) return <Text type="secondary">—</Text>;
  return (
    <Link href={`/dashboard/users/${userId}`}>
      {user ? user.email : <code>{userId}</code>}
    </Link>
  );
}

function nodeLabel(name: string | null, address: string | null, role: string | null) {
  if (!name && !address) return <Text type="secondary">none</Text>;
  return (
    <Space size={4}>
      <span>{name || address}</span>
      {role ? <Tag style={{ marginInlineEnd: 0 }}>{role}</Tag> : null}
      {name && address ? <Text type="secondary">{address}</Text> : null}
    </Space>
  );
}

export function PlacementCard({ debug, nowMs }: { debug: SandboxDebug | null; nowMs: number }) {
  const placement = sectionData(debug?.placement);
  const pods = sectionData(debug?.pods) ?? [];
  const podColumns: ColumnsType<DebugPod> = [
    {
      title: 'Pod',
      dataIndex: 'pod_name',
      render: (name: string, pod) => (
        <Space size={4}>
          <Text code copyable={{ text: name }}>{name}</Text>
          {pod.active ? <Tag color="green">active</Tag> : null}
        </Space>
      ),
    },
    { title: 'Namespace', dataIndex: 'pod_namespace', width: 120 },
    { title: 'Node', dataIndex: 'k8s_node_name', width: 180, render: (v: string | null) => v || '—' },
    { title: 'Gen', dataIndex: 'generation', width: 60 },
    { title: 'State', dataIndex: 'state', width: 110, render: (v: string) => <Tag>{v}</Tag> },
    { title: 'IP', dataIndex: 'pod_ip', width: 120, render: (v: string | null) => v || '—' },
    { title: 'Created', dataIndex: 'created_at', width: 110, render: (v: string) => <When value={v} nowMs={nowMs} /> },
    { title: 'Terminated', dataIndex: 'terminated_at', width: 110, render: (v: string | null) => <When value={v} nowMs={nowMs} /> },
  ];
  return (
    <Card size="small" title={<Space><ClusterOutlined /> Placement and pods</Space>}>
      <SectionError error={sectionError(debug?.placement) ?? sectionError(debug?.pods)} />
      <Descriptions size="small" column={2} style={{ marginBottom: 12 }}>
        <Descriptions.Item label="Placement">{placement?.state ?? '—'}</Descriptions.Item>
        <Descriptions.Item label="Changed">
          <When value={placement?.changed_at} nowMs={nowMs} />
        </Descriptions.Item>
        <Descriptions.Item label="Active node">
          {nodeLabel(placement?.active_k8s_node_name ?? null, placement?.active_node_address ?? null, placement?.active_node_role ?? null)}
        </Descriptions.Item>
        <Descriptions.Item label="Target node">
          {nodeLabel(placement?.target_k8s_node_name ?? null, placement?.target_node_address ?? null, placement?.target_node_role ?? null)}
        </Descriptions.Item>
      </Descriptions>
      <Table
        columns={podColumns}
        dataSource={pods}
        rowKey="id"
        size="small"
        pagination={false}
        scroll={{ x: 900 }}
        locale={{ emptyText: 'No pods recorded' }}
      />
    </Card>
  );
}

export function ResourcesCard({ debug, nowMs }: { debug: SandboxDebug | null; nowMs: number }) {
  const resources = sectionData(debug?.resources);
  const ports = sectionData(debug?.ports) ?? [];
  const observed = resources?.observed ?? null;
  const shape = (value: number | null | undefined, unit: string) =>
    value === null || value === undefined ? <Text type="secondary">template default</Text> : `${value}${unit}`;
  // Only variable names: values can hold credentials.
  const envKeys = Object.keys(resources?.env ?? {}).sort();
  const metadata = Object.entries(resources?.metadata ?? {});
  const portColumns: ColumnsType<DebugPort> = [
    { title: 'Port', dataIndex: 'guest_port', width: 80 },
    { title: 'Protocol', dataIndex: 'protocol', width: 100 },
    { title: 'Visibility', dataIndex: 'visibility', width: 100 },
    {
      title: 'State',
      width: 160,
      render: (_, port) => `${port.desired_state} / ${port.apply_state}`,
    },
    { title: 'Last error', dataIndex: 'last_error', render: (v: unknown) => (v ? <Text type="danger">{String(v)}</Text> : '—') },
  ];
  return (
    <Card size="small" title={<Space><ProfileOutlined /> Resources and runtime</Space>}>
      <SectionError error={sectionError(debug?.resources)} />
      <Descriptions size="small" column={3}>
        <Descriptions.Item label="vCPU">{shape(resources?.vcpu, '')}</Descriptions.Item>
        <Descriptions.Item label="Memory">{shape(resources?.memory_mib, ' MiB')}</Descriptions.Item>
        <Descriptions.Item label="Disk">{shape(resources?.disk_mib, ' MiB')}</Descriptions.Item>
        <Descriptions.Item label="Network rx / tx">
          {observed ? `${formatBytes(observed.network_rx_bytes)} / ${formatBytes(observed.network_tx_bytes)}` : '—'}
        </Descriptions.Item>
        <Descriptions.Item label="Observed">
          <When value={observed?.observed_at} nowMs={nowMs} />
        </Descriptions.Item>
        <Descriptions.Item label="Missed node reports">{observed ? observed.missing_publish_count : '—'}</Descriptions.Item>
        <Descriptions.Item label="Environment" span={3}>
          {envKeys.length ? (
            <Space size={4} wrap>{envKeys.map((key) => <Tag key={key}>{key}</Tag>)}</Space>
          ) : <Text type="secondary">none</Text>}
        </Descriptions.Item>
        <Descriptions.Item label="Metadata" span={3}>
          {metadata.length ? (
            <Space size={4} wrap>
              {metadata.map(([key, value]) => (
                <Tag key={key}>{`${key}=${formatMetadataValue(value)}`}</Tag>
              ))}
            </Space>
          ) : <Text type="secondary">none</Text>}
        </Descriptions.Item>
      </Descriptions>
      <Table
        columns={portColumns}
        dataSource={ports}
        rowKey={(port) => `${port.protocol}:${port.guest_port}`}
        size="small"
        pagination={false}
        locale={{ emptyText: 'No exposed ports' }}
        style={{ marginTop: 8 }}
      />
    </Card>
  );
}

const TIMELINE_COLORS: Record<string, string> = {
  sandbox_created: 'blue',
  pod_created: 'cyan',
  pod_terminated: 'orange',
  snapshot_ready: 'green',
  snapshot_failed: 'red',
};

export function TimelineCard({ debug, nowMs }: { debug: SandboxDebug | null; nowMs: number }) {
  const events = sortedTimeline(sectionData(debug?.timeline) ?? []);
  const columns: ColumnsType<DebugTimelineEvent> = [
    { title: 'When', dataIndex: 'at', width: 120, render: (v: string) => <When value={v} nowMs={nowMs} /> },
    { title: 'Event', dataIndex: 'kind', width: 190, render: (v: string) => <Tag color={TIMELINE_COLORS[v] ?? 'default'}>{v}</Tag> },
    { title: 'Detail', render: (_, event) => summarizeTimelineDetail(event) || '—' },
  ];
  return (
    <Card size="small" title={<Space><HistoryOutlined /> Timeline</Space>}>
      <SectionError error={sectionError(debug?.timeline)} />
      <Table
        columns={columns}
        dataSource={events}
        rowKey={(event, index) => `${event.at}:${event.kind}:${index}`}
        size="small"
        pagination={tablePagination('events', 10)}
        locale={{ emptyText: 'No events recorded' }}
      />
    </Card>
  );
}

const OPERATION_COLORS: Record<string, string> = {
  succeeded: 'green',
  failed: 'red',
  canceled: 'default',
  expired: 'orange',
  running: 'processing',
  queued: 'processing',
};

function operationError(op: AdminOperationRecord): string | null {
  if (op.terminal_error === null || op.terminal_error === undefined) return null;
  return typeof op.terminal_error === 'string' ? op.terminal_error : JSON.stringify(op.terminal_error);
}

export function OperationsCard({
  operations,
  error,
  nowMs,
}: {
  operations: AdminOperationRecord[];
  error: string | null;
  nowMs: number;
}) {
  const columns: ColumnsType<AdminOperationRecord> = [
    { title: 'Requested', dataIndex: 'created_at', width: 110, render: (v: string) => <When value={v} nowMs={nowMs} /> },
    { title: 'Operation', dataIndex: 'kind', width: 170, render: (v: string) => <code>{v}</code> },
    { title: 'State', dataIndex: 'state', width: 100, render: (v: string) => <Tag color={OPERATION_COLORS[v] ?? 'default'}>{v}</Tag> },
    { title: 'Via', dataIndex: 'api_product', width: 80, render: (v: string | null) => v || '—' },
    {
      // Admin-surface creates record the sandbox's owner here, not the
      // operator who issued them; the admin audit log records the operator.
      title: <Tooltip title="The user the operation was recorded for. Creates through the admin API record the sandbox owner.">Recorded for</Tooltip>,
      dataIndex: 'actor_user_id',
      width: 220,
      render: (v: string | null) => <UserLink userId={v} />,
    },
    { title: 'Attempts', dataIndex: 'attempts', width: 80 },
    {
      title: 'Result',
      render: (_, op) => {
        const failure = operationError(op);
        return failure ? <Text type="danger">{op.terminal_error_code ? `${op.terminal_error_code}: ` : ''}{failure}</Text> : op.progress_message || '—';
      },
    },
  ];
  return (
    <Card size="small" title={<Space><DatabaseOutlined /> Operations</Space>}>
      {error ? <Alert type="warning" showIcon title={`Could not load operations: ${error}`} style={{ marginBottom: 8 }} /> : null}
      <Table
        columns={columns}
        dataSource={operations}
        rowKey="id"
        size="small"
        pagination={tablePagination('operations', 10)}
        scroll={{ x: 1000 }}
        locale={{ emptyText: 'No operations recorded' }}
      />
    </Card>
  );
}
