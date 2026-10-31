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
  type DebugSection,
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

function SectionError({ error, what }: { error: string | null; what?: string }) {
  return error ? <Alert type="warning" showIcon title={`Could not load${what ? ` ${what}` : ''}: ${error}`} style={{ marginBottom: 8 }} /> : null;
}

// The debug read as the cards see it. `error` is set when the whole request
// failed; a single failed section carries its own error. Neither may render
// as empty data ("none", "No pods recorded").
export interface DebugView {
  debug: SandboxDebug | null;
  error: string | null;
}

function sectionState(view: DebugView, section: DebugSection<unknown> | undefined) {
  const error = view.error ?? (view.debug ? sectionError(section) : null);
  return { error, pending: !view.debug && !view.error };
}

const unknown = <Text type="secondary">—</Text>;

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

export function PlacementCard({ view, nowMs }: { view: DebugView; nowMs: number }) {
  const { debug } = view;
  const placement = sectionData(debug?.placement);
  const pods = sectionData(debug?.pods) ?? [];
  const placementState = sectionState(view, debug?.placement);
  const podsState = sectionState(view, debug?.pods);
  const node = (name: string | null | undefined, address: string | null | undefined, role: string | null | undefined) =>
    placement ? nodeLabel(name ?? null, address ?? null, role ?? null) : unknown;
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
      <SectionError what="placement" error={placementState.error} />
      {podsState.error !== placementState.error ? <SectionError what="pods" error={podsState.error} /> : null}
      <Descriptions size="small" column={2} style={{ marginBottom: 12 }}>
        <Descriptions.Item label="Placement">{placement?.state ?? '—'}</Descriptions.Item>
        <Descriptions.Item label="Changed">
          <When value={placement?.changed_at} nowMs={nowMs} />
        </Descriptions.Item>
        <Descriptions.Item label="Active node">
          {node(placement?.active_k8s_node_name, placement?.active_node_address, placement?.active_node_role)}
        </Descriptions.Item>
        <Descriptions.Item label="Target node">
          {node(placement?.target_k8s_node_name, placement?.target_node_address, placement?.target_node_role)}
        </Descriptions.Item>
      </Descriptions>
      <Table
        columns={podColumns}
        dataSource={pods}
        rowKey="id"
        size="small"
        pagination={false}
        scroll={{ x: 900 }}
        loading={podsState.pending}
        locale={{ emptyText: podsState.error ? 'Could not load pods' : 'No pods recorded' }}
      />
    </Card>
  );
}

export function ResourcesCard({ view, nowMs }: { view: DebugView; nowMs: number }) {
  const { debug } = view;
  const resources = sectionData(debug?.resources);
  const ports = sectionData(debug?.ports) ?? [];
  const resourcesState = sectionState(view, debug?.resources);
  const portsState = sectionState(view, debug?.ports);
  const observed = resources?.observed ?? null;
  // "template default" and "none" are answers only when the section loaded.
  const shape = (value: number | null | undefined, unit: string) =>
    !resources ? unknown : value === null || value === undefined ? <Text type="secondary">template default</Text> : `${value}${unit}`;
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
      <SectionError what="resources" error={resourcesState.error} />
      {portsState.error !== resourcesState.error ? <SectionError what="ports" error={portsState.error} /> : null}
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
          {!resources ? unknown : envKeys.length ? (
            <Space size={4} wrap>{envKeys.map((key) => <Tag key={key}>{key}</Tag>)}</Space>
          ) : <Text type="secondary">none</Text>}
        </Descriptions.Item>
        <Descriptions.Item label="Metadata" span={3}>
          {!resources ? unknown : metadata.length ? (
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
        loading={portsState.pending}
        locale={{ emptyText: portsState.error ? 'Could not load ports' : 'No exposed ports' }}
        style={{ marginTop: 8 }}
      />
    </Card>
  );
}

// `LIMIT 200` in the debug route's timeline source
// (`crates/libs/api/src/routes/debug/sources/timeline.rs`).
const TIMELINE_CAP = 200;

const TIMELINE_COLORS: Record<string, string> = {
  sandbox_created: 'blue',
  pod_created: 'cyan',
  pod_terminated: 'orange',
  snapshot_ready: 'green',
  snapshot_failed: 'red',
};

export function TimelineCard({ view, nowMs }: { view: DebugView; nowMs: number }) {
  const { debug } = view;
  const events = sortedTimeline(sectionData(debug?.timeline) ?? []);
  const timelineState = sectionState(view, debug?.timeline);
  // The debug route returns at most the newest TIMELINE_CAP events.
  const capped = events.length >= TIMELINE_CAP;
  const columns: ColumnsType<DebugTimelineEvent> = [
    { title: 'When', dataIndex: 'at', width: 120, render: (v: string) => <When value={v} nowMs={nowMs} /> },
    { title: 'Event', dataIndex: 'kind', width: 190, render: (v: string) => <Tag color={TIMELINE_COLORS[v] ?? 'default'}>{v}</Tag> },
    { title: 'Detail', render: (_, event) => summarizeTimelineDetail(event) || '—' },
  ];
  return (
    <Card
      size="small"
      title={<Space><HistoryOutlined /> Timeline</Space>}
      extra={capped ? <Text type="secondary" style={{ fontSize: 12 }}>Newest {TIMELINE_CAP} events; older ones are not shown.</Text> : null}
    >
      <SectionError what="the timeline" error={timelineState.error} />
      <Table
        columns={columns}
        dataSource={events}
        rowKey={(event, index) => `${event.at}:${event.kind}:${index}`}
        size="small"
        pagination={tablePagination('events', 10)}
        loading={timelineState.pending}
        locale={{ emptyText: timelineState.error ? 'Could not load the timeline' : 'No events recorded' }}
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
  more,
  error,
  loaded,
  nowMs,
}: {
  operations: AdminOperationRecord[];
  // More than the newest `operations.length` are recorded.
  more: boolean;
  error: string | null;
  loaded: boolean;
  nowMs: number;
}) {
  const columns: ColumnsType<AdminOperationRecord> = [
    { title: 'Requested', dataIndex: 'created_at', width: 110, render: (v: string) => <When value={v} nowMs={nowMs} /> },
    {
      title: 'Operation',
      dataIndex: 'kind',
      width: 170,
      render: (v: string, op) =>
        op.target_id && !op.target_id.startsWith('sbx_') ? (
          <Tooltip title="Recorded under this sandbox's bare UUID: a fork names its source sandbox that way.">
            <code>{v}</code>
          </Tooltip>
        ) : <code>{v}</code>,
    },
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
    <Card
      size="small"
      title={<Space><DatabaseOutlined /> Operations</Space>}
      extra={
        <Text type="secondary" style={{ fontSize: 12 }}>
          {more ? `Newest ${operations.length} shown; older ones exist. ` : ''}
          Finished operations are removed 7 days after they were requested; unfinished ones are kept.
        </Text>
      }
    >
      {error ? <Alert type="warning" showIcon title={`Could not load operations: ${error}`} style={{ marginBottom: 8 }} /> : null}
      <Table
        columns={columns}
        dataSource={operations}
        rowKey="id"
        size="small"
        pagination={tablePagination('operations', 10)}
        scroll={{ x: 1000 }}
        loading={!loaded && !error}
        locale={{ emptyText: error ? 'Could not load operations' : 'No operations recorded' }}
      />
    </Card>
  );
}
