'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import { Alert, App, Button, Card, Descriptions, Result, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { ArrowLeftOutlined, ReloadOutlined } from '@ant-design/icons';

import { adminApi } from '@/lib/api';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
import type { AdminNodeResponse, NodeHealthStatus } from '@/lib/types';

const { Title, Text } = Typography;

interface NodeSandboxPage {
  sandboxes: { id: string; lifecycle: string; active_node_id: string | null; target_node_id: string | null }[];
  next_after: string | null;
}

// Node actions on `POST /admin/api/v1/nodes/{id}:{action}`, with what each
// does to the node's sandboxes.
const ACTIONS: {
  action: string;
  label: string;
  body?: unknown;
  danger?: boolean;
  when: (node: AdminNodeResponse) => boolean;
  confirm: string;
}[] = [
  // The admin node response does not report whether the substrate node is
  // cordoned, so both are offered outside a drain.
  {
    action: 'cordon', label: 'Cordon', when: (n) => !n.drain_phase,
    confirm: 'New sandboxes stop being placed on this node; running sandboxes are not touched.',
  },
  {
    action: 'uncordon', label: 'Uncordon', when: (n) => !n.drain_phase,
    confirm: 'The node accepts new sandboxes again. Has no effect if it is not cordoned.',
  },
  {
    action: 'drain', label: 'Drain (gentle)', body: { mode: 'gentle' }, when: (n) => !n.drain_phase,
    confirm: 'The node stops taking new sandboxes and waits for resident sandboxes to leave on their own.',
  },
  {
    action: 'drain', label: 'Drain (forced)', body: { mode: 'forced' }, danger: true, when: (n) => !n.drain_phase || n.drain_mode !== 'forced',
    confirm: 'Resident sandboxes are evacuated now: running sandboxes are suspended and moved, interrupting their work.',
  },
  {
    action: 'undrain', label: 'Undrain', when: (n) => !!n.drain_phase && n.drain_phase !== 'terminating',
    confirm: 'The drain is cancelled and the node accepts new sandboxes again.',
  },
  {
    action: 'force-upload-local-snapshots', label: 'Upload local snapshots', when: (n) => !!n.drain_phase && (n.sandboxes_local_snapshots ?? 0) > 0,
    confirm: 'Suspended sandboxes pinned to this node re-upload their snapshots to the object store, so they can restore elsewhere.',
  },
  {
    action: 'terminate', label: 'Terminate', danger: true, when: (n) => n.drain_phase === 'drained',
    confirm: 'The node is terminated. The API refuses while it still holds sandboxes or node-local snapshots.',
  },
];

function formatUnix(secs: number) {
  return secs ? new Date(secs * 1000).toLocaleString() : '—';
}

export default function NodeDetailPage() {
  const { message, modal } = App.useApp();
  const { id } = useParams<{ id: string }>();
  const [node, setNode] = useState<AdminNodeResponse | null>(null);
  const [health, setHealth] = useState<NodeHealthStatus | null>(null);
  const [healthError, setHealthError] = useState<string | null>(null);
  const [page, setPage] = useState<NodeSandboxPage | null>(null);
  const [cursor, setCursor] = useState<string | undefined>();
  const [error, setError] = useState<{ status: number | null; message: string } | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const loadNode = () => {
    adminApi
      .get<AdminNodeResponse>(`/v1/nodes/${id}?include_occupancy=true`)
      .then((loaded) => {
        setNode(loaded);
        setError(null);
      })
      .catch((err: any) => setError({ status: typeof err?.status === 'number' ? err.status : null, message: err?.message || 'Failed to load node' }));
    adminApi
      .get<NodeHealthStatus>(`/v1/nodes/${id}/health`)
      .then((loaded) => {
        setHealth(loaded);
        setHealthError(null);
      })
      .catch((err: any) => setHealthError(err?.message || 'Failed to load health'));
  };

  useEffect(loadNode, [id]);
  useEffect(() => {
    const params = new URLSearchParams();
    if (cursor) params.set('after', cursor);
    setPage(null);
    adminApi.get<NodeSandboxPage>(`/v1/nodes/${id}/sandboxes?${params}`).then(setPage).catch(() => setPage({ sandboxes: [], next_after: null }));
  }, [id, cursor]);

  const runAction = (item: (typeof ACTIONS)[number]) => {
    modal.confirm({
      title: `${item.label} ${node?.name ?? id}?`,
      content: item.confirm,
      okText: item.label,
      okButtonProps: { danger: item.danger },
      onOk: async () => {
        setBusy(item.label);
        try {
          await adminApi.post(`/v1/nodes/${id}:${item.action}`, item.body);
          message.success(`${item.label} requested`);
          loadNode();
        } catch (err: any) {
          message.error(err?.message || `${item.label} failed`);
        } finally {
          setBusy(null);
        }
      },
    });
  };

  if (error) {
    return (
      <Result
        status="warning"
        title={error.status === 404 ? 'Node not found' : 'Could not load the node'}
        subTitle={<>{error.message} <code>{id}</code></>}
        extra={
          <Space>
            {error.status !== 404 ? <Button type="primary" icon={<ReloadOutlined />} onClick={loadNode}>Retry</Button> : null}
            <Link href="/dashboard/nodes"><ArrowLeftOutlined /> Nodes</Link>
          </Space>
        }
      />
    );
  }
  if (!node) return null;

  const slots = node.network_slots;
  const status = node.status;

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }} wrap>
        <Space align="center">
          <Link href="/dashboard/nodes"><ArrowLeftOutlined /> Nodes</Link>
          <Title level={3} style={{ margin: 0 }}>{node.name || node.address || node.id}</Title>
          <Tag>{node.role}</Tag>
          {node.drain_phase ? <Tag color="orange">{node.drain_phase} · {node.drain_mode ?? 'gentle'}</Tag> : null}
          <Tag color={node.scheduling_status === 'ready' ? 'green' : 'default'}>{node.scheduling_status ?? 'unknown'}</Tag>
        </Space>
        <Space wrap>
          {ACTIONS.filter((item) => item.when(node)).map((item) => (
            <Button key={item.label} danger={item.danger} loading={busy === item.label} disabled={!!busy} onClick={() => runAction(item)}>
              {item.label}
            </Button>
          ))}
          <Button icon={<ReloadOutlined />} onClick={loadNode}>Refresh</Button>
        </Space>
      </Space>

      <Card size="small" title="Node">
        <Descriptions size="small" column={3}>
          <Descriptions.Item label="ID"><Text code copyable>{node.id}</Text></Descriptions.Item>
          <Descriptions.Item label="Address">{node.address || '—'}</Descriptions.Item>
          <Descriptions.Item label="Kubernetes node">{node.k8s_node_name || '—'}</Descriptions.Item>
          <Descriptions.Item label="Managed by">{node.managed_by}</Descriptions.Item>
          <Descriptions.Item label="State">{node.state || 'registered'}</Descriptions.Item>
          <Descriptions.Item label="Scheduling">{node.scheduling_reason || node.scheduling_status || '—'}</Descriptions.Item>
          <Descriptions.Item label="Last heartbeat">{node.last_heartbeat ? new Date(node.last_heartbeat).toLocaleString() : '—'}</Descriptions.Item>
          <Descriptions.Item label="Status observed">
            {status ? `${new Date(status.observed_at).toLocaleString()}${status.stale ? ' (stale)' : ''}` : '—'}
          </Descriptions.Item>
          <Descriptions.Item label="Drain error">{node.drain_last_error ? <Text type="danger">{node.drain_last_error}</Text> : '—'}</Descriptions.Item>
        </Descriptions>
      </Card>

      <Card size="small" title="Occupancy">
        <Descriptions size="small" column={3}>
          <Descriptions.Item label="Active sandboxes">{node.sandboxes_active ?? 'not computed'}</Descriptions.Item>
          <Descriptions.Item label="Node-local snapshots">{node.sandboxes_local_snapshots ?? 'not computed'}</Descriptions.Item>
          <Descriptions.Item label="Network slots leased">
            {slots ? `${slots.leased ?? '—'} of ${slots.total ?? '—'} (max ${slots.max_slots ?? '—'})` : '—'}
          </Descriptions.Item>
          <Descriptions.Item label="Slots ready / min">{slots ? `${slots.ready ?? '—'} / ${slots.min_ready ?? '—'}` : '—'}</Descriptions.Item>
          <Descriptions.Item label="Slots degraded">{slots?.degraded ?? '—'}</Descriptions.Item>
          <Descriptions.Item label="Slots quarantined">{slots?.quarantined ?? '—'}</Descriptions.Item>
        </Descriptions>
      </Card>

      <Card size="small" title="Health">
        {healthError ? <Alert type="warning" showIcon title={`Could not load health: ${healthError}`} style={{ marginBottom: 8 }} /> : null}
        {health ? (
          <>
            <Space style={{ marginBottom: 8 }}>
              <Tag color={health.healthy ? 'green' : 'red'}>{health.healthy ? 'healthy' : 'unhealthy'}</Tag>
              {!health.reachable ? <Tag color="red">unreachable</Tag> : null}
              {health.error ? <Text type="danger">{health.error}</Text> : null}
            </Space>
            {health.services.length ? (
              <Space wrap style={{ marginBottom: 8 }}>
                {health.services.map((service) => (
                  <Tooltip key={service.name} title={service.error || 'reachable'}>
                    <Tag color={service.reachable ? 'green' : 'red'}>{service.name}</Tag>
                  </Tooltip>
                ))}
              </Space>
            ) : null}
            <Table
              size="small"
              rowKey="name"
              pagination={false}
              dataSource={health.node_ctl?.reconciliations ?? []}
              locale={{ emptyText: health.node_ctl ? 'No reconcilers reported' : 'node-ctl not reachable' }}
              columns={[
                { title: 'Reconciler', dataIndex: 'name' },
                { title: 'Last success', dataIndex: 'last_success_at_unix_secs', render: formatUnix },
                { title: 'Next due', dataIndex: 'next_due_at_unix_secs', render: formatUnix },
                { title: 'Seen (templates / sandboxes)', render: (_, r) => `${r.templates_seen} / ${r.sandboxes_seen}` },
                { title: 'Last error', dataIndex: 'last_error', render: (v: string) => (v ? <Text type="danger">{v}</Text> : '—') },
              ]}
            />
          </>
        ) : null}
      </Card>

      <Card
        size="small"
        title="Sandboxes on this node"
        extra={
          <Space>
            <Button size="small" disabled={!cursor} onClick={() => setCursor(undefined)}>First page</Button>
            <Button size="small" disabled={!page?.next_after} onClick={() => page?.next_after && setCursor(page.next_after)}>Next 100</Button>
          </Space>
        }
      >
        <Table
          size="small"
          rowKey="id"
          loading={page === null}
          dataSource={page?.sandboxes ?? []}
          pagination={false}
          locale={{ emptyText: 'No sandboxes placed on this node' }}
          columns={[
            {
              title: 'Sandbox', dataIndex: 'id',
              render: (sid: string) => <Link href={`/dashboard/sandboxes/${publicSandboxIdFromAny(sid)}`}><code>{publicSandboxIdFromAny(sid)}</code></Link>,
            },
            { title: 'Lifecycle', dataIndex: 'lifecycle', width: 140, render: (v: string) => <Tag>{v}</Tag> },
            {
              title: 'Placement', width: 160,
              render: (_, row) => [row.active_node_id === node.id ? 'active' : '', row.target_node_id === node.id ? 'target' : ''].filter(Boolean).join(', '),
            },
          ]}
        />
      </Card>
    </Space>
  );
}
