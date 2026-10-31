'use client';

import { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { useParams } from 'next/navigation';
import { Alert, App, Button, Card, Descriptions, Result, Space, Table, Tag, Tooltip, Typography } from 'antd';
import { ArrowLeftOutlined, ReloadOutlined } from '@ant-design/icons';

import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import {
  UPLOAD_ACCEPTED_MEANS,
  UPLOAD_ATTEMPTS_PER_CALL,
  UPLOAD_LISTED_PER_CALL,
  UPLOAD_RETRY_ORDER,
  nodeActions,
  type ForceUploadResponse,
  type NodeAction,
} from '@/lib/node-actions';
import { publicSandboxIdFromAny } from '@/lib/sandbox-id';
import type { AdminNodeResponse, NodeHealthStatus } from '@/lib/types';

const { Title, Text } = Typography;

interface NodeSandboxPage {
  sandboxes: { id: string; lifecycle: string; active_node_id: string | null; target_node_id: string | null }[];
  next_after: string | null;
}

function formatUnix(secs: number) {
  return secs ? new Date(secs * 1000).toLocaleString() : '—';
}

export default function NodeDetailPage() {
  const { message, modal } = App.useApp();
  const { id } = useParams<{ id: string }>();
  const { substrate } = useAuth();
  const [node, setNode] = useState<AdminNodeResponse | null>(null);
  const [page, setPage] = useState<NodeSandboxPage | null>(null);
  const [pageError, setPageError] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | undefined>();
  // Paging and Refresh can overlap; only the newest request's answer is kept.
  const pageRequest = useRef(0);
  // The same for node reads. An action bumps it as it starts, so a read that
  // began before the action cannot land after it and restore the old state.
  const nodeRequest = useRef(0);
  const [error, setError] = useState<{ status: number | null; message: string } | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  // Health comes from the node response's own status snapshot, so it is
  // always shown with that snapshot's time, never with a newer read's.
  const loadNode = (): Promise<void> => {
    const request = ++nodeRequest.current;
    return adminApi
      .get<AdminNodeResponse>(`/v1/nodes/${id}?include_occupancy=true`)
      .then((loaded) => {
        if (request !== nodeRequest.current) return;
        setNode(loaded);
        setError(null);
      })
      .catch((err: any) => {
        if (request !== nodeRequest.current) return;
        setError({ status: typeof err?.status === 'number' ? err.status : null, message: err?.message || 'Failed to load node' });
      });
  };

  const loadSandboxes = (after: string | undefined = cursor) => {
    const request = ++pageRequest.current;
    const params = new URLSearchParams();
    if (after) params.set('after', after);
    setPage(null);
    setPageError(null);
    adminApi
      .get<NodeSandboxPage>(`/v1/nodes/${id}/sandboxes?${params}`)
      .then((loaded) => {
        if (request === pageRequest.current) setPage(loaded);
      })
      .catch((err: any) => {
        if (request === pageRequest.current) setPageError(err?.message || 'Failed to load sandboxes');
      });
  };

  const refreshAll = () => {
    void loadNode();
    loadSandboxes();
  };

  useEffect(() => {
    void loadNode();
  }, [id]);
  useEffect(() => loadSandboxes(cursor), [id, cursor]);

  const showUploadResult = (result: ForceUploadResponse) => {
    const accepted = result.reuploaded.length;
    const failed = result.failed ?? [];
    const notAttempted = result.deferred ?? [];
    modal.info({
      title: `Upload requests accepted for ${accepted} sandbox${accepted === 1 ? '' : 'es'}${failed.length ? `; ${failed.length} not accepted` : ''}`,
      width: 640,
      content: (
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          <Text>{UPLOAD_ACCEPTED_MEANS}</Text>
          {failed.length ? <Text strong>Requests that failed:</Text> : null}
          {failed.map((f) => (
            <Text key={f.sandbox_id} type="danger">{publicSandboxIdFromAny(f.sandbox_id)}: {f.error}</Text>
          ))}
          {notAttempted.length ? (
            <Text>
              {notAttempted.length} sandbox{notAttempted.length === 1 ? '' : 'es'} not attempted in this call: one call attempts at most {UPLOAD_ATTEMPTS_PER_CALL}.
            </Text>
          ) : null}
          {result.truncated ? (
            <Text>More sandboxes are pinned to this node than one call lists ({UPLOAD_LISTED_PER_CALL}).</Text>
          ) : null}
          {notAttempted.length || result.truncated || failed.length ? <Text type="secondary">{UPLOAD_RETRY_ORDER}</Text> : null}
        </Space>
      ),
    });
  };

  const runAction = (item: NodeAction) => {
    modal.confirm({
      title: `${item.label} ${node?.name ?? id}?`,
      content: item.confirm,
      okText: item.label,
      okButtonProps: { danger: item.dangerous },
      autoFocusButton: item.dangerous ? 'cancel' : 'ok',
      onOk: async () => {
        setBusy(item.label);
        // Earlier reads describe the node before this action: drop them.
        nodeRequest.current += 1;
        try {
          const result = await adminApi.post<ForceUploadResponse | unknown>(`/v1/nodes/${id}:${item.action}`, item.body);
          if (item.action === 'force-upload-local-snapshots' && result && typeof result === 'object' && 'reuploaded' in result) {
            showUploadResult(result as ForceUploadResponse);
          } else {
            message.success(`${item.label} requested`);
          }
        } catch (err: any) {
          message.error(err?.message || `${item.label} failed`);
        } finally {
          // Some actions commit and then report an error: always reload, and
          // keep the actions disabled until that read settles.
          loadSandboxes();
          await loadNode();
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
            {error.status !== 404 ? <Button type="primary" icon={<ReloadOutlined />} onClick={refreshAll}>Retry</Button> : null}
            <Link href="/dashboard/nodes"><ArrowLeftOutlined /> Nodes</Link>
          </Space>
        }
      />
    );
  }
  if (!node) return null;

  const slots = node.network_slots;
  const status = node.status;
  const health: NodeHealthStatus | null = status?.health ?? null;
  const hasHealth = node.role === 'worker' || node.role === 'builder';

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
          {nodeActions(node, substrate).map((item) => (
            <Button key={item.label} danger={item.dangerous} loading={busy === item.label} disabled={!!busy} onClick={() => runAction(item)}>
              {item.label}
            </Button>
          ))}
          <Button icon={<ReloadOutlined />} onClick={refreshAll} disabled={!!busy}>Refresh</Button>
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

      {hasHealth ? (
        <Card size="small" title="Health">
          {!health ? <Text type="secondary">No status snapshot has been received from this node.</Text> : null}
          {health ? (
            <>
              <Space style={{ marginBottom: 8 }} wrap>
                {!health.reachable ? (
                  <Tag color="red">unreachable</Tag>
                ) : (
                  <Tag color={health.healthy ? 'green' : 'red'}>{health.healthy ? 'healthy' : 'unhealthy'}</Tag>
                )}
                {status?.observed_at ? (
                  <Text type="secondary">
                    as of {new Date(status.observed_at).toLocaleString()}{status.stale ? ' (stale)' : ''}
                  </Text>
                ) : null}
                {health.error ? <Text type="danger">{health.error}</Text> : null}
                {health.node_ctl && !health.node_ctl.reachable ? (
                  <Text type="danger">node-ctl unreachable{health.node_ctl.error ? `: ${health.node_ctl.error}` : ''}</Text>
                ) : null}
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
      ) : null}

      <Card
        size="small"
        title="Sandboxes on this node"
        extra={
          <Space>
            <Button size="small" disabled={!cursor} onClick={() => setCursor(undefined)}>First page</Button>
            <Button size="small" icon={<ReloadOutlined />} onClick={() => loadSandboxes()}>Reload</Button>
            <Button size="small" disabled={!page?.next_after} onClick={() => page?.next_after && setCursor(page.next_after)}>Next 100</Button>
          </Space>
        }
      >
        {pageError ? (
          <Alert
            type="error"
            showIcon
            title={`Could not load this node's sandboxes: ${pageError}`}
            action={<Button size="small" onClick={() => loadSandboxes()}>Retry</Button>}
            style={{ marginBottom: 8 }}
          />
        ) : null}
        <Table
          size="small"
          rowKey="id"
          loading={page === null && !pageError}
          dataSource={page?.sandboxes ?? []}
          pagination={false}
          locale={{ emptyText: pageError ? 'Not loaded' : 'No sandboxes placed on this node' }}
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
