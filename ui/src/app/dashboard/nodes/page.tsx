'use client';

import { type ReactNode, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import {
  AdminNodeResponse,
  DrainMode,
  NodeRole,
  NodeView,
  ReconciliationStatus,
} from '@/lib/types';
import { tablePagination } from '@/lib/table-pagination';
import {
  markViewRefreshed,
  runLockedViewRefresh,
  VIEW_REFRESH_INTERVAL_MS,
  VIEW_REFRESH_STALE_MS,
} from '@/lib/view-refresh';
import {
  Alert,
  Button,
  Drawer,
  Card,
  Checkbox,
  Form,
  Input,
  Popconfirm,
  Select,
  Space,
  Table,
  Tag,
  Typography,
  App,
  Tooltip,
} from 'antd';
import { DeleteOutlined, PauseCircleOutlined, PlayCircleOutlined, PlusOutlined, ReloadOutlined, SyncOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

const { Title, Text } = Typography;

const roleOptions = [
  { label: 'Worker', value: 'worker' },
  { label: 'Builder', value: 'builder' },
  { label: 'Cluster', value: 'cluster' },
];

interface NodeSandboxPage {
  sandboxes: { id: string; lifecycle: string; active_node_id: string | null; target_node_id: string | null }[];
  next_after: string | null;
}

const NODE_STATUS_REFRESH_KEY_PREFIX = 'dashboard:nodes:status';
const DEFAULT_RECONCILIATION_PERIOD_SECS = 60;
const RECONCILER_STALE_GRACE_SECS = 30;
const REFRESH_CLICK_COOLDOWN_MS = 3000;

function roleColor(role: NodeRole) {
  switch (role) {
    case 'worker':
      return 'blue';
    case 'builder':
      return 'green';
    case 'cluster':
      return 'gold';
    default:
      return 'default';
  }
}

function managedByColor(managedBy: string) {
  switch (managedBy) {
    case 'k8s':
      return 'purple';
    case 'space':
      return 'cyan';
    default:
      return 'default';
  }
}

function drainPhaseColor(phase: string) {
  switch (phase) {
    case 'draining':
      return 'orange';
    case 'drained':
      return 'blue';
    case 'terminating':
      return 'red';
    default:
      return 'default';
  }
}

function formatUnixAge(unixSecs: number, nowMs = Date.now()) {
  if (!unixSecs) {
    return 'never';
  }
  const seconds = Math.max(0, Math.floor(nowMs / 1000) - unixSecs);
  if (seconds < 60) {
    return `${seconds}s ago`;
  }
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }
  return `${Math.floor(hours / 24)}d ago`;
}

function formatIsoAge(value: string | null | undefined, nowMs = Date.now()) {
  if (!value) {
    return 'never';
  }
  return formatUnixAge(Math.floor(new Date(value).getTime() / 1000), nowMs);
}

function formatUnixTime(unixSecs: number) {
  return unixSecs ? new Date(unixSecs * 1000).toLocaleString() : 'never';
}

function isReconciliationStale(status: ReconciliationStatus, nowMs = Date.now()) {
  const lastAt = status.last_success_at_unix_secs || status.last_completed_at_unix_secs;
  if (status.running) {
    return false;
  }
  if (!lastAt) {
    return true;
  }
  const graceSecs = Math.max(status.poll_interval_secs * 3, RECONCILER_STALE_GRACE_SECS);
  return Math.floor(nowMs / 1000) - lastAt > graceSecs;
}

function formatReasonLabel(reason: string) {
  return reason
    .split('_')
    .filter(Boolean)
    .join(' ');
}

function triggerReasons(node: NodeView) {
  const trigger = node.reconcile_trigger;
  if (!trigger) {
    return [];
  }
  if (trigger.reasons.length > 0) {
    return trigger.reasons;
  }
  return trigger.diagnostic_reason
    .split(',')
    .map((reason) => reason.trim())
    .filter(Boolean);
}

function latestIsoTimestamp(values: Array<string | null | undefined>) {
  return values.reduce<{ value: string | null; ms: number }>(
    (latest, value) => {
      if (!value) {
        return latest;
      }
      const ms = new Date(value).getTime();
      if (!Number.isFinite(ms) || ms <= latest.ms) {
        return latest;
      }
      return { value, ms };
    },
    { value: null, ms: 0 },
  ).value;
}

function heartbeatValue(node: NodeView) {
  return latestIsoTimestamp([node.status?.observed_at, node.last_heartbeat]);
}

function reconciliationPeriodSecs(node: NodeView) {
  const intervals = node.status?.health?.node_ctl?.reconciliations
    .map((status) => status.poll_interval_secs)
    .filter((value) => Number.isFinite(value) && value > 0) ?? [];
  return intervals.length > 0 ? Math.max(...intervals) : DEFAULT_RECONCILIATION_PERIOD_SECS;
}

function isNodeHeartbeatStale(node: NodeView, nowMs = Date.now()) {
  const value = heartbeatValue(node);
  if (!value) {
    return true;
  }
  const heartbeatMs = new Date(value).getTime();
  if (!Number.isFinite(heartbeatMs)) {
    return true;
  }
  return nowMs - heartbeatMs > reconciliationPeriodSecs(node) * 2 * 1000;
}

function reconcileUnavailableReason(node: NodeView) {
  if (node.role !== 'worker' && node.role !== 'builder') {
    return 'Only worker and builder nodes support reconciliation';
  }
  if (!node.status?.health) {
    return 'Reconciler status is not loaded';
  }
  if (!node.status.health.reachable) {
    return node.status.health.error || 'node-api is not reachable';
  }
  if (!node.status.health.node_ctl) {
    return 'node-ctl status is not available';
  }
  if (!node.status.health.node_ctl.reachable) {
    return node.status.health.node_ctl.error || 'node-ctl is not reachable';
  }
  return null;
}

function observedAtMs(node: NodeView) {
  return node.status?.observed_at ? new Date(node.status.observed_at).getTime() : 0;
}

function mergeNodesMonotonic(previous: NodeView[], next: NodeView[]) {
  const previousById = new Map(previous.map((node) => [node.id, node]));
  return next.map((node) => {
    const previousNode = previousById.get(node.id);
    if (previousNode?.status && !node.status) {
      return { ...node, status: previousNode.status };
    }
    if (!previousNode?.status || !node.status) {
      return node;
    }
    return observedAtMs(previousNode) > observedAtMs(node)
      ? { ...node, status: previousNode.status }
      : node;
  });
}

function renderNodeStatus(node: NodeView, nowMs: number) {
  if (node.role === 'cluster') {
    return <Text type="secondary">—</Text>;
  }
  const heartbeat = heartbeatValue(node);
  const seen = formatIsoAge(heartbeat, nowMs);
  const periodSecs = reconciliationPeriodSecs(node);
  const stale = isNodeHeartbeatStale(node, nowMs);
  const scheduling = node.status?.scheduling;
  const reachable = node.status?.reachable ?? false;
  const current = stale
    ? 'stale'
    : !node.status
      ? 'unknown'
      : !reachable
        ? 'unreachable'
        : 'ok';
  const tooltip = [
    `heartbeat: ${heartbeat ? new Date(heartbeat).toLocaleString() : 'never'}`,
    `stale threshold: ${periodSecs * 2}s without heartbeat`,
    node.status ? `node-api: ${reachable ? 'reachable' : 'unreachable'}` : 'node-api: not loaded',
    node.status?.error,
    scheduling ? `scheduling: ${scheduling.status}` : 'scheduling: unknown',
    scheduling?.reason,
    `ready slots: ${scheduling?.network_slots_ready ?? '—'}`,
    `leased slots: ${scheduling?.network_slots_leased ?? '—'}`,
    `max slots: ${scheduling?.network_pool_max_slots ?? '—'}`,
  ]
    .filter(Boolean)
    .join('\n');
  const failed = stale || (node.status ? !reachable : false);
  const color = failed ? 'red' : current === 'unknown' ? 'default' : 'green';
  return (
    <Tooltip title={<span style={{ whiteSpace: 'pre-line' }}>{tooltip}</span>}>
      <Tag color={color}>
        {current} · seen {seen}
      </Tag>
    </Tooltip>
  );
}

function renderReconciler(node: NodeView, nowMs: number) {
  if (node.role === 'cluster') {
    return <Text type="secondary">—</Text>;
  }
  const renderCell = (primary: ReactNode) => {
    const trigger = node.reconcile_trigger;
    const reasons = triggerReasons(node);
    if (!trigger) {
      return primary;
    }
    const failed = !!trigger.dead_at || !!trigger.last_error || !!trigger.dead_reason;
    const reasonText = reasons.length > 0
      ? reasons.map(formatReasonLabel).join('\n')
      : 'unspecified';
    const tooltip = [
      `reasons:\n${reasonText}`,
      `requested: ${new Date(trigger.requested_at).toLocaleString()}`,
      `next attempt: ${new Date(trigger.next_attempt_at).toLocaleString()}`,
      `attempts: ${trigger.attempts}`,
      trigger.last_error ? `last error: ${trigger.last_error}` : null,
      trigger.dead_at ? `dead at: ${new Date(trigger.dead_at).toLocaleString()}` : null,
      trigger.dead_reason ? `dead reason: ${trigger.dead_reason}` : null,
    ]
      .filter(Boolean)
      .join('\n');
    return (
      <Space size={4} wrap>
        {primary}
        <Tooltip title={<span style={{ whiteSpace: 'pre-line' }}>{tooltip}</span>}>
          <Tag color={failed ? 'red' : 'blue'}>queued {reasons.length || 1}</Tag>
        </Tooltip>
      </Space>
    );
  };
  if (!node.status?.health) {
    return renderCell(<Text type="secondary">not loaded</Text>);
  }
  if (!node.status.health.reachable) {
    return renderCell(
      <Tooltip title={node.status.health.error || 'node-api is not reachable'}>
        <Tag color="red">{formatIsoAge(node.status.observed_at, nowMs)}</Tag>
      </Tooltip>,
    );
  }
  const nodeCtl = node.status.health.node_ctl;
  if (!nodeCtl || !nodeCtl.reachable) {
    return renderCell(
      <Tooltip title={nodeCtl?.error || 'node-ctl status is not available'}>
        <Tag color="red">{formatIsoAge(node.status.observed_at, nowMs)}</Tag>
      </Tooltip>,
    );
  }
  const status =
    nodeCtl.reconciliations.find((entry) => entry.name === 'network-policies') ||
    nodeCtl.reconciliations[0];
  if (!status) {
    return renderCell(<Tag color="green">never</Tag>);
  }
  const failed = !!status.last_error;
  const eventAt = failed
    ? status.last_completed_at_unix_secs
    : status.last_success_at_unix_secs || status.last_completed_at_unix_secs;
  const stale = isReconciliationStale(status, nowMs);
  const current = status.running ? 'reconciling' : failed ? 'failing' : stale ? 'stale' : 'ok';
  const tooltip = [
    `current: ${current}`,
    `${failed ? 'next retry' : 'next due'}: ${formatUnixTime(status.next_due_at_unix_secs)}`,
    failed ? `last failure: ${status.last_error}` : null,
  ]
    .filter(Boolean)
    .join('\n');
  const color = failed || stale ? 'red' : 'green';

  return renderCell(
    <Tooltip title={<span style={{ whiteSpace: 'pre-line' }}>{tooltip}</span>}>
      <Tag color={color}>{eventAt ? formatUnixAge(eventAt, nowMs) : 'never'}</Tag>
    </Tooltip>,
  );
}

export default function NodesPage() {
  const { message } = App.useApp();
  const { substrate } = useAuth();
  const [selectedNode, setSelectedNode] = useState<NodeView | null>(null);
  const [sandboxPage, setSandboxPage] = useState<NodeSandboxPage | null>(null);
  const [loadingSandboxes, setLoadingSandboxes] = useState(false);
  const [sandboxError, setSandboxError] = useState<string | null>(null);
  const sandboxRequest = useRef(0);

  const loadNodeSandboxes = async (node: NodeView, after?: string) => {
    const request = ++sandboxRequest.current;
    setSelectedNode(node);
    setLoadingSandboxes(true);
    setSandboxError(null);
    setSandboxPage(null);
    try {
      const params = new URLSearchParams();
      if (after) params.set('after', after);
      const page = await adminApi.get<NodeSandboxPage>(`/v1/nodes/${node.id}/sandboxes?${params}`);
      if (request === sandboxRequest.current) setSandboxPage(page);
    } catch (err) {
      if (request === sandboxRequest.current) setSandboxError(err instanceof Error ? err.message : 'Could not load sandboxes');
    } finally {
      if (request === sandboxRequest.current) setLoadingSandboxes(false);
    }
  };

  const [nodes, setNodes] = useState<NodeView[]>([]);
  const [loadingNodes, setLoadingNodes] = useState(true);
  const loadingNodesRef = useRef(true);
  const [drainingNodeId, setDrainingNodeId] = useState<string | null>(null);
  const [undrainingNodeId, setUndrainingNodeId] = useState<string | null>(null);
  const [deletingNodeId, setDeletingNodeId] = useState<string | null>(null);
  const [reconcilingNodeId, setReconcilingNodeId] = useState<string | null>(null);
  const [refreshingNodeStatus, setRefreshingNodeStatus] = useState(false);
  const refreshingNodeStatusRef = useRef(false);
  const [refreshCooldown, setRefreshCooldown] = useState(false);
  const refreshCooldownRef = useRef(false);
  const refreshCooldownTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [roleFilter, setRoleFilter] = useState<NodeRole | 'all'>('all');
  const [nodeAgeReferenceMs, setNodeAgeReferenceMs] = useState(() => Date.now());
  const [form] = Form.useForm();
  const selectedRole = Form.useWatch('role', form) as NodeRole | undefined;
  const endpointPlaceholder =
    selectedRole === 'cluster'
      ? '10.0.0.12:9800'
      : selectedRole === 'builder'
        ? '10.0.0.12:9801'
        : '10.0.0.12:9802';

  const nodeRefreshKey = `${NODE_STATUS_REFRESH_KEY_PREFIX}:${roleFilter}`;

  const startRefreshCooldown = () => {
    refreshCooldownRef.current = true;
    setRefreshCooldown(true);
    if (refreshCooldownTimerRef.current) {
      clearTimeout(refreshCooldownTimerRef.current);
    }
    refreshCooldownTimerRef.current = setTimeout(() => {
      refreshCooldownRef.current = false;
      setRefreshCooldown(false);
      refreshCooldownTimerRef.current = null;
    }, REFRESH_CLICK_COOLDOWN_MS);
  };

  const fetchAdminNodes = async () => {
    const params = new URLSearchParams({ include_occupancy: 'false' });
    if (roleFilter !== 'all') params.set('role', roleFilter);
    return adminApi.get<AdminNodeResponse[]>(`/v1/nodes?${params.toString()}`);
  };

  const buildNodeView = (admin: AdminNodeResponse): NodeView => ({
    id: admin.id,
    name: admin.name,
    address: admin.address,
    node_api_endpoint: admin.node_api_endpoint,
    role: admin.role as NodeRole,
    managed_by: admin.managed_by,
    state: admin.state,
    scheduling_status: admin.scheduling_status,
    scheduling_reason: admin.scheduling_reason,
    drain_phase: admin.drain_phase,
    drain_mode: admin.drain_mode,
    drain_started_at: admin.drain_started_at,
    drain_last_error: admin.drain_last_error,
    sandboxes_active: admin.sandboxes_active,
    sandboxes_local_snapshots: admin.sandboxes_local_snapshots,
    last_heartbeat: admin.last_heartbeat,
    created_at: admin.created_at,
    updated_at: admin.updated_at,
    deleted_at: admin.deleted_at,
    network_slots: admin.network_slots,
    status: admin.status,
    reconcile_trigger: null,
  });

  const loadNodes = async ({
    showLoading = true,
    suppressError = false,
  }: { showLoading?: boolean; suppressError?: boolean } = {}) => {
    if (showLoading) setLoadingNodes(true);
    try {
      const adminNodes = await fetchAdminNodes();
      const next = adminNodes.map((admin) => buildNodeView(admin));
      setNodes((previous) => mergeNodesMonotonic(previous, next));
      setNodeAgeReferenceMs(Date.now());
    } catch (err: any) {
      if (!suppressError) message.error(err.message);
      throw err;
    } finally {
      if (showLoading) setLoadingNodes(false);
    }
  };

  const maybeRefreshNodes = () => {
    if (loadingNodesRef.current) return;
    if (refreshingNodeStatusRef.current) return;
    if (document.hidden) return;
    void runLockedViewRefresh(
      nodeRefreshKey,
      () => loadNodes({ showLoading: false, suppressError: true }),
      { staleMs: VIEW_REFRESH_STALE_MS },
    ).catch(() => {});
  };

  useEffect(() => {
    loadingNodesRef.current = loadingNodes;
  }, [loadingNodes]);

  useEffect(() => {
    refreshingNodeStatusRef.current = refreshingNodeStatus;
  }, [refreshingNodeStatus]);

  useEffect(() => {
    void loadNodes().then(() => markViewRefreshed(nodeRefreshKey)).catch(() => {});
    const interval = setInterval(maybeRefreshNodes, VIEW_REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [roleFilter]);

  useEffect(() => () => {
    if (refreshCooldownTimerRef.current) {
      clearTimeout(refreshCooldownTimerRef.current);
    }
  }, []);

  const refreshNodeStatus = async (force: boolean) => {
    refreshingNodeStatusRef.current = true;
    setRefreshingNodeStatus(true);
    try {
      await loadNodes({ showLoading: false });
      markViewRefreshed(nodeRefreshKey);
      if (force) {
        message.success('Nodes refreshed');
      }
    } catch (err: any) {
      message.error(err.message);
    } finally {
      refreshingNodeStatusRef.current = false;
      setRefreshingNodeStatus(false);
    }
  };

  const refreshAll = () => {
    void loadNodes().catch(() => {});
  };

  const handleCreate = async (values: {
    name?: string;
    address?: string;
    node_api_endpoint: string;
    role: NodeRole;
    verify?: boolean;
  }) => {
    try {
      await adminApi.post('/v1/nodes', {
        name: values.name?.trim() || undefined,
        address: values.address?.trim() || undefined,
        node_api_endpoint: values.node_api_endpoint.trim(),
        role: values.role,
        verify: values.verify ?? true,
      });
      form.resetFields();
      form.setFieldsValue({ role: 'worker', verify: true });
      await loadNodes({ showLoading: false });
      message.success('Node registered');
    } catch (err: any) {
      message.error(err.message);
    }
  };

  const handleDelete = async (id: string) => {
    setDeletingNodeId(id);
    try {
      await adminApi.delete(`/v1/nodes/${id}`);
      await loadNodes({ showLoading: false });
      message.success('Node deleted');
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setDeletingNodeId(null);
    }
  };

  const handleReconcileNetworkPolicy = async (id: string) => {
    const node = nodes.find((node) => node.id === id);
    const disabledReason = node ? reconcileUnavailableReason(node) : null;
    if (disabledReason) {
      message.warning(disabledReason);
      return;
    }
    setReconcilingNodeId(id);
    try {
      await adminApi.post(`/v1/nodes/${id}/network-policy:reconcile`);
      await loadNodes({ showLoading: false });
      message.success('Network policy reconcile queued');
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setReconcilingNodeId(null);
    }
  };

  // Sends `mode` explicitly rather than relying on the API default, so the
  // dashboard's behavior is pinned here instead of tracking a server-side
  // default it doesn't display.
  const handleDrain = async (id: string, mode: DrainMode) => {
    setDrainingNodeId(id);
    try {
      await adminApi.post(`/v1/nodes/${id}:drain`, { mode });
      message.success('Node drain initiated — waiting for resident sandboxes to leave');
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setDrainingNodeId(null);
      void loadNodes({ showLoading: false }).catch(() => {});
    }
  };

  const handleUndrain = async (id: string) => {
    setUndrainingNodeId(id);
    try {
      await adminApi.post(`/v1/nodes/${id}:undrain`);
      message.success('Node undrain initiated');
    } catch (err: any) {
      message.error(err.message);
    } finally {
      setUndrainingNodeId(null);
      void loadNodes({ showLoading: false }).catch(() => {});
    }
  };

  const nodeColumns: ColumnsType<NodeView> = [
    {
      title: 'Name',
      dataIndex: 'name',
      width: 180,
      render: (name: string | null, record) => (
        <Button type="link" style={{ padding: 0 }} onClick={() => void loadNodeSandboxes(record)}>
          {name || record.id.slice(0, 8)}
        </Button>
      ),
    },
    {
      title: 'Role',
      dataIndex: 'role',
      width: 100,
      render: (role: NodeRole) => <Tag color={roleColor(role)}>{role}</Tag>,
    },
    {
      title: 'Substrate',
      dataIndex: 'managed_by',
      width: 100,
      render: (value: string) => <Tag color={managedByColor(value)}>{value}</Tag>,
    },
    {
      title: 'Endpoint',
      dataIndex: 'node_api_endpoint',
      render: (value: string | null) => (value ? <code>{value}</code> : <Text type="secondary">—</Text>),
    },
    { title: 'Address', dataIndex: 'address', width: 140, render: (value: string | null) => value || '—' },
    { title: 'State', dataIndex: 'state', width: 120, render: (value: string | null) => value || 'registered' },
    {
      title: 'Drain',
      width: 170,
      render: (_, record) => {
        if (!record.drain_phase) {
          return <Text type="secondary">—</Text>;
        }
        const phase = record.drain_phase;
        const elapsed = record.drain_started_at
          ? formatIsoAge(record.drain_started_at, nodeAgeReferenceMs)
          : null;
        // An absent mode means the column is NULL — a drain that predates it, or
        // a row nobody touched. Every reader resolves that to gentle, so the
        // label has to say gentle rather than blank or forced.
        const mode: DrainMode = record.drain_mode ?? 'gentle';
        const label = elapsed ? `${phase} · ${mode} · ${elapsed}` : `${phase} · ${mode}`;
        const tooltip = [
          mode === 'gentle'
            ? 'gentle: waiting for resident sandboxes to leave on their own'
            : 'forced: evacuating resident sandboxes now',
          record.drain_last_error ? `error: ${record.drain_last_error}` : null,
          record.drain_started_at
            ? `started: ${new Date(record.drain_started_at).toLocaleString()}`
            : null,
          `active sandboxes: ${record.sandboxes_active ?? 'not computed'}`,
          `local snapshots: ${record.sandboxes_local_snapshots ?? 'not computed'}`,
        ]
          .filter(Boolean)
          .join('\n');
        return (
          <Tooltip title={<span style={{ whiteSpace: 'pre-line' }}>{tooltip}</span>}>
            <Tag color={drainPhaseColor(phase)}>{label}</Tag>
          </Tooltip>
        );
      },
    },
    {
      title: 'Status',
      width: 210,
      render: (_, record) => renderNodeStatus(record, nodeAgeReferenceMs),
    },
    {
      title: 'Reconciler',
      width: 190,
      render: (_, record) => renderReconciler(record, nodeAgeReferenceMs),
    },
    {
      title: 'Actions',
      width: 120,
      render: (_, record) => {
        const drainPhase = record.drain_phase;
        // The direct/self-managed substrate refuses node drain with 501;
        // undrain stays available there to clear a phase an earlier drain left.
        const canDrain =
          substrate !== 'space' &&
          (record.role === 'worker' || record.role === 'builder') &&
          drainPhase === null;
        const canUndrain =
          (record.role === 'worker' || record.role === 'builder') &&
          (drainPhase === 'draining' || drainPhase === 'drained');
        return (
          <Space size={4}>
            {(record.role === 'worker' || record.role === 'builder') && (
              <Tooltip title={reconcileUnavailableReason(record) || 'Reconcile network policies'}>
                <Button
                  type="text"
                  size="small"
                  className="icon-action-btn"
                  icon={<SyncOutlined />}
                  aria-label="Reconcile network policies"
                  loading={reconcilingNodeId === record.id}
                  disabled={!!reconcileUnavailableReason(record)}
                  onClick={() => handleReconcileNetworkPolicy(record.id)}
                />
              </Tooltip>
            )}
            {canDrain && (
              // Gentle only, deliberately. A destructive action must never sit
              // on a confirm dialog's cancel slot — that slot universally means
              // "back out", so the click a user reaches for to abort would
              // instead suspend and migrate every sandbox on the node. Forced
              // drain is therefore a CLI action (`space-admin-cli drain-node --forced`),
              // which is where the rollout runbooks already point, and not a
              // dashboard button one misclick away from evacuating a node.
              <Popconfirm
                title="Drain this node?"
                description="Cordons the node and waits for its sandboxes to leave on their own. Use `space-admin-cli drain-node --forced` to evacuate them immediately instead."
                okText="Drain"
                onConfirm={() => handleDrain(record.id, 'gentle')}
              >
                <Tooltip title="Drain">
                  <Button
                    type="text"
                    size="small"
                    className="icon-action-btn"
                    icon={<PauseCircleOutlined />}
                    aria-label="Drain"
                    loading={drainingNodeId === record.id}
                  />
                </Tooltip>
              </Popconfirm>
            )}
            {canUndrain && (
              <Tooltip title="Undrain">
                <Button
                  type="text"
                  size="small"
                  className="icon-action-btn"
                  icon={<PlayCircleOutlined />}
                  aria-label="Undrain"
                  loading={undrainingNodeId === record.id}
                  onClick={() => handleUndrain(record.id)}
                />
              </Tooltip>
            )}
            {record.managed_by === 'space' ? (
              <Popconfirm title="Delete this node?" onConfirm={() => handleDelete(record.id)}>
                <Tooltip title="Delete">
                  <Button
                    type="text"
                    size="small"
                    className="icon-action-btn"
                    danger
                    icon={<DeleteOutlined />}
                    loading={deletingNodeId === record.id}
                    aria-label="Delete"
                  />
                </Tooltip>
              </Popconfirm>
            ) : (
              <Tooltip title="Lifecycle managed by Kubernetes">
                <Text type="secondary">—</Text>
              </Tooltip>
            )}
          </Space>
        );
      },
    },
  ];

  return (
    <div className="compact-table">
      <Drawer
        title={`Sandboxes on ${selectedNode?.name || selectedNode?.id || 'node'}`}
        open={selectedNode !== null}
        size={760}
        onClose={() => { ++sandboxRequest.current; setSelectedNode(null); }}
      >
        <p>Active and target placements recorded for this node. Open a sandbox for runtime details.</p>
        {sandboxError && <Alert type="error" title={sandboxError} action={<Button onClick={() => selectedNode && void loadNodeSandboxes(selectedNode)}>Retry</Button>} />}
        <Table
          rowKey="id"
          loading={loadingSandboxes}
          dataSource={sandboxPage?.sandboxes ?? []}
          size="small"
          scroll={{ y: 'calc(100vh - 300px)' }}
          pagination={false}
          columns={[
            { title: 'Sandbox', dataIndex: 'id', width: 360, render: (id: string) => <Link href={`/dashboard/sandboxes/${id}`}>{id}</Link> },
            { title: 'Lifecycle', dataIndex: 'lifecycle', width: 120 },
            { title: 'Placement', render: (_, row) => [row.active_node_id === selectedNode?.id ? 'active' : '', row.target_node_id === selectedNode?.id ? 'target' : ''].filter(Boolean).join(', ') },
          ]}
        />
        <Space style={{ marginTop: 16 }}>
          <Button disabled={loadingSandboxes} onClick={() => selectedNode && void loadNodeSandboxes(selectedNode)}>First page</Button>
          <Button disabled={loadingSandboxes || !sandboxPage?.next_after} onClick={() => selectedNode && sandboxPage?.next_after && void loadNodeSandboxes(selectedNode, sandboxPage.next_after)}>Next 100</Button>
        </Space>
      </Drawer>
      <style>{`.compact-table .ant-table-cell { padding: 4px 6px !important; } .compact-table .icon-action-btn { padding-left: 6px; padding-right: 6px; } .compact-table .icon-action-btn .anticon { font-size: 18px; }`}</style>
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <Title level={3}>Nodes</Title>
        <Space>
          <Button
            icon={<ReloadOutlined />}
            loading={refreshingNodeStatus || loadingNodes}
            disabled={refreshCooldown}
            onClick={() => {
              if (refreshCooldownRef.current) return;
              startRefreshCooldown();
              void refreshNodeStatus(true);
            }}
          >
            Refresh
          </Button>
        </Space>
      </Space>

      {substrate === 'space' && (
        <Card size="small" title="Register direct node" style={{ marginBottom: 24 }}>
          <Form
            form={form}
            layout="vertical"
            onFinish={handleCreate}
            initialValues={{ role: 'worker', verify: true }}
          >
            <Space wrap align="start">
              <Form.Item name="name" label="Name">
                <Input placeholder="worker-a" style={{ width: 180 }} />
              </Form.Item>
              <Form.Item name="role" label="Role" rules={[{ required: true }]}>
                <Select options={roleOptions} style={{ width: 130 }} />
              </Form.Item>
              <Form.Item
                name="node_api_endpoint"
                label="Node API endpoint"
                rules={[{ required: true, message: 'Endpoint required' }]}
              >
                <Input placeholder={endpointPlaceholder} style={{ width: 240 }} />
              </Form.Item>
              <Form.Item name="address" label="Address">
                <Input placeholder="10.0.0.12" style={{ width: 160 }} />
              </Form.Item>
              <Form.Item name="verify" valuePropName="checked" label=" ">
                <Checkbox>Verify NodeInfo</Checkbox>
              </Form.Item>
              <Form.Item label=" ">
                <Button type="primary" htmlType="submit" icon={<PlusOutlined />}>Register</Button>
              </Form.Item>
            </Space>
          </Form>
        </Card>
      )}
      <Space style={{ marginBottom: 12 }}>
        <Text strong>Registered nodes</Text>
        <Select
          value={roleFilter}
          onChange={setRoleFilter}
          style={{ width: 140 }}
          options={[{ label: 'All roles', value: 'all' }, ...roleOptions]}
        />
      </Space>
      <Table
        columns={nodeColumns}
        dataSource={nodes}
        rowKey="id"
        loading={loadingNodes}
        size="small"
        pagination={tablePagination('nodes')}
        style={{ marginBottom: 24 }}
      />
    </div>
  );
}
