'use client';

import { Suspense, useEffect, useId, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import styles from './inventory.module.css';
import { usePathname, useSearchParams } from 'next/navigation';
import { sandboxLabelOrId } from '@/lib/sandbox-id';
import {
  createConsoleSandboxForOrg,
  forceConsoleSandboxAction,
  getConsoleSandboxes,
  getPublicPage,
  postPublicOperation,
  publicSnapshotToConsoleSnapshot,
  publicTemplateToConsoleTemplate,
  updateConsoleSandbox,
  waitForAdminOperation,
  waitForPublicOperation,
} from '@/lib/public-api';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import {
  NetworkPolicyV1,
  Organization,
  PublicSnapshot,
  PublicTemplate,
  Sandbox,
  SnapshotInfo,
  Template,
} from '@/lib/types';
import { tablePagination } from '@/lib/table-pagination';
import {
  markViewRefreshed,
  runLockedViewRefresh,
  VIEW_REFRESH_INTERVAL_MS,
  VIEW_REFRESH_STALE_MS,
} from '@/lib/view-refresh';
import NetworkPolicyDesigner, {
  compactNetworkPolicyLabel,
  networkPoliciesEqual,
} from '@/components/network-policy-designer';
import NetworkPolicyGenerationTag from '@/components/network-policy-generation-tag';
import {
  AutoLifecycleEditor,
  type AutoLifecycleFormValue,
  formValueToPolicy,
  formatNextAutoEvent,
  policyToFormValue,
  summarizeAutoLifecycle,
} from '@/components/auto-lifecycle';
import {
  App, Table, Button, Form, Input, Select, Typography, Space, Tag, Popconfirm, Card, Tooltip, Switch, Checkbox, Modal, Collapse, Alert,
} from 'antd';
import {
  PlusOutlined, StopOutlined, PauseCircleOutlined, ThunderboltOutlined,
  SaveOutlined, DeleteOutlined, CameraOutlined, RollbackOutlined, ReloadOutlined,
  CopyOutlined, ClockCircleOutlined, GlobalOutlined, LockOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

const { Title, Text } = Typography;
const SANDBOXES_VIEW_REFRESH_KEY = 'dashboard:sandboxes';
const REFRESH_CLICK_COOLDOWN_MS = 3000;

type AgeWindow = '1h' | '24h' | '7d' | '30d' | '90d' | '1y' | 'all';
type NodeFilter = 'any' | 'assigned' | 'unassigned';

const VALID_AGE_WINDOWS: readonly AgeWindow[] = ['1h', '24h', '7d', '30d', '90d', '1y', 'all'];
const VALID_NODE_FILTERS: readonly NodeFilter[] = ['any', 'assigned', 'unassigned'];
const DEFAULT_AGE_WINDOW: AgeWindow = '24h';
const DEFAULT_NODE_FILTER: NodeFilter = 'any';

const parseAgeParam = (raw: string | null): AgeWindow =>
  VALID_AGE_WINDOWS.includes(raw as AgeWindow) ? (raw as AgeWindow) : DEFAULT_AGE_WINDOW;
const parseNodeParam = (raw: string | null): NodeFilter =>
  VALID_NODE_FILTERS.includes(raw as NodeFilter) ? (raw as NodeFilter) : DEFAULT_NODE_FILTER;
const parseCsvParam = (raw: string | null): string[] =>
  raw ? raw.split(',').map((s) => s.trim()).filter(Boolean) : [];

const AGE_WINDOW_OPTIONS: { value: AgeWindow; label: string; ms: number | null }[] = [
  { value: '1h',  label: 'Last 1h',     ms: 1 * 60 * 60 * 1000 },
  { value: '24h', label: 'Last 24h',    ms: 24 * 60 * 60 * 1000 },
  { value: '7d',  label: 'Last 7 days', ms: 7 * 24 * 60 * 60 * 1000 },
  { value: '30d', label: 'Last 30 days',ms: 30 * 24 * 60 * 60 * 1000 },
  { value: '90d', label: 'Last 90 days',ms: 90 * 24 * 60 * 60 * 1000 },
  { value: '1y',  label: 'Last year',   ms: 365 * 24 * 60 * 60 * 1000 },
  { value: 'all', label: 'All time',    ms: null },
];

const STATUS_FILTER_OPTIONS = [
  'created', 'creating', 'starting', 'started', 'running', 'stopping', 'stopped',
  'pausing', 'paused', 'resuming',
  'suspending', 'suspended', 'restoring',
  'deleting', 'deleted', 'failed',
];

function formatBytes(n: number | null | undefined) {
  if (n === null || n === undefined) return '—';
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)} KB`;
  return `${n} B`;
}

function formatRelativeTime(value: string | null | undefined, nowMs: number) {
  if (!value) return '—';
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return '—';
  const seconds = Math.max(0, Math.floor((nowMs - timestamp) / 1000));
  if (seconds < 60) return 'just now';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months}mo ago`;
  return `${Math.floor(days / 365)}y ago`;
}

type ActivitySource = 'action' | 'command' | 'activity';

function parseTimestampMs(value: string | null | undefined): number | null {
  if (!value) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function latestSandboxActivity(record: Sandbox): { at: string | null; source: ActivitySource | null } {
  const accessMs = parseTimestampMs(record.last_access_at);
  const commandMs = parseTimestampMs(record.last_command_at);

  if (commandMs !== null && (accessMs === null || commandMs >= accessMs)) {
    return { at: record.last_command_at, source: 'command' };
  }
  if (accessMs !== null) {
    return { at: record.last_access_at, source: 'action' };
  }
  return record.last_used_at ? { at: record.last_used_at, source: 'activity' } : { at: null, source: null };
}

function activityTimeCell(record: Sandbox, nowMs: number) {
  const activity = latestSandboxActivity(record);
  if (!activity.at || !activity.source) return '—';
  const timestamp = Date.parse(activity.at);
  if (!Number.isFinite(timestamp)) return '—';
  const label = formatRelativeTime(activity.at, nowMs);
  const sourceLabel =
    activity.source === 'command' ? 'command' : activity.source === 'action' ? 'action' : 'activity';
  return (
    <Tooltip
      title={
        <Space direction="vertical" size={0}>
          <span>{`Last ${sourceLabel}: ${new Date(timestamp).toLocaleString()}`}</span>
          {record.updated_at ? (
            <span>{`Row updated: ${formatRelativeTime(record.updated_at, nowMs)}`}</span>
          ) : null}
        </Space>
      }
    >
      <Tag color={activity.source === 'command' ? 'blue' : 'default'} style={{ marginInlineEnd: 0 }}>
        {activity.source === 'command' ? 'cmd' : 'act'} {label}
      </Tag>
    </Tooltip>
  );
}

// State machine
const STATE_ACTIONS: Record<string, Record<string, boolean>> = {
  creating:  { wake: false, stop: false, pause: false, suspend: false, delete: true },
  created:   { wake: true,  stop: false, pause: false, suspend: false, delete: true },
  started:   { wake: false, stop: true,  pause: true,  suspend: true,  delete: true },
  running:   { wake: false, stop: true,  pause: true,  suspend: true,  delete: true },
  stopped:   { wake: true,  stop: false, pause: false, suspend: false, delete: true },
  paused:    { wake: true,  stop: false, pause: false, suspend: false, delete: true },
  suspended: { wake: true,  stop: false, pause: false, suspend: false, delete: true },
  deleting:  { wake: false, stop: false, pause: false, suspend: false, delete: false },
  deleted:   { wake: false, stop: false, pause: false, suspend: false, delete: true },
  failed:    { wake: false, stop: false, pause: false, suspend: false, delete: true },
};
const TRANSIENT = { wake: false, stop: false, pause: false, suspend: false, delete: true };

function statusTag(status: string | null, failureReason?: string | null) {
  if (!status) return <Tag style={{ marginInlineEnd: 0 }}>unknown</Tag>;
  const colorMap: Record<string, string> = {
    started: 'green', running: 'green', created: 'gold', stopped: 'default',
    paused: 'orange', suspended: 'purple', failed: 'red',
  };
  const tag = (
    <Tag color={status.endsWith('ing') ? 'processing' : (colorMap[status] || 'default')} style={{ marginInlineEnd: 0 }}>
      {status}
    </Tag>
  );
  if (status === 'failed' && failureReason) {
    return <Tooltip title={failureReason}>{tag}</Tooltip>;
  }
  return tag;
}

function isDigestTag(tag: string) {
  return tag.startsWith('sha256:') || /^[a-f0-9]{64}$/i.test(tag);
}

function normalizedDigest(value: string) {
  return value.replace(/^sha256:/, '');
}

function visibleReleaseTags(tags: string[] | null | undefined, contentHash: string | null | undefined) {
  const contentDigest = contentHash ? normalizedDigest(contentHash) : null;
  return (tags || []).filter((tag) => {
    if (isDigestTag(tag)) return false;
    return !contentDigest || normalizedDigest(tag) !== contentDigest;
  });
}

function templateTooltip(sandbox: Sandbox, contentHash: string | null | undefined) {
  const releaseTags = visibleReleaseTags(sandbox.release_tags, contentHash);
  if (releaseTags.length === 0) return `Content hash: ${contentHash}`;

  return (
    <div>
      <div><strong>Content hash:</strong> {contentHash}</div>
      <div><strong>Release tags:</strong> {releaseTags.join(', ')}</div>
    </div>
  );
}

function isPoolSandbox(sandbox: Sandbox) {
  return sandbox.resource_type === 'pool_slot';
}

function sandboxResourceTag(sandbox: Sandbox) {
  switch (sandbox.resource_type) {
    case 'pool_slot':
      return <Tag color="cyan" style={{ marginInlineEnd: 0 }}>Pool slot</Tag>;
    case 'observed_local':
      return <Tag color="default" style={{ marginInlineEnd: 0 }}>Observed local</Tag>;
    default:
      return null;
  }
}

function encryptedTag(sandbox: Sandbox) {
  if (!sandbox.volume_encryption) return null;
  return (
    <Tooltip title="BYOK: this sandbox runs on encrypted volumes keyed to its org's master key">
      <Tag color="geekblue" icon={<LockOutlined />} style={{ marginInlineEnd: 0 }}>
        encrypted
      </Tag>
    </Tooltip>
  );
}

function snapshotsPanelAvailable(sandbox: Sandbox) {
  return !isPoolSandbox(sandbox) && (Boolean(sandbox.snapshots_enabled_desired) || Boolean(sandbox.snapshots_enabled));
}

function hasAutoLifecyclePolicy(policy: Sandbox['auto_lifecycle']) {
  return summarizeAutoLifecycle(policy) !== 'off';
}

function snapshotsToggleTitle(sandbox: Sandbox) {
  if (sandbox.pool_template) {
    return `Snapshots are disabled for pool sandboxes (${sandbox.pool_template})`;
  }
  const desired = !!sandbox.snapshots_enabled_desired;
  const observed = !!sandbox.snapshots_enabled;
  if (desired !== observed) {
    return desired
      ? 'Snapshots requested; waiting for node to enable'
      : 'Snapshots disable requested; waiting for node to disable';
  }
  return desired
    ? 'Snapshots enabled - click to disable'
    : 'Snapshots disabled - click to enable';
}

function formatSandboxNode(node: Sandbox['active_node']) {
  if (!node) return null;
  return node.name || node.address || node.id.slice(0, 8);
}

function nodeRoleColor(role: string | null | undefined) {
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

function sandboxNodeTooltipRows(
  node: Sandbox['active_node'],
  source: 'active',
) {
  if (!node) return [];
  return [
    ['Source', source],
    ['Name', node.name || '—'],
    ['Role', node.role || 'node'],
    ['Managed by', node.managed_by || '—'],
    ['Address', node.address || '—'],
    ['ID', node.id],
  ];
}

function sandboxNodeTooltip(rows: string[][], title?: string) {
  return (
    <div>
      {title ? <div><strong>{title}</strong></div> : null}
      {rows.map(([label, value], index) => (
        <div key={`${label}-${index}`}>
          <strong>{label}:</strong> {value}
        </div>
      ))}
    </div>
  );
}

function sandboxNodeDisplay(sandbox: Sandbox) {
  const active = sandbox.active_node;
  const node = active;
  const label = formatSandboxNode(node);
  if (!node || !label) return '—';

  const tag = (
    <Tag
      color={nodeRoleColor(node.role)}
      style={{
        display: 'inline-block',
        marginInlineEnd: 0,
        maxWidth: 88,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        verticalAlign: 'middle',
        whiteSpace: 'nowrap',
      }}
    >
      {label}
    </Tag>
  );

  return (
    <Tooltip title={sandboxNodeTooltip(sandboxNodeTooltipRows(node, 'active'))}>
      {tag}
    </Tooltip>
  );
}

function SandboxActions({ status, id, resourceType, poolTemplate, onAction }: {
  status: string | null;
  id: string;
  resourceType: Sandbox['resource_type'];
  poolTemplate?: string | null;
  onAction: (id: string, action: string) => Promise<void>;
}) {
  const [stopping, setStopping] = useState(false);
  const stopPending = useRef(false);
  const s = status || '';
  const managedSandbox = resourceType === 'sandbox';
  const poolSlot = resourceType === 'pool_slot';
  const observedLocal = resourceType === 'observed_local';
  const actions = managedSandbox
    ? { ...(s.endsWith('ing') ? TRANSIENT : (STATE_ACTIONS[s] || TRANSIENT)) }
    : { wake: false, stop: false, pause: false, suspend: false, delete: true };
  if (poolSlot) {
    actions.suspend = false;
  }

  const confirmStop = async () => {
    if (!actions.stop || stopPending.current) return;
    stopPending.current = true;
    setStopping(true);
    try {
      await onAction(id, 'stop');
    } finally {
      stopPending.current = false;
      setStopping(false);
    }
  };

  const btn = (action: string, enabled: boolean, icon: React.ReactNode, label: string, danger = false) => {
    const button = (
      <Button type="text" size="small" className="icon-action-btn" icon={icon} disabled={!enabled || (action === 'stop' && stopping)} danger={danger}
        aria-label={label}
        onClick={action === 'stop' ? undefined : () => enabled && onAction(id, action)} />
    );
    return (
      <Tooltip
        title={!enabled
          ? (managedSandbox
            ? `Cannot ${label.toLowerCase()} in ${s || 'unknown'} state`
            : poolSlot
              ? 'Pool slots can only be deleted'
              : 'Observed local sandboxes only support delete')
          : label}
        key={action}
      >
        {action === 'stop' ? (
          <Popconfirm
            title={`Stop sandbox ${sandboxLabelOrId(id)}?`}
            description="Stopping interrupts current execution without guaranteeing a snapshot."
            okText="Stop"
            cancelText="Cancel"
            okButtonProps={{ danger: true }}
            cancelButtonProps={{ disabled: stopping }}
            onConfirm={confirmStop}
            disabled={!enabled || stopping}
          >
            {button}
          </Popconfirm>
        ) : button}
      </Tooltip>
    );
  };

  return (
    <Space size={0}>
      {actions.wake ? btn('wake', true, <ThunderboltOutlined />, 'Wake')
        : btn('stop', actions.stop, <StopOutlined />, 'Stop', true)}
      {btn('pause', actions.pause, <PauseCircleOutlined />, 'Pause')}
      {btn('suspend', actions.suspend, <SaveOutlined />, 'Suspend')}
      {actions.delete ? (
        <Tooltip title={poolSlot ? 'Delete pool slot' : observedLocal ? 'Delete observed local sandbox' : 'Delete'}>
          <Popconfirm
            title={
              poolSlot
                ? `Delete this ${poolTemplate || 'template'} pool slot?`
                : observedLocal
                  ? 'Delete this observed local sandbox?'
                  : 'Delete this sandbox?'
            }
            description={
              poolSlot
                ? 'The pool controller may recreate it to maintain the configured target size.'
                : observedLocal
                  ? 'This deletes node-local runtime state that is not tracked by a control-plane sandbox row.'
                  : undefined
            }
            onConfirm={() => onAction(id, 'delete')}
          >
            <Button type="text" size="small" className="icon-action-btn" danger icon={<DeleteOutlined />} aria-label="Delete" />
          </Popconfirm>
        </Tooltip>
      ) : btn('delete', false, <DeleteOutlined />, 'Delete', true)}
    </Space>
  );
}

function SnapshotsPanel({ sandboxId }: { sandboxId: string }) {
  const { message } = App.useApp();
  const [snapshots, setSnapshots] = useState<SnapshotInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getPublicPage<PublicSnapshot>(`/sandboxes/${sandboxId}/snapshots`)
      .then((items) => setSnapshots(items.map(publicSnapshotToConsoleSnapshot)))
      .catch((err: any) => message.error(err.message || 'failed to load snapshots'))
      .finally(() => setLoading(false));
  }, [sandboxId]);

  const handleRollback = async (snapshotName: string) => {
    try {
      await postPublicOperation(`/sandboxes/${sandboxId}:rollback`, {
        snapshot_name: snapshotName,
        auto_restart: true,
      });
      message.success('Snapshot rollback queued');
    } catch (err: any) {
      message.error(err.message || 'failed to queue rollback');
    }
  };

  const formatSnapshotBytes = (n: number) => {
    if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
    if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
    if (n >= 1e3) return `${(n / 1e3).toFixed(1)} KB`;
    return `${n} B`;
  };

  const snapshotColumns: ColumnsType<SnapshotInfo> = [
    { title: 'Name', dataIndex: 'id', width: 220, ellipsis: true },
    { title: 'Type', dataIndex: 'type', width: 60 },
    {
      title: 'Timestamp', dataIndex: 'timestamp', width: 200,
      render: (v: string | null) => {
        if (!v) return '—';
        // Parse compact "20260331T140000Z" → ISO "2026-03-31T14:00:00Z"
        if (v.length >= 16) {
          const iso = `${v.slice(0,4)}-${v.slice(4,6)}-${v.slice(6,8)}T${v.slice(9,11)}:${v.slice(11,13)}:${v.slice(13,15)}Z`;
          const d = new Date(iso);
          if (!isNaN(d.getTime())) {
            return new Intl.DateTimeFormat(undefined, {
              dateStyle: 'medium', timeStyle: 'medium',
            }).format(d);
          }
        }
        // Fallback: try parsing as-is (RFC3339 etc.)
        const d = new Date(v);
        if (!isNaN(d.getTime())) {
          return new Intl.DateTimeFormat(undefined, {
            dateStyle: 'medium', timeStyle: 'medium',
          }).format(d);
        }
        return v;
      },
    },
    { title: 'Size', dataIndex: 'size_bytes', width: 100, render: (v: number) => v ? formatSnapshotBytes(v) : '0' },
    {
      title: 'Action', width: 120,
      render: (_, record) => (
        <Tooltip title="Roll back sandbox to this snapshot">
          <Popconfirm
            title={`Roll back to ${record.id}?`}
            onConfirm={() => handleRollback(record.id)}
          >
            <Button type="link" size="small" icon={<RollbackOutlined />}>Rollback</Button>
          </Popconfirm>
        </Tooltip>
      ),
    },
  ];

  return (
    <div style={{ padding: '8px 0' }}>
      <Text strong><CameraOutlined /> Snapshots</Text>
      <Table
        columns={snapshotColumns}
        dataSource={snapshots}
        rowKey="id"
        loading={loading}
        size="small"
        pagination={tablePagination('snapshots', 10)}
        style={{ marginTop: 8 }}
        locale={{ emptyText: 'No snapshots' }}
      />
    </div>
  );
}

// `useSearchParams` inside the component below opts the route into client-side
// rendering. Next 15 requires a Suspense boundary around any component that
// reads searchParams; `next build` errors without one. The fallback is empty
// because the hydration on mount is synchronous (initial state derived from
// URL via lazy initializers) so the boundary effectively never resolves
// "after" — it's just there to make the static analysis happy.
export default function SandboxesPageRoute() {
  return (
    <Suspense fallback={null}>
      <SandboxesPage />
    </Suspense>
  );
}

function SandboxReceipt({ sandboxId }: { sandboxId: string }) {
  return (
    <Space size={6}>
      <Link href={`/dashboard/sandboxes/${sandboxId}`}>
        <code>{sandboxLabelOrId(sandboxId)}</code>
      </Link>
      <Button
        type="text"
        size="small"
        icon={<CopyOutlined />}
        onClick={() => {
          if (typeof navigator !== 'undefined' && navigator.clipboard) {
            void navigator.clipboard.writeText(sandboxId);
          }
        }}
        aria-label="Copy sandbox ID"
      />
    </Space>
  );
}

function SandboxesPage() {
  const { message } = App.useApp();
  const [sandboxes, setSandboxes] = useState<Sandbox[]>([]);
  const [templates, setTemplates] = useState<Template[]>([]);
  const { org: authOrg } = useAuth();
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(true);
  const loadingRef = useRef(true);
  const [form] = Form.useForm();
  const createFormId = useId();
  const [createOpen, setCreateOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const createPending = useRef(false);
  // Closing modal content can still submit before it unmounts.
  const createDraftActive = useRef(false);
  const [createLifecycleExpanded, setCreateLifecycleExpanded] = useState(false);
  const [hasCustomCreateLifecycle, setHasCustomCreateLifecycle] = useState(false);
  const [creationWarning, setCreationWarning] = useState<{ sandboxId: string; error: string } | null>(null);
  const [lifecycleForm] = Form.useForm();
  const [editingLifecycleSandbox, setEditingLifecycleSandbox] = useState<Sandbox | null>(null);
  const [editingNetworkSandbox, setEditingNetworkSandbox] = useState<Sandbox | null>(null);
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const [refreshCooldown, setRefreshCooldown] = useState(false);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const refreshCooldownRef = useRef(false);
  const refreshCooldownTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Filter state is hydrated once from the URL on mount and then written back
  // on every change so the page is deep-linkable / shareable. We use
  // `replaceState` rather than `router.replace` to avoid Next's full
  // navigation cycle (no refetch, no re-render of the route tree) and to keep
  // the browser history clean — filter twiddling shouldn't pile up entries
  // the user has to step through.
  const searchParams = useSearchParams();
  const pathname = usePathname();

  const [idFilter, setIdFilter] = useState(() => searchParams.get('id') ?? '');
  const [templateFilter, setTemplateFilter] = useState<string[]>(() => parseCsvParam(searchParams.get('template')));
  const [statusFilter, setStatusFilter] = useState<string[]>(() => parseCsvParam(searchParams.get('status')));
  const [nodeFilter, setNodeFilter] = useState<NodeFilter>(() => parseNodeParam(searchParams.get('node')));
  const [ageWindow, setAgeWindow] = useState<AgeWindow>(() => parseAgeParam(searchParams.get('age')));

  useEffect(() => {
    const params = new URLSearchParams();
    if (idFilter.trim()) params.set('id', idFilter.trim());
    if (templateFilter.length) params.set('template', templateFilter.join(','));
    if (statusFilter.length) params.set('status', statusFilter.join(','));
    if (nodeFilter !== DEFAULT_NODE_FILTER) params.set('node', nodeFilter);
    if (ageWindow !== DEFAULT_AGE_WINDOW) params.set('age', ageWindow);
    const qs = params.toString();
    const next = qs ? `${pathname}?${qs}` : pathname;
    window.history.replaceState(null, '', next);
  }, [idFilter, templateFilter, statusFilter, nodeFilter, ageWindow, pathname]);

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

  const parseUptime = (s: string | null) => {
    if (!s) return 0;
    let secs = 0;
    const d = s.match(/(\d+)d/); if (d) secs += +d[1] * 86400;
    const h = s.match(/(\d+)h/); if (h) secs += +h[1] * 3600;
    const m = s.match(/(\d+)m/); if (m) secs += +m[1] * 60;
    const sec = s.match(/(\d+)s/); if (sec) secs += +sec[1];
    return secs;
  };

  const refresh = async ({ showLoading = true, reconcile = false }: { showLoading?: boolean; reconcile?: boolean } = {}) => {
    if (showLoading) setLoading(true);
    try {
      const data = await getConsoleSandboxes();
      data.sort((a, b) => parseUptime(b.uptime) - parseUptime(a.uptime) || a.id.localeCompare(b.id));
      setSandboxes(data);
      markViewRefreshed(SANDBOXES_VIEW_REFRESH_KEY);
    } catch (err: any) {
      // Keep the last rendered view when a node-local manager is briefly unavailable.
      throw new Error(err.message || 'failed to refresh sandboxes');
    } finally {
      if (showLoading) setLoading(false);
    }
  };

  const refreshAndReport = () => {
    void refresh().catch((err: any) => message.error(err.message || 'failed to refresh sandboxes'));
  };

  const maybeRefresh = () => {
    if (loadingRef.current) {
      return;
    }
    if (document.hidden) {
      return;
    }
    void runLockedViewRefresh(
      SANDBOXES_VIEW_REFRESH_KEY,
      () => refresh({ showLoading: false }),
      { staleMs: VIEW_REFRESH_STALE_MS },
    ).catch((err: any) => console.warn(err.message || err));
  };

  useEffect(() => {
    loadingRef.current = loading;
  }, [loading]);

  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then(setOrgs).catch(() => setOrgs([]));
  }, []);

  useEffect(() => {
    void refresh().catch((err: any) => message.error(err.message));
    getPublicPage<PublicTemplate>('/templates?limit=1000').then((items) => {
      const t = items.map(publicTemplateToConsoleTemplate);
      setTemplates(t);
      if (t.length > 0) form.setFieldsValue({ template: t[0].name });
    }).catch((err: any) => message.error(err.message || 'failed to load templates'));
    const interval = setInterval(maybeRefresh, VIEW_REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const interval = setInterval(() => setNowMs(Date.now()), 60_000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => () => {
    if (refreshCooldownTimerRef.current) {
      clearTimeout(refreshCooldownTimerRef.current);
    }
  }, []);

  const handleManualRefresh = () => {
    if (refreshCooldownRef.current || manualRefreshing) return;
    startRefreshCooldown();
    setManualRefreshing(true);
    void runLockedViewRefresh(
      SANDBOXES_VIEW_REFRESH_KEY,
      () => refresh({ showLoading: false, reconcile: true }),
      { force: true },
    ).then((ran) => {
      if (!ran) {
        message.info('Refresh already in progress');
        return;
      }
      message.success('Sandboxes refreshed');
    }).catch((err: any) => {
      message.error(err.message);
    }).finally(() => {
      setManualRefreshing(false);
    });
  };

  const resetCreateDraft = () => {
    form.resetFields();
    form.setFieldsValue({ runtime: 'fc', template: templates[0]?.name });
    setHasCustomCreateLifecycle(false);
    setCreateLifecycleExpanded(false);
  };

  const closeCreate = () => {
    if (createPending.current) return;
    createDraftActive.current = false;
    setCreateOpen(false);
    resetCreateDraft();
  };

  const handleCreate = async (values: {
    name?: string;
    org_id?: string;
    template: string;
    runtime: string;
    boot_mode?: string;
    snapshots?: boolean;
    auto_lifecycle?: AutoLifecycleFormValue;
  }) => {
    if (!createDraftActive.current || createPending.current) return;
    createPending.current = true;
    setCreating(true);
    try {
      const payload = {
        name: values.name || null,
        template: values.template,
        runtime: values.runtime,
        env: {},
        metadata: {},
        resources: null,
        auto_lifecycle: hasCustomCreateLifecycle ? formValueToPolicy(values.auto_lifecycle) : undefined,
      };
      // Cross-org creates go through the admin surface; the public path
      // only ever creates under the signed-in admin's own org.
      const targetOrgId =
        values.org_id && (!authOrg || values.org_id !== authOrg.id) ? values.org_id : null;
      const sandbox = targetOrgId
        ? await createConsoleSandboxForOrg(targetOrgId, payload)
        : await postPublicOperation('/sandboxes', payload);
      createDraftActive.current = false;
      let snapshotFailure: string | undefined;
      try {
        if (values.snapshots) {
          if (targetOrgId) {
            await waitForAdminOperation(sandbox.operation_id);
            await forceConsoleSandboxAction(sandbox.sandbox_id, 'enable_snapshots');
          } else {
            await waitForPublicOperation(sandbox.operation_id);
            await postPublicOperation(`/sandboxes/${sandbox.sandbox_id}:enable_snapshots`);
          }
        }
      } catch (err: any) {
        snapshotFailure = err.message || 'Snapshot setup failed';
      }
      // An accepted create must not remain available for retry after a follow-up failure.
      setCreateOpen(false);
      resetCreateDraft();
      refreshAndReport();
      if (snapshotFailure) {
        setCreationWarning({ sandboxId: sandbox.sandbox_id, error: snapshotFailure });
      } else {
        message.success({
          content: (
            <Space size={6}>
              <span>Sandbox created</span>
              <SandboxReceipt sandboxId={sandbox.sandbox_id} />
            </Space>
          ),
          duration: 8,
        });
      }
    } catch (err: any) {
      message.error(err.message);
    } finally {
      createPending.current = false;
      setCreating(false);
    }
  };

  const openLifecycleModal = (sandboxId: string) => {
    const sandbox = sandboxes.find((entry) => entry.id === sandboxId);
    if (!sandbox) return;
    lifecycleForm.setFieldsValue({
      auto_lifecycle: policyToFormValue(sandbox.auto_lifecycle),
    });
    setEditingLifecycleSandbox(sandbox);
  };

  const closeLifecycleModal = () => {
    setEditingLifecycleSandbox(null);
    lifecycleForm.resetFields();
  };

  const handleLifecycleUpdate = async (values: {
    auto_lifecycle?: unknown;
  }) => {
    if (!editingLifecycleSandbox) return;
    try {
      await updateConsoleSandbox(editingLifecycleSandbox.id, {
        auto_lifecycle: {
          mode: 'set',
          policy: formValueToPolicy(values.auto_lifecycle as any),
        },
      });
      closeLifecycleModal();
      refreshAndReport();
      message.success('Sandbox lifecycle policy update queued');
    } catch (err: any) {
      message.error(err.message || 'failed to update lifecycle policy');
    }
  };

  const handleLifecycleClear = async () => {
    if (!editingLifecycleSandbox) return;
    try {
      await updateConsoleSandbox(editingLifecycleSandbox.id, {
        auto_lifecycle: { mode: 'inherit_template_default' },
      });
      closeLifecycleModal();
      refreshAndReport();
      message.success('Sandbox lifecycle policy reset queued');
    } catch (err: any) {
      message.error(err.message || 'failed to reset lifecycle policy');
    }
  };

  const handleAction = async (id: string, action: string) => {
    try {
      const operation = await forceConsoleSandboxAction(id, action);
      message.success(action === 'delete' ? 'Sandbox delete queued' : `Sandbox ${action} queued`);
      await waitForAdminOperation(operation.operation_id);
      message.success(action === 'delete' ? 'Sandbox deleted' : `Sandbox ${action} completed`);
      refreshAndReport();
    } catch (err: any) {
      message.error(err.message);
    }
  };

  const handleToggleSnapshots = async (id: string, enable: boolean) => {
    try {
      await forceConsoleSandboxAction(id, enable ? 'enable_snapshots' : 'disable_snapshots');
      refreshAndReport();
      message.success(`Snapshot ${enable ? 'enable' : 'disable'} queued`);
    } catch (err: any) {
      message.error(err.message || 'failed to update snapshot policy');
    }
  };

  const templatePolicyForSandbox = (sandbox: Sandbox | null) => {
    if (sandbox?.template_network_policy) return sandbox.template_network_policy;
    if (!sandbox?.template) return null;
    return templates.find((template) => template.name === sandbox.template)?.network_policy ?? null;
  };

  const effectiveNetworkPolicy = (sandbox: Sandbox) =>
    sandbox.effective_desired_network_policy ??
    sandbox.network_policy_override ??
    sandbox.network_policy ??
    templatePolicyForSandbox(sandbox);

  const handleNetworkSave = async (sandbox: Sandbox, policy: NetworkPolicyV1) => {
    try {
      await updateConsoleSandbox(sandbox.id, {
        network_policy: { mode: 'set', policy },
      });
      setEditingNetworkSandbox(null);
      refreshAndReport();
      message.success('Sandbox network policy update queued');
    } catch (err: any) {
      message.error(err.message || 'failed to update network policy');
    }
  };

  const handleNetworkClear = async (sandbox: Sandbox) => {
    try {
      await updateConsoleSandbox(sandbox.id, {
        network_policy: { mode: 'inherit_template_default' },
      });
      setEditingNetworkSandbox(null);
      refreshAndReport();
      message.success('Sandbox network policy reset queued');
    } catch (err: any) {
      message.error(err.message || 'failed to reset network policy');
    }
  };

  const filteredSandboxes = useMemo(() => {
    const idQuery = idFilter.trim().toLowerCase().replaceAll('-', '');
    const ageMs = AGE_WINDOW_OPTIONS.find((opt) => opt.value === ageWindow)?.ms ?? null;
    const cutoff = ageMs === null ? null : Date.now() - ageMs;

    return sandboxes.filter((s) => {
      if (idQuery) {
        const compactId = s.id.replaceAll('-', '').toLowerCase();
        const nameMatch = (s.name || '').toLowerCase().includes(idFilter.trim().toLowerCase());
        if (!compactId.includes(idQuery) && !nameMatch) return false;
      }

      if (templateFilter.length > 0 && !templateFilter.includes(s.template || '')) {
        return false;
      }

      if (statusFilter.length > 0 && !statusFilter.includes(s.status || '')) {
        return false;
      }

      if (nodeFilter !== 'any') {
        const hasNode = !!s.active_node_id;
        if (nodeFilter === 'assigned' && !hasNode) return false;
        if (nodeFilter === 'unassigned' && hasNode) return false;
      }

      if (cutoff !== null) {
        if (!s.created_at) return false;
        const created = Date.parse(s.created_at);
        if (Number.isNaN(created) || created < cutoff) return false;
      }

      return true;
    });
  }, [sandboxes, idFilter, templateFilter, statusFilter, nodeFilter, ageWindow]);

  const templateFilterOptions = useMemo(() => {
    const set = new Set<string>();
    for (const s of sandboxes) {
      if (s.template) set.add(s.template);
    }
    for (const t of templates) {
      set.add(t.name);
    }
    return Array.from(set).sort().map((name) => ({ value: name, label: name }));
  }, [sandboxes, templates]);

  const clearFilters = () => {
    setIdFilter('');
    setTemplateFilter([]);
    setStatusFilter([]);
    setNodeFilter('any');
    setAgeWindow('24h');
  };

  const filtersActive =
    !!idFilter ||
    templateFilter.length > 0 ||
    statusFilter.length > 0 ||
    nodeFilter !== 'any' ||
    ageWindow !== '24h';

  const columns: ColumnsType<Sandbox> = [
    {
      title: 'ID',
      dataIndex: 'id',
      width: 90,
      render: (id: string) => (
        <Link href={`/dashboard/sandboxes/${id}`}>
          <code>{sandboxLabelOrId(id)}</code>
        </Link>
      ),
    },
    { title: 'Runtime', dataIndex: 'runtime', width: 76, render: (v: string) => v || 'fc' },
    {
      title: 'State',
      dataIndex: 'status',
      width: 96,
      render: (status: string, record) => (
        <Space size={2} wrap>
          {sandboxResourceTag(record)}
          {statusTag(status, record.failure_reason)}
          {encryptedTag(record)}
        </Space>
      ),
    },
    {
      title: 'Node',
      width: 96,
      render: (_, record) => sandboxNodeDisplay(record),
    },
    {
      title: 'Template',
      dataIndex: 'template',
      width: 120,
      render: (v: string, record) => {
        if (!v) return '—';
        const contentHash = record.content_hash || record.image_hash;
        return contentHash ? (
          <Tooltip title={templateTooltip(record, contentHash)}>
            <span style={{ cursor: 'help' }}>{v}</span>
          </Tooltip>
        ) : v;
      },
    },
    {
      title: 'Lifecycle',
      width: 88,
      align: 'center',
      render: (_, record) => {
        const configured = hasAutoLifecyclePolicy(record.auto_lifecycle);
        const nextAuto = formatNextAutoEvent(record.auto_lifecycle_status);
        return (
          <Tooltip
            title={
              nextAuto === '—'
                ? configured
                  ? 'Manage lifecycle policy'
                  : 'Set lifecycle policy'
                : `Next auto: ${nextAuto}`
            }
          >
            <Button
              type="text"
              size="small"
              icon={<ClockCircleOutlined />}
              aria-label={configured ? 'Manage lifecycle policy' : 'Set lifecycle policy'}
              onClick={() => openLifecycleModal(record.id)}
            />
          </Tooltip>
        );
      },
    },
    {
      title: 'Network',
      width: 118,
      render: (_, record) => {
        const effective = effectiveNetworkPolicy(record);
        return (
          <Space size={2} wrap>
            <NetworkPolicyGenerationTag
              desired={record.network_policy_generation}
              applied={record.network_policy_status?.observed_generation}
              policyLabel={compactNetworkPolicyLabel(effective)}
              desiredSource={record.network_policy_override ? 'override' : 'template'}
              observedSource={record.network_policy_status?.observed_source}
            />
            <Tooltip title="Manage network policy">
              <Button
                type="text"
                size="small"
                icon={<GlobalOutlined />}
                aria-label="Manage network policy"
                onClick={() => setEditingNetworkSandbox(record)}
              />
            </Tooltip>
          </Space>
        );
      },
    },
    { title: 'PID', dataIndex: 'pid', width: 65, render: (v: number) => v ? <code>{v}</code> : '—' },
    { title: 'Disk', dataIndex: 'disk_usage_bytes', width: 70, render: (v: number | null) => formatBytes(v) },
    { title: 'Age', dataIndex: 'age', width: 55, render: (v: string) => v || '—' },
    {
      title: 'Activity',
      width: 116,
      render: (_, record) => activityTimeCell(record, nowMs),
    },
    { title: 'User', dataIndex: 'user', width: 95, ellipsis: true, render: (v: string) => v || '—' },
    {
      title: 'Snaps',
      width: 70,
      render: (_, record) => {
        return (
          <Tooltip title={snapshotsToggleTitle(record)}>
            <Switch
              size="small"
              checked={!isPoolSandbox(record) && !!record.snapshots_enabled_desired}
              disabled={isPoolSandbox(record)}
              onChange={(checked) => handleToggleSnapshots(record.id, checked)}
            />
          </Tooltip>
        );
      },
    },
    {
      title: 'Actions',
      fixed: 'right',
      width: 122,
      render: (_, record) => (
        <SandboxActions
          status={record.status}
          id={record.id}
          resourceType={record.resource_type}
          poolTemplate={record.pool_template}
          onAction={handleAction}
        />
      ),
    },
  ];

  return (
    <div className={styles.inventory}>
      <Space align="center" wrap style={{ width: '100%', justifyContent: 'space-between', marginBottom: 24 }}>
        <Title level={3} style={{ margin: 0 }}>Sandboxes</Title>
        <Space wrap>
          <Button
            icon={<ReloadOutlined />}
            loading={manualRefreshing}
            disabled={refreshCooldown}
            onClick={handleManualRefresh}
          >
            Refresh
          </Button>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            disabled={creating}
            onClick={() => {
              if (createPending.current) return;
              resetCreateDraft();
              createDraftActive.current = true;
              setCreateOpen(true);
            }}
          >
            Create sandbox
          </Button>
        </Space>
      </Space>

      {creationWarning ? (
        <Alert
          type="warning"
          showIcon
          closable
          onClose={() => setCreationWarning(null)}
          title="Sandbox creation accepted; snapshot setup incomplete"
          description={(
            <Space orientation="vertical" size={4}>
              <span>{creationWarning.error}</span>
              <SandboxReceipt sandboxId={creationWarning.sandboxId} />
            </Space>
          )}
          style={{ marginBottom: 16 }}
        />
      ) : null}

      <Modal
        title="Create sandbox"
        open={createOpen}
        onCancel={closeCreate}
        okText="Create sandbox"
        okButtonProps={{ htmlType: 'submit', form: createFormId }}
        confirmLoading={creating}
        cancelButtonProps={{ disabled: creating }}
        closable={!creating}
        mask={{ closable: !creating }}
        keyboard={!creating}
        destroyOnHidden
        width={760}
        styles={{ body: { maxHeight: '70vh', overflowY: 'auto' } }}
      >
        <Form
          id={createFormId}
          form={form}
          layout="vertical"
          onFinish={handleCreate}
          initialValues={{ runtime: 'fc' }}
          disabled={creating}
          preserve={false}
          onValuesChange={(changed) => {
            if ('auto_lifecycle' in changed) setHasCustomCreateLifecycle(true);
          }}
          onFinishFailed={({ errorFields }) => {
            if (errorFields.some(({ name }) => name[0] === 'auto_lifecycle')) setCreateLifecycleExpanded(true);
          }}
        >
          {orgs.length > 1 && (
            <Form.Item label="Organization" name="org_id">
              <Select
                placeholder="Org: mine"
                allowClear
                options={orgs.map((org) => ({
                  label: authOrg && org.id === authOrg.id ? `${org.name} (mine)` : org.name,
                  value: org.id,
                }))}
              />
            </Form.Item>
          )}
          <Form.Item label="Name (optional)" name="name">
            <Input />
          </Form.Item>
          <Form.Item label="Runtime" name="runtime">
            <Select>
              <Select.Option value="fc">Firecracker</Select.Option>
              <Select.Option value="gvisor" disabled>gVisor</Select.Option>
              <Select.Option value="ch" disabled>Cloud Hypervisor</Select.Option>
              <Select.Option value="hyperv" disabled>Hyper-V</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item label="Template" name="template" rules={[{ required: true }]}>
            <Select placeholder="Template">
              {templates.map((t) => (
                <Select.Option key={t.name} value={t.name}>{t.name}</Select.Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item label="Boot mode" name="boot_mode">
            <Select placeholder="Boot mode" allowClear>
              <Select.Option value="cold">Cold boot</Select.Option>
              <Select.Option value="snapshot">Snapshot</Select.Option>
              <Select.Option value="pool">Pool</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item name="snapshots" valuePropName="checked">
            <Checkbox>Snapshots</Checkbox>
          </Form.Item>
          <Collapse
            size="small"
            activeKey={createLifecycleExpanded ? ['lifecycle'] : []}
            onChange={(keys) => setCreateLifecycleExpanded(keys.includes('lifecycle'))}
            collapsible={creating ? 'disabled' : undefined}
            items={[{
              key: 'lifecycle',
              label: 'Custom Auto Lifecycle',
              forceRender: true,
              children: (
                <Space orientation="vertical" size={12} style={{ width: '100%' }}>
                  <Space wrap>
                    <Text type="secondary">
                      {hasCustomCreateLifecycle ? 'Custom policy overrides the template.' : 'Uses the template policy until customized.'}
                    </Text>
                    <Button
                      size="small"
                      danger
                      disabled={!hasCustomCreateLifecycle || creating}
                      onClick={() => {
                        form.resetFields([['auto_lifecycle']]);
                        setHasCustomCreateLifecycle(false);
                      }}
                    >
                      Clear
                    </Button>
                  </Space>
                  <AutoLifecycleEditor prefix={['auto_lifecycle']} disabled={creating} />
                </Space>
              ),
            }]}
          />
        </Form>
      </Modal>

      <Card size="small" style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <Text strong>Filters</Text>
          <Input
            allowClear
            size="small"
            placeholder="ID or name"
            value={idFilter}
            onChange={(e) => setIdFilter(e.target.value)}
            style={{ width: 180 }}
          />
          <Select
            mode="multiple"
            size="small"
            placeholder="Template"
            value={templateFilter}
            onChange={(value) => setTemplateFilter(value as string[])}
            options={templateFilterOptions}
            allowClear
            style={{ minWidth: 180 }}
            maxTagCount="responsive"
          />
          <Select
            mode="multiple"
            size="small"
            placeholder="Status"
            value={statusFilter}
            onChange={(value) => setStatusFilter(value as string[])}
            options={STATUS_FILTER_OPTIONS.map((s) => ({ value: s, label: s }))}
            allowClear
            style={{ minWidth: 180 }}
            maxTagCount="responsive"
          />
          <Select
            size="small"
            value={nodeFilter}
            onChange={(value) => setNodeFilter(value as NodeFilter)}
            options={[
              { value: 'any', label: 'Node: any' },
              { value: 'assigned', label: 'Node: assigned' },
              { value: 'unassigned', label: 'Node: unassigned' },
            ]}
            style={{ width: 170 }}
          />
          <Select
            size="small"
            value={ageWindow}
            onChange={(value) => setAgeWindow(value as AgeWindow)}
            options={AGE_WINDOW_OPTIONS.map((opt) => ({ value: opt.value, label: opt.label }))}
            style={{ width: 150 }}
          />
          <Text type="secondary" style={{ fontSize: 12 }}>
            {filteredSandboxes.length} / {sandboxes.length}
          </Text>
          {filtersActive ? (
            <Button size="small" type="link" onClick={clearFilters}>Reset</Button>
          ) : null}
        </div>
      </Card>

      <Table
        columns={columns}
        dataSource={filteredSandboxes}
        rowKey="id"
        loading={loading}
        size="small"
        pagination={tablePagination('sandboxes')}
        scroll={{ x: 1280 }}
        expandable={{
          showExpandColumn: false,
          expandRowByClick: true,
          expandedRowRender: (record) => <SnapshotsPanel sandboxId={record.id} />,
          rowExpandable: snapshotsPanelAvailable,
        }}
      />

      {editingNetworkSandbox ? (
        <NetworkPolicyDesigner
          open={!!editingNetworkSandbox}
          targetKind="sandbox"
          targetName={sandboxLabelOrId(editingNetworkSandbox.id)}
          templateName={editingNetworkSandbox.template}
          policy={editingNetworkSandbox.network_policy_override}
          templatePolicy={templatePolicyForSandbox(editingNetworkSandbox)}
          onClose={() => setEditingNetworkSandbox(null)}
          onSave={(policy) => handleNetworkSave(editingNetworkSandbox, policy)}
          onClear={() => handleNetworkClear(editingNetworkSandbox)}
        />
      ) : null}

      <Modal
        title={editingLifecycleSandbox ? `Auto Lifecycle: ${sandboxLabelOrId(editingLifecycleSandbox.id)}` : 'Auto Lifecycle'}
        open={!!editingLifecycleSandbox}
        onCancel={closeLifecycleModal}
        footer={[
          <Button key="clear" danger onClick={handleLifecycleClear}>Clear</Button>,
          <Button key="cancel" onClick={closeLifecycleModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => lifecycleForm.submit()}>Save</Button>,
        ]}
        destroyOnHidden
        width={760}
      >
        <Form form={lifecycleForm} layout="vertical" onFinish={handleLifecycleUpdate}>
          <AutoLifecycleEditor prefix={['auto_lifecycle']} />
        </Form>
      </Modal>
    </div>
  );
}
