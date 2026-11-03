'use client';

import { Suspense, useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import styles from './inventory.module.css';
import { lookupScopeText, orgOptionLabels, parseOrgParam, scopedRefreshKey } from '@/lib/org-filter';
import { usePathname, useSearchParams } from 'next/navigation';
import {
  STORED_LIFECYCLE_FILTER_STATES,
  bringUpAction,
  chooseAppliedStatuses,
  splitStatusFilter,
  statusFilterKey,
  statusFilterOptions,
  storedLifecycles,
} from '@/lib/sandbox-lifecycle';
import {
  DEFAULT_LIST_LIMIT,
  LIST_LIMIT_OPTIONS,
  listIsCapped,
  matchesSandboxQuery,
  parseListLimit,
  sandboxLabelOrId,
  sandboxQuery,
  sandboxSearch,
} from '@/lib/sandbox-id';
import {
  createConsoleSandboxForOrg,
  forceConsoleSandboxAction,
  getConsoleSandbox,
  getConsoleSandboxList,
  getConsoleSnapshots,
  getPublicPage,
  postPublicOperation,
  publicTemplateToConsoleTemplate,
  updateConsoleSandbox,
  waitForAdminOperation,
  waitForPublicOperation,
} from '@/lib/public-api';
import { adminApi } from '@/lib/api';
import {
  CREATED_AGES,
  FIRST_PAGE,
  ListSelection,
  OlderFormatAnswer,
  PAGE_SIZE_OPTIONS,
  createdAgeMs,
  fragmentSearchScope,
  listRequestQuery,
  listSections,
  loadedRowFilter,
  nodeReportedPlacement,
  parseListLink,
  parsePageSize,
  sameScope,
  templateFilterOptions,
  type AgeWindow,
  type CreatedAge,
  type ListMode,
  type ListScope,
  type NodeFilter,
  type OlderThan,
  type PagePosition,
  type SandboxListFilters,
} from '@/lib/sandbox-list';
import { OperationFailedError, createFailureText, settleAcceptedCreate } from '@/lib/accepted-create';
import {
  SNAPSHOTS_NOT_ENABLED,
  SNAPSHOTS_OTHER_ORG_UNSUPPORTED,
  isHiddenOperationError,
} from '@/lib/hidden-operations';
import { PrefixRequests, supplementalMatches } from '@/lib/prefix-search';
import { useAuth } from '@/lib/auth';
import { isOwnOrg } from '@/lib/own-org';
import {
  AutoLifecyclePolicy,
  NetworkPolicyV1,
  Organization,
  PublicTemplate,
  Sandbox,
  SnapshotInfo,
  Template,
} from '@/lib/types';
import { formatBytes } from '@/lib/sandbox-debug';
import { tablePagination } from '@/lib/table-pagination';
import {
  markViewRefreshed,
  runLockedViewRefresh,
  VIEW_REFRESH_INTERVAL_MS,
  VIEW_REFRESH_STALE_MS,
} from '@/lib/view-refresh';
import NetworkPolicyDesigner, {
  compactNetworkPolicyLabel,
} from '@/components/network-policy-designer';
import NetworkPolicyGenerationTag from '@/components/network-policy-generation-tag';
import {
  AutoLifecycleEditor,
  disabledAutoLifecycle,
  formValueToPolicy,
  formatNextAutoEvent,
  policyToFormValue,
  summarizeAutoLifecycle,
} from '@/components/auto-lifecycle';
import {
  Alert, App, Table, Button, Form, Input, Select, Typography, Space, Tag, Popconfirm, Card, Tooltip, Switch, Checkbox, Modal,
} from 'antd';
import {
  PlusOutlined, StopOutlined, PauseCircleOutlined, PlayCircleOutlined,
  SaveOutlined, DeleteOutlined, CameraOutlined, RollbackOutlined, ReloadOutlined,
  CopyOutlined, ClockCircleOutlined, GlobalOutlined, LockOutlined, LeftOutlined, RightOutlined,
  QuestionCircleOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

const { Title, Text } = Typography;
const SANDBOXES_VIEW_REFRESH_KEY = 'dashboard:sandboxes';
const LARGE_LIST_STALE_MS = 15_000;

// See `scopedRefreshKey`: each org and load size of a sampled list keeps its
// own timestamp, and so does each page of a paged list, with its filters.
// The key holds relative creation-time bounds as chosen, so it does not
// change from one poll to the next.
function sandboxesRefreshKey(scope: ListScope): string {
  return scope.mode === 'sampled'
    ? scopedRefreshKey(SANDBOXES_VIEW_REFRESH_KEY, scope.limit, DEFAULT_LIST_LIMIT, scope.filters.org)
    : `${SANDBOXES_VIEW_REFRESH_KEY}:paged:${scope.key}`;
}

// A refresh that loaded nothing, and did not mark the view's key fresh: its
// page, org, size or filter is no longer selected.
class RefreshSkipped extends Error {}
const SCOPE_CHANGED = 'the selected page, filters or size changed';
const VERSION_CHANGED = 'the API changed version while this page was open';
// A load error, a stopped list or an older-format answer, which the page
// already shows above the table.
class ShownListError extends Error {}
const alreadyReported = (err: unknown) => err instanceof RefreshSkipped || err instanceof ShownListError;
const REFRESH_CLICK_COOLDOWN_MS = 3000;

// No age limit by default: long-running sandboxes are the ones support
// cases tend to be about.
const DEFAULT_AGE_WINDOW: AgeWindow = 'all';
const DEFAULT_NODE_FILTER: NodeFilter = 'any';

const AGE_WINDOW_OPTIONS: { value: AgeWindow; label: string; ms: number | null }[] = [
  { value: '1h',  label: 'Last 1h',     ms: CREATED_AGES['1h'] },
  { value: '24h', label: 'Last 24h',    ms: CREATED_AGES['24h'] },
  { value: '7d',  label: 'Last 7 days', ms: CREATED_AGES['7d'] },
  { value: '30d', label: 'Last 30 days',ms: CREATED_AGES['30d'] },
  { value: '90d', label: 'Last 90 days',ms: CREATED_AGES['90d'] },
  { value: '1y',  label: 'Last year',   ms: CREATED_AGES['1y'] },
  { value: 'all', label: 'All time',    ms: null },
];

// "Created more than ... ago", the other end of the creation-time range.
const OLDER_THAN_OPTIONS: { value: OlderThan; label: string; ms: number | null }[] = [
  { value: 'any', label: 'Older than: any', ms: null },
  ...AGE_WINDOW_OPTIONS.filter((opt) => opt.ms !== null).map((opt) => ({
    value: opt.value as OlderThan,
    label: `Older than ${opt.label.replace(/^Last /, '')}`,
    ms: opt.ms,
  })),
];
const ageWindowAge = (value: AgeWindow): CreatedAge | null => (value === 'all' ? null : value);
const olderThanAge = (value: OlderThan): CreatedAge | null => (value === 'any' ? null : value);
const ageWindowMs = (value: AgeWindow) => createdAgeMs(ageWindowAge(value));
const olderThanMs = (value: OlderThan) => createdAgeMs(olderThanAge(value));

// What the server-side filters are built from.
interface FilterInputs {
  org: string | null;
  statuses: string[];
  template: string | null;
  user: string | null;
  age: AgeWindow;
  older: OlderThan;
  // The search box as a paging server's prefix; see `sandboxSearch`. A
  // sampled request does not send it.
  prefix: string;
}

function serverFilters(inputs: FilterInputs): SandboxListFilters {
  return {
    org: inputs.org,
    lifecycles: storedLifecycles(inputs.statuses),
    template: inputs.template,
    userId: inputs.user,
    createdWithin: ageWindowAge(inputs.age),
    olderThan: olderThanAge(inputs.older),
    prefix: inputs.prefix,
  };
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
        <Space orientation="vertical" size={0}>
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
  stopped:   { wake: true,  stop: false, pause: false, suspend: false, delete: true },
  paused:    { wake: true,  stop: false, pause: false, suspend: false, delete: true },
  suspended: { wake: true,  stop: false, pause: false, suspend: false, delete: true },
  deleting:  { wake: false, stop: false, pause: false, suspend: false, delete: false },
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

  // The node page lists the sandboxes placed on the node, which the list's
  // node filter cannot: the server has no node filter.
  return (
    <Tooltip title={sandboxNodeTooltip(sandboxNodeTooltipRows(node, 'active'))}>
      <Link href={`/dashboard/nodes/${node.id}`} onClick={(event) => event.stopPropagation()}>
        {tag}
      </Link>
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
  const bringUp = managedSandbox ? bringUpAction(status) : null;

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
      {actions.wake && bringUp
        ? btn(bringUp.action, true, bringUp.action === 'restore' ? <RollbackOutlined /> : <PlayCircleOutlined />, bringUp.label)
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

// The operator's own organization's snapshots list through the public routes;
// another organization's through the admin list (see `listSnapshots`). The
// list is display-only: rollback restores a snapshot the sandbox's node keeps,
// which only the sandbox page shows (`rollbackTargets`). The panel lists when
// the row expands and on its Refresh; a release without the admin list lists
// no other organization's, until a Refresh finds one that has it.
function SnapshotsPanel({ sandboxId, ownOrg }: { sandboxId: string; ownOrg: boolean }) {
  const { message } = App.useApp();
  const [snapshots, setSnapshots] = useState<SnapshotInfo[]>([]);
  const [more, setMore] = useState(false);
  const [loading, setLoading] = useState(true);
  const [snapshotsOff, setSnapshotsOff] = useState<string | null>(null);
  // Only the newest listing may write: the row's sandbox or organization can
  // change while a listing is in flight.
  const listingRequest = useRef(0);

  const list = () => {
    const request = ++listingRequest.current;
    setLoading(true);
    getConsoleSnapshots(sandboxId, { ownOrg })
      .then((listing) => {
        if (request !== listingRequest.current) return;
        if (listing.via === 'none') {
          setSnapshotsOff(SNAPSHOTS_OTHER_ORG_UNSUPPORTED);
          return;
        }
        setSnapshotsOff(null);
        setSnapshots(listing.rows);
        setMore(listing.via === 'admin' && listing.more);
      })
      .catch((err: any) => {
        if (request !== listingRequest.current) return;
        if (ownOrg && isHiddenOperationError(err)) {
          setSnapshotsOff(SNAPSHOTS_NOT_ENABLED);
          return;
        }
        message.error(err.message || 'failed to load snapshots');
      })
      .finally(() => {
        if (request === listingRequest.current) setLoading(false);
      });
  };

  useEffect(list, [sandboxId, ownOrg]);
  // Refresh: a panel that showed why it has no listing asks again.
  const relist = () => {
    setSnapshotsOff(null);
    list();
  };
  const refreshButton = (
    <Button size="small" icon={<ReloadOutlined />} loading={loading} onClick={relist}>Refresh</Button>
  );

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
  ];

  if (snapshotsOff) {
    return (
      <div style={{ padding: '8px 0' }}>
        <Space size={12} wrap>
          <Text strong><CameraOutlined /> Snapshots</Text>
          {refreshButton}
        </Space>
        <div style={{ marginTop: 8 }}>
          <Text type="secondary">{snapshotsOff}</Text>
        </div>
      </div>
    );
  }

  return (
    <div style={{ padding: '8px 0' }}>
      <Space size={12} wrap>
        <Text strong><CameraOutlined /> Snapshots</Text>
        {refreshButton}
        <Link href={`/dashboard/sandboxes/${sandboxId}`}>
          <Text type="secondary" style={{ fontSize: 12 }}>Roll back from the sandbox page</Text>
        </Link>
      </Space>
      <Table
        columns={snapshotColumns}
        dataSource={snapshots}
        rowKey="id"
        loading={loading}
        size="small"
        pagination={tablePagination('snapshots', 10)}
        style={{ marginTop: 8 }}
        locale={{ emptyText: 'No snapshots' }}
        footer={more ? () => <Text type="secondary" style={{ fontSize: 12 }}>The newest {snapshots.length} snapshots; older ones are not listed.</Text> : undefined}
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

function SandboxesPage() {
  const { message } = App.useApp();
  const [sandboxes, setSandboxes] = useState<Sandbox[]>([]);
  const [templates, setTemplates] = useState<Template[]>([]);
  // The admin template list's names, for the template filter: undefined
  // while loading, null when it could not be read.
  const [adminTemplateNames, setAdminTemplateNames] = useState<string[] | null | undefined>(undefined);
  const [templateSearch, setTemplateSearch] = useState('');
  const { org: authOrg } = useAuth();
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [loading, setLoading] = useState(true);
  const loadingRef = useRef(true);
  const [form] = Form.useForm();
  const [createLifecycleForm] = Form.useForm();
  const [createLifecycleModalOpen, setCreateLifecycleModalOpen] = useState(false);
  const [customCreateAutoLifecycle, setCustomCreateAutoLifecycle] = useState<AutoLifecyclePolicy | null>(null);
  const [lifecycleForm] = Form.useForm();
  const [editingLifecycleSandbox, setEditingLifecycleSandbox] = useState<Sandbox | null>(null);
  const [editingNetworkSandbox, setEditingNetworkSandbox] = useState<Sandbox | null>(null);
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const [creating, setCreating] = useState(false);
  const creatingRef = useRef(false);
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

  // Only what the list can apply is taken from the link; the rest is listed
  // in a notice, and the rewritten URL drops it.
  const [link] = useState(() => {
    const parsed = parseListLink(searchParams);
    const rawOrg = searchParams.get('org')?.trim();
    if (rawOrg && !parseOrgParam(rawOrg)) parsed.ignored.push({ param: 'org', value: rawOrg, reason: 'not an organization ID' });
    return parsed;
  });
  const [linkNoticeOpen, setLinkNoticeOpen] = useState(link.ignored.length > 0);
  const [idFilter, setIdFilter] = useState(() => searchParams.get('id') ?? '');
  const [templateFilter, setTemplateFilter] = useState<string | null>(link.template);
  const [statusFilter, setStatusFilter] = useState<string[]>(link.statuses);
  const [userFilter, setUserFilter] = useState<string | null>(link.user);
  const [nodeFilter, setNodeFilter] = useState<NodeFilter>(link.node);
  const [ageWindow, setAgeWindow] = useState<AgeWindow>(link.age);
  const [olderThan, setOlderThan] = useState<OlderThan>(link.older);
  // The sample size of a server that does not page.
  const [listLimit, setListLimit] = useState(() => parseListLimit(searchParams.get('limit')));
  const [pageSize, setPageSize] = useState(link.pageSize);
  // The list shape and page position of the rows on screen. Requests read
  // the selection's (`selection` below), which the first answer can switch
  // to sampling before the sample arrives; see `ListSelection.load`.
  const [listMode, setListMode] = useState<ListMode>('paged');
  const [position, setPosition] = useState<PagePosition>(FIRST_PAGE);
  // Bumped after every list load, so a direct lookup refreshes with the list.
  const [listGeneration, setListGeneration] = useState(0);
  // Applied by the admin API (`org_id`) on either list shape.
  const [orgFilter, setOrgFilter] = useState<string | null>(() => parseOrgParam(searchParams.get('org')));
  const query = useMemo(() => sandboxQuery(idFilter), [idFilter]);
  // The selected statuses this list applies, and those it cannot.
  const statusSplit = useMemo(() => splitStatusFilter(listMode === 'paged', statusFilter), [listMode, statusFilter]);
  // What the search box does on this list's shape: a direct lookup, a prefix
  // the server matches, a match within the loaded rows, or both of the last
  // two for hex digits and dashes.
  const search = useMemo(() => sandboxSearch(idFilter, listMode), [idFilter, listMode]);
  // The filters are those of a paged request whatever the shape on screen;
  // a sampled request sends only the org (`listScope`).
  const pagedSearch = useMemo(() => sandboxSearch(idFilter, 'paged'), [idFilter]);
  const filterInputs: FilterInputs = {
    org: orgFilter,
    statuses: splitStatusFilter(true, statusFilter).applied,
    template: templateFilter,
    user: userFilter,
    age: ageWindow,
    older: olderThan,
    prefix: pagedSearch.kind === 'prefix' ? pagedSearch.prefix : '',
  };
  const filterInputsKey = JSON.stringify(filterInputs);
  const appliedInputsRef = useRef({ key: filterInputsKey, prefix: filterInputs.prefix });
  // The selected scope, the rows' scope and the next page's cursor, and the
  // loads in flight; see `ListSelection`. The filters change only once a
  // filter does. The creation-time bounds stay relative ("the last hour")
  // and are computed as each request is sent, so a refresh an hour later
  // still shows the last hour.
  const [selection] = useState(() => new ListSelection({ filters: serverFilters(filterInputs), pageSize, sampleSize: listLimit }));
  // The selected scope and the next page's cursor, for rendering.
  const [selectedScope, setSelectedScope] = useState<ListScope>(() => selection.selected);
  // Whether the first answer decided the list shape (`ListSelection.shapeDecided`).
  const [shapeDecided, setShapeDecided] = useState(false);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  // The error of the last load; a load of another scope that failed clears
  // the rows rather than show them under the new label.
  const [listError, setListError] = useState<string | null>(null);
  // Only bare arrays answered paged requests, the list's or the prefix
  // search's, for a minute of retries (`STOP_AFTER_MISMATCH_MS`) and at
  // least twenty times in a row (`STOP_AFTER_MISMATCHES`): the API went back
  // to a version without paging, and the list reads nothing more until the
  // page is reloaded. Until then each is a failed refresh, and any page
  // between them starts both over. A hidden tab sends nothing, and the first
  // bare array after it starts the minute over (`MISMATCH_RUN_GAP_MS`).
  const [listStopped, setListStopped] = useState(false);
  // The list's last answer was a bare array to a paged request, as during a
  // rolling deploy; the rows on screen are kept while the list retries. A
  // failed request leaves it as it was, and the prefix search's answers,
  // which its section reports, do not change it.
  const [olderFormat, setOlderFormat] = useState(false);
  // A full identifier that matches no loaded row is looked up directly, so a
  // sandbox outside the loaded rows can still be found.
  const [lookup, setLookup] = useState<
    // `answered`: some answer for this ID has arrived; until then the table
    // shows as loading. Later refetches never bring the spinner back.
    { publicId: string; sandbox: Sandbox | null; error: string | null; pending: boolean; answered: boolean } | null
  >(null);
  const lookupInFlightRef = useRef<string | null>(null);
  const lookupTargetRef = useRef<string | null>(null);
  // The names and IDs that start with a `fragment` search, from a request of
  // its own (`fragmentSearchScope`), keyed by that request's scope.
  const [fragmentMatches, setFragmentMatches] = useState<
    { key: string; sandboxes: Sandbox[]; more: boolean; error: string | null } | null
  >(null);
  const fragmentTargetRef = useRef<string | null>(null);
  const prefixRequestsRef = useRef<PrefixRequests | null>(null);
  if (!prefixRequestsRef.current) prefixRequestsRef.current = new PrefixRequests();
  const prefixRequests = prefixRequestsRef.current;

  useEffect(() => {
    const params = new URLSearchParams();
    if (idFilter.trim()) params.set('id', idFilter.trim());
    if (templateFilter) params.set('template', templateFilter);
    if (statusFilter.length) params.set('status', statusFilter.join(','));
    if (userFilter) params.set('user', userFilter);
    if (nodeFilter !== DEFAULT_NODE_FILTER) params.set('node', nodeFilter);
    if (ageWindow !== DEFAULT_AGE_WINDOW) params.set('age', ageWindow);
    if (olderThan !== 'any') params.set('older', olderThan);
    if (listLimit !== DEFAULT_LIST_LIMIT) params.set('limit', String(listLimit));
    if (pageSize !== parsePageSize(null)) params.set('page_size', String(pageSize));
    if (orgFilter) params.set('org', orgFilter);
    const qs = params.toString();
    const next = qs ? `${pathname}?${qs}` : pathname;
    window.history.replaceState(null, '', next);
  }, [idFilter, templateFilter, statusFilter, userFilter, nodeFilter, ageWindow, olderThan, listLimit, pageSize, orgFilter, pathname]);

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

  // After the selection changes, for rendering.
  const selectionChanged = () => {
    setSelectedScope(selection.selected);
    setNextCursor(selection.nextCursor());
    setShapeDecided(selection.shapeDecided);
  };

  // The list stopped (`ListSelection.noteAnswer`), on a load or on the prefix
  // search's answer.
  const stopList = (kept: boolean) => {
    setListStopped(true);
    selectionChanged();
    // Rows of another org, page or filter are not shown under this one's label.
    if (!kept) setSandboxes([]);
  };

  const refresh = async ({
    showLoading = true,
    scope = selection.selected,
  }: { showLoading?: boolean; reconcile?: boolean; scope?: ListScope } = {}) => {
    // A poll or Refresh captures its scope before its lock callback runs; if
    // the selection changed meanwhile, it must not invalidate the load for
    // the new selection.
    if (!sameScope(selection.selected, scope)) throw new RefreshSkipped(SCOPE_CHANGED);
    if (showLoading) setLoading(true);
    const result = await selection.load(scope, (sent) => getConsoleSandboxList(listRequestQuery(sent, Date.now())));
    // Only a load that applies ends `loading`, whichever request started it;
    // a change of scope that keeps the rows on screen ends it itself.
    if (result.kind === 'skipped') throw new RefreshSkipped(SCOPE_CHANGED);
    setLoading(false);
    selectionChanged();
    if (result.kind === 'shown') setOlderFormat(false);
    else if (result.kind === 'failed' && result.error instanceof OlderFormatAnswer) setOlderFormat(true);
    if (result.kind === 'stopped') {
      stopList(result.kept);
      throw new ShownListError(VERSION_CHANGED);
    }
    // The shape and the page position on screen change with the rows.
    setListMode(result.scope.mode);
    setPosition(selection.position);
    if (result.kind === 'failed') {
      const err = result.error as { message?: string } | null;
      const text = err?.message || 'failed to refresh sandboxes';
      if (!result.kept) {
        // The rows on screen belong to another org or size; do not show them
        // under this one's label.
        setSandboxes([]);
        setListError(text);
        // A failure all the same: the key stays stale, and the alert, not a
        // toast, reports it.
        throw new ShownListError(text);
      }
      // The notice above the table reports an older format.
      if (result.error instanceof OlderFormatAnswer) throw new ShownListError(text);
      // Keep the last rendered view when a node-local manager is briefly unavailable.
      throw new Error(text);
    }
    const data = result.response.sandboxes;
    // A page keeps the server's order, newest first, so that the pages join
    // up; a sample is sorted as it was before paging.
    if (result.response.mode === 'sampled') {
      data.sort((a, b) => parseUptime(b.uptime) - parseUptime(a.uptime) || a.id.localeCompare(b.id));
    }
    setSandboxes(data);
    setListError(null);
    setListGeneration((n) => n + 1);
    markViewRefreshed(sandboxesRefreshKey(result.scope));
  };

  const refreshAndReport = () => {
    void refresh().catch((err: any) => {
      if (!alreadyReported(err)) message.error(err.message || 'failed to refresh sandboxes');
    });
  };

  const maybeRefresh = () => {
    if (loadingRef.current) {
      return;
    }
    if (selection.stopped || document.hidden) {
      return;
    }
    const scope = selection.selected;
    void runLockedViewRefresh(
      sandboxesRefreshKey(scope),
      () => refresh({ showLoading: false, scope }),
      // A larger load is a heavier query; poll it less often.
      { staleMs: scope.limit > DEFAULT_LIST_LIMIT ? LARGE_LIST_STALE_MS : VIEW_REFRESH_STALE_MS },
    ).catch((err: any) => {
      if (!alreadyReported(err)) console.warn(err.message || err);
    });
  };

  useEffect(() => {
    loadingRef.current = loading;
  }, [loading]);

  useEffect(() => {
    adminApi.get<Organization[]>('/v1/orgs').then(setOrgs).catch(() => setOrgs([]));
    adminApi.get<Template[]>('/v1/templates')
      .then((items) => setAdminTemplateNames(items.map((t) => t.name)))
      .catch(() => setAdminTemplateNames(null));
  }, []);

  useEffect(() => {
    void refresh().catch((err: any) => {
      if (!alreadyReported(err)) message.error(err.message);
    });
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
    const scope = selection.selected;
    void runLockedViewRefresh(
      sandboxesRefreshKey(scope),
      () => refresh({ showLoading: false, reconcile: true, scope }),
      { force: true },
    ).then((ran) => {
      if (!ran) {
        message.info('Refresh already in progress');
        return;
      }
      message.success('Sandboxes refreshed');
    }).catch((err: any) => {
      if (err instanceof RefreshSkipped) message.info(`Refresh skipped: ${err.message}`);
      else if (!(err instanceof ShownListError)) message.error(err.message);
    }).finally(() => {
      setManualRefreshing(false);
    });
  };

  const handleCreate = async (values: {
    name?: string;
    org_id?: string;
    template: string;
    runtime: string;
    snapshots?: boolean;
  }) => {
    if (creatingRef.current) return;
    creatingRef.current = true;
    setCreating(true);
    try {
      const payload = {
        name: values.name || null,
        template: values.template,
        runtime: values.runtime,
        env: {},
        metadata: {},
        resources: null,
        auto_lifecycle: customCreateAutoLifecycle ?? undefined,
      };
      // Cross-org creates go through the admin surface; the public path
      // only ever creates under the signed-in admin's own org.
      const targetOrgId =
        values.org_id && (!authOrg || values.org_id !== authOrg.id) ? values.org_id : null;
      const sandbox = targetOrgId
        ? await createConsoleSandboxForOrg(targetOrgId, payload)
        : await postPublicOperation('/sandboxes', payload);
      // Accepted: the sandbox exists from here on, so whatever happens next
      // is reported against it rather than through the error path below,
      // where resubmitting the form would create a second sandbox.
      const outcome = await settleAcceptedCreate({
        waitForCreate: values.snapshots
          ? () => targetOrgId
            ? waitForAdminOperation(sandbox.operation_id)
            : waitForPublicOperation(sandbox.operation_id)
          : undefined,
        enableSnapshots: values.snapshots
          ? () => targetOrgId
            ? forceConsoleSandboxAction(sandbox.sandbox_id, 'enable_snapshots')
            : postPublicOperation(`/sandboxes/${sandbox.sandbox_id}:enable_snapshots`)
          : undefined,
        snapshotsOff: isHiddenOperationError,
      });
      form.resetFields();
      form.setFieldsValue({
        runtime: 'fc',
        template: templates[0]?.name,
      });
      setCustomCreateAutoLifecycle(null);
      createLifecycleForm.resetFields();
      refreshAndReport();
      // The new row is listed once its create operation has run; refresh then
      // instead of waiting for the next background refresh. With snapshots
      // checked, settleAcceptedCreate has already waited for the create and the
      // refresh above read the finished state.
      if (!values.snapshots) {
        void (targetOrgId ? waitForAdminOperation : waitForPublicOperation)(sandbox.operation_id)
          .then(() => refreshAndReport(), (err: unknown) => {
            if (err instanceof OperationFailedError) message.error(createFailureText(err), 10);
            refreshAndReport();
          });
      }
      const sandboxId = sandbox.sandbox_id;
      const copyId = () => {
        if (typeof navigator !== 'undefined' && navigator.clipboard) {
          void navigator.clipboard.writeText(sandboxId);
        }
      };
      message.open({
        type: outcome.type,
        content: (
          <Space size={6}>
            <span>{outcome.text}</span>
            <Link href={`/dashboard/sandboxes/${sandboxId}`}>
              <code>{sandboxLabelOrId(sandboxId)}</code>
            </Link>
            <Button
              type="text"
              size="small"
              icon={<CopyOutlined />}
              onClick={copyId}
              aria-label="Copy sandbox ID"
            />
          </Space>
        ),
        duration: 8,
      });
    } catch (err: any) {
      message.error(err.message);
    } finally {
      creatingRef.current = false;
      setCreating(false);
    }
  };

  const openCreateLifecycleModal = () => {
    createLifecycleForm.setFieldsValue({
      auto_lifecycle: policyToFormValue(customCreateAutoLifecycle ?? disabledAutoLifecycle()),
    });
    setCreateLifecycleModalOpen(true);
  };

  const closeCreateLifecycleModal = () => {
    setCreateLifecycleModalOpen(false);
    createLifecycleForm.resetFields();
  };

  const handleCreateLifecycleSave = (values: { auto_lifecycle?: unknown }) => {
    setCustomCreateAutoLifecycle(formValueToPolicy(values.auto_lifecycle as any));
    closeCreateLifecycleModal();
  };

  const handleCreateLifecycleClear = () => {
    setCustomCreateAutoLifecycle(null);
    closeCreateLifecycleModal();
  };

  // Takes the row itself: a looked-up row is not among the loaded sandboxes.
  const openLifecycleModal = (sandbox: Sandbox) => {
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

  const paged = listMode === 'paged';
  const listTruncated = !paged && listIsCapped(sandboxes, listLimit);

  // A changed filter, page size or sample size restarts paging from the
  // first page, and reloads when the request it makes has changed: on a
  // sampled list only the org and the sample size reach the server. Typing in
  // the search box waits for a pause.
  const appliedSizesRef = useRef({ pageSize, listLimit });
  useEffect(() => {
    if (appliedInputsRef.current.key === filterInputsKey && appliedSizesRef.current.pageSize === pageSize && appliedSizesRef.current.listLimit === listLimit) {
      return;
    }
    const delay = appliedInputsRef.current.prefix !== filterInputs.prefix ? 300 : 0;
    const timer = setTimeout(() => {
      appliedInputsRef.current = { key: filterInputsKey, prefix: filterInputs.prefix };
      appliedSizesRef.current = { pageSize, listLimit };
      const plan = selection.select({ filters: serverFilters(filterInputs), pageSize, sampleSize: listLimit });
      if (plan === 'unchanged') return;
      setPosition(selection.position);
      selectionChanged();
      // A change undone before its answer arrived: that answer is dropped and
      // the rows on screen are the selected scope's already.
      if (plan === 'keep') {
        setLoading(false);
        return;
      }
      // An error belongs to the request that failed.
      setListError(null);
      refreshAndReport();
    }, delay);
    return () => clearTimeout(timer);
  }, [filterInputsKey, pageSize, listLimit]);

  const goToPage = (next: PagePosition | null) => {
    if (!selection.goTo(next)) return;
    setPosition(selection.position);
    selectionChanged();
    setListError(null);
    refreshAndReport();
  };

  const filteredSandboxes = useMemo(() => {
    // A full identifier names one sandbox; the other filters do not hide it.
    if (query.kind === 'ref') {
      const loaded = sandboxes.filter((s) => matchesSandboxQuery(query, s));
      if (loaded.length > 0) return loaded;
      return lookup?.publicId === query.ref.publicId && lookup.sandbox ? [lookup.sandbox] : [];
    }

    // The server applied the rest on a paged list; see `loadedRowFilter`.
    return sandboxes.filter(loadedRowFilter(listMode, {
      matchesSearch: search.kind === 'page' || search.kind === 'fragment' ? (s) => matchesSandboxQuery(search.query, s) : null,
      template: templateFilter,
      statuses: statusSplit.applied,
      statusKey: statusFilterKey,
      userId: userFilter,
      node: nodeFilter,
      createdWithin: ageWindowAge(ageWindow),
      olderThan: olderThanAge(olderThan),
      now: Date.now(),
    }));
  }, [sandboxes, query, search, lookup, listMode, templateFilter, statusSplit, userFilter, nodeFilter, ageWindow, olderThan]);

  // A page lists its sandboxes, and the first page of the unfiltered list
  // then the rows nodes report without a database row, apart from them. A
  // full ID shows the row it names in the main table, whatever its kind.
  const exactLookup = query.kind === 'ref';
  const shownRows = useMemo(
    () => listSections(listMode, filteredSandboxes, exactLookup),
    [listMode, filteredSandboxes, exactLookup],
  );
  const loadedRows = useMemo(() => listSections(listMode, sandboxes, false), [listMode, sandboxes]);
  const nodeReportedAt = nodeReportedPlacement(selectedScope, exactLookup);

  const lookupNeeded =
    query.kind === 'ref' && !loading && !sandboxes.some((s) => matchesSandboxQuery(query, s));
  const lookupId = lookupNeeded && query.kind === 'ref' ? query.ref.publicId : null;
  // The ID the search box names now; an answer for any other ID is dropped.
  lookupTargetRef.current = query.kind === 'ref' ? query.ref.publicId : null;

  useEffect(() => {
    if (!lookupId) return;
    // One request per ID at a time; the next list load refreshes it again.
    if (lookupInFlightRef.current === lookupId) return;
    lookupInFlightRef.current = lookupId;
    setLookup((prev) =>
      prev?.publicId === lookupId
        ? { ...prev, pending: true }
        : { publicId: lookupId, sandbox: null, error: null, pending: true, answered: false },
    );
    getConsoleSandbox(lookupId)
      .then((sandbox) => {
        if (lookupTargetRef.current !== lookupId) return;
        setLookup({ publicId: lookupId, sandbox, error: null, pending: false, answered: true });
      })
      .catch((err: any) => {
        if (lookupTargetRef.current !== lookupId) return;
        const message = err?.message || 'lookup failed';
        // A 404 means the sandbox is gone; any other failure keeps the last row.
        setLookup((prev) => ({
          publicId: lookupId,
          sandbox: err?.status === 404 || prev?.publicId !== lookupId ? null : prev.sandbox,
          error: message,
          pending: false,
          answered: true,
        }));
      })
      .finally(() => {
        if (lookupInFlightRef.current === lookupId) lookupInFlightRef.current = null;
      });
  }, [lookupId, listGeneration]);

  const lookupPending = lookupId !== null && !(lookup?.publicId === lookupId && lookup.answered);

  // Sent only once the first answer has decided the list shape: before then
  // the page only assumes paging, and a server without it would answer this
  // request with a bare array too.
  const fragmentScope = useMemo(
    () => (search.kind === 'fragment' && shapeDecided ? fragmentSearchScope(selectedScope, search.prefix) : null),
    [search, selectedScope, shapeDecided],
  );
  // The request the search box names now; an answer for any other is dropped.
  fragmentTargetRef.current = fragmentScope?.key ?? null;

  useEffect(() => {
    if (!fragmentScope || listStopped) return;
    const scope = fragmentScope;
    // Typing waits for a pause, as a prefix the list sends does; the next
    // list load refreshes it again.
    const timer = setTimeout(() => {
      const ticket = prefixRequests.begin(scope.key);
      const sentAt = selection.now();
      getConsoleSandboxList(listRequestQuery(scope, Date.now()), ticket.signal)
        .then((response) => {
          if (!prefixRequests.applies(ticket, fragmentTargetRef.current)) return;
          // A bare array counts toward stopping the list, as one answering
          // the list does, and fails this refresh of the section.
          const note = selection.noteAnswer('paged', response.mode, sentAt);
          if (note.kind === 'stopped') return stopList(note.kept);
          // Not sent before then; should it answer first, it fails this refresh.
          if (note.kind === 'undecided') throw new Error('the list has not loaded yet; retrying');
          if (response.mode !== 'paged') throw new OlderFormatAnswer();
          setFragmentMatches({ key: scope.key, sandboxes: response.sandboxes, more: response.nextCursor !== null, error: null });
        })
        .catch((err: any) => {
          if (!prefixRequests.applies(ticket, fragmentTargetRef.current)) return;
          const message = err?.message || 'search failed';
          // A failed refresh keeps the last answer for the same search.
          setFragmentMatches((prev) =>
            prev?.key === scope.key ? { ...prev, error: message } : { key: scope.key, sandboxes: [], more: false, error: message });
        });
    }, 300);
    return () => {
      clearTimeout(timer);
      prefixRequests.cancel();
    };
  }, [fragmentScope?.key, listGeneration, listStopped]);

  const fragmentAnswer = fragmentScope && fragmentMatches?.key === fragmentScope.key ? fragmentMatches : null;
  // The server applied every filter but the node filter.
  // Matches the main table already shows are left out of it.
  const fragmentRows = useMemo(
    () => supplementalMatches(fragmentAnswer?.sandboxes ?? [], shownRows.sandboxes).filter(loadedRowFilter('paged', {
      matchesSearch: null,
      template: null,
      statuses: [],
      statusKey: statusFilterKey,
      userId: null,
      node: nodeFilter,
      createdWithin: null,
      olderThan: null,
      now: Date.now(),
    })),
    [fragmentAnswer, nodeFilter, shownRows.sandboxes],
  );

  // The window and the "older than" bound must leave a range between them;
  // a window that would not drops the bound.
  const changeAgeWindow = (value: AgeWindow) => {
    setAgeWindow(value);
    const within = ageWindowMs(value);
    const older = olderThanMs(olderThan);
    if (within !== null && older !== null && older >= within) setOlderThan('any');
  };

  // The selector's labels, which add the slug where a name repeats, so the
  // notices never read "It belongs to Acme, not Acme".
  const orgNames = useMemo(() => new Map(orgOptionLabels(orgs).map((option) => [option.value, option.label])), [orgs]);

  const templateOptions = useMemo(() => {
    const options = templateFilterOptions({
      // Until the admin list answers, the loaded and own templates.
      admin: adminTemplateNames ?? null,
      loaded: sandboxes.map((s) => s.template),
      own: templates.map((t) => t.name),
      selected: templateFilter,
      typed: templateSearch,
    });
    return {
      partial: options.partial && adminTemplateNames === null,
      options: [
        ...(options.typed ? [{ value: options.typed, label: `Filter on "${options.typed}"` }] : []),
        ...options.names.map((name) => ({ value: name, label: name })),
      ],
    };
  }, [adminTemplateNames, sandboxes, templates, templateFilter, templateSearch]);

  // A paged list filters on the stored lifecycle; a sampled one on the
  // State column, with any other status its rows carry.
  const statusFilterOptionList = useMemo(
    () => (paged ? [...STORED_LIFECYCLE_FILTER_STATES] : statusFilterOptions(sandboxes.map((s) => s.status)))
      .map((status) => ({ value: status, label: status })),
    [paged, sandboxes],
  );

  const userFilterLabel = useMemo(() => {
    if (!userFilter) return null;
    return sandboxes.find((s) => s.user_id === userFilter)?.user || userFilter.slice(0, 8);
  }, [sandboxes, userFilter]);

  const clearFilters = () => {
    setIdFilter('');
    setTemplateFilter(null);
    setStatusFilter([]);
    setUserFilter(null);
    setNodeFilter('any');
    setAgeWindow(DEFAULT_AGE_WINDOW);
    setOlderThan('any');
    setOrgFilter(null);
  };

  const filtersActive =
    !!idFilter ||
    !!templateFilter ||
    statusFilter.length > 0 ||
    !!userFilter ||
    nodeFilter !== 'any' ||
    ageWindow !== DEFAULT_AGE_WINDOW ||
    olderThan !== 'any' ||
    !!orgFilter;

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
              onClick={() => openLifecycleModal(record)}
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
    {
      title: 'Org',
      dataIndex: 'org_id',
      width: 130,
      ellipsis: true,
      render: (orgId: string | null) => (orgId ? <Link href={`/dashboard/orgs/${orgId}`}>{orgNames.get(orgId) ?? orgId}</Link> : '—'),
    },
    {
      title: 'User',
      dataIndex: 'user',
      width: 95,
      ellipsis: true,
      render: (v: string, record) => {
        if (!record.user_id) return v || '—';
        const userId = record.user_id;
        return (
          <Tooltip title={`Show only ${v || 'this user'}'s sandboxes`}>
            <Button
              type="link"
              size="small"
              style={{ padding: 0, height: 'auto' }}
              onClick={(event) => {
                event.stopPropagation();
                setUserFilter(userId);
              }}
            >
              {v || userId.slice(0, 8)}
            </Button>
          </Tooltip>
        );
      },
    },
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
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <Title level={3} style={{ marginTop: 0 }}>Sandboxes</Title>
        <Button
          icon={<ReloadOutlined />}
          loading={manualRefreshing}
          disabled={refreshCooldown || listStopped}
          onClick={handleManualRefresh}
        >
          Refresh
        </Button>
      </Space>

      <Card size="small" style={{ marginBottom: 24 }}>
        <Form
          form={form}
          layout="inline"
          onFinish={handleCreate}
          initialValues={{
            runtime: 'fc',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, width: '100%', flexWrap: 'wrap' }}>
            <Text strong>Create sandbox</Text>
            {orgs.length > 1 && (
              <Form.Item name="org_id" style={{ marginBottom: 0 }}>
                <Select
                  placeholder="Org: mine"
                  style={{ width: 160 }}
                  allowClear
                  options={orgs.map((org) => ({
                    label: authOrg && org.id === authOrg.id ? `${org.name} (mine)` : org.name,
                    value: org.id,
                  }))}
                />
              </Form.Item>
            )}
            <Form.Item name="name" style={{ marginBottom: 0 }}>
              <Input placeholder="Name" style={{ width: 150 }} />
            </Form.Item>
            <Form.Item name="runtime" style={{ marginBottom: 0 }}>
              <Select style={{ width: 130 }}>
                <Select.Option value="fc">Firecracker</Select.Option>
                <Select.Option value="gvisor" disabled>gVisor</Select.Option>
                <Select.Option value="ch" disabled>Cloud Hypervisor</Select.Option>
                <Select.Option value="hyperv" disabled>Hyper-V</Select.Option>
              </Select>
            </Form.Item>
            <Form.Item name="template" rules={[{ required: true }]} style={{ marginBottom: 0 }}>
              <Select placeholder="Template" style={{ width: 150 }}>
                {templates.map((t) => (
                  <Select.Option key={t.name} value={t.name}>{t.name}</Select.Option>
                ))}
              </Select>
            </Form.Item>
            <Form.Item name="snapshots" valuePropName="checked" style={{ marginBottom: 0 }}>
              <Checkbox>Snapshots</Checkbox>
            </Form.Item>
            <Space size={4} style={{ marginLeft: 'auto' }}>
              <Button size="small" onClick={openCreateLifecycleModal}>
                {customCreateAutoLifecycle ? 'Manage auto lifecycle' : 'Custom auto lifecycle'}
              </Button>
              {customCreateAutoLifecycle ? (
                <Button type="text" size="small" danger onClick={() => setCustomCreateAutoLifecycle(null)}>
                  Clear
                </Button>
              ) : null}
              <Button type="primary" htmlType="submit" icon={<PlusOutlined />} loading={creating}>Create</Button>
            </Space>
          </div>
        </Form>
      </Card>

      <Card size="small" style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <Text strong>Filters</Text>
          <Space size={2}>
            <Input
              allowClear
              size="small"
              placeholder={paged ? 'Case-sensitive name prefix, ID prefix or full ID' : 'ID, pod name or name'}
              value={idFilter}
              onChange={(e) => setIdFilter(e.target.value)}
              style={{ width: 290 }}
              aria-label="Search sandboxes"
            />
            {paged ? (
              <Tooltip title="A name or ID prefix is searched on the server as typed. Names match case-sensitively; an ID prefix starts with sbx_ or is a dashed UUID start, and its hex matches in either case. Hex digits and dashes alone, such as the ID column's label or a name like e2e-1, are matched two ways: anywhere in the IDs and names of this page, ignoring case, and on the server as the start of any name or ID, listed after this page. A full ID or pod name is looked up directly.">
                <QuestionCircleOutlined aria-label="About the search" style={{ color: 'rgba(0, 0, 0, 0.45)' }} />
              </Tooltip>
            ) : null}
          </Space>
          <Space size={2}>
            <Select
              size="small"
              placeholder={templateOptions.partial ? 'Template (partial list)' : 'Template'}
              value={templateFilter ?? undefined}
              onChange={(value) => {
                setTemplateFilter((value as string | undefined) ?? null);
                setTemplateSearch('');
              }}
              options={templateOptions.options}
              allowClear
              showSearch={{ optionFilterProp: 'label', onSearch: setTemplateSearch }}
              style={{ minWidth: 160 }}
              aria-label="Template"
            />
            {templateOptions.partial ? (
              <Tooltip title="The admin template list could not be read, so these are only the templates of the loaded rows and of your organization. Type a template name to filter on another.">
                <QuestionCircleOutlined aria-label="About the template list" style={{ color: 'rgba(0, 0, 0, 0.45)' }} />
              </Tooltip>
            ) : null}
          </Space>
          <Space size={2}>
            <Select
              mode="multiple"
              size="small"
              placeholder={paged ? 'Stored lifecycle' : 'Status'}
              value={statusSplit.applied}
              // The control shows the applied statuses; the rest stay under "Not applied".
              onChange={(value) => setStatusFilter((current) => chooseAppliedStatuses(paged, current, value as string[]))}
              options={statusFilterOptionList}
              allowClear
              style={{ minWidth: 180 }}
              maxTagCount="responsive"
              aria-label={paged ? 'Stored lifecycle' : 'Status'}
            />
            {paged ? (
              <Tooltip title="Matches the lifecycle the control plane stores for each sandbox, not the State column, which shows what the sandbox's node reports. A sandbox can match one lifecycle and show another state.">
                <QuestionCircleOutlined aria-label="About the lifecycle filter" style={{ color: 'rgba(0, 0, 0, 0.45)' }} />
              </Tooltip>
            ) : null}
          </Space>
          {statusSplit.notApplied.length ? (
            <Tooltip title="The list filters on the lifecycle the control plane stores, and these are not stored lifecycles, so they are not applied.">
              <Tag color="warning" closable onClose={() => setStatusFilter(statusSplit.applied)} style={{ marginInlineEnd: 0 }}>
                Not applied: {statusSplit.notApplied.join(', ')}
              </Tag>
            </Tooltip>
          ) : null}
          {userFilter ? (
            <Tag closable onClose={() => setUserFilter(null)} style={{ marginInlineEnd: 0 }}>
              User: {userFilterLabel}
            </Tag>
          ) : null}
          <Tooltip title={paged ? 'The server has no node filter yet, so this filters the page loaded below. A node\'s page, linked from the Node column, lists the sandboxes placed on it; the warm-pool slots and other rows it reports without a database row are listed on the first page of this list with no filter set.' : undefined}>
            <Select
              size="small"
              value={nodeFilter}
              onChange={(value) => setNodeFilter(value as NodeFilter)}
              options={[
                { value: 'any', label: paged ? 'Node (this page): any' : 'Node: any' },
                { value: 'assigned', label: paged ? 'Node (this page): assigned' : 'Node: assigned' },
                { value: 'unassigned', label: paged ? 'Node (this page): unassigned' : 'Node: unassigned' },
              ]}
              style={{ width: paged ? 230 : 170 }}
              aria-label="Node"
            />
          </Tooltip>
          <Select
            size="small"
            value={ageWindow}
            onChange={(value) => changeAgeWindow(value as AgeWindow)}
            options={AGE_WINDOW_OPTIONS.map((opt) => ({ value: opt.value, label: opt.label }))}
            style={{ width: 150 }}
            aria-label="Created within"
          />
          <Select
            size="small"
            value={olderThan}
            onChange={(value) => setOlderThan(value as OlderThan)}
            options={OLDER_THAN_OPTIONS.map((opt) => {
              const within = ageWindowMs(ageWindow);
              return {
                value: opt.value,
                label: opt.label,
                disabled: opt.ms !== null && within !== null && opt.ms >= within,
              };
            })}
            style={{ width: 170 }}
            aria-label="Created before"
          />
          <Select
            size="small"
            allowClear
            placeholder="Org: any"
            value={orgFilter ?? undefined}
            onChange={(value) => setOrgFilter(value ?? null)}
            options={orgOptionLabels(orgs)}
            showSearch={{ optionFilterProp: 'label' }}
            style={{ width: 200 }}
            aria-label="Organization"
          />
          {paged ? null : (
            <Select
              size="small"
              value={listLimit}
              onChange={setListLimit}
              options={Array.from(new Set([...LIST_LIMIT_OPTIONS, listLimit]))
                .sort((a, b) => a - b)
                .map((value) => ({ value, label: `Load newest ${value}` }))}
              style={{ width: 170 }}
              aria-label="Number of sandboxes to load"
            />
          )}
          <Text type="secondary" style={{ fontSize: 12 }}>
            {paged
              ? `${shownRows.sandboxes.length === loadedRows.sandboxes.length ? '' : `${shownRows.sandboxes.length} / `}${loadedRows.sandboxes.length} on page ${position.index + 1}`
              : `${filteredSandboxes.length} / ${sandboxes.length}${listTruncated ? '+' : ''}`}
          </Text>
          {filtersActive ? (
            <Button size="small" type="link" onClick={clearFilters}>Reset</Button>
          ) : null}
        </div>
      </Card>

      {query.kind === 'ref' && lookupNeeded && lookup?.publicId === query.ref.publicId ? (
        lookup.sandbox ? (
          <Alert
            type="info"
            showIcon
            style={{ marginBottom: 12 }}
            title={`Found ${query.ref.publicId} by direct lookup. ${lookupScopeText(lookup.sandbox, orgFilter, (id) => orgNames.get(id), paged ? null : listLimit)}`}
            description={lookup.error ? `The last refresh of this row failed: ${lookup.error}` : undefined}
          />
        ) : lookup.error ? (
          <Alert type="warning" showIcon style={{ marginBottom: 12 }} title={`${query.ref.publicId}: ${lookup.error}`} />
        ) : null
      ) : listTruncated ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          title={`Showing the newest ${listLimit} sandboxes${orgFilter ? ` of ${orgNames.get(orgFilter) ?? orgFilter}` : ''}. Filters apply to these rows only; enter a full sandbox ID or pod name to look up any sandbox.`}
        />
      ) : null}

      {linkNoticeOpen ? (
        <Alert
          type="warning"
          showIcon
          closable
          onClose={() => setLinkNoticeOpen(false)}
          style={{ marginBottom: 12 }}
          title={`Left out of this link's filters: ${link.ignored.map((entry) => `${entry.param}=${entry.value} (${entry.reason})`).join('; ')}.`}
        />
      ) : null}

      {paged && search.kind === 'fragment' ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          title={`"${idFilter.trim()}" matches IDs and names anywhere within page ${position.index + 1}, ignoring case, in the table below. Names and IDs that start with it are searched across the whole list and listed after this page; paste the full ID to look it up.`}
        />
      ) : paged && search.kind === 'page' ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          title={`"${idFilter.trim()}" matches IDs and names within this page, ignoring case; paste the full ID to look it up.`}
        />
      ) : null}

      {listStopped ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          title="The API changed version while this page was open. Reload to continue."
          // A fresh page view; the URL keeps the filters.
          action={<Button size="small" onClick={() => window.location.reload()}>Reload</Button>}
        />
      ) : listError ? (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 12 }}
          title={`${paged ? `Could not load page ${position.index + 1} of the sandboxes` : `Could not load the newest ${listLimit} sandboxes`}${orgFilter ? ` of ${orgNames.get(orgFilter) ?? orgFilter}` : ''}: ${listError}`}
          action={<Button size="small" onClick={refreshAndReport}>Retry</Button>}
        />
      ) : olderFormat ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          title={paged
            ? 'The API answered in an older format; retrying, and keeping the rows on screen.'
            : 'The API answered in an older format; retrying the switch to pages, and keeping the sample on screen.'}
        />
      ) : null}

      <Table
        // A table that mounted without pagination ignores `defaultPageSize`
        // when it gains one, so a switch to sampling mounts a new table.
        key={listMode}
        columns={columns}
        dataSource={shownRows.sandboxes}
        rowKey="id"
        loading={loading || lookupPending}
        size="small"
        // A page is the server's; a sample is paged here.
        pagination={paged ? false : tablePagination('sandboxes')}
        scroll={{ x: 1280 }}
        expandable={{
          showExpandColumn: false,
          expandRowByClick: true,
          // The table keeps a collapsed row's content mounted; unmounting the
          // panel stops its reads.
          expandedRowRender: (record, _index, _indent, expanded) => (expanded
            ? <SnapshotsPanel sandboxId={record.id} ownOrg={isOwnOrg(authOrg?.id, record.org_id)} />
            : null),
          rowExpandable: snapshotsPanelAvailable,
        }}
      />

      {paged ? (
        <div style={{ display: 'flex', justifyContent: 'flex-end', alignItems: 'center', gap: 8, marginTop: 12 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Page {position.index + 1}, newest first</Text>
          <Button
            size="small"
            icon={<LeftOutlined />}
            disabled={loading || listStopped || position.index === 0}
            onClick={() => goToPage(selection.previousPosition())}
          >
            Previous
          </Button>
          <Button
            size="small"
            icon={<RightOutlined />}
            iconPlacement="end"
            disabled={loading || listStopped || !nextCursor}
            onClick={() => goToPage(selection.nextPosition())}
          >
            Next
          </Button>
          <Select
            size="small"
            value={pageSize}
            onChange={setPageSize}
            options={PAGE_SIZE_OPTIONS.map((value) => ({ value, label: `${value} / page` }))}
            style={{ width: 110 }}
            aria-label="Sandboxes per page"
          />
        </div>
      ) : null}

      {/* Left out once an answer leaves nothing to show beyond the main table. */}
      {paged && !listStopped && search.kind === 'fragment' && !(fragmentAnswer && !fragmentAnswer.error && fragmentRows.length === 0) ? (
        <div style={{ marginTop: 24 }}>
          <Title level={5} style={{ marginTop: 0 }}>
            More matches beyond this page{fragmentAnswer ? ` (${fragmentRows.length}${fragmentAnswer.more && fragmentRows.length > 0 ? '+' : ''})` : ''}
          </Title>
          <Text type="secondary" style={{ display: 'block', fontSize: 12, marginBottom: 8 }}>
            {`Names and IDs starting with "${search.prefix}", not already listed above. Searched on the server with the filters above: names that start with it, case-sensitively, and IDs that start with it. Names and IDs that contain it elsewhere, such as an ID ending in the ID column's label, match on the page above only.${fragmentAnswer?.more ? ` Showing the first ${pageSize}, newest first; type more of the name or ID to narrow them.` : ''}`}
          </Text>
          {fragmentAnswer?.error ? (
            <Alert type="warning" showIcon style={{ marginBottom: 8 }} title={`Could not search names and IDs starting with "${search.prefix}": ${fragmentAnswer.error}`} />
          ) : null}
          <Table
            columns={columns}
            dataSource={fragmentRows}
            rowKey="id"
            loading={!fragmentAnswer}
            size="small"
            pagination={false}
            scroll={{ x: 1280 }}
          />
        </div>
      ) : null}

      {paged && nodeReportedAt === 'here' && loadedRows.nodeReported.length > 0 ? (
        <div style={{ marginTop: 24 }}>
          <Title level={5} style={{ marginTop: 0 }}>
            Reported by nodes, with no database row ({shownRows.nodeReported.length === loadedRows.nodeReported.length ? '' : `${shownRows.nodeReported.length} / `}{loadedRows.nodeReported.length})
          </Title>
          <Text type="secondary" style={{ display: 'block', fontSize: 12, marginBottom: 8 }}>
            Warm-pool slots and observed-local sandboxes. They belong to no organization and are listed here, on the first page of the list with no filter set; any filter but the node filter leaves them out.
          </Text>
          <Table
            columns={columns}
            dataSource={shownRows.nodeReported}
            rowKey="id"
            loading={loading}
            size="small"
            pagination={tablePagination('node-reported rows')}
            scroll={{ x: 1280 }}
          />
        </div>
      ) : paged && (nodeReportedAt === 'filtered' || nodeReportedAt === 'first-page') ? (
        <Text type="secondary" style={{ display: 'block', fontSize: 12, marginTop: 8 }}>
          {nodeReportedAt === 'filtered'
            ? 'Filters leave out the warm-pool slots and other rows nodes report without a database row; they are listed on the first page with no filter set.'
            : 'The warm-pool slots and other rows nodes report without a database row are listed on the first page.'}
        </Text>
      ) : null}

      <Modal
        title="Custom Auto Lifecycle"
        open={createLifecycleModalOpen}
        onCancel={closeCreateLifecycleModal}
        footer={[
          <Button key="clear" danger onClick={handleCreateLifecycleClear}>Clear</Button>,
          <Button key="cancel" onClick={closeCreateLifecycleModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => createLifecycleForm.submit()}>Save</Button>,
        ]}
        destroyOnHidden
        width={760}
      >
        <Form form={createLifecycleForm} layout="vertical" onFinish={handleCreateLifecycleSave}>
          <AutoLifecycleEditor prefix={['auto_lifecycle']} />
        </Form>
      </Modal>

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
