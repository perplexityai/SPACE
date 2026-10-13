'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { adminApi } from '@/lib/api';
import { useAuth } from '@/lib/auth';
import { publicSandboxIdFromAny, sandboxLabelOrId } from '@/lib/sandbox-id';
import { isManagingOrgAdmin } from '@/lib/userRoles';
import { idempotencyKey, waitForPublicOperation } from '@/lib/public-api';
import {
  NetworkPolicyV1,
  Template,
  PoolInfo,
  NodeRole,
  NodeTemplateInventory,
  SpaceNode,
  ClusterPoolConfig,
  WarmPoolPolicy,
  WarmPoolPolicyCapabilities,
  TemplateRelease,
  TemplateReleaseWarmPool,
  TemplateOperationResponse,
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
  disabledAutoLifecycle,
  formValueToPolicy,
  policyToFormValue,
  summarizeAutoLifecycle,
} from '@/components/auto-lifecycle';
import {
  Table, Button, Form, Input, InputNumber, Select, Typography, Space, Tag, Popconfirm,
  message, Modal, Switch, Tooltip,
} from 'antd';
import {
  DeleteOutlined, ReloadOutlined, DownloadOutlined,
  CheckCircleFilled, ClockCircleOutlined, TagOutlined,
  DownOutlined, RightOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

const { Title, Text } = Typography;
const DEFAULT_TEMPLATE_TAG = 'latest';
const TEMPLATES_VIEW_REFRESH_KEY = 'dashboard:templates';
const REFRESH_CLICK_COOLDOWN_MS = 3000;

interface TemplateAvailabilityRow {
  key: string;
  name: string;
  content_hash: string | null;
  tags: string[];
  build_date: string | null;
  runtime: string | null;
  arch: string | null;
  artifact_size_bytes: number | null;
  cache_size_bytes: number | null;
  disk_size_mb: number | null;
  vcpu: number | null;
  memory_mib: number | null;
  release: TemplateRelease | null;
  nodes: TemplateNodeAvailability[];
}

interface TemplateNodeAvailability {
  node: SpaceNode;
  snapshots: NonNullable<NodeTemplateInventory['builds'][number]['snapshots']>;
}

interface BaseSnapshotPolicy {
  template: string;
  ensure_base_snapshot: boolean;
}

interface SnapshotPolicyFanoutResponse {
  fanout_queued: boolean;
  nodes: {
    skipped: boolean;
    ok: boolean;
    reconciled: boolean;
    snapshot_built: boolean;
    snapshot_deleted: boolean;
    error: string | null;
  }[];
}

interface TemplateNetworkPolicyUpdateResponse {
  ok: boolean;
  network_policy_generation: number | null;
}

interface RefreshOptions {
  showLoading?: boolean;
  reconcile?: boolean;
  includeBaseSnapshotPolicies?: boolean;
}

interface TemplatePoolStatusResponse {
  template: string;
  policy: {
    target_count: number;
    boot_mode: string;
  } | null;
  observed: {
    pool_target: number | null;
    pool_current: number;
    pool_pending: number;
    sandbox_ids: string[];
  } | null;
}

function formatBytes(n: number | null | undefined) {
  if (n === null || n === undefined) return '—';
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)} KB`;
  return `${n} B`;
}

function formatDiskSize(mib: number | null | undefined) {
  return mib && mib > 0 ? formatBytes(mib * 1024 * 1024) : '—';
}

function formNumberIsBlank(value: number | null | undefined) {
  return value === null || value === undefined;
}

const ROOTFS_SIZE_MIN_DELTA_BYTES = 128 * 1024 * 1024;
const ROOTFS_SIZE_MIN_DELTA_RATIO = 0.01;

function hasMeaningfulByteDelta(a: number, b: number) {
  const delta = Math.abs(a - b);
  const reference = Math.max(a, b);
  const threshold = Math.max(ROOTFS_SIZE_MIN_DELTA_BYTES, reference * ROOTFS_SIZE_MIN_DELTA_RATIO);
  return delta > threshold;
}

function shouldShowLogicalRootfsSize(values: {
  disk_size_mb?: number | null;
  cache_size_bytes?: number | null;
  artifact_size_bytes?: number | null;
}) {
  if (values.artifact_size_bytes === null || values.artifact_size_bytes === undefined) return false;
  const diskSizeBytes = values.disk_size_mb && values.disk_size_mb > 0
    ? values.disk_size_mb * 1024 * 1024
    : null;
  if (diskSizeBytes !== null && !hasMeaningfulByteDelta(values.artifact_size_bytes, diskSizeBytes)) {
    return false;
  }
  if (values.cache_size_bytes === null || values.cache_size_bytes === undefined) return true;
  return hasMeaningfulByteDelta(values.artifact_size_bytes, values.cache_size_bytes);
}

function sizeTooltipLines(values: {
  disk_size_mb?: number | null;
  cache_size_bytes?: number | null;
  artifact_size_bytes?: number | null;
}) {
  return (
    <Space direction="vertical" size={0}>
      <span>Sandbox disk: {formatDiskSize(values.disk_size_mb)}</span>
      {values.cache_size_bytes !== null && values.cache_size_bytes !== undefined ? (
        <span>Rootfs allocated: {formatBytes(values.cache_size_bytes)}</span>
      ) : null}
      {shouldShowLogicalRootfsSize(values) ? (
        <span>Rootfs logical: {formatBytes(values.artifact_size_bytes)}</span>
      ) : null}
    </Space>
  );
}

function renderDiskCell(record: {
  disk_size_mb?: number | null;
  cache_size_bytes?: number | null;
  artifact_size_bytes?: number | null;
}) {
  return (
    <Tooltip title={sizeTooltipLines(record)}>
      <span>{formatDiskSize(record.disk_size_mb)}</span>
    </Tooltip>
  );
}

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

function roleTextColor(role: NodeRole) {
  switch (role) {
    case 'worker':
      return '#1677ff';
    case 'builder':
      return '#52c41a';
    case 'cluster':
      return '#d48806';
    default:
      return undefined;
  }
}

function shortHash(hash: string | null) {
  if (!hash) return DEFAULT_TEMPLATE_TAG;
  return hash.replace('sha256:', '').slice(0, 12);
}

function isDigestTag(tag: string) {
  return tag.startsWith('sha256:') || /^[a-f0-9]{64}$/i.test(tag);
}

function normalizedDigest(value: string) {
  return value.replace(/^sha256:/, '');
}

function visibleTemplateTags(tags: string[], contentHash: string | null) {
  const contentDigest = contentHash ? normalizedDigest(contentHash) : null;
  return tags.filter((tag) => {
    if (isDigestTag(tag)) return false;
    return !contentDigest || normalizedDigest(tag) !== contentDigest;
  });
}

function canDeleteVisibleTemplateTag(tag: string) {
  return tag !== DEFAULT_TEMPLATE_TAG && !isDigestTag(tag);
}

function errorMessage(err: unknown) {
  return err instanceof Error ? err.message : String(err);
}

function hasObservedPoolSandboxes(template: Template) {
  return Boolean(
    (template.pool_current && template.pool_current > 0)
      || (template.pool_target && template.pool_target > 0),
  );
}

function tableFilterOptions(values: string[]) {
  return Array.from(new Set(values))
    .sort((a, b) => a.localeCompare(b))
    .map((value) => ({ text: value, value }));
}

function nodeLabel(node: SpaceNode) {
  const identity = node.name || node.id.slice(0, 8);
  const where = node.node_api_endpoint ?? `${node.managed_by} substrate`;
  return `${identity} - ${node.role} - ${where}`;
}

function encodedPathSegment(value: string) {
  return encodeURIComponent(value);
}

function shortNodeIdentity(node: SpaceNode): string {
  const raw = node.name || node.id.slice(0, 8);
  return raw.replace(/^ip-/, '').replace(/\.ec2\.internal$/, '');
}

function snapshotShapeLabel(vcpu: number, memory_mib: number) {
  return `${vcpu}c / ${memory_mib} MiB`;
}

function observedSnapshotShapeLabels(snapshots: TemplateNodeAvailability['snapshots']) {
  return Array.from(new Set(
    snapshots.map((snapshot) => snapshotShapeLabel(snapshot.vcpu, snapshot.memory_mib)),
  ));
}

function snapshotStatusDot(color: string, filled: boolean) {
  return (
    <span
      style={{
        display: 'inline-block',
        width: 10,
        height: 10,
        borderRadius: '50%',
        background: filled ? color : 'transparent',
        border: `1.5px solid ${color}`,
      }}
    />
  );
}

function nodeSnapshotIcon(availability: TemplateNodeAvailability) {
  const observedShapes = observedSnapshotShapeLabels(availability.snapshots);
  if (observedShapes.length > 0) {
    return (
      <Tooltip title={`Observed base snapshot: ${observedShapes.join(', ')}`}>
        {snapshotStatusDot('#389e0d', true)}
      </Tooltip>
    );
  }
  return (
    <Tooltip title="No base snapshot">
      {snapshotStatusDot('#8c8c8c', false)}
    </Tooltip>
  );
}

function formatShortDateTime(value: string) {
  const date = new Date(value);
  if (isNaN(date.getTime())) return value;
  const pad = (n: number) => String(n).padStart(2, '0');
  return [
    pad(date.getDate()),
    pad(date.getMonth() + 1),
    pad(date.getFullYear() % 100),
  ].join('/') + ` ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function releasePoolKey(release: TemplateRelease) {
  return `${release.template_name}:${release.content_hash}`;
}

function warmPoolLabel(pool: TemplateReleaseWarmPool) {
  if (!pool.enabled) return 'Off';
  return `${pool.ready_slots}/${pool.target_ready_slots}`;
}

function matchesWarmPoolFilter(pools: TemplateReleaseWarmPool[], filter: string) {
  switch (filter) {
    case 'enabled':
      return pools.some((pool) => pool.enabled);
    case 'disabled':
      return pools.some((pool) => !pool.enabled);
    case 'none':
      return pools.length === 0;
    default:
      return false;
  }
}

function warmPoolTooltip(pool: TemplateReleaseWarmPool, diskCapacityEnabled: boolean) {
  return (
    <Space direction="vertical" size={0}>
      <span>Owner: {pool.managed_by}</span>
      <span>Shape: {pool.vcpu}c / {pool.memory_mib} MiB</span>
      {diskCapacityEnabled ? (
        <span>Disk: {pool.disk_mib ? `${pool.disk_mib} MiB` : 'Template default'}</span>
      ) : null}
      <span>Ready: {pool.ready_slots}</span>
      <span>Warming: {pool.warming_slots}</span>
      <span>Claimed: {pool.claimed_slots}</span>
      <span>Refill: {pool.refill_per_tick} per tick</span>
      <span>Max warming: {pool.max_warming_slots}</span>
      <span>Namespace: {pool.namespace}</span>
    </Space>
  );
}

function positiveOrDefault(value: number | null | undefined, fallback: number) {
  return value && value > 0 ? value : fallback;
}

function baseSnapshotTooltip(template: Template, desired: boolean) {
  return (
    <Space direction="vertical" size={0}>
      <span>Desired: {desired ? 'enabled' : 'disabled'}</span>
      <span>Observed: {template.has_base_snapshot ? 'ready' : 'not ready'}</span>
    </Space>
  );
}

function pluralize(count: number, singular: string, plural = `${singular}s`) {
  return `${count} ${count === 1 ? singular : plural}`;
}

function snapshotPolicyFanoutMessage(
  template: string,
  enabled: boolean,
  response?: SnapshotPolicyFanoutResponse,
) {
  if (!response?.fanout_queued) {
    return `Base snapshot policy ${enabled ? 'enabled' : 'disabled'} for ${template}`;
  }
  const failures = response.nodes.filter((node) => node.error || (!node.skipped && !node.ok)).length;
  if (failures > 0) {
    return `Base snapshot policy saved for ${template}; ${pluralize(failures, 'node')} reported an error`;
  }
  return `Base snapshot policy ${enabled ? 'enabled' : 'disabled'} for ${template}; reconcile queued`;
}

function hasAutoLifecyclePolicy(policy: Template['default_auto_lifecycle']) {
  return JSON.stringify(policy ?? disabledAutoLifecycle()) !== JSON.stringify(disabledAutoLifecycle());
}

function PoolSandboxesPanel({ templateName }: { templateName: string }) {
  const [loading, setLoading] = useState(true);
  const [poolInfo, setPoolInfo] = useState<PoolInfo | null>(null);
  const [deletingSandbox, setDeletingSandbox] = useState<string | null>(null);

  const refreshPool = async () => {
    setLoading(true);
    try {
      const status = await adminApi.get<TemplatePoolStatusResponse>(
        `/v1/templates/${encodedPathSegment(templateName)}/pool`,
      );
      const target = status.policy?.target_count ?? status.observed?.pool_target ?? 0;
      setPoolInfo(status.policy || status.observed ? {
        template: status.template,
        target_count: target,
        current_count: status.observed?.pool_current ?? 0,
        pending_count: status.observed?.pool_pending ?? 0,
        sandbox_ids: status.observed?.sandbox_ids ?? [],
      } : null);
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refreshPool();
  }, [templateName]);

  const deleteSandbox = async (sandboxId: string) => {
    setDeletingSandbox(sandboxId);
    try {
      await adminApi.post(
        `/v1/sandboxes/${publicSandboxIdFromAny(sandboxId)}:force-delete`,
        undefined,
        { 'Idempotency-Key': idempotencyKey() },
      );
      await refreshPool();
      message.success('Pool sandbox delete queued');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      setDeletingSandbox(null);
    }
  };

  const sandboxIds = poolInfo?.sandbox_ids ?? [];

  return (
    <Table
      size="small"
      loading={loading}
      dataSource={sandboxIds.map((id) => ({ id }))}
      rowKey="id"
      pagination={false}
      locale={{ emptyText: 'No observed pool sandboxes' }}
      columns={[
        {
          title: 'Sandbox',
          dataIndex: 'id',
          render: (id: string) => <code>{sandboxLabelOrId(id)}</code>,
        },
        {
          title: 'Actions',
          width: 80,
          render: (_, record: { id: string }) => (
            <Popconfirm
              title={`Delete pool sandbox ${sandboxLabelOrId(record.id)}?`}
              okText="Delete"
              okType="danger"
              onConfirm={() => deleteSandbox(record.id)}
            >
              <Button
                type="text"
                size="small"
                danger
                icon={<DeleteOutlined />}
                loading={deletingSandbox === record.id}
                aria-label="Delete pool sandbox"
              />
            </Popconfirm>
          ),
        },
      ]}
    />
  );
}

export default function TemplatesPage() {
  const { user, cluster, substrate } = useAuth();
  const [templateReleases, setTemplateReleases] = useState<TemplateRelease[]>([]);
  const [templates, setTemplates] = useState<Template[]>([]);
  const [baseSnapshotPolicies, setBaseSnapshotPolicies] = useState<Record<string, BaseSnapshotPolicy>>({});
  const [inventory, setInventory] = useState<NodeTemplateInventory[]>([]);
  const [clusterPools, setClusterPools] = useState<ClusterPoolConfig[]>([]);
  const [warmPoolPolicies, setWarmPoolPolicies] = useState<WarmPoolPolicy[]>([]);
  const [warmPoolPolicyCapabilities, setWarmPoolPolicyCapabilities] = useState<WarmPoolPolicyCapabilities>({});
  const [loadingReleases, setLoadingReleases] = useState(true);
  const [loadingTemplates, setLoadingTemplates] = useState(true);
  const [loadingInventory, setLoadingInventory] = useState(true);
  const loadingRef = useRef(true);
  const [poolForm] = Form.useForm();
  const [clusterPoolForm] = Form.useForm();
  const [warmPoolPolicyForm] = Form.useForm();
  const [warmPoolPolicyModal, setWarmPoolPolicyModal] = useState<{
    open: boolean;
    policy: WarmPoolPolicy | null;
  }>({ open: false, policy: null });
  const [clusterPoolModal, setClusterPoolModal] = useState<{
    open: boolean;
    release: TemplateRelease | null;
    config: ClusterPoolConfig | null;
  }>({
    open: false,
    release: null,
    config: null,
  });
  const [poolModal, setPoolModal] = useState<{ open: boolean; name: string; mode: 'create' | 'manage' }>({
    open: false,
    name: '',
    mode: 'create',
  });
  const [pullForm] = Form.useForm();
  const [pullModal, setPullModal] = useState<{ open: boolean; template: TemplateAvailabilityRow | null }>({
    open: false,
    template: null,
  });
  const [retagForm] = Form.useForm();
  const [retagModal, setRetagModal] = useState<{ open: boolean; release: TemplateRelease | null }>({
    open: false,
    release: null,
  });
  const [expandedTemplateRows, setExpandedTemplateRows] = useState<string[]>([]);
  const [templateOperations, setTemplateOperations] = useState<Set<string>>(() => new Set());
  const [buildingSnapshot, setBuildingSnapshot] = useState<string | null>(null);
  const [templateLifecycleForm] = Form.useForm();
  const [editingTemplateLifecycle, setEditingTemplateLifecycle] = useState<Template | null>(null);
  const [editingTemplateNetwork, setEditingTemplateNetwork] = useState<Template | null>(null);
  const [manualRefreshing, setManualRefreshing] = useState(false);
  const [refreshCooldown, setRefreshCooldown] = useState(false);
  const refreshCooldownRef = useRef(false);
  const refreshCooldownTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isDirectSubstrate = substrate === 'space';
  const supportsClusterPools = substrate === 'k8s';
  const canAdminTemplates = isManagingOrgAdmin(user, cluster);
  const canManageTemplateReleases = canAdminTemplates;

  const startTemplateOperation = (operation: string) => {
    setTemplateOperations((current) => new Set(current).add(operation));
  };

  const finishTemplateOperation = (operation: string) => {
    setTemplateOperations((current) => {
      const next = new Set(current);
      next.delete(operation);
      return next;
    });
  };

  const templateOperationRunning = (operation: string) => templateOperations.has(operation);

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

  const refreshTemplateReleases = async ({ showLoading = true }: RefreshOptions = {}) => {
    if (showLoading) setLoadingReleases(true);
    try {
      const items = await adminApi.get<TemplateRelease[]>('/v1/template-releases');
      setTemplateReleases(items);
      return items;
    } catch (err: unknown) {
      console.warn(errorMessage(err));
      throw new Error('failed to refresh template releases');
    } finally {
      if (showLoading) setLoadingReleases(false);
    }
  };

  const refreshBaseSnapshotPolicies = async (items: Template[]) => {
    const entries = await Promise.all(
      items.map(async (template) => {
        try {
          const policy = await adminApi.get<BaseSnapshotPolicy>(
            `/v1/templates/${encodedPathSegment(template.name)}/base-snapshot-policy`,
          );
          return [template.name, policy] as const;
        } catch {
          return null;
        }
      }),
    );
    setBaseSnapshotPolicies(Object.fromEntries(entries.filter(Boolean) as [string, BaseSnapshotPolicy][]));
  };

  const hydrateDirectTemplatePools = async (items: Template[]) => {
    if (!isDirectSubstrate || items.length === 0) {
      return items;
    }

    const statuses = await Promise.all(
      items.map(async (template) => {
        const status = await adminApi.get<TemplatePoolStatusResponse>(
          `/v1/templates/${encodedPathSegment(template.name)}/pool`,
        );
        return [template.name, status] as const;
      }),
    );
    const statusByTemplate = new Map(statuses);
    return items.map((template) => {
      const status = statusByTemplate.get(template.name);
      if (!status) return template;
      const observed = status.observed;
      const target = status.policy?.target_count ?? observed?.pool_target ?? null;
      return {
        ...template,
        pool_target: target,
        pool_current: observed?.pool_current ?? null,
      };
    });
  };

  const refreshTemplates = async ({
    showLoading = true,
    includeBaseSnapshotPolicies = false,
  }: RefreshOptions = {}) => {
    if (showLoading) setLoadingTemplates(true);
    try {
      const items = await hydrateDirectTemplatePools(await adminApi.get<Template[]>('/v1/templates'));
      setTemplates(items);
      const liveTemplateNames = new Set(items.map((template) => template.name));
      setExpandedTemplateRows((current) => current.filter((name) => liveTemplateNames.has(name)));
      if (includeBaseSnapshotPolicies) {
        await refreshBaseSnapshotPolicies(items);
      }
      return items;
    } catch (err: unknown) {
      console.warn(errorMessage(err));
      throw new Error('failed to refresh templates');
    } finally {
      if (showLoading) setLoadingTemplates(false);
    }
  };

  const refreshInventory = async ({ showLoading = true }: RefreshOptions = {}) => {
    if (showLoading) setLoadingInventory(true);
    try {
      const items = await adminApi.get<NodeTemplateInventory[]>('/v1/nodes/templates');
      setInventory(items);
      return items;
    } catch (err: unknown) {
      console.warn(errorMessage(err));
      // Keep the last rendered view when node-api inventory is briefly unavailable.
      throw new Error('failed to refresh template inventory');
    } finally {
      if (showLoading) setLoadingInventory(false);
    }
  };

  const refreshClusterPools = async () => {
    if (!supportsClusterPools) {
      setClusterPools([]);
      return [];
    }
    try {
      const items = await adminApi.get<ClusterPoolConfig[]>('/v1/warm-pools');
      setClusterPools(items);
      return items;
    } catch {
      throw new Error('failed to refresh cluster pools');
    }
  };

  const refreshWarmPoolPolicies = async () => {
    if (!supportsClusterPools) {
      setWarmPoolPolicies([]);
      return [];
    }
    try {
      const items = await adminApi.get<WarmPoolPolicy[]>('/v1/warm-pool-policies');
      setWarmPoolPolicies(items);
      return items;
    } catch {
      throw new Error('failed to refresh warm pool policies');
    }
  };

  const refreshWarmPoolPolicyCapabilities = async () => {
    if (!supportsClusterPools) {
      setWarmPoolPolicyCapabilities({});
      return {};
    }
    try {
      const capabilities = await adminApi.get<WarmPoolPolicyCapabilities>(
        '/v1/warm-pool-policy-capabilities',
      );
      setWarmPoolPolicyCapabilities(capabilities);
      return capabilities;
    } catch (err: unknown) {
      // During a rolling deploy, an older API replica may not have this optional endpoint yet.
      console.warn('failed to refresh warm pool policy capabilities', err);
      setWarmPoolPolicyCapabilities({});
      return {};
    }
  };

  const refreshCachedViews = async ({ showLoading = true }: RefreshOptions = {}) => {
    await Promise.all([
      refreshTemplateReleases({ showLoading }),
      refreshTemplates({ showLoading }),
      refreshInventory({ showLoading }),
      refreshClusterPools(),
      refreshWarmPoolPolicies(),
      refreshWarmPoolPolicyCapabilities(),
    ]);
    markViewRefreshed(TEMPLATES_VIEW_REFRESH_KEY);
  };

  const refresh = async ({
    showLoading = true,
    reconcile = false,
    includeBaseSnapshotPolicies = false,
  }: RefreshOptions = {}) => {
    await Promise.all([
      refreshTemplateReleases({ showLoading }),
      refreshTemplates({ showLoading, includeBaseSnapshotPolicies }),
      refreshInventory({ showLoading }),
      refreshClusterPools(),
      refreshWarmPoolPolicies(),
      refreshWarmPoolPolicyCapabilities(),
    ]);
    markViewRefreshed(TEMPLATES_VIEW_REFRESH_KEY);
  };

  const maybeRefresh = () => {
    if (loadingRef.current) {
      return;
    }
    if (document.hidden) {
      return;
    }
    void runLockedViewRefresh(
      TEMPLATES_VIEW_REFRESH_KEY,
      () => refreshCachedViews({ showLoading: false }),
      { staleMs: VIEW_REFRESH_STALE_MS },
    ).catch((err: unknown) => console.warn(errorMessage(err)));
  };

  useEffect(() => {
    loadingRef.current = loadingReleases || loadingTemplates || loadingInventory;
  }, [loadingReleases, loadingTemplates, loadingInventory]);

  useEffect(() => {
    void refresh({ includeBaseSnapshotPolicies: true }).catch((err: unknown) => message.error(errorMessage(err)));
    const interval = setInterval(maybeRefresh, VIEW_REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [substrate]);

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
      TEMPLATES_VIEW_REFRESH_KEY,
      () => refresh({
        showLoading: false,
        reconcile: true,
        includeBaseSnapshotPolicies: true,
      }),
      { force: true },
    ).then((ran) => {
      if (!ran) {
        message.info('Refresh already in progress');
        return;
      }
      message.success('Templates refreshed');
    }).catch((err: unknown) => {
      message.error(errorMessage(err));
    }).finally(() => {
      setManualRefreshing(false);
    });
  };

  const clusterPoolById = useMemo(() => {
    const pools = new Map<string, ClusterPoolConfig>();
    for (const pool of clusterPools) {
      pools.set(pool.id, pool);
    }
    return pools;
  }, [clusterPools]);

  const releaseByTemplateHash = useMemo(() => {
    const releases = new Map<string, TemplateRelease>();
    for (const release of templateReleases) {
      releases.set(releasePoolKey(release), release);
    }
    return releases;
  }, [templateReleases]);

  const releaseTemplateFilters = useMemo(
    () => tableFilterOptions(templateReleases.map((release) => release.template_name)),
    [templateReleases],
  );

  const releaseTagFilters = useMemo(
    () => tableFilterOptions(
      templateReleases.flatMap((release) => visibleTemplateTags(release.tags, release.content_hash)),
    ),
    [templateReleases],
  );

  const openRetagModal = (release: TemplateRelease) => {
    if (!canManageTemplateReleases) return;
    const visibleTags = visibleTemplateTags(release.tags, release.content_hash);
    retagForm.setFieldsValue({ tag: visibleTags[0] ?? DEFAULT_TEMPLATE_TAG });
    setRetagModal({ open: true, release });
  };

  const closeRetagModal = () => {
    setRetagModal({ open: false, release: null });
    retagForm.resetFields();
  };

  const handleRetagRelease = async (values: { tag: string }) => {
    const release = retagModal.release;
    if (!release || !canManageTemplateReleases) return;

    try {
      await adminApi.put(`/v1/template-releases/${release.id}/tag`, {
        tag: values.tag.trim(),
      });
      closeRetagModal();
      await refreshTemplateReleases({ showLoading: false });
      message.success('Template release tag updated');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleDeleteReleaseTag = async (release: TemplateRelease, tag: string) => {
    if (!canManageTemplateReleases || !canDeleteVisibleTemplateTag(tag)) return;
    const operation = `delete-tag:${release.id}:${tag}`;
    startTemplateOperation(operation);
    try {
      await adminApi.delete(`/v1/template-releases/${release.id}/tags/${encodedPathSegment(tag)}`);
      await refreshTemplateReleases({ showLoading: false });
      message.success('Template release tag deleted');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const handleDeleteRelease = async (release: TemplateRelease) => {
    if (!canManageTemplateReleases) return;
    const operation = `delete-release:${release.id}`;
    startTemplateOperation(operation);
    try {
      await adminApi.delete(`/v1/template-releases/${release.id}`);
      await Promise.all([
        refreshTemplateReleases({ showLoading: false }),
        refreshTemplates({ showLoading: false }),
      ]);
      message.success('Template release deleted');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const openClusterPoolModal = (release: TemplateRelease, configId?: string) => {
    const existing = configId ? clusterPoolById.get(configId) : undefined;
    clusterPoolForm.setFieldsValue({
      target_ready_slots: existing?.target_ready_slots ?? 3,
      refill_per_tick: existing?.refill_per_tick ?? 1,
      max_warming_slots: existing?.max_warming_slots ?? 2,
      vcpu: existing?.vcpu ?? 2,
      memory_mib: existing?.memory_mib ?? 1024,
      disk_mib: existing?.disk_mib,
      namespace: existing?.namespace,
      enabled: existing?.enabled ?? true,
    });
    setClusterPoolModal({ open: true, release, config: existing ?? null });
  };

  const closeClusterPoolModal = () => {
    setClusterPoolModal({ open: false, release: null, config: null });
    clusterPoolForm.resetFields();
  };

  const handleClusterPoolSubmit = async (values: {
    target_ready_slots: number;
    refill_per_tick: number;
    max_warming_slots: number;
    vcpu: number;
    memory_mib: number;
    disk_mib?: number | null;
    namespace?: string;
    enabled?: boolean;
  }) => {
    const release = clusterPoolModal.release;
    if (!release) return;

    try {
      await adminApi.put('/v1/warm-pools', {
        template_name: release.template_name,
        content_hash: release.content_hash,
        vcpu: values.vcpu,
        memory_mib: values.memory_mib,
        ...(warmPoolPolicyCapabilities.disk_capacity && !formNumberIsBlank(values.disk_mib)
          ? { disk_mib: values.disk_mib }
          : {}),
        ...(warmPoolPolicyCapabilities.disk_capacity &&
        clusterPoolModal.config?.disk_mib !== undefined &&
        formNumberIsBlank(values.disk_mib)
          ? { reset_disk_to_template_default: true }
          : {}),
        target_ready_slots: values.target_ready_slots,
        refill_per_tick: values.refill_per_tick,
        max_warming_slots: values.max_warming_slots,
        namespace: values.namespace?.trim() || undefined,
        enabled: values.enabled ?? true,
      });
      closeClusterPoolModal();
      await Promise.all([
        refreshClusterPools(),
        refreshTemplateReleases({ showLoading: false }),
      ]);
      message.success('Cluster pool config saved');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleDisableClusterPool = async () => {
    const existing = clusterPoolModal.config;
    if (!existing) return;

    try {
      if (existing.managed_by === 'autoscaler') {
        await adminApi.put('/v1/warm-pools', {
          template_name: existing.template_name,
          content_hash: existing.content_hash,
          vcpu: existing.vcpu,
          memory_mib: existing.memory_mib,
          ...(existing.disk_mib !== undefined ? { disk_mib: existing.disk_mib } : {}),
          target_ready_slots: 0,
          refill_per_tick: existing.refill_per_tick,
          max_warming_slots: existing.max_warming_slots,
          namespace: existing.namespace,
          enabled: false,
        });
      } else {
        await adminApi.delete(`/v1/warm-pools/${existing.id}`);
      }
      closeClusterPoolModal();
      await Promise.all([
        refreshClusterPools(),
        refreshTemplateReleases({ showLoading: false }),
      ]);
      message.success('Cluster pool disabled');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const openWarmPoolPolicyModal = (policy: WarmPoolPolicy | null = null) => {
    warmPoolPolicyForm.setFieldsValue({
      template_name: policy?.template_name,
      vcpu: policy?.vcpu ?? 2,
      memory_mib: policy?.memory_mib ?? 8192,
      disk_mib: policy?.disk_mib,
      min_ready_slots: policy?.min_ready_slots ?? 5,
      max_ready_slots: policy?.max_ready_slots ?? 30,
      refill_per_tick: policy?.refill_per_tick ?? 5,
      max_warming_slots: policy?.max_warming_slots ?? 10,
      retain_releases: policy?.retain_releases ?? 2,
      disk_io_max_operations_per_sec: policy?.disk_io_max_operations_per_sec,
      disk_io_max_bandwidth_mib_per_sec: policy?.disk_io_max_bandwidth_mib_per_sec,
      enabled: policy?.enabled ?? true,
    });
    setWarmPoolPolicyModal({ open: true, policy });
  };

  const closeWarmPoolPolicyModal = () => {
    setWarmPoolPolicyModal({ open: false, policy: null });
    warmPoolPolicyForm.resetFields();
  };

  const saveWarmPoolPolicy = async (values: {
    template_name: string;
    vcpu: number;
    memory_mib: number;
    disk_mib?: number | null;
    min_ready_slots: number;
    max_ready_slots: number;
    refill_per_tick: number;
    max_warming_slots: number;
    retain_releases: number;
    disk_io_max_operations_per_sec?: number | null;
    disk_io_max_bandwidth_mib_per_sec?: number | null;
    enabled?: boolean;
  }, resetDiskIoToDefault = false, resetDiskToTemplateDefault = false) => {
    const hasDiskIoLimits = !formNumberIsBlank(values.disk_io_max_operations_per_sec)
      && !formNumberIsBlank(values.disk_io_max_bandwidth_mib_per_sec);
    await adminApi.put('/v1/warm-pool-policies', {
      template_name: values.template_name,
      vcpu: values.vcpu,
      memory_mib: values.memory_mib,
      ...(warmPoolPolicyCapabilities.disk_capacity && !formNumberIsBlank(values.disk_mib)
        ? { disk_mib: values.disk_mib }
        : {}),
      ...(resetDiskToTemplateDefault ? { reset_disk_to_template_default: true } : {}),
      min_ready_slots: values.min_ready_slots,
      max_ready_slots: values.max_ready_slots,
      refill_per_tick: values.refill_per_tick,
      max_warming_slots: values.max_warming_slots,
      retain_releases: values.retain_releases,
      ...(hasDiskIoLimits ? {
        disk_io_max_operations_per_sec: values.disk_io_max_operations_per_sec,
        disk_io_max_bandwidth_mib_per_sec: values.disk_io_max_bandwidth_mib_per_sec,
      } : {}),
      ...(resetDiskIoToDefault ? { reset_disk_io_to_default: true } : {}),
      enabled: values.enabled ?? true,
    });
    closeWarmPoolPolicyModal();
    await Promise.all([
      refreshWarmPoolPolicies(),
      refreshTemplateReleases({ showLoading: false }),
    ]);
    message.success('Warm pool policy saved');
  };

  const handleWarmPoolPolicySubmit = async (values: {
    template_name: string;
    vcpu: number;
    memory_mib: number;
    disk_mib?: number | null;
    min_ready_slots: number;
    max_ready_slots: number;
    refill_per_tick: number;
    max_warming_slots: number;
    retain_releases: number;
    disk_io_max_operations_per_sec?: number | null;
    disk_io_max_bandwidth_mib_per_sec?: number | null;
    enabled?: boolean;
  }) => {
    try {
      const policy = warmPoolPolicyModal.policy;
      const hadExplicitDiskIo = policy?.disk_io_max_operations_per_sec !== undefined
        && policy.disk_io_max_bandwidth_mib_per_sec !== undefined;
      const clearedDiskIo = formNumberIsBlank(values.disk_io_max_operations_per_sec)
        && formNumberIsBlank(values.disk_io_max_bandwidth_mib_per_sec);
      const diskIoControlsAvailable = warmPoolPolicyCapabilities.disk_io !== undefined;
      const hadExplicitDisk = policy?.disk_mib !== undefined;
      const clearedDisk = formNumberIsBlank(values.disk_mib);
      await saveWarmPoolPolicy(
        values,
        Boolean(diskIoControlsAvailable && hadExplicitDiskIo && clearedDiskIo),
        Boolean(warmPoolPolicyCapabilities.disk_capacity && hadExplicitDisk && clearedDisk),
      );
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleDisableWarmPoolPolicy = async () => {
    const policy = warmPoolPolicyModal.policy;
    if (!policy) return;
    try {
      await saveWarmPoolPolicy({
        template_name: policy.template_name,
        vcpu: policy.vcpu,
        memory_mib: policy.memory_mib,
        disk_mib: policy.disk_mib,
        min_ready_slots: policy.min_ready_slots,
        max_ready_slots: policy.max_ready_slots,
        refill_per_tick: policy.refill_per_tick,
        max_warming_slots: policy.max_warming_slots,
        retain_releases: policy.retain_releases,
        enabled: false,
      });
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const openTemplateLifecycleModal = (template: Template) => {
    templateLifecycleForm.setFieldsValue({
      auto_lifecycle: policyToFormValue(template.default_auto_lifecycle),
    });
    setEditingTemplateLifecycle(template);
  };

  const closeTemplateLifecycleModal = () => {
    setEditingTemplateLifecycle(null);
    templateLifecycleForm.resetFields();
  };

  const handleTemplateLifecycleUpdate = async (values: { auto_lifecycle?: any }) => {
    if (!editingTemplateLifecycle) return;
    try {
      const templateName = editingTemplateLifecycle.name;
      await adminApi.patch(`/v1/templates/${encodedPathSegment(templateName)}`, {
        default_auto_lifecycle: formValueToPolicy(values.auto_lifecycle),
      });
      closeTemplateLifecycleModal();
      await refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true });
      message.success('Template lifecycle policy updated');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleTemplateLifecycleClear = async () => {
    if (!editingTemplateLifecycle) return;
    try {
      const templateName = editingTemplateLifecycle.name;
      await adminApi.patch(`/v1/templates/${encodedPathSegment(templateName)}`, {
        default_auto_lifecycle: disabledAutoLifecycle(),
      });
      closeTemplateLifecycleModal();
      await refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true });
      message.success('Template lifecycle policy cleared');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleTemplateNetworkSave = async (policy: NetworkPolicyV1) => {
    if (!editingTemplateNetwork) return;
    if (networkPoliciesEqual(editingTemplateNetwork.network_policy, policy)) {
      setEditingTemplateNetwork(null);
      return;
    }

    try {
      const templateName = editingTemplateNetwork.name;
      await adminApi.patch(`/v1/templates/${encodedPathSegment(templateName)}`, {
        network_policy: policy,
      });
      setEditingTemplateNetwork(null);
      await refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true });
      message.success('Template network policy updated');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleDelete = async (name: string) => {
    const operation = `delete-template:${name}`;
    startTemplateOperation(operation);
    try {
      const response = await adminApi.delete<TemplateOperationResponse>(
        `/v1/templates/${encodedPathSegment(name)}`,
        { 'Idempotency-Key': idempotencyKey() },
      );
      await waitForPublicOperation(response.operation_id, 120_000);
      await refresh({ showLoading: false, includeBaseSnapshotPolicies: true });
      message.success(`Template ${name} deleted`);
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const openPoolModal = (template: Template) => {
    poolForm.setFieldsValue({
      target_count: template.pool_target ?? template.pool_current ?? 3,
      boot_mode: template.has_base_snapshot ? 'snapshot' : 'cold',
    });
    setPoolModal({
      open: true,
      name: template.name,
      mode: hasObservedPoolSandboxes(template) ? 'manage' : 'create',
    });
  };

  const closePoolModal = () => {
    setPoolModal({ open: false, name: '', mode: 'create' });
    poolForm.resetFields();
  };

  const handlePoolSubmit = async (values: { target_count: number; boot_mode: 'cold' | 'snapshot' }) => {
    if (!poolModal.name) return;
    try {
      await adminApi.post(`/v1/templates/${encodedPathSegment(poolModal.name)}/pool`, values);
      closePoolModal();
      await refreshTemplates({ showLoading: false });
      message.success('Template pool policy saved');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleDeletePool = async (name: string) => {
    try {
      await adminApi.delete(`/v1/templates/${encodedPathSegment(name)}/pool`);
      closePoolModal();
      await refreshTemplates({ showLoading: false });
      message.success('Template pool policy deleted');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    }
  };

  const handleToggleBaseSnapshot = async (template: Template, enabled: boolean) => {
    setBuildingSnapshot(template.name);
    try {
      await adminApi.patch(`/v1/templates/${encodedPathSegment(template.name)}/base-snapshot-policy`, {
        ensure_base_snapshot: enabled,
      });
      await refreshTemplates({ showLoading: false, includeBaseSnapshotPolicies: true });
      message.success(enabled ? 'Base snapshot policy enabled' : 'Base snapshot policy disabled');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      setBuildingSnapshot(null);
    }
  };

  const availabilityRows = useMemo<TemplateAvailabilityRow[]>(() => {
    const rows = new Map<string, TemplateAvailabilityRow>();
    const templateByName = new Map(templates.map((template) => [template.name, template]));

    for (const entry of inventory) {
      if (!entry.reachable || entry.error) continue;
      for (const build of entry.builds) {
        const key = `${build.name}:${build.content_hash || DEFAULT_TEMPLATE_TAG}`;
        const release = releaseByTemplateHash.get(key) ?? null;
        const template = templateByName.get(build.name);
        const row = rows.get(key) || {
          key,
          name: build.name,
          content_hash: build.content_hash,
          tags: release ? visibleTemplateTags(release.tags, release.content_hash) : [],
          build_date: build.build_date,
          runtime: build.runtime,
          arch: build.arch,
          artifact_size_bytes: build.artifact_size_bytes,
          cache_size_bytes: build.cache_size_bytes,
          disk_size_mb: build.disk_size_mb,
          vcpu: template?.vcpu ?? null,
          memory_mib: template?.memory_mib ?? null,
          release,
          nodes: [],
        };
        if (!row.nodes.some((availability) => availability.node.id === entry.node.id)) {
          row.nodes.push({
            node: entry.node,
            snapshots: build.snapshots,
          });
        }
        rows.set(key, row);
      }
    }

    return Array.from(rows.values()).sort((a, b) => {
      const byName = a.name.localeCompare(b.name);
      if (byName !== 0) return byName;
      return shortHash(a.content_hash).localeCompare(shortHash(b.content_hash));
    });
  }, [inventory, releaseByTemplateHash, templates]);

  const inventoryTemplateFilters = useMemo(
    () => tableFilterOptions(availabilityRows.map((row) => row.name)),
    [availabilityRows],
  );

  const inventoryTagFilters = useMemo(
    () => tableFilterOptions(availabilityRows.flatMap((row) => row.tags)),
    [availabilityRows],
  );

  const templateCapableNodes = useMemo<SpaceNode[]>(() => {
    const nodes = new Map<string, SpaceNode>();
    for (const entry of inventory) {
      if (!entry.reachable || entry.error) continue;
      if (entry.node.role !== 'worker' && entry.node.role !== 'builder') continue;
      // The pull mutation hits POST /nodes/:id/template-pulls, which is
      // direct-substrate-only on the backend. Exclude k8s rows so the picker
      // can't offer a target that would 501 on submit.
      if (entry.node.managed_by !== 'space') continue;
      nodes.set(entry.node.id, entry.node);
    }
    return Array.from(nodes.values()).sort((a, b) => nodeLabel(a).localeCompare(nodeLabel(b)));
  }, [inventory]);

  const reconcileTemplateInventory = async (nodeIds: string[], templateName: string) => {
    if (!canAdminTemplates || nodeIds.length === 0) return false;
    try {
      await adminApi.post('/v1/nodes:reconcile', {
        node_ids: nodeIds,
        refs: [{ facet: 'template-inventory', element: templateName }],
      });
      await refreshInventory({ showLoading: false });
      message.success('Template inventory reconcile queued');
      return true;
    } catch (err: unknown) {
      message.error(errorMessage(err) || 'Failed to queue template inventory reconcile');
      return false;
    }
  };

  const openPullModal = (template: TemplateAvailabilityRow) => {
    const existingNodeIds = new Set(template.nodes.map((availability) => availability.node.id));
    const targetNode = templateCapableNodes.find((node) => !existingNodeIds.has(node.id));
    pullForm.setFieldsValue({
      source_node_id: template.nodes[0]?.node.id,
      target_node_id: targetNode?.id,
    });
    setPullModal({ open: true, template });
  };

  const handlePullTemplate = async (values: { source_node_id: string; target_node_id: string }) => {
    const template = pullModal.template;
    if (!template) return;

    const operation = `pull:${template.key}`;
    startTemplateOperation(operation);
    try {
      setPullModal({ open: false, template: null });
      pullForm.resetFields();
      const source = template.nodes.find((availability) => availability.node.id === values.source_node_id);
      const snapshot = source?.snapshots[0] ?? template.nodes.flatMap((availability) => availability.snapshots)[0];
      await adminApi.post(`/v1/nodes/${values.target_node_id}/template-pulls`, {
        template_id: template.name,
        content_hash: template.content_hash || undefined,
        source_node_id: values.source_node_id,
        include_snapshot: Boolean(snapshot),
        snapshot_vcpu: snapshot?.vcpu,
        snapshot_memory_mib: snapshot?.memory_mib,
        skip_if_exists: true,
        block_on_snapshot_policy_achieved: true,
      });
      await Promise.all([
        refreshInventory({ showLoading: false }),
        refreshTemplateReleases({ showLoading: false }),
      ]);
      message.success('Template pull queued');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const handleDeleteTemplateFromNode = async (template: TemplateAvailabilityRow, node: SpaceNode) => {
    const operation = `delete:${template.key}:${node.id}`;
    startTemplateOperation(operation);
    try {
      const query = template.content_hash
        ? `?content_hash=${encodeURIComponent(template.content_hash)}`
        : '';
      await adminApi.delete(`/v1/nodes/${node.id}/templates/${encodedPathSegment(template.name)}${query}`);
      await refreshInventory({ showLoading: false });
      message.success('Template deleted from node');
    } catch (err: unknown) {
      message.error(errorMessage(err));
    } finally {
      finishTemplateOperation(operation);
    }
  };

  const renderReleaseTagControls = (release: TemplateRelease, allowActions = true) => {
    const actionsAllowed = allowActions && canManageTemplateReleases;
    const visibleTags = visibleTemplateTags(release.tags, release.content_hash);
    return (
      <Space wrap size={4}>
        {visibleTags.length > 0 ? visibleTags.map((tag) => {
          const deletable = actionsAllowed && canDeleteVisibleTemplateTag(tag);
          const deleteTagOperation = `delete-tag:${release.id}:${tag}`;
          const deleteTagRunning = templateOperationRunning(deleteTagOperation);
          return (
            <Tag key={tag} color={tag === DEFAULT_TEMPLATE_TAG ? 'green' : 'blue'} style={{ marginRight: 0 }}>
              <Space size={2}>
                <span>{tag}</span>
                {deletable ? (
                  <Popconfirm
                    title={`Delete tag ${tag}?`}
                    okText="Delete"
                    okType="danger"
                    onConfirm={() => handleDeleteReleaseTag(release, tag)}
                  >
                    <Button
                      type="text"
                      size="small"
                      danger
                      icon={<DeleteOutlined />}
                      loading={deleteTagRunning}
                      disabled={deleteTagRunning}
                      aria-label={`Delete tag ${tag}`}
                      style={{ width: 16, height: 16, minWidth: 16, padding: 0 }}
                    />
                  </Popconfirm>
                ) : null}
              </Space>
            </Tag>
          );
        }) : <Text type="secondary">untagged</Text>}
        {actionsAllowed ? (
          <Tooltip title="Assign or move tag">
            <Button
              type="text"
              size="small"
              icon={<TagOutlined />}
              aria-label="Assign or move release tag"
              onClick={() => openRetagModal(release)}
            />
          </Tooltip>
        ) : null}
      </Space>
    );
  };

  const templateNameFilters = useMemo(
    () => tableFilterOptions(templates.map((template) => template.name)),
    [templates],
  );

  const templateColumns = useMemo<ColumnsType<Template>>(() => {
    const columns: ColumnsType<Template> = [
      {
        title: 'Name',
        dataIndex: 'name',
        width: 140,
        fixed: 'left',
        defaultSortOrder: 'ascend',
        filters: templateNameFilters,
        filterSearch: true,
        onFilter: (value, record) => record.name === String(value),
        sorter: (a, b) => a.name.localeCompare(b.name),
        render: (value: string) => <strong>{value}</strong>,
      },
      {
        title: 'Hash',
        dataIndex: 'content_hash',
        width: 110,
        render: (value: string | null) => (
          <Tooltip title={value ?? DEFAULT_TEMPLATE_TAG}>
            <code>{shortHash(value)}</code>
          </Tooltip>
        ),
      },
      {
        title: 'Runtime',
        dataIndex: 'runtime',
        width: 90,
        render: (value: string | null) => value || '—',
      },
      {
        title: 'Disk',
        dataIndex: 'disk_size_mb',
        width: 90,
        render: (_, record) => renderDiskCell(record),
      },
      {
        title: 'Built',
        dataIndex: 'built_at',
        width: 115,
        render: (value: string | null) => value ? formatShortDateTime(value) : '—',
      },
      {
        title: 'Snap',
        dataIndex: 'has_base_snapshot',
        width: 58,
        render: (value: boolean | null) => (
          <Tooltip title={value ? 'Base snapshot observed' : 'No base snapshot observed'}>
            {value ? (
              <CheckCircleFilled style={{ color: '#389e0d', fontSize: 13 }} />
            ) : (
              <ClockCircleOutlined style={{ color: '#8c8c8c', fontSize: 13 }} />
            )}
          </Tooltip>
        ),
      },
      {
        title: 'Default Auto',
        dataIndex: 'default_auto_lifecycle',
        width: 155,
        render: (_, record) => (
          <Space size={4}>
            <Tooltip title={summarizeAutoLifecycle(record.default_auto_lifecycle)}>
              <Tag color={hasAutoLifecyclePolicy(record.default_auto_lifecycle) ? 'blue' : undefined} style={{ marginRight: 0 }}>
                {hasAutoLifecyclePolicy(record.default_auto_lifecycle) ? 'custom' : 'disabled'}
              </Tag>
            </Tooltip>
            <Button type="text" size="small" onClick={() => openTemplateLifecycleModal(record)}>
              Edit
            </Button>
          </Space>
        ),
      },
      {
        title: 'Network',
        dataIndex: 'network_policy',
        width: 150,
        render: (_, record) => (
          <Space size={4}>
            <Tag
              color={compactNetworkPolicyLabel(record.network_policy) === 'open' ? 'green' : undefined}
              style={{ marginRight: 0 }}
            >
              {compactNetworkPolicyLabel(record.network_policy)}
            </Tag>
            <NetworkPolicyGenerationTag
              desired={record.network_policy_generation}
              applied={record.network_policy_status?.observed_generation}
              policyLabel={compactNetworkPolicyLabel(record.network_policy)}
              desiredSource="template"
              observedSource={record.network_policy_status?.observed_source}
            />
            <Button type="text" size="small" onClick={() => setEditingTemplateNetwork(record)}>
              Edit
            </Button>
          </Space>
        ),
      },
      {
        title: 'Base Snap',
        width: 110,
        render: (_, record) => {
          const policy = baseSnapshotPolicies[record.name];
          const desired = policy?.ensure_base_snapshot ?? false;
          return (
            <Tooltip title={baseSnapshotTooltip(record, desired)}>
              <Switch
                size="small"
                checked={desired}
                loading={buildingSnapshot === record.name}
                onChange={(checked) => handleToggleBaseSnapshot(record, checked)}
              />
            </Tooltip>
          );
        },
      },
    ];

    if (isDirectSubstrate) {
      columns.push({
        title: 'Pool',
        width: 145,
        render: (_, record) => {
          const hasPool = hasObservedPoolSandboxes(record);
          const current = record.pool_current ?? 0;
          const target = record.pool_target;
          return (
            <Space size={4}>
              {hasPool ? (
                <Tooltip title={target !== null && target !== undefined ? `current ${current} / target ${target}` : `current ${current}`}>
                  <Tag color="purple" style={{ marginRight: 0 }}>
                    {target !== null && target !== undefined ? `${current}/${target}` : `${current}`}
                  </Tag>
                </Tooltip>
              ) : (
                <Text type="secondary">No pool</Text>
              )}
              <Button type="text" size="small" onClick={() => openPoolModal(record)}>
                {hasPool ? 'Manage' : 'Create'}
              </Button>
            </Space>
          );
        },
      });
    }

    columns.push({
      title: 'Actions',
      fixed: 'right',
      width: 70,
      render: (_, record) => (
        <Popconfirm
          title={`Delete template ${record.name}?`}
          okText="Delete"
          okType="danger"
          onConfirm={() => handleDelete(record.name)}
        >
          <Button
            type="text"
            size="small"
            danger
            icon={<DeleteOutlined />}
            loading={templateOperationRunning(`delete-template:${record.name}`)}
            aria-label="Delete template"
          />
        </Popconfirm>
      ),
    });

    return columns;
  }, [
    templateNameFilters,
    baseSnapshotPolicies,
    buildingSnapshot,
    isDirectSubstrate,
    expandedTemplateRows,
    templateOperations,
  ]);

  const warmPoolPolicyColumns: ColumnsType<WarmPoolPolicy> = [
    {
      title: 'Template',
      dataIndex: 'template_name',
      width: 180,
      render: (value: string, record) => (
        <Space size={4}>
          <strong>{value}</strong>
          <Text type="secondary" style={{ fontSize: 12 }}>({record.runtime})</Text>
        </Space>
      ),
    },
    {
      title: 'Shape',
      width: 135,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <span>{record.vcpu}c / {record.memory_mib} MiB</span>
          {warmPoolPolicyCapabilities.disk_capacity ? (
            <Text type="secondary">
              {record.disk_mib ? `${record.disk_mib} MiB disk` : 'Template disk'}
            </Text>
          ) : null}
        </Space>
      ),
    },
    ...(warmPoolPolicyCapabilities.disk_io ? [{
      title: 'Disk I/O',
      width: 190,
      render: (_: unknown, record: WarmPoolPolicy) => {
        const diskIo = warmPoolPolicyCapabilities.disk_io!;
        if (record.disk_io_max_operations_per_sec !== undefined
          && record.disk_io_max_bandwidth_mib_per_sec !== undefined) {
          return `${record.disk_io_max_operations_per_sec.toLocaleString()} IOPS / ${record.disk_io_max_bandwidth_mib_per_sec} MiB/s`;
        }
        return (
          <Text type="secondary">
            Default ({diskIo.default_operations_per_sec.toLocaleString()} IOPS / {diskIo.default_bandwidth_mib_per_sec} MiB/s)
          </Text>
        );
      },
    }] : []),
    {
      title: 'Ready / release',
      width: 135,
      render: (_, record) => `${record.min_ready_slots} - ${record.max_ready_slots}`,
    },
    {
      title: 'Refill / tick',
      dataIndex: 'refill_per_tick',
      width: 105,
    },
    {
      title: 'Warming slots',
      dataIndex: 'max_warming_slots',
      width: 120,
    },
    {
      title: 'Retained releases',
      dataIndex: 'retain_releases',
      width: 135,
    },
    {
      title: 'Baseline slots',
      width: 125,
      render: (_, record) => record.min_ready_slots * record.retain_releases,
    },
    {
      title: 'Status',
      dataIndex: 'enabled',
      width: 105,
      render: (enabled: boolean) => (
        <Tag color={enabled ? 'green' : undefined}>{enabled ? 'Enabled' : 'Disabled'}</Tag>
      ),
    },
    {
      title: 'Actions',
      width: 70,
      render: (_, record) => (
        canAdminTemplates ? (
          <Button type="text" size="small" onClick={() => openWarmPoolPolicyModal(record)}>
            Edit
          </Button>
        ) : <Text type="secondary">—</Text>
      ),
    },
  ];

  const releaseColumns: ColumnsType<TemplateRelease> = [
    {
      title: 'Template',
      dataIndex: 'template_name',
      width: 180,
      defaultSortOrder: 'ascend',
      filters: releaseTemplateFilters,
      filterSearch: true,
      onFilter: (value, record) => record.template_name === String(value),
      sorter: (a, b) => a.template_name.localeCompare(b.template_name),
      render: (value: string, record) => (
        <Space size={4} wrap={false}>
          <strong style={{ whiteSpace: 'nowrap' }}>{value}</strong>
          <Text type="secondary" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>
            ({record.runtime || '—'})
          </Text>
        </Space>
      ),
    },
    {
      title: 'Tags',
      dataIndex: 'tags',
      width: 320,
      filters: releaseTagFilters,
      filterSearch: true,
      onFilter: (value, record) =>
        visibleTemplateTags(record.tags, record.content_hash).includes(String(value)),
      render: (_tags: string[], record) => renderReleaseTagControls(record),
    },
    {
      title: 'Hash',
      dataIndex: 'content_hash',
      width: 130,
      render: (value: string) => (
        <Tooltip title={value}>
          <code>{shortHash(value)}</code>
        </Tooltip>
      ),
    },
    {
      title: 'Created',
      dataIndex: 'created_at',
      width: 125,
      render: (value: string) => formatShortDateTime(value),
    },
    {
      title: 'Warm Pool',
      dataIndex: 'warm_pools',
      width: 230,
      filters: supportsClusterPools ? [
        { text: 'Enabled', value: 'enabled' },
        { text: 'Disabled', value: 'disabled' },
        { text: 'No pool', value: 'none' },
      ] : undefined,
      onFilter: supportsClusterPools
        ? (value, record) => matchesWarmPoolFilter(record.warm_pools, String(value))
        : undefined,
      render: (pools: TemplateReleaseWarmPool[], record) => {
        if (!supportsClusterPools) return <Text type="secondary">Kubernetes only</Text>;
        if (pools.length === 0) return <Text type="secondary">No warm pool</Text>;
        return (
          <Space wrap size={4}>
            {pools.map((pool) => {
              const tag = (
                <Tag
                  color={!pool.enabled ? undefined : pool.managed_by === 'autoscaler' ? 'purple' : 'gold'}
                  style={{ marginRight: 0 }}
                >
                  {warmPoolLabel(pool)} {pool.managed_by}
                </Tag>
              );
              return (
                <Tooltip
                  key={pool.id}
                  title={warmPoolTooltip(pool, Boolean(warmPoolPolicyCapabilities.disk_capacity))}
                >
                  {canAdminTemplates ? (
                    <Button
                      type="text"
                      size="small"
                      aria-label={`Manage ${pool.managed_by} pool`}
                      onClick={() => openClusterPoolModal(record, pool.id)}
                      style={{ height: 'auto', padding: 0 }}
                    >
                      {tag}
                    </Button>
                  ) : tag}
                </Tooltip>
              );
            })}
          </Space>
        );
      },
    },
    {
      title: 'Actions',
      fixed: 'right',
      width: 136,
      render: (_, record) => {
        const deleteReleaseRunning = templateOperationRunning(`delete-release:${record.id}`);
        return (
          <Space size={4}>
            {supportsClusterPools ? (
              record.warm_pools.length === 0 ? (
                <Button type="text" size="small" onClick={() => openClusterPoolModal(record)}>
                  Create Pool
                </Button>
              ) : <Text type="secondary">Select pool</Text>
            ) : (
              <Text type="secondary">—</Text>
            )}
            {canManageTemplateReleases ? (
              <Popconfirm
                title={`Unpublish ${record.template_name}@${shortHash(record.content_hash)}?`}
                description="Node-local cached copies are not deleted."
                okText="Unpublish"
                okType="danger"
                disabled={record.warm_pools.length > 0 || deleteReleaseRunning}
                onConfirm={() => handleDeleteRelease(record)}
              >
                <Tooltip title={record.warm_pools.length > 0 ? 'Delete pool policy first' : 'Unpublish release'}>
                  <span style={{ display: 'inline-block' }}>
                    <Button
                      type="text"
                      size="small"
                      danger
                      disabled={record.warm_pools.length > 0 || deleteReleaseRunning}
                      icon={<DeleteOutlined />}
                      loading={deleteReleaseRunning}
                      aria-label="Unpublish release"
                    />
                  </span>
                </Tooltip>
              </Popconfirm>
            ) : null}
          </Space>
        );
      },
    },
  ];

  const inventoryColumns: ColumnsType<TemplateAvailabilityRow> = [
    {
      title: 'Template',
      dataIndex: 'name',
      width: 130,
      filters: inventoryTemplateFilters,
      filterSearch: true,
      onFilter: (value, record) => record.name === String(value),
      render: (value: string) => <strong>{value}</strong>,
    },
    {
      title: 'Tags',
      dataIndex: 'tags',
      width: 170,
      filters: inventoryTagFilters,
      filterSearch: true,
      onFilter: (value, record) => record.tags.includes(String(value)),
      render: (_tags: string[], record) =>
        record.release ? renderReleaseTagControls(record.release) : <Text type="secondary">unpublished</Text>,
    },
    {
      title: 'Hash',
      dataIndex: 'content_hash',
      width: 110,
      render: (value: string | null) => <code>{shortHash(value)}</code>,
    },
    {
      title: 'Runtime',
      dataIndex: 'runtime',
      width: 90,
      render: (value: string | null) => value || '—',
    },
    {
      title: 'Built',
      dataIndex: 'build_date',
      width: 115,
      render: (value: string | null) => value ? formatShortDateTime(value) : '—',
    },
    {
      title: 'Disk',
      dataIndex: 'disk_size_mb',
      width: 90,
      render: (_, record) => renderDiskCell(record),
    },
    {
      title: 'Nodes',
      render: (_, record) => {
        if (record.nodes.length === 0) return <Text type="secondary">No nodes</Text>;
        const sortedItems = [...record.nodes].sort((a, b) =>
          shortNodeIdentity(a.node).localeCompare(shortNodeIdentity(b.node)));
        return (
          <Space wrap size={4}>
            {sortedItems.map((availability) => {
              const node = availability.node;
              const nodeText = shortNodeIdentity(node);
              const color = roleTextColor(node.role);
              return (
                <Tag
                  key={node.id}
                  color={roleColor(node.role)}
                  style={{ marginRight: 0, paddingRight: node.managed_by === 'space' ? 2 : undefined }}
                >
                  <Space size={4}>
                    {nodeSnapshotIcon(availability)}
                    <code style={{ fontSize: 12 }}>{nodeText}</code>
                    {node.managed_by !== 'space' ? (
                      <span style={{ opacity: 0.7, fontSize: 11 }}>{node.managed_by}</span>
                    ) : (
                      <Popconfirm
                        title={`Delete ${record.name}@${shortHash(record.content_hash)} from ${node.name || node.node_api_endpoint || node.id}?`}
                        okText="Delete"
                        okType="danger"
                        onConfirm={() => handleDeleteTemplateFromNode(record, node)}
                      >
                        <Tooltip title="Delete from node">
                          <Button
                            type="text"
                            size="small"
                            icon={<DeleteOutlined />}
                            loading={templateOperationRunning(`delete:${record.key}:${node.id}`)}
                            aria-label="Delete from node"
                            style={{ width: 18, height: 18, minWidth: 18, padding: 0, color }}
                          />
                        </Tooltip>
                      </Popconfirm>
                    )}
                  </Space>
                </Tag>
              );
            })}
          </Space>
        );
      },
    },
    {
      title: 'Actions',
      width: 52,
      render: (_, record) => {
        const existingNodeIds = new Set(record.nodes.map((availability) => availability.node.id));
        const hasTarget = templateCapableNodes.some((node) => !existingNodeIds.has(node.id));
        return (
          <Tooltip title="Pull to node">
            <Button
              type="text"
              size="small"
              className="icon-action-btn"
              icon={<DownloadOutlined />}
              disabled={!hasTarget}
              loading={templateOperationRunning(`pull:${record.key}`)}
              aria-label="Pull to node"
              onClick={() => openPullModal(record)}
            />
          </Tooltip>
        );
      },
    },
  ];

  const pullTemplate = pullModal.template;
  const pullSourceNodes = pullTemplate?.nodes ?? [];
  const pullTargetNodes = pullTemplate
    ? templateCapableNodes.filter((node) => !pullTemplate.nodes.some((source) => source.node.id === node.id))
    : [];

  return (
    <div className="compact-table">
      <style>{`.compact-table .ant-table-cell { padding: 4px 6px !important; } .compact-table .icon-action-btn { padding-left: 6px; padding-right: 6px; } .compact-table .icon-action-btn .anticon { font-size: 18px; }`}</style>
      {supportsClusterPools ? (
        <>
          <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
            <div>
              <Title level={4} style={{ marginBottom: 0 }}>Rolling Warm Pool Policies</Title>
              <Text type="secondary">
                Deployment-like policies create release-specific pools; the baseline is minimum ready slots multiplied by retained releases.
              </Text>
            </div>
            {canAdminTemplates ? (
              <Button type="primary" onClick={() => openWarmPoolPolicyModal()}>
                New Policy
              </Button>
            ) : null}
          </Space>
          <Table
            columns={warmPoolPolicyColumns}
            dataSource={warmPoolPolicies}
            rowKey={(policy) => `${policy.template_name}:${policy.runtime}:${policy.vcpu}:${policy.memory_mib}`}
            loading={loadingReleases}
            size="small"
            pagination={false}
            style={{ marginTop: 12, marginBottom: 24 }}
            scroll={{ x: 1080 }}
          />
        </>
      ) : null}
      <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
        <Title level={4}>Template Releases</Title>
        <Button
          icon={<ReloadOutlined />}
          loading={manualRefreshing}
          disabled={refreshCooldown}
          onClick={handleManualRefresh}
        >
          Refresh
        </Button>
      </Space>

      <Table
        columns={releaseColumns}
        dataSource={templateReleases}
        rowKey="id"
        loading={loadingReleases}
        size="small"
        pagination={tablePagination('template releases')}
        scroll={{ x: 1120 }}
      />

      <Title level={4} style={{ marginTop: 24 }}>Templates</Title>
      <Table
        columns={templateColumns}
        dataSource={templates}
        rowKey="name"
        loading={loadingTemplates}
        size="small"
        pagination={tablePagination('templates')}
        scroll={{ x: isDirectSubstrate ? 1180 : 1000 }}
        expandable={isDirectSubstrate ? {
          expandedRowKeys: expandedTemplateRows,
          onExpandedRowsChange: (keys) => setExpandedTemplateRows(keys.map(String)),
          expandedRowRender: (record) => <PoolSandboxesPanel templateName={record.name} />,
          rowExpandable: hasObservedPoolSandboxes,
          columnWidth: 34,
          fixed: 'left',
          expandIcon: ({ expanded, expandable, onExpand, record }) => {
            if (!expandable) {
              return <span style={{ display: 'inline-block', width: 22 }} />;
            }
            return (
              <Tooltip title={expanded ? 'Hide pool sandboxes' : 'Show pool sandboxes'}>
                <Button
                  type="text"
                  size="small"
                  icon={expanded ? <DownOutlined /> : <RightOutlined />}
                  aria-label={expanded ? 'Collapse pool sandboxes' : 'Expand pool sandboxes'}
                  onClick={(event) => onExpand(record, event)}
                  style={{ width: 22, height: 22, minWidth: 22, padding: 0 }}
                />
              </Tooltip>
            );
          },
        } : undefined}
      />

      <Title level={4} style={{ marginTop: 24 }}>Node Availability</Title>
      <Table
        columns={inventoryColumns}
        dataSource={availabilityRows}
        rowKey="key"
        loading={loadingInventory}
        size="small"
        pagination={tablePagination('template builds')}
        scroll={{ x: 1000 }}
      />

      <Modal
        title={warmPoolPolicyModal.policy
          ? `Edit rolling policy for "${warmPoolPolicyModal.policy.template_name}"`
          : 'Create rolling warm pool policy'}
        open={warmPoolPolicyModal.open}
        onCancel={closeWarmPoolPolicyModal}
        footer={[
          warmPoolPolicyModal.policy?.enabled ? (
            <Popconfirm
              key="disable"
              title="Disable this policy and drain its autoscaled release pools?"
              okText="Disable"
              okType="danger"
              onConfirm={handleDisableWarmPoolPolicy}
            >
              <Button danger>Disable Policy</Button>
            </Popconfirm>
          ) : null,
          <Button key="cancel" onClick={closeWarmPoolPolicyModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => warmPoolPolicyForm.submit()}>
            Save
          </Button>,
        ]}
        destroyOnHidden
      >
        <Form form={warmPoolPolicyForm} layout="vertical" onFinish={handleWarmPoolPolicySubmit}>
          <Form.Item name="template_name" label="Template" rules={[{ required: true }]}>
            <Select
              showSearch
              disabled={Boolean(warmPoolPolicyModal.policy)}
              options={templates.map((template) => ({ value: template.name, label: template.name }))}
              placeholder="Select a template"
            />
          </Form.Item>
          <Space.Compact block>
            <Form.Item name="vcpu" label="vCPU" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={1} max={64} disabled={Boolean(warmPoolPolicyModal.policy)} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="memory_mib" label="Memory (MiB)" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={128} max={262144} disabled={Boolean(warmPoolPolicyModal.policy)} style={{ width: '100%' }} />
            </Form.Item>
          </Space.Compact>
          {warmPoolPolicyCapabilities.disk_capacity ? (
            <Form.Item
              name="disk_mib"
              label="Disk capacity (MiB)"
              extra="Leave blank for the template-baked size. Changing this drains and refills existing warm slots."
            >
              <InputNumber min={1} max={2147483647} placeholder="Template default" style={{ width: '100%' }} />
            </Form.Item>
          ) : null}
          <Space.Compact block>
            <Form.Item name="min_ready_slots" label="Minimum Ready / Release" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="max_ready_slots" label="Maximum Ready / Release" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
          </Space.Compact>
          <Space.Compact block>
            <Form.Item name="refill_per_tick" label="Refill Per Tick" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="max_warming_slots" label="Warming Slots" rules={[{ required: true }]} style={{ width: '50%' }}>
              <InputNumber min={0} max={1000} style={{ width: '100%' }} />
            </Form.Item>
          </Space.Compact>
          <Form.Item name="retain_releases" label="Retained Releases" rules={[{ required: true }]}>
            <InputNumber min={1} max={50} style={{ width: '100%' }} />
          </Form.Item>
          {warmPoolPolicyCapabilities.disk_io ? (
            <>
              <Space.Compact block>
                <Form.Item
                  name="disk_io_max_operations_per_sec"
                  label="Disk IOPS"
                  dependencies={['disk_io_max_bandwidth_mib_per_sec']}
                  rules={[{
                    validator: (_, value) => {
                      const bandwidth = warmPoolPolicyForm.getFieldValue('disk_io_max_bandwidth_mib_per_sec');
                      return formNumberIsBlank(value) === formNumberIsBlank(bandwidth)
                        ? Promise.resolve()
                        : Promise.reject(new Error('Set both disk I/O limits or leave both blank'));
                    },
                  }]}
                  style={{ width: '50%' }}
                >
                  <InputNumber
                    min={1}
                    max={warmPoolPolicyCapabilities.disk_io.max_operations_per_sec}
                    placeholder={String(warmPoolPolicyCapabilities.disk_io.default_operations_per_sec)}
                    style={{ width: '100%' }}
                  />
                </Form.Item>
                <Form.Item
                  name="disk_io_max_bandwidth_mib_per_sec"
                  label="Disk bandwidth (MiB/s)"
                  dependencies={['disk_io_max_operations_per_sec']}
                  rules={[{
                    validator: (_, value) => {
                      const operations = warmPoolPolicyForm.getFieldValue('disk_io_max_operations_per_sec');
                      return formNumberIsBlank(value) === formNumberIsBlank(operations)
                        ? Promise.resolve()
                        : Promise.reject(new Error('Set both disk I/O limits or leave both blank'));
                    },
                  }]}
                  style={{ width: '50%' }}
                >
                  <InputNumber
                    min={1}
                    max={warmPoolPolicyCapabilities.disk_io.max_bandwidth_mib_per_sec}
                    placeholder={String(warmPoolPolicyCapabilities.disk_io.default_bandwidth_mib_per_sec)}
                    style={{ width: '100%' }}
                  />
                </Form.Item>
              </Space.Compact>
              <Text type="secondary">
                Leave both blank for the platform default. Cluster maximum: {' '}
                {warmPoolPolicyCapabilities.disk_io.max_operations_per_sec.toLocaleString()} IOPS / {' '}
                {warmPoolPolicyCapabilities.disk_io.max_bandwidth_mib_per_sec} MiB/s.
              </Text>
            </>
          ) : null}
          <Form.Item name="enabled" label="Enabled" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Space direction="vertical" size={4}>
            <Text type="secondary">
              Each retained release receives the configured minimum ready slots before demand.
            </Text>
            {warmPoolPolicyModal.policy ? (
              <Text type="secondary">
                Template and VM shape identify this policy. Disable it and create a replacement policy to change shape.
              </Text>
            ) : null}
          </Space>
        </Form>
      </Modal>

      <Modal
        title={retagModal.release ? `Assign tag to ${retagModal.release.template_name}@${shortHash(retagModal.release.content_hash)}` : 'Assign tag'}
        open={retagModal.open}
        onCancel={closeRetagModal}
        onOk={() => retagForm.submit()}
        destroyOnHidden
      >
        <Form form={retagForm} layout="vertical" onFinish={handleRetagRelease}>
          <Form.Item
            name="tag"
            label="Tag"
            rules={[{ required: true, message: 'Tag is required' }]}
          >
            <Input placeholder={DEFAULT_TEMPLATE_TAG} />
          </Form.Item>
          <Space direction="vertical" size={4}>
            <Text type="secondary">
              If this tag already exists for the template, it will move to this release.
            </Text>
            <Text type="secondary">
              The latest tag can be moved but not deleted. Existing warm pools are pinned to release IDs.
            </Text>
          </Space>
        </Form>
      </Modal>

      <Modal
        title={poolModal.mode === 'manage' ? `Manage Pool for "${poolModal.name}"` : `Create Pool for "${poolModal.name}"`}
        open={poolModal.open}
        onCancel={closePoolModal}
        onOk={() => poolForm.submit()}
        footer={[
          poolModal.mode === 'manage' ? (
            <Popconfirm
              key="delete"
              title="Delete this pool policy and let workers drain existing slots?"
              okText="Delete"
              okType="danger"
              onConfirm={() => handleDeletePool(poolModal.name)}
            >
              <Button danger>Delete Pool</Button>
            </Popconfirm>
          ) : null,
          <Button key="cancel" onClick={closePoolModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => poolForm.submit()}>
            Save
          </Button>,
        ]}
        destroyOnHidden
      >
        <Form form={poolForm} layout="vertical" onFinish={handlePoolSubmit}>
          <Form.Item name="target_count" label="Target Warm Sandboxes" rules={[{ required: true }]}>
            <InputNumber min={1} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="boot_mode" label="Boot Mode" rules={[{ required: true }]}>
            <Select
              options={[
                { value: 'cold', label: 'Cold boot' },
                { value: 'snapshot', label: 'Base snapshot' },
              ]}
            />
          </Form.Item>
          <Text type="secondary">
            Direct worker pools are reconciled by node controllers from the template policy.
          </Text>
        </Form>
      </Modal>

      <Modal
        title={clusterPoolModal.release ? `Cluster Pool for "${clusterPoolModal.release.template_name}"` : 'Cluster Pool'}
        open={clusterPoolModal.open}
        onCancel={closeClusterPoolModal}
        onOk={() => clusterPoolForm.submit()}
        footer={[
          clusterPoolModal.config ? (
            <Popconfirm
              key="disable"
              title={clusterPoolModal.config.managed_by === 'autoscaler'
                ? 'Detach this pool from its rolling policy and disable it?'
                : 'Disable this cluster pool and remove unclaimed warm slots?'}
              okText="Disable"
              okType="danger"
              onConfirm={handleDisableClusterPool}
            >
              <Button danger>
                {clusterPoolModal.config.managed_by === 'autoscaler' ? 'Detach and Disable' : 'Disable Pool'}
              </Button>
            </Popconfirm>
          ) : null,
          <Button key="cancel" onClick={closeClusterPoolModal}>Cancel</Button>,
          clusterPoolModal.config?.managed_by === 'autoscaler' ? (
            <Popconfirm
              key="save"
              title="Detach this pool from its rolling policy and save it as manually managed?"
              okText="Detach and Save"
              onConfirm={() => clusterPoolForm.submit()}
            >
              <Button type="primary">Detach and Save</Button>
            </Popconfirm>
          ) : (
            <Button key="save" type="primary" onClick={() => clusterPoolForm.submit()}>
              Save
            </Button>
          ),
        ]}
        destroyOnHidden
      >
        <Form form={clusterPoolForm} layout="vertical" onFinish={handleClusterPoolSubmit}>
          <Form.Item name="enabled" label="Enabled" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Form.Item name="target_ready_slots" label="Target Ready Slots" rules={[{ required: true }]}>
            <InputNumber min={0} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="refill_per_tick" label="Refill Per Tick" rules={[{ required: true }]}>
            <InputNumber min={0} max={100} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="max_warming_slots" label="Max Warming Slots" rules={[{ required: true }]}>
            <InputNumber min={0} max={1000} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="vcpu" label="vCPU" rules={[{ required: true }]}>
            <InputNumber min={1} max={64} disabled={Boolean(clusterPoolModal.config)} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item name="memory_mib" label="Memory (MiB)" rules={[{ required: true }]}>
            <InputNumber min={128} max={262144} disabled={Boolean(clusterPoolModal.config)} style={{ width: '100%' }} />
          </Form.Item>
          {warmPoolPolicyCapabilities.disk_capacity ? (
            <Form.Item
              name="disk_mib"
              label="Disk capacity (MiB)"
              extra="Leave blank for the template-baked size. Changing this drains and refills existing warm slots."
            >
              <InputNumber min={1} max={2147483647} placeholder="Template default" style={{ width: '100%' }} />
            </Form.Item>
          ) : null}
          <Form.Item name="namespace" label="Namespace">
            <Input placeholder="Cluster default" />
          </Form.Item>
          <Text type="secondary">
            Applies to {clusterPoolModal.release ? shortHash(clusterPoolModal.release.content_hash) : 'the selected release'}.
            The controller reconciles target changes on its next tick.
          </Text>
          {clusterPoolModal.config?.managed_by === 'autoscaler' ? (
            <Text type="warning">
              Detaching stops policy updates and preserves this release-specific pool under manual control.
            </Text>
          ) : clusterPoolModal.config ? (
            <Text type="secondary">Release and VM shape identify this manually managed pool.</Text>
          ) : null}
        </Form>
      </Modal>

      <Modal
        title={pullTemplate ? `Pull ${pullTemplate.name}@${shortHash(pullTemplate.content_hash)} to node` : 'Pull template to node'}
        open={pullModal.open}
        onCancel={() => {
          setPullModal({ open: false, template: null });
          pullForm.resetFields();
        }}
        onOk={() => pullForm.submit()}
        confirmLoading={pullTemplate ? templateOperationRunning(`pull:${pullTemplate.key}`) : false}
        destroyOnHidden
      >
        <Form form={pullForm} layout="vertical" onFinish={handlePullTemplate}>
          <Form.Item name="source_node_id" label="Source Node" rules={[{ required: true }]}>
            <Select
              options={pullSourceNodes.map((availability) => ({
                value: availability.node.id,
                label: nodeLabel(availability.node),
              }))}
            />
          </Form.Item>
          <Form.Item name="target_node_id" label="Target Node" rules={[{ required: true }]}>
            <Select
              placeholder="Select node"
              options={pullTargetNodes.map((node) => ({
                value: node.id,
                label: nodeLabel(node),
              }))}
            />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={editingTemplateLifecycle ? `Default Auto Lifecycle for "${editingTemplateLifecycle.name}"` : 'Default Auto Lifecycle'}
        open={Boolean(editingTemplateLifecycle)}
        onCancel={closeTemplateLifecycleModal}
        onOk={() => templateLifecycleForm.submit()}
        footer={[
          <Button key="clear" onClick={handleTemplateLifecycleClear}>
            Clear
          </Button>,
          <Button key="cancel" onClick={closeTemplateLifecycleModal}>Cancel</Button>,
          <Button key="save" type="primary" onClick={() => templateLifecycleForm.submit()}>
            Save
          </Button>,
        ]}
        width={760}
        destroyOnHidden
      >
        <Form form={templateLifecycleForm} layout="vertical" onFinish={handleTemplateLifecycleUpdate}>
          <AutoLifecycleEditor prefix={['auto_lifecycle']} />
        </Form>
      </Modal>

      <NetworkPolicyDesigner
        open={Boolean(editingTemplateNetwork)}
        targetKind="template"
        targetName={editingTemplateNetwork?.name ?? ''}
        policy={editingTemplateNetwork?.network_policy ?? null}
        onClose={() => setEditingTemplateNetwork(null)}
        onSave={handleTemplateNetworkSave}
      />

    </div>
  );
}
