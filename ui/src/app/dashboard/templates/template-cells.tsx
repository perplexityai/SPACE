'use client';

import type { CSSProperties } from 'react';
import { Alert, Button, Space, Tag, Tooltip, Typography, theme } from 'antd';
import type { LatestBuild } from '@/lib/template-list';
import { formatAge, formatBytes } from '@/lib/sandbox-debug';
import type { NodeRole, SpaceNode, Template, TemplateBuildListEntry, TemplateReleaseWarmPool } from '@/lib/types';
import type { TemplateNodeAvailability, TemplatesData } from './use-templates-data';

const { Text } = Typography;

// The Templates views' dense tables, and the row a search marked.
export const COMPACT_TABLE_STYLE = `.compact-table .ant-table-cell { padding: 4px 6px !important; } .compact-table .icon-action-btn { padding-left: 6px; padding-right: 6px; } .compact-table .icon-action-btn .anticon { font-size: 18px; } .compact-table .template-release-highlight > td { background: rgba(250, 173, 20, 0.12); }`;

export function formatDiskSize(mib: number | null | undefined) {
  return mib && mib > 0 ? formatBytes(mib * 1024 * 1024) : '—';
}

const ROOTFS_SIZE_MIN_DELTA_BYTES = 128 * 1024 * 1024;
const ROOTFS_SIZE_MIN_DELTA_RATIO = 0.01;

function hasMeaningfulByteDelta(a: number, b: number) {
  const delta = Math.abs(a - b);
  const reference = Math.max(a, b);
  const threshold = Math.max(ROOTFS_SIZE_MIN_DELTA_BYTES, reference * ROOTFS_SIZE_MIN_DELTA_RATIO);
  return delta > threshold;
}

interface SizeValues {
  disk_size_mb?: number | null;
  cache_size_bytes?: number | null;
  artifact_size_bytes?: number | null;
}

function shouldShowLogicalRootfsSize(values: SizeValues) {
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

function sizeTooltipLines(values: SizeValues) {
  return (
    <Space orientation="vertical" size={0}>
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

export function renderDiskCell(record: SizeValues) {
  return (
    <Tooltip title={sizeTooltipLines(record)}>
      <span>{formatDiskSize(record.disk_size_mb)}</span>
    </Tooltip>
  );
}

export function roleColor(role: NodeRole) {
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

export function shortNodeIdentity(node: SpaceNode): string {
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

export function NodeSnapshotIcon({ availability }: { availability: TemplateNodeAvailability }) {
  const { token } = theme.useToken();
  const observedShapes = observedSnapshotShapeLabels(availability.snapshots);
  if (observedShapes.length > 0) {
    return (
      <Tooltip title={`Observed base snapshot: ${observedShapes.join(', ')}`}>
        {snapshotStatusDot(token.colorSuccess, true)}
      </Tooltip>
    );
  }
  return (
    <Tooltip title="No base snapshot">
      {snapshotStatusDot(token.colorTextTertiary, false)}
    </Tooltip>
  );
}

export function formatShortDateTime(value: string) {
  const date = new Date(value);
  if (isNaN(date.getTime())) return value;
  const pad = (n: number) => String(n).padStart(2, '0');
  return [
    pad(date.getDate()),
    pad(date.getMonth() + 1),
    pad(date.getFullYear() % 100),
  ].join('/') + ` ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

export function warmPoolLabel(pool: TemplateReleaseWarmPool) {
  if (!pool.enabled) return 'Off';
  return `${pool.ready_slots}/${pool.target_ready_slots}`;
}

export function matchesWarmPoolFilter(pools: TemplateReleaseWarmPool[], filter: string) {
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

export function warmPoolTooltip(pool: TemplateReleaseWarmPool, diskCapacityEnabled: boolean) {
  return (
    <Space orientation="vertical" size={0}>
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

export function hasObservedPoolSandboxes(template: Template) {
  return Boolean(
    (template.pool_current && template.pool_current > 0)
      || (template.pool_target && template.pool_target > 0),
  );
}

export function tableFilterOptions(values: string[]) {
  return Array.from(new Set(values))
    .sort((a, b) => a.localeCompare(b))
    .map((value) => ({ text: value, value }));
}

export const TEMPLATE_BUILD_STATE_COLORS: Record<string, string> = {
  pending: 'default',
  running: 'processing',
  succeeded: 'success',
  failed: 'error',
  publish_failed: 'error',
  unspecified: 'default',
};

export function BuildStateTag({ state, statusError }: { state: TemplateBuildListEntry['state']; statusError?: string | null }) {
  if (!state) {
    return (
      <Tooltip title={statusError ?? undefined}>
        <Tag style={{ marginInlineEnd: 0 }}>unknown</Tag>
      </Tooltip>
    );
  }
  return (
    <Tag color={TEMPLATE_BUILD_STATE_COLORS[state] ?? 'default'} style={{ marginInlineEnd: 0 }}>
      {state.replace(/_/g, ' ')}
    </Tag>
  );
}

// The list's latest-build cell: its state and how long ago it started.
export function LatestBuildCell({ latest, now }: { latest: LatestBuild | undefined; now: number }) {
  if (latest === undefined) return <Text type="secondary">…</Text>;
  switch (latest.kind) {
    case 'none':
      return <Text type="secondary">No builds</Text>;
    case 'unsupported':
      return (
        <Tooltip title="This API release does not list a template's builds.">
          <Text type="secondary">—</Text>
        </Tooltip>
      );
    case 'error':
      return (
        <Tooltip title={`Could not read the builds: ${latest.message}`}>
          <Text type="warning">not read</Text>
        </Tooltip>
      );
    case 'build':
      return (
        <Space size={4} wrap={false}>
          <BuildStateTag state={latest.build.state} statusError={latest.build.status_error} />
          <Tooltip title={new Date(latest.build.created_at).toLocaleString()}>
            <Text type="secondary" style={{ fontSize: 12, whiteSpace: 'nowrap' }}>{formatAge(latest.build.created_at, now)} ago</Text>
          </Tooltip>
        </Space>
      );
  }
}

// A read that failed before it ever answered (`firstReadError`): the view says
// so, with Retry, rather than loading forever or showing an empty table.
export function ReadErrorAlert({ what, error, data, style }: {
  what: string;
  error: string;
  data: Pick<TemplatesData, 'handleManualRefresh' | 'refreshCooldown'>;
  style?: CSSProperties;
}) {
  return (
    <Alert
      type="error"
      showIcon
      style={style}
      title={`Could not read ${what}: ${error}`}
      action={<Button size="small" onClick={data.handleManualRefresh} disabled={data.refreshCooldown}>Retry</Button>}
    />
  );
}
