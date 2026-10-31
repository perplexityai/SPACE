// `GET /admin/api/v1/debug/sandbox/{uuid}`: everything the control plane's
// Postgres records about one sandbox, one section per source. A section that
// failed to load is `{ error, data: null }` instead of its data.

export type DebugSection<T> = T | { error: string; data: null };

export function sectionData<T>(section: DebugSection<T> | undefined): T | null {
  if (section === undefined || section === null) return null;
  if (typeof section === 'object' && 'error' in (section as object) && (section as { data?: unknown }).data === null) {
    return null;
  }
  return section as T;
}

export function sectionError(section: DebugSection<unknown> | undefined): string | null {
  if (section && typeof section === 'object' && 'error' in section && (section as { data?: unknown }).data === null) {
    return (section as { error: string }).error;
  }
  return null;
}

export interface DebugLifecycle {
  state: string;
  managed_by: string;
  name: string | null;
  user_id: string | null;
  org_id: string | null;
  last_used_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface DebugPlacement {
  state: string;
  active_node_id: string | null;
  active_node_address: string | null;
  active_node_role: string | null;
  active_k8s_node_name: string | null;
  target_node_id: string | null;
  target_node_address: string | null;
  target_node_role: string | null;
  target_k8s_node_name: string | null;
  changed_at: string | null;
}

export interface DebugObserved {
  node_id: string | null;
  k8s_node_name: string | null;
  status: string | null;
  pid: number | null;
  namespace: string | null;
  uptime: string | null;
  image_hash: string | null;
  disk_usage_bytes: number | null;
  failure_reason: string | null;
  network_tx_bytes: number | null;
  network_rx_bytes: number | null;
  missing_publish_count: number;
  network_policy_authority: string | null;
  auto_lifecycle_authority: string | null;
  observed_at: string;
}

export interface DebugResources {
  template_id: string | null;
  content_hash: string | null;
  vcpu: number | null;
  memory_mib: number | null;
  disk_mib: number | null;
  env: Record<string, unknown> | null;
  metadata: Record<string, unknown> | null;
  next_auto_event_at: number | null;
  network_policy_generation: number;
  snapshots_enabled_desired: boolean;
  observed: DebugObserved | null;
}

export interface DebugPod {
  id: string;
  generation: number;
  pod_name: string;
  pod_namespace: string;
  pod_uid: string | null;
  pod_ip: string | null;
  k8s_node_name: string | null;
  state: string;
  active: boolean;
  created_at: string;
  updated_at: string;
  terminated_at: string | null;
}

export interface DebugSnapshot {
  id: string;
  type: string;
  state: string;
  timestamp: string;
  source_node_id: string | null;
  label: string | null;
  retention_tier: string | null;
  size_bytes: number | null;
  template_id: string | null;
  content_hash: string | null;
  failure_reason: string | null;
}

export interface DebugSnapshotInventory {
  snapshot_id: string;
  type: string;
  state: string;
  timestamp: string | null;
  source_node_id: string | null;
  size_bytes: number;
  locations: unknown;
  observed_at: string;
}

export interface DebugPort {
  protocol: string;
  guest_port: number;
  visibility: string;
  source: string;
  desired_state: string;
  apply_state: string;
  last_error: string | null;
  last_observed_at: string | null;
}

export interface DebugTimelineEvent {
  at: string;
  kind: string;
  detail: Record<string, unknown>;
}

export interface SandboxDebug {
  sandbox_id: string;
  fetched_at: string;
  lifecycle: DebugSection<DebugLifecycle>;
  placement: DebugSection<DebugPlacement>;
  resources: DebugSection<DebugResources>;
  pods: DebugSection<DebugPod[]>;
  snapshots: DebugSection<{ snapshots: DebugSnapshot[]; inventory: DebugSnapshotInventory[] }>;
  ports: DebugSection<DebugPort[]>;
  timeline: DebugSection<DebugTimelineEvent[]>;
}

// One line describing a timeline event's detail, most informative fields first.
export function summarizeTimelineDetail(event: DebugTimelineEvent): string {
  const detail = event.detail ?? {};
  const keys = ['lifecycle', 'state', 'placement', 'pod_name', 'generation', 'snapshot_type', 'size_bytes', 'failure_reason', 'guest_port', 'error'];
  const parts: string[] = [];
  for (const key of keys) {
    const value = detail[key];
    if (value === null || value === undefined || value === '') continue;
    parts.push(`${key.replace(/_/g, ' ')}: ${key === 'size_bytes' ? formatBytes(Number(value)) : String(value)}`);
  }
  return parts.join(' · ');
}

// Events newest first; the API already orders them, but merge order across
// sources with equal timestamps is not guaranteed.
export function sortedTimeline(events: DebugTimelineEvent[]): DebugTimelineEvent[] {
  return [...events].sort((a, b) => Date.parse(b.at) - Date.parse(a.at));
}

export function formatBytes(n: number | null | undefined): string {
  if (n === null || n === undefined || Number.isNaN(n)) return '—';
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)} KB`;
  return `${n} B`;
}

// A short age such as "5m" or "3d" for a timestamp relative to `nowMs`.
export function formatAge(value: string | null | undefined, nowMs: number): string {
  if (!value) return '—';
  const at = Date.parse(value);
  if (!Number.isFinite(at)) return '—';
  const seconds = Math.max(0, Math.floor((nowMs - at) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}
