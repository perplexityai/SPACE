export type UserRole = 'organization_admin' | 'member';

export interface Organization {
  id: string;
  name: string;
  slug: string;
  parent_id: string | null;
  theme_primary: string | null;
  theme_secondary: string | null;
  logo_url: string | null;
  byok_enabled: boolean;
  created_at: string;
  updated_at: string;
}

/// Active per-org Key Encryption Key from GET /admin/api/v1/orgs/{id}/master-key.
export interface OrgMasterKey {
  id: string;
  org_id: string;
  provider: string;
  key_identifier: string;
  retired_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface User {
  id: string;
  email: string;
  display_name: string;
  org_id: string;
  role: UserRole;
  created_at: string;
  updated_at: string;
}

export interface AdminUser extends User {
  password_login_configured: boolean;
}

export type ApiSurface = 'ecw' | 'native';

export interface ApiKeyMetadata {
  id: string;
  org_id: string;
  name: string;
  created_by: string | null;
  created_by_email: string | null;
  allowed_surfaces: ApiSurface[];
  created_at: string;
  updated_at: string;
}

export interface CreatedApiKey {
  api_key: ApiKeyMetadata;
  secret: string;
}

export interface Cluster {
  id: string;
  name: string;
  managing_org_id: string;
  created_at: string;
  updated_at: string;
}

export interface Sandbox {
  id: string;
  resource_type: 'sandbox' | 'pool_slot' | 'observed_local';
  name: string | null;
  user_id: string | null;
  org_id: string | null;
  managed_by: string | null;
  lifecycle: string | null;
  placement: string | null;
  active_node_id: string | null;
  active_node: SandboxNodeSummary | null;
  created_at: string | null;
  updated_at: string | null;
  last_used_at: string | null;
  last_access_at: string | null;
  last_command_at: string | null;
  // Live/observed fields from sbx-mgr plus template release identity.
  status: string | null;
  template: string | null;
  content_hash: string | null;
  release_tags: string[];
  runtime: string | null;
  pid: number | null;
  namespace: string | null;
  uptime: string | null;
  user: string | null;
  image_hash: string | null;
  spaced_version: string | null;
  snapshots_enabled_desired: boolean;
  snapshots_enabled: boolean | null;
  pool_template: string | null;
  disk_usage_bytes: number | null;
  failure_reason: string | null;
  age: string | null;
  volume_encryption: boolean;
  auto_lifecycle: AutoLifecyclePolicy | null;
  auto_lifecycle_status: AutoLifecycleStatus | null;
  network_policy_override: NetworkPolicyV1 | null;
  template_network_policy: NetworkPolicyV1 | null;
  effective_desired_network_policy: NetworkPolicyV1 | null;
  network_policy: NetworkPolicyV1 | null;
  network_policy_generation: number | null;
  network_policy_status: NetworkPolicyStatus | null;
}

export interface SandboxNodeSummary {
  id: string;
  name: string | null;
  role: NodeRole | string | null;
  address: string | null;
  managed_by: string | null;
}

export interface SnapshotInfo {
  id: string;         // snapshot name — use for rollback/delete
  type: string;       // "disk" or "full"
  state: string;
  timestamp: string | null;
  label: string | null;
  retention_tier: string | null;
  size_bytes: number;
}

export interface TemplateSnapshotInfo {
  vcpu: number;
  memory_mib: number;
  snapshot_hash: string;
}

export interface Template {
  name: string;
  description: string | null;
  org_id: string | null;
  desired_state: string;
  created_at: string | null;
  built_at: string | null;
  content_hash: string | null;
  runtime: string | null;
  arch: string | null;
  has_base_snapshot: boolean | null;
  pool_target: number | null;
  pool_current: number | null;
  artifact_size_bytes: number | null;
  cache_size_bytes: number | null;
  base_snapshot_size_bytes: number | null;
  snapshots: TemplateSnapshotInfo[];
  vcpu: number | null;
  memory_mib: number | null;
  disk_size_mb: number | null;
  default_auto_lifecycle: AutoLifecyclePolicy | null;
  network_policy: NetworkPolicyV1 | null;
  network_policy_generation: number | null;
  network_policy_status: NetworkPolicyStatus | null;
}

export interface TemplateOperationResponse {
  operation_id: string;
  template: string;
}

export interface PublicPage<T> {
  data: T[];
  next_cursor: string | null;
}

export interface PublicSandboxResources {
  vcpu: number | null;
  memory_mib: number | null;
  disk_mib: number | null;
}

export interface PublicSandboxCondition {
  condition_type: string;
  status: string;
  reason: string;
  message: string;
}

export type SandboxFailureCode = 'runtime_failure';

export interface PublicSandbox {
  id: string;
  name: string | null;
  state: string;
  failure_code: SandboxFailureCode | null;
  conditions: PublicSandboxCondition[];
  template: string | null;
  template_digest: string | null;
  resources: PublicSandboxResources;
  metadata: Record<string, string | number | boolean>;
  created_at: string;
  updated_at: string;
}

export interface PublicTemplateSnapshot {
  vcpu: number;
  memory_mib: number;
  snapshot_hash: string;
}

export interface PublicTemplateVariant {
  reference: string;
  content_hash: string;
  runtime: string | null;
  arch: string | null;
  built_at: string | null;
  artifact_size_bytes: number | null;
  has_base_snapshot: boolean;
  base_snapshot_size_bytes: number | null;
  snapshots: PublicTemplateSnapshot[];
  resources: PublicSandboxResources;
}

export interface PublicTemplate {
  name: string;
  description: string | null;
  variants: PublicTemplateVariant[];
  created_at: string;
  updated_at: string;
}

export interface PublicSnapshot {
  id: string;
  sandbox_id: string;
  snapshot_type: string;
  state: string;
  label: string | null;
  retention_tier: string | null;
  size_bytes: number | null;
  resources: PublicSandboxResources;
  template: string | null;
  template_digest: string | null;
  failure_reason: string | null;
  created_at: string;
}

export interface SandboxOperationResponse {
  operation_id: string;
  sandbox_id: string;
}

export interface PublicOperationRecord {
  id: string;
  kind: string;
  state: 'queued' | 'running' | 'succeeded' | 'failed' | 'canceled' | 'expired';
  progress_message: string;
  terminal_error_code: string | null;
  terminal_error: unknown | null;
  result: unknown | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
}

export interface NetworkPolicyCondition {
  type: string;
  status: string;
  reason: string | null;
  message: string | null;
  last_transition_at: string | null;
}

export interface NetworkPolicyStatus {
  observed_network_policy: NetworkPolicyV1 | null;
  observed_generation: number | null;
  observed_source: 'template' | 'override' | null;
  observed_authority: string | null;
  observed_at: string | null;
  inherited_from_template: boolean | null;
  source_template: string | null;
  conditions: NetworkPolicyCondition[];
}

export type NetworkPolicyAction = 'allow' | 'deny';
export type NetworkPolicyTlsInterception = 'off' | 'on';

export type NetworkPolicyDestinationV1 =
  | { ip: string }
  | { cidr: string }
  | { domain: string }
  | { wildcard_domain: string };

export interface CredentialHeaderBindingV1 {
  header_name: string;
  credential_reference_id: string;
}

export interface NetworkCredentialRuleV1 {
  rule_id: string;
  destination: {
    exact_host: string;
    port?: number;
    path_prefix?: string;
  };
  credential_header_bindings: CredentialHeaderBindingV1[];
  supported_http_methods: Array<'GET' | 'POST'>;
}

export interface NetworkPolicyV1 {
  version: 'v1';
  egress: {
    default_action: NetworkPolicyAction;
    allow?: NetworkPolicyDestinationV1[];
    deny?: NetworkPolicyDestinationV1[];
  };
  tls_interception?: NetworkPolicyTlsInterception;
  network_credential_rules?: NetworkCredentialRuleV1[];
  tls_interception_exemptions?: NetworkPolicyDestinationV1[];
}

export type NodeRole = 'worker' | 'builder' | 'cluster';

/// How aggressively the reconciler empties a draining node. Mirrors
/// `NodeDrainMode` in `crates/libs/common/core/src/types/node.rs`.
export type DrainMode = 'gentle' | 'forced';

export interface ServiceHealth {
  name: string;
  reachable: boolean;
  error: string | null;
}

export interface ReconciliationStatus {
  name: string;
  poll_interval_secs: number;
  last_started_at_unix_secs: number;
  last_completed_at_unix_secs: number;
  last_success_at_unix_secs: number;
  next_due_at_unix_secs: number;
  running: boolean;
  templates_seen: number;
  sandboxes_seen: number;
  templates_applied: number;
  sandboxes_applied: number;
  last_error: string;
}

export interface NodeCtlStatus {
  reachable: boolean;
  error: string | null;
  reconciliations: ReconciliationStatus[];
}

export interface NodeHealthStatus {
  reachable: boolean;
  healthy: boolean;
  error: string | null;
  services: ServiceHealth[];
  node_ctl: NodeCtlStatus | null;
}

export interface NodeSchedulingStatus {
  status: string;
  reason: string | null;
  network_slots_ready: number | null;
  network_pool_min_ready: number | null;
  network_slots_degraded: number | null;
  network_slots_quarantined: number | null;
  network_slots_total: number | null;
  network_slots_leased: number | null;
  network_pool_max_slots: number | null;
}

export interface NodeStatusSnapshot {
  observed_at: string;
  stale_after: string;
  stale: boolean;
  reachable: boolean;
  error: string | null;
  role: string | null;
  node_id_reported: string | null;
  scheduling: NodeSchedulingStatus | null;
  health: NodeHealthStatus | null;
}

export interface SpaceNode {
  id: string;
  name: string | null;
  address: string | null;
  node_api_endpoint: string | null;
  role: NodeRole;
  managed_by: string;
  state: string | null;
  metadata: Record<string, unknown>;
  last_heartbeat: string | null;
  created_at: string;
  updated_at: string | null;
  deleted_at: string | null;
  status: NodeStatusSnapshot | null;
  reconcile_trigger: NodeReconcileTrigger | null;
}

/// Merged view type used by the Nodes dashboard.
/// Core fields and cached node status come from GET /admin/api/v1/nodes.
export interface NodeView {
  id: string;
  name: string | null;
  address: string | null;
  node_api_endpoint: string | null;
  role: NodeRole;
  managed_by: string;
  state: string | null;
  scheduling_status: string | null;
  scheduling_reason: string | null;
  drain_phase: string | null;
  /** Absent when the column is NULL: no drain in progress, or a drain that
   * started before the column existed. Drain-decision code resolves that to
   * `gentle` — a missing choice never selects the evacuating mode — so an
   * absent value renders as gentle. */
  drain_mode: DrainMode | null;
  drain_started_at: string | null;
  drain_last_error: string | null;
  sandboxes_active: number | null;
  sandboxes_local_snapshots: number | null;
  last_heartbeat: string | null;
  created_at: string;
  updated_at: string | null;
  deleted_at: string | null;
  network_slots: {
    ready: number | null;
    min_ready: number | null;
    degraded: number | null;
    quarantined: number | null;
    total: number | null;
    leased: number | null;
    max_slots: number | null;
  };
  status: NodeStatusSnapshot | null;
  reconcile_trigger: NodeReconcileTrigger | null;
}

/// Response shape for GET /admin/api/v1/nodes and action endpoints.
export interface AdminNodeResponse {
  id: string;
  name: string | null;
  address: string | null;
  node_api_endpoint: string | null;
  role: string;
  managed_by: string;
  state: string | null;
  scheduling_status: string | null;
  scheduling_reason: string | null;
  drain_phase: string | null;
  /** Absent when the column is NULL: no drain in progress, or a drain that
   * started before the column existed. Drain-decision code resolves that to
   * `gentle` — a missing choice never selects the evacuating mode — so an
   * absent value renders as gentle. */
  drain_mode: DrainMode | null;
  drain_started_at: string | null;
  drain_last_error: string | null;
  k8s_node_name: string | null;
  metadata: Record<string, unknown>;
  last_heartbeat: string | null;
  created_at: string;
  updated_at: string | null;
  deleted_at: string | null;
  sandboxes_active: number | null;
  sandboxes_local_snapshots: number | null;
  network_slots: {
    ready: number | null;
    min_ready: number | null;
    degraded: number | null;
    quarantined: number | null;
    total: number | null;
    leased: number | null;
    max_slots: number | null;
  };
  status: NodeStatusSnapshot | null;
}

export interface NodeReconcileTrigger {
  reasons: string[];
  diagnostic_reason: string;
  requested_at: string;
  next_attempt_at: string;
  attempts: number;
  last_error: string;
  dead_at: string | null;
  dead_reason: string;
}

export interface NodeTemplateBuild {
  name: string;
  content_hash: string | null;
  build_date: string | null;
  artifact_size_bytes: number | null;
  cache_size_bytes: number | null;
  disk_size_mb: number | null;
  runtime: string | null;
  arch: string | null;
  snapshots: TemplateSnapshotInfo[];
}

export interface NodeTemplateInventory {
  node: SpaceNode;
  reachable: boolean;
  error: string | null;
  builds: NodeTemplateBuild[];
}

export type AutoLifecycleIdleAction = 'none' | 'pause' | 'suspend' | 'stop' | 'delete';
export type AutoLifecycleWakeSource = 'access' | 'command';
export type AutoLifecycleConditionKind = 'inactivity' | 'guest_activity';
export type AutoLifecycleActivitySource = 'access' | 'command';
export type AutoLifecycleConditionSetId = 'access_idle' | 'command_idle' | 'guest_activity';
export type AutoLifecycleTransition = 'noop' | 'pause' | 'suspend' | 'stop' | 'delete';

export interface AutoLifecycleWakePolicy {
  on?: AutoLifecycleWakeSource[];
}

export interface AutoLifecycleConditionSet {
  kind: AutoLifecycleConditionKind;
  activity_source?: AutoLifecycleActivitySource | null;
  max_guest_idle_extension?: string | null;
}

export interface AutoLifecycleStage {
  condition_set: AutoLifecycleConditionSetId;
  at: string;
  action: AutoLifecycleIdleAction;
}

export interface AutoLifecyclePolicy {
  wake?: AutoLifecycleWakePolicy;
  condition_sets?: Partial<Record<AutoLifecycleConditionSetId, AutoLifecycleConditionSet>>;
  stages?: AutoLifecycleStage[];
}

export interface AutoLifecycleConditionStatus {
  armed: boolean;
  epoch_at?: number | null;
  last_touched_at?: number | null;
}

export interface AutoLifecycleStatus {
  conditions?: Partial<Record<AutoLifecycleConditionSetId, AutoLifecycleConditionStatus>>;
  last_access_at?: number | null;
  last_command_at?: number | null;
  next_auto_event_at?: number | null;
  open_activity_leases?: number;
  last_auto_transition?: AutoLifecycleTransition | null;
  last_auto_transition_at?: number | null;
  last_auto_error?: string | null;
}

export interface PoolInfo {
  template: string;
  target_count: number;
  current_count: number;
  pending_count: number;
  sandbox_ids: string[];
}

export type PoolConfigOwner = 'manual' | 'autoscaler';

export interface ClusterPoolConfig {
  id: string;
  template_release_id: string;
  template_name: string;
  content_hash: string;
  vcpu: number;
  memory_mib: number;
  disk_mib?: number;
  target_ready_slots: number;
  refill_per_tick: number;
  max_warming_slots: number;
  namespace: string;
  enabled: boolean;
  managed_by: PoolConfigOwner;
  warming_slots: number;
  ready_slots: number;
  claimed_slots: number;
  created_at: string;
  updated_at: string;
}

export interface TemplateReleaseWarmPool {
  id: string;
  vcpu: number;
  memory_mib: number;
  disk_mib?: number;
  target_ready_slots: number;
  refill_per_tick: number;
  max_warming_slots: number;
  namespace: string;
  enabled: boolean;
  managed_by: PoolConfigOwner;
  warming_slots: number;
  ready_slots: number;
  claimed_slots: number;
}

export interface WarmPoolPolicy {
  template_name: string;
  runtime: string;
  vcpu: number;
  memory_mib: number;
  disk_mib?: number;
  min_ready_slots: number;
  max_ready_slots: number;
  refill_per_tick: number;
  max_warming_slots: number;
  retain_releases: number;
  retain_shape_on_miss?: boolean;
  disk_io_max_operations_per_sec?: number;
  disk_io_max_bandwidth_mib_per_sec?: number;
  disk_io_source?: 'warm_pool';
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface WarmPoolPolicyCapabilities {
  retain_shape_on_miss?: boolean;
  disk_capacity?: boolean;
  disk_io?: {
    max_operations_per_sec: number;
    max_bandwidth_mib_per_sec: number;
    default_operations_per_sec: number;
    default_bandwidth_mib_per_sec: number;
  };
}

export interface TemplateRelease {
  id: string;
  template_name: string;
  runtime: string;
  content_hash: string;
  tags: string[];
  warm_pools: TemplateReleaseWarmPool[];
  created_at: string;
}

export interface LoginResponse {
  token: string;
  user: User;
  org: Organization;
}

export type Substrate = 'space' | 'k8s';

export interface MeResponse {
  user: User;
  org: Organization;
  substrate: Substrate;
}
