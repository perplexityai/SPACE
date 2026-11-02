use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use serde::Serialize;
use serde_json::Value;
use sqlx::types::Json as SqlxJson;
use time::OffsetDateTime;
use uuid::Uuid;

use pplx_space_common_core::types::{NodeAddress, NodeId, NodeManagedBy, NodeRole, PodUid};

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(super) enum DebugSection<T> {
    Data(T),
    Error { error: String, data: Option<()> },
}

impl<T> DebugSection<T> {
    pub(super) fn data(data: T) -> Self {
        Self::Data(data)
    }

    pub(super) fn error(error: impl ToString) -> Self {
        Self::Error {
            error: error.to_string(),
            data: None,
        }
    }
}

/// Stable, agent-oriented sandbox debug response.
///
/// All keys are snake_case and all top-level diagnostic sections are always
/// present. Successful sections serialize directly as their data payload
/// (`lifecycle.state`, `timeline[0].kind`). Failed sections
/// serialize as `{ "error": "...", "data": null }` so callers can distinguish
/// source failures without losing the rest of the response.
#[derive(Debug, Serialize)]
pub(super) struct SandboxDebugResponse {
    pub sandbox_id: SandboxId,
    #[serde(with = "time::serde::rfc3339")]
    pub fetched_at: OffsetDateTime,
    pub schema_version: &'static str,
    pub lifecycle: DebugSection<LifecycleSection>,
    pub placement: DebugSection<PlacementSection>,
    pub resources: DebugSection<ResourcesSection>,
    pub pods: DebugSection<Vec<PodExecution>>,
    pub snapshots: DebugSection<SnapshotsSection>,
    pub ports: DebugSection<Vec<PortExposure>>,
    pub timeline: DebugSection<Vec<TimelineEvent>>,
    pub quarantine: DebugSection<QuarantineSection>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(super) struct SandboxRecord {
    pub id: SandboxId,
    pub name: Option<String>,
    pub user_id: Option<UserId>,
    pub org_id: Option<OrgId>,
    pub managed_by: String,
    pub lifecycle: String,
    pub placement: String,
    pub active_node_id: Option<NodeId>,
    pub active_node_address: Option<NodeAddress>,
    pub active_node_role: Option<NodeRole>,
    pub active_node_managed_by: Option<NodeManagedBy>,
    pub active_k8s_node_name: Option<String>,
    pub target_node_id: Option<NodeId>,
    pub target_node_address: Option<NodeAddress>,
    pub target_node_role: Option<NodeRole>,
    pub target_node_managed_by: Option<NodeManagedBy>,
    pub target_k8s_node_name: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub placement_changed_at: Option<OffsetDateTime>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    pub env: SqlxJson<Value>,
    pub metadata: SqlxJson<Value>,
    pub auto_lifecycle: SqlxJson<Value>,
    pub auto_lifecycle_state: SqlxJson<Value>,
    pub next_auto_event_at: Option<i64>,
    pub network_policy_override_json: Option<SqlxJson<Value>>,
    pub network_policy_generation: i64,
    pub snapshots_enabled_desired: bool,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_used_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Serialize)]
pub(super) struct LifecycleSection {
    pub state: String,
    pub managed_by: String,
    pub name: Option<String>,
    pub user_id: Option<UserId>,
    pub org_id: Option<OrgId>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_used_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Serialize)]
pub(super) struct PlacementSection {
    pub state: String,
    pub active_node_id: Option<NodeId>,
    pub active_node_address: Option<NodeAddress>,
    pub active_node_role: Option<NodeRole>,
    pub active_node_managed_by: Option<NodeManagedBy>,
    pub active_k8s_node_name: Option<String>,
    pub target_node_id: Option<NodeId>,
    pub target_node_address: Option<NodeAddress>,
    pub target_node_role: Option<NodeRole>,
    pub target_node_managed_by: Option<NodeManagedBy>,
    pub target_k8s_node_name: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub changed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize)]
pub(super) struct ResourcesSection {
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    pub env: SqlxJson<Value>,
    pub metadata: SqlxJson<Value>,
    pub auto_lifecycle: SqlxJson<Value>,
    pub auto_lifecycle_state: SqlxJson<Value>,
    pub next_auto_event_at: Option<i64>,
    pub network_policy_override_json: Option<SqlxJson<Value>>,
    pub network_policy_generation: i64,
    pub snapshots_enabled_desired: bool,
    pub observed: Option<ObservedResources>,
}

#[derive(Debug, Serialize)]
pub(super) struct SandboxSections {
    pub lifecycle: LifecycleSection,
    pub placement: PlacementSection,
    pub resources: ResourcesSection,
}

impl SandboxRecord {
    pub(super) fn into_sections(self, observed: Option<ObservedResources>) -> SandboxSections {
        SandboxSections {
            lifecycle: LifecycleSection {
                state: self.lifecycle,
                managed_by: self.managed_by,
                name: self.name,
                user_id: self.user_id,
                org_id: self.org_id,
                last_used_at: self.last_used_at,
                created_at: self.created_at,
                updated_at: self.updated_at,
            },
            placement: PlacementSection {
                state: self.placement,
                active_node_id: self.active_node_id,
                active_node_address: self.active_node_address,
                active_node_role: self.active_node_role,
                active_node_managed_by: self.active_node_managed_by,
                active_k8s_node_name: self.active_k8s_node_name,
                target_node_id: self.target_node_id,
                target_node_address: self.target_node_address,
                target_node_role: self.target_node_role,
                target_node_managed_by: self.target_node_managed_by,
                target_k8s_node_name: self.target_k8s_node_name,
                changed_at: self.placement_changed_at,
            },
            resources: ResourcesSection {
                template_id: self.template_id,
                content_hash: self.content_hash,
                vcpu: self.vcpu,
                memory_mib: self.memory_mib,
                disk_mib: self.disk_mib,
                env: self.env,
                metadata: self.metadata,
                auto_lifecycle: self.auto_lifecycle,
                auto_lifecycle_state: self.auto_lifecycle_state,
                next_auto_event_at: self.next_auto_event_at,
                network_policy_override_json: self.network_policy_override_json,
                network_policy_generation: self.network_policy_generation,
                snapshots_enabled_desired: self.snapshots_enabled_desired,
                observed,
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub(super) struct ObservedResources {
    pub sandbox_id: SandboxId,
    pub node_id: Option<NodeId>,
    pub node_address: Option<NodeAddress>,
    pub node_role: Option<NodeRole>,
    pub node_managed_by: Option<NodeManagedBy>,
    pub k8s_node_name: Option<String>,
    pub status: Option<String>,
    pub pid: Option<i32>,
    pub namespace: Option<String>,
    pub uptime: Option<String>,
    pub sandbox_user: Option<String>,
    pub image_hash: Option<String>,
    pub snapshots_enabled: bool,
    pub disk_usage_bytes: Option<i64>,
    pub pool_template: Option<String>,
    pub failure_reason: Option<String>,
    pub observed_created_at: Option<String>,
    pub network_tx_bytes: Option<i64>,
    pub network_rx_bytes: Option<i64>,
    pub missing_publish_count: i32,
    pub template: Option<String>,
    pub network_policy_authority: Option<String>,
    pub auto_lifecycle_authority: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(super) struct PodExecution {
    pub id: Uuid,
    pub sandbox_id: SandboxId,
    pub generation: i64,
    pub pod_name: String,
    pub pod_namespace: String,
    pub pod_uid: Option<PodUid>,
    pub pod_ip: Option<String>,
    pub k8s_node_name: Option<String>,
    pub state: String,
    pub active: bool,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub terminated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize)]
pub(super) struct SnapshotsSection {
    pub snapshots: Vec<Snapshot>,
    pub inventory: Vec<SnapshotInventory>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(super) struct Snapshot {
    pub id: Uuid,
    pub sandbox_id: SandboxId,
    pub r#type: String,
    pub state: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub source_node_id: Option<NodeId>,
    pub label: Option<String>,
    pub retention_tier: Option<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub size_bytes: Option<i64>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub org_id: Option<OrgId>,
    pub failure_reason: Option<String>,
    /// Filled from the snapshot catalog's storage placement after the read.
    #[sqlx(skip)]
    pub storage: Option<SnapshotStorage>,
}

/// Where a catalog snapshot's bytes are.
#[derive(Debug, Serialize)]
pub(super) struct SnapshotStorage {
    pub location: SnapshotStorageLocation,
    /// The node holding the local copy, for `node_local` and `both`.
    pub node_id: Option<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SnapshotStorageLocation {
    /// Only on the sandbox's node: the current local artifact, not yet
    /// uploaded.
    NodeLocal,
    /// Only in the object store.
    ObjectStore,
    Both,
    /// Neither: an upload in flight or failed, or an older artifact whose
    /// stored copy has been orphaned.
    None,
}

impl SnapshotStorage {
    pub(super) fn new(in_object_store: bool, node_local_node_id: Option<NodeId>) -> Self {
        let location = match (in_object_store, node_local_node_id.is_some()) {
            (true, true) => SnapshotStorageLocation::Both,
            (true, false) => SnapshotStorageLocation::ObjectStore,
            (false, true) => SnapshotStorageLocation::NodeLocal,
            (false, false) => SnapshotStorageLocation::None,
        };
        Self {
            location,
            node_id: node_local_node_id,
        }
    }
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(super) struct SnapshotInventory {
    pub sandbox_id: SandboxId,
    /// The node that reported the snapshot, which holds it locally.
    pub node_id: NodeId,
    pub snapshot_id: String,
    pub r#type: String,
    pub state: String,
    pub timestamp: Option<String>,
    pub source_node_id: Option<String>,
    pub label: Option<String>,
    pub retention_tier: Option<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub size_bytes: i64,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub locations: SqlxJson<Value>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// A sandbox's quarantine state: the open episode, if any, and its recent
/// quarantine events across all episodes.
#[derive(Debug, Serialize)]
pub(super) struct QuarantineSection {
    pub quarantined: bool,
    pub active: Option<ActiveQuarantine>,
    /// Newest first, at most [`QUARANTINE_HISTORY_LIMIT`] entries.
    pub history: Vec<QuarantineHistoryEntry>,
}

pub(super) const QUARANTINE_HISTORY_LIMIT: u32 = 50;

#[derive(Debug, Serialize)]
pub(super) struct ActiveQuarantine {
    pub quarantine_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub entered_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    pub entered_from_lifecycle: Option<String>,
    pub source: String,
    pub reason_code: String,
    pub reason_message: Option<String>,
    pub next_action: String,
    #[serde(with = "time::serde::rfc3339::option")]
    pub next_action_after: Option<OffsetDateTime>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub last_observed_node_id: Option<NodeId>,
}

#[derive(Debug, Serialize)]
pub(super) struct QuarantineHistoryEntry {
    pub id: Uuid,
    pub quarantine_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
    pub source: String,
    pub action: String,
    pub reason_code: Option<String>,
    pub message: Option<String>,
    pub next_action: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(super) struct PortExposure {
    pub sandbox_id: SandboxId,
    pub protocol: String,
    pub guest_port: i32,
    pub visibility: String,
    pub source: String,
    pub desired_state: String,
    pub apply_state: String,
    pub last_applied_node_id: Option<NodeId>,
    pub last_error: Option<String>,
    pub last_bridge_status: Option<String>,
    pub last_applied_bridge_generation: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_observed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub(super) struct TimelineEvent {
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    pub kind: String,
    pub detail: SqlxJson<Value>,
}
