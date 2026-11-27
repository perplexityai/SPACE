use pplx_space_accounts::{OrgId, UserId};
use pplx_space_sandbox_model::SandboxId;

use serde::Serialize;
use serde_json::{Map, Value};
use sqlx::types::Json as SqlxJson;
use time::OffsetDateTime;
use uuid::Uuid;

use pplx_space_accounts::NodeId;
use pplx_space_k8s_ids::PodUid;
use pplx_space_node_model::{NodeAddress, NodeManagedBy, NodeRole};
use pplx_space_sandbox_store::debug_reads::SandboxDebugRecord;

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
///
/// `resources.env` lists the sandbox's environment variable names, not their
/// values: it keeps the stored `{ "NAME": value }` object shape with every
/// value `null`, so diagnostics can see which variables are set.
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
    /// Variable names, each mapped to `null`; see [`env_names`].
    pub env: Map<String, Value>,
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

/// Split the sandbox's stored row into the response's sections.
pub(super) fn sandbox_sections(
    record: SandboxDebugRecord,
    observed: Option<ObservedResources>,
) -> SandboxSections {
    SandboxSections {
        lifecycle: LifecycleSection {
            state: record.lifecycle,
            managed_by: record.managed_by,
            name: record.name,
            user_id: record.user_id,
            org_id: record.org_id,
            last_used_at: record.last_used_at,
            created_at: record.created_at,
            updated_at: record.updated_at,
        },
        placement: PlacementSection {
            state: record.placement,
            active_node_id: record.active_node_id,
            active_node_address: record.active_node_address,
            active_node_role: record.active_node_role,
            active_node_managed_by: record.active_node_managed_by,
            active_k8s_node_name: record.active_k8s_node_name,
            target_node_id: record.target_node_id,
            target_node_address: record.target_node_address,
            target_node_role: record.target_node_role,
            target_node_managed_by: record.target_node_managed_by,
            target_k8s_node_name: record.target_k8s_node_name,
            changed_at: record.placement_changed_at,
        },
        resources: ResourcesSection {
            template_id: record.template_id,
            content_hash: record.content_hash,
            vcpu: record.vcpu,
            memory_mib: record.memory_mib,
            disk_mib: record.disk_mib,
            env: env_names(record.env.0),
            metadata: record.metadata,
            auto_lifecycle: record.auto_lifecycle,
            auto_lifecycle_state: record.auto_lifecycle_state,
            next_auto_event_at: record.next_auto_event_at,
            network_policy_override_json: record.network_policy_override_json,
            network_policy_generation: record.network_policy_generation,
            snapshots_enabled_desired: record.snapshots_enabled_desired,
            observed,
        },
    }
}

/// The names in a stored `sandboxes.env` object, each mapped to `null`.
///
/// The column has no object constraint, so anything other than an object
/// yields no names rather than echoing the stored value.
fn env_names(env: Value) -> Map<String, Value> {
    match env {
        Value::Object(env) => env
            .into_iter()
            .map(|(name, _)| (name, Value::Null))
            .collect(),
        _ => Map::new(),
    }
}

#[derive(Clone, Debug, Serialize)]
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

#[derive(Debug, Serialize)]
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
    /// Why the snapshots carry no `storage`, when the placement read failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_error: Option<String>,
}

#[derive(Debug, Serialize)]
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
    /// Omitted when that read failed; the section's `storage_error` says why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<SnapshotStorage>,
}

impl From<pplx_space_snapshot_store::listing::StoredSnapshot> for Snapshot {
    fn from(row: pplx_space_snapshot_store::listing::StoredSnapshot) -> Self {
        let pplx_space_snapshot_store::listing::StoredSnapshot {
            id,
            sandbox_id,
            r#type,
            state,
            timestamp,
            source_node_id,
            label,
            retention_tier,
            vcpu,
            memory_mib,
            size_bytes,
            template_id,
            content_hash,
            org_id,
            failure_reason,
        } = row;
        Self {
            id: id.as_uuid(),
            sandbox_id,
            r#type,
            state,
            timestamp,
            source_node_id,
            label,
            retention_tier,
            vcpu,
            memory_mib,
            size_bytes,
            template_id,
            content_hash,
            org_id,
            failure_reason,
            storage: None,
        }
    }
}

/// Where a catalog snapshot's bytes are.
#[derive(Debug, Serialize)]
pub(super) struct SnapshotStorage {
    pub location: SnapshotStorageLocation,
    /// The node holding the local copy, for `node_local` and `both`: the
    /// node the snapshot uploaded from, which is also the sandbox's target
    /// node, else its active node.
    pub node_id: Option<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SnapshotStorageLocation {
    /// Only on the sandbox's node, not in the object store: the current local
    /// artifact of a sandbox settled in `suspended` or `stopped` on the node
    /// the snapshot uploaded from. A sandbox in any other lifecycle, one whose
    /// placement names a different node (as after a failed cross-node
    /// restore), and a snapshot with no recorded source node report no
    /// node-local copy.
    NodeLocal,
    /// Only in the object store.
    ObjectStore,
    Both,
    /// Neither recorded. An older artifact with no live stored copy (an
    /// upload that failed or is in flight, or one orphaned), or a current
    /// artifact not yet stored whose local copy the rule above cannot place.
    /// The current artifact of a settled sandbox on its upload node reads
    /// `node_local` even while its upload is in flight or after it failed.
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

#[derive(Debug, Serialize)]
pub(super) struct SnapshotInventory {
    pub sandbox_id: SandboxId,
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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
pub(super) struct TimelineEvent {
    #[serde(with = "time::serde::rfc3339")]
    pub at: OffsetDateTime,
    pub kind: String,
    pub detail: SqlxJson<Value>,
}
