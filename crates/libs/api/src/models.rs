use pplx_space_common_core::types::{OrgId, SandboxId, UserId};

use crate::api_surface::ApiSurface;
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::FromRow;
use std::collections::BTreeMap;
use time::OffsetDateTime;
use uuid::Uuid;

use pplx_space_common_core::types::{
    ArtifactVersion, AutoLifecyclePolicy, AutoLifecycleRuntimeState, AutoLifecycleStatus, Digest,
    EcwCredentialEpoch, NodeAddress, NodeId, NodeManagedBy, NodePoolName, NodeRole, PodUid,
    SandboxActivationAttemptId, SandboxAutoLifecyclePatch, SandboxOwnerSource, SemVer,
    TemplateReference,
};
use pplx_space_net_common::{NetworkPolicy, NetworkPolicyV1};

use crate::user_role::UserRole;
use pplx_space_cluster_pool::PoolConfigOwner;

/// The `sandboxes.metadata` document shape, owned by the sandbox store
/// (it is the shape of a column on that table) and re-exported here so the
/// api's existing `crate::models::SandboxMetadata` paths keep resolving.
pub use pplx_space_sandbox_store::{SandboxMetadata, SandboxMetadataValue};

// --- Database models ---

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Organization {
    pub id: OrgId,
    pub name: String,
    pub slug: String,
    pub parent_id: Option<Uuid>,
    pub theme_primary: Option<String>,
    pub theme_secondary: Option<String>,
    pub logo_url: Option<String>,
    /// Advisory BYOK opt-in flag. Signals callers to route writes
    /// through credmgr + populate the matching `*_encrypted_fields`
    /// sidecar; does not itself encrypt or decrypt anything.
    pub byok_enabled: bool,
    /// Per-org object storage override for base snapshot uploads.
    /// `s3://bucket/prefix?region=...` URI. When set, snapshot files
    /// are uploaded to the org's own bucket instead of the cluster
    /// default. NULL falls through to the cluster-wide env-var config.
    pub snapshot_base_object_storage_uri: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Cluster {
    pub id: Uuid,
    pub name: String,
    pub managing_org_id: OrgId,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct User {
    pub id: UserId,
    pub email: String,
    #[serde(skip_serializing)]
    pub password_hash: String,
    pub display_name: String,
    pub org_id: OrgId,
    pub role: UserRole,
    #[serde(skip_serializing)]
    pub owner_source: SandboxOwnerSource,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl User {
    pub fn password_login_configured(&self) -> bool {
        // Bootstrap service users created before password markers were normalized
        // store this non-bcrypt value. Keep recognizing it for legacy rows; new
        // service-user provisioning writes the shared `!` marker.
        const LEGACY_DISABLED_SERVICE_ACCOUNT: &str = "disabled-service-account";

        self.owner_source == SandboxOwnerSource::Local
            && self.password_hash != "!"
            && self.password_hash != LEGACY_DISABLED_SERVICE_ACCOUNT
    }
}

/// User metadata returned only by the authenticated admin surface.
///
/// The password hash stays private while administrators can see whether local
/// password authentication is enabled. Federated users always report false.
#[derive(Debug, Clone, Serialize)]
pub struct AdminUserResponse {
    #[serde(flatten)]
    pub user: User,
    pub password_login_configured: bool,
}

impl From<User> for AdminUserResponse {
    fn from(user: User) -> Self {
        let password_login_configured = user.password_login_configured();
        Self {
            user,
            password_login_configured,
        }
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct Session {
    pub id: Uuid,
    pub user_id: UserId,
    pub token_hash: String,
    pub expires_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow)]
pub struct ApiKey {
    pub id: Uuid,
    pub org_id: OrgId,
    pub name: String,
    pub key_hash: String,
    pub created_by: Option<Uuid>,
    pub last_used_at: Option<OffsetDateTime>,
    pub federated_owner_assertions_enabled: bool,
    /// Surfaces this credential may authenticate against. Enforced at the
    /// lookup rather than by each caller, so a new surface cannot forget.
    pub allowed_surfaces: Vec<ApiSurface>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Sandbox {
    pub id: SandboxId,
    pub name: Option<String>,
    /// SPACE user row that owns the sandbox. Federated owners map onto local
    /// user/org rows so downstream paths keep the normal sandbox FK shape.
    pub user_id: UserId,
    pub org_id: OrgId,
    pub owner_source: SandboxOwnerSource,
    pub upstream_issuer: Option<String>,
    pub upstream_org_id: Option<String>,
    pub upstream_subject_id: Option<String>,
    pub managed_by: String,
    pub lifecycle: String,
    pub placement: String,
    pub active_node_id: Option<NodeId>,
    pub active_node_name: Option<String>,
    pub active_node_role: Option<NodeRole>,
    pub active_node_address: Option<NodeAddress>,
    pub active_node_managed_by: Option<NodeManagedBy>,
    pub target_node_id: Option<NodeId>,
    pub active_execution_generation: Option<i64>,
    pub active_pod_uid: Option<PodUid>,
    pub active_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub ecw_credential_epoch: EcwCredentialEpoch,
    pub placement_changed_at: Option<OffsetDateTime>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
    pub release_tags: Vec<String>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub disk_mib: Option<i32>,
    pub disk_io_max_operations_per_sec: i64,
    pub disk_io_max_bandwidth_mib_per_sec: i64,
    pub env: sqlx::types::Json<BTreeMap<String, String>>,
    pub metadata: sqlx::types::Json<SandboxMetadata>,
    pub auto_lifecycle: sqlx::types::Json<AutoLifecyclePolicy>,
    pub auto_lifecycle_state: sqlx::types::Json<AutoLifecycleRuntimeState>,
    pub network_policy: Option<sqlx::types::Json<NetworkPolicy>>,
    pub network_policy_generation: i64,
    pub template_network_policy: Option<sqlx::types::Json<NetworkPolicyV1>>,
    pub template_network_policy_generation: Option<i64>,
    pub next_auto_event_at: Option<i64>,
    pub snapshots_enabled_desired: bool,
    /// BYOK: this sandbox runs on dm-crypt encrypted volumes. Immutable for
    /// the sandbox's lifetime — stamped in the create INSERT, never UPDATEd.
    /// Loaded with COALESCE so pre-column (NULL) rows read plaintext.
    pub volume_encryption: bool,
    /// `started` was reported to the caller before the guest command API was
    /// confirmed ready (a budget-exhausted slow create whose pod proxy was
    /// reachable). The access path forces a command-ready confirmation and
    /// clears this on success; until then a data-plane op gets the
    /// SDK-recoverable `sandbox_timeout` rather than a bare upstream error.
    #[serde(default)]
    pub command_ready_pending: bool,
    pub last_used_at: Option<OffsetDateTime>,
    pub last_access_at: Option<OffsetDateTime>,
    pub last_command_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Per-organization Key Encryption Key. Populated by tenant
/// onboarding; consumed by the credmgr at wrap/unwrap time.
///
/// `retired_at IS NULL` = active KEK. Retired KEKs stay valid for
/// unwrap; **never filter by `retired_at IS NULL` when resolving a
/// `*_encrypted_fields.master_key_id`** — only when selecting the
/// active key for new wraps.
///
/// `updated_at` is trigger-maintained; do not set manually.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct OrgMasterKey {
    pub id: Uuid,
    pub org_id: OrgId,
    pub provider: String,
    pub key_identifier: String,
    #[serde(default, with = "time::serde::rfc3339::option")]
    pub retired_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Per-sandbox encryption sidecar. Presence-of-row IS the encryption
/// invariant; one `wrapped_dek` encrypts all encrypted columns on
/// the row. Cross-tenant binding is impossible structurally — two
/// composite FKs on `(sandbox_id, org_id)` and
/// `(master_key_id, org_id)` enforce `sandbox.org_id = master_key.org_id`.
///
/// Encrypted columns mirror their `sandboxes` counterparts (BYTEA
/// here vs JSONB on the parent). Nullability matches the parent:
/// `env` and `metadata` are NOT NULL; `network_policy_override_json`
/// is nullable.
///
/// **Stand-in convention** (application contract, not schema-
/// enforced): when this row exists, the parent's plaintext columns
/// MUST hold their default (`'{}'` / NULL), not the real value. The
/// sidecar is the source of truth.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SandboxEncryptedFields {
    pub id: Uuid,
    pub sandbox_id: SandboxId,
    pub org_id: OrgId,
    pub master_key_id: Uuid,
    pub wrapped_dek: Vec<u8>,
    pub env: Vec<u8>,
    pub metadata: Vec<u8>,
    pub network_policy_override_json: Option<Vec<u8>>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Per-template encryption sidecar. Keyed on `templates.name`
/// (templates have no UUID PK). Same shape and invariants as
/// [`SandboxEncryptedFields`]; one DEK encrypts `description` (BYTEA,
/// nullable like its parent) and `network_policy_json` (BYTEA, NOT
/// NULL like its parent).
///
/// **System templates** (`templates.org_id IS NULL`) cannot have a
/// sidecar — intentional, shared-infra templates have no org KEK.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct TemplateEncryptedFields {
    pub id: Uuid,
    pub template_name: String,
    pub org_id: OrgId,
    pub master_key_id: Uuid,
    pub wrapped_dek: Vec<u8>,
    pub description: Option<Vec<u8>>,
    pub network_policy_json: Vec<u8>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Per-storage-location encryption sidecar for a snapshot replica.
/// Same shape and invariants as [`SandboxEncryptedFields`]. One row
/// per storage location (per-replica DEK). The encrypted column
/// `storage_connection_string` is BYTEA NOT NULL, mirroring the
/// parent's `text NOT NULL`; stand-in convention applies.
///
/// Storage_locations whose `org_id` is still NULL (pre-rollout
/// callers) cannot have a sidecar — gates encryption on the caller-
/// update having happened.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SnapshotStorageLocationEncryptedFields {
    pub id: Uuid,
    pub snapshot_storage_location_id: Uuid,
    pub org_id: OrgId,
    pub master_key_id: Uuid,
    pub wrapped_dek: Vec<u8>,
    pub storage_connection_string: Vec<u8>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Per-storage-location encryption sidecar for a template-release
/// artifact replica. Same shape and invariants as
/// [`SnapshotStorageLocationEncryptedFields`]; system templates
/// (org_id NULL) propagate NULL down the chain and cannot have one.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct TemplateReleaseStorageLocationEncryptedFields {
    pub id: Uuid,
    pub template_release_storage_location_id: Uuid,
    pub org_id: OrgId,
    pub master_key_id: Uuid,
    pub wrapped_dek: Vec<u8>,
    pub storage_connection_string: Vec<u8>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SandboxNodeSummary {
    pub id: NodeId,
    pub name: Option<String>,
    pub role: Option<NodeRole>,
    pub address: Option<NodeAddress>,
    pub managed_by: Option<NodeManagedBy>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkPolicyCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub last_transition_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NetworkPolicyStatus {
    pub observed_network_policy: Option<NetworkPolicy>,
    pub observed_generation: Option<u64>,
    pub observed_source: Option<String>,
    pub observed_authority: Option<String>,
    pub observed_at: Option<String>,
    pub inherited_from_template: Option<bool>,
    pub source_template: Option<String>,
    pub conditions: Vec<NetworkPolicyCondition>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SandboxResourceType {
    #[default]
    Sandbox,
    PoolSlot,
    ObservedLocal,
}

/// Sandbox enriched with observed status.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SandboxResponse {
    pub id: SandboxId,
    pub resource_type: SandboxResourceType,
    pub name: Option<String>,
    pub user_id: Option<UserId>,
    pub org_id: Option<OrgId>,
    pub lifecycle: Option<String>,
    pub placement: Option<String>,
    pub active_node_id: Option<NodeId>,
    pub active_node: Option<SandboxNodeSummary>,
    pub metadata: SandboxMetadata,
    #[serde(with = "time::serde::rfc3339::option")]
    pub created_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub updated_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_used_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_access_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_command_at: Option<OffsetDateTime>,
    pub auto_lifecycle: Option<AutoLifecyclePolicy>,
    pub auto_lifecycle_status: Option<AutoLifecycleStatus>,
    pub network_policy: Option<NetworkPolicy>,
    pub network_policy_override: Option<NetworkPolicy>,
    pub template_network_policy: Option<NetworkPolicyV1>,
    pub effective_desired_network_policy: Option<NetworkPolicy>,
    pub network_policy_generation: Option<u64>,
    pub network_policy_status: Option<NetworkPolicyStatus>,
    // Live/observed fields from sbx-mgr plus template release identity.
    pub status: Option<String>,
    pub template: Option<String>,
    pub content_hash: Option<String>,
    pub release_tags: Vec<String>,
    pub pid: Option<u32>,
    pub namespace: Option<String>,
    pub uptime: Option<String>,
    pub user: Option<String>,
    pub image_hash: Option<String>,
    pub runtime: Option<String>,
    pub spaced_version: Option<SemVer>,
    pub snapshots_enabled_desired: bool,
    pub snapshots_enabled: Option<bool>,
    pub pool_template: Option<String>,
    pub disk_usage_bytes: Option<u64>,
    pub failure_reason: Option<String>,
    pub age: Option<String>,
    /// BYOK: the sandbox runs on dm-crypt encrypted volumes. Stamped at
    /// create time and immutable for the sandbox's lifetime.
    pub volume_encryption: bool,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct SandboxPortExposure {
    pub sandbox_id: SandboxId,
    pub protocol: String,
    pub guest_port: i32,
    pub visibility: String,
    pub source: String,
    pub desired_state: String,
    pub apply_state: String,
    pub last_applied_node_id: Option<NodeId>,
    pub last_error: Option<String>,
    /// Last observed in-VM loopback bridge status. One of `direct`,
    /// `loopback_forwarded`, `waiting_for_listener`, `bind_conflict`,
    /// `failed`, or NULL when the bridge has never been observed (e.g., a
    /// `failed` apply that didn't reach the in-VM agent).
    pub last_bridge_status: Option<String>,
    /// Last observed `service_generation` from `spaced-guest-svc`. A change
    /// between successive observations means the guest agent restarted; the
    /// reconciler treats that as a forced re-application.
    pub last_applied_bridge_generation: Option<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_observed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Template {
    pub name: String,
    pub description: Option<String>,
    pub org_id: Option<OrgId>,
    pub network_policy_generation: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfoResponse {
    pub vcpu: u32,
    pub memory_mib: u32,
    pub snapshot_hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TemplateResponse {
    pub name: String,
    pub description: Option<String>,
    pub org_id: Option<OrgId>,
    pub desired_state: String,
    pub created_at: Option<String>,
    pub built_at: Option<String>,
    pub content_hash: Option<String>,
    pub runtime: Option<String>,
    pub arch: Option<String>,
    pub has_base_snapshot: Option<bool>,
    pub pool_target: Option<u32>,
    pub pool_current: Option<u32>,
    pub artifact_size_bytes: Option<u64>,
    pub cache_size_bytes: Option<u64>,
    pub base_snapshot_size_bytes: Option<u64>,
    pub snapshots: Vec<SnapshotInfoResponse>,
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub disk_size_mb: Option<u32>,
    pub default_auto_lifecycle: Option<AutoLifecyclePolicy>,
    pub network_policy: Option<NetworkPolicyV1>,
    pub network_policy_generation: Option<u64>,
    pub network_policy_status: Option<NetworkPolicyStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TemplateOperationResponse {
    pub operation_id: pplx_space_public_api::OperationId,
    pub template: String,
}

// --- Request DTOs ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NullablePatch<T> {
    Set(T),
    Clear,
}

fn deserialize_nullable_patch<'de, D, T>(
    deserializer: D,
) -> Result<Option<NullablePatch<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
        .map(|value| Some(value.map_or(NullablePatch::Clear, NullablePatch::Set)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub user: User,
    pub org: Organization,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateOrgRequest {
    pub name: String,
    pub slug: String,
    pub parent_id: Option<Uuid>,
    pub theme_primary: Option<String>,
    pub theme_secondary: Option<String>,
    pub logo_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateOrgRequest {
    pub name: Option<String>,
    pub slug: Option<String>,
    #[serde(default, deserialize_with = "deserialize_nullable_patch")]
    pub theme_primary: Option<NullablePatch<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable_patch")]
    pub theme_secondary: Option<NullablePatch<String>>,
    #[serde(default, deserialize_with = "deserialize_nullable_patch")]
    pub logo_url: Option<NullablePatch<String>>,
    /// Set the per-org object storage override for base snapshot uploads.
    /// Must be a valid `s3://bucket/prefix?region=...` URI. Omit to leave
    /// the existing value unchanged; set to `null` to clear the override.
    #[serde(default, deserialize_with = "deserialize_nullable_patch")]
    pub snapshot_base_object_storage_uri: Option<NullablePatch<String>>,
    /// Advisory BYOK opt-in flag (see [`Organization::byok_enabled`]).
    /// Affects new sandboxes only; existing sandboxes keep their
    /// create-time `volume_encryption` stamp. Omit to leave unchanged.
    pub byok_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateClusterRequest {
    pub managing_org_id: OrgId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateUserRequest {
    pub email: String,
    pub password: String,
    pub display_name: String,
    pub role: UserRole,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateUserRequest {
    pub email: Option<String>,
    pub password: Option<String>,
    pub display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateUserRoleRequest {
    pub role: UserRole,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSandboxRequest {
    pub name: Option<String>,
    pub template: Option<TemplateReference>,
    pub runtime: Option<String>,
    #[serde(default)]
    pub boot_mode: Option<String>, // "cold", "snapshot", "pool" (default: unspecified = fallthrough)
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub metadata: SandboxMetadata,
    #[serde(default)]
    pub vcpu: Option<u32>,
    #[serde(default)]
    pub memory_mib: Option<u32>,
    #[serde(default)]
    pub disk_mib: Option<u32>,
    #[serde(default)]
    pub file_changes: Option<FileChangeTrackingRequest>,
    #[serde(default)]
    pub auto_lifecycle: Option<AutoLifecyclePolicy>,
    #[serde(default)]
    pub wait_until_ready: bool,
    #[serde(default)]
    pub network_policy: Option<NetworkPolicyV1>,
    /// Internal-only: pin this sandbox to a named isolation node pool at create
    /// time. Honored only when `SPACE_ALLOW_SANDBOX_PLACEMENT` is on; set only
    /// via the admin surface. Not exposed on the public or ECW/E2B surfaces.
    #[serde(default)]
    pub node_pool: Option<NodePoolName>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChangeTrackingRequest {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default)]
    pub include_globs: Vec<String>,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
    #[serde(default)]
    pub use_default_excludes: Option<bool>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateSandboxRequest {
    #[serde(default)]
    pub metadata: Option<SandboxMetadata>,
    #[serde(default)]
    pub vcpu: Option<u32>,
    #[serde(default)]
    pub memory_mib: Option<u32>,
    #[serde(default)]
    pub pids_max: Option<u32>,
    #[serde(default)]
    pub auto_lifecycle: Option<SandboxAutoLifecyclePatch>,
    #[serde(default)]
    pub network_policy: Option<SandboxNetworkPolicyPatch>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum SandboxNetworkPolicyPatch {
    Set { policy: NetworkPolicyV1 },
    InheritTemplateDefault,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxPortProtocol {
    #[default]
    Tcp,
}

impl SandboxPortProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxPortVisibility {
    #[default]
    Private,
    Public,
}

impl SandboxPortVisibility {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Public => "public",
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenSandboxPortRequest {
    #[serde(default)]
    pub protocol: Option<SandboxPortProtocol>,
    #[serde(default)]
    pub visibility: Option<SandboxPortVisibility>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseSandboxPortRequest {
    #[serde(default)]
    pub protocol: Option<SandboxPortProtocol>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxActivationRequest {
    #[serde(default)]
    pub blocking: Option<bool>,
    #[serde(default)]
    pub ready_timeout_secs: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterSandboxRequest {
    pub id: SandboxId,
    pub active_node_id: Option<NodeId>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateTemplateRequest {
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "disabled_auto_lifecycle")]
    pub default_auto_lifecycle: AutoLifecyclePolicy,
    #[serde(default = "default_network_policy")]
    pub network_policy: NetworkPolicyV1,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTemplateRequest {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub default_auto_lifecycle: Option<AutoLifecyclePolicy>,
    #[serde(default)]
    pub network_policy: Option<NetworkPolicyV1>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBaseSnapshotPolicyRequest {
    pub ensure_base_snapshot: bool,
    #[serde(default)]
    pub snapshot_vcpu: Option<u32>,
    #[serde(default)]
    pub snapshot_memory_mib: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateReleaseWarmPoolResponse {
    pub id: Uuid,
    pub vcpu: u32,
    pub memory_mib: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_mib: Option<u32>,
    pub target_ready_slots: u32,
    pub refill_per_tick: u32,
    pub max_warming_slots: u32,
    pub namespace: String,
    pub enabled: bool,
    /// Distinguishes an encryptable config from its plaintext twin at the
    /// same (release, shape); without it the listing rows are identical.
    #[serde(default)]
    pub encryptable: bool,
    pub managed_by: PoolConfigOwner,
    pub warming_slots: u32,
    pub ready_slots: u32,
    pub claimed_slots: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TemplateReleaseResponse {
    pub id: Uuid,
    pub template_name: String,
    pub runtime: String,
    pub content_hash: String,
    pub tags: Vec<String>,
    pub warm_pools: Vec<TemplateReleaseWarmPoolResponse>,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateReleaseTagRequest {
    pub tag: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildRequest {
    pub recipe: TemplateBuildRecipe,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub publish_tags: Vec<String>,
    #[serde(default)]
    pub client: Option<TemplateBuildClientContext>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemTemplateBuildRequest {
    #[serde(default)]
    pub base: Option<pplx_space_common_core::types::TemplateReference>,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub publish_tags: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildClientContext {
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub cli_version: Option<String>,
    #[serde(default)]
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildRecipe {
    pub metadata: TemplateBuildRecipeMetadata,
    #[serde(default)]
    pub build_config: Option<TemplateBuildConfig>,
    pub origin: TemplateBuildOrigin,
    #[serde(default)]
    pub steps: Vec<TemplateBuildStep>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildRecipeMetadata {
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildConfig {
    #[serde(default)]
    pub vcpu: Option<u32>,
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    #[serde(default)]
    pub disk_bytes: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildOrigin {
    #[serde(default)]
    pub from_template: Option<TemplateBuildFromTemplate>,
    #[serde(default)]
    pub from_scratch: Option<TemplateBuildFromScratch>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildFromTemplate {
    pub base: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildFromScratch {
    pub source: TemplateBuildSource,
    #[serde(default)]
    pub guest_kernel: Option<TemplateBuildGuestKernel>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildGuestKernel {
    pub version: ArtifactVersion,
    pub digest: Digest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildSource {
    #[serde(default)]
    pub ubuntu_minimal_cloud_squashfs: Option<TemplateBuildUbuntuMinimalCloudSquashfs>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildUbuntuMinimalCloudSquashfs {
    pub release: String,
    pub version: String,
    pub date: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildStep {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub env_refs: Vec<String>,
    #[serde(default)]
    pub package_install: Option<TemplateBuildPackageInstallStep>,
    #[serde(default)]
    pub run: Option<TemplateBuildRunStep>,
    #[serde(default)]
    pub env: Option<TemplateBuildEnvStep>,
    #[serde(default)]
    pub copy: Option<TemplateBuildCopyStep>,
    #[serde(default)]
    pub playwright_install: Option<TemplateBuildPlaywrightInstallStep>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TemplateBuildPackageManager {
    Apt,
    Pip,
    Npm,
    NpmGlobal,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildPackageInstallStep {
    pub manager: TemplateBuildPackageManager,
    #[serde(default)]
    pub packages: Vec<String>,
    #[serde(default)]
    pub extra_args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildRunStep {
    pub command: String,
    #[serde(default)]
    pub user: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildEnvStep {
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildCopyStep {
    #[serde(default)]
    pub inline_bytes_base64: Option<String>,
    #[serde(default)]
    pub blob_sha256: Option<String>,
    pub dest: String,
    #[serde(default)]
    pub source_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateBuildPlaywrightInstallStep {
    #[serde(default)]
    pub browsers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TemplateBuildStartResponse {
    pub job_id: String,
    pub template_name: String,
}

pub use pplx_space_common_core::types::TemplateBuildJobState as TemplateBuildStateResponse;

#[derive(Debug, Clone, Serialize)]
pub struct TemplateBuildStatusResponse {
    pub job_id: String,
    pub template_name: String,
    pub state: TemplateBuildStateResponse,
    pub content_hash: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterPoolConfigResponse {
    pub id: Uuid,
    pub template_release_id: Uuid,
    pub template_name: String,
    pub content_hash: String,
    pub vcpu: u32,
    pub memory_mib: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_mib: Option<u32>,
    pub target_ready_slots: u32,
    pub refill_per_tick: u32,
    pub max_warming_slots: u32,
    pub namespace: String,
    pub enabled: bool,
    pub encryptable: bool,
    pub managed_by: PoolConfigOwner,
    pub warming_slots: u32,
    pub ready_slots: u32,
    pub claimed_slots: u32,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterPoolConfigRequest {
    pub template_name: String,
    #[serde(default)]
    pub content_hash: Option<String>,
    pub vcpu: u32,
    pub memory_mib: u32,
    #[serde(default)]
    pub disk_mib: Option<u32>,
    // Omission preserves an existing override. Clearing is explicit so older
    // admin clients cannot silently resize a pool back to the template default.
    #[serde(default)]
    pub reset_disk_to_template_default: bool,
    pub target_ready_slots: u32,
    pub refill_per_tick: u32,
    pub max_warming_slots: u32,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default = "default_cluster_pool_enabled")]
    pub enabled: bool,
    /// Members of an encryptable config serve only encrypted creates.
    #[serde(default)]
    pub encryptable: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct WarmPoolPolicyResponse {
    pub template_name: String,
    pub runtime: String,
    pub vcpu: u32,
    pub memory_mib: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_mib: Option<u32>,
    pub min_ready_slots: u32,
    pub max_ready_slots: u32,
    pub refill_per_tick: u32,
    pub max_warming_slots: u32,
    pub retain_releases: u32,
    pub retain_shape_on_miss: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_io_max_operations_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_io_max_bandwidth_mib_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_io_source: Option<&'static str>,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WarmPoolPolicyRequest {
    pub template_name: String,
    pub vcpu: u32,
    pub memory_mib: u32,
    #[serde(default)]
    pub disk_mib: Option<u32>,
    // Omission preserves an existing override. Clearing is explicit so older
    // admin clients cannot silently resize every retained release.
    #[serde(default)]
    pub reset_disk_to_template_default: bool,
    pub min_ready_slots: u32,
    pub max_ready_slots: u32,
    #[serde(default = "default_warm_pool_policy_refill_per_tick")]
    pub refill_per_tick: u32,
    #[serde(default = "default_warm_pool_policy_max_warming_slots")]
    pub max_warming_slots: u32,
    pub retain_releases: u32,
    // Optional so older admin clients preserve an existing assignment.
    #[serde(default)]
    pub retain_shape_on_miss: Option<bool>,
    #[serde(default)]
    pub disk_io_max_operations_per_sec: Option<u64>,
    #[serde(default)]
    pub disk_io_max_bandwidth_mib_per_sec: Option<u64>,
    // Omitted disk-I/O fields preserve an existing assignment so older admin
    // clients remain safe. Clearing requires this explicit operation.
    #[serde(default)]
    pub reset_disk_io_to_default: bool,
    #[serde(default = "default_cluster_pool_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreatePoolApiRequest {
    #[serde(default)]
    pub target_count: Option<u32>,
    #[serde(default)]
    pub boot_mode: Option<String>,
}

fn disabled_auto_lifecycle() -> AutoLifecyclePolicy {
    AutoLifecyclePolicy::disabled()
}

fn default_cluster_pool_enabled() -> bool {
    true
}

fn default_warm_pool_policy_refill_per_tick() -> u32 {
    5
}

fn default_warm_pool_policy_max_warming_slots() -> u32 {
    10
}

fn default_network_policy() -> NetworkPolicyV1 {
    NetworkPolicyV1::default()
}

// --- Query params ---

#[derive(Debug, Deserialize)]
pub struct SandboxFilters {
    pub user_id: Option<UserId>,
    pub org_id: Option<OrgId>,
}

#[derive(Debug, Deserialize)]
pub struct TemplateFilters {
    pub org_id: Option<OrgId>,
}

/// Merged credential for the internal sandbox route (user overrides org).
/// Carries the sealed document and wrapped DEK plus the identity needed to
/// decrypt it: `user_id` is the owning user for user-scoped rows and
/// absent for org-scoped ones (the org comes from the sandbox itself).
/// `id` is the row id, which is the AAD the document was sealed under.
#[derive(Debug, Serialize)]
pub struct MergedCredential {
    pub id: Uuid,
    pub key: String,
    pub sealed: Vec<u8>,
    pub wrapped_key: Vec<u8>,
    pub user_id: Option<UserId>,
}

/// An opaque credential-provider grant stored for a federated-owned sandbox.
///
/// `grant_blob` is opaque bearer-style material SPACE never parses; it is a secret
/// (replayable at the credential provider until expiry). It is therefore never
/// serialized and is omitted from the manual `Debug` below — keep it out of any log,
/// metric, trace, event, or error path. Owner binding is resolved through the sandbox's
/// mapped organization and user rows.
#[derive(Clone, FromRow)]
pub struct SandboxFederatedServiceGrant {
    pub sandbox_id: SandboxId,
    pub grant_audience: String,
    pub grant_purpose: String,
    pub grant_blob: Vec<u8>,
    pub grant_expires_at: OffsetDateTime,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

impl std::fmt::Debug for SandboxFederatedServiceGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxFederatedServiceGrant")
            .field("sandbox_id", &self.sandbox_id)
            .field("grant_audience", &self.grant_audience)
            .field("grant_purpose", &self.grant_purpose)
            .field(
                "grant_blob",
                &format_args!("[redacted {} bytes]", self.grant_blob.len()),
            )
            .field("grant_expires_at", &self.grant_expires_at)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

#[cfg(test)]
mod tests;
