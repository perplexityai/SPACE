//! Cluster control-plane gRPC API (`ClusterApi`) backed by the Postgres
//! cluster-state SSOT. Extracted from `pplx-space-node-api-server` (the former
//! `--role cluster` surface of `space-node-api-svc`).
//!
//! Proto types and the generated `ClusterApi` service come from the shared
//! `pplx-space-node-api-client` crate; auth helpers come from
//! `pplx-space-common-svc`. Both are re-exported as `crate::proto` / `crate::auth`
//! so the moved modules resolve those paths unchanged.
//!
//! This crate is the whole service. The `space-internal-api-svc` binary is a
//! thin `main` that parses flags, initializes telemetry, and calls [`run`], so
//! a deployment that needs private wiring can supply its own `main` against
//! this library without copying any of the service.
pub use pplx_space_common_svc::auth;
pub use pplx_space_node_api_client::proto;

mod api_key_store;
pub mod node_info;
pub use node_info::ClusterNodeInfoService;

mod data_plane_sessions;
pub mod ingress_activation;
mod node_inventory;
mod route_identity;
mod run;
mod sandbox_service_ports;
mod trusted_storage;
mod visibility_observations;

pub use run::run;
pub use run::{
    DEFAULT_LISTEN_ADDR, InternalApiRunConfig, NodeIdError, RunInternalApiError, SERVICE_NAME,
};

use pplx_space_common_core::sql;
use std::net::IpAddr;
use std::str::FromStr;

use pplx_space_net_common::{
    NetworkPolicyV1,
    proto::{NetworkPolicySource, NetworkPolicyV1 as NetworkPolicyProto},
};
use pplx_space_snapshot_store as snapshot_store;
use sqlx::FromRow;
use sqlx::types::Json;
use time::OffsetDateTime;
use tonic::{Request, Response, Status};

use pplx_space_common_core::node_drain_signals::EVACUATION_RESTORE_READY_CHANNEL;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, AutoLifecyclePolicy, BackupAttemptId, Digest, ExactStorageUri,
    ExecutionGeneration, LocalArtifactId, NodeId, NodeRole as SpaceNodeRole, OrgId, RuntimeKind,
    SandboxId, SnapshotArtifact as SnapshotArtifactFile, SnapshotContentHash, SnapshotId,
    SnapshotKind, SnapshotName, SnapshotState, SubstrateKind, TemplateName,
};
use pplx_space_common_svc::types::{
    FACET_NETWORK_POLICY, FACET_NODE_STATUS, FACET_RUNTIME_PROVISIONING,
    FACET_SANDBOX_DESIRED_STATE, FACET_SANDBOX_INVENTORY, FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
    FACET_TEMPLATE_DESIRED_STATE, FACET_TEMPLATE_INVENTORY, NODE_RECONCILE_CLAIM_STALE_SECS,
    NetworkPolicyTarget, ReconcileFacet,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use uuid::Uuid;

use crate::proto::FacetRef;
use crate::proto::cluster_api_server::ClusterApi;
use crate::proto::{
    BeginArtifactBackupRequest, BeginArtifactBackupResponse, BindDataPlaneSessionProcessRequest,
    BindDataPlaneSessionProcessResponse, ClaimNodeReconcileRefsRequest,
    ClaimNodeReconcileRefsResponse, CompleteNodeReconcileRefsRequest,
    CompleteNodeReconcileRefsResponse, CreateSystemTemplatePublicationGrantRequest,
    CreateSystemTemplatePublicationGrantResponse, FinalizeArtifactBackupRequest,
    FinalizeArtifactBackupResponse, FindTemplateReleaseByHashRequest, FindTemplateReleaseRequest,
    FindTemplateReleaseResponse, GetArtifactBackupLocationRequest,
    GetArtifactBackupLocationResponse, GetDataPlaneOrgRevocationEpochRequest,
    GetDataPlaneOrgRevocationEpochResponse, GetDataPlaneSessionRevocationStateRequest,
    GetDataPlaneSessionRevocationStateResponse, GetNodeNetworkPolicyStateRequest,
    GetNodeNetworkPolicyStateResponse, GetNodeRuntimeDesiredStateRequest,
    GetNodeRuntimeDesiredStateResponse, GetNodeSandboxDesiredStateRequest,
    GetNodeSandboxDesiredStateResponse, GetNodeTemplateBaseSnapshotPolicyStateRequest,
    GetNodeTemplateBaseSnapshotPolicyStateResponse, GetNodeTemplateDesiredStateRequest,
    GetNodeTemplateDesiredStateResponse, GetQuarantineRecoveryPlanRequest,
    GetQuarantineRecoveryPlanResponse, GetSandboxRuntimeAccessRequest,
    GetSandboxRuntimeAccessResponse, GetSandboxSnapshotStorageUriRequest,
    GetSandboxSnapshotStorageUriResponse, GetSuspendSnapshotLocationRequest,
    GetSuspendSnapshotLocationResponse, ListLatestTemplateReleasesEntry,
    ListLatestTemplateReleasesRequest, ListLatestTemplateReleasesResponse, ListNodesRequest,
    ListNodesResponse, ListWarmPoolTemplateReleasesRequest, ListWarmPoolTemplateReleasesResponse,
    NetworkPolicyTargetRef, NodeCtlStatus, NodeRecord, NodeRole as ProtoNodeRole,
    NodeSchedulingStatus, PublishNodeStateRequest, PublishNodeStateResponse,
    PublishSandboxPortExposureStatesRequest, PublishSandboxPortExposureStatesResponse,
    PublishedNodeStatus, QuarantineRecoverySuspendSnapshot, QuarantineRecoveryTargetState,
    ReconcileFacet as ProtoReconcileFacet, RecordDataPlaneSessionReconnectRequest,
    RecordDataPlaneSessionReconnectResponse, RegisterTemplateReleaseRequest,
    RegisterTemplateReleaseResponse, RegisterTemplateRequest, RegisterTemplateResponse,
    ResolveDataPlaneSessionRouteRequest, ResolveDataPlaneSessionRouteResponse, SandboxDesiredState,
    SandboxNetworkPolicyState, SandboxPortApplyStateKind, SandboxPortDesiredStateKind,
    SandboxPortExposureApplyStateError, SandboxPortExposureDesiredState, SandboxPortProtocol,
    SuspendSnapshotTerminalState, Template, TemplateBaseSnapshotPolicyState, TemplateDesiredState,
    TemplateNetworkPolicyState, TemplatePoolPolicyState, TemplateRelease,
    TemplateReleaseStorageLocation, TemplateReleaseTag, TemplateVisibility,
    TouchArtifactBackupRequest, TouchArtifactBackupResponse,
    UnstableResetSandboxServiceRouteRequest, UnstableResetSandboxServiceRouteResponse,
    UnstableResolveSandboxServiceRouteRequest, UnstableResolveSandboxServiceRouteResponse,
    WarmPoolTemplateRelease, facet_ref, network_policy_target_ref,
};
/// The `snapshots` row [`ClusterApiService::finalize_backup_attempt`] flips,
/// exactly as that statement's `RETURNING` produces it.
#[derive(Debug, FromRow)]
struct FinalizedSnapshotRow {
    sandbox_id: SandboxId,
    source_node_id: Option<NodeId>,
    local_artifact_id: Option<LocalArtifactId>,
    artifact_kind: Option<String>,
}

impl FinalizedSnapshotRow {
    /// The pending-backup queue identity this finalize is entitled to delete,
    /// if the row carries a complete one.
    ///
    /// `source_node_id` is NULL on rows that predate node attribution and
    /// `local_artifact_id` on rollout-skew rows with no artifact identity; a
    /// row missing either cannot name a queue row, since every mirrored row
    /// carries both, so `None` costs nothing the census set-replace does not
    /// heal.
    ///
    /// The `artifact_kind` arm is unreachable rather than lenient:
    /// `snapshots_artifact_identity_check` makes the column non-NULL whenever
    /// `local_artifact_id` is, and its own `CHECK` admits only `disk` and
    /// `full`. It is written as a fallible parse anyway because the type
    /// system cannot see either constraint.
    fn artifact(&self) -> Option<node_local_backup_pending::FinalizedArtifact> {
        Some(node_local_backup_pending::FinalizedArtifact {
            source_node_id: self.source_node_id?,
            sandbox_id: self.sandbox_id,
            artifact_id: self.local_artifact_id?,
            artifact_kind: ArtifactKind::from_str(self.artifact_kind.as_deref()?).ok()?,
        })
    }
}

#[path = "published_state.rs"]
mod published_state;
/// Re-exported so Space's Datadog DBM query tests (private to Space) can drive the publish path.
pub use published_state::publish_sandbox_states;
mod sandbox_quarantine_store;
use published_state::{
    capture_host_cpu, project_ready_suspend_snapshot, prune_absent_node_templates,
    publish_template_states, snapshot_execution_metadata,
};

const SUPPORTED_TEMPLATE_RUNTIMES: &[RuntimeKind] = &[RuntimeKind::Firecracker];

const NODE_RECONCILE_CLAIM_DEFAULT_LIMIT: i64 = 256;
const NODE_RECONCILE_MAX_ATTEMPTS: i32 = 16;
const NODE_RECONCILE_MAX_RETRY_DELAY_SECS: i64 = 300;
const NODE_STATUS_STALE_AFTER_SECS: i64 = 30;

fn db_guest_port_to_proto(value: i32) -> u32 {
    u16::try_from(value).map(u32::from).unwrap_or(0)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn db_generation_to_proto(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn emit_ready_suspend_projection_event(
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    from_lifecycle: &str,
) {
    SandboxEvent::new(
        sandbox_id,
        SandboxEventType::SuspendCompleted,
        SandboxEventSource::Api,
    )
    .emit_with(
        SandboxEventDetails::default()
            .reason("artifact_backup_finalized")
            .operation("finalize_suspend_snapshot")
            .trigger(SandboxEventTrigger::System)
            .with_from_state(from_lifecycle)
            .with_to_state("suspended")
            .source_node_id(source_node_id.to_string()),
    );
}

fn u32_to_i32(value: u32, field: &str) -> Result<i32, Status> {
    i32::try_from(value)
        .map_err(|error| Status::invalid_argument(format!("{field} exceeds i32::MAX: {error}")))
}

fn scheduling_status_label(status: i32) -> &'static str {
    match NodeSchedulingStatus::try_from(status).unwrap_or_default() {
        NodeSchedulingStatus::Ready => "ready",
        NodeSchedulingStatus::Warming => "warming",
        NodeSchedulingStatus::Draining => "draining",
        NodeSchedulingStatus::Degraded => "degraded",
        NodeSchedulingStatus::Empty => "empty",
        NodeSchedulingStatus::Unavailable => "unavailable",
        NodeSchedulingStatus::Full => "full",
        NodeSchedulingStatus::Unspecified => "unknown",
    }
}

fn node_ctl_status_json(status: NodeCtlStatus) -> serde_json::Value {
    serde_json::json!({
        "reachable": status.reachable,
        "error": status.error,
        "reconciliations": status
            .reconciliations
            .into_iter()
            .map(|status| {
                serde_json::json!({
                    "name": status.name,
                    "poll_interval_secs": status.poll_interval_secs,
                    "last_started_at_unix_secs": status.last_started_at_unix_secs,
                    "last_completed_at_unix_secs": status.last_completed_at_unix_secs,
                    "last_success_at_unix_secs": status.last_success_at_unix_secs,
                    "next_due_at_unix_secs": status.next_due_at_unix_secs,
                    "running": status.running,
                    "templates_seen": status.templates_seen,
                    "sandboxes_seen": status.sandboxes_seen,
                    "templates_applied": status.templates_applied,
                    "sandboxes_applied": status.sandboxes_applied,
                    "last_error": status.last_error,
                })
            })
            .collect::<Vec<_>>(),
    })
}

struct PublishedNodeStatusDb {
    scheduling_status: String,
    scheduling_reason: String,
    network_slots_ready: i32,
    network_pool_min_ready: i32,
    network_slots_degraded: i32,
    network_slots_quarantined: i32,
    network_slots_total: i32,
    network_slots_leased: i32,
    network_pool_max_slots: i32,
    node_ctl: Option<serde_json::Value>,
}

fn published_node_status_db(status: PublishedNodeStatus) -> Result<PublishedNodeStatusDb, Status> {
    Ok(PublishedNodeStatusDb {
        scheduling_status: scheduling_status_label(status.scheduling_status).to_string(),
        scheduling_reason: status.scheduling_reason,
        network_slots_ready: u32_to_i32(status.network_slots_ready, "network_slots_ready")?,
        network_pool_min_ready: u32_to_i32(
            status.network_pool_min_ready,
            "network_pool_min_ready",
        )?,
        network_slots_degraded: u32_to_i32(
            status.network_slots_degraded,
            "network_slots_degraded",
        )?,
        network_slots_quarantined: u32_to_i32(
            status.network_slots_quarantined,
            "network_slots_quarantined",
        )?,
        network_slots_total: u32_to_i32(status.network_slots_total, "network_slots_total")?,
        network_slots_leased: u32_to_i32(status.network_slots_leased, "network_slots_leased")?,
        network_pool_max_slots: u32_to_i32(
            status.network_pool_max_slots,
            "network_pool_max_slots",
        )?,
        node_ctl: status.node_ctl.map(node_ctl_status_json),
    })
}

#[derive(Clone)]
pub struct ClusterApiService {
    ingress_activation: Option<std::sync::Arc<dyn ingress_activation::IngressActivation>>,
    reserved_template_names: template_publication::ReservedTemplateNames,
    trusted_template_storage: trusted_storage::TrustedTemplateStorageBuckets,
    pool: sqlx::PgPool,
    /// Pool for the best-effort template-visibility lookup, kept separate from
    /// `pool` so that lookup can never starve release resolution.
    ///
    /// The release queries never read `templates`, so DDL on that table cannot
    /// block them directly — but the visibility lookup does read it, and a
    /// blocked statement holds its connection until the role's
    /// `statement_timeout`. Sharing `pool` would therefore let an
    /// `ALTER TABLE templates` drain it and fail the *release* query on acquire,
    /// which is exactly the "never take down release resolution" property the
    /// lookup is designed around. A small dedicated pool bounds the damage to
    /// the lookup itself: its acquires fail and `pool` is untouched. Note the
    /// second half is *not* "and it degrades to `Unspecified`" — that was true
    /// before the cache. An acquire failure carries no `DatabaseError`, so it
    /// takes the serve-stale arm, and with a warm entry the request is answered
    /// from the last successful read and emits no `degraded`. Exhaustion is
    /// visible as `template.visibility_lookup.cache{outcome:served_stale}`;
    /// degradation only begins once the cache can no longer cover.
    ///
    /// Defaults to a clone of `pool`; the service binary replaces it via
    /// [`ClusterApiService::with_visibility_pool`]. Tests share one pool because
    /// the isolation is a pool-configuration property, not request logic.
    visibility_pool: sqlx::PgPool,
    visibility_observations: std::sync::Arc<visibility_observations::Observations>,
    template_releases_store: template_releases_store::TemplateReleasesStore,
    route_ed25519_authority: Option<
        std::sync::Arc<
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
        >,
    >,
    ecw_credential_keyring:
        std::sync::Arc<pplx_space_common_svc::ecw_credential::EcwCredentialKeyring>,
    /// Last-known `templates.visibility` per template, so one database problem
    /// cannot suppress visibility-gated prefetch warming fleet-wide. Sits in
    /// front of `visibility_pool`, which is the complementary half: the pool
    /// keeps this lookup from harming release resolution, the cache keeps a
    /// failed lookup from harming the lookup's consumers. Per-service rather
    /// than a `static` so each instance — and each test fixture — owns its own.
    /// See `template_visibility_cache`.
    template_visibility_cache: template_visibility_cache::TemplateVisibilityCache,
}

#[derive(Debug, Clone, FromRow)]
struct NodeRow {
    id: String,
    name: Option<String>,
    address: Option<String>,
    node_api_endpoint: String,
    role: String,
    managed_by: String,
    state: Option<String>,
    last_heartbeat: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    deleted_at: Option<OffsetDateTime>,
    metadata_json: String,
}

#[derive(Debug, Clone, FromRow)]
struct OriginNodeRow {
    id: NodeId,
    address: Option<String>,
    role: String,
}

#[derive(Debug, Clone)]
struct OriginNode {
    id: NodeId,
    display_id: String,
    address: Option<IpAddr>,
    role: SpaceNodeRole,
}

/// Capture-time fields the `snapshots` row inherits from the source sandbox.
#[derive(FromRow)]
struct SnapshotSandboxRow {
    org_id: OrgId,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
    template_id: Option<String>,
    content_hash: Option<String>,
}

impl SnapshotSandboxRow {
    /// The node the snapshot is physically uploading **from**, recorded as
    /// `snapshots.source_node_id`. For a fresh suspend the VM is still placed,
    /// so `active_node_id` is the source. For a **reupload** of an
    /// already-suspended sandbox, `active_node_id` is NULL (cleared at
    /// suspend), but the on-disk artifacts and the reupload worker live on
    /// `target_node_id` (preserved across suspend) — so fall back to it.
    /// Without this, reupload snapshots get a NULL `source_node_id` and the
    /// node-scoped in-flight-upload drain guard can't attribute them to the
    /// node actually running the upload, letting that node be terminated
    /// mid-reupload.
    fn source_node_id(&self) -> Option<NodeId> {
        self.active_node_id.or(self.target_node_id)
    }

    fn new_snapshot(
        &self,
        id: SnapshotId,
        sandbox_id: SandboxId,
        kind: SnapshotKind,
        state: SnapshotState,
        storage_uri: ExactStorageUri,
    ) -> Result<snapshot_store::NewSnapshot, Status> {
        let template_id = self
            .template_id
            .as_deref()
            .map(TemplateName::parse)
            .transpose()
            .map_err(|error| {
                Status::failed_precondition(format!("sandbox has invalid template_id: {error}"))
            })?;
        let content_hash = self
            .content_hash
            .clone()
            .map(SnapshotContentHash::from_stored);
        Ok(
            snapshot_store::NewSnapshot::new(id, sandbox_id, kind, state, self.org_id, storage_uri)
                .with_source_node_id(self.source_node_id())
                .with_resources(self.vcpu, self.memory_mib)
                .with_template(template_id, content_hash),
        )
    }
}

/// What a `GetArtifactBackupLocation` request asks. The two wire shapes are
/// told apart before any lookup, so a readiness query cannot select a
/// storage location.
enum ArtifactBackupQuery {
    /// Where the ready backup of this exact local artifact lives.
    Location(LocalArtifactId, ArtifactKind),
    Readiness(BackupReadiness),
}

/// An identity the calling node already holds, to prove backed up.
enum BackupReadiness {
    /// These exact immutable bytes have a ready, non-orphaned backup.
    Artifact(LocalArtifactId, ArtifactKind),
    /// An unpinned cache's suspend attempt is still the latest ready one.
    LatestReadySuspend(SnapshotId),
    /// The sandbox is durably restorable from a ready snapshot.
    DurablyRestorable,
}

fn parse_artifact_backup_query(
    req: GetArtifactBackupLocationRequest,
) -> Result<(SandboxId, ArtifactBackupQuery), Status> {
    use crate::proto::backup_readiness_query::Artifact;
    let sandbox_id = SandboxId::from_str(req.sandbox_id.trim())
        .map_err(|e| Status::invalid_argument(format!("invalid sandbox_id: {e}")))?;
    let parse_artifact = |local_artifact_id: &str, artifact_kind| {
        let local_artifact_id = LocalArtifactId::from_str(local_artifact_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid local_artifact_id: {e}")))?;
        let artifact_kind = artifact_kind_from_proto(artifact_kind)
            .ok_or_else(|| Status::invalid_argument("artifact_kind must be DISK or FULL"))?;
        Ok::<_, Status>((local_artifact_id, artifact_kind))
    };
    let Some(readiness) = req.readiness else {
        let (local_artifact_id, artifact_kind) =
            parse_artifact(&req.local_artifact_id, req.artifact_kind)?;
        return Ok((
            sandbox_id,
            ArtifactBackupQuery::Location(local_artifact_id, artifact_kind),
        ));
    };
    // A readiness query names its identity only inside `readiness`, which is
    // what makes a server without readiness support reject it.
    if !req.local_artifact_id.is_empty()
        || req.artifact_kind != pplx_space_sbx_mgr_client::proto::ArtifactKind::Unspecified as i32
    {
        return Err(Status::invalid_argument(
            "a readiness query must leave local_artifact_id and artifact_kind unset",
        ));
    }
    let readiness = match readiness.artifact {
        Some(Artifact::LocalArtifact(identity)) => {
            let (local_artifact_id, artifact_kind) =
                parse_artifact(&identity.local_artifact_id, identity.artifact_kind)?;
            BackupReadiness::Artifact(local_artifact_id, artifact_kind)
        }
        Some(Artifact::LatestReadySuspendSnapshotId(snapshot_id)) => {
            BackupReadiness::LatestReadySuspend(SnapshotId::from_str(snapshot_id.trim()).map_err(
                |e| {
                    Status::invalid_argument(format!(
                        "invalid latest_ready_suspend_snapshot_id: {e}"
                    ))
                },
            )?)
        }
        Some(Artifact::DurablyRestorable(true)) => BackupReadiness::DurablyRestorable,
        Some(Artifact::DurablyRestorable(false)) => {
            return Err(Status::invalid_argument("durably_restorable must be true"));
        }
        None => {
            return Err(Status::invalid_argument(
                "readiness artifact identity is required",
            ));
        }
    };
    Ok((sandbox_id, ArtifactBackupQuery::Readiness(readiness)))
}

/// Parse the digests a node reports at finalize: one entry per artifact.
fn parse_artifact_digests(
    digests: &[crate::proto::SnapshotArtifactDigest],
) -> Result<Vec<snapshot_store::SnapshotArtifactDigest>, Status> {
    let mut seen = std::collections::BTreeSet::new();
    digests
        .iter()
        .map(|entry| {
            let artifact = SnapshotArtifactFile::from_str(&entry.artifact).map_err(|e| {
                Status::invalid_argument(format!("invalid artifact_digests.artifact: {e}"))
            })?;
            if !seen.insert(artifact) {
                return Err(Status::invalid_argument(format!(
                    "duplicate artifact_digests entry for {artifact}"
                )));
            }
            let digest = Digest::from_str(&entry.digest).map_err(|e| {
                Status::invalid_argument(format!(
                    "invalid artifact_digests.digest for {artifact}: {e}"
                ))
            })?;
            Ok(snapshot_store::SnapshotArtifactDigest {
                artifact,
                digest,
                size_bytes: entry.size_bytes,
            })
        })
        .collect()
}

fn snapshot_insert_status(error: snapshot_store::InsertSnapshotError) -> Status {
    match error {
        snapshot_store::InsertSnapshotError::IdentityConflict { .. }
        | snapshot_store::InsertSnapshotError::StorageLocationConflict { .. } => {
            Status::failed_precondition(error.to_string())
        }
        _ => Status::internal(format!("insert snapshot catalog: {error}")),
    }
}

/// The node uploads each attempt under `.../<sandbox_id>/<backup_attempt_id>`;
/// requiring that suffix keeps a row from naming another sandbox's objects.
fn storage_uri_names_backup_attempt(
    storage_uri: &ExactStorageUri,
    sandbox_id: SandboxId,
    backup_attempt_id: SnapshotId,
) -> bool {
    let attempt_prefix = format!("{sandbox_id}/{backup_attempt_id}");
    let key_prefix = storage_uri.parsed().key_prefix.as_str();
    key_prefix == attempt_prefix || key_prefix.ends_with(&format!("/{attempt_prefix}"))
}

impl ClusterApiService {
    pub fn new(pool: sqlx::PgPool) -> Self {
        let route_ed25519_authority =
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority::load_from_env()
                .ok()
                .flatten()
                .map(std::sync::Arc::new);
        let ecw_credential_keyring = std::sync::Arc::new(
            pplx_space_common_svc::ecw_credential::EcwCredentialKeyring::load_from_env()
                .expect("ClusterApiService ECW credential configuration must be valid"),
        );
        let trusted_template_storage = trusted_storage::TrustedTemplateStorageBuckets::from_env()
            .expect("ClusterApiService template storage configuration must be valid");
        Self {
            trusted_template_storage,
            ..Self::with_security_state(pool, route_ed25519_authority, ecw_credential_keyring)
        }
    }

    fn with_security_state(
        pool: sqlx::PgPool,
        route_ed25519_authority: Option<
            std::sync::Arc<
                pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
            >,
        >,
        ecw_credential_keyring: std::sync::Arc<
            pplx_space_common_svc::ecw_credential::EcwCredentialKeyring,
        >,
    ) -> Self {
        let template_releases_store =
            template_releases_store::TemplateReleasesStore::new(pool.clone());
        Self {
            ingress_activation: None,
            reserved_template_names: template_publication::ReservedTemplateNames::default(),
            trusted_template_storage: trusted_storage::TrustedTemplateStorageBuckets::default(),
            visibility_pool: pool.clone(),
            visibility_observations: std::sync::Arc::new(
                visibility_observations::Observations::new(),
            ),
            template_visibility_cache: template_visibility_cache::TemplateVisibilityCache::from_env(
            ),
            pool,
            template_releases_store,
            route_ed25519_authority,
            ecw_credential_keyring,
        }
    }

    /// Point the template-visibility lookup at its own pool.
    ///
    /// See [`ClusterApiService::visibility_pool`]. Call this with a small
    /// dedicated pool in any deployment that serves sandbox-create pulls;
    /// without it the lookup shares the load-bearing pool and an
    /// `ALTER TABLE templates` can starve release resolution.
    #[must_use]
    pub fn with_visibility_pool(mut self, visibility_pool: sqlx::PgPool) -> Self {
        self.visibility_pool = visibility_pool;
        self
    }

    /// Replace the template-visibility cache, for tests that need a specific
    /// freshness/staleness configuration.
    ///
    /// Production reads the configuration from the environment in
    /// `with_security_state`; this exists so a test can pin
    /// one window open and the other shut, which is the only way to attribute
    /// an answer to a specific cache path from outside. Mirrors
    /// [`ClusterApiService::with_visibility_pool`].
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_template_visibility_cache(
        mut self,
        cache: template_visibility_cache::TemplateVisibilityCache,
    ) -> Self {
        self.template_visibility_cache = cache;
        self
    }

    pub async fn from_env_with_security_state(
        pool: sqlx::PgPool,
        route_ed25519_authority: Option<
            std::sync::Arc<
                pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
            >,
        >,
        ecw_credential_keyring: std::sync::Arc<
            pplx_space_common_svc::ecw_credential::EcwCredentialKeyring,
        >,
    ) -> Result<Self, Status> {
        let mut service =
            Self::with_security_state(pool, route_ed25519_authority, ecw_credential_keyring);
        service.reserved_template_names = template_publication::ReservedTemplateNames::from_env()?;
        service.trusted_template_storage =
            trusted_storage::TrustedTemplateStorageBuckets::from_env()
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(service)
    }

    /// Transition a snapshot row from `creating` to a terminal state. Does
    /// **not** touch `snapshots.timestamp`: the column is the suspend's
    /// creation time and the portability gate
    /// (`timestamp >= placement_changed_at`) relies on its immutability to
    /// reject late finalizes of stale uploads.
    ///
    /// Idempotent on `(snapshot_id, target_state)`: re-finalizing into the
    /// same terminal state is a no-op. Crossing terminal states (READY →
    /// FAILED or vice versa) is rejected as `FAILED_PRECONDITION` so a buggy
    /// caller can't silently rewrite history.
    ///
    /// `target_state` must be `Ready` or `Failed`; the wire boundary
    /// ([`ClusterApiService::finalize_artifact_backup`]) rejects anything
    /// else before calling this.
    ///
    /// Only the attempt's source node may close it; the check precedes the
    /// transaction so a refused caller writes nothing.
    async fn finalize_backup_attempt(
        &self,
        snapshot_id: SnapshotId,
        target_state: SnapshotState,
        bytes_uploaded: i64,
        failure_reason: Option<&str>,
        artifact_digests: &[snapshot_store::SnapshotArtifactDigest],
        remote_addr: Option<std::net::SocketAddr>,
    ) -> Result<(), Status> {
        let attempt = self
            .registered_backup_attempt(snapshot_id)
            .await?
            .ok_or_else(|| Status::not_found(format!("snapshot {snapshot_id} not found")))?;
        let source_node = self
            .authorize_backup_source_node(
                &self.pool,
                attempt.source_node_id(),
                remote_addr,
                "backup attempt",
            )
            .await?;

        // For READY, persist the uploaded bytes and clear any
        // prior-attempt failure reason. For FAILED, persist the operator
        // reason (best-effort — callers may omit it) and ignore the byte
        // count.
        let size_bytes = target_state.is_ready().then_some(bytes_uploaded);
        let failure_reason: Option<&str> = if target_state.is_ready() {
            None
        } else {
            failure_reason.map(str::trim).filter(|s| !s.is_empty())
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal(format!("begin finalize snapshot tx: {e}")))?;

        // `RETURNING` the sandbox so a READY transition can wake the
        // cohort-restore listener without a second lookup, and the node and
        // artifact identity so it can clear exactly this artifact's row from
        // the node's pending-backup mirror. Taking all four from the row this
        // statement just flipped is what makes that delete safe: nothing else
        // here knows which artifact the caller finalized.
        let finalized_snapshot: Option<FinalizedSnapshotRow> = sqlx::query_as(sql!(
            "UPDATE snapshots
                SET state = $2,
                    size_bytes = COALESCE($3, size_bytes),
                    failure_reason = CASE
                        WHEN $2 = 'failed' THEN COALESCE($4, failure_reason)
                        ELSE NULL
                    END
              WHERE id = $1
                AND state IN ('creating', $2)
            RETURNING sandbox_id, source_node_id, local_artifact_id, artifact_kind"
        ))
        .bind(snapshot_id)
        .bind(target_state.as_str())
        .bind(size_bytes)
        .bind(failure_reason)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Status::internal(format!("finalize snapshot: {e}")))?;

        let Some(finalized_snapshot) = finalized_snapshot else {
            // Either the row doesn't exist or it's already in the other
            // terminal state. Disambiguate so the caller gets the right
            // gRPC code: NOT_FOUND for missing, FAILED_PRECONDITION for
            // crossed terminal states.
            let current_state: Option<String> =
                sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
                    .bind(snapshot_id)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| Status::internal(format!("snapshot state lookup: {e}")))?;
            return match current_state {
                None => Err(Status::not_found(format!(
                    "snapshot {snapshot_id} not found"
                ))),
                Some(existing) => Err(Status::failed_precondition(format!(
                    "snapshot {snapshot_id} already in terminal state {existing:?}; \
                     cannot transition to {}",
                    target_state.as_str()
                ))),
            };
        };
        // Decide on the row this statement flipped, not the earlier read.
        if finalized_snapshot.source_node_id != Some(source_node) {
            return Err(Status::permission_denied(
                "backup attempt is bound to a different node than the caller",
            ));
        }
        let sandbox_id = finalized_snapshot.sandbox_id;

        if target_state.is_ready() {
            snapshot_store::record_artifact_digests(&mut tx, snapshot_id, artifact_digests)
                .await
                .map_err(|error| match &error {
                    snapshot_store::RecordArtifactDigestsError::Conflict { .. } => {
                        Status::failed_precondition(error.to_string())
                    }
                    snapshot_store::RecordArtifactDigestsError::SizeOutOfRange { .. } => {
                        Status::invalid_argument(error.to_string())
                    }
                    snapshot_store::RecordArtifactDigestsError::Database(_) => {
                        Status::internal(format!("record artifact digests: {error}"))
                    }
                })?;
        }

        let suspend_projection = if target_state.is_ready() {
            project_ready_suspend_snapshot(&mut tx, snapshot_id).await?
        } else {
            None
        };

        // The fast path out of the node's pending-backup mirror. Scoped to the
        // exact artifact this call made durable, so finalizing an older
        // artifact cannot remove a newer artifact's row.
        //
        // `is_ready()` is a cheap early-out, not the guarantee: a `failed`
        // finalize produced no durable bytes, so `clear_finalized`'s own
        // `artifact_has_durable_copy` conjunct would decline it anyway. The
        // two only diverge for an artifact already made durable by a
        // different snapshot, where skipping is the conservative answer.
        // A snapshot row missing either identity cannot name a queue row —
        // every mirrored row carries a non-null node and artifact — so the
        // fast path skips it and the census set-replace stays the backstop for
        // every row this path does not reach.
        let cleared_pending_backup = match (target_state.is_ready(), finalized_snapshot.artifact())
        {
            (true, Some(artifact)) => {
                node_local_backup_pending::clear_finalized(&mut tx, artifact).await?
            }
            _ => false,
        };

        tx.commit()
            .await
            .map_err(|e| Status::internal(format!("commit finalize snapshot tx: {e}")))?;

        if cleared_pending_backup {
            pplx_space_metrics::internal_api::local_backup_queue_removed(
                pplx_space_metrics::internal_api::LocalBackupQueueRemoval::Finalized,
                1,
            );
        }

        // Every backup upload finalizes `ready` here, so this is the primary
        // wake point for the controller's event-driven cohort restore.
        if target_state.is_ready() {
            self.notify_evacuation_restore_ready(sandbox_id).await;
            if let Some((projected_sandbox_id, source_node_id, from_lifecycle)) = suspend_projection
            {
                emit_ready_suspend_projection_event(
                    projected_sandbox_id,
                    source_node_id,
                    &from_lifecycle,
                );
            }
        }

        Ok(())
    }

    /// Heartbeat a still-in-flight backup upload by bumping
    /// `snapshots.last_attempt_at` to `NOW()`. The on-node upload worker
    /// calls this best-effort at the start of each retry attempt so the
    /// cluster-controller's stuck-snapshot reaper — which ages a row from
    /// `COALESCE(last_attempt_at, timestamp)` — pauses its clock alongside
    /// the worker's own retry clock while the node is making progress (or
    /// recovering after an outage).
    ///
    /// Scoped to `state='creating'` so a row the worker has already
    /// finalized (`ready`/`failed`) is a silent no-op rather than having a
    /// terminal row's heartbeat resurrected. A missing row is likewise a
    /// no-op — the heartbeat is advisory, not a correctness gate, so there's
    /// no value in distinguishing "missing" from "already terminal" with an
    /// error the caller would only swallow.
    async fn touch_backup_attempt(&self, snapshot_id: SnapshotId) -> Result<(), Status> {
        sqlx::query(sql!(
            "UPDATE snapshots
                SET last_attempt_at = NOW()
              WHERE id = $1
                AND state = 'creating'"
        ))
        .bind(snapshot_id)
        .execute(&self.pool)
        .await
        .map_err(|e| Status::internal(format!("touch snapshot: {e}")))?;

        Ok(())
    }

    pub(crate) async fn begin_artifact_backup(
        &self,
        request: Request<BeginArtifactBackupRequest>,
    ) -> Result<Response<BeginArtifactBackupResponse>, Status> {
        let remote_addr = request.remote_addr();
        let req = request.into_inner();
        let sandbox_id = SandboxId::from_str(req.sandbox_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid sandbox_id: {e}")))?;
        let local_artifact_id = LocalArtifactId::from_str(req.local_artifact_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid local_artifact_id: {e}")))?;
        let backup_attempt_id = BackupAttemptId::from_str(req.backup_attempt_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid backup_attempt_id: {e}")))?;
        // The catalog schema stores every backup attempt in `snapshots`; keep
        // the workflow ID distinct outside this store boundary.
        let snapshot_id = SnapshotId::from_uuid(backup_attempt_id.as_uuid());
        let artifact_kind = artifact_kind_from_proto(req.artifact_kind)
            .ok_or_else(|| Status::invalid_argument("artifact_kind must be DISK or FULL"))?;
        let artifact_origin =
            match pplx_space_sbx_mgr_client::proto::ArtifactOrigin::try_from(req.artifact_origin) {
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop) => ArtifactOrigin::Stop,
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend) => {
                    ArtifactOrigin::Suspend
                }
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Snapshot) => {
                    ArtifactOrigin::Snapshot
                }
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Fork) => ArtifactOrigin::Fork,
                _ => {
                    return Err(Status::invalid_argument(
                        "artifact_origin must be STOP, SUSPEND, SNAPSHOT, or FORK",
                    ));
                }
            };
        let snapshot_name = req
            .snapshot_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty());
        let snapshot_name = snapshot_name
            .map(SnapshotName::parse)
            .transpose()
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot_name: {e}")))?;
        let artifact = snapshot_store::SnapshotArtifact::new(
            Some(local_artifact_id),
            artifact_kind,
            artifact_origin,
            snapshot_name,
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let snapshot_kind = match artifact_kind {
            ArtifactKind::Disk => SnapshotKind::Disk,
            ArtifactKind::Full => SnapshotKind::Full,
        };
        let storage_uri = req.storage_uri.trim();
        if storage_uri.is_empty() {
            return Err(Status::invalid_argument("storage_uri is required"));
        }
        let storage_uri = ExactStorageUri::parse(storage_uri)
            .map_err(|e| Status::invalid_argument(format!("invalid storage_uri: {e}")))?;
        if !storage_uri_names_backup_attempt(&storage_uri, sandbox_id, snapshot_id) {
            return Err(Status::invalid_argument(format!(
                "storage_uri must end with {sandbox_id}/{backup_attempt_id}, the per-attempt \
                 prefix the node uploads under"
            )));
        }
        let execution_metadata = snapshot_execution_metadata(req.execution.as_ref())?
            .with_host_cpu(capture_host_cpu(
                req.capture_host_cpu.as_deref(),
                artifact_kind,
                sandbox_id,
            ));
        let execution_generation = req
            .execution_generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|e| Status::invalid_argument(format!("invalid execution_generation: {e}")))?;
        let sandbox = self.lookup_snapshot_sandbox(sandbox_id).await?;
        let snapshot = sandbox
            .new_snapshot(
                snapshot_id,
                sandbox_id,
                snapshot_kind,
                SnapshotState::Creating,
                storage_uri,
            )?
            .with_artifact(artifact)
            .with_execution_metadata(execution_metadata);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Status::internal(format!("begin tx: {e}")))?;

        let (persisted, outcome) = snapshot_store::insert(&mut tx, &snapshot)
            .await
            .map_err(snapshot_insert_status)?;
        // Only a capture's first registration is fenced. Refusing a replay
        // would fail an upload the API's own suspend may be waiting on. A
        // fresh attempt for bytes an earlier attempt already registered is a
        // retry of a capture the control plane admitted while its execution
        // was current, which started recovery may be restoring from, so it
        // registers too. A row registered before the fence was deployed, or
        // without a generation, replays unfenced. The fence runs before the
        // source-node check: a superseded generation is superseded on every
        // node, so a stale runtime on a node the sandbox has since moved off
        // still learns so and releases itself instead of retrying a
        // permission refusal.
        if outcome == snapshot_store::InsertOutcome::Created
            && let Some(execution_generation) = execution_generation
            && !snapshot_store::artifact_registered_by_another_attempt(
                &mut *tx,
                sandbox_id,
                local_artifact_id,
                persisted.id(),
            )
            .await
            .map_err(|e| Status::internal(format!("backup registration lookup: {e}")))?
        {
            backup_generation_fence::refuse_superseded_backup_execution(
                &mut tx,
                sandbox_id,
                execution_generation,
            )
            .await?;
        }
        // Authorize on the persisted row: a replay stays bound to the node
        // that registered the attempt even after retirement or relocation,
        // and a read before the transaction could see a row retention
        // deletes before the insert. A refusal rolls the insert back.
        self.authorize_backup_source_node(
            &mut *tx,
            persisted.source_node_id(),
            remote_addr,
            "backup attempt",
        )
        .await?;
        // A verdict-only request has had its answer: roll the insert back, so
        // asking can't leave a row that outranks the sandbox's ready snapshot.
        if req.fence_verdict_only {
            tx.rollback()
                .await
                .map_err(|e| Status::internal(format!("rollback tx: {e}")))?;
        } else {
            tx.commit()
                .await
                .map_err(|e| Status::internal(format!("commit tx: {e}")))?;
        }

        Ok(Response::new(BeginArtifactBackupResponse {
            local_artifact_id: local_artifact_id.to_string(),
            backup_attempt_id: backup_attempt_id.to_string(),
            fence_verdict_only: req.fence_verdict_only,
        }))
    }

    pub(crate) async fn finalize_artifact_backup(
        &self,
        request: Request<FinalizeArtifactBackupRequest>,
    ) -> Result<Response<FinalizeArtifactBackupResponse>, Status> {
        let remote_addr = request.remote_addr();
        let req = request.into_inner();
        let backup_attempt_id = BackupAttemptId::from_str(req.backup_attempt_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid backup_attempt_id: {e}")))?;
        let target_state = match SuspendSnapshotTerminalState::try_from(req.target_state) {
            Ok(SuspendSnapshotTerminalState::Ready) => SnapshotState::Ready,
            Ok(SuspendSnapshotTerminalState::Failed) => SnapshotState::Failed,
            Ok(SuspendSnapshotTerminalState::Unspecified) | Err(_) => {
                return Err(Status::invalid_argument(
                    "target_state must be READY or FAILED",
                ));
            }
        };
        let artifact_digests = parse_artifact_digests(&req.artifact_digests)?;
        self.finalize_backup_attempt(
            SnapshotId::from_uuid(backup_attempt_id.as_uuid()),
            target_state,
            req.bytes_uploaded,
            req.failure_reason.as_deref(),
            &artifact_digests,
            remote_addr,
        )
        .await?;
        Ok(Response::new(FinalizeArtifactBackupResponse {}))
    }

    pub(crate) async fn touch_artifact_backup(
        &self,
        request: Request<TouchArtifactBackupRequest>,
    ) -> Result<Response<TouchArtifactBackupResponse>, Status> {
        let req = request.into_inner();
        let backup_attempt_id = BackupAttemptId::from_str(req.backup_attempt_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid backup_attempt_id: {e}")))?;
        self.touch_backup_attempt(SnapshotId::from_uuid(backup_attempt_id.as_uuid()))
            .await?;
        Ok(Response::new(TouchArtifactBackupResponse {}))
    }

    pub(crate) async fn get_artifact_backup_location(
        &self,
        request: Request<GetArtifactBackupLocationRequest>,
    ) -> Result<Response<GetArtifactBackupLocationResponse>, Status> {
        let node_ids = self.caller_node_ids(&request).await?;
        let (sandbox_id, query) = parse_artifact_backup_query(request.into_inner())?;
        let (local_artifact_id, artifact_kind) = match query {
            ArtifactBackupQuery::Location(local_artifact_id, artifact_kind) => {
                (local_artifact_id, artifact_kind)
            }
            ArtifactBackupQuery::Readiness(readiness) => {
                return self
                    .artifact_backup_readiness(sandbox_id, &node_ids, readiness)
                    .await;
            }
        };

        let mut tx = self.begin_restore_lookup().await?;

        let row = snapshot_store::ready_backup_location_placed_on(
            &mut *tx,
            sandbox_id,
            local_artifact_id,
            artifact_kind,
            &node_ids,
        )
        .await
        .map_err(|e| Status::internal(format!("artifact backup lookup: {e}")))?
        .ok_or_else(|| {
            Status::not_found(format!(
                "no ready backup for local artifact {local_artifact_id}"
            ))
        })?;
        let artifact_digests = Self::artifact_digests_for_restore(&mut tx, row.snapshot_id).await?;
        tx.commit()
            .await
            .map_err(|e| Status::internal(format!("finish artifact backup lookup: {e}")))?;
        Ok(Response::new(GetArtifactBackupLocationResponse {
            local_artifact_id: local_artifact_id.to_string(),
            artifact_kind: artifact_kind_to_proto(artifact_kind),
            backup_attempt_id: row.snapshot_id.to_string(),
            storage_uri: row.storage_connection_string,
            bytes_uploaded: row.size_bytes.unwrap_or(0),
            artifact_digests,
            ready: None,
        }))
    }

    /// Answer a readiness query with `ready` alone. Former hosts retain local
    /// caches after placement moves, so any registered node may ask about an
    /// identity it already holds; location reads stay placement-scoped, and
    /// eviction needs no storage credentials. The durably-restorable query
    /// names only the sandbox, so it tells any registered node whether a
    /// sandbox it names exists and is restorable, and nothing more.
    async fn artifact_backup_readiness(
        &self,
        sandbox_id: SandboxId,
        node_ids: &[NodeId],
        readiness: BackupReadiness,
    ) -> Result<Response<GetArtifactBackupLocationResponse>, Status> {
        if node_ids.is_empty() {
            return Err(Status::permission_denied("caller is not a registered node"));
        }
        let readiness_error =
            |e: &dyn std::fmt::Display| Status::internal(format!("artifact backup readiness: {e}"));
        let ready = match readiness {
            BackupReadiness::Artifact(local_artifact_id, artifact_kind) => {
                snapshot_store::artifact_backup_ready(
                    &self.pool,
                    sandbox_id,
                    local_artifact_id,
                    artifact_kind,
                )
                .await
                .map_err(|e| readiness_error(&e))?
            }
            BackupReadiness::LatestReadySuspend(snapshot_id) => {
                snapshot_store::is_latest_ready_suspend(&self.pool, sandbox_id, snapshot_id)
                    .await
                    .map_err(|e| readiness_error(&e))?
            }
            BackupReadiness::DurablyRestorable => {
                pplx_space_sandbox_store::durably_restorable(&self.pool, sandbox_id)
                    .await
                    .map_err(|e| readiness_error(&e))?
            }
        };
        Ok(Response::new(GetArtifactBackupLocationResponse {
            ready: Some(ready),
            ..Default::default()
        }))
    }

    pub(crate) async fn get_quarantine_recovery_plan(
        &self,
        request: Request<GetQuarantineRecoveryPlanRequest>,
    ) -> Result<Response<GetQuarantineRecoveryPlanResponse>, Status> {
        let role_hint = status::optional_db_role_from_proto(request.get_ref().role)?
            .or(Some(SpaceNodeRole::Worker));
        let origin = self
            .authenticated_origin_node_id(&request, &request.get_ref().node_id, role_hint)
            .await?;
        let sandbox_id = SandboxId::from_str(request.get_ref().sandbox_id.trim())
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let plan = sandbox_quarantine_store::get_quarantine_recovery_plan(
            &self.pool, sandbox_id, origin.id,
        )
        .await?;

        Ok(Response::new(GetQuarantineRecoveryPlanResponse {
            recovery_id: plan.recovery_id.to_string(),
            execution_generation: plan.execution_generation,
            reason_code: plan.reason_code.to_string(),
            target_state: QuarantineRecoveryTargetState::Suspended as i32,
            suspend_snapshot: Some(QuarantineRecoverySuspendSnapshot {
                snapshot_id: plan.snapshot_id.to_string(),
                local_artifact_id: plan
                    .local_artifact_id
                    .map(|local_artifact_id| local_artifact_id.to_string())
                    .unwrap_or_default(),
                storage_uri: plan.storage_uri,
                bytes_uploaded: plan.bytes_uploaded,
            }),
        }))
    }

    /// Wake the cluster-controller's node-drain loop so it drives the cohort
    /// restore of a drain-evacuated sandbox the moment its suspend snapshot is
    /// durable on S3 (`ready`), instead of waiting for the next periodic
    /// sweep. Best-effort and unconditional: the controller's sweep re-derives
    /// the restore-pending set (only marker-carrying evacuations are
    /// restored), and the periodic sweep is the correctness fallback, so a
    /// failed or spurious NOTIFY only affects latency.
    async fn notify_evacuation_restore_ready(&self, sandbox_id: SandboxId) {
        if let Err(err) = sqlx::query(sql!("SELECT pg_notify($1, $2)"))
            .bind(EVACUATION_RESTORE_READY_CHANNEL)
            .bind(sandbox_id.to_string())
            .execute(&self.pool)
            .await
        {
            tracing::warn!(
                error = %err,
                sandbox_id = %sandbox_id,
                "failed to NOTIFY cohort-restore listener; the periodic restore sweep \
                 will still pick it up"
            );
        }
    }

    /// Shared sandbox lookup for the artifact-backup write path. Fetches
    /// the org-scoped, capture-time fields that the `snapshots` row inherits
    /// from the source sandbox (org_id, active_node_id, vcpu, memory_mib,
    /// template_id, content_hash).
    async fn lookup_snapshot_sandbox(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<SnapshotSandboxRow, Status> {
        sqlx::query_as::<_, SnapshotSandboxRow>(sql!(
            // A primary-key fetch is sufficient: `snapshot_execution_metadata`
            // requires caller-supplied execution metadata, so no template or
            // runtime-guest lookup is needed here.
            "SELECT org_id, active_node_id, target_node_id, vcpu, memory_mib,
                    template_id, content_hash
             FROM sandboxes
             WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| Status::internal(format!("sandbox lookup: {e}")))?
        .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))
    }

    pub(crate) async fn get_sandbox_snapshot_storage_uri(
        &self,
        request: Request<GetSandboxSnapshotStorageUriRequest>,
    ) -> Result<Response<GetSandboxSnapshotStorageUriResponse>, Status> {
        let node_ids = self.caller_node_ids(&request).await?;
        let req = request.into_inner();
        let sandbox_id = SandboxId::from_str(req.sandbox_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid sandbox_id: {e}")))?;

        // The store distinguishes "no sandbox placed on the caller" (`None`,
        // surfaced as `NotFound`) from "the organization has no override"
        // (`Some` with an empty URI), so callers can tell "wrong id" from
        // "no override; use env default".
        let storage =
            pplx_space_sandbox_store::snapshot_storage_placed_on(&self.pool, sandbox_id, &node_ids)
                .await
                .map_err(|e| Status::internal(format!("org snapshot uri lookup: {e}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))?;

        Ok(Response::new(GetSandboxSnapshotStorageUriResponse {
            storage_uri: storage.org_storage_uri,
        }))
    }

    pub(crate) async fn get_suspend_snapshot_location(
        &self,
        request: Request<GetSuspendSnapshotLocationRequest>,
    ) -> Result<Response<GetSuspendSnapshotLocationResponse>, Status> {
        let node_ids = self.caller_node_ids(&request).await?;
        let req = request.into_inner();
        let sandbox_id = SandboxId::from_str(req.sandbox_id.trim())
            .map_err(|e| Status::invalid_argument(format!("invalid sandbox_id: {e}")))?;

        let mut tx = self.begin_restore_lookup().await?;

        // TODO: [legacy] Remove once all restore callers look up exact artifact
        // backups with `GetArtifactBackupLocation`.
        let row = snapshot_store::latest_ready_suspend_location_placed_on(
            &mut *tx, sandbox_id, &node_ids,
        )
        .await
        .map_err(|e| Status::internal(format!("snapshot lookup: {e}")))?
        .ok_or_else(|| {
            Status::not_found(format!(
                "no suspend snapshot registered for sandbox {sandbox_id}"
            ))
        })?;

        let artifact_digests = Self::artifact_digests_for_restore(&mut tx, row.snapshot_id).await?;
        tx.commit()
            .await
            .map_err(|e| Status::internal(format!("finish snapshot lookup: {e}")))?;
        Ok(Response::new(GetSuspendSnapshotLocationResponse {
            snapshot_id: row.snapshot_id.to_string(),
            storage_uri: row.storage_connection_string,
            bytes_uploaded: row.size_bytes.unwrap_or(0),
            artifact_digests,
        }))
    }

    /// The registered nodes at the caller's remote address: the identity a
    /// sandbox-scoped read acts for, bound the way `validate_origin_remote_addr`
    /// binds the node-bound RPCs. The reads these nodes make are then limited
    /// to sandboxes placed on one of them, active there or as the target of a
    /// pending migration or restore. A host may register more than one node
    /// (a worker and a builder), so the identity is a set; an address with no
    /// registered node is an empty set and resolves nothing. A request with no
    /// remote address is refused.
    async fn caller_node_ids<T>(&self, request: &Request<T>) -> Result<Vec<NodeId>, Status> {
        let remote_ip = request
            .remote_addr()
            .map(|addr| normalize_ip(addr.ip()))
            .ok_or_else(|| {
                Status::permission_denied("node-bound cluster RPC has no remote address")
            })?;
        match self
            .template_releases_store
            .node_scope_for_address(remote_ip)
            .await
            .map_err(|err| Status::internal(format!("resolve caller nodes: {err}")))?
        {
            template_releases_store::TemplateReadScope::NodeScoped { node_ids } => Ok(node_ids),
            template_releases_store::TemplateReadScope::Unscoped => Ok(Vec::new()),
        }
    }

    // Catalog deletion cascades to digests before object cleanup. Both reads
    // must see the same snapshot so deletion cannot look like a legacy backup
    // without digests; READ COMMITTED would take a fresh snapshot per query.
    async fn begin_restore_lookup(
        &self,
    ) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, Status> {
        self.pool
            .begin_with(sql!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            .await
            .map_err(|e| Status::internal(format!("begin snapshot restore lookup: {e}")))
    }

    async fn artifact_digests_for_restore(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        snapshot_id: SnapshotId,
    ) -> Result<Vec<crate::proto::SnapshotArtifactDigest>, Status> {
        let digests = snapshot_store::artifact_digests(&mut **tx, snapshot_id)
            .await
            .map_err(|e| Status::internal(format!("artifact digest lookup: {e}")))?;
        Ok(digests
            .into_iter()
            .map(|digest| crate::proto::SnapshotArtifactDigest {
                artifact: digest.artifact.file_name().to_string(),
                digest: digest.digest.to_string(),
                size_bytes: digest.size_bytes,
            })
            .collect())
    }

    async fn fetch_nodes(
        &self,
        role: Option<SpaceNodeRole>,
        include_deleted: bool,
    ) -> Result<Vec<NodeRow>, Status> {
        sqlx::query_as::<_, NodeRow>(sql!(
            "SELECT n.id::TEXT AS id,
                    nd.name,
                    host(n.address) AS address,
                    nd.node_api_endpoint,
                    n.role,
                    n.managed_by,
                    n.state,
                    n.last_heartbeat,
                    n.created_at,
                    nd.updated_at,
                    nd.deleted_at,
                    nd.metadata::TEXT AS metadata_json
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE ($1::TEXT IS NULL OR n.role = $1)
               AND ($2::BOOL OR nd.deleted_at IS NULL)
               AND n.managed_by = $3
             ORDER BY n.role ASC, nd.name ASC NULLS LAST, nd.node_api_endpoint ASC"
        ))
        .bind(role.map(SpaceNodeRole::as_str))
        .bind(include_deleted)
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("query nodes: {err}")))
    }

    /// Resolve a caller-named node to its existing registration. Rows are
    /// created only by the cluster controller's Kubernetes node sync, sandbox
    /// pod placement and Direct admin registration; a name with no row is
    /// `NotFound`, never minted from the request.
    async fn resolve_origin_node_id(
        &self,
        value: &str,
        role_hint: Option<SpaceNodeRole>,
    ) -> Result<OriginNode, Status> {
        let value = trimmed_required(value, "origin_node_id")?;
        if let Ok(node_id) = value.parse::<NodeId>() {
            return registered_origin_node(&self.pool, &value, node_id, role_hint).await;
        }

        let Some(role_hint) = role_hint else {
            return Err(Status::invalid_argument(
                "node role is required when origin_node_id is a Kubernetes node name",
            ));
        };
        let role_hint_str = role_hint.as_str();
        let row = sqlx::query_as::<_, OriginNodeRow>(sql!(
            "SELECT n.id,
                    host(n.address) AS address,
                    n.role
             FROM nodes n
             JOIN node_k8s nk ON nk.node_id = n.id
             WHERE nk.k8s_node_name = $1
               AND n.managed_by = $3
               AND n.role = $2
               AND COALESCE(n.state, 'registered') != 'deleted'
             ORDER BY nk.updated_at DESC, n.created_at DESC
             LIMIT 1"
        ))
        .bind(&value)
        .bind(role_hint_str)
        .bind(SubstrateKind::Kubernetes)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| Status::internal(format!("resolve Kubernetes origin node: {err}")))?;
        let Some(row) = row else {
            return Err(Status::not_found(format!(
                "origin_node_id {value} is not registered"
            )));
        };
        let role = SpaceNodeRole::parse(&row.role).map_err(|error| {
            Status::internal(format!(
                "registered Kubernetes node {} has unknown role {}: {error}",
                value, row.role
            ))
        })?;
        Ok(OriginNode {
            id: row.id,
            display_id: value.to_string(),
            address: parse_origin_address(row.address.as_deref())?,
            role,
        })
    }

    async fn authenticated_origin_node_id<T>(
        &self,
        request: &Request<T>,
        value: &str,
        role_hint: Option<SpaceNodeRole>,
    ) -> Result<OriginNode, Status> {
        let origin = self.resolve_origin_node_id(value, role_hint).await?;
        validate_origin_remote_addr(&origin, request.remote_addr())?;
        Ok(origin)
    }

    async fn registered_backup_attempt(
        &self,
        snapshot_id: SnapshotId,
    ) -> Result<Option<snapshot_store::SnapshotRecord>, Status> {
        snapshot_store::get(&self.pool, snapshot_id)
            .await
            .map_err(|e| Status::internal(format!("snapshot lookup: {e}")))
    }

    /// Admit a backup RPC only from the attempt's source node: the caller's
    /// address must be that node's registered address, as for the RPCs that
    /// name a node. No source node fails closed. Returns the admitted node.
    async fn authorize_backup_source_node(
        &self,
        executor: impl sqlx::PgExecutor<'_>,
        source_node_id: Option<NodeId>,
        remote_addr: Option<std::net::SocketAddr>,
        subject: &str,
    ) -> Result<NodeId, Status> {
        let node_id = source_node_id.ok_or_else(|| {
            Status::failed_precondition(format!(
                "{subject} has no source node that may publish a backup for it"
            ))
        })?;
        let origin = registered_origin_node(executor, &node_id.to_string(), node_id, None).await?;
        validate_origin_remote_addr(&origin, remote_addr)?;
        Ok(node_id)
    }
}

/// Look up a registered node by ID on `executor`, so a caller holding a
/// transaction does not need a second pooled connection.
async fn registered_origin_node(
    executor: impl sqlx::PgExecutor<'_>,
    display_id: &str,
    node_id: NodeId,
    role_hint: Option<SpaceNodeRole>,
) -> Result<OriginNode, Status> {
    let row = sqlx::query_as::<_, OriginNodeRow>(sql!(
        "SELECT n.id,
                host(n.address) AS address,
                n.role
         FROM nodes n
         WHERE n.id = $1
           AND ($2::TEXT IS NULL OR n.role = $2)
           AND n.managed_by IN ($3, $4)
           AND COALESCE(n.state, 'registered') != 'deleted'
         LIMIT 1"
    ))
    .bind(node_id.as_uuid())
    .bind(role_hint.map(SpaceNodeRole::as_str))
    .bind(SubstrateKind::Space)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(executor)
    .await
    .map_err(|err| Status::internal(format!("resolve origin node: {err}")))?;
    let Some(row) = row else {
        return Err(Status::not_found(format!(
            "origin_node_id {display_id} is not registered"
        )));
    };
    let role = SpaceNodeRole::parse(&row.role).map_err(|error| {
        Status::internal(format!(
            "registered node {display_id} has unknown role {}: {error}",
            row.role
        ))
    })?;
    Ok(OriginNode {
        id: row.id,
        display_id: display_id.to_string(),
        address: parse_origin_address(row.address.as_deref())?,
        role,
    })
}

fn parse_origin_address(value: Option<&str>) -> Result<Option<IpAddr>, Status> {
    value
        .map(|value| {
            value.parse::<IpAddr>().map_err(|err| {
                Status::internal(format!(
                    "registered node address {value:?} is invalid: {err}"
                ))
            })
        })
        .transpose()
}

fn validate_origin_remote_addr(
    origin: &OriginNode,
    remote_addr: Option<std::net::SocketAddr>,
) -> Result<(), Status> {
    let remote_ip = remote_addr
        .map(|addr| normalize_ip(addr.ip()))
        .ok_or_else(|| Status::permission_denied("node-bound cluster RPC has no remote address"))?;
    let expected_ip = origin.address.map(normalize_ip).ok_or_else(|| {
        Status::failed_precondition(format!(
            "origin node {} has no registered address for node-bound authentication",
            origin.display_id
        ))
    })?;
    if remote_ip != expected_ip {
        return Err(Status::permission_denied(format!(
            "node-bound cluster RPC from {remote_ip} cannot act as {} at {expected_ip}",
            origin.display_id
        )));
    }
    Ok(())
}

fn normalize_ip(value: IpAddr) -> IpAddr {
    match value {
        IpAddr::V6(value) => value
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(value)),
        IpAddr::V4(value) => IpAddr::V4(value),
    }
}

mod adapter;
mod auto_lifecycle_delete_completions;
mod backup_generation_fence;
mod execution_observation;
mod metadata;
mod node_local_backup_pending;
mod publish;
mod status;
mod template_publication;
mod template_registration_store;
mod template_releases_store;
mod template_system_publication_grants;
mod template_tenant_publication_grants;
mod template_visibility_cache;
#[cfg(test)]
mod tests;

use metadata::*;
