//! The Direct substrate: [`DirectSubstrate`] dials each worker's node-api at
//! `node_direct.node_api_endpoint`, with no sandbox pods. The rules for
//! changing it are in this directory's `AGENTS.md`. Below is what it does
//! today: a selection, not a complete classification.
//!
//! # Operations
//!
//! - Create/delete, start, suspend/restore and wake go through
//!   [`SubstrateAdapter`]. Stop, kill, pause/resume and checkpoint go through
//!   the shared controller over Direct's runtime clients, which never recreate
//!   an execution.
//! - Warm-pool claims adopt node-side; custom vcpu/memory or encrypted claims
//!   go cold. Admission-time pool slot claims always miss.
//! - [`SubstrateError::Unsupported`]: node-pool placement, forks, and cluster
//!   pool configs. Custom vCPU or memory uses separate create and start calls:
//!   only `StartRequest` carries those fields. A readiness wait still blocks
//!   on start. Default shapes and disk-only overrides can use one-shot create,
//!   whose `CreateRequest` forwards disk size as `rootfs_size_mb`.
//! - No-ops returning `None` or nothing: `sandbox_ip`, `access_sandbox_ip`,
//!   `list_inventory_nodes` (Postgres is the inventory),
//!   `retire_fence_mismatch` and `observe_pod_identities`.
//!   `retire_execution` ignores its fence, but still stops the sandbox and
//!   clears its routing. It enqueues no stopped-disk backup (see below); the
//!   stopped disk stays on the node.
//! - Direct-only: the node registry and the template pull and delete routes in
//!   `routes/admin/node_maintenance.rs` (501 on Kubernetes).
//! - No drain or evacuation ([`SubstrateCapabilities::supports_node_drain`]
//!   is false). Admin `:drain`, `:terminate`, `:cordon`, `:uncordon` and
//!   `:force-upload-local-snapshots` return 501 before reading or writing the
//!   node. `:undrain` is the way out of a `draining` or `drained` phase an
//!   earlier drain left: it clears it without a substrate call.
//!
//! # Kubernetes-only reconcilers
//!
//! The cluster controller's `run_reconcilers` starts its drain, orphan,
//! failed-row repair and node-sync loops only for Kubernetes. The loops it
//! starts for both substrates include the public-operation dispatcher, which
//! retries transient failures of the create, lifecycle and delete operations
//! it executes. Direct reaches `retire_execution` only through the
//! transient-lifecycle sweeper's best-effort `cleanup_after_sweep`, and the
//! sweeper's failed-cleanup retry does nothing unless the running substrate is
//! Kubernetes, so nothing retries a failed Direct retire. That is also why
//! Direct's retire enqueues no stopped-disk backup: the Kubernetes teardown
//! fails its retire when a retryable enqueue fails and relies on that retry,
//! and even a best-effort enqueue is not a cheap side call, since sbx-mgr
//! refuses it with `FailedPrecondition` on a node without cluster-api wiring
//! and otherwise takes a btrfs snapshot of the stopped sandbox.

use pplx_space_common_core::sql;
use pplx_space_common_svc::sql::contention;
use std::fmt;
use std::time::Duration;

use pplx_space_common_core::types::{
    ArtifactOrigin, AutoLifecyclePolicy, ExecutionGeneration, ExecutionGenerationParseError,
    NodeId, NodeManagedBy, NodeRole as SpaceNodeRole, PodUid, PodUidParseError, PolicyAuthority,
    RouteGeneration, RouteGenerationParseError, SandboxActivationAttemptId, SandboxId,
    SandboxLifecycle, SandboxRouteIdentity, SandboxRouteIdentityInvariantError, SubstrateKind,
};
use pplx_space_net_common::{NetworkPolicyV1, proto::NetworkPolicyV1 as NetworkPolicyProto};
use pplx_space_sandbox_portability::{
    no_newer_full_snapshot, restore_retry_source_snapshot_is_current,
};
use pplx_space_sbx_mgr_client::proto::{
    BootMode, StartRequest, SuspendRequest, WakeResponse, WakeTransition,
};
use sqlx::{FromRow, PgPool};
use tonic::{Code, transport::Endpoint};
use tracing::Instrument;
use uuid::Uuid;

use super::{
    ActivationCompletion, ActivationOptions, BuilderClient, CreateBootMode, CreateExecutionOutcome,
    CreateExecutionRequest, DbContext, ExecutionActivation, ExecutionActivityKind,
    ExecutionBinding, NodeApiTarget, NodeInventoryEntry, OwnedNodeApiTarget, PooledExecutionClaim,
    PooledExecutionClaimAttempt, PooledExecutionClaimRequest, PooledExecutionPoolShape,
    RetireExecutionOutcome, RetireExecutionRequest, RuntimeClient, SandboxResources,
    SubstrateAdapter, SubstrateCapabilities, SubstrateError, connect_builder, connect_worker,
    node_api_endpoint, substrate_db_error,
};
use crate::controllers::auto_lifecycle::policy_to_proto;
use crate::controllers::{ControllerError, db_contention_error};
use crate::models::Sandbox;

fn network_policy_to_proto(policy: &NetworkPolicyV1) -> NetworkPolicyProto {
    NetworkPolicyProto::from(policy)
}

use super::override_request_policy;

#[derive(Debug, Clone, FromRow)]
struct DirectNode {
    id: NodeId,
    node_api_endpoint: String,
}

#[derive(Debug, Clone, FromRow)]
struct DirectPoolCandidate {
    id: NodeId,
    node_api_endpoint: String,
    content_hash: String,
    vcpu: Option<i32>,
    memory_mib: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectRestoreCandidateKind {
    SourceNode,
    RemoteSnapshot,
}

impl DirectRestoreCandidateKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceNode => "source_node",
            Self::RemoteSnapshot => "remote_snapshot",
        }
    }
}

#[derive(Debug, Clone)]
struct DirectRestoreCandidate {
    node: DirectNode,
    storage_uri: Option<String>,
    source_node_id: Option<NodeId>,
    kind: DirectRestoreCandidateKind,
}

struct DirectRestoreResult {
    endpoint: Endpoint,
    execution_generation: u64,
}

#[derive(Debug, Clone, FromRow)]
struct DirectRestoreCandidateRow {
    id: NodeId,
    node_api_endpoint: String,
    storage_uri: Option<String>,
    source_node_id: Option<NodeId>,
}

impl DirectRestoreCandidate {
    fn source_node(row: DirectRestoreCandidateRow) -> Self {
        Self {
            node: DirectNode {
                id: row.id,
                node_api_endpoint: row.node_api_endpoint,
            },
            storage_uri: None,
            source_node_id: row.source_node_id,
            kind: DirectRestoreCandidateKind::SourceNode,
        }
    }

    fn remote_snapshot(row: DirectRestoreCandidateRow) -> Self {
        Self {
            node: DirectNode {
                id: row.id,
                node_api_endpoint: row.node_api_endpoint,
            },
            storage_uri: row.storage_uri,
            source_node_id: row.source_node_id,
            kind: DirectRestoreCandidateKind::RemoteSnapshot,
        }
    }
}

#[derive(Clone)]
pub struct DirectSubstrate {
    pool: PgPool,
    worker_channels: std::sync::Arc<super::worker_channels::WorkerChannels>,
}

const DIRECT_NODE_CREATE_FAILURE_TTL_SECS: i64 = 10;
const DIRECT_NODE_RPC_TIMEOUT: Duration = crate::controllers::substrate::NODE_RPC_TIMEOUT;
const DIRECT_NODE_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DIRECT_PULL_ON_CREATE_RPC_TIMEOUT: Duration = Duration::from_secs(600);
const DIRECT_REMOTE_RESTORE_CANDIDATE_LIMIT: i64 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectRestoreLease {
    token: Uuid,
    restore_retry: bool,
}

fn direct_delete_error(sandbox_id: SandboxId, error: tonic::Status) -> SubstrateError {
    if pplx_space_sbx_mgr_client::is_retained_filesystem_only_copy_status(&error) {
        SubstrateError::RetainedFilesystemOnlyCopy {
            message: format!(
                "sandbox {sandbox_id} node-local delete refused because the retained filesystem may be the only copy: {error}"
            ),
        }
    } else if pplx_space_sbx_mgr_client::is_suspend_publication_pending_status(&error) {
        SubstrateError::SuspendInFlight {
            message: format!(
                "sandbox {sandbox_id} direct node-local delete deferred while suspend publication is pending: {error}"
            ),
        }
    } else {
        SubstrateError::Failed {
            message: format!("node-api delete error: {error}"),
        }
    }
}

type WorkerCreateSelection = (
    DirectNode,
    Endpoint,
    RuntimeClient,
    pplx_space_sbx_mgr_client::proto::CreateResponse,
);

fn direct_create_boot_mode(boot_mode: CreateBootMode) -> Result<i32, SubstrateError> {
    match boot_mode {
        // API-level pool creation owns pool adoption. If that path misses, the
        // normal direct create path may still fall through from base-snapshot
        // to cold, but it must not adopt a different node-local pool member.
        CreateBootMode::Fallthrough | CreateBootMode::BaseSnapshot => {
            Ok(BootMode::BaseSnapshot.into())
        }
        CreateBootMode::Cold => Ok(BootMode::Cold.into()),
        CreateBootMode::Pool => Err(SubstrateError::Unavailable {
            message: "boot_mode=pool requested but no direct warm-pool sandbox is available"
                .to_string(),
        }),
    }
}

fn use_one_shot_create(
    wait_until_ready: bool,
    boot_mode: CreateBootMode,
    resources: &SandboxResources,
) -> bool {
    // CreateRequest cannot carry CPU or memory; StartRequest can.
    resources.vcpu.is_none()
        && resources.memory_mib.is_none()
        && wait_until_ready
        && matches!(
            boot_mode,
            CreateBootMode::BaseSnapshot | CreateBootMode::Cold
        )
}

fn pull_on_create_allowed(content_hash: Option<&str>, boot_mode: CreateBootMode) -> bool {
    content_hash.is_some()
        && !matches!(
            boot_mode,
            CreateBootMode::BaseSnapshot | CreateBootMode::Pool
        )
}

fn is_idempotent_pool_rollback_delete(error: &tonic::Status) -> bool {
    error.code() == Code::NotFound
        || (error.code() == Code::FailedPrecondition
            && error.message().to_ascii_lowercase().contains("deleting"))
}

fn should_try_next_worker_candidate(error: &tonic::Status) -> bool {
    matches!(
        error.code(),
        Code::ResourceExhausted
            | Code::NotFound
            | Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Aborted
            | Code::Internal
            | Code::Unknown
    )
}

fn should_try_next_restore_candidate(error: &tonic::Status) -> bool {
    if pplx_space_credmgr_client::status_is_invalid_key_reference(error) {
        return false;
    }
    matches!(
        error.code(),
        Code::FailedPrecondition
            | Code::Unavailable
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Internal
    )
}

fn direct_route_generation_reservation_error(error: sqlx::Error) -> SubstrateError {
    // Retryable but not a lock race, so these three keep the unmarked
    // `Unavailable` rather than joining the contention set: `57014` counts
    // because this reservation is a short CAS, so a statement-timeout
    // cancellation means the row was contended rather than the statement being
    // inherently slow, and a transport error or a drained pool is worth one
    // more attempt on a fresh connection. None of the three should reach the
    // client as `LockContention`, whose one-second `Retry-After` would answer
    // pool exhaustion by synchronising the stalled callers' return
    // (see `crate::controllers::db_contention_error`).
    if matches!(error, sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut)
        || contention::is_query_canceled(&error)
    {
        return SubstrateError::Unavailable {
            message: format!("failed to reserve Direct route identity: {error}"),
        };
    }
    // The contention SQLSTATEs, and every other `sqlx` error, from here.
    substrate_db_error("failed to reserve Direct route identity", &error)
}

fn restore_status_error(status: &tonic::Status) -> SubstrateError {
    let message = format!("node-api restore error: {status}");
    if pplx_space_credmgr_client::status_is_invalid_key_reference(status) {
        SubstrateError::FailedPrecondition { message }
    } else if status.code() == Code::Aborted {
        SubstrateError::Unavailable { message }
    } else {
        SubstrateError::Failed { message }
    }
}

fn split_restore_attempt_timeout(total: Duration, candidate_count: usize) -> Duration {
    if candidate_count <= 1 {
        return total;
    }
    let nanos = total
        .as_nanos()
        .saturating_div(candidate_count as u128)
        .max(1);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn timeout_secs_ceil(timeout: Duration) -> u32 {
    let seconds = timeout
        .as_secs()
        .saturating_add(u64::from(timeout.subsec_nanos() > 0))
        .max(1);
    u32::try_from(seconds).unwrap_or(u32::MAX)
}

#[derive(Debug, thiserror::Error)]
enum DirectRouteIdentityReceiptError {
    #[error(
        "node-api returned invalid direct route identity: sandbox_id does not match the requested sandbox"
    )]
    SandboxBinding,
    #[error("node-api returned invalid direct route identity: missing route_generation")]
    MissingRouteGeneration,
    #[error("node-api returned invalid direct route identity: {0}")]
    InvalidRouteGeneration(#[from] RouteGenerationParseError),
    #[error("node-api returned invalid direct route identity: missing current_node_id")]
    MissingCurrentNodeId,
    #[error("node-api returned invalid direct route identity: invalid current_node_id: {0}")]
    InvalidCurrentNodeId(#[source] pplx_space_common_core::types::AuthIdParseError),
    #[error("node-api returned invalid direct route identity: missing execution_generation")]
    MissingExecutionGeneration,
    #[error("node-api returned invalid direct route identity: {0}")]
    InvalidExecutionGeneration(#[from] ExecutionGenerationParseError),
    #[error("node-api returned invalid direct route identity: missing activation_attempt_id")]
    MissingActivationAttemptId,
    #[error("node-api returned invalid direct route identity: invalid activation_attempt_id: {0}")]
    InvalidActivationAttemptId(#[source] uuid::Error),
    #[error("node-api returned invalid direct route identity: unknown substrate_kind {0}")]
    UnknownSubstrateKind(i32),
    #[error("node-api returned invalid direct route identity: expected direct substrate, got {0}")]
    WrongSubstrateKind(&'static str),
    #[error("node-api returned invalid direct route identity: invalid pod_uid: {0}")]
    InvalidPodUid(#[from] PodUidParseError),
    #[error("node-api returned invalid direct route identity: {0}")]
    InvalidIdentity(#[from] SandboxRouteIdentityInvariantError),
}

fn parse_direct_route_identity(
    sandbox_id: SandboxId,
    identity: pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
) -> Result<SandboxRouteIdentity, DirectRouteIdentityReceiptError> {
    if identity
        .sandbox_id
        .as_deref()
        .is_some_and(|bound| bound.parse::<SandboxId>().ok() != Some(sandbox_id))
    {
        return Err(DirectRouteIdentityReceiptError::SandboxBinding);
    }
    let route_generation = identity
        .route_generation
        .ok_or(DirectRouteIdentityReceiptError::MissingRouteGeneration)?
        .try_into()?;
    let current_node_id = identity
        .current_node_id
        .as_deref()
        .ok_or(DirectRouteIdentityReceiptError::MissingCurrentNodeId)?
        .parse::<NodeId>()
        .map_err(DirectRouteIdentityReceiptError::InvalidCurrentNodeId)?;
    let execution_generation = identity
        .execution_generation
        .ok_or(DirectRouteIdentityReceiptError::MissingExecutionGeneration)?
        .try_into()?;
    let activation_attempt_id = identity
        .activation_attempt_id
        .as_deref()
        .ok_or(DirectRouteIdentityReceiptError::MissingActivationAttemptId)?
        .parse::<SandboxActivationAttemptId>()
        .map_err(DirectRouteIdentityReceiptError::InvalidActivationAttemptId)?;
    let substrate_kind =
        pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::try_from(identity.substrate_kind)
            .map_err(|_| {
            DirectRouteIdentityReceiptError::UnknownSubstrateKind(identity.substrate_kind)
        })?;
    if substrate_kind != pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct {
        return Err(DirectRouteIdentityReceiptError::WrongSubstrateKind(
            substrate_kind.as_str_name(),
        ));
    }
    let pod_uid = identity.pod_uid.as_deref().map(PodUid::parse).transpose()?;
    Ok(SandboxRouteIdentity::try_new(
        sandbox_id,
        route_generation,
        current_node_id,
        NodeManagedBy::Space,
        execution_generation,
        activation_attempt_id,
        pod_uid,
    )?)
}

/// The sbx-mgr substrate of a stored route identity's `substrate_kind` text,
/// or `None` for text that names no substrate.
fn route_identity_substrate_kind(
    substrate_kind: &str,
) -> Option<pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind> {
    match substrate_kind.parse::<SubstrateKind>() {
        Ok(SubstrateKind::Space) => {
            Some(pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct)
        }
        Ok(SubstrateKind::Kubernetes) => {
            Some(pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes)
        }
        Err(_) => None,
    }
}

const LEGACY_DIRECT_EXECUTION_GENERATION: u64 = 1;

fn receipt_execution_generation(identity: Option<&SandboxRouteIdentity>) -> u64 {
    identity
        .and_then(|identity| u64::try_from(identity.execution_generation().get()).ok())
        .unwrap_or(LEGACY_DIRECT_EXECUTION_GENERATION)
}

fn direct_restore_start_request(
    sandbox_id: SandboxId,
    vcpu: Option<u32>,
    blocking: bool,
    ready_timeout: Duration,
    candidate: &DirectRestoreCandidate,
    route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
) -> StartRequest {
    let mut request = pplx_space_sbx_mgr_client::direct_restore_start_request(
        sandbox_id.to_string(),
        blocking,
        Some(timeout_secs_ceil(ready_timeout)),
        candidate.storage_uri.clone(),
        candidate
            .storage_uri
            .as_ref()
            .and_then(|_| candidate.source_node_id.map(|node_id| node_id.to_string())),
    );
    request.route_identity = route_identity;
    request.vcpu = vcpu;
    request
}

fn direct_wake_request(
    sandbox_id: SandboxId,
    vcpu: Option<u32>,
    route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
    wake_activity: Option<ExecutionActivityKind>,
) -> pplx_space_sbx_mgr_client::proto::WakeRequest {
    pplx_space_sbx_mgr_client::proto::WakeRequest {
        implicit_wake_source: wake_activity.map_or(0, ExecutionActivityKind::wake_source),
        sandbox_id: sandbox_id.to_string(),
        blocking: true,
        start_context: (route_identity.is_some() || vcpu.is_some()).then(|| {
            pplx_space_sbx_mgr_client::proto::WakeStartContext {
                route_identity,
                vcpu,
                ..Default::default()
            }
        }),
        target_state: 0,
    }
}

/// Whether waking `resident` restores or starts it, which sandbox-manager
/// admits only with a reserved route identity.
fn resident_is_dormant(resident: &pplx_space_sbx_mgr_client::proto::SandboxInfo) -> bool {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    matches!(
        resident
            .lifecycle
            .and_then(|state| SandboxLifecycleState::try_from(state).ok()),
        Some(
            SandboxLifecycleState::Suspended
                | SandboxLifecycleState::Stopped
                | SandboxLifecycleState::Created
        )
    )
}

fn wake_requires_restore(status: &tonic::Status) -> bool {
    status.code() == Code::NotFound
}

fn wake_lifecycle_allows_restore_fallback(lifecycle: &str) -> bool {
    matches!(
        lifecycle.parse::<SandboxLifecycle>(),
        Ok(SandboxLifecycle::Suspended | SandboxLifecycle::Restoring)
    )
}

fn wake_response_requires_activation_identity(response: &WakeResponse) -> bool {
    response.route_identity.is_none()
        && matches!(
            WakeTransition::try_from(response.transition),
            Ok(WakeTransition::Restore | WakeTransition::Start)
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectRouteIdentityOperation {
    Create,
    Rollback,
    Start,
    Resume,
    Wake,
}

async fn upsert_direct_route_provenance(
    connection: &mut sqlx::PgConnection,
    sandbox_id: SandboxId,
    route_generation: RouteGeneration,
    execution_generation: ExecutionGeneration,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance (
             sandbox_id,
             last_route_generation,
             last_execution_generation,
             updated_at
         )
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (sandbox_id) DO UPDATE
         SET last_route_generation = GREATEST(
                 sandbox_direct_route_provenance.last_route_generation,
                 EXCLUDED.last_route_generation
             ),
             last_execution_generation = GREATEST(
                 sandbox_direct_route_provenance.last_execution_generation,
                 EXCLUDED.last_execution_generation
             ),
             updated_at = NOW()"
    ))
    .bind(sandbox_id)
    .bind(route_generation)
    .bind(execution_generation)
    .execute(connection)
    .await?;
    Ok(())
}

async fn persist_monotonic_direct_route_identity(
    connection: &mut sqlx::PgConnection,
    identity: &SandboxRouteIdentity,
) -> Result<bool, sqlx::Error> {
    let persisted = sqlx::query_scalar::<_, i64>(sql!(
        "INSERT INTO sandbox_direct_route_identities (
             sandbox_id, route_generation, current_node_id, substrate_kind,
             execution_generation, activation_attempt_id, pod_uid, updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
         ON CONFLICT (sandbox_id) DO UPDATE
         SET route_generation = EXCLUDED.route_generation,
             current_node_id = EXCLUDED.current_node_id,
             substrate_kind = EXCLUDED.substrate_kind,
             execution_generation = EXCLUDED.execution_generation,
             activation_attempt_id = EXCLUDED.activation_attempt_id,
             pod_uid = EXCLUDED.pod_uid,
             updated_at = NOW()
         WHERE sandbox_direct_route_identities.route_generation <= EXCLUDED.route_generation
           AND sandbox_direct_route_identities.execution_generation <= EXCLUDED.execution_generation
         RETURNING execution_generation"
    ))
    .bind(identity.sandbox_id().as_uuid())
    .bind(identity.route_generation())
    .bind(identity.current_node_id().as_uuid())
    .bind(identity.substrate_kind().as_str())
    .bind(identity.execution_generation())
    .bind(identity.activation_attempt_id().as_uuid())
    .bind(identity.pod_uid().map(ToString::to_string))
    .fetch_optional(&mut *connection)
    .await?;
    if persisted.is_none() {
        return Ok(false);
    }
    upsert_direct_route_provenance(
        connection,
        identity.sandbox_id(),
        identity.route_generation(),
        identity.execution_generation(),
    )
    .await?;
    Ok(true)
}

impl DirectRouteIdentityOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Rollback => "rollback",
            Self::Start => "start",
            Self::Resume => "resume",
            Self::Wake => "wake",
        }
    }
}

impl fmt::Display for DirectRouteIdentityOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerCandidateSource {
    CachedTemplate,
    PullOnCreate,
}

impl WorkerCandidateSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::CachedTemplate => "cached_template",
            Self::PullOnCreate => "pull_on_create",
        }
    }

    fn request_timeout(self) -> Duration {
        match self {
            Self::CachedTemplate => DIRECT_NODE_RPC_TIMEOUT,
            Self::PullOnCreate => DIRECT_PULL_ON_CREATE_RPC_TIMEOUT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CachedWorkerFailure {
    Full,
    Draining,
    Unavailable,
    Degraded,
}

impl CachedWorkerFailure {
    fn scheduling_status(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Draining => "draining",
            Self::Unavailable => "unavailable",
            Self::Degraded => "degraded",
        }
    }

    fn reachable(self) -> bool {
        !matches!(self, Self::Unavailable)
    }
}

fn cached_worker_failure_from_create_status(status: &tonic::Status) -> Option<CachedWorkerFailure> {
    if pplx_space_credmgr_client::status_is_invalid_key_reference(status)
        || pplx_space_sbx_mgr_client::is_template_policy_dependency(status)
    {
        return None;
    }
    match status.code() {
        Code::ResourceExhausted => {
            if status.message().to_ascii_lowercase().contains("drain") {
                Some(CachedWorkerFailure::Draining)
            } else {
                Some(CachedWorkerFailure::Full)
            }
        }
        Code::Unavailable | Code::DeadlineExceeded => Some(CachedWorkerFailure::Unavailable),
        Code::FailedPrecondition | Code::Internal => Some(CachedWorkerFailure::Degraded),
        _ => None,
    }
}

fn direct_status_error(status: tonic::Status, message: String) -> SubstrateError {
    if pplx_space_credmgr_client::status_is_invalid_key_reference(&status) {
        return SubstrateError::FailedPrecondition { message };
    }
    SubstrateError::Failed { message }
}

fn direct_create_status_error(status: tonic::Status) -> SubstrateError {
    let message = format!("node-api create error: {status}");
    direct_status_error(status, message)
}

fn direct_create_start_status_error(status: tonic::Status) -> SubstrateError {
    let message = format!("node-api start error: {status}");
    direct_status_error(status, message)
}

impl DirectSubstrate {
    async fn activation_auto_lifecycle(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy, SubstrateError> {
        pplx_space_sandbox_store::auto_lifecycle_policy(&self.pool, sandbox_id)
            .await
            .map_err(sqlx::Error::from)
            .db_context("failed to reload activation auto lifecycle policy")?
            .map(Into::into)
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            })
    }

    async fn activation_vcpu(&self, sandbox_id: SandboxId) -> Result<Option<u32>, SubstrateError> {
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&self.pool, sandbox_id)
            .await
            .db_context("failed to reload activation CPU allocation")?
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            })?;
        Ok(sandbox.vcpu.and_then(|value| u32::try_from(value).ok()))
    }

    async fn wake_request_with_policy(
        &self,
        sandbox: &Sandbox,
        route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
        expected_execution_generation: Option<i64>,
        activity: Option<ExecutionActivityKind>,
    ) -> Result<pplx_space_sbx_mgr_client::proto::WakeRequest, SubstrateError> {
        let sandbox_id = sandbox.id;
        let vcpu = sandbox.vcpu.and_then(|value| u32::try_from(value).ok());
        let mut request = direct_wake_request(sandbox_id, vcpu, route_identity, activity);
        let context = request.start_context.get_or_insert_default();
        context.activation_auto_lifecycle = Some(self.activation_auto_lifecycle(sandbox_id).await?);
        context.expected_execution_generation = expected_execution_generation;
        Ok(request)
    }

    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            worker_channels: std::sync::Arc::new(super::worker_channels::WorkerChannels::new(
                DIRECT_NODE_CONNECT_TIMEOUT,
                DIRECT_NODE_RPC_TIMEOUT,
            )),
        }
    }

    async fn persist_route_identity_receipt(
        &self,
        sandbox_id: SandboxId,
        expected_node_id: NodeId,
        response_sandbox_id: &str,
        route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
        operation: DirectRouteIdentityOperation,
    ) -> Result<Option<SandboxRouteIdentity>, SubstrateError> {
        if response_sandbox_id != sandbox_id.to_string() {
            return self
                .clear_direct_route_identity_then_fail(
                    sandbox_id,
                    SubstrateError::Failed {
                message: format!(
                    "node-api {operation} returned sandbox {response_sandbox_id}, expected {sandbox_id}"
                ),
                    },
                )
                .await;
        }
        let Some(identity) = route_identity else {
            // A successful activation through an older sandbox-manager must
            // not leave a strict receipt from the previous incarnation live.
            self.clear_direct_route_identity(sandbox_id).await?;
            return Ok(None);
        };
        let identity = match parse_direct_route_identity(sandbox_id, identity) {
            Ok(identity) => identity,
            Err(error) => {
                return self
                    .clear_direct_route_identity_then_fail(
                        sandbox_id,
                        SubstrateError::Failed {
                            message: error.to_string(),
                        },
                    )
                    .await;
            }
        };
        if identity.current_node_id() != expected_node_id {
            return self
                .clear_direct_route_identity_then_fail(
                    sandbox_id,
                    SubstrateError::Failed {
                        message: format!(
                            "node-api {operation} route identity targets node {}, expected {expected_node_id}",
                            identity.current_node_id()
                        ),
                    },
                )
                .await;
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .db_context("failed to begin direct route identity transaction")?;
        let persisted = sqlx::query_scalar::<_, i64>(sql!(
            "INSERT INTO sandbox_direct_route_identities (
                 sandbox_id,
                 route_generation,
                 current_node_id,
                 substrate_kind,
                 execution_generation,
                 activation_attempt_id,
                 pod_uid,
                 updated_at
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
             ON CONFLICT (sandbox_id) DO UPDATE
             SET route_generation = EXCLUDED.route_generation,
                 current_node_id = EXCLUDED.current_node_id,
                 substrate_kind = EXCLUDED.substrate_kind,
                 execution_generation = EXCLUDED.execution_generation,
                 activation_attempt_id = EXCLUDED.activation_attempt_id,
                 pod_uid = EXCLUDED.pod_uid,
                 updated_at = NOW()
             WHERE (
                 sandbox_direct_route_identities.current_node_id = EXCLUDED.current_node_id
                 AND sandbox_direct_route_identities.route_generation
                     <= EXCLUDED.route_generation
                 AND (
                     sandbox_direct_route_identities.execution_generation
                         < EXCLUDED.execution_generation
                     OR (
                         sandbox_direct_route_identities.execution_generation
                             = EXCLUDED.execution_generation
                         AND sandbox_direct_route_identities.activation_attempt_id
                             <= EXCLUDED.activation_attempt_id
                     )
                 )
             )
             OR (
                 sandbox_direct_route_identities.current_node_id <> EXCLUDED.current_node_id
                 AND EXISTS (
                     SELECT 1
                     FROM sandboxes s
                     WHERE s.id = EXCLUDED.sandbox_id
                       AND s.active_node_id = EXCLUDED.current_node_id
                 )
             )
             RETURNING execution_generation"
        ))
        .bind(identity.sandbox_id().as_uuid())
        .bind(identity.route_generation())
        .bind(identity.current_node_id().as_uuid())
        .bind(identity.substrate_kind().as_str())
        .bind(identity.execution_generation())
        .bind(identity.activation_attempt_id().as_uuid())
        .bind(identity.pod_uid().map(ToString::to_string))
        .fetch_optional(&mut *transaction)
        .await
        .db_context("failed to persist direct route identity")?;
        if persisted.is_none() {
            return Err(SubstrateError::Failed {
                message: format!(
                    "refused stale direct route identity receipt for sandbox {sandbox_id}"
                ),
            });
        }
        upsert_direct_route_provenance(
            &mut transaction,
            identity.sandbox_id(),
            identity.route_generation(),
            identity.execution_generation(),
        )
        .await
        .db_context("failed to retain direct route provenance")?;
        transaction
            .commit()
            .await
            .db_context("failed to commit direct route identity")?;
        Ok(Some(identity))
    }

    async fn persist_owned_restore_route_identity_receipt(
        &self,
        sandbox_id: SandboxId,
        expected_node_id: NodeId,
        response: &pplx_space_sbx_mgr_client::proto::StartResponse,
        supplied_identity: pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity,
        lease: DirectRestoreLease,
    ) -> Result<Option<SandboxRouteIdentity>, SubstrateError> {
        // During a rolling deploy, an older sandbox-manager can apply the
        // supplied identity without returning a receipt. The API still owns
        // the exact identity it sent and can persist it under the restore
        // lease; a returned receipt remains authoritative when present.
        let identity = response.route_identity.clone().unwrap_or(supplied_identity);
        let identity = parse_direct_route_identity(sandbox_id, identity).map_err(|error| {
            SubstrateError::Failed {
                message: error.to_string(),
            }
        })?;
        if response.sandbox_id != sandbox_id.to_string() {
            return Err(SubstrateError::Failed {
                message: format!(
                    "node-api restore returned sandbox {}, expected {sandbox_id}",
                    response.sandbox_id
                ),
            });
        }
        if identity.current_node_id() != expected_node_id {
            return Err(SubstrateError::Failed {
                message: format!(
                    "node-api restore route identity targets node {}, expected {expected_node_id}",
                    identity.current_node_id()
                ),
            });
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .db_context("failed to begin owned direct restore transaction")?;
        let owns_restore = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT TRUE
               FROM sandboxes
              WHERE id = $1
                AND restore_lease_token = $2
              FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(lease.token)
        .fetch_optional(&mut *transaction)
        .await
        .db_context("failed to validate direct restore ownership")?;
        if owns_restore.is_none() {
            return Err(SubstrateError::restore_in_flight(format!(
                "direct restore ownership changed for sandbox {sandbox_id}"
            )));
        }
        let persisted = persist_monotonic_direct_route_identity(&mut transaction, &identity)
            .await
            .db_context("failed to persist owned direct restore receipt")?;
        if !persisted {
            return Err(SubstrateError::Failed {
                message: format!("refused stale direct restore receipt for sandbox {sandbox_id}"),
            });
        }
        let placed = sqlx::query(sql!(
            "UPDATE sandboxes
                SET active_node_id = $2,
                    target_node_id = $2,
                    placement = 'on_node',
                    placement_changed_at = NOW()
              WHERE id = $1
                AND restore_lease_token = $3"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(expected_node_id.as_uuid())
        .bind(lease.token)
        .execute(&mut *transaction)
        .await
        .db_context("failed to persist owned direct restore placement")?;
        if placed.rows_affected() != 1 {
            return Err(SubstrateError::restore_in_flight(format!(
                "direct restore ownership changed for sandbox {sandbox_id}"
            )));
        }
        transaction
            .commit()
            .await
            .db_context("failed to commit owned direct restore receipt")?;
        Ok(Some(identity))
    }

    async fn persist_start_route_identity(
        &self,
        sandbox_id: SandboxId,
        expected_node_id: NodeId,
        response: &pplx_space_sbx_mgr_client::proto::StartResponse,
    ) -> Result<Option<SandboxRouteIdentity>, SubstrateError> {
        self.persist_route_identity_receipt(
            sandbox_id,
            expected_node_id,
            &response.sandbox_id,
            response.route_identity.clone(),
            DirectRouteIdentityOperation::Start,
        )
        .await
    }

    async fn load_active_direct_route_identity(
        &self,
        sandbox_id: SandboxId,
        expected_node_id: NodeId,
    ) -> Result<Option<SandboxRouteIdentity>, SubstrateError> {
        let row: Option<(i64, Uuid, String, i64, Uuid, Option<String>)> = sqlx::query_as(sql!(
            "SELECT route_generation,
                        current_node_id,
                        substrate_kind,
                        execution_generation,
                        activation_attempt_id,
                        pod_uid
                   FROM sandbox_direct_route_identities
                  WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .db_context_with(|| {
            format!("failed to load current Direct route identity for sandbox {sandbox_id}")
        })?;
        let Some((
            route_generation,
            current_node_id,
            substrate_kind,
            execution_generation,
            activation_attempt_id,
            pod_uid,
        )) = row
        else {
            return Ok(None);
        };
        if current_node_id != expected_node_id.as_uuid() {
            return Err(SubstrateError::Failed {
                message: format!(
                    "current Direct route identity for sandbox {sandbox_id} targets node {current_node_id}, expected {expected_node_id}"
                ),
            });
        }
        let Some(substrate_kind) = route_identity_substrate_kind(&substrate_kind) else {
            return Err(SubstrateError::Failed {
                message: format!(
                    "current Direct route identity for sandbox {sandbox_id} has invalid substrate {substrate_kind:?}"
                ),
            });
        };
        let route_generation = u64::try_from(route_generation).map_err(|_| {
            SubstrateError::Failed {
                message: format!(
                    "current Direct route identity for sandbox {sandbox_id} has invalid route generation {route_generation}"
                ),
            }
        })?;
        parse_direct_route_identity(
            sandbox_id,
            pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
                sandbox_id: None,
                route_generation: Some(route_generation),
                current_node_id: Some(current_node_id.to_string()),
                substrate_kind: substrate_kind as i32,
                execution_generation: Some(execution_generation),
                activation_attempt_id: Some(activation_attempt_id.to_string()),
                pod_uid,
            },
        )
        .map(Some)
        .map_err(|error| SubstrateError::Failed {
            message: error.to_string(),
        })
    }

    async fn persist_wake_route_identity(
        &self,
        sandbox_id: SandboxId,
        expected_node_id: NodeId,
        response: &WakeResponse,
    ) -> Result<Option<SandboxRouteIdentity>, SubstrateError> {
        let transition =
            WakeTransition::try_from(response.transition).map_err(|_| SubstrateError::Failed {
                message: format!(
                    "node-api wake returned unknown transition {}",
                    response.transition
                ),
            })?;
        match (transition, response.route_identity.clone()) {
            (WakeTransition::Unspecified, _) => Err(SubstrateError::Failed {
                message: "node-api wake omitted its transition".to_string(),
            }),
            (_, Some(identity)) => {
                self.persist_route_identity_receipt(
                    sandbox_id,
                    expected_node_id,
                    &response.sandbox_id,
                    Some(identity),
                    DirectRouteIdentityOperation::Wake,
                )
                .await
            }
            (WakeTransition::Restore | WakeTransition::Start, None) => {
                self.persist_route_identity_receipt(
                    sandbox_id,
                    expected_node_id,
                    &response.sandbox_id,
                    None,
                    DirectRouteIdentityOperation::Wake,
                )
                .await
            }
            (WakeTransition::Noop | WakeTransition::Resume | WakeTransition::Wait, None) => {
                if response.sandbox_id != sandbox_id.to_string() {
                    return Err(SubstrateError::Failed {
                        message: format!(
                            "node-api wake returned sandbox {}, expected {sandbox_id}",
                            response.sandbox_id
                        ),
                    });
                }
                self.load_active_direct_route_identity(sandbox_id, expected_node_id)
                    .await
            }
        }
    }

    async fn reserve_direct_activation_route_identity(
        &self,
        sandbox_id: SandboxId,
        target_node_id: NodeId,
    ) -> Result<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity, SubstrateError> {
        self.recover_direct_route_provenance(sandbox_id).await?;
        let reserved = self
            .advance_direct_activation_route_generation(sandbox_id)
            .await?;
        let (route_generation, execution_generation) =
            reserved.ok_or_else(|| SubstrateError::Failed {
                message: format!("sandbox {sandbox_id} has no recoverable direct route provenance"),
            })?;
        let route_generation =
            u64::try_from(route_generation).map_err(|_| SubstrateError::Failed {
                message: "direct restore route generation is out of range".to_string(),
            })?;
        Ok(pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity {
            sandbox_id: None,
            route_generation: Some(route_generation),
            current_node_id: Some(target_node_id.to_string()),
            substrate_kind: pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Direct as i32,
            execution_generation: Some(execution_generation),
            activation_attempt_id: Some(SandboxActivationAttemptId::new().to_string()),
            pod_uid: None,
        })
    }

    pub(super) async fn reserve_rollback_route_identity(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity, SubstrateError> {
        let node = self.sandbox_node(sandbox_id).await?;
        self.reserve_direct_activation_route_identity(sandbox_id, node.id)
            .await
    }

    pub(super) async fn persist_rollback_route_identity(
        &self,
        sandbox_id: SandboxId,
        route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
    ) -> Result<(), SubstrateError> {
        let node = self.sandbox_node(sandbox_id).await?;
        self.persist_route_identity_receipt(
            sandbox_id,
            node.id,
            &sandbox_id.to_string(),
            route_identity,
            DirectRouteIdentityOperation::Rollback,
        )
        .await?;
        Ok(())
    }

    async fn advance_direct_activation_route_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<(i64, i64)>, SubstrateError> {
        let reserved: Option<(i64, i64)> = sqlx::query_as(sql!(
            "UPDATE sandbox_direct_route_provenance
             SET last_route_generation = GREATEST(
                     sandbox_direct_route_provenance.last_route_generation,
                     sandbox_direct_route_provenance.last_execution_generation
                 ) + 1,
                 last_execution_generation = GREATEST(
                     sandbox_direct_route_provenance.last_route_generation,
                     sandbox_direct_route_provenance.last_execution_generation
                 ) + 1,
                 updated_at = NOW()
             WHERE sandbox_id = $1
             RETURNING last_route_generation, last_execution_generation"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(direct_route_generation_reservation_error)?;
        Ok(reserved)
    }

    async fn recover_direct_route_provenance(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .db_context("failed to begin direct route provenance recovery")?;
        let active_receipt_retained = self
            .retain_active_direct_route_provenance(&mut transaction, sandbox_id)
            .await?;
        transaction
            .commit()
            .await
            .db_context("failed to commit active direct route provenance")?;
        if active_receipt_retained {
            return Ok(());
        }

        if let Some(source_node) = self.direct_provenance_source_node(sandbox_id).await?
            && let Some((route_generation, execution_generation)) = self
                .load_direct_route_provenance_from_node(sandbox_id, &source_node)
                .await?
        {
            let mut connection = self
                .pool
                .acquire()
                .await
                .db_context("failed to acquire direct route provenance connection")?;
            upsert_direct_route_provenance(
                &mut connection,
                sandbox_id,
                route_generation,
                execution_generation,
            )
            .await
            .db_context_with(|| {
                format!("failed to recover direct route provenance for sandbox {sandbox_id}")
            })?;
            return Ok(());
        }

        // Historical Direct activations before this allocator all used
        // generation 1. Initialize only this requested sandbox after active
        // and node-local sources have been exhausted; never scan the hot
        // sandboxes table from a startup migration.
        sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_provenance (
                 sandbox_id,
                 last_route_generation,
                 last_execution_generation
             )
             VALUES ($1, 1, 1)
             ON CONFLICT (sandbox_id) DO NOTHING"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(direct_route_generation_reservation_error)?;
        Ok(())
    }

    async fn load_direct_route_provenance_from_node(
        &self,
        sandbox_id: SandboxId,
        source_node: &DirectNode,
    ) -> Result<Option<(RouteGeneration, ExecutionGeneration)>, SubstrateError> {
        let endpoint = Self::endpoint(&source_node.node_api_endpoint)?;
        let mut client = connect_worker(endpoint.clone()).await.map_err(|error| {
            SubstrateError::Unavailable {
                message: format!(
                    "cannot recover direct route provenance for sandbox {sandbox_id} from source node {} at {endpoint:?}: {error}",
                    source_node.id
                ),
            }
        })?;
        let response = client
            .sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
                sandbox_ids: vec![sandbox_id.to_string()],
            })
            .await
            .map_err(|error| SubstrateError::Unavailable {
                message: format!(
                    "source node {} could not report direct route provenance for sandbox {sandbox_id}: {error}",
                    source_node.id
                ),
            })?
            .into_inner();
        let Some(provenance) = response
            .sandboxes
            .into_iter()
            .find(|provenance| provenance.sandbox_id == sandbox_id.to_string())
        else {
            return Ok(None);
        };
        let substrate_kind = pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::try_from(
            provenance.route_provenance_substrate_kind,
        )
        .map_err(|_| SubstrateError::Failed {
            message: format!(
                "source node {} returned an invalid route substrate for sandbox {sandbox_id}",
                source_node.id
            ),
        })?;
        // Schema 044 retained aligned generations before it retained the
        // substrate kind. Unspecified is usable only from this sandbox's
        // registered Direct source; explicit Kubernetes remains a hard fence.
        if substrate_kind == pplx_space_sbx_mgr_client::proto::SandboxSubstrateKind::Kubernetes {
            return Err(SubstrateError::Failed {
                message: format!(
                    "source node {} reports Kubernetes route ownership for Direct sandbox {sandbox_id}",
                    source_node.id
                ),
            });
        }
        let Some(route_generation) = provenance
            .route_provenance_generation
            .filter(|generation| *generation > 0)
        else {
            return Ok(None);
        };
        let Some(execution_generation) = provenance
            .route_provenance_execution_generation
            .filter(|generation| *generation > 0)
        else {
            return Ok(None);
        };
        let route_generation =
            RouteGeneration::try_from(route_generation).map_err(|_| SubstrateError::Failed {
                message: format!(
                    "source node {} returned an out-of-range route generation for sandbox {sandbox_id}",
                    source_node.id
                ),
            })?;
        let execution_generation = ExecutionGeneration::try_from(execution_generation).map_err(
            |_| SubstrateError::Failed {
                message: format!(
                    "source node {} returned an invalid execution generation for sandbox {sandbox_id}",
                    source_node.id
                ),
            },
        )?;
        Ok(Some((route_generation, execution_generation)))
    }

    async fn initialize_direct_activation_route_identity(
        &self,
        sandbox_id: SandboxId,
        target_node_id: NodeId,
    ) -> Result<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity, SubstrateError> {
        let inserted = sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_provenance (
                 sandbox_id,
                 last_route_generation,
                 last_execution_generation
             )
             VALUES ($1, 1, 1)
             ON CONFLICT (sandbox_id) DO NOTHING"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(direct_route_generation_reservation_error)?;
        if inserted.rows_affected() != 1 {
            return Err(SubstrateError::Failed {
                message: format!(
                    "sandbox {sandbox_id} already has direct route provenance before its initial activation"
                ),
            });
        }
        self.reserve_direct_activation_route_identity(sandbox_id, target_node_id)
            .await
    }

    async fn persist_created_route_identity_or_cleanup(
        &self,
        client: &mut RuntimeClient,
        sandbox_id: SandboxId,
        expected_node_id: NodeId,
        response_sandbox_id: &str,
        route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
        operation: DirectRouteIdentityOperation,
    ) -> Result<Option<SandboxRouteIdentity>, SubstrateError> {
        let error = match self
            .persist_route_identity_receipt(
                sandbox_id,
                expected_node_id,
                response_sandbox_id,
                route_identity,
                operation,
            )
            .await
        {
            Ok(identity) => return Ok(identity),
            Err(error) => error,
        };
        let delete_outcome = client
            .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.to_string(),
                template_filter: None,
                blocking: false,
            })
            .await;
        // `NotFound`, or `FailedPrecondition` naming a delete already in
        // progress, both mean there is nothing left to leak — the same reading
        // `is_idempotent_pool_rollback_delete` gives every other compensating
        // delete in this file. Treating them as a failed cleanup would demote a
        // contention abort that is safe to retry, which is the availability
        // cost this change exists to remove.
        if let Err(delete_error) = delete_outcome
            && !is_idempotent_pool_rollback_delete(&delete_error)
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %operation,
                %error,
                %delete_error,
                "best-effort sandbox delete RPC failed after create receipt persistence failure"
            );
            // The node `Create`/`Start` already succeeded and the compensating
            // delete did not, so the sandbox is live on the worker. A retryable
            // answer would have the caller re-create under a fresh
            // `SandboxId`, leaving this one running and unreachable — so the
            // contention disposition is dropped here rather than at each
            // statement inside `persist_route_identity_receipt`.
            //
            // Only on this arm: when the delete succeeds — or reports the
            // sandbox already gone or already being deleted — nothing is left
            // behind and retrying is exactly right, which is why the
            // disposition survives the cleanup path below.
            return Err(error.demoted_after_side_effect());
        }
        Err(error)
    }

    async fn retain_active_direct_route_provenance(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        let retained = sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_provenance (
                 sandbox_id,
                 last_route_generation,
                 last_execution_generation,
                 updated_at
             )
             SELECT sandbox_id,
                    route_generation,
                    execution_generation,
                    NOW()
             FROM sandbox_direct_route_identities
             WHERE sandbox_id = $1
             ON CONFLICT (sandbox_id) DO UPDATE
             SET last_route_generation = GREATEST(
                     sandbox_direct_route_provenance.last_route_generation,
                     EXCLUDED.last_route_generation
                 ),
                 last_execution_generation = GREATEST(
                     sandbox_direct_route_provenance.last_execution_generation,
                     EXCLUDED.last_execution_generation
                 ),
                 updated_at = NOW()"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&mut **transaction)
        .await
        .db_context_with(|| {
            format!("failed to retain active direct route provenance for sandbox {sandbox_id}")
        })?;
        Ok(retained.rows_affected() > 0)
    }

    async fn clear_direct_route_identity(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .db_context("failed to begin direct route identity clear")?;
        self.retain_active_direct_route_provenance(&mut transaction, sandbox_id)
            .await?;
        sqlx::query(sql!(
            "DELETE FROM sandbox_direct_route_identities
             WHERE sandbox_id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .execute(&mut *transaction)
        .await
        .db_context("failed to clear direct route identity")?;
        transaction
            .commit()
            .await
            .db_context("failed to commit direct route identity clear")?;
        Ok(())
    }

    async fn clear_direct_route_identity_then_fail<T>(
        &self,
        sandbox_id: SandboxId,
        error: SubstrateError,
    ) -> Result<T, SubstrateError> {
        match self.clear_direct_route_identity(sandbox_id).await {
            Ok(()) => Err(error),
            Err(clear_error) => Err(SubstrateError::Failed {
                message: format!(
                    "{error}; additionally failed to clear stale direct route identity: {clear_error}"
                ),
            }),
        }
    }

    fn endpoint(addr: &str) -> Result<Endpoint, SubstrateError> {
        Self::endpoint_with_timeout(addr, DIRECT_NODE_RPC_TIMEOUT)
    }

    fn endpoint_with_timeout(
        addr: &str,
        request_timeout: Duration,
    ) -> Result<Endpoint, SubstrateError> {
        node_api_endpoint(addr, DIRECT_NODE_CONNECT_TIMEOUT, request_timeout)
    }

    async fn random_ready_nodes(&self, role: &str) -> Result<Vec<DirectNode>, SubstrateError> {
        sqlx::query_as::<_, DirectNode>(sql!(
            "SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $2
               AND n.role = $1
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
             ORDER BY random()"
        ))
        .bind(role)
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .db_context_with(|| format!("failed to select cached-ready {role} node"))
        .and_then(|nodes| {
            if nodes.is_empty() {
                Err(SubstrateError::Unavailable {
                    message: format!("no cached-ready {role} node-api nodes"),
                })
            } else {
                Ok(nodes)
            }
        })
    }

    pub(crate) async fn eligible_start_node_count(&self) -> Result<u64, SubstrateError> {
        // Mirror the *tolerance* of direct placement's last-known-worker
        // fallback (LEFT JOIN, no `stale_after`, exclude only
        // full/draining/unavailable), not the strict cached-ready predicate.
        // A worker with template inventory but a missing/stale heartbeat is
        // still placement-eligible via that fallback, so counting only fresh
        // snapshots here would falsely reject creates as `empty_cluster`. This
        // is template-agnostic by design — it gates before request-specific
        // template/runtime constraints — so only genuinely-absent workers
        // (zero rows) read as an empty cluster.
        let count = sqlx::query_scalar::<_, i64>(
            sql!("SELECT COUNT(*)
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $1
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND COALESCE(nss.reachable, TRUE) IS TRUE
               AND COALESCE(nss.scheduling_status, 'ready') NOT IN ('full', 'draining', 'unavailable')"),
        )
        .bind(SubstrateKind::Space)
        .fetch_one(&self.pool)
        .await
        .db_context("failed to count eligible direct start nodes")?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    async fn cached_worker_create_candidates(
        &self,
        template: &str,
        content_hash: Option<&str>,
        runtime: &str,
        boot_mode: CreateBootMode,
    ) -> Result<Vec<DirectNode>, SubstrateError> {
        let require_base_snapshot = matches!(boot_mode, CreateBootMode::BaseSnapshot);
        let cached_ready = sqlx::query_as::<_, DirectNode>(
            sql!("SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             JOIN node_template_inventory nti ON nti.node_id = n.id
             WHERE n.managed_by = $5
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND ($3 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $3
                          AND guest.guest_os = nti.guest_os
                          AND guest.guest_arch = nti.arch
                    )
               ))
               AND nti.template_name = $1
               AND ($2::TEXT IS NULL OR nti.content_hash = $2)
               AND (($3 = 'fc' AND nti.runtime IS NULL) OR nti.runtime = $3)
               AND (
                    ($3 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $3
                    )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    OR ($3 = 'fc' AND (
                        nti.state_format_name IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM node_runtime_installations inventory
                            WHERE inventory.node_id = n.id
                        )
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_state_formats state_format
                        WHERE state_format.node_id = n.id
                          AND state_format.runtime = $3
                          AND state_format.state_format_name = nti.state_format_name
                          AND split_part(state_format.state_format_version, '.', 1)
                              = split_part(nti.state_format_version, '.', 1)
                          AND state_format.can_load IS TRUE
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                    FROM node_template_components requested
                    WHERE requested.node_id = nti.node_id
                      AND requested.template_name = nti.template_name
                      AND requested.content_hash = nti.content_hash
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = requested.role_key
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND ($4::BOOLEAN IS FALSE OR nti.has_base_snapshot IS TRUE)
             ORDER BY random()"),
        )
        .bind(template)
        .bind(content_hash)
        .bind(runtime)
        .bind(require_base_snapshot)
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to select cached-ready worker node")?;

        if !cached_ready.is_empty() {
            return Ok(cached_ready);
        }

        let last_known = sqlx::query_as::<_, DirectNode>(
            sql!("SELECT DISTINCT ON (n.id) n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_template_inventory nti ON nti.node_id = n.id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $5
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND COALESCE(nss.reachable, TRUE) IS TRUE
               AND COALESCE(nss.scheduling_status, 'ready') NOT IN ('full', 'draining', 'unavailable')
               AND ($3 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $3
                          AND guest.guest_os = nti.guest_os
                          AND guest.guest_arch = nti.arch
                    )
               ))
               AND nti.template_name = $1
               AND ($2::TEXT IS NULL OR nti.content_hash = $2)
               AND (($3 = 'fc' AND nti.runtime IS NULL) OR nti.runtime = $3)
               AND (
                    ($3 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $3
                    )
               )
               AND (
                    $4::BOOLEAN IS FALSE
                    OR ($3 = 'fc' AND (
                        nti.state_format_name IS NULL
                        OR NOT EXISTS (
                            SELECT 1 FROM node_runtime_installations inventory
                            WHERE inventory.node_id = n.id
                        )
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_state_formats state_format
                        WHERE state_format.node_id = n.id
                          AND state_format.runtime = $3
                          AND state_format.state_format_name = nti.state_format_name
                          AND split_part(state_format.state_format_version, '.', 1)
                              = split_part(nti.state_format_version, '.', 1)
                          AND state_format.can_load IS TRUE
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                    FROM node_template_components requested
                    WHERE requested.node_id = nti.node_id
                      AND requested.template_name = nti.template_name
                      AND requested.content_hash = nti.content_hash
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = requested.role_key
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND ($4::BOOLEAN IS FALSE OR nti.has_base_snapshot IS TRUE)
             ORDER BY n.id, nti.observed_at DESC, nss.observed_at DESC NULLS LAST"),
        )
        .bind(template)
        .bind(content_hash)
        .bind(runtime)
        .bind(require_base_snapshot)
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to select last-known worker node")?;

        if !last_known.is_empty() {
            tracing::warn!(
                template,
                content_hash,
                runtime,
                candidate_count = last_known.len(),
                "no cached-ready worker candidates; falling back to last-known worker inventory"
            );
            return Ok(last_known);
        }

        Err(SubstrateError::Unavailable {
            message: format!("no cached-ready worker node-api nodes host template {template}"),
        })
    }

    async fn pull_on_create_worker_candidates(
        &self,
        runtime: &str,
    ) -> Result<Vec<DirectNode>, SubstrateError> {
        sqlx::query_as::<_, DirectNode>(sql!(
            "SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (
                    ($1 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $1
                    )
               )
               AND ($1 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $1
                    )
               ))
             ORDER BY random()"
        ))
        .bind(runtime)
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to select pull-on-create worker node")
    }

    async fn pooled_worker_candidates(
        &self,
        template: &str,
        content_hash: Option<&str>,
        runtime: &str,
    ) -> Result<Vec<DirectPoolCandidate>, ControllerError> {
        sqlx::query_as::<_, DirectPoolCandidate>(
            sql!("SELECT n.id, nd.node_api_endpoint, nti.content_hash, nti.vcpu, nti.memory_mib
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             JOIN node_template_inventory nti ON nti.node_id = n.id
             WHERE n.managed_by = $4
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND ($3 = 'fc' OR (
                    nss.reachable IS TRUE
                    AND nss.scheduling_status = 'ready'
                    AND nss.stale_after > NOW()
                    AND nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND EXISTS (
                        SELECT 1 FROM node_runtime_guests guest
                        WHERE guest.node_id = n.id
                          AND guest.runtime = $3
                          AND guest.guest_os = nti.guest_os
                          AND guest.guest_arch = nti.arch
                    )
               ))
               AND nti.template_name = $1
               AND ($2::TEXT IS NULL OR nti.content_hash = $2)
               AND (($3 = 'fc' AND nti.runtime IS NULL) OR nti.runtime = $3)
               AND (
                    ($3 = 'fc' AND NOT EXISTS (
                        SELECT 1 FROM node_runtime_installations inventory
                        WHERE inventory.node_id = n.id
                    ))
                    OR EXISTS (
                        SELECT 1 FROM node_runtime_installations runtime
                        WHERE runtime.node_id = n.id
                          AND runtime.runtime = $3
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                    FROM node_template_components requested
                    WHERE requested.node_id = nti.node_id
                      AND requested.template_name = nti.template_name
                      AND requested.content_hash = nti.content_hash
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = requested.role_key
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
               AND nti.pool_current > 0
             ORDER BY nti.pool_current DESC, random()"),
        )
        .bind(template)
        .bind(content_hash)
        .bind(runtime)
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| db_contention_error("failed to select direct pool candidates", &err))
    }

    fn pool_resource_value(
        value: Option<i32>,
        field: &'static str,
    ) -> Result<Option<u32>, ControllerError> {
        value
            .map(u32::try_from)
            .transpose()
            .map_err(|err| ControllerError::internal(format!("pooled {field} is invalid: {err}")))
    }

    fn pool_candidate_shape(
        candidate: &DirectPoolCandidate,
    ) -> Result<Option<PooledExecutionPoolShape>, ControllerError> {
        let vcpu = Self::pool_resource_value(candidate.vcpu, "vcpu")?;
        let memory_mib = Self::pool_resource_value(candidate.memory_mib, "memory_mib")?;
        Ok(match (vcpu, memory_mib) {
            (Some(vcpu), Some(memory_mib)) => Some(PooledExecutionPoolShape::new(
                vcpu,
                memory_mib,
                pplx_space_common_core::types::DiskIoLimits::default(),
            )),
            (None, None) => None,
            _ => {
                return Err(ControllerError::internal(
                    "pooled inventory vcpu and memory_mib must be provided together",
                ));
            }
        })
    }

    async fn delete_adopted_pool_sandbox_best_effort(
        endpoint: Endpoint,
        node_id: NodeId,
        sandbox_id: &str,
        reason: &'static str,
    ) {
        let mut client = match connect_worker(endpoint).await {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(
                    node_id = %node_id,
                    sandbox_id,
                    reason,
                    %error,
                    "failed to connect for cleanup after direct pool adoption error"
                );
                return;
            }
        };

        match client
            .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.to_string(),
                template_filter: None,
                blocking: false,
            })
            .await
        {
            Ok(_) => {
                tracing::warn!(
                    node_id = %node_id,
                    sandbox_id,
                    reason,
                    "deleted adopted direct pool sandbox after claim construction failed"
                );
            }
            Err(error) if is_idempotent_pool_rollback_delete(&error) => {
                tracing::debug!(
                    node_id = %node_id,
                    sandbox_id,
                    reason,
                    %error,
                    "adopted direct pool sandbox was already gone during cleanup"
                );
            }
            Err(error) => {
                tracing::warn!(
                    node_id = %node_id,
                    sandbox_id,
                    reason,
                    %error,
                    "failed to clean up adopted direct pool sandbox after claim construction failed"
                );
            }
        }
    }

    async fn sandbox_node(&self, sandbox_id: SandboxId) -> Result<DirectNode, SubstrateError> {
        sqlx::query_as::<_, DirectNode>(sql!(
            "SELECT n.id, nd.node_api_endpoint
             FROM sandboxes s
             JOIN nodes n ON n.id = s.active_node_id
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
             LIMIT 1"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(SubstrateKind::Space)
        .fetch_optional(&self.pool)
        .await
        .db_context_with(|| format!("failed to load worker placement for sandbox {sandbox_id}"))?
        .ok_or_else(|| SubstrateError::Unavailable {
            message: format!("sandbox {sandbox_id} is not assigned to an active worker node"),
        })
    }

    async fn direct_provenance_source_node(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<DirectNode>, SubstrateError> {
        sqlx::query_as::<_, DirectNode>(sql!(
            "SELECT n.id, nd.node_api_endpoint
             FROM sandboxes s
             JOIN nodes n ON n.id = COALESCE(s.target_node_id, s.active_node_id)
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
             LIMIT 1"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(SubstrateKind::Space)
        .fetch_optional(&self.pool)
        .await
        .db_context_with(|| {
            format!("failed to load provenance source node for sandbox {sandbox_id}")
        })
    }

    async fn worker_client_for_node(
        &self,
        node: &DirectNode,
    ) -> Result<(Endpoint, RuntimeClient), SubstrateError> {
        let endpoint = Self::endpoint(&node.node_api_endpoint)?;
        let client = self
            .worker_channels
            .client(&node.node_api_endpoint)
            .await
            .map_err(|err| SubstrateError::Unavailable {
                message: format!("worker node {} not reachable: {err}", node.id),
            })?;
        Ok((endpoint, client))
    }

    async fn direct_node_by_id(&self, node_id: NodeId) -> Result<DirectNode, SubstrateError> {
        sqlx::query_as::<_, DirectNode>(sql!(
            "SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE n.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
             LIMIT 1"
        ))
        .bind(node_id)
        .bind(SubstrateKind::Space)
        .fetch_optional(&self.pool)
        .await
        .db_context_with(|| format!("failed to load direct worker node {node_id}"))?
        .ok_or_else(|| SubstrateError::Unavailable {
            message: format!("direct worker node {node_id} is not registered"),
        })
    }

    async fn worker_client_for_node_with_timeout(
        &self,
        node: &DirectNode,
        request_timeout: Duration,
    ) -> Result<(Endpoint, RuntimeClient), SubstrateError> {
        let endpoint = Self::endpoint_with_timeout(&node.node_api_endpoint, request_timeout)?;
        let client =
            connect_worker(endpoint.clone())
                .await
                .map_err(|err| SubstrateError::Unavailable {
                    message: format!("worker node {} not reachable: {err}", node.id),
                })?;
        Ok((endpoint, client))
    }

    async fn direct_restore_candidates(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<DirectRestoreCandidate>, SubstrateError> {
        self.direct_restore_candidates_for_mode(sandbox_id, false)
            .await
    }

    async fn direct_restore_retry_candidates(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Vec<DirectRestoreCandidate>, SubstrateError> {
        self.direct_restore_candidates_for_mode(sandbox_id, true)
            .await
    }

    async fn direct_restore_candidates_for_mode(
        &self,
        sandbox_id: SandboxId,
        restore_retry: bool,
    ) -> Result<Vec<DirectRestoreCandidate>, SubstrateError> {
        let mut candidates = Vec::new();
        let retry_source = restore_retry_source_snapshot_is_current("s", "sn");
        let source_snapshot = if restore_retry {
            retry_source.as_str()
        } else {
            "TRUE"
        };

        // A fresh restore uses the newest full row regardless of upload state:
        // those execution columns describe the artifact held by the source
        // node even when its upload failed or its durable copy was claimed.
        // A retry instead uses the same pre-claim source row as the remote leg;
        // newer post-claim attempts are not authoritative for compatibility.
        // The observed-state join supplies only existence and placement.
        let source_query = format!(
            "WITH latest_snapshot AS (
                 SELECT sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND {source_snapshot}
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    NULL::TEXT AS storage_uri,
                    COALESCE(s.target_node_id, s.active_node_id) AS source_node_id
             FROM sandboxes s
             CROSS JOIN latest_snapshot
             JOIN sandbox_observed_states sos
               ON sos.sandbox_id = s.id
              AND sos.node_id = COALESCE(s.target_node_id, s.active_node_id)
             JOIN nodes n ON n.id = sos.node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE s.id = $1
               AND n.managed_by = $2
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    JOIN node_runtime_state_formats state_format
                      ON state_format.node_id = runtime.node_id
                     AND state_format.runtime = runtime.runtime
                     AND state_format.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND state_format.state_format_name = latest_snapshot.state_format_name
                      AND split_part(state_format.state_format_version, '.', 1)
                          = split_part(latest_snapshot.state_format_version, '.', 1)
                      AND state_format.can_load IS TRUE
                   )
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
             LIMIT 1"
        );
        let source_candidates = sqlx::query_as::<_, DirectRestoreCandidateRow>(
            sqlx::AssertSqlSafe(sql!(source_query.as_str())),
        )
        .bind(sandbox_id.as_uuid())
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to select direct source restore candidate")?;
        candidates.extend(
            source_candidates
                .into_iter()
                .map(DirectRestoreCandidate::source_node),
        );

        let no_newer = no_newer_full_snapshot("s", "sn");
        let remote_source = if restore_retry {
            retry_source.as_str()
        } else {
            no_newer.as_str()
        };
        let remote_query = format!(
            "WITH latest_snapshot AS (
                 SELECT sn.id AS snapshot_id,
                        storage.storage_connection_string AS storage_uri,
                        COALESCE(sn.source_node_id, s.target_node_id, s.active_node_id) AS source_node_id,
                        sn.runtime,
                        sn.guest_os,
                        sn.guest_arch,
                        sn.state_format_name,
                        sn.state_format_version,
                        sn.execution_metadata
                 FROM sandboxes s
                 JOIN snapshots sn ON sn.sandbox_id = s.id
                 JOIN LATERAL (
                     SELECT ssl.storage_connection_string
                     FROM snapshot_storage_locations ssl
                     WHERE ssl.snapshot_id = sn.id
                     ORDER BY ssl.created_at DESC, ssl.id DESC
                     LIMIT 1
                 ) storage ON TRUE
                 WHERE s.id = $1
                   AND sn.type = 'full'
                   AND sn.state = 'ready'
                   AND {remote_source}
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             )
             SELECT n.id,
                    nd.node_api_endpoint,
                    latest_snapshot.storage_uri,
                    latest_snapshot.source_node_id
             FROM latest_snapshot
             JOIN nodes n ON n.id IS DISTINCT FROM latest_snapshot.source_node_id
             JOIN node_direct nd ON nd.node_id = n.id
             JOIN node_status_snapshots nss ON nss.node_id = n.id
             WHERE n.managed_by = $3
               AND n.role = 'worker'
               AND nd.deleted_at IS NULL
               AND COALESCE(n.state, 'registered') = 'registered'
               AND nss.reachable IS TRUE
               AND nss.stale_after > NOW()
               AND nss.scheduling_status = 'ready'
               AND (latest_snapshot.runtime = 'fc' OR (
                    nss.execution_inventory_status = 'complete'
                    AND nss.execution_inventory_received_at
                        + (nss.stale_after - nss.observed_at) > NOW()
                    AND latest_snapshot.execution_metadata IS NOT NULL
               ))
               AND latest_snapshot.runtime IS NOT NULL
               AND latest_snapshot.guest_os IS NOT NULL
               AND latest_snapshot.guest_arch IS NOT NULL
               AND latest_snapshot.state_format_name IS NOT NULL
               AND latest_snapshot.state_format_version IS NOT NULL
               AND EXISTS (
                    SELECT 1
                    FROM node_runtime_installations runtime
                    JOIN node_runtime_guests guest
                      ON guest.node_id = runtime.node_id
                     AND guest.runtime = runtime.runtime
                     AND guest.runtime_version = runtime.runtime_version
                    JOIN node_runtime_state_formats state_format
                      ON state_format.node_id = runtime.node_id
                     AND state_format.runtime = runtime.runtime
                     AND state_format.runtime_version = runtime.runtime_version
                    WHERE runtime.node_id = n.id
                      AND runtime.runtime = latest_snapshot.runtime
                      AND guest.guest_os = latest_snapshot.guest_os
                      AND guest.guest_arch = latest_snapshot.guest_arch
                      AND state_format.state_format_name = latest_snapshot.state_format_name
                      AND split_part(state_format.state_format_version, '.', 1)
                          = split_part(latest_snapshot.state_format_version, '.', 1)
                      AND state_format.can_load IS TRUE
                   )
                   -- Component compatibility reads the components array of the
                   -- execution_metadata document written by BeginArtifactBackup.
                   -- Omitted optional keys surface as NULL
                   -- columns: NULL digest/version/format arms are unconstrained,
                   -- and an absent role maps to node_artifact_components'
                   -- role_key ''. A NULL document (legacy rows) means no
                   -- compatibility requirements. Malformed documents fail
                   -- closed: a non-array components value errors the whole
                   -- candidate query rather than silently passing, and a
                   -- component with no kind can never match an installed
                   -- component, so it excludes the node.
                   AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_to_recordset(
                             COALESCE(latest_snapshot.execution_metadata->'components',
                                      '[]'::jsonb)
                         ) AS requested(kind text, role text, digest text,
                                        version text, format text)
                    WHERE (latest_snapshot.runtime <> 'fc' OR requested.kind IS DISTINCT FROM 'kernel')
                      AND NOT EXISTS (
                          SELECT 1
                          FROM node_artifact_components installed
                          WHERE installed.node_id = n.id
                            AND installed.kind = requested.kind
                            AND installed.role_key = COALESCE(requested.role, '')
                            AND (
                                (requested.digest IS NOT NULL AND installed.digest = requested.digest)
                                OR (
                                    requested.digest IS NULL
                                    AND (requested.version IS NULL OR installed.version = requested.version)
                                    AND (requested.format IS NULL OR installed.format = requested.format)
                                )
                            )
                      )
               )
             ORDER BY random()
             LIMIT $2",
        );
        let remote_candidates = sqlx::query_as::<_, DirectRestoreCandidateRow>(
            sqlx::AssertSqlSafe(sql!(remote_query.as_str())),
        )
        .bind(sandbox_id.as_uuid())
        .bind(DIRECT_REMOTE_RESTORE_CANDIDATE_LIMIT)
        .bind(SubstrateKind::Space)
        .fetch_all(&self.pool)
        .await
        .db_context("failed to select direct remote restore candidates")?;
        candidates.extend(
            remote_candidates
                .into_iter()
                .map(DirectRestoreCandidate::remote_snapshot),
        );

        Ok(candidates)
    }

    async fn direct_restore_candidates_or_unavailable(
        &self,
        sandbox_id: SandboxId,
        lease: DirectRestoreLease,
    ) -> Result<Vec<DirectRestoreCandidate>, SubstrateError> {
        self.direct_restore_candidates_for_mode_or_unavailable(sandbox_id, lease.restore_retry)
            .await
    }

    async fn direct_restore_candidates_for_mode_or_unavailable(
        &self,
        sandbox_id: SandboxId,
        restore_retry: bool,
    ) -> Result<Vec<DirectRestoreCandidate>, SubstrateError> {
        let candidates = if restore_retry {
            self.direct_restore_retry_candidates(sandbox_id).await?
        } else {
            self.direct_restore_candidates(sandbox_id).await?
        };
        if candidates.is_empty() {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "no compatible direct restore candidates for sandbox {sandbox_id}: \
                     source node is unavailable or incompatible, and no ready full snapshot \
                     storage URI matches a ready worker"
                ),
            });
        }
        Ok(candidates)
    }

    async fn direct_restore_preflight_mode(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        let (lifecycle, lease_remaining_seconds) =
            sqlx::query_as::<_, (String, Option<f64>)>(sql!(
                "SELECT lifecycle,
                    CASE
                        WHEN restore_lease_expires_at > NOW() THEN
                            EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW()))::float8
                        ELSE NULL
                    END
               FROM sandboxes
              WHERE id = $1"
            ))
            .bind(sandbox_id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .db_context("failed to inspect direct restore lifecycle")?
            .ok_or_else(|| SubstrateError::NotFound {
                message: format!("sandbox {sandbox_id} not found"),
            })?;
        if let Some(lease_remaining_seconds) = lease_remaining_seconds {
            return Err(SubstrateError::restore_in_flight_with_lease(
                format!("another Direct restore owns sandbox {sandbox_id}"),
                Duration::from_secs_f64(lease_remaining_seconds),
            ));
        }
        match lifecycle.parse::<SandboxLifecycle>() {
            Ok(SandboxLifecycle::Suspended) => Ok(false),
            Ok(SandboxLifecycle::Restoring) => Ok(true),
            Ok(lifecycle) => Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox {sandbox_id} cannot begin Direct restore from lifecycle {lifecycle}"
                ),
            }),
            Err(error) => Err(SubstrateError::Failed {
                message: format!("sandbox {sandbox_id} has invalid lifecycle: {error}"),
            }),
        }
    }

    async fn claim_direct_restore_lease(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<DirectRestoreLease, SubstrateError> {
        let token = Uuid::now_v7();
        let claimed = sqlx::query_as::<_, (Uuid, bool)>(sql!(
            "WITH candidate AS (
                 SELECT id,
                        LOWER(TRIM(lifecycle)) = 'restoring' AS restore_retry
                   FROM sandboxes
                  WHERE id = $1
                    AND LOWER(TRIM(lifecycle)) IN ('suspended', 'restoring')
                    AND (restore_lease_expires_at IS NULL OR restore_lease_expires_at <= NOW())
                  FOR UPDATE
             )
             UPDATE sandboxes s
                SET lifecycle = 'restoring',
                    restore_lease_expires_at = NOW() + ($2::bigint * INTERVAL '1 second'),
                    restore_lease_token = $3
               FROM candidate c
              WHERE s.id = c.id
          RETURNING $3::UUID, c.restore_retry"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(i64::try_from(DIRECT_NODE_RPC_TIMEOUT.as_secs()).expect("node timeout fits i64"))
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to claim direct restore lease")?;
        if let Some((claimed_token, restore_retry)) = claimed {
            debug_assert_eq!(claimed_token, token);
            return Ok(DirectRestoreLease {
                token,
                restore_retry,
            });
        }

        let (lifecycle, remaining_seconds) = sqlx::query_as::<_, (String, Option<f64>)>(sql!(
            "SELECT lifecycle,
                    CASE
                        WHEN restore_lease_expires_at IS NULL THEN NULL
                        ELSE GREATEST(
                            EXTRACT(EPOCH FROM (restore_lease_expires_at - NOW())),
                            0.0
                        )::float8
                    END
               FROM sandboxes
              WHERE id = $1"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to inspect direct restore lease")?
        .ok_or_else(|| SubstrateError::NotFound {
            message: format!("sandbox {sandbox_id} not found"),
        })?;
        let lifecycle =
            lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|error| SubstrateError::Failed {
                    message: format!("sandbox {sandbox_id} has invalid lifecycle: {error}"),
                })?;
        let Some(remaining_seconds) = remaining_seconds else {
            return Err(SubstrateError::Conflict {
                message: format!(
                    "sandbox {sandbox_id} cannot begin Direct restore from lifecycle {lifecycle}"
                ),
            });
        };
        Err(SubstrateError::restore_in_flight_with_lease(
            format!("another Direct restore owns sandbox {sandbox_id}"),
            Duration::from_secs_f64(remaining_seconds),
        ))
    }

    async fn release_direct_restore_lease(
        &self,
        sandbox_id: SandboxId,
        lease: DirectRestoreLease,
    ) -> Result<(), SubstrateError> {
        let released = sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = CASE WHEN $3 THEN lifecycle ELSE 'suspended' END,
                    restore_lease_expires_at = CASE
                        WHEN $3 THEN restore_lease_expires_at
                        ELSE NULL
                    END,
                    restore_lease_token = CASE
                        WHEN $3 THEN restore_lease_token
                        ELSE NULL
                    END
              WHERE id = $1
                AND restore_lease_token = $2
                AND lifecycle = 'restoring'"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(lease.token)
        .bind(lease.restore_retry)
        .execute(&self.pool)
        .await
        .db_context("failed to release direct restore lease")?;
        if released.rows_affected() != 1 {
            return Err(SubstrateError::restore_in_flight(format!(
                "direct restore ownership changed before release for sandbox {sandbox_id}"
            )));
        }
        Ok(())
    }

    async fn release_direct_restore_lease_best_effort(
        &self,
        sandbox_id: SandboxId,
        lease: DirectRestoreLease,
        primary_error: &SubstrateError,
    ) {
        if let Err(release_error) = self.release_direct_restore_lease(sandbox_id, lease).await {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                %primary_error,
                %release_error,
                "failed to release direct restore lease while preserving the primary restore error"
            );
        }
    }

    async fn renew_direct_restore_lease(
        &self,
        sandbox_id: SandboxId,
        lease: DirectRestoreLease,
    ) -> Result<(), SubstrateError> {
        let renewed = sqlx::query(sql!(
            "UPDATE sandboxes
                SET restore_lease_expires_at = NOW() + ($3::bigint * INTERVAL '1 second')
              WHERE id = $1
                AND restore_lease_token = $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(lease.token)
        .bind(
            i64::try_from(DIRECT_NODE_RPC_TIMEOUT.as_secs() * 2)
                .expect("renewed node timeout fits i64"),
        )
        .execute(&self.pool)
        .await
        .db_context("failed to renew direct restore lease")?;
        if renewed.rows_affected() != 1 {
            return Err(SubstrateError::restore_in_flight(format!(
                "direct restore ownership changed for sandbox {sandbox_id}"
            )));
        }
        Ok(())
    }

    async fn complete_direct_restore_lease(
        &self,
        sandbox_id: SandboxId,
        lease: DirectRestoreLease,
    ) -> Result<(), SubstrateError> {
        let completed = sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'started',
                    placement = 'on_node'
              WHERE id = $1
                AND restore_lease_token = $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(lease.token)
        .execute(&self.pool)
        .await
        .db_context("failed to complete direct restore lease")?;
        if completed.rows_affected() != 1 {
            return Err(SubstrateError::restore_in_flight(format!(
                "direct restore ownership changed before completion for sandbox {sandbox_id}"
            )));
        }
        Ok(())
    }

    async fn restore_execution_with_candidates(
        &self,
        sandbox_id: SandboxId,
        options: ActivationOptions,
    ) -> Result<DirectRestoreResult, SubstrateError> {
        // A read-only preflight avoids two lifecycle writes for a wake that
        // has no usable source. It is only an optimization: the candidate set
        // is selected again after the lease claim, which is the serialized
        // correctness boundary shared with retention.
        let restore_retry = self.direct_restore_preflight_mode(sandbox_id).await?;
        self.direct_restore_candidates_for_mode_or_unavailable(sandbox_id, restore_retry)
            .await?;
        let lease = self.claim_direct_restore_lease(sandbox_id).await?;
        // Claim the sandbox before selecting a durable restore source. The
        // retention claim takes the same row lock and rechecks lifecycle after
        // it acquires it, so it cannot mark a source `deleting` between
        // candidate selection and restore.
        let candidates = match self
            .direct_restore_candidates_or_unavailable(sandbox_id, lease)
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                self.release_direct_restore_lease_best_effort(sandbox_id, lease, &error)
                    .await;
                return Err(error);
            }
        };
        let attempt_timeout =
            split_restore_attempt_timeout(options.ready_timeout, candidates.len());
        let mut failures = Vec::new();

        for candidate in candidates {
            let attempt_deadline = tokio::time::Instant::now() + attempt_timeout;
            let worker = self
                .worker_client_for_node_with_timeout(&candidate.node, attempt_timeout)
                .await;
            let (endpoint, mut client) = match worker {
                Ok(worker) => worker,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        node_id = %candidate.node.id,
                        candidate_kind = candidate.kind.as_str(),
                        %error,
                        "direct restore candidate is not reachable; trying another worker"
                    );
                    failures.push(format!(
                        "{} ({}): {error}",
                        candidate.node.id,
                        candidate.kind.as_str()
                    ));
                    continue;
                }
            };

            // No Start has been dispatched, so capability failures can safely
            // try another candidate. Keep one restore lease across attempts.
            let probe_budget =
                attempt_deadline.saturating_duration_since(tokio::time::Instant::now());
            if let Err(error) = client.require_activation_policy_support(probe_budget).await {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    node_id = %candidate.node.id,
                    candidate_kind = candidate.kind.as_str(),
                    %error,
                    "direct restore candidate capability could not be verified; trying another worker"
                );
                failures.push(format!(
                    "{} ({}): {error}",
                    candidate.node.id,
                    candidate.kind.as_str()
                ));
                continue;
            }
            let remote_restore = candidate.storage_uri.is_some();
            self.renew_direct_restore_lease(sandbox_id, lease).await?;
            let route_identity = match self
                .reserve_direct_activation_route_identity(sandbox_id, candidate.node.id)
                .await
            {
                Ok(identity) => identity,
                Err(error) => {
                    self.release_direct_restore_lease_best_effort(sandbox_id, lease, &error)
                        .await;
                    return Err(error);
                }
            };
            let vcpu = match self.activation_vcpu(sandbox_id).await {
                Ok(vcpu) => vcpu,
                Err(error) => {
                    self.release_direct_restore_lease_best_effort(sandbox_id, lease, &error)
                        .await;
                    return Err(error);
                }
            };
            let mut request = direct_restore_start_request(
                sandbox_id,
                vcpu,
                options.blocking,
                attempt_timeout,
                &candidate,
                Some(route_identity.clone()),
            );
            request.activation_auto_lifecycle =
                match self.activation_auto_lifecycle(sandbox_id).await {
                    Ok(policy) => Some(policy),
                    Err(error) => {
                        self.release_direct_restore_lease_best_effort(sandbox_id, lease, &error)
                            .await;
                        return Err(error);
                    }
                };
            let restore_result =
                tokio::time::timeout_at(attempt_deadline, client.start_sandbox(request)).await;
            match restore_result {
                Ok(Ok(response)) => {
                    let persist_result = self
                        .persist_owned_restore_route_identity_receipt(
                            sandbox_id,
                            candidate.node.id,
                            response.get_ref(),
                            route_identity,
                            lease,
                        )
                        .await;
                    let route_identity = match persist_result {
                        Ok(route_identity) => route_identity,
                        Err(error) => {
                            if remote_restore {
                                match client
                                    .delete_sandbox(
                                        pplx_space_sbx_mgr_client::proto::DeleteRequest {
                                            sandbox_id: sandbox_id.to_string(),
                                            template_filter: None,
                                            blocking: false,
                                        },
                                    )
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(cleanup_error) => {
                                        tracing::warn!(
                                            sandbox_id = %sandbox_id,
                                            node_id = %candidate.node.id,
                                            %error,
                                            %cleanup_error,
                                            "failed to clean destination after direct restore receipt persistence failure"
                                        );
                                    }
                                }
                            }
                            // The node reported a successful restore. If the
                            // receipt cannot be committed, retain the lease:
                            // destination cleanup is best-effort and the
                            // execution may still be live.
                            return Err(error);
                        }
                    };
                    if remote_restore && let Some(source_node_id) = candidate.source_node_id {
                        self.delete_source_suspended_sandbox_best_effort(
                            sandbox_id,
                            source_node_id,
                            candidate.node.id,
                            lease,
                        )
                        .await?;
                    }
                    self.complete_direct_restore_lease(sandbox_id, lease)
                        .await?;
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        node_id = %candidate.node.id,
                        candidate_kind = candidate.kind.as_str(),
                        remote_restore,
                        "direct restore succeeded"
                    );
                    return Ok(DirectRestoreResult {
                        endpoint,
                        execution_generation: receipt_execution_generation(route_identity.as_ref()),
                    });
                }
                Ok(Err(status)) if should_try_next_restore_candidate(&status) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        node_id = %candidate.node.id,
                        candidate_kind = candidate.kind.as_str(),
                        code = ?status.code(),
                        error = %status,
                        "direct restore candidate failed with retryable node status; trying another worker"
                    );
                    failures.push(format!(
                        "{} ({}): node-api restore error: {status}",
                        candidate.node.id,
                        candidate.kind.as_str()
                    ));
                }
                Ok(Err(status)) => {
                    let error = restore_status_error(&status);
                    self.release_direct_restore_lease_best_effort(sandbox_id, lease, &error)
                        .await;
                    return Err(error);
                }
                Err(_) => {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        node_id = %candidate.node.id,
                        candidate_kind = candidate.kind.as_str(),
                        timeout_ms = attempt_timeout.as_millis(),
                        "direct restore candidate timed out; retaining restore ownership for convergence"
                    );
                    return Err(SubstrateError::Unavailable {
                        message: format!(
                            "direct restore candidate {} ({}) timed out after {:?}; the restore may still be active",
                            candidate.node.id,
                            candidate.kind.as_str(),
                            attempt_timeout
                        ),
                    });
                }
            }
        }

        let failure_summary = if failures.is_empty() {
            "no eligible candidates".to_string()
        } else {
            failures.join("; ")
        };
        let error = SubstrateError::Unavailable {
            message: format!(
                "no direct restore candidates succeeded for sandbox {sandbox_id}: {failure_summary}"
            ),
        };
        self.release_direct_restore_lease_best_effort(sandbox_id, lease, &error)
            .await;
        Err(error)
    }

    async fn delete_source_suspended_sandbox_best_effort(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        restored_node_id: NodeId,
        lease: DirectRestoreLease,
    ) -> Result<(), SubstrateError> {
        if source_node_id == restored_node_id {
            return Ok(());
        }

        // The source delete cannot carry a fencing token to node-manager, so
        // renew the database lease for twice the bounded RPC window before
        // issuing it. A successor cannot claim the sandbox while this owner
        // can still delete the old source execution.
        let source_node = match self.direct_node_by_id(source_node_id).await {
            Ok(node) => node,
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    source_node_id = %source_node_id,
                    restored_node_id = %restored_node_id,
                    %error,
                    "direct cross-node restore could not resolve source node for cleanup"
                );
                return Ok(());
            }
        };

        let (_, mut client) = match self.worker_client_for_node(&source_node).await {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    source_node_id = %source_node_id,
                    restored_node_id = %restored_node_id,
                    %error,
                    "direct cross-node restore could not reach source node for cleanup"
                );
                return Ok(());
            }
        };

        if let Err(error) = self.renew_direct_restore_lease(sandbox_id, lease).await {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                source_node_id = %source_node_id,
                restored_node_id = %restored_node_id,
                %error,
                "skipping old source-node cleanup because the direct restore lease could not be renewed"
            );
            return Ok(());
        }
        let delete = tokio::time::timeout(
            DIRECT_NODE_RPC_TIMEOUT,
            client.delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.to_string(),
                template_filter: None,
                blocking: false,
            }),
        )
        .await;
        match delete {
            Err(_) => tracing::warn!(
                sandbox_id = %sandbox_id,
                source_node_id = %source_node_id,
                restored_node_id = %restored_node_id,
                "timed out cleaning old source-node suspended sandbox after direct cross-node restore"
            ),
            Ok(Ok(_)) => tracing::info!(
                sandbox_id = %sandbox_id,
                source_node_id = %source_node_id,
                restored_node_id = %restored_node_id,
                "deleted old source-node suspended sandbox after direct cross-node restore"
            ),
            Ok(Err(error)) if error.code() == Code::NotFound => tracing::debug!(
                sandbox_id = %sandbox_id,
                source_node_id = %source_node_id,
                restored_node_id = %restored_node_id,
                %error,
                "old source-node suspended sandbox was already absent after direct cross-node restore"
            ),
            Ok(Err(error)) => tracing::warn!(
                sandbox_id = %sandbox_id,
                source_node_id = %source_node_id,
                restored_node_id = %restored_node_id,
                %error,
                "failed to clean up old source-node suspended sandbox after direct cross-node restore"
            ),
        }
        Ok(())
    }

    async fn mark_cached_worker_create_failure(
        &self,
        node: &DirectNode,
        failure: CachedWorkerFailure,
        reason: String,
    ) {
        let scheduling_status = failure.scheduling_status();
        let reachable = failure.reachable();
        if let Err(error) = sqlx::query(sql!(
            "INSERT INTO node_status_snapshots (
                 node_id,
                 observed_at,
                 stale_after,
                 reachable,
                 error,
                 role,
                 scheduling_status,
                 scheduling_reason,
                 health_healthy,
                 services,
                 updated_at
             )
             SELECT n.id,
                    NOW(),
                    NOW() + ($4::TEXT || ' seconds')::INTERVAL,
                    $2,
                    CASE WHEN $2 THEN NULL ELSE $3 END,
                    n.role,
                    $5,
                    $3,
                    CASE WHEN $2 THEN NULL ELSE FALSE END,
                    '[]'::JSONB,
                    NOW()
             FROM nodes n
             WHERE n.id = $1
             ON CONFLICT (node_id) DO UPDATE
             SET observed_at = EXCLUDED.observed_at,
                 stale_after = EXCLUDED.stale_after,
                 reachable = EXCLUDED.reachable,
                 error = EXCLUDED.error,
                 role = EXCLUDED.role,
                 scheduling_status = EXCLUDED.scheduling_status,
                 scheduling_reason = EXCLUDED.scheduling_reason,
                 network_slots_ready = CASE
                     WHEN EXCLUDED.scheduling_status IN ('full', 'draining', 'unavailable')
                     THEN 0
                     ELSE node_status_snapshots.network_slots_ready
                 END,
                 health_healthy = CASE
                     WHEN EXCLUDED.reachable THEN node_status_snapshots.health_healthy
                     ELSE FALSE
                 END,
                 updated_at = NOW()"
        ))
        .bind(node.id)
        .bind(reachable)
        .bind(&reason)
        .bind(DIRECT_NODE_CREATE_FAILURE_TTL_SECS)
        .bind(scheduling_status)
        .execute(&self.pool)
        .await
        {
            tracing::warn!(
                node_id = %node.id,
                scheduling_status,
                %error,
                "failed to update cached worker status after create failure"
            );
        }
    }

    async fn try_create_on_worker_candidates<'req>(
        &self,
        request: &CreateExecutionRequest<'req>,
        one_shot_create: bool,
        candidates: Vec<DirectNode>,
        source: WorkerCandidateSource,
        failures: &mut Vec<String>,
    ) -> Result<Option<WorkerCreateSelection>, SubstrateError> {
        let sandbox_id = request.sandbox_id;
        let create_policy = override_request_policy(request.network_policy_override);
        for node in candidates {
            let worker = self
                .worker_client_for_node_with_timeout(&node, source.request_timeout())
                .await;
            match worker {
                Ok((endpoint, mut client)) => {
                    tracing::info!(
                        sandbox_id = %sandbox_id,
                        node_id = %node.id,
                        node_api_endpoint = %node.node_api_endpoint,
                        candidate_source = source.as_str(),
                        "selected direct worker node"
                    );

                    let boot_mode = direct_create_boot_mode(request.template_ref.boot_mode)?;
                    let create_response = client
                        .create_sandbox(pplx_space_sbx_mgr_client::proto::CreateRequest {
                            environment: None,
                            template: request.template_ref.template.as_str().to_string(),
                            owner: request.actor.sandbox_owner.to_string(),
                            runtime: request.template_ref.runtime.to_string(),
                            snapshots_enabled: false,
                            autostart: one_shot_create,
                            pool_template: None,
                            volume_encryption: request.volume_encryption,
                            boot_mode,
                            blocking: one_shot_create,
                            sandbox_id: Some(sandbox_id.to_string()),
                            content_hash: request.template_ref.content_hash.map(ToOwned::to_owned),
                            env: request.env.clone().into_iter().collect(),
                            auto_lifecycle: Some(policy_to_proto(request.auto_lifecycle)),
                            rootfs_size_mb: request.resources.disk_mib,
                            network_policy: create_policy.clone(),
                            network_policy_generation: request.network_policy_generation,
                            network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
                            auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
                            template_network_policy: Some(network_policy_to_proto(
                                request.template_network_policy,
                            )),
                            template_network_policy_generation: request
                                .template_network_policy_generation,
                            template_network_policy_authority: PolicyAuthority::Cluster
                                .as_proto_i32(),
                            route_identity: None,
                            disk_io_max_operations_per_sec: Some(
                                request.disk_io_limits.max_operations_per_sec(),
                            ),
                            disk_io_max_bandwidth_mib_per_sec: Some(
                                request.disk_io_limits.max_bandwidth_mib_per_sec(),
                            ),
                        })
                        .await;

                    match create_response {
                        Ok(response) => {
                            return Ok(Some((node, endpoint, client, response.into_inner())));
                        }
                        Err(status) => {
                            if pplx_space_sbx_mgr_client::is_template_policy_dependency(&status)
                                && should_try_next_worker_candidate(&status)
                            {
                                // A different worker may already have the owner policy.
                                // Keep failover, without poisoning worker health/capacity
                                // when its template-manager is unavailable or behind.
                                tracing::warn!(
                                    sandbox_id = %sandbox_id,
                                    node_id = %node.id,
                                    error = %status,
                                    "template policy not ready; trying another worker"
                                );
                                failures
                                    .push(format!("{}: template policy error: {status}", node.id));
                                continue;
                            }
                            if let Some(failure) = cached_worker_failure_from_create_status(&status)
                            {
                                let reason = format!("node-api create error: {status}");
                                self.mark_cached_worker_create_failure(
                                    &node,
                                    failure,
                                    reason.clone(),
                                )
                                .await;
                                tracing::warn!(
                                    sandbox_id = %sandbox_id,
                                    node_id = %node.id,
                                    scheduling_status = failure.scheduling_status(),
                                    candidate_source = source.as_str(),
                                    error = %status,
                                    "direct worker create failed with retryable node status; trying another worker"
                                );
                                failures.push(format!("{}: {reason}", node.id));
                                continue;
                            }
                            return Err(direct_create_status_error(status));
                        }
                    }
                }
                Err(error) => {
                    self.mark_cached_worker_create_failure(
                        &node,
                        CachedWorkerFailure::Unavailable,
                        format!("worker connect error: {error}"),
                    )
                    .await;
                    tracing::warn!(
                        node_id = %node.id,
                        candidate_source = source.as_str(),
                        %error,
                        "ready direct worker node is not reachable; trying another worker"
                    );
                    failures.push(format!("{}: {error}", node.id));
                }
            }
        }

        Ok(None)
    }

    async fn worker_client_for_sandbox(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(DirectNode, Endpoint, RuntimeClient), SubstrateError> {
        let node = self.sandbox_node(sandbox_id).await?;
        self.worker_client_for_node(&node)
            .await
            .map(|(endpoint, client)| (node, endpoint, client))
    }

    async fn node_client(&self, sandbox_id: SandboxId) -> Result<RuntimeClient, ControllerError> {
        let (_, _, client) = self
            .worker_client_for_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        Ok(client)
    }

    async fn builder_node_client(&self) -> Result<BuilderClient, ControllerError> {
        let mut failures = Vec::new();
        for node in self
            .random_ready_nodes(SpaceNodeRole::Builder.as_str())
            .await
            .map_err(ControllerError::from)?
        {
            let endpoint = match Self::endpoint(&node.node_api_endpoint) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    tracing::warn!(
                        node_id = %node.id,
                        %error,
                        "registered direct builder node has invalid endpoint; trying another builder"
                    );
                    failures.push(format!("{}: {error}", node.id));
                    continue;
                }
            };
            match connect_builder(endpoint).await {
                Ok(client) => return Ok(client),
                Err(error) => {
                    tracing::warn!(
                        node_id = %node.id,
                        %error,
                        "registered direct builder node is not reachable; trying another builder"
                    );
                    failures.push(format!("{}: {error}", node.id));
                }
            }
        }

        Err(ControllerError::service_unavailable(format!(
            "no reachable cached-ready builder node-api nodes: {}",
            failures.join("; ")
        )))
    }
}

impl SubstrateAdapter for DirectSubstrate {
    fn managed_by(&self) -> &'static str {
        "space"
    }

    fn capabilities(&self) -> SubstrateCapabilities {
        SubstrateCapabilities::DIRECT
    }

    fn pool_eligible_for_request(
        &self,
        boot_mode: CreateBootMode,
        backend: &str,
        wait_until_ready: bool,
        disk_mib: Option<u32>,
        file_changes_enabled: bool,
        implicit_wake_enabled: bool,
    ) -> bool {
        super::default_pool_eligible_for_request(
            boot_mode,
            backend,
            wait_until_ready,
            disk_mib,
            file_changes_enabled,
            implicit_wake_enabled,
        )
    }

    /// The direct substrate adopts pool sandboxes via `AdoptPoolSandbox`, which
    /// carries `env` and has sbx-mgr update `process_env` before resume, so
    /// env-bearing creates stay pool-eligible here.
    fn pool_supports_request_env(&self) -> bool {
        true
    }

    async fn claim_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        if request.vcpu.is_some() || request.memory_mib.is_some() {
            return Ok(PooledExecutionClaimAttempt::missed(None));
        }
        // Direct pools have no encryptable partition and AdoptPoolSandbox
        // cannot apply encryption to a running plaintext member; miss to the
        // cold path, mirroring the Kubernetes partition filter.
        if request.volume_encryption {
            return Ok(PooledExecutionClaimAttempt::missed(None));
        }

        let adopt_policy = override_request_policy(request.network_policy_override);
        let candidates = self
            .pooled_worker_candidates(request.template, request.content_hash, request.runtime)
            .await?;
        let mut attempted_pool_shape = None;
        for candidate in candidates {
            let pool_shape = match Self::pool_candidate_shape(&candidate) {
                Ok(pool_shape) => pool_shape,
                Err(error) => {
                    tracing::warn!(
                        node_id = %candidate.id,
                        content_hash = %candidate.content_hash,
                        %error,
                        "skipping direct pool candidate with incomplete shape inventory"
                    );
                    continue;
                }
            };
            let (vcpu, memory_mib) = match pool_shape {
                Some(pool_shape) => (Some(pool_shape.vcpu), Some(pool_shape.memory_mib)),
                None => (None, None),
            };
            attempted_pool_shape = pool_shape;
            let endpoint =
                Self::endpoint(&candidate.node_api_endpoint).map_err(ControllerError::from)?;
            let mut runtime = match connect_worker(endpoint.clone()).await {
                Ok(client) => client,
                Err(error) => {
                    tracing::warn!(
                        node_id = %candidate.id,
                        %error,
                        "direct pool candidate is not reachable; trying another worker"
                    );
                    continue;
                }
            };

            if let Err(error) = runtime
                .require_activation_policy_support(DIRECT_NODE_RPC_TIMEOUT)
                .await
            {
                tracing::warn!(node_id = %candidate.id, %error,
                    "direct pool candidate cannot install activation policy; trying another worker");
                continue;
            }
            let mut client = runtime.internal_client();
            let response = client
                .adopt_pool_sandbox(pplx_space_sbx_mgr_client::proto::AdoptPoolSandboxRequest {
                    template: request.template.to_string(),
                    owner: request.actor.sandbox_owner.to_string(),
                    env: request
                        .env
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                    auto_lifecycle: Some(policy_to_proto(request.auto_lifecycle)),
                    network_policy: adopt_policy.clone(),
                    network_policy_generation: request.network_policy_generation,
                    network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
                    auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
                    content_hash: request.content_hash.map(ToOwned::to_owned),
                    template_network_policy: Some(network_policy_to_proto(
                        request.template_network_policy,
                    )),
                    template_network_policy_generation: request.template_network_policy_generation,
                    template_network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
                    route_identity: None,
                })
                .instrument(tracing::info_span!(
                    "substrate.direct.pool.adopt",
                    template = request.template,
                    content_hash = request.content_hash.unwrap_or(""),
                    node_id = %candidate.id,
                ))
                .await;

            match response {
                Ok(response) => {
                    let response = response.into_inner();
                    let sandbox_id = response.sandbox_id;
                    let sandbox_uuid = match sandbox_id.parse::<SandboxId>() {
                        Ok(sandbox_uuid) => sandbox_uuid,
                        Err(error) => {
                            Self::delete_adopted_pool_sandbox_best_effort(
                                endpoint,
                                candidate.id,
                                &sandbox_id,
                                "adopt_pool_sandbox returned invalid sandbox id",
                            )
                            .await;
                            return Err(ControllerError::internal(format!(
                                "direct pool adopted invalid sandbox id {sandbox_id}: {error}"
                            )));
                        }
                    };
                    let route_identity = match response
                        .route_identity
                        .map(|identity| parse_direct_route_identity(sandbox_uuid, identity))
                        .transpose()
                    {
                        Ok(None) => {
                            Self::delete_adopted_pool_sandbox_best_effort(
                                endpoint,
                                candidate.id,
                                &sandbox_id,
                                "pool adoption omitted its route identity",
                            )
                            .await;
                            return Err(ControllerError::internal(
                                "pool adoption omitted its route identity",
                            ));
                        }
                        Ok(Some(identity)) if identity.current_node_id() == candidate.id => {
                            identity
                        }
                        Ok(Some(identity)) => {
                            Self::delete_adopted_pool_sandbox_best_effort(
                                endpoint,
                                candidate.id,
                                &sandbox_id,
                                "pool adoption route receipt targeted another node",
                            )
                            .await;
                            return Err(ControllerError::internal(format!(
                                "pool adoption route identity targets node {}, expected {}",
                                identity.current_node_id(),
                                candidate.id
                            )));
                        }
                        Err(error) => {
                            Self::delete_adopted_pool_sandbox_best_effort(
                                endpoint,
                                candidate.id,
                                &sandbox_id,
                                "pool adoption route receipt was invalid",
                            )
                            .await;
                            return Err(ControllerError::internal(error.to_string()));
                        }
                    };
                    let claim = PooledExecutionClaim {
                        claim_id: Uuid::now_v7(),
                        pool_slot_id: None,
                        sandbox_id: sandbox_uuid,
                        content_hash: candidate.content_hash,
                        pod_name: sandbox_id.clone(),
                        pod_uid: None,
                        execution_generation: route_identity
                            .execution_generation()
                            .get()
                            .cast_unsigned(),
                        pod_ip: String::new(),
                        vcpu,
                        memory_mib,
                        disk_mib: None,
                        disk_io_limits: pplx_space_common_core::types::DiskIoLimits::default(),
                        active_node_id: Some(candidate.id),
                        route_identity: Some(route_identity),
                    };
                    return Ok(match pool_shape {
                        Some(pool_shape) => PooledExecutionClaimAttempt::claimed(claim, pool_shape),
                        None => PooledExecutionClaimAttempt {
                            claim: Some(claim),
                            pool_shape: None,
                            refusal: None,
                        },
                    });
                }
                Err(error) if matches!(error.code(), Code::ResourceExhausted | Code::NotFound) => {
                    tracing::debug!(
                        node_id = %candidate.id,
                        template = request.template,
                        %error,
                        "direct pool candidate had stale inventory; trying another worker"
                    );
                }
                Err(error) if should_try_next_worker_candidate(&error) => {
                    tracing::warn!(
                        node_id = %candidate.id,
                        template = request.template,
                        code = ?error.code(),
                        %error,
                        "direct pool candidate adoption failed; trying another worker"
                    );
                }
                Err(error) => {
                    return Err(ControllerError::internal(format!(
                        "direct pool adoption failed: {error}"
                    )));
                }
            }
        }

        Ok(PooledExecutionClaimAttempt::missed(attempted_pool_shape))
    }

    async fn persist_pooled_execution_claim(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        _request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        let Some(identity) = claim.route_identity.as_ref() else {
            return Ok(());
        };
        let persisted = persist_monotonic_direct_route_identity(tx, identity)
            .await
            .map_err(|error| {
                db_contention_error("failed to persist pooled direct route identity", &error)
            })?;
        if !persisted {
            return Err(ControllerError::internal(format!(
                "refused stale pooled direct route identity for sandbox {}",
                claim.sandbox_id
            )));
        }
        Ok(())
    }

    async fn prepare_pooled_execution(
        &self,
        _metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        _request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
        wait_until_ready: bool,
        timeout: Duration,
    ) -> Result<(), ControllerError> {
        let sandbox_id = claim.sandbox_id;
        let (node, _, mut client) = self
            .worker_client_for_sandbox(sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        client.require_activation_policy_support(timeout).await?;
        let generation = claim
            .route_identity
            .as_ref()
            .map(|identity| identity.execution_generation())
            .ok_or_else(|| {
                ControllerError::service_unavailable("pooled resume has no execution generation")
            })?;
        let response = client
            .resume_sandbox(pplx_space_sbx_mgr_client::proto::ResumeRequest {
                activation_auto_lifecycle: Some(policy.into()),
                expected_execution_generation: Some(generation.get()),
                sandbox_id: sandbox_id.to_string(),
                blocking: wait_until_ready,
                ready_timeout_secs: Some(timeout.as_secs() as u32),
            })
            .await
            .map_err(|err| ControllerError::internal(format!("node-api resume error: {err}")))?;
        self.persist_route_identity_receipt(
            sandbox_id,
            node.id,
            &sandbox_id.to_string(),
            response.into_inner().route_identity,
            DirectRouteIdentityOperation::Resume,
        )
        .await
        .map_err(ControllerError::from)?;
        Ok(())
    }

    async fn rollback_pooled_execution_claim(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        let Some(node_id) = claim.active_node_id else {
            return Ok(());
        };
        let node = match self.direct_node_by_id(node_id).await {
            Ok(node) => node,
            Err(SubstrateError::Unavailable { message }) => {
                tracing::warn!(
                    node_id = %node_id,
                    sandbox_id = %claim.sandbox_id,
                    %message,
                    "direct pool rollback could not resolve adopted worker; treating as best-effort no-op"
                );
                return Ok(());
            }
            Err(error) => return Err(ControllerError::from(error)),
        };
        let (_, mut client) = self
            .worker_client_for_node(&node)
            .await
            .map_err(ControllerError::from)?;
        if let Err(err) = client
            .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: claim.sandbox_id.to_string(),
                template_filter: None,
                blocking: false,
            })
            .await
            && !is_idempotent_pool_rollback_delete(&err)
        {
            return Err(ControllerError::internal(format!(
                "pool claim leaked on node {node_id}: node-api delete adopted pool sandbox failed: {err}"
            )));
        }
        self.clear_direct_route_identity(claim.sandbox_id)
            .await
            .map_err(ControllerError::from)?;
        Ok(())
    }

    async fn create_execution<'a>(
        &'a self,
        request: CreateExecutionRequest<'a>,
    ) -> Result<CreateExecutionOutcome, SubstrateError> {
        if let Some(node_pool) = request.node_pool {
            return Err(SubstrateError::Unsupported {
                message: format!(
                    "node-pool placement {node_pool} requires the Kubernetes substrate"
                ),
            });
        }
        ensure_direct_create_supported(request.restore_from.is_some())?;
        let one_shot_create = use_one_shot_create(
            request.wait_until_ready,
            request.template_ref.boot_mode,
            request.resources,
        );
        let sandbox_id = request.sandbox_id;
        let cached_candidates = self
            .cached_worker_create_candidates(
                request.template_ref.template.as_str(),
                request.template_ref.content_hash,
                request.template_ref.runtime,
                request.template_ref.boot_mode,
            )
            .await?;
        let mut failures = Vec::new();
        let mut selected = self
            .try_create_on_worker_candidates(
                &request,
                one_shot_create,
                cached_candidates,
                WorkerCandidateSource::CachedTemplate,
                &mut failures,
            )
            .await?;

        if selected.is_none()
            && pull_on_create_allowed(
                request.template_ref.content_hash,
                request.template_ref.boot_mode,
            )
        {
            tracing::info!(
                sandbox_id = %sandbox_id,
                template = %request.template_ref.template,
                content_hash = ?request.template_ref.content_hash,
                "no cached direct worker accepted create; trying pull-on-create candidates"
            );
            let pull_candidates = self
                .pull_on_create_worker_candidates(request.template_ref.runtime)
                .await?;
            selected = self
                .try_create_on_worker_candidates(
                    &request,
                    one_shot_create,
                    pull_candidates,
                    WorkerCandidateSource::PullOnCreate,
                    &mut failures,
                )
                .await?;
        }

        let Some((node, endpoint, mut client, create_response)) = selected else {
            let failure_summary = if failures.is_empty() {
                "no eligible candidates".to_string()
            } else {
                failures.join("; ")
            };
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "no direct worker node-api nodes accepted create for template {}{}: {}",
                    request.template_ref.template,
                    request
                        .template_ref
                        .content_hash
                        .map(|hash| format!("@{hash}"))
                        .unwrap_or_default(),
                    failure_summary
                ),
            });
        };

        let boot_mode_used =
            BootMode::try_from(create_response.boot_mode_used).unwrap_or(BootMode::Unspecified);

        if one_shot_create {
            let route_identity = self
                .persist_created_route_identity_or_cleanup(
                    &mut client,
                    sandbox_id,
                    node.id,
                    &create_response.sandbox_id,
                    create_response.route_identity.clone(),
                    DirectRouteIdentityOperation::Create,
                )
                .await?;
            let execution_generation = receipt_execution_generation(route_identity.as_ref());
            return Ok(CreateExecutionOutcome {
                lease_completion: None,
                sandbox_id,
                binding: Some(ExecutionBinding {
                    execution_object_id: sandbox_id.to_string(),
                    runtime_endpoint: endpoint,
                    execution_generation,
                }),
                initial_lifecycle: "started",
                initial_placement: "on_node",
                active_node_id: Some(node.id),
                command_ready: true,
            });
        }

        if boot_mode_used == BootMode::Pool {
            let resume_result = client
                .resume_sandbox(pplx_space_sbx_mgr_client::proto::ResumeRequest {
                    activation_auto_lifecycle: None,
                    expected_execution_generation: None,
                    sandbox_id: sandbox_id.to_string(),
                    blocking: request.wait_until_ready,
                    ready_timeout_secs: None,
                })
                .await;
            let resume_response = match resume_result {
                Ok(response) => response.into_inner(),
                Err(err) => {
                    if let Err(delete_err) = client
                        .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                            sandbox_id: sandbox_id.to_string(),
                            template_filter: None,
                            blocking: false,
                        })
                        .await
                    {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            error = %delete_err,
                            "best-effort sandbox delete RPC failed after pooled resume failure"
                        );
                    }
                    return Err(SubstrateError::Failed {
                        message: format!("node-api resume error: {err}"),
                    });
                }
            };
            let route_identity = self
                .persist_created_route_identity_or_cleanup(
                    &mut client,
                    sandbox_id,
                    node.id,
                    &sandbox_id.to_string(),
                    resume_response.route_identity,
                    DirectRouteIdentityOperation::Resume,
                )
                .await?;
            let execution_generation = receipt_execution_generation(route_identity.as_ref());

            return Ok(CreateExecutionOutcome {
                lease_completion: None,
                sandbox_id,
                binding: Some(ExecutionBinding {
                    execution_object_id: sandbox_id.to_string(),
                    runtime_endpoint: endpoint,
                    execution_generation,
                }),
                initial_lifecycle: "started",
                initial_placement: "on_node",
                active_node_id: Some(node.id),
                command_ready: true,
            });
        }

        let from_warm = boot_mode_used == BootMode::BaseSnapshot;
        let node_id = node.id;
        let start_route_identity_result = if create_response.route_identity.is_some() {
            self.persist_created_route_identity_or_cleanup(
                &mut client,
                sandbox_id,
                node_id,
                &create_response.sandbox_id,
                create_response.route_identity,
                DirectRouteIdentityOperation::Create,
            )
            .await?;
            self.reserve_direct_activation_route_identity(sandbox_id, node_id)
                .await
        } else {
            self.initialize_direct_activation_route_identity(sandbox_id, node_id)
                .await
        };
        let start_route_identity = match start_route_identity_result {
            Ok(identity) => identity,
            Err(error) => {
                let removal_confirmed = match client
                    .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                        sandbox_id: sandbox_id.to_string(),
                        template_filter: None,
                        blocking: false,
                    })
                    .await
                {
                    Ok(_) => true,
                    // Already gone, or already being deleted — nothing is left
                    // for a retry to duplicate.
                    Err(delete_error) if is_idempotent_pool_rollback_delete(&delete_error) => true,
                    Err(delete_error) => {
                        tracing::warn!(
                            sandbox_id = %sandbox_id,
                            %error,
                            %delete_error,
                            "best-effort sandbox delete RPC failed after route identity preparation failure"
                        );
                        false
                    }
                };
                // Route-identity preparation reads and writes this service's
                // database (`recover_direct_route_provenance` classifies
                // contention), so `error` can be a retryable 503. The node has
                // already created the sandbox by this point, so when the
                // compensating delete did not confirm removal the caller must
                // not be told to retry: `create` mints a fresh `SandboxId`, so
                // the retry leaves this one live under an id it never learns.
                if removal_confirmed {
                    return Err(error);
                }
                return Err(error.demoted_after_side_effect());
            }
        };
        let start_result = client
            .start_sandbox(pplx_space_sbx_mgr_client::proto::StartRequest {
                activation_auto_lifecycle: None,
                operation_activation_attempt_id: None,
                restore_artifact_as_new: None,
                expected_local_artifact_id: None,
                sandbox_id: sandbox_id.to_string(),
                vcpu: request.resources.vcpu,
                memory_mib: request.resources.memory_mib,
                checkpoint: None,
                from_warm,
                blocking: request.wait_until_ready,
                netns_path: None,
                ready_timeout_secs: None,
                target_state: 0,
                suspend_restore_storage_uri: None,
                suspend_restore_source_node: None,
                route_identity: Some(start_route_identity),
            })
            .await;
        let start_response = match start_result {
            Ok(response) => response.into_inner(),
            Err(err) => {
                if let Err(delete_err) = client
                    .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                        sandbox_id: sandbox_id.to_string(),
                        template_filter: None,
                        blocking: false,
                    })
                    .await
                {
                    tracing::warn!(
                        sandbox_id = %sandbox_id,
                        error = %delete_err,
                        "best-effort sandbox delete RPC failed after start failure"
                    );
                }
                return Err(direct_create_start_status_error(err));
            }
        };
        let route_identity = self
            .persist_created_route_identity_or_cleanup(
                &mut client,
                sandbox_id,
                node_id,
                &start_response.sandbox_id,
                start_response.route_identity,
                DirectRouteIdentityOperation::Start,
            )
            .await?;
        let execution_generation = receipt_execution_generation(route_identity.as_ref());

        Ok(CreateExecutionOutcome {
            lease_completion: None,
            sandbox_id,
            binding: Some(ExecutionBinding {
                execution_object_id: sandbox_id.to_string(),
                runtime_endpoint: endpoint,
                execution_generation,
            }),
            initial_lifecycle: "started",
            initial_placement: "on_node",
            active_node_id: Some(node.id),
            command_ready: true,
        })
    }

    async fn delete_execution(&self, sandbox_id: SandboxId) -> Result<(), SubstrateError> {
        let (_, _, mut client) = self.worker_client_for_sandbox(sandbox_id).await?;

        // Delete must reach sbx-mgr's suspend-publication fence directly. A
        // pre-delete Stop would change an in-flight `Suspending` row to
        // `Stopping`, allowing the delete to erase the publisher.
        client
            .delete_sandbox(pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.to_string(),
                template_filter: None,
                blocking: true,
            })
            .await
            .map_err(|err| direct_delete_error(sandbox_id, err))?;
        self.clear_direct_route_identity(sandbox_id).await?;

        Ok(())
    }

    /// The direct substrate has no pod object, so a
    /// [`PodIdentityFence`](super::PodIdentityFence) has nothing to fence:
    /// this path addresses the node's sbx-mgr directly and never deletes a
    /// Kubernetes pod, which is the destructive step the fence exists to
    /// withhold. A fenced request is therefore accepted and retired, not
    /// rejected — a substrate-agnostic caller (the transient-lifecycle
    /// sweeper) must not have to know which substrate it is talking to. The
    /// argument is destructured to `_fence` deliberately: it is ignored
    /// because it cannot apply here, not because it was overlooked.
    async fn retire_execution(
        &self,
        request: RetireExecutionRequest,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        let RetireExecutionRequest {
            sandbox_id,
            fence: _fence,
        } = request;
        let (_, _, mut client) = self.worker_client_for_sandbox(sandbox_id).await?;

        // Unlike `delete_execution`, the stop is the whole operation here, so
        // its outcome must propagate to the caller. Nothing retries a failed
        // Direct retire: the failed-cleanup pass skips every substrate but Kubernetes.
        // No stopped-disk backup follows the stop, unlike the Kubernetes
        // teardown, whose enqueue failure relies on that retry.
        client
            .stop_sandbox(pplx_space_sbx_mgr_client::proto::StopRequest {
                sandbox_id: sandbox_id.to_string(),
                template_filter: None,
                blocking: true,
                immediate_kill: false,
            })
            .await
            .map_err(|err| SubstrateError::Failed {
                message: format!("node-api stop error: {err}"),
            })?;
        self.clear_direct_route_identity(sandbox_id).await?;

        Ok(RetireExecutionOutcome::Retired)
    }

    async fn start_execution(
        &self,
        _metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        // This resident start path does not claim the portable-restore lease,
        // so its deferred restore charge is spent at entry.
        charge.charge_restore_claim().await?;
        let (node, endpoint, mut client) = self.worker_client_for_sandbox(sandbox_id).await?;
        let route_identity = self
            .reserve_direct_activation_route_identity(sandbox_id, node.id)
            .await?;
        client
            .require_activation_policy_support(options.ready_timeout)
            .await
            .map_err(|error| SubstrateError::Unavailable {
                message: error.to_string(),
            })?;
        let activation_auto_lifecycle = Some(self.activation_auto_lifecycle(sandbox_id).await?);
        let vcpu = self.activation_vcpu(sandbox_id).await?;
        let response = client
            .start_sandbox(pplx_space_sbx_mgr_client::proto::StartRequest {
                activation_auto_lifecycle,
                operation_activation_attempt_id: None,
                restore_artifact_as_new: None,
                expected_local_artifact_id: None,
                sandbox_id: sandbox_id.to_string(),
                vcpu,
                memory_mib: None,
                checkpoint: None,
                from_warm: false,
                blocking: options.blocking,
                netns_path: None,
                ready_timeout_secs: Some(options.ready_timeout.as_secs() as u32),
                target_state: 0,
                suspend_restore_storage_uri: None,
                suspend_restore_source_node: None,
                route_identity: Some(route_identity),
            })
            .await
            .map_err(|err| SubstrateError::Failed {
                message: format!("node-api start error: {err}"),
            })?;
        let route_identity = self
            .persist_start_route_identity(sandbox_id, node.id, response.get_ref())
            .await?;
        let execution_generation = receipt_execution_generation(route_identity.as_ref());

        Ok(ExecutionActivation::owned(ExecutionBinding {
            execution_object_id: sandbox_id.to_string(),
            runtime_endpoint: endpoint,
            execution_generation,
        }))
    }

    async fn restore_execution(
        &self,
        _metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ActivationCompletion, SubstrateError> {
        // Direct restore claims its substrate lease below; admission remains
        // charged at entry because the Direct path does not support joining a
        // prior owner's completion.
        charge.charge_restore_claim().await?;
        self.restore_execution_with_candidates(sandbox_id, options)
            .await?;
        Ok(ActivationCompletion::Owned)
    }
    async fn suspend_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<super::PublishedLocalArtifact>, SubstrateError> {
        let (_, _, mut client) = self.worker_client_for_sandbox(sandbox_id).await?;
        let response = client
            .suspend_sandbox(SuspendRequest {
                sandbox_id: sandbox_id.to_string(),
                blocking: true,
                // Unspecified → Full at the handler. The substrate
                // never opts into btrfs-send; per-request codec choice
                // is exposed only via the on-node sbx-mgr CLI today.
                format: 0,
            })
            .await
            .map_err(super::suspend_rpc_error)?
            .into_inner();
        self.clear_direct_route_identity(sandbox_id).await?;
        let published_artifact = super::PublishedLocalArtifact::from_committed_suspend_response(
            response, sandbox_id, "direct",
        );
        super::persist_optional_current_local_artifact(
            &self.pool,
            sandbox_id,
            published_artifact,
            ArtifactOrigin::Suspend,
        )
        .await?;
        Ok(published_artifact)
    }

    async fn wake_execution(
        &self,
        _metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        wake_activity: Option<ExecutionActivityKind>,
        charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        charge.charge_restore_claim().await?;
        let sandbox_id = sandbox.id;
        let node = self.sandbox_node(sandbox_id).await?;
        let endpoint = Self::endpoint(&node.node_api_endpoint)?;
        let mut observer = connect_worker(endpoint.clone()).await.map_err(|error| {
            SubstrateError::Unavailable {
                message: format!(
                    "worker node {} API is unavailable at {endpoint:?}: {error}",
                    node.id
                ),
            }
        })?;
        observer
            .require_activation_policy_support(timeout)
            .await
            .map_err(|error| SubstrateError::Unavailable {
                message: error.to_string(),
            })?;
        let mut client = observer.internal_client();
        // Missing published identity does not prove the resident is absent.
        // Keep the generation through transitional states: a Pausing or
        // Checkpointing resident may settle to Paused before Wake admits it.
        // This fences only resident resume.
        let observed = observer
            .sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
                sandbox_ids: vec![sandbox_id.to_string()],
            })
            .await;
        let resident = match observed {
            Ok(response) => response
                .into_inner()
                .sandboxes
                .into_iter()
                .find(|row| row.sandbox_id == sandbox_id.to_string()),
            Err(error) if error.code() == Code::NotFound => None,
            Err(error) => {
                return Err(SubstrateError::Unavailable {
                    message: format!("cannot observe resident execution before wake: {error}"),
                });
            }
        };
        let expected_execution_generation =
            resident.as_ref().and_then(|row| row.execution_generation);
        // A dormant resident wakes by restore or start, which needs a reserved
        // identity, so reserve it before the first attempt rather than after a
        // refusal. The answers below still cover a resident that changed state
        // in between.
        let initial_identity = if resident.as_ref().is_some_and(resident_is_dormant) {
            Some(
                self.reserve_direct_activation_route_identity(sandbox_id, node.id)
                    .await?,
            )
        } else {
            None
        };
        let initial_wake = client
            .wake_sandbox(
                self.wake_request_with_policy(
                    sandbox,
                    initial_identity,
                    expected_execution_generation,
                    wake_activity,
                )
                .await?,
            )
            .await;
        let wake = match initial_wake {
            Err(status)
                if pplx_space_sbx_mgr_client::is_authoritative_route_identity_required(&status) =>
            {
                let route_identity = self
                    .reserve_direct_activation_route_identity(sandbox_id, node.id)
                    .await?;
                client
                    .wake_sandbox(
                        self.wake_request_with_policy(
                            sandbox,
                            Some(route_identity),
                            expected_execution_generation,
                            wake_activity,
                        )
                        .await?,
                    )
                    .await
            }
            Ok(response) if wake_response_requires_activation_identity(response.get_ref()) => {
                // Compatibility mode can allow an identity-free activation. Hydrate
                // that now-active execution with the control-plane identity before
                // returning it to callers or persisting its receipt.
                let route_identity = self
                    .reserve_direct_activation_route_identity(sandbox_id, node.id)
                    .await?;
                client
                    .wake_sandbox(
                        self.wake_request_with_policy(
                            sandbox,
                            Some(route_identity),
                            expected_execution_generation,
                            wake_activity,
                        )
                        .await?,
                    )
                    .await
            }
            result => result,
        };
        let response = match wake {
            Ok(response) => response,
            Err(status)
                if wake_requires_restore(&status)
                    && wake_lifecycle_allows_restore_fallback(&sandbox.lifecycle) =>
            {
                let restored = self
                    .restore_execution_with_candidates(
                        sandbox_id,
                        ActivationOptions {
                            blocking: true,
                            ready_timeout: timeout,
                        },
                    )
                    .await?;
                return Ok(ExecutionActivation::owned(ExecutionBinding {
                    execution_object_id: sandbox.id.to_string(),
                    runtime_endpoint: restored.endpoint,
                    execution_generation: restored.execution_generation,
                }));
            }
            // The worker's effective policy refused an implicit wake. That is
            // a terminal policy answer, not a transient runtime failure.
            Err(status) if wake_activity.is_some() && status.code() == Code::FailedPrecondition => {
                return Err(SubstrateError::Conflict {
                    message: status.message().to_string(),
                });
            }
            Err(error) => {
                return Err(SubstrateError::Failed {
                    message: format!("node-api wake error: {error}"),
                });
            }
        };
        let response = response.into_inner();
        let route_identity = self
            .persist_wake_route_identity(sandbox_id, node.id, &response)
            .await?;
        let execution_generation = receipt_execution_generation(route_identity.as_ref());

        Ok(ExecutionActivation::owned(ExecutionBinding {
            execution_object_id: sandbox.id.to_string(),
            runtime_endpoint: endpoint,
            execution_generation,
        }))
    }

    async fn runtime_client(
        &self,
        sandbox_id: SandboxId,
        // Direct mode does not recreate Kubernetes pods, so there is no
        // recovery-path activation to charge here.
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        self.node_client(sandbox_id).await
    }

    async fn reachable_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        self.node_client(sandbox_id).await.map(Some)
    }

    async fn probe_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        self.node_client(sandbox_id).await.map(Some)
    }

    async fn access_runtime_client(
        &self,
        metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        activity: ExecutionActivityKind,
        timeout: Duration,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        if matches!(
            sandbox.lifecycle.parse::<SandboxLifecycle>(),
            Ok(SandboxLifecycle::Suspended | SandboxLifecycle::Restoring)
        ) {
            if !activity.allows_implicit_wake(&sandbox.auto_lifecycle.0) {
                return Err(ControllerError::conflict(
                    "automatic access wake is disabled",
                ));
            }
            // The access entrypoint already charged admission. A placed
            // sandbox goes through the worker's implicit wake so its
            // effective policy, including a local override, is enforced under
            // the wake lock; the worker asks for a restore only when its local
            // row is missing. An unplaced sandbox has no worker to consult and
            // uses the direct restore lease and completion owner.
            if sandbox.active_node_id.is_some() {
                self.wake_execution(
                    metric_identity,
                    sandbox,
                    timeout,
                    Some(activity),
                    crate::controllers::substrate::RecoveryCharge::Exempt,
                )
                .await
                .map_err(ControllerError::from)?;
            } else {
                self.restore_execution_with_candidates(
                    sandbox.id,
                    ActivationOptions {
                        blocking: true,
                        ready_timeout: timeout,
                    },
                )
                .await
                .map_err(ControllerError::from)?;
            }
        }
        self.node_client(sandbox.id).await
    }

    async fn sandbox_ip(&self, _sandbox_id: SandboxId) -> Result<Option<String>, ControllerError> {
        Ok(None)
    }

    async fn access_sandbox_ip(
        &self,
        _metric_identity: pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity,
        _sandbox: &Sandbox,
        _timeout: Duration,
        _charge: crate::controllers::substrate::RecoveryCharge,
    ) -> Result<Option<String>, ControllerError> {
        Ok(None)
    }

    async fn builder_client(&self) -> Result<BuilderClient, ControllerError> {
        self.builder_node_client().await
    }

    async fn builder_client_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<BuilderClient, ControllerError> {
        let node = sqlx::query_as::<_, DirectNode>(sql!(
            "SELECT n.id, nd.node_api_endpoint
             FROM nodes n
             JOIN node_direct nd ON nd.node_id = n.id
             WHERE n.id = $1
               AND n.managed_by = $2
               AND n.role = 'builder'
               AND nd.deleted_at IS NULL
             LIMIT 1"
        ))
        .bind(node_id.as_uuid())
        .bind(SubstrateKind::Space)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            db_contention_error(&format!("failed to load builder node {node_id}"), &error)
        })?
        .ok_or_else(|| {
            ControllerError::service_unavailable(format!(
                "builder node {node_id} is not registered"
            ))
        })?;
        let endpoint = Self::endpoint(&node.node_api_endpoint).map_err(ControllerError::from)?;
        connect_builder(endpoint).await.map_err(|error| {
            ControllerError::service_unavailable(format!(
                "builder node {node_id} is not reachable: {error}"
            ))
        })
    }

    async fn resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        target
            .direct_endpoint
            .map(str::to_string)
            .ok_or_else(|| SubstrateError::NotFound {
                message: "direct node has no node_api_endpoint".to_string(),
            })
    }

    async fn select_ready_worker_node_api_target(
        &self,
    ) -> Result<OwnedNodeApiTarget, SubstrateError> {
        let node = self
            .random_ready_nodes(SpaceNodeRole::Worker.as_str())
            .await?
            .into_iter()
            .next()
            .expect("random_ready_nodes rejects an empty result");
        Ok(OwnedNodeApiTarget {
            managed_by: NodeManagedBy::Space,
            role: SpaceNodeRole::Worker,
            direct_endpoint: Some(node.node_api_endpoint),
            substrate_node_id: None,
        })
    }

    async fn list_inventory_nodes(
        &self,
        _role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        // Direct mode persists every node row in `nodes` + `node_direct`; the
        // database is the source of truth, so the route should fall back to
        // its SQL query.
        Ok(None)
    }
}

fn ensure_direct_create_supported(fork_disk_restore: bool) -> Result<(), SubstrateError> {
    if fork_disk_restore {
        return Err(SubstrateError::Unsupported {
            message: "restoring a durable fork disk as a new sandbox is unsupported on the direct substrate"
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod cluster_admission_tests;
#[cfg(test)]
mod tests;
