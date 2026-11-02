pub mod direct;
#[cfg(feature = "kubernetes")]
pub mod kubernetes;
mod runtime_rpc;
mod worker_channels;

pub(crate) use runtime_rpc::RuntimeRpcError;

#[cfg(feature = "kubernetes")]
pub use kubernetes::ReflectorReady;

#[cfg(not(feature = "kubernetes"))]
mod readiness_stub {
    /// Substrate readiness handle used when no substrate has its own
    /// readiness cache wired in. Always reports ready so the direct
    /// substrate (local dev) does not need a readiness probe dance.
    #[derive(Clone, Default)]
    pub struct ReflectorReady;
    impl ReflectorReady {
        pub fn always_ready() -> Self {
            Self
        }
        pub fn is_ready(&self) -> bool {
            true
        }
    }
}

#[cfg(not(feature = "kubernetes"))]
pub use readiness_stub::ReflectorReady;

use pplx_space_common_core::sql;
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, AutoLifecyclePolicy, BackupAttemptId, BackupSource, Email,
    LocalArtifactId, NodeAddress, NodeId, NodeManagedBy, NodePoolName, NodeRole, OrgId, PodName,
    PodResourceVersion, PodUid, SandboxActivationAttemptId, SandboxId, SandboxLifecycle,
    SandboxOwner, SandboxRouteIdentity, TemplateName, UserId,
};
use pplx_space_common_svc::auth::SpaceBearerInterceptor;
use pplx_space_common_svc::bearer_token::SpaceBearerToken;
use pplx_space_common_svc::bootstrap::{GrpcClientTrace, wrap_grpc_client_channel};
use pplx_space_common_svc::space_api_route::wait_ready_lifecycle_request;
use pplx_space_metrics::sandbox::api::SandboxApiMetricIdentity;
use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_net_common::proto::NetworkPolicyV1 as NetworkPolicyProto;

/// Convert a request override at the V1-only sandbox-manager boundary.
pub(super) fn override_request_policy(
    policy: Option<&NetworkPolicyV1>,
) -> Option<NetworkPolicyProto> {
    policy.map(NetworkPolicyProto::from)
}
use pplx_space_node_api_client::{
    NodeApiClient, NodeApiEndpointConfig, WorkerApiClient as RetryingWorkerApiClient,
    proto::{
        builder_api_client::BuilderApiClient, worker_api_client::WorkerApiClient,
        worker_api_internal_client::WorkerApiInternalClient,
    },
};
use pplx_space_public_api::OperationId;
use pplx_space_sandbox_portability::deleting_snapshot_matches_artifact;
use pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest;
use time::OffsetDateTime;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use super::{ControllerError, ControllerRetryAfterCause};
use crate::models::{ClusterPoolConfigRequest, ClusterPoolConfigResponse, Sandbox};

use sqlx::{PgPool, Postgres, Transaction};

mod execution_fence;
mod local_artifact;
pub(crate) mod native_access;
mod node_api_client;
mod pool_claims;
mod recovery_charge;
pub mod runtime_upgrade;
mod sandbox_state_probe;

pub use execution_fence::*;
pub use local_artifact::*;
pub use node_api_client::*;
pub use pool_claims::*;
pub use recovery_charge::*;
pub use sandbox_state_probe::*;

/// Outcome of a non-recovering guest command-readiness probe.
///
/// A slow create that exceeded the API blocking budget is preserved as a
/// `started` row carrying `command_ready_pending = true` (see the create
/// preserve path). The access path clears the marker on the first successful
/// guest call. If no client ever retries, the marker sticks and the row sits
/// `started` forever — either a healthy-but-unaccessed guest, or a genuinely
/// wedged one. The cluster-controller's reaper distinguishes them with this
/// probe before deciding to heal (clear the marker), reap (flip to `failed`),
/// or defer.
///
/// The three outcomes deliberately separate *positive wedge evidence* from
/// *inability to reach the guest*: only a guest we actually talked to that
/// failed to report ready is reaped. A transport-level failure is
/// [`Unreachable`](Self::Unreachable) — never reaped on the strength of one
/// probe, so a transient Kube-API / pod-proxy / node-mgr blip can't destroy a
/// healthy-but-abandoned sandbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandReadyProbe {
    /// The guest answered `SpacedWaitReady` within the probe deadline — the
    /// command surface is live and the marker was a stale leftover. Heal it.
    Ready,
    /// We reached the guest's runtime but it did not report command-ready
    /// within the deadline (`SpacedWaitReady` errored or the probe timed out).
    /// This is positive evidence the guest is wedged — the transport works, the
    /// command surface never came up. Reap it.
    Wedged,
    /// We could not reach the guest's runtime at all (no resolvable execution,
    /// or a controller/RPC error connecting), or `SpacedWaitReady` returned a
    /// transient transport code. Inconclusive — could be a wedged pod *or* a
    /// transient control-plane/network blip. Defer: leave the row alone and
    /// re-probe on the next tick rather than reap on one bad sample.
    Unreachable,
}

/// Whether a `SpacedWaitReady` error code is a transient transport/overload
/// signal that should *defer* the reaper (re-probe next tick) rather than count
/// as positive wedge evidence. Mirrors the create-path readiness poll, which
/// treats `Unavailable` as retryable. `ResourceExhausted` / `Aborted` are
/// likewise "try again later" at the infra level. Every other code (including
/// `DeadlineExceeded` and `FailedPrecondition` — the guest reached but not
/// ready) is left to the caller as wedge evidence.
fn probe_status_is_transient(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable | tonic::Code::ResourceExhausted | tonic::Code::Aborted
    )
}

/// `Retry-After` hint attached to the structured response returned when a wait
/// exhausts the budget. The node keeps converging in the background, so a
/// short delay is enough for a follow-up to reach a ready sandbox.
pub(crate) const GATEWAY_RETRY_AFTER: Duration = Duration::from_secs(5);

/// `Retry-After` for a request that lost a race for a database lock.
///
/// Both extremes are wrong here. The 5s [`GATEWAY_RETRY_AFTER`] is written for
/// a sandbox converging on a node and is ~20x the sub-second bound the request
/// actually hit. But omitting the hint is worse than it looks: before the
/// bound, a contending caller was *paced by the lock wait itself*, and an
/// immediate retryable 503 with no hint replaces one 9s wait with dozens of
/// round-trips against the row that is already contended — the drained-node
/// terminator holding `nodes` for ~9s is the documented case.
///
/// One second is the floor of what the header can express, not a tuned value:
/// `Retry-After` in delay-seconds form is an integer number of seconds, so no
/// hint below 1s is representable and anything larger is a choice to make the
/// caller wait longer than the bound it hit. Note this does *not* claim the
/// peer has released by then — the bound and the hint are independent, and a
/// second collision is answered by another 503 rather than by a longer wait.
pub(crate) const LOCK_CONTENTION_RETRY_AFTER: Duration = Duration::from_secs(1);
/// Lower bound for the lease-derived `Retry-After` on restore-lease
/// collisions. Even a nearly-expired lease must not advertise an immediate
/// retry, or hot-retrying clients collapse into a busy loop against the
/// claim CAS.
pub(crate) const RESTORE_RETRY_AFTER_FLOOR: Duration = Duration::from_secs(1);

/// Whether a `create_execution` wait error means "the pod is still coming up,
/// leave it running so the node converges" (preserve node-side work) rather
/// than "creation genuinely failed, tear it down". Only a [`SubstrateError::
/// Timeout`] — the budget-exhausted slow tail — is preservable; every other
/// error is a real failure that should clean up the pod.
///
/// Only the Kubernetes substrate's create path preserves node-side work, so
/// this is gated to that feature to stay dead-code-free in default builds.
#[cfg(feature = "kubernetes")]
pub(crate) fn create_wait_should_preserve_node_work(err: &SubstrateError) -> bool {
    matches!(err, SubstrateError::Timeout { .. })
}

/// Runtime endpoint for the active execution object backing a stable sandbox.
///
/// A sandbox ID is durable. Execution objects are not: an execution object
/// may be terminated and replaced during restore, and the endpoint belongs
/// to that active execution object rather than to the sandbox forever.
#[derive(Debug, Clone)]
pub struct ExecutionBinding {
    pub execution_object_id: String,
    pub runtime_endpoint: Endpoint,
    pub execution_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationCompletion {
    Owned,
    Joined,
}

impl ActivationCompletion {
    pub fn should_finalize_lifecycle(self) -> bool {
        self == Self::Owned
    }
}

pub use pplx_space_common_core::types::AbandonedRestoreRetirement;

/// Exact operation lease to release after the controller publishes its lifecycle.
/// A later owner or execution must never be released by this completion.
#[derive(Debug, Clone, Copy)]
pub struct ActivationLeaseCompletion {
    pub generation: i64,
    pub attempt_id: SandboxActivationAttemptId,
}

#[derive(Debug, Clone)]
pub struct ExecutionActivation {
    pub binding: ExecutionBinding,
    pub completion: ActivationCompletion,
    pub lease_completion: Option<ActivationLeaseCompletion>,
}

impl ExecutionActivation {
    pub fn owned(binding: ExecutionBinding) -> Self {
        Self {
            binding,
            completion: ActivationCompletion::Owned,
            lease_completion: None,
        }
    }

    pub fn joined(binding: ExecutionBinding) -> Self {
        Self {
            binding,
            completion: ActivationCompletion::Joined,
            lease_completion: None,
        }
    }
}

impl std::ops::Deref for ExecutionActivation {
    type Target = ExecutionBinding;

    fn deref(&self) -> &Self::Target {
        &self.binding
    }
}

pub struct TemplateRef<'a> {
    pub template: &'a TemplateName,
    pub content_hash: Option<&'a str>,
    pub boot_mode: CreateBootMode,
    pub runtime: &'a str,
}

pub struct SandboxResources {
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub disk_mib: Option<u32>,
}

impl SandboxResources {
    pub fn has_custom_vm_shape(&self) -> bool {
        self.vcpu.is_some() || self.memory_mib.is_some() || self.disk_mib.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateBootMode {
    /// Try pool, then base snapshot, then cold.
    Fallthrough,
    Cold,
    BaseSnapshot,
    Pool,
}

impl CreateBootMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fallthrough => "fallthrough",
            Self::Cold => "cold",
            Self::BaseSnapshot => "base-snapshot",
            Self::Pool => "pool",
        }
    }

    pub fn from_api(
        requested: Option<&str>,
        has_custom_vm_shape: bool,
    ) -> Result<Self, ControllerError> {
        match requested {
            None if has_custom_vm_shape => Ok(Self::Cold),
            None => Ok(Self::Fallthrough),
            Some("cold") => Ok(Self::Cold),
            Some("snapshot" | "base-snapshot") if has_custom_vm_shape => {
                Err(ControllerError::bad_request(
                    "custom vcpu, memory_mib, or disk_mib require cold boot; boot_mode=snapshot is not supported",
                ))
            }
            Some("pool") if has_custom_vm_shape => Err(ControllerError::bad_request(
                "custom vcpu, memory_mib, or disk_mib require cold boot; boot_mode=pool is not supported",
            )),
            Some("snapshot" | "base-snapshot") => Ok(Self::BaseSnapshot),
            Some("pool") => Ok(Self::Pool),
            Some(other) => Err(ControllerError::bad_request(format!(
                "boot_mode must be one of cold, snapshot, base-snapshot, pool; got {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod pool_eligibility_tests;

#[derive(Debug, Clone, Copy)]
pub struct ActivationOptions {
    pub blocking: bool,
    pub ready_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueuedArtifactBackup {
    pub artifact: PublishedLocalArtifact,
    pub attempt_id: BackupAttemptId,
}

pub(crate) async fn persist_optional_current_local_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    published_artifact: Option<PublishedLocalArtifact>,
    origin: ArtifactOrigin,
) -> Result<(), SubstrateError> {
    let mut tx = pool
        .begin()
        .await
        .db_context("failed to start current local artifact transaction")?;
    let sandbox_exists =
        sqlx::query_scalar::<_, Uuid>(sql!("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE"))
            .bind(sandbox_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .db_context("failed to lock sandbox for current local artifact")?
            .is_some();
    if !sandbox_exists {
        tx.rollback()
            .await
            .db_context("failed to close missing-sandbox artifact transaction")?;
        return Ok(());
    }
    if let Some(artifact) = published_artifact {
        reject_deleting_current_local_artifact(&mut tx, sandbox_id, artifact).await?;
    }
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = $3,
                current_local_artifact_origin = CASE WHEN $2 IS NULL THEN NULL ELSE $4 END
          WHERE id = $1"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(published_artifact.map(|artifact| artifact.id.as_uuid()))
    .bind(published_artifact.map(|artifact| artifact.kind.as_str()))
    .bind(origin.as_str())
    .execute(&mut *tx)
    .await
    .db_context("failed to persist current local artifact identity")?;
    tx.commit()
        .await
        .db_context("failed to commit current local artifact identity")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionActivityKind {
    Access,
    Command,
}

impl ExecutionActivityKind {
    pub fn wake_source(self) -> i32 {
        use pplx_space_sbx_mgr_client::proto::AutoLifecycleWakeSource;
        match self {
            Self::Access => AutoLifecycleWakeSource::Access as i32,
            Self::Command => AutoLifecycleWakeSource::Command as i32,
        }
    }

    pub fn allows_implicit_wake(self, policy: &AutoLifecyclePolicy) -> bool {
        match self {
            Self::Access => policy.wake_on_access(),
            Self::Command => policy.wake_on_access() || policy.wake_on_command(),
        }
    }
}

pub struct ActorRef {
    pub user_id: UserId,
    pub org_id: OrgId,
    pub email: Email,
    pub sandbox_owner: SandboxOwner,
}

impl ActorRef {
    pub fn new(user_id: UserId, org_id: OrgId, email: Email) -> Self {
        let sandbox_owner = SandboxOwner::platform_user(user_id);
        Self {
            user_id,
            org_id,
            email,
            sandbox_owner,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SnapshotRef<'a> {
    pub snapshot_id: &'a str,
    pub storage_uri: Option<&'a str>,
    pub source_sandbox_id: SandboxId,
}

pub struct CreateExecutionRequest<'a> {
    pub metric_identity: SandboxApiMetricIdentity,
    pub sandbox_id: SandboxId,
    pub actor: &'a ActorRef,
    pub template_ref: &'a TemplateRef<'a>,
    pub resources: &'a SandboxResources,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub env: &'a BTreeMap<String, String>,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override: Option<&'a NetworkPolicyV1>,
    pub network_policy_generation: u64,
    pub template_network_policy: &'a NetworkPolicyV1,
    pub template_network_policy_generation: u64,
    pub restore_from: Option<SnapshotRef<'a>>,
    pub wait_until_ready: bool,
    /// BYOK: create this sandbox on dm-crypt encrypted volumes. Immutable for
    /// the sandbox's lifetime; the control plane stamps it on the `sandboxes`
    /// row and the substrate must carry it to the node create (or fail closed
    /// if it cannot).
    pub volume_encryption: bool,
    /// Create-time isolation node pool pin. When `Some`, the k8s substrate adds
    /// a `nodeSelector` + toleration for the pool to the cold-create PodSpec;
    /// substrates without a node-pool scheduling primitive reject the request.
    /// PodSpec-only and not persisted, so it applies at create and does not
    /// survive to a later reconcile/restore.
    pub node_pool: Option<&'a NodePoolName>,
}

pub struct CreateExecutionOutcome {
    pub lease_completion: Option<ActivationLeaseCompletion>,
    pub sandbox_id: SandboxId,
    pub binding: Option<ExecutionBinding>,
    pub initial_lifecycle: &'static str,
    pub initial_placement: &'static str,
    pub active_node_id: Option<NodeId>,
    /// Whether the runtime reached command-readiness before create returned.
    /// `false` on the slow-tail path where the readiness wait exhausted its
    /// ALB-aware budget: the pod is left running and the sandbox is reported
    /// `started` (so the transient-lifecycle sweeper won't age out a healthy
    /// create), but the runtime is still converging and the caller emits a
    /// `create_pending` event instead of `create_completed`.
    pub command_ready: bool,
}

/// Inputs to resolve a node-api gRPC endpoint regardless of substrate.
///
/// Substrates that store an endpoint directly populate `direct_endpoint`;
/// substrates that resolve nodes by an opaque identifier populate
/// `substrate_node_id`. The concrete substrate decodes the identifier.
pub struct NodeApiTarget<'a> {
    pub managed_by: NodeManagedBy,
    pub role: NodeRole,
    pub direct_endpoint: Option<&'a str>,
    pub substrate_node_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedNodeApiTarget {
    pub managed_by: NodeManagedBy,
    pub role: NodeRole,
    pub direct_endpoint: Option<String>,
    pub substrate_node_id: Option<String>,
}

impl OwnedNodeApiTarget {
    pub fn as_target(&self) -> NodeApiTarget<'_> {
        NodeApiTarget {
            managed_by: self.managed_by,
            role: self.role,
            direct_endpoint: self.direct_endpoint.as_deref(),
            substrate_node_id: self.substrate_node_id.as_deref(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedDurableSnapshot {
    pub storage_uri: String,
    pub bytes_published: u64,
}

/// Substrate-synthesized inventory row. The direct substrate never produces
/// this (the database is the source of truth); substrates that own their own
/// node view synthesize entries so builder-only nodes and idle workers stay
/// visible.
#[derive(Debug, Clone)]
pub struct NodeInventoryEntry {
    pub id: NodeId,
    pub name: Option<String>,
    pub address: Option<NodeAddress>,
    pub role: NodeRole,
    pub managed_by: NodeManagedBy,
    pub state: Option<String>,
    pub last_heartbeat: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub substrate_node_id: Option<String>,
}

/// Which readiness wait a substrate timeout fired in. Carried on
/// [`SubstrateError::Timeout`] so consumers (timeout messages, and the
/// `phase` tag of `node_drain.restore_converging`, where node-drain's cohort
/// restore counts these timeouts rather than on `node_drain.failures`)
/// attribute the timeout to the step that actually stalled instead of a blanket "node-mgr unavailable" label —
/// node-mgr is named only when a probe really returned gRPC `Unavailable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitReadyTimeoutPhase {
    /// The pod never reached scheduled / IP-assigned within the window.
    PodNeverReady,
    /// The pod had an IP but TCP connects to the sandbox proxy port never
    /// succeeded.
    ProxyUnreachable,
    /// The sandbox proxy accepted connections but the guest never reported
    /// command readiness.
    GuestNotReady,
}

impl WaitReadyTimeoutPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PodNeverReady => "pod_never_ready",
            Self::ProxyUnreachable => "proxy_unreachable",
            Self::GuestNotReady => "guest_not_ready",
        }
    }
}

impl std::fmt::Display for WaitReadyTimeoutPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// `Clone` is deliberate: every variant is plain data (no `#[source]` chains),
// and the wake path shares one coalesced run's failure with every caller that
// joined it, each of which needs an owned error to return.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SubstrateError {
    #[error("{message}")]
    Unavailable { message: String },
    #[error("{message}")]
    Conflict { message: String },
    #[error("{message}")]
    FailedPrecondition { message: String },
    #[error("{message}")]
    Failed { message: String },
    #[error("{message}")]
    NotFound { message: String },
    /// A readiness wait exhausted its window. `phase` is `Some` when the
    /// timeout fired inside a specific readiness wait (see
    /// [`WaitReadyTimeoutPhase`]); `None` for outer budgets that wrap the
    /// whole operation.
    #[error("{message}")]
    Timeout {
        message: String,
        phase: Option<WaitReadyTimeoutPhase>,
    },
    /// A replacement execution requires restore state that is not available.
    /// This attempt cannot materialize safely without intervention or new
    /// restore evidence. Pending publication uses `RestoreSourcePending`.
    #[error("{message}")]
    RestoreContextUnavailable { message: String },
    /// Started recovery found current local suspend state, but it cannot yet
    /// restore from the source node or durable storage.
    #[error("{message}")]
    RestoreSourcePending { message: String },
    /// Another request owns the bounded restore lease and this caller's join
    /// window elapsed before that owner published completion. The restore may
    /// still converge, so callers should retry the same sandbox.
    #[error("{message}")]
    RestoreInFlight {
        message: String,
        /// Time left on the owner's restore lease when the collision was
        /// observed, when the emitter knows it. Drives the client-facing
        /// `Retry-After` so retry cadence tracks the lease instead of the
        /// flat gateway constant.
        lease_remaining: Option<Duration>,
    },
    /// Teardown owns the current execution while it durably suspends and
    /// retires the exact Kubernetes pod. Activation callers must retry the
    /// same sandbox without allocating a successor execution.
    #[error("{message}")]
    SuspendInFlight { message: String },
    /// Node-local deletion was refused because the retained filesystem may
    /// be the sandbox's only copy. Cluster deletion must preserve its row for
    /// recovery, but node drain must report the sandbox as stuck rather than
    /// treating it as an in-flight suspend that will converge unaided.
    #[error("{message}")]
    RetainedFilesystemOnlyCopy { message: String },
    /// Another request owns the bounded activation lease for this execution.
    /// Same-target callers may join its result; different-target callers must
    /// retry after the lease lapses.
    #[error("{message}")]
    ActivationInFlight {
        message: String,
        lease_remaining: Duration,
        same_target: bool,
        owner_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    },
    /// Cluster activation admission rejected a freshly-won restore claim.
    /// The claim has already been released; the caller should retry after
    /// the hint.
    #[error("{message}")]
    AdmissionRejected {
        message: String,
        retry_after: Option<Duration>,
        /// Rate-limit rejections carry the admission cause the async
        /// dispatcher treats as indefinite backpressure; empty-cluster
        /// rejections get a bounded retry instead.
        rate_limited: bool,
    },
    #[error("{message}")]
    Cancelled { message: String },
    /// The operation isn't implemented by the active substrate (e.g. setting
    /// node schedulability against a substrate that has no concept of it).
    #[error("{message}")]
    Unsupported { message: String },
    /// A node that was already admin-terminated still appears in substrate
    /// state and an upsert was attempted against it. Inventory callers
    /// should skip; pod-placement callers should treat as a conflict.
    #[error("{message}")]
    NodeTombstoned { message: String },
    /// A marker-guarded evacuation restore claim
    /// (`begin_evacuation_restore_lifecycle`) found nothing it may take on
    /// this attempt. The cases are listed where the claim's miss is
    /// classified, in `restore_lease.rs`. Distinct from [`Self::Conflict`] so
    /// the node-drain cohort restore can treat it as a clean deferral rather
    /// than a restore failure — without swallowing genuine `Conflict`s (e.g. a
    /// real wake-RPC failure on the rebuilt pod) that must stay visible to
    /// `node_drain.failures`.
    #[error("{message}")]
    EvacuationClaimLost { message: String },
    /// A database statement lost a race for a lock: `40001`
    /// (`serialization_failure`), `40P01` (`deadlock_detected`), or `55P03`
    /// (`lock_not_available`). The identical statement can succeed on retry.
    ///
    /// Distinct from [`Self::Unavailable`] in both directions, which is why it
    /// is a variant rather than a reuse. Outward, `Unavailable` reaches
    /// [`ControllerError`] as an unmarked 503 and the public and ECW
    /// translators re-label every unmarked 503 as a fleet condition, blaming
    /// the sandbox fleet for a Postgres lock race. Inward, the substrate's own
    /// predicates read `Unavailable` as "this pod has no reachable runtime"
    /// (`delete_execution` swallows it and skips in-VM cleanup) or as
    /// "node-mgr is unreachable" (which arms the cascade-amplifier defer
    /// path); a contended read must trigger neither.
    #[error("{message}")]
    LockContention { message: String },
}

#[derive(Debug, thiserror::Error)]
pub enum DurableSnapshotError {
    /// No staging or transfer work started; retry the same durable operation.
    #[error("durable snapshot copy admission is busy")]
    Backpressure,
    #[error("node-api durable snapshot creation error: {0}")]
    NodeRpc(#[source] tonic::Status),
    #[error(transparent)]
    Substrate(#[from] SubstrateError),
}

pub(crate) fn durable_snapshot_rpc_error(error: tonic::Status) -> DurableSnapshotError {
    if error.code() == tonic::Code::ResourceExhausted
        && pplx_space_common_grpc::is_refused_before_start(&error)
    {
        return DurableSnapshotError::Backpressure;
    }
    DurableSnapshotError::NodeRpc(error)
}

/// Classify a `sqlx` error raised by one of the substrate's own statements.
///
/// A contention SQLSTATE means the statement lost a race and can succeed on
/// retry, so it becomes a [`SubstrateError::LockContention`] (a marked 503)
/// rather than a [`SubstrateError::Failed`] (a 500). Which statements can
/// contend is wider than it looks — see [`crate::controllers::db_contention_error`],
/// whose reasoning about `INSERT ... ON CONFLICT`, plain `UPDATE`/`DELETE`,
/// and classifying every statement on a path rather than only the guard
/// applies here unchanged.
///
/// Unlike the retry predicates in [`direct`], this does *not* fold in
/// `sqlx::Error::Io` / `PoolTimedOut` or `57014`. Those gate bounded
/// in-process retries, where one more attempt on a fresh connection is cheap.
/// This is a client-facing disposition: telling every caller stalled behind a
/// drained pool to retry turns pool exhaustion into a synchronised retry wave.
///
/// The `warn` lives here so the level tracks the disposition — a bounded lock
/// wait is the expected outcome of enabling the bound, not a page.
pub(crate) fn substrate_db_error(what: &str, error: &sqlx::Error) -> SubstrateError {
    let message = format!("{what}: {error}");
    if pplx_space_common_svc::sql::contention::is_lock_contention(error) {
        // `what` rather than `message`, which already interpolates `error`.
        tracing::warn!(%error, what, "substrate statement lost a lock race; retryable");
        return SubstrateError::LockContention { message };
    }
    SubstrateError::Failed { message }
}

/// Postfix [`substrate_db_error`], so classifying a call site is a one-line
/// change rather than a closure rewrite.
///
/// An extension trait rather than an inherent method because `Result` is
/// foreign: the orphan rule permits `impl LocalTrait for ForeignType` in the
/// crate that owns the trait, which is what makes `.db_context(..)` available
/// on a `sqlx` result here. Implemented for exactly two error types —
/// `sqlx::Error` and the warm-pool store's `PoolError`, which wraps one — so
/// reaching for it on a `kube` or `tonic` result does not compile. The type
/// checker, not review, is what keeps the classification on database errors.
pub(crate) trait DbContext<T> {
    /// Classify, prefixing `what` onto the message. Phrase `what` as the
    /// existing messages are — `"failed to load the sandbox row"` — since the
    /// rendered error is `"{what}: {error}"`.
    fn db_context(self, what: &'static str) -> Result<T, SubstrateError>;

    /// [`Self::db_context`] for a message that has to interpolate. The closure
    /// runs only on the error path, so the `format!` costs nothing on success.
    fn db_context_with<F: FnOnce() -> String>(self, what: F) -> Result<T, SubstrateError>;
}

impl<T> DbContext<T> for Result<T, sqlx::Error> {
    fn db_context(self, what: &'static str) -> Result<T, SubstrateError> {
        self.map_err(|error| substrate_db_error(what, &error))
    }

    fn db_context_with<F: FnOnce() -> String>(self, what: F) -> Result<T, SubstrateError> {
        self.map_err(|error| substrate_db_error(&what(), &error))
    }
}

/// Classify a warm-pool store error.
///
/// `PoolError::Sql` is `#[error(transparent)]`, so routing it through
/// [`substrate_db_error`] leaves the rendered message unchanged and only moves
/// the disposition. The other variants are slot-state and parse failures,
/// which are genuine 500s: a slot that is not `warming` does not become
/// `warming` on retry.
///
/// A free function rather than only a [`DbContext`] impl because two call
/// sites cannot use the trait — `kubernetes::pool` maps
/// `PoolError::EncryptableConfigFenced` to a 400 in a `match` whose fallback
/// arm this is, and one site returns the mapped result as a tail expression
/// with no `?` to convert through. `crate::controllers::pool_store_error`
/// wraps this for them, so the variant split is written once.
pub(crate) fn pool_error(what: &str, error: &pplx_space_cluster_pool::PoolError) -> SubstrateError {
    match error {
        pplx_space_cluster_pool::PoolError::Sql(sql) => substrate_db_error(what, sql),
        other => SubstrateError::Failed {
            message: format!("{what}: {other}"),
        },
    }
}

impl<T> DbContext<T> for Result<T, pplx_space_cluster_pool::PoolError> {
    fn db_context(self, what: &'static str) -> Result<T, SubstrateError> {
        self.map_err(|error| pool_error(what, &error))
    }

    fn db_context_with<F: FnOnce() -> String>(self, what: F) -> Result<T, SubstrateError> {
        self.map_err(|error| pool_error(&what(), &error))
    }
}

impl SubstrateError {
    pub fn suspend_in_flight(sandbox_id: SandboxId) -> Self {
        Self::SuspendInFlight {
            message: format!(
                "sandbox {sandbox_id} suspend finalization is in flight; retry activation after \
                 the current execution is retired"
            ),
        }
    }

    pub fn restore_in_flight(message: impl Into<String>) -> Self {
        Self::RestoreInFlight {
            message: message.into(),
            lease_remaining: None,
        }
    }

    pub fn restore_source_pending(sandbox_id: SandboxId) -> Self {
        Self::RestoreSourcePending {
            message: format!(
                "sandbox {sandbox_id} restore source is not yet portable; waiting for snapshot \
                 upload or an eligible source node"
            ),
        }
    }

    pub fn restore_in_flight_with_lease(
        message: impl Into<String>,
        lease_remaining: Duration,
    ) -> Self {
        Self::RestoreInFlight {
            message: message.into(),
            lease_remaining: Some(lease_remaining),
        }
    }

    /// Strip the retryable disposition from an error raised *after*
    /// irreversible work landed, turning [`Self::LockContention`] back into
    /// [`Self::Failed`].
    ///
    /// The [`ControllerError`] twin of this carries the full rationale. The
    /// short version: once a pod exists or a warm slot is adopted, telling the
    /// caller to retry makes it do that again under a new id, so the hazard is
    /// a property of the *path* and not of any one statement on it. Applied to
    /// a whole result at the boundary rather than statement by statement —
    /// which is what left five siblings of an exempted statement still
    /// answering retryable.
    pub(crate) fn demoted_after_side_effect(self) -> Self {
        match self {
            Self::LockContention { message } => Self::Failed { message },
            other => other,
        }
    }

    /// The readiness phase a [`Self::Timeout`] fired in, when known.
    pub fn wait_ready_timeout_phase(&self) -> Option<WaitReadyTimeoutPhase> {
        match self {
            Self::Timeout { phase, .. } => *phase,
            _ => None,
        }
    }
}

impl From<WaitReadyTimeoutPhase> for ControllerRetryAfterCause {
    fn from(phase: WaitReadyTimeoutPhase) -> Self {
        match phase {
            WaitReadyTimeoutPhase::PodNeverReady => Self::PodNeverReady,
            WaitReadyTimeoutPhase::ProxyUnreachable => Self::ProxyUnreachable,
            WaitReadyTimeoutPhase::GuestNotReady => Self::GuestNotReady,
        }
    }
}

impl From<SubstrateError> for ControllerError {
    fn from(err: SubstrateError) -> Self {
        match err {
            SubstrateError::Unavailable { message } => {
                ControllerError::service_unavailable(message)
            }
            SubstrateError::Conflict { message } => {
                ControllerError::new(axum::http::StatusCode::CONFLICT, message)
            }
            SubstrateError::FailedPrecondition { message } => {
                ControllerError::new(axum::http::StatusCode::PRECONDITION_FAILED, message)
            }
            SubstrateError::Failed { message } => ControllerError::internal(message),
            SubstrateError::NotFound { message } => ControllerError::not_found(message),
            // A substrate wait that exhausted its ALB-aware budget is not a
            // dead end: the node keeps converging, so surface a retryable
            // 503 + `Retry-After` rather than an opaque 504. The
            // `retry_after` marker is also what the ECW boundary keys off to
            // return the E2B SDK's recoverable `sandbox_timeout` (502) shape.
            SubstrateError::Timeout { message, phase } => {
                let retry_after_cause = phase.map(ControllerRetryAfterCause::from);
                let error = ControllerError::service_unavailable(message)
                    .with_retry_after(GATEWAY_RETRY_AFTER);
                if let Some(cause) = retry_after_cause {
                    error.with_retry_after_cause(cause)
                } else {
                    error
                }
            }
            SubstrateError::RestoreContextUnavailable { message } => {
                // No verified state supports this recovery attempt. Advertising
                // convergence would make clients repeatedly resume the same
                // execution, without any work that can satisfy the request.
                ControllerError::restore_context_unavailable(message)
            }
            SubstrateError::RestoreSourcePending { message } => {
                ControllerError::service_unavailable(message)
                    .with_retry_after(GATEWAY_RETRY_AFTER)
                    .with_retry_after_cause(ControllerRetryAfterCause::RestoreInFlight)
            }
            SubstrateError::RestoreInFlight {
                message,
                lease_remaining,
            } => {
                let retry_after = lease_remaining
                    .map(|remaining| {
                        remaining.clamp(RESTORE_RETRY_AFTER_FLOOR, GATEWAY_RETRY_AFTER)
                    })
                    .unwrap_or(GATEWAY_RETRY_AFTER);
                ControllerError::service_unavailable(message)
                    .with_retry_after(retry_after)
                    .with_retry_after_cause(ControllerRetryAfterCause::RestoreInFlight)
            }
            SubstrateError::SuspendInFlight { message } => {
                ControllerError::service_unavailable(message)
                    .with_retry_after(GATEWAY_RETRY_AFTER)
                    .with_retry_after_cause(ControllerRetryAfterCause::RestoreInFlight)
            }
            SubstrateError::RetainedFilesystemOnlyCopy { message } => {
                ControllerError::new(axum::http::StatusCode::CONFLICT, message)
            }
            SubstrateError::ActivationInFlight {
                message,
                lease_remaining,
                ..
            } => ControllerError::service_unavailable(message)
                .with_retry_after(
                    lease_remaining.clamp(RESTORE_RETRY_AFTER_FLOOR, GATEWAY_RETRY_AFTER),
                )
                .with_retry_after_cause(ControllerRetryAfterCause::RestoreInFlight),
            SubstrateError::AdmissionRejected {
                message,
                retry_after,
                rate_limited,
            } => {
                let mut error = ControllerError::rate_limited(message);
                if let Some(retry_after) = retry_after {
                    error = error.with_retry_after_header(retry_after);
                }
                if rate_limited {
                    error = error.with_retry_after_cause(
                        ControllerRetryAfterCause::ClusterActivationAdmission,
                    );
                }
                error
            }
            SubstrateError::Cancelled { message } => ControllerError::service_unavailable(message),
            SubstrateError::Unsupported { message } => {
                ControllerError::new(axum::http::StatusCode::NOT_IMPLEMENTED, message)
            }
            SubstrateError::NodeTombstoned { message } => {
                ControllerError::new(axum::http::StatusCode::CONFLICT, message)
            }
            SubstrateError::EvacuationClaimLost { message } => {
                ControllerError::new(axum::http::StatusCode::CONFLICT, message)
            }
            // The message is dropped rather than forwarded: it names the
            // statement and carries the SQLSTATE, a 503 body reaches the
            // client verbatim, and `substrate_db_error` already logged it.
            SubstrateError::LockContention { .. } => ControllerError::lock_contention(),
        }
    }
}

/// Placement resolution failures all fail the pod build closed; this is where
/// each one picks its client-visible shape.
///
/// `no_compatible_node_group` maps onto the existing [`SubstrateError::Conflict`]
/// rather than a variant of its own. A dedicated variant would have been
/// observationally identical — `Conflict` is already the 409 with no
/// `Retry-After`, which is exactly right here (nothing is broken; the cluster
/// simply has no capacity this template may run on, and retrying changes
/// nothing until an operator provisions a group) — while taxing every
/// exhaustive match on this enum. The triage distinction lives where it is
/// actually queryable: `node_group_placement.resolved{outcome}`.
#[cfg(feature = "kubernetes")]
impl From<crate::node_group_placement::NodeGroupPlacementResolveError> for SubstrateError {
    fn from(err: crate::node_group_placement::NodeGroupPlacementResolveError) -> Self {
        use crate::node_group_placement::NodeGroupCatalogError;
        use crate::node_group_placement::NodeGroupPlacementResolveError as ResolveError;
        let message = err.to_string();
        match err {
            // The one condition an operator can act on and a caller can
            // understand: this cluster has no capacity this template may use.
            ResolveError::NoCompatibleNodeGroup { .. } => Self::Conflict { message },
            // A lost lock race on the catalog read is not a control-plane
            // inconsistency — the identical read succeeds on retry, and the
            // resolve runs while the pod spec is still being built, so failing
            // closed left nothing behind for a retry to duplicate. Keeping it
            // in the `Failed` arm below would make this the one store read on
            // the pod-build path that reports a bounded lock wait as
            // permanent, which is what the rest of this change removed.
            ResolveError::Catalog(NodeGroupCatalogError::Database(ref sql))
                if pplx_space_common_svc::sql::contention::is_lock_contention(sql) =>
            {
                Self::LockContention { message }
            }
            // A missing `templates` row for a live sandbox, a genuine database
            // failure, and a candidate the renderer rejected after this module
            // accepted it are all control-plane inconsistencies rather than
            // anything the caller expressed. They keep the shape every other
            // store failure on these paths has.
            ResolveError::TemplateAbsent { .. }
            | ResolveError::Catalog(_)
            | ResolveError::Invariant(_) => Self::Failed { message },
        }
    }
}

/// A local-only restore whose source node left the candidate set needs a
/// migration, and the sandbox's owner is who has to ask for it — a 409, like
/// every other "this resource's current state forbids the operation".
#[cfg(feature = "kubernetes")]
impl From<crate::node_group_placement::LocalRestoreSourceError> for SubstrateError {
    fn from(err: crate::node_group_placement::LocalRestoreSourceError) -> Self {
        Self::Conflict {
            message: err.to_string(),
        }
    }
}

/// Map a node suspend RPC error onto a [`SubstrateError`], preserving the
/// suspend-vs-wake arbitration signal: sbx-mgr returns `Aborted` when the
/// suspend queued behind a concurrent wake and was rejected as superseded.
/// That is a caller-resolvable conflict (re-issue the pause if the sandbox
/// should still be suspended), not an internal failure — surface it as a 409,
/// not a 500, and never retry it inside the substrate.
pub(crate) fn suspend_rpc_error(err: tonic::Status) -> SubstrateError {
    if err.code() == tonic::Code::Aborted {
        return SubstrateError::Conflict {
            message: format!("sandbox suspend superseded: {}", err.message()),
        };
    }
    SubstrateError::Failed {
        message: format!("sandbox suspend failed: {err}"),
    }
}

/// How the substrate addresses a node for node-api calls: which field of a
/// [`NodeApiTarget`] must be present. Must match the field the substrate's
/// [`SubstrateAdapter::resolve_node_api_endpoint`] requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeAddressing {
    /// The node row's stored node-api endpoint (`direct_endpoint`).
    DirectEndpoint,
    /// An opaque substrate node identifier (`substrate_node_id`) that the
    /// substrate resolves to an endpoint.
    SubstrateNodeId,
}

/// Static facts about what a substrate supports. Shared code reads these
/// instead of matching on which substrate is active. Each substrate returns
/// one of the constants below; behavior that needs the substrate to act stays
/// a [`SubstrateAdapter`] method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubstrateCapabilities {
    /// Every restore-shaped wake (from `suspended` or `restoring`) claims
    /// `restoring` before its node call and leaves `restoring -> started` to
    /// the caller's completion CAS. The caller can then treat a `suspended`
    /// row at completion as a newer, finalized suspend and fence it, and must
    /// shield that CAS against request cancellation.
    ///
    /// On Kubernetes the owner of a user restore, the wake holding its lease
    /// token, also commits `started` itself before it returns, so a user
    /// request never dials a `restoring` pod and the access path, which runs
    /// no completion CAS, doesn't wait on the node's published state. The
    /// caller's CAS then finds the row `started` and completes as before; it
    /// still covers every wake whose owner commit didn't land.
    ///
    /// `false` does not mean the substrate's rows are never `restoring`.
    /// Direct's worker-local wake leaves its pre-wake lifecycle (`suspended`,
    /// or `restoring` kept by an earlier lease restore) for the completion CAS
    /// to advance; its lease restore claims `restoring` and writes `started`
    /// itself.
    pub caller_completes_claimed_restore: bool,
    /// How node-api targets are addressed.
    pub node_addressing: NodeAddressing,
    /// Nodes live in the admin API's node registry: they are registered,
    /// updated and deleted there, and their templates are pulled and deleted
    /// there, rather than discovered by the substrate.
    pub registers_nodes_via_admin_api: bool,
    /// The control plane resolves a cold Firecracker create's shape: a
    /// fallthrough create takes a configured or policy pool shape, and
    /// otherwise vCPU and memory the request left unset come from the
    /// template's build shape. Without it, the requested shape goes to the
    /// node unchanged; Direct sends a custom vCPU or memory on its separate
    /// start call (see the [`direct`] module doc).
    pub resolves_cold_create_shape: bool,
    /// Create requests may carry a Kubernetes node-pool placement pin.
    pub supports_node_pool_placement: bool,
    /// The substrate has a node scheduling primitive for
    /// [`Substrate::set_node_schedulability`] to patch, and the cluster
    /// controller has a node-drain reconciler for it (opt-in through
    /// `SPACE_NODE_DRAIN_ENABLED`; the controller picks its loops by
    /// substrate, not from this field). When false, the admin drain, cordon
    /// and force-upload actions refuse before reading the node, except
    /// `:undrain`, which only clears a `drain_phase` an earlier drain left.
    /// A node row's counterpart is
    /// [`SubstrateKind::has_node_drain_reconciler`](pplx_space_common_core::types::SubstrateKind::has_node_drain_reconciler).
    pub supports_node_drain: bool,
    /// The adapter drives sandboxes through a Kubernetes apiserver, with a
    /// client the process builds from its environment (an in-cluster config
    /// or a kubeconfig). The cluster controller can therefore reach a leader
    /// `Lease` on that apiserver wherever it runs, and may share the cluster
    /// with in-cluster replicas, so it contends for the `Lease` even outside
    /// a pod.
    pub uses_kubernetes_api: bool,
    /// A successful [`SubstrateAdapter::suspend_execution`] has already
    /// committed `lifecycle = 'suspended'`, under the fence of the execution
    /// it suspended. The caller then writes no lifecycle of its own: an
    /// unfenced trailing `suspended` write would land after a wake that
    /// claimed the row in between and put a live, routed sandbox back to
    /// `suspended` with its execution still active.
    ///
    /// Direct's suspend leaves the lifecycle to that caller write.
    pub suspend_commits_lifecycle: bool,
}

impl SubstrateCapabilities {
    pub const DIRECT: Self = Self {
        caller_completes_claimed_restore: false,
        node_addressing: NodeAddressing::DirectEndpoint,
        registers_nodes_via_admin_api: true,
        resolves_cold_create_shape: false,
        supports_node_pool_placement: false,
        supports_node_drain: false,
        uses_kubernetes_api: false,
        suspend_commits_lifecycle: false,
    };

    pub const KUBERNETES: Self = Self {
        caller_completes_claimed_restore: true,
        node_addressing: NodeAddressing::SubstrateNodeId,
        registers_nodes_via_admin_api: false,
        resolves_cold_create_shape: true,
        supports_node_pool_placement: true,
        supports_node_drain: true,
        uses_kubernetes_api: true,
        suspend_commits_lifecycle: true,
    };
}

/// What [`SubstrateAdapter::ensure_routable_execution`] found for a sandbox's
/// active execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRoutabilityCheck {
    /// The substrate's `started` makes no pod-routability promise, so the row
    /// is routable as written. Nothing was checked or written.
    Unconditional,
    /// A live pod matching the active execution backed it, and its identity
    /// was copied onto the execution row. The execution is now routable.
    Hydrated,
    /// The execution row was already routable.
    AlreadyRoutable,
    /// The execution is not routable. At most, the pod identity was recorded
    /// on an execution row that is not running.
    NotRoutable,
}

impl ExecutionRoutabilityCheck {
    /// Whether a caller may treat the execution as routable.
    pub const fn is_routable(self) -> bool {
        match self {
            Self::Unconditional | Self::Hydrated | Self::AlreadyRoutable => true,
            Self::NotRoutable => false,
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait SubstrateAdapter: Send + Sync {
    /// Stable label written to control-plane metadata for this substrate.
    ///
    /// This is descriptive metadata. Runtime routing should use the active
    /// execution binding, not this label.
    fn managed_by(&self) -> &'static str;

    /// What this substrate supports. Required, so every substrate (and the
    /// `Substrate` forwarding impl) states its answer.
    fn capabilities(&self) -> SubstrateCapabilities;

    /// Create the active execution object for a newly-created sandbox.
    ///
    /// Implementations may return a binding if the endpoint is known
    /// immediately, but callers must tolerate placement happening
    /// asynchronously.
    async fn create_execution<'a>(
        &'a self,
        request: CreateExecutionRequest<'a>,
    ) -> Result<CreateExecutionOutcome, SubstrateError>;

    /// Finish an owned operation only after its target lifecycle is published.
    async fn complete_activation_lease(
        &self,
        _sandbox_id: SandboxId,
        _completion: Option<ActivationLeaseCompletion>,
    ) {
    }

    /// Delete the active execution object for a sandbox.
    ///
    /// This removes substrate-owned runtime state and any substrate-owned
    /// binding/cache rows. The stable sandbox row is deleted by the controller.
    async fn delete_execution(&self, sandbox_id: SandboxId) -> Result<(), SubstrateError>;

    /// Retire the active execution object for a sandbox while preserving
    /// node-local sandbox state.
    ///
    /// Issues an in-VM stop and releases substrate-owned pod/binding state,
    /// but never sends the sbx-mgr delete that removes the sandbox's
    /// filesystem and local-artifact rows. Kubernetes also enqueues a
    /// stopped-disk backup between the stop and the release when the pod has
    /// a reachable runtime, and keeps the execution tracked for the
    /// failed-cleanup retry when a retryable enqueue fails; a terminal
    /// refusal (no sbx-mgr row, a failed sandbox, a full suspend snapshot)
    /// lets the retire finish without a backup. Direct enqueues none (see the
    /// [`direct`] module doc): the stopped disk stays on its node.
    /// Failure reconcilers must use this instead of [`Self::delete_execution`]:
    /// a sandbox that reached `failed` mid-suspend may hold its only
    /// up-to-date copy on the node, and retire keeps that copy salvageable
    /// before releasing the execution binding.
    ///
    /// Callers reach this after their own probe and CAS, both of which are
    /// point-in-time. [`RetireExecutionRequest::fence`] pins the teardown to
    /// the pod identity the caller observed, so a pod that recovered or was
    /// replaced in that window yields
    /// [`RetireExecutionOutcome::PodIdentityChanged`] instead of a live
    /// sandbox losing its pod. Callers must treat that outcome as *defer*, not
    /// as an error to retry immediately, and must read its
    /// [`TeardownProgress`] before undoing anything of their own.
    async fn retire_execution(
        &self,
        request: RetireExecutionRequest,
    ) -> Result<RetireExecutionOutcome, SubstrateError>;

    /// Whether a retire carrying this fence would be permitted right now,
    /// without tearing anything down.
    ///
    /// A caller that must commit a state transition before it can retire
    /// otherwise learns the fence was lost only after that transition is
    /// durable. Asking first turns "commit, then discover" into "discover,
    /// then commit". It does not close the window between the two, which is
    /// why [`RetireExecutionOutcome::PodIdentityChanged`] also reports how far
    /// a refused teardown got.
    ///
    /// Not a pure read: the Kubernetes implementation records the UID of a pod
    /// it finds but cannot fence against, so the caller's *next* attempt has
    /// an enforceable fence. That write is guarded on the execution's
    /// generation and attempt and only fills a UID the row is missing, so it
    /// cannot land on a successor and changes nothing a caller has to undo.
    ///
    /// Answers `None` when the fence holds, and the mismatch otherwise. A
    /// substrate with no pod object to fence against always answers `None`.
    async fn retire_fence_mismatch(
        &self,
        _sandbox_id: SandboxId,
        _fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, SubstrateError> {
        Ok(None)
    }

    /// Snapshot how the substrate currently sees these pods.
    ///
    /// This is what lets a caller fence on more than a name and a persisted
    /// UID. A UID alone cannot tell a pod that has *moved* — Pending becoming
    /// Running is the same object — so a caller whose evidence is about
    /// runtime state, not identity, needs a resourceVersion captured at the
    /// moment it formed that evidence. Implementations must answer from cached
    /// state: this is called per candidate over a batch, and an apiserver
    /// round trip each would be its own outage. A pod the substrate cannot see
    /// is simply absent from the result.
    async fn observe_pod_identities(
        &self,
        _pod_names: &[PodName],
    ) -> HashMap<PodName, ObservedPodIdentity> {
        HashMap::new()
    }

    /// Delete a node-observed execution that has no API sandbox row.
    ///
    /// The control plane has no stable sandbox row to route through, so the
    /// caller supplies the observed node target.
    async fn delete_observed_execution(
        &self,
        sandbox_id: SandboxId,
        target: &NodeApiTarget<'_>,
    ) -> Result<(), SubstrateError> {
        delete_observed_execution_via_node_api(self, sandbox_id, target).await
    }

    async fn enqueue_artifact_backup(
        &self,
        sandbox_id: SandboxId,
        expected_kind: ArtifactKind,
        source: BackupSource,
        target: &NodeApiTarget<'_>,
    ) -> Result<EnqueuedArtifactBackup, SubstrateError> {
        let endpoint_addr = self.resolve_node_api_endpoint(target).await?;
        enqueue_artifact_backup_via_retrying_node_api(
            &endpoint_addr,
            sandbox_id,
            expected_kind,
            source,
        )
        .await
    }

    async fn create_snapshot_from_durable_artifact(
        &self,
        target: &NodeApiTarget<'_>,
        operation_id: OperationId,
        source_sandbox_id: SandboxId,
        target_sandbox_id: SandboxId,
        source_storage_uri: String,
        destination_storage_uri: String,
    ) -> Result<PublishedDurableSnapshot, DurableSnapshotError> {
        let endpoint_addr = self.resolve_node_api_endpoint(target).await?;
        let mut node_api = retrying_worker_node_api(&endpoint_addr, Duration::from_secs(30))?;
        let mut client = RetryingWorkerApiClient::new(&mut node_api);
        let response = client
            .create_sandbox_snapshot(
                pplx_space_sbx_mgr_client::proto::CreateSnapshotRequest {
                    sandbox_id: source_sandbox_id.to_string(),
                    r#type: pplx_space_sbx_mgr_client::proto::SnapshotType::Disk as i32,
                    label: None,
                    creation_identity: Some(
                        pplx_space_sbx_mgr_client::proto::SnapshotCreationIdentity {
                            request_id: operation_id.as_uuid().to_string(),
                        },
                    ),
                    source: Some(pplx_space_sbx_mgr_client::proto::CreateSnapshotSource {
                        source: Some(
                            pplx_space_sbx_mgr_client::proto::create_snapshot_source::Source::DurableArtifact(
                                pplx_space_sbx_mgr_client::proto::DurableArtifactSnapshotSource {
                                    source_storage_uri,
                                    destination_storage_uri,
                                    destination_sandbox_id: target_sandbox_id.to_string(),
                                    destination_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Fork as i32,
                                },
                            ),
                        ),
                    }),
                },
                Duration::from_secs(600),
            )
            .await
            .map_err(durable_snapshot_rpc_error)?
            .into_inner();
        let published =
            response
                .published_durable_artifact
                .ok_or_else(|| SubstrateError::Failed {
                    message: "durable snapshot response omitted its storage identity".to_string(),
                })?;
        Ok(PublishedDurableSnapshot {
            storage_uri: published.storage_uri,
            bytes_published: published.bytes_published,
        })
    }

    /// Ensure the active execution object exists and is usable for runtime calls.
    ///
    /// This is the substrate reconciliation point for start: it may create,
    /// start, refresh, or wait for substrate-owned execution state as needed.
    async fn start_execution(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError>;

    /// Restore a suspended sandbox through the active execution object.
    ///
    /// This is separate from `start_execution` because restore may use
    /// checkpoint or snapshot behavior that is specific to the substrate.
    async fn restore_execution(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: RecoveryCharge,
    ) -> Result<ActivationCompletion, SubstrateError>;

    /// Verify, and repair where the substrate can, that a `restoring`
    /// sandbox's active execution is routable before a caller converges the
    /// row to `started`.
    ///
    /// On a substrate whose `started` promises a routable execution row that
    /// may need hydrating from a live pod (Kubernetes), this checks the
    /// execution against the substrate and copies a live matching pod's
    /// identity onto a row a dead wake handler never finished. `budget`
    /// bounds the apiserver read; a timeout or transport error is returned as
    /// an error, never as `NotRoutable`. The answer is advisory: a caller's
    /// lifecycle CAS must still re-check routability inside its own write.
    ///
    /// The default is for a substrate whose `started` makes no such promise
    /// (Direct): it answers `Unconditional` without checking anything.
    async fn ensure_routable_execution(
        &self,
        _sandbox_id: SandboxId,
        _budget: Duration,
    ) -> Result<ExecutionRoutabilityCheck, SubstrateError> {
        Ok(ExecutionRoutabilityCheck::Unconditional)
    }

    /// Suspend a sandbox through the active execution object.
    ///
    /// Substrates own any post-suspend execution-object bookkeeping, such as
    /// retaining or replacing the execution object after sandbox-manager has
    /// persisted suspend state. Returns once the suspend has been requested;
    /// the snapshot upload itself runs asynchronously on the node and
    /// registers its storage location with the cluster API separately.
    async fn suspend_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError>;

    /// Wake to command readiness. An activity source makes this implicit and
    /// requires the worker's effective policy to allow that source; `None` is
    /// explicit operator lifecycle control.
    async fn wake_execution(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        wake_activity: Option<ExecutionActivityKind>,
        charge: RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError>;

    /// Connect to the sandbox runtime API for the active execution object.
    ///
    /// Higher-level lifecycle reconciliation should live on explicit substrate
    /// methods, not in controller-side runtime-client setup.
    /// `admission` charges the cluster limiter when this call has to recreate a
    /// missing pod for a recoverable non-dormant sandbox (the control-wake
    /// path). Callers that must not be shed as activations — stop/kill/pause and
    /// other teardown/read paths — pass `None`.
    async fn runtime_client(
        &self,
        sandbox_id: SandboxId,
        charge: RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError>;

    /// Connect without creating or waking execution; stale routing evidence may be cleared.
    ///
    /// This is used for best-effort live metadata fan-out, where missing or
    /// dormant execution must not create pods or change lifecycle state.
    async fn reachable_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError>;

    /// Probe without mutating routing evidence or deleting execution objects.
    /// Cancellable ingress preparation uses this before assigning an activation owner.
    async fn probe_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError>;

    /// Connect to the runtime API for a guest-facing access path.
    ///
    /// This may reconcile substrate-owned execution state, including
    /// recreating the execution object, but must still honor auto-wake
    /// policy for dormant sandboxes.
    async fn access_runtime_client(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        activity: ExecutionActivityKind,
        timeout: Duration,
        charge: RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError>;

    /// Connect to the node-api builder surface for substrate-level template work.
    ///
    /// The substrate owns where this client points; callers own the common
    /// template/build/pool RPC behavior.
    async fn builder_client(&self) -> Result<BuilderClient, ControllerError>;

    /// Connect to the builder node that accepted a template build.
    async fn builder_client_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<BuilderClient, ControllerError>;

    fn pool_eligible_for_request(
        &self,
        _boot_mode: CreateBootMode,
        _backend: &str,
        _wait_until_ready: bool,
        _disk_mib: Option<u32>,
        _file_changes_enabled: bool,
        _implicit_wake_enabled: bool,
    ) -> bool {
        false
    }

    /// Whether this substrate can apply a create's process env to a sandbox
    /// adopted from the warm pool. A cold create bakes `req.env` into the
    /// sandbox at boot; a substrate that can't reproduce that on an
    /// already-pooled sandbox must treat env-bearing creates as pool-ineligible
    /// (see [`pool_eligible_with_env`]).
    ///
    /// Defaults to `false` — the safe assumption that a substrate can't set env
    /// on a booted pooled sandbox, so env-bearing creates fall back to the cold
    /// path rather than silently dropping the requested variables. A substrate
    /// that genuinely can apply env at adoption overrides this to `true`.
    fn pool_supports_request_env(&self) -> bool {
        false
    }

    /// Attempt to claim a substrate-owned warm-pool execution.
    ///
    /// This runs outside the API sandbox-row transaction because substrates
    /// may claim resources in their own stores or through node-local RPCs.
    /// The result includes the exact release-and-shape pool selected by the
    /// attempt even when no ready execution is claimable. Callers must either
    /// persist a returned claim with
    /// `persist_pooled_execution_claim` or compensate it with
    /// `rollback_pooled_execution_claim`.
    async fn claim_pooled_execution(
        &self,
        _request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        Ok(PooledExecutionClaimAttempt::missed(None))
    }

    /// Look up the slot a create was admitted against. Substrates without
    /// admission-time pool claims own no slot under the ID, so the default
    /// answers [`AdmittedPoolClaimLookup::Released`] and the create builds
    /// cold.
    async fn lookup_admitted_pooled_execution(
        &self,
        _request: &PooledExecutionClaimRequest<'_>,
        _admitted: AdmittedPoolClaim,
        _sandbox_id: SandboxId,
    ) -> Result<AdmittedPoolClaimLookup, ControllerError> {
        Ok(AdmittedPoolClaimLookup::Released)
    }

    /// Before a cold create under an ID whose admitted slot was not adopted,
    /// record that the slot's pod used this ID's first execution generation,
    /// so the cold execution is allocated the next one and cannot share a
    /// name with a pod that may still be shutting down. Default: nothing to
    /// skip.
    async fn skip_admitted_pool_generation(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<(), ControllerError> {
        Ok(())
    }

    /// Whether a failed adoption under this ID still owes a pool cleanup that
    /// will tear the sandbox down. A create that finds its row in that state
    /// has not succeeded yet. Default: no such cleanup exists.
    async fn admitted_pool_cleanup_pending(
        &self,
        _sandbox_id: SandboxId,
    ) -> Result<bool, ControllerError> {
        Ok(false)
    }

    /// Claim a ready, plaintext, default-shape warm-pool slot of
    /// `template_release_id` inside `tx`, the transaction that records the
    /// create being admitted. The claim commits or rolls back with that
    /// transaction. `None` is a miss. Default: substrates without a warm pool
    /// always miss.
    async fn claim_admission_pool_slot(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        _template_release_id: Uuid,
    ) -> Result<Option<AdmissionPoolClaim>, ControllerError> {
        Ok(None)
    }

    /// Persist substrate metadata for a previously claimed pool execution.
    ///
    /// This is called inside the API transaction that inserts the sandbox row,
    /// so the sandbox record and any substrate binding commit together.
    async fn persist_pooled_execution_claim(
        &self,
        _tx: &mut Transaction<'_, Postgres>,
        _request: &PooledExecutionClaimRequest<'_>,
        _claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        Ok(())
    }

    async fn rollback_pooled_execution_claim(
        &self,
        _claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        Ok(())
    }

    async fn cleanup_failed_pooled_prepare(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, ControllerError> {
        self.rollback_pooled_execution_claim(claim).await?;
        Ok(PooledPrepareFailureCleanup::FinalizeSandboxDbDelete)
    }

    async fn apply_pooled_execution_auto_lifecycle(
        &self,
        _claim: &PooledExecutionClaim,
        _policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        Ok(())
    }

    async fn prepare_pooled_execution(
        &self,
        _metric_identity: SandboxApiMetricIdentity,
        _request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
        _wait_until_ready: bool,
        _timeout: Duration,
    ) -> Result<(), ControllerError> {
        self.apply_pooled_execution_auto_lifecycle(claim, policy)
            .await
    }

    async fn list_pool_configs(&self) -> Result<Vec<ClusterPoolConfigResponse>, ControllerError> {
        Err(SubstrateError::Unsupported {
            message: "cluster pools are not supported by this substrate".to_string(),
        }
        .into())
    }

    async fn upsert_pool_config(
        &self,
        _request: ClusterPoolConfigRequest,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        Err(SubstrateError::Unsupported {
            message: "cluster pools are not supported by this substrate".to_string(),
        }
        .into())
    }

    async fn disable_pool_config(
        &self,
        _id: Uuid,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        Err(SubstrateError::Unsupported {
            message: "cluster pools are not supported by this substrate".to_string(),
        }
        .into())
    }

    /// Resolve the node-api gRPC endpoint (`host:port`) for a node row.
    ///
    /// The direct substrate returns the endpoint stored in the database;
    /// substrates that resolve addresses dynamically synthesize one from
    /// their own address store, off the hot path of the control plane.
    async fn resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError>;

    /// Select a ready worker for node-local control-plane work using the
    /// substrate's placement and readiness policy.
    async fn select_ready_worker_node_api_target(
        &self,
    ) -> Result<OwnedNodeApiTarget, SubstrateError> {
        Err(SubstrateError::Unsupported {
            message: "ready worker selection is not supported by this substrate".to_string(),
        })
    }

    /// Substrate-synthesized inventory. The direct substrate returns `None`
    /// (the database is the source of truth); substrates that own their own
    /// node view return `Some(entries)`.
    async fn list_inventory_nodes(
        &self,
        role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError>;
}

async fn delete_observed_execution_via_node_api<A>(
    substrate: &A,
    sandbox_id: SandboxId,
    target: &NodeApiTarget<'_>,
) -> Result<(), SubstrateError>
where
    A: SubstrateAdapter + ?Sized,
{
    let endpoint_addr = substrate.resolve_node_api_endpoint(target).await?;
    let mut node_api = retrying_worker_node_api(&endpoint_addr, NODE_RPC_TIMEOUT)?;
    let mut client = RetryingWorkerApiClient::new(&mut node_api);

    match client
        .delete_sandbox(
            pplx_space_sbx_mgr_client::proto::DeleteRequest {
                sandbox_id: sandbox_id.to_string(),
                template_filter: None,
                blocking: true,
            },
            NODE_RPC_TIMEOUT,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) if err.code() == tonic::Code::NotFound => Ok(()),
        Err(err) => Err(SubstrateError::Failed {
            message: format!("node-api delete observed sandbox error: {err}"),
        }),
    }
}

/// The running substrate. Shared code asks it what it supports
/// ([`SubstrateAdapter::capabilities`]) and asks it to act (the
/// [`SubstrateAdapter`] and inherent methods) rather than branching on which
/// substrate it is; [`SubstrateAdapter::managed_by`] still names it, for SQL
/// row scope and wire labels. The variants live in the private `Inner`, so
/// only this module can match them; code outside it builds a `Substrate` with
/// `from_direct` or `from_kubernetes`.
#[derive(Clone)]
pub struct Substrate {
    inner: Inner,
}

#[derive(Clone)]
enum Inner {
    Direct(Box<direct::DirectSubstrate>),
    #[cfg(feature = "kubernetes")]
    Kubernetes(Box<kubernetes::KubernetesSubstrate>),
}

impl Substrate {
    /// Run on the Direct substrate.
    pub fn from_direct(substrate: direct::DirectSubstrate) -> Self {
        Self {
            inner: Inner::Direct(Box::new(substrate)),
        }
    }

    /// Run on the Kubernetes substrate.
    #[cfg(feature = "kubernetes")]
    pub fn from_kubernetes(substrate: kubernetes::KubernetesSubstrate) -> Self {
        Self {
            inner: Inner::Kubernetes(Box::new(substrate)),
        }
    }

    /// The Kubernetes substrate, for code that needs Kubernetes-only state
    /// (its client, caches, or reconcilers typed on it). Behavior shared code
    /// asks a substrate to perform belongs on [`SubstrateAdapter`], and static
    /// facts on [`SubstrateCapabilities`].
    #[cfg(feature = "kubernetes")]
    pub fn as_kubernetes(&self) -> Option<&kubernetes::KubernetesSubstrate> {
        match &self.inner {
            Inner::Direct(_) => None,
            Inner::Kubernetes(substrate) => Some(substrate),
        }
    }

    pub fn live_node_names(&self) -> Option<std::collections::HashSet<String>> {
        match &self.inner {
            Inner::Direct(_) => None,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.live_node_names(),
        }
    }

    pub async fn try_resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.resolve_node_api_endpoint(target).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.try_resolve_node_api_endpoint(target),
        }
    }

    pub async fn from_env(pool: &sqlx::PgPool) -> Result<Self, SubstrateError> {
        Self::from_env_with_lifecycle(pool, CancellationToken::new(), TaskTracker::new()).await
    }

    pub async fn from_env_with_lifecycle(
        pool: &sqlx::PgPool,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Result<Self, SubstrateError> {
        #[cfg(feature = "kubernetes")]
        {
            match kubernetes::KubernetesSubstrate::try_from_env(pool, shutdown_token, task_tracker)
                .await
            {
                Ok(Some(substrate)) => return Ok(Self::from_kubernetes(substrate)),
                Ok(None) => {}
                Err(err) => return Err(err),
            }
        }
        #[cfg(not(feature = "kubernetes"))]
        drop((shutdown_token, task_tracker));

        Ok(Self::from_direct(direct::DirectSubstrate::new(
            pool.clone(),
        )))
    }

    pub(crate) async fn reserve_rollback_route_identity(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>, SubstrateError>
    {
        match &self.inner {
            Inner::Direct(substrate) => substrate
                .reserve_rollback_route_identity(sandbox_id)
                .await
                .map(Some),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(_) => Ok(None),
        }
    }

    pub(crate) async fn persist_rollback_route_identity(
        &self,
        sandbox_id: SandboxId,
        route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
    ) -> Result<(), SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .persist_rollback_route_identity(sandbox_id, route_identity)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(_) => Ok(()),
        }
    }

    /// Probe a preserved `started` + `command_ready_pending` sandbox's guest
    /// readiness *without* recreating its execution or honoring auto-wake.
    ///
    /// Reuses [`SubstrateAdapter::reachable_runtime_client`] (non-recovering:
    /// it never recreates a pod or wakes a dormant sandbox) plus a single
    /// bounded `SpacedWaitReady`. The whole probe is wrapped in
    /// `tokio::time::timeout(probe_timeout, …)` so a wedged guest that blocks
    /// the RPC indefinitely still resolves rather than hanging the reaper tick.
    ///
    /// The classification is deliberately three-way so the reaper never reaps
    /// on the strength of one transport failure:
    /// - reached the runtime + `SpacedWaitReady` Ok -> [`Ready`](CommandReadyProbe::Ready)
    /// - reached the runtime + `SpacedWaitReady` returned a *non-transient*
    ///   error, or didn't answer within the deadline ->
    ///   [`Wedged`](CommandReadyProbe::Wedged) (positive evidence: the transport
    ///   works, the command surface isn't coming up)
    /// - couldn't reach the runtime, *or* `SpacedWaitReady` returned a
    ///   transient transport code (`Unavailable` / `ResourceExhausted` /
    ///   `Aborted`) -> [`Unreachable`](CommandReadyProbe::Unreachable)
    ///   (inconclusive — a node-mgr / proxy / Kube-API blip is
    ///   indistinguishable from a dead pod on one sample, so defer)
    ///
    /// Both phases are bounded by `probe_timeout`: client resolution can
    /// otherwise run TCP-connect retries / apiserver lookups unbounded, so a
    /// batch of blackholed runtimes would stall the reaper tick well past the
    /// configured per-row deadline. Worst case per row is therefore ~2×
    /// `probe_timeout` (resolution + RPC), which the batch cap bounds further.
    pub async fn probe_command_ready(
        &self,
        sandbox_id: SandboxId,
        probe_timeout: Duration,
    ) -> CommandReadyProbe {
        // Phase 1 — resolve a client, bounded. A timeout or any failure to
        // resolve is inconclusive (could be a dead pod or a transient blip).
        let mut client =
            match tokio::time::timeout(probe_timeout, self.reachable_runtime_client(sandbox_id))
                .await
            {
                Ok(Ok(Some(client))) => client,
                Ok(Ok(None)) | Ok(Err(_)) | Err(_) => return CommandReadyProbe::Unreachable,
            };
        let request = SpacedWaitReadyRequest {
            sandbox_id: sandbox_id.to_string(),
            // Hint the server to return by the probe deadline. The outer
            // `tokio::time::timeout` is the real bound; a `None` here (only
            // possible if the conversion ever overflowed) just lets the server
            // fall back to its own default, still capped by the outer timeout.
            timeout: prost_types::Duration::try_from(probe_timeout).ok(),
        };
        // Phase 2 — ask the guest, bounded. A transient transport code defers;
        // a non-transient error or no answer within the deadline is wedge
        // evidence (we reached the runtime and it isn't reporting ready).
        match tokio::time::timeout(
            probe_timeout,
            client.spaced_wait_ready(wait_ready_lifecycle_request(request)),
        )
        .await
        {
            Ok(Ok(_)) => CommandReadyProbe::Ready,
            Ok(Err(status)) if probe_status_is_transient(status.code()) => {
                CommandReadyProbe::Unreachable
            }
            Ok(Err(_)) | Err(_) => CommandReadyProbe::Wedged,
        }
    }

    /// Handle whose `is_ready()` flips to `true` once the substrate's
    /// execution-object cache has populated from its initial sync. The HTTP
    /// `/readyz` probe checks this so a fresh API replica does not get
    /// traffic before its cache is warm. The direct substrate has no such
    /// cache and is always ready.
    pub fn readiness(&self) -> ReflectorReady {
        match &self.inner {
            Inner::Direct(_) => ReflectorReady::always_ready(),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.readiness(),
        }
    }

    /// Count worker nodes that can accept a new sandbox start before
    /// request-specific template/runtime constraints are applied.
    ///
    /// Each substrate must exclude nodes that its own placement path would not
    /// target for new create/start/restore work: deleted or tombstoned nodes,
    /// non-worker roles, non-registered nodes, stale or unreachable node-status
    /// snapshots, scheduler-disabled nodes, and substrate-specific
    /// placement-ineligible nodes such as Kubernetes nodes outside the sandbox
    /// pod selector or blocked by scheduling taints.
    ///
    /// Returns `None` when capacity can't be determined confidently — e.g. the
    /// Kubernetes node reflector cache hasn't completed its initial LIST — so
    /// the caller can fail open rather than treating an unknown as an empty
    /// cluster. `Some(0)` is a confident "no start-capable nodes".
    pub async fn eligible_start_node_count(&self) -> Result<Option<u64>, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => Ok(Some(substrate.eligible_start_node_count().await?)),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => Ok(substrate.eligible_start_node_count()),
        }
    }

    /// Set the underlying scheduling primitive on the given node. The
    /// concrete substrate decides how (or whether) to honor this; substrates
    /// that have no scheduling primitive return `SubstrateError::Unsupported`.
    ///
    /// Keeping this on `Substrate` means admin handlers stay free of
    /// substrate-specific clients and the substrate boundary owns the
    /// scheduling primitive.
    ///
    /// Returns `Unsupported` exactly where
    /// [`SubstrateCapabilities::supports_node_drain`] is false, which the
    /// admin drain routes check before committing a phase; keep the two in
    /// step (`node_drain_capability_matches_set_node_schedulability` asserts
    /// it).
    pub async fn set_node_schedulability(
        &self,
        _request: NodeSchedulabilityRequest<'_>,
        _desired: NodeSchedulability,
    ) -> Result<(), SubstrateError> {
        match &self.inner {
            Inner::Direct(_) => Err(SubstrateError::Unsupported {
                message: "the active substrate does not support setting node schedulability"
                    .to_string(),
            }),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate.set_node_schedulability(_request, _desired).await
            }
        }
    }
}

/// Desired scheduling state of a node, applied via `set_node_schedulability`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum NodeSchedulability {
    /// New placements are allowed.
    Schedulable,
    /// New placements are blocked.
    Unschedulable,
}

/// Inputs required to identify a node when toggling its scheduling primitive.
///
/// This substrate-agnostic module deliberately stays generic about what
/// `substrate_node_id` means: it is an opaque string that the concrete
/// substrate decodes.
pub struct NodeSchedulabilityRequest<'a> {
    pub substrate_node_id: &'a str,
}

pub async fn enqueue_artifact_backup_with_client(
    client: &mut RuntimeClient,
    endpoint_addr: &str,
    sandbox_id: SandboxId,
    expected_kind: ArtifactKind,
    source: BackupSource,
) -> Result<EnqueuedArtifactBackup, SubstrateError> {
    let source = backup_source_to_proto(source);
    let response = client
        .enqueue_sandbox_artifact_backup(
            pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupRequest {
                sandbox_id: sandbox_id.to_string(),
                expected_artifact_kind: match expected_kind {
                    ArtifactKind::Disk => {
                        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32
                    }
                    ArtifactKind::Full => {
                        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32
                    }
                },
                source: Some(source),
            },
        )
        .await
        .map_err(|err| match err.code() {
            tonic::Code::NotFound => SubstrateError::NotFound {
                message: format!("sandbox {sandbox_id} not present on node-api at {endpoint_addr}"),
            },
            tonic::Code::FailedPrecondition => SubstrateError::Conflict {
                message: format!("node-api refused artifact backup: {}", err.message()),
            },
            _ => SubstrateError::Failed {
                message: format!("node-api artifact backup error: {err}"),
            },
        })?
        .into_inner();
    parse_enqueued_artifact_backup(response, expected_kind)
}

fn parse_enqueued_artifact_backup(
    response: pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupResponse,
    expected_kind: ArtifactKind,
) -> Result<EnqueuedArtifactBackup, SubstrateError> {
    let artifact_id = response
        .local_artifact_id
        .parse()
        .map_err(|err| SubstrateError::Failed {
            message: format!("node-api returned invalid local_artifact_id: {err}"),
        })?;
    let artifact_kind =
        match pplx_space_sbx_mgr_client::proto::ArtifactKind::try_from(response.artifact_kind) {
            Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk) => ArtifactKind::Disk,
            Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full) => ArtifactKind::Full,
            _ => {
                return Err(SubstrateError::Failed {
                    message: "node-api returned invalid artifact_kind".to_string(),
                });
            }
        };
    if artifact_kind != expected_kind {
        return Err(SubstrateError::Failed {
            message: format!(
                "node-api enqueued {artifact_kind} artifact, expected {expected_kind}"
            ),
        });
    }
    let attempt_id = response
        .backup_attempt_id
        .parse()
        .map_err(|err| SubstrateError::Failed {
            message: format!("node-api returned invalid backup_attempt_id: {err}"),
        })?;
    Ok(EnqueuedArtifactBackup {
        artifact: PublishedLocalArtifact {
            id: artifact_id,
            kind: artifact_kind,
        },
        attempt_id,
    })
}

async fn enqueue_artifact_backup_via_retrying_node_api(
    endpoint_addr: &str,
    sandbox_id: SandboxId,
    expected_kind: ArtifactKind,
    source: BackupSource,
) -> Result<EnqueuedArtifactBackup, SubstrateError> {
    let mut node_api = retrying_worker_node_api(endpoint_addr, Duration::from_secs(30))?;
    enqueue_artifact_backup_with_node_api(
        &mut node_api,
        endpoint_addr,
        sandbox_id,
        expected_kind,
        source,
    )
    .await
}

/// **Known cost, deliberately left.** Both callers resolve a `NodeApiTarget`
/// from a three-table join before calling in, and the cached client is returned
/// before that target is read — so on every call after the first for a node, the
/// join was paid for nothing. Making the target lazy would fix it, and would
/// also route the lookup's failure through one channel instead of the two the
/// callers classify it into today (`Publish` vs `Lookup`, which is the `phase`
/// tag on `node_drain.failures`). Left because the cost is three primary-key
/// lookups on small tables, incurred only while a drain is in flight, and the
/// fix touches every caller including the pre-existing suspend path. Revisit if
/// the lookup ever grows anything that is not a PK read.
pub async fn enqueue_artifact_backup_with_node_api(
    client: &mut NodeApiClient,
    endpoint_addr: &str,
    sandbox_id: SandboxId,
    expected_kind: ArtifactKind,
    source: BackupSource,
) -> Result<EnqueuedArtifactBackup, SubstrateError> {
    let source = backup_source_to_proto(source);
    let mut client = RetryingWorkerApiClient::new(client);
    let response = client
        .enqueue_sandbox_artifact_backup(
            pplx_space_sbx_mgr_client::proto::EnqueueArtifactBackupRequest {
                sandbox_id: sandbox_id.to_string(),
                expected_artifact_kind: match expected_kind {
                    ArtifactKind::Disk => {
                        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32
                    }
                    ArtifactKind::Full => {
                        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32
                    }
                },
                source: Some(source),
            },
            Duration::from_secs(30),
        )
        .await
        .map_err(|err| match err.code() {
            tonic::Code::NotFound => SubstrateError::NotFound {
                message: format!("sandbox {sandbox_id} not present on node-api at {endpoint_addr}"),
            },
            tonic::Code::FailedPrecondition => SubstrateError::Conflict {
                message: format!("node-api refused artifact backup: {}", err.message()),
            },
            _ => SubstrateError::Failed {
                message: format!("node-api artifact backup error: {err}"),
            },
        })?
        .into_inner();
    parse_enqueued_artifact_backup(response, expected_kind)
}

/// What the node itself reports for one sandbox: its status, and the artifact
/// its own row currently pins.
///
/// The pin is the part that matters. sbx-mgr decides whether to mint a fresh
/// disk artifact or reuse an existing one from *this* value, and nothing keeps
/// it in step with Postgres' column of the same name for a `stopped` sandbox —
/// the projection that would carry it up is gated to the suspend population.
/// So a caller that needs to know "will the next enqueue mint, or reuse?" has to
/// ask the node.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeReportedArtifact {
    /// The node has a row for this sandbox at all. `false` means its local
    /// state is already gone, so there is nothing to publish or tear down.
    pub present: bool,
    /// The node's own `current_local_artifact_id`, if it holds one and it could
    /// be read. `None` with [`Self::pin_unreadable`] set is *not* "pinless".
    pub pinned_artifact: Option<LocalArtifactId>,
    /// The node reported a pin, and it could not be parsed.
    ///
    /// Separate from [`Self::pinned_artifact`] so an unreadable value never
    /// travels as a nil UUID: that would reach a refusal's outcome and log line
    /// looking like a real artifact id, which is the objection
    /// `tear_down_stopped_local_copy` already raises for its own impossible
    /// case. A caller must treat this exactly as it treats a readable pin —
    /// refuse — because the whole point is that the bytes cannot be attributed.
    pub pin_unreadable: bool,
    /// The node reports the sandbox's lifecycle as `stopped`.
    ///
    /// **Not sufficient on its own** — see [`Self::vm_alive`]. The node's status
    /// classifier only overrides a persisted state when it can report `paused`
    /// (`pid_alive && vm_state == Paused`); every other combination returns the
    /// SQLite state as-is. So a row that says `stopped` while its Firecracker
    /// process is running reports `STOPPED` here.
    pub reports_stopped: bool,
    /// The Firecracker process is alive.
    ///
    /// This outranks [`Self::reports_stopped`], and the existing observation
    /// path treats it the same way — `sandbox_states_with_client` classifies
    /// `VmAlive` before it looks at the status or the artifact at all. A caller
    /// that acts on "the node says stopped" without checking this can capture
    /// and then destroy a rootfs that a running guest is still writing to.
    ///
    /// **Two node-side signals feed it, and either one is enough.** The node
    /// reports `runtime_alive` from the watchdog's cached health record,
    /// falling back to a live `is_sandbox_running(id)` probe only when the
    /// watchdog has no record, and it sets `namespace` only when that same
    /// live probe succeeds (`live_sandbox_namespace` in
    /// `sbx/mgr/server/src/service/grpc_state_snapshot.rs`). A stale-`false`
    /// health record would otherwise read as `reports_stopped = true, vm_alive
    /// = false` while the process runs, so this is the OR of the two. `pid`
    /// would not do: the node reports it only when the row also holds one,
    /// and a row that went through a stop path that clears it holds none.
    pub vm_alive: bool,
    /// The node flagged its answer as degraded. Three paths set it
    /// (`grpc_state_snapshot.rs`): the node couldn't read its row, or couldn't
    /// build an answer from it, and published a stub carrying the raw
    /// lifecycle with the pin and liveness at their defaults; or the row was
    /// fine but its route provenance wasn't. In the stub an absent pin, a dead
    /// VM and `reports_stopped` aren't answers, and read literally they're the
    /// one shape a fresh evacuation may open on: present, stopped, pinless, not
    /// alive. A caller must treat any of the three as the node disagreeing and
    /// defer. That's fail-closed, and for the
    /// provenance case it holds the node for as long as the provenance stays
    /// unreadable.
    pub state_degraded: bool,
    /// When the node's row last stamped its state. The writers in
    /// `local_artifacts.rs`, `cleanup_episodes.rs` and `state_write.rs` that
    /// move a row into `stopped` stamp it only on a state change, and
    /// `transfer.rs` reaches `stopped` only from `creating`, so for a row one
    /// of them stopped it is the current stopped episode's start rather than a
    /// last-touch. Startup recovery restamps without a state change, but never
    /// a `stopped` row.
    ///
    /// For a stopped sandbox this is the instant its rootfs froze: a start
    /// that boots the guest and fails moves the row out of `stopped` and back,
    /// restamping it. That makes it half of [`Self::pin_capture_order`];
    /// [`Self::pin_captured_at`] is the other.
    pub stopped_since: Option<prost_types::Timestamp>,
    /// When the node recorded [`Self::pinned_artifact`], from the node's
    /// `local_artifacts.created_at`: stamped when the catalog row is inserted
    /// and never rewritten. Same clock as [`Self::stopped_since`]. A `stopped`
    /// sandbox's pin is recorded just after its snapshot is taken, either as
    /// the row enters `stopped` or later on the already-stopped row (the
    /// ordinary stop-time mint), or, for a disk a stopped evacuation restored
    /// onto this node, as the rootfs is materialized from it, so the rootfs is
    /// that artifact. Compare it only
    /// when [`Self::reports_stopped`]; other recordings (a suspend artifact
    /// adopted at backup time, a restored suspend checkpoint) can lag the
    /// capture.
    ///
    /// `None` whenever there's no readable pin, and also when the node doesn't
    /// report it: older nodes don't, and a node omits it for a pin whose
    /// catalog row is missing, unreadable, or recorded by an older node. Read
    /// `None` as unknown, never as old or new.
    pub pin_captured_at: Option<prost_types::Timestamp>,
}

impl NodeReportedArtifact {
    /// How the pinned artifact's capture time compares with the instant the
    /// row entered its current state, or `None` when either is unknown.
    ///
    /// Both are the node's own clock, which is what makes the comparison
    /// meaningful at all; a cluster timestamp against either would let clock
    /// skew accept a stale artifact. They're whole epoch seconds, and the
    /// clock is the wall clock rather than a monotonic one, so the orderings
    /// mean:
    ///
    /// * `Greater`: captured in a later second than the row stopped, so the
    ///   artifact describes the rootfs as it stands.
    /// * `Equal`: same second, which is the ordinary case: the execution
    ///   retire stops the sandbox and enqueues its disk backup straight after,
    ///   so the stop-time mint usually lands in the stop's own second. On its
    ///   own the second can't separate that from a mint followed by a start
    ///   that boots, writes and fails within it. The node closes that case
    ///   instead: it drops a `stopped` row's pin when a start that launched
    ///   fails and before a process-snapshot rollback rewrites the disk, and
    ///   it refuses to pin a disk backup whose row has started since the
    ///   backup took its snapshot, so a pin that survives on a `stopped` row
    ///   was taken after the last write. A node reports a capture time only for
    ///   an artifact it recorded under all three, which shipped before the
    ///   capture time did; an artifact recorded by an older node has none.
    /// * `Less`: captured before the row's current stopped episode began, so
    ///   the rootfs may have changed since. Reusing it is the data-loss case.
    ///   Not every `Less` pin is stale: a start refused before it launched
    ///   rolls the row back to `stopped`, restamping it, and keeps the pin of
    ///   a rootfs nothing touched. Such a sandbox isn't evacuated, which fails
    ///   closed.
    ///
    /// A backward step of the node's clock between a mint and a later restamp
    /// can make a stale artifact read as current. That residual isn't
    /// closable from a wall-clock second, and is accepted.
    ///
    /// A time before the Unix epoch, or with nanos outside `0..1_000_000_000`,
    /// is unknown too. The node never writes one, so it can only come from a
    /// corrupt row, and read literally a negative stop time is older than every
    /// capture, which would make any pin look current.
    pub fn pin_capture_order(&self) -> Option<std::cmp::Ordering> {
        let valid = |time: &prost_types::Timestamp| {
            time.seconds >= 0 && (0..1_000_000_000).contains(&time.nanos)
        };
        let captured = self.pin_captured_at.as_ref().filter(|time| valid(time))?;
        let stopped = self.stopped_since.as_ref().filter(|time| valid(time))?;
        Some((captured.seconds, captured.nanos).cmp(&(stopped.seconds, stopped.nanos)))
    }

    /// The node shows its pin was captured during the current stopped
    /// episode: `Greater` or `Equal` (see [`Self::pin_capture_order`]). The
    /// only orders on which a fresh stopped evacuation may reuse a pin. An
    /// unknown order proves nothing, so it's refused.
    pub fn pin_shown_current(&self) -> bool {
        matches!(
            self.pin_capture_order(),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        )
    }
}

/// Which of three things the node's `current_local_artifact_id` says, given that
/// the wire type is an `optional string` and two of the three are unreadable.
///
/// **The empty string belongs with the malformed value, not with the absent
/// one.** `None` means the node reported no pin — a real answer, and the only
/// one a fresh evacuation may proceed on. Anything present but unreadable, `""`
/// included, is a pin we cannot identify, and reporting *that* as pinless would
/// let a teardown run on evidence nobody could read.
///
/// Unreadable is signalled by the returned flag rather than by a nil UUID. A nil
/// id would travel into the refusal's outcome and its log line in a form
/// indistinguishable from a real artifact — the exact objection
/// `tear_down_stopped_local_copy` raises for its own impossible case, and it
/// would be inconsistent to accept the shape here after rejecting it there.
///
/// Extracted from the RPC so the table above is testable without a node.
fn parse_reported_pin(raw: Option<&str>, sandbox_id: SandboxId) -> (Option<LocalArtifactId>, bool) {
    // Matching the `Option` rather than flattening it, because that is the whole
    // distinction: `unwrap_or_default()` turns an absent field and an empty
    // string into the same `""`, which forces empty to be read as pinless to
    // keep absent working — reintroducing exactly the asymmetry this function
    // exists to remove.
    let Some(raw) = raw else {
        return (None, false);
    };
    match raw.parse::<LocalArtifactId>() {
        Ok(id) => (Some(id), false),
        Err(_) => {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                raw,
                "node reported an unreadable current local artifact id; treating the \
                 sandbox as pinned so nothing is destroyed on evidence we cannot read"
            );
            (None, true)
        }
    }
}

/// Read one sandbox's node-local state through the same connection the publish
/// uses. `SandboxState` returns `SandboxInfo`, which carries the node's artifact
/// identity; a sandbox the node does not know about comes back in `missing_ids`.
pub async fn sandbox_state_with_node_api(
    client: &mut NodeApiClient,
    endpoint_addr: &str,
    sandbox_id: SandboxId,
) -> Result<NodeReportedArtifact, SubstrateError> {
    let mut client = RetryingWorkerApiClient::new(client);
    let response = client
        .sandbox_state(
            pplx_space_sbx_mgr_client::proto::StateRequest {
                sandbox_ids: vec![sandbox_id.to_string()],
            },
            Duration::from_secs(30),
        )
        .await
        .map_err(|err| match err.code() {
            tonic::Code::NotFound => SubstrateError::NotFound {
                message: format!("sandbox {sandbox_id} not present on node-api at {endpoint_addr}"),
            },
            _ => SubstrateError::Failed {
                message: format!("node-api sandbox state error: {err}"),
            },
        })?
        .into_inner();
    let Some(info) = response
        .sandboxes
        .into_iter()
        .find(|info| info.sandbox_id == sandbox_id.to_string())
    else {
        // Either explicitly in `missing_ids` or simply absent. Both mean the
        // node has no row, which is a refusal rather than a pinless yes: a
        // sandbox the node does not know about has no rootfs there to publish.
        return Ok(NodeReportedArtifact::default());
    };
    Ok(node_reported_artifact_from_info(info, sandbox_id))
}

/// What the node's `SandboxInfo` says about the evacuation guard's inputs.
/// Extracted from the RPC so the mapping is testable without a node.
fn node_reported_artifact_from_info(
    info: pplx_space_sbx_mgr_client::proto::SandboxInfo,
    sandbox_id: SandboxId,
) -> NodeReportedArtifact {
    let (pinned_artifact, pin_unreadable) =
        parse_reported_pin(info.current_local_artifact_id.as_deref(), sandbox_id);
    // Only beside a readable pin: a time with no artifact to attach it to is
    // evidence about nothing.
    let pin_captured_at = info
        .current_local_artifact_created_at
        .filter(|_| pinned_artifact.is_some());
    NodeReportedArtifact {
        present: true,
        pinned_artifact,
        pin_unreadable,
        // An undecodable lifecycle reads as not stopped, which the caller
        // treats as the node disagreeing: a deferral, never a teardown.
        reports_stopped: matches!(
            info.observed_lifecycle(),
            Ok(pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Stopped)
        ),
        // `namespace`, not `pid`: the node sets `namespace` from its live
        // process probe alone, while `pid` also needs the row's persisted pid,
        // which every write into `stopped` clears.
        vm_alive: info.runtime_alive || info.namespace.is_some(),
        state_degraded: info.state_degraded,
        stopped_since: info.state_changed_at,
        pin_captured_at,
    }
}

fn backup_source_to_proto(
    source: BackupSource,
) -> pplx_space_sbx_mgr_client::proto::enqueue_artifact_backup_request::Source {
    match source {
        BackupSource::CurrentInstance => {
            pplx_space_sbx_mgr_client::proto::enqueue_artifact_backup_request::Source::CurrentInstance(
                pplx_space_sbx_mgr_client::proto::CurrentInstanceSource {},
            )
        }
        BackupSource::Snapshot(snapshot_name) => {
            pplx_space_sbx_mgr_client::proto::enqueue_artifact_backup_request::Source::SnapshotName(
                snapshot_name.to_string(),
            )
        }
        BackupSource::LocalArtifact(local_artifact_id) => {
            pplx_space_sbx_mgr_client::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(
                local_artifact_id.to_string(),
            )
        }
    }
}

impl SubstrateAdapter for Substrate {
    fn managed_by(&self) -> &'static str {
        match &self.inner {
            Inner::Direct(substrate) => substrate.managed_by(),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.managed_by(),
        }
    }

    fn capabilities(&self) -> SubstrateCapabilities {
        match &self.inner {
            Inner::Direct(substrate) => substrate.capabilities(),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.capabilities(),
        }
    }

    async fn create_execution(
        &self,
        request: CreateExecutionRequest<'_>,
    ) -> Result<CreateExecutionOutcome, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.create_execution(request).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.create_execution(request).await,
        }
    }

    async fn complete_activation_lease(
        &self,
        sandbox_id: SandboxId,
        completion: Option<ActivationLeaseCompletion>,
    ) {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .complete_activation_lease(sandbox_id, completion)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .complete_activation_lease(sandbox_id, completion)
                    .await
            }
        }
    }

    async fn delete_execution(&self, sandbox_id: SandboxId) -> Result<(), SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.delete_execution(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.delete_execution(sandbox_id).await,
        }
    }

    async fn retire_execution(
        &self,
        request: RetireExecutionRequest,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.retire_execution(request).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.retire_execution(request).await,
        }
    }

    async fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.retire_fence_mismatch(sandbox_id, fence).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate.retire_fence_mismatch(sandbox_id, fence).await
            }
        }
    }

    async fn observe_pod_identities(
        &self,
        pod_names: &[PodName],
    ) -> HashMap<PodName, ObservedPodIdentity> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.observe_pod_identities(pod_names).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.observe_pod_identities(pod_names).await,
        }
    }

    async fn delete_observed_execution(
        &self,
        sandbox_id: SandboxId,
        target: &NodeApiTarget<'_>,
    ) -> Result<(), SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .delete_observed_execution(sandbox_id, target)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .delete_observed_execution(sandbox_id, target)
                    .await
            }
        }
    }

    async fn start_execution(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .start_execution(metric_identity, sandbox_id, options, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .start_execution(metric_identity, sandbox_id, options, charge)
                    .await
            }
        }
    }

    async fn restore_execution(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox_id: SandboxId,
        options: ActivationOptions,
        charge: RecoveryCharge,
    ) -> Result<ActivationCompletion, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .restore_execution(metric_identity, sandbox_id, options, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .restore_execution(metric_identity, sandbox_id, options, charge)
                    .await
            }
        }
    }

    async fn ensure_routable_execution(
        &self,
        sandbox_id: SandboxId,
        budget: Duration,
    ) -> Result<ExecutionRoutabilityCheck, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .ensure_routable_execution(sandbox_id, budget)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .ensure_routable_execution(sandbox_id, budget)
                    .await
            }
        }
    }

    async fn suspend_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.suspend_execution(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.suspend_execution_tracked(sandbox_id).await,
        }
    }

    async fn enqueue_artifact_backup(
        &self,
        sandbox_id: SandboxId,
        expected_kind: ArtifactKind,
        source: BackupSource,
        target: &NodeApiTarget<'_>,
    ) -> Result<EnqueuedArtifactBackup, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .enqueue_artifact_backup(sandbox_id, expected_kind, source, target)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .enqueue_artifact_backup(sandbox_id, expected_kind, source, target)
                    .await
            }
        }
    }

    async fn create_snapshot_from_durable_artifact(
        &self,
        target: &NodeApiTarget<'_>,
        operation_id: OperationId,
        source_sandbox_id: SandboxId,
        target_sandbox_id: SandboxId,
        source_storage_uri: String,
        destination_storage_uri: String,
    ) -> Result<PublishedDurableSnapshot, DurableSnapshotError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                SubstrateAdapter::create_snapshot_from_durable_artifact(
                    substrate.as_ref(),
                    target,
                    operation_id,
                    source_sandbox_id,
                    target_sandbox_id,
                    source_storage_uri,
                    destination_storage_uri,
                )
                .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                SubstrateAdapter::create_snapshot_from_durable_artifact(
                    substrate.as_ref(),
                    target,
                    operation_id,
                    source_sandbox_id,
                    target_sandbox_id,
                    source_storage_uri,
                    destination_storage_uri,
                )
                .await
            }
        }
    }

    async fn wake_execution(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        wake_activity: Option<ExecutionActivityKind>,
        charge: RecoveryCharge,
    ) -> Result<ExecutionActivation, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .wake_execution(metric_identity, sandbox, timeout, wake_activity, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .wake_execution(metric_identity, sandbox, timeout, wake_activity, charge)
                    .await
            }
        }
    }

    async fn runtime_client(
        &self,
        sandbox_id: SandboxId,
        charge: RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.runtime_client(sandbox_id, charge).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.runtime_client(sandbox_id, charge).await,
        }
    }

    async fn reachable_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.reachable_runtime_client(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.reachable_runtime_client(sandbox_id).await,
        }
    }

    async fn probe_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.probe_runtime_client(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.probe_runtime_client(sandbox_id).await,
        }
    }

    async fn access_runtime_client(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        activity: ExecutionActivityKind,
        timeout: Duration,
        charge: RecoveryCharge,
    ) -> Result<RuntimeClient, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .access_runtime_client(metric_identity, sandbox, activity, timeout, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .access_runtime_client(metric_identity, sandbox, activity, timeout, charge)
                    .await
            }
        }
    }

    async fn builder_client(&self) -> Result<BuilderClient, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.builder_client().await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.builder_client().await,
        }
    }

    async fn builder_client_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<BuilderClient, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.builder_client_for_node(node_id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.builder_client_for_node(node_id).await,
        }
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
        match &self.inner {
            Inner::Direct(substrate) => substrate.pool_eligible_for_request(
                boot_mode,
                backend,
                wait_until_ready,
                disk_mib,
                file_changes_enabled,
                implicit_wake_enabled,
            ),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.pool_eligible_for_request(
                boot_mode,
                backend,
                wait_until_ready,
                disk_mib,
                file_changes_enabled,
                implicit_wake_enabled,
            ),
        }
    }

    fn pool_supports_request_env(&self) -> bool {
        match &self.inner {
            Inner::Direct(substrate) => substrate.pool_supports_request_env(),
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.pool_supports_request_env(),
        }
    }

    async fn claim_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.claim_pooled_execution(request).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.claim_pooled_execution(request).await,
        }
    }

    async fn lookup_admitted_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
        admitted: AdmittedPoolClaim,
        sandbox_id: SandboxId,
    ) -> Result<AdmittedPoolClaimLookup, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .lookup_admitted_pooled_execution(request, admitted, sandbox_id)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .lookup_admitted_pooled_execution(request, admitted, sandbox_id)
                    .await
            }
        }
    }

    async fn skip_admitted_pool_generation(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<(), ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.skip_admitted_pool_generation(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate.skip_admitted_pool_generation(sandbox_id).await
            }
        }
    }

    async fn admitted_pool_cleanup_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.admitted_pool_cleanup_pending(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate.admitted_pool_cleanup_pending(sandbox_id).await
            }
        }
    }

    async fn claim_admission_pool_slot(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
    ) -> Result<Option<AdmissionPoolClaim>, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .claim_admission_pool_slot(tx, template_release_id)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .claim_admission_pool_slot(tx, template_release_id)
                    .await
            }
        }
    }

    async fn persist_pooled_execution_claim(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .persist_pooled_execution_claim(tx, request, claim)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .persist_pooled_execution_claim(tx, request, claim)
                    .await
            }
        }
    }

    async fn rollback_pooled_execution_claim(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<(), ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.rollback_pooled_execution_claim(claim).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.rollback_pooled_execution_claim(claim).await,
        }
    }

    async fn cleanup_failed_pooled_prepare(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.cleanup_failed_pooled_prepare(claim).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.cleanup_failed_pooled_prepare(claim).await,
        }
    }

    async fn apply_pooled_execution_auto_lifecycle(
        &self,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .apply_pooled_execution_auto_lifecycle(claim, policy)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .apply_pooled_execution_auto_lifecycle(claim, policy)
                    .await
            }
        }
    }

    async fn prepare_pooled_execution(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        request: &PooledExecutionClaimRequest<'_>,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
        wait_until_ready: bool,
        timeout: Duration,
    ) -> Result<(), ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => {
                substrate
                    .prepare_pooled_execution(
                        metric_identity,
                        request,
                        claim,
                        policy,
                        wait_until_ready,
                        timeout,
                    )
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => {
                substrate
                    .prepare_pooled_execution(
                        metric_identity,
                        request,
                        claim,
                        policy,
                        wait_until_ready,
                        timeout,
                    )
                    .await
            }
        }
    }

    async fn list_pool_configs(&self) -> Result<Vec<ClusterPoolConfigResponse>, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.list_pool_configs().await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.list_pool_configs().await,
        }
    }

    async fn upsert_pool_config(
        &self,
        request: ClusterPoolConfigRequest,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.upsert_pool_config(request).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.upsert_pool_config(request).await,
        }
    }

    async fn disable_pool_config(
        &self,
        id: Uuid,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.disable_pool_config(id).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.disable_pool_config(id).await,
        }
    }

    async fn resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.resolve_node_api_endpoint(target).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.resolve_node_api_endpoint(target).await,
        }
    }

    async fn select_ready_worker_node_api_target(
        &self,
    ) -> Result<OwnedNodeApiTarget, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.select_ready_worker_node_api_target().await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.select_ready_worker_node_api_target().await,
        }
    }

    async fn list_inventory_nodes(
        &self,
        role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        match &self.inner {
            Inner::Direct(substrate) => substrate.list_inventory_nodes(role_filter).await,
            #[cfg(feature = "kubernetes")]
            Inner::Kubernetes(substrate) => substrate.list_inventory_nodes(role_filter).await,
        }
    }
}

#[cfg(test)]
mod tests;
