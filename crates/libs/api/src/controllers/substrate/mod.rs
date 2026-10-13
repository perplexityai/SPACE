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

type RuntimeTransport = GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>>;

/// Runtime RPCs and capability checks share the resolved worker connection.
#[derive(Clone, Debug)]
pub struct RuntimeClient {
    worker: WorkerApiClient<RuntimeTransport>,
    channel: Channel,
}

impl RuntimeClient {
    fn new(channel: Channel) -> Self {
        let transport = node_api_channel(channel.clone(), SpaceBearerToken::NodeApi);
        Self {
            worker: WorkerApiClient::new(transport),
            channel,
        }
    }

    pub(super) async fn require_activation_policy_support(
        &mut self,
        timeout: Duration,
    ) -> Result<(), ControllerError> {
        require_activation_policy_support(
            node_api_channel(self.channel.clone(), SpaceBearerToken::NodeApi),
            timeout,
        )
        .await
    }

    fn internal_client(&self) -> RuntimeInternalClient {
        WorkerApiInternalClient::new(node_api_channel(
            self.channel.clone(),
            SpaceBearerToken::NodeApiInternal,
        ))
    }
}

impl std::ops::Deref for RuntimeClient {
    type Target = WorkerApiClient<RuntimeTransport>;

    fn deref(&self) -> &Self::Target {
        &self.worker
    }
}

impl std::ops::DerefMut for RuntimeClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.worker
    }
}
pub type RuntimeInternalClient =
    WorkerApiInternalClient<GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>>>;
pub type BuilderClient =
    BuilderApiClient<GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>>>;

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

/// Per-request node-RPC channel timeout (the tonic `Endpoint::timeout` on
/// worker/guest channels). This — NOT the ALB — is the binding internal cut
/// behind the create/restore latency cliff: a slow-but-not-stuck node RPC that
/// rides up to it is severed by tonic as `Code::Cancelled`, which surfaces as
/// an opaque HTTP 408 with no sandbox id and no retry semantics. Every API-side
/// blocking wait must finish, *and* a structured retryable response must flush,
/// below it. (The ECW ALB idle timeout is far higher — see
/// [`ALB_IDLE_TIMEOUT_SECS`] — so the ALB is not the binding constraint.)
///
/// The 150s timeout leaves a 135s blocking budget after reserving 15s for
/// completing the response.
///
/// The value lives in `common-core`'s `restore_ownership` module because the
/// placement crate's restore lease TTL must equal it; deriving both from one
/// constant is what keeps a future timeout bump from silently splitting the
/// lease-outlives-RPC invariant across crates.
pub(crate) const NODE_RPC_TIMEOUT: Duration =
    pplx_space_common_core::restore_ownership::NODE_RPC_TIMEOUT;
pub(crate) const NODE_RPC_TIMEOUT_SECS: u64 = NODE_RPC_TIMEOUT.as_secs();
const NODE_API_HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const NODE_API_HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);
/// Headroom below [`NODE_RPC_TIMEOUT`] for the API poll loop / wait wrapper to
/// detect exhaustion and return a structured, retryable response (carrying the
/// sandbox id + `Retry-After`) before tonic severs the channel with a bare
/// `Cancelled` 408.
const BLOCKING_WAIT_MARGIN_SECS: u64 = 15;
/// API-side blocking-wait budget for create/restore/wake/access, held below
/// [`NODE_RPC_TIMEOUT`] so a slow wait surfaces as a structured retryable
/// `SubstrateError::Timeout` before the per-request channel timeout fires. The
/// single budget every blocking lifecycle wait is sized against.
///
/// `pub` (not `pub(crate)`) so out-of-crate callers that pass a
/// `ready_timeout` — e.g. the cluster-controller's node-drain restore —
/// can report the effective (clamped) per-attempt wait instead of
/// hand-copying this value into comments and logs.
pub const NODE_RPC_BLOCKING_BUDGET: Duration =
    Duration::from_secs(NODE_RPC_TIMEOUT_SECS - BLOCKING_WAIT_MARGIN_SECS);
/// The ECW ALB idle timeout (`idle_timeout.timeout_seconds`), set via the AWS
/// LB controller annotation on the deployment's Ingress. It is the OUTER
/// ceiling, far above the node-RPC
/// timeout — documented here so the hierarchy is explicit and version-checked;
/// keep it in sync with the Helm values. The static assert pins
/// `NODE_RPC_TIMEOUT < ALB idle`, so the binding cut can never silently exceed
/// the gateway.
pub(crate) const ALB_IDLE_TIMEOUT_SECS: u64 = 1800;
const _: () = assert!(
    NODE_RPC_TIMEOUT_SECS < ALB_IDLE_TIMEOUT_SECS,
    "node-RPC channel timeout must stay below the ALB idle timeout",
);
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

pub(crate) fn node_api_endpoint(
    addr: &str,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Result<Endpoint, SubstrateError> {
    let uri = pplx_space_common_grpc::internal_transport::qualify_endpoint(addr);
    Endpoint::from_shared(uri)
        .map(|endpoint| configure_node_api_endpoint(endpoint, connect_timeout, request_timeout))
        .map_err(|err| SubstrateError::Failed {
            message: format!("invalid node-api endpoint '{addr}': {err}"),
        })
}

fn configure_node_api_endpoint(
    endpoint: Endpoint,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Endpoint {
    pplx_space_node_api_client::configure_node_api_endpoint(
        endpoint,
        NodeApiEndpointConfig::default()
            .with_connect_timeout(connect_timeout)
            .with_request_timeout(request_timeout)
            .with_http2_keep_alive(
                NODE_API_HTTP2_KEEP_ALIVE_INTERVAL,
                NODE_API_HTTP2_KEEP_ALIVE_TIMEOUT,
            ),
    )
}

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

pub(crate) fn default_pool_eligible_for_request(
    boot_mode: CreateBootMode,
    backend: &str,
    wait_until_ready: bool,
    disk_mib: Option<u32>,
    file_changes_enabled: bool,
    implicit_wake_enabled: bool,
) -> bool {
    backend == "fc"
        && matches!(
            boot_mode,
            CreateBootMode::Fallthrough | CreateBootMode::Pool
        )
        && disk_mib.is_none()
        && !file_changes_enabled
        && (wait_until_ready || implicit_wake_enabled)
}

/// Combine a substrate's generic warm-pool eligibility with env handling. A cold
/// create bakes `req.env` into the sandbox at boot; if the substrate can't
/// reproduce that on a pooled sandbox at adoption (`substrate_supports_pool_env`
/// is false), an env-bearing create must take the cold path rather than silently
/// drop the requested variables.
pub(crate) fn pool_eligible_with_env(
    substrate_eligible: bool,
    substrate_supports_pool_env: bool,
    has_request_env: bool,
) -> bool {
    substrate_eligible && (substrate_supports_pool_env || !has_request_env)
}

#[cfg(test)]
mod pool_eligibility_tests;

#[derive(Debug, Clone, Copy)]
pub struct ActivationOptions {
    pub blocking: bool,
    pub ready_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedLocalArtifact {
    pub id: LocalArtifactId,
    pub kind: ArtifactKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnqueuedArtifactBackup {
    pub artifact: PublishedLocalArtifact,
    pub attempt_id: BackupAttemptId,
}

impl PublishedLocalArtifact {
    pub(crate) fn from_published(
        published: Option<pplx_space_sbx_mgr_client::proto::PublishedArtifact>,
        operation: &str,
        expected_kind: ArtifactKind,
    ) -> Result<Self, SubstrateError> {
        let published = published.ok_or_else(|| SubstrateError::Failed {
            message: format!("blocking {operation} returned no local_artifact_id"),
        })?;
        let id = published
            .local_artifact_id
            .parse()
            .map_err(|err| SubstrateError::Failed {
                message: format!("blocking {operation} returned invalid local_artifact_id: {err}"),
            })?;
        let kind =
            match pplx_space_sbx_mgr_client::proto::ArtifactKind::try_from(published.artifact_kind)
            {
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Full) => ArtifactKind::Full,
                Ok(pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk) => ArtifactKind::Disk,
                _ => {
                    return Err(SubstrateError::Failed {
                        message: format!("blocking {operation} returned unsupported artifact_kind"),
                    });
                }
            };
        if kind != expected_kind {
            return Err(SubstrateError::Failed {
                message: format!(
                    "blocking {operation} returned {kind} artifact, expected {expected_kind}"
                ),
            });
        }
        Ok(Self { id, kind })
    }

    pub(crate) fn from_suspend_response(
        response: pplx_space_sbx_mgr_client::proto::SuspendResponse,
    ) -> Result<Self, SubstrateError> {
        Self::from_published(response.published_artifact, "suspend", ArtifactKind::Full)
    }

    pub(crate) fn from_committed_suspend_response(
        response: pplx_space_sbx_mgr_client::proto::SuspendResponse,
        sandbox_id: SandboxId,
        substrate: &'static str,
    ) -> Option<Self> {
        match Self::from_suspend_response(response) {
            Ok(published_artifact) => Some(published_artifact),
            Err(err) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    substrate,
                    error = %err,
                    "sandbox suspend completed but returned unusable artifact metadata"
                );
                None
            }
        }
    }
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

/// Call only after locking the sandbox row in the same transaction. Retention
/// takes that lock before changing a snapshot from `ready` to `deleting`, so a
/// later statement observes the committed winner and cannot pin a claimed
/// artifact for restore.
pub(crate) async fn reject_deleting_current_local_artifact(
    tx: &mut Transaction<'_, Postgres>,
    sandbox_id: SandboxId,
    artifact: PublishedLocalArtifact,
) -> Result<(), SubstrateError> {
    let deleting_snapshot = deleting_snapshot_matches_artifact("$1", "$2", "$3");
    let query = format!("SELECT {deleting_snapshot}");
    let deleting = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(sandbox_id.as_uuid())
        .bind(artifact.id.as_uuid())
        .bind(artifact.kind.as_str())
        .fetch_one(&mut **tx)
        .await
        .db_context("failed to check current artifact retention claim")?;
    if deleting {
        return Err(SubstrateError::Unavailable {
            message: format!(
                "local artifact {} is already claimed for snapshot deletion",
                artifact.id
            ),
        });
    }
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

/// Pod identity a caller observed and wants a retire pinned to.
///
/// `retire_execution` addresses a sandbox by id, resolves whatever pod is
/// tracked at call time, and tears it down. Callers reach that call after a
/// probe and a CAS, and the pod can change in between — recover, be replaced
/// by a same-name successor, or be superseded by a newer-generation execution.
/// A fence carries the identity the caller actually saw so the substrate can
/// refuse instead of acting on stale evidence.
///
/// The name is required; the other two are what the caller happened to know,
/// and each one it has strengthens the fence:
///
/// * **Name alone** may act only on an *absent* pod. It pins the teardown to
///   one execution — the control plane can answer "is this still the active
///   execution?", which catches a successor — but it can prove nothing about
///   a live object at that name, so a live pod refuses. This is the shape the
///   sweeper falls back to for an execution whose pod UID has not been
///   persisted yet, which is a real state: the UID is written after the pod is
///   created. It is what lets the sweep still collect rows whose pod is gone,
///   without ever deleting a pod whose identity nobody observed. The state is
///   self-clearing: once pod metadata persists, the next candidate select
///   carries a UID and the full fence applies.
/// * **`pod_uid`** adds the apiserver-enforced precondition, catching a
///   same-name pod that was replaced.
/// * **`resource_version`** catches the same pod having *moved* — bound,
///   status changed, recovered. Only a caller that read the live pod object
///   has one; `sandbox_k8s_executions` has no column for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodIdentityFence {
    pub pod_name: PodName,
    pub pod_uid: Option<PodUid>,
    /// What the caller's execution row said about itself when it was read.
    /// Distinct from [`Self::pod_uid`], which is what the caller saw of
    /// the live object — a row can record a UID whose pod is long gone, and a
    /// row re-claimed for a replacement records none while its predecessor's
    /// pod is still there.
    pub recorded: RecordedExecution,
    pub resource_version: Option<PodResourceVersion>,
    /// The activation attempt the caller selected.
    ///
    /// Neither the generation nor the pod name identifies an *attempt*: an
    /// activation retry or a cross-node redrive reuses both, and the pod name
    /// is derived from the generation. So a replacement attempt can look
    /// identical to the one the caller observed — same generation, same name,
    /// and before it materializes, the same absent UID. This is the only field
    /// that tells them apart, which matters most where the rest of the fence
    /// is weakest.
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    /// The execution generation the caller selected, when it recorded one.
    pub generation: Option<i64>,
}

/// What the execution row records about itself.
///
/// Grouped by *who observed it* rather than by field. Everything inside
/// [`Self::Read`] came from one read of one row, so it is all known or all
/// unknown together, and "the caller never looked" is one state rather than a
/// `None` on each field. That distinction is the whole point: for a row, each
/// of these absences is a *value* — a re-claimed row records no pod, a
/// completed activation records no attempt — so an `Option` per field would
/// let "I did not look" authorize exactly what "the row records nothing"
/// should be caught by.
///
/// A new fact the row keeps about itself goes inside `Read`, where the
/// exhaustive destructure in [`ExecutionIdentity::reconcile`] forces it to be
/// given a rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedExecution {
    /// The caller did not read the row, so it constrains nothing.
    Unread,
    /// What the row said when the caller read it.
    Read {
        /// Which pod the row claims as its own. Cleared by a replacement
        /// claim, and left behind when a pod is deleted, so it is neither the
        /// live pod's UID nor derivable from it.
        pod_uid: Option<PodUid>,
        /// The durable identity of the activation that owns this execution.
        /// Outlives the ephemeral attempt id, which is cleared on completion —
        /// so two activations that have both completed are otherwise
        /// indistinguishable, and this is what tells them apart.
        route_activation_id: Option<SandboxActivationAttemptId>,
    },
}

impl RecordedExecution {
    /// The row's pod claim. `Unread` is not a row state; only row-derived
    /// values are ever bound into SQL, and those are never `Unread`.
    #[must_use]
    pub fn pod_uid(&self) -> Option<&PodUid> {
        match self {
            Self::Read { pod_uid, .. } => pod_uid.as_ref(),
            Self::Unread => None,
        }
    }

    /// The row's durable activation identity, with the same caveat.
    #[must_use]
    pub fn route_activation_id(&self) -> Option<SandboxActivationAttemptId> {
        match self {
            Self::Read {
                route_activation_id,
                ..
            } => *route_activation_id,
            Self::Unread => None,
        }
    }
}

/// What a caller saw of the pod object itself.
///
/// A sum type rather than two more `Option`s because "I could not see the pod"
/// and "I saw that there is no pod" authorize opposite things, and an `Option`
/// spells them the same way. Every bug this fence has had came from a site
/// reading an absent field as "no expectation here" — so absence is spelled
/// out as a variant that has to be matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedPod {
    /// The caller had no view of the pod object. Nothing about a live pod can
    /// be proven from such an observation, so it may act only on a pod that is
    /// *still* absent.
    Unobserved,
    /// The caller looked and there was no pod at that name.
    Absent,
    /// The caller saw this exact object.
    Seen {
        pod_uid: PodUid,
        /// Only a caller that read the live object has one; the execution
        /// table has no column for it.
        resource_version: Option<PodResourceVersion>,
        /// Whether the pod has reached a phase it cannot leave.
        ///
        /// A pod that is merely running can still *change* — Pending going
        /// Running keeps its UID, which is why a caller with no
        /// resourceVersion may not act on one. A terminal pod cannot, so its
        /// UID is a complete identification. That distinction is what keeps
        /// terminal pods collectable at all: the filtered reflector drops
        /// them, so no caller can ever observe one, and without this every
        /// terminal pod would defer forever behind a name-only fence.
        terminal: bool,
    },
}

/// One execution's identity, as observed at a single instant.
///
/// This type exists because "is this still the execution I observed?" was
/// being answered independently at six places — the pod-name check, the
/// attempt check, the live-pod check, the apiserver delete precondition, the
/// tracking-release CAS, and the UID backfill — and those six disagreed about
/// which fields matter *and* about what an absent field means. Every review
/// finding against this fence was one of those six picking a wrong subset or
/// reading absence as a wildcard. One value, compared in one place, is what
/// makes that class of bug unrepresentable rather than repeatedly patched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionIdentity {
    pub pod_name: PodName,
    pub generation: Option<i64>,
    pub activation_attempt_id: Option<SandboxActivationAttemptId>,
    /// What the execution row said about itself when the caller read it.
    ///
    /// Distinct from [`Self::pod`], which is the live object, and not
    /// derivable from it. This is the only part of the identity that can catch
    /// a row changing underneath a caller whose pod is absent — the case where
    /// there is no live object left to compare at all. It is also what the
    /// database guards bind, so the values they enforce and the values
    /// compared here cannot drift apart.
    pub recorded: RecordedExecution,
    pub pod: ObservedPod,
}

impl ExecutionIdentity {
    /// The single answer to "does `current` describe the execution I observed?"
    ///
    /// `self` is the observation; `current` is the world now. Every field is
    /// destructured rather than accessed, so adding one to
    /// [`ExecutionIdentity`] fails to compile here until it is given a rule —
    /// the structural reason a future field cannot be silently left out of the
    /// comparison the way `generation` originally was.
    ///
    /// Absence is a value everywhere except where it is *unobservable*: a
    /// caller that never saw the pod cannot claim anything about a live one,
    /// and a resourceVersion is only available to a caller that read the
    /// object, so its absence constrains nothing.
    pub fn reconcile(&self, current: &Self) -> Result<(), PodFenceMismatch> {
        let Self {
            pod_name,
            generation,
            activation_attempt_id,
            recorded,
            pod,
        } = self;

        if pod_name != &current.pod_name {
            return Err(PodFenceMismatch::ExecutionSuperseded);
        }
        if generation.is_some() && generation != &current.generation {
            return Err(PodFenceMismatch::ExecutionSuperseded);
        }
        if activation_attempt_id != &current.activation_attempt_id {
            return Err(PodFenceMismatch::ActivationAttemptChanged);
        }
        // A row that claimed no pod and now claims one has been taken by a
        // create since the caller looked; one that claimed a pod and now
        // claims none has been re-claimed for a replacement. Both are the row
        // becoming someone else's, and when the pod is absent this is the only
        // field left that can say so.
        if let RecordedExecution::Read {
            pod_uid,
            route_activation_id,
        } = recorded
        {
            if pod_uid.as_ref() != current.recorded.pod_uid() {
                return Err(PodFenceMismatch::PodUidChanged);
            }
            if *route_activation_id != current.recorded.route_activation_id() {
                return Err(PodFenceMismatch::ActivationAttemptChanged);
            }
        }
        match (pod, &current.pod) {
            // Nothing left at that name to protect: the delete would 404 and
            // the rest of the retire is still wanted. Refusing here would
            // strand every row whose pod is genuinely gone. Safe to permit
            // only because the recorded-UID check above already rejected a row
            // that was re-claimed while its replacement's pod had yet to
            // appear — without that, an absent pod would make every remaining
            // field match a row in the middle of becoming someone else's.
            (_, ObservedPod::Absent) => Ok(()),
            // A caller that never saw the pod is normally refused against a
            // live one: a Pending pod going Running keeps its UID, so a UID
            // alone cannot tell "the object I meant" from "the recovery I must
            // not destroy". A *terminal* pod cannot recover, so its UID is a
            // complete identification — and it has to be, because the filtered
            // reflector drops terminal pods and no caller can ever observe
            // one. Without this arm every terminal pod defers forever.
            (
                ObservedPod::Unobserved,
                ObservedPod::Seen {
                    pod_uid: current_uid,
                    terminal: true,
                    ..
                },
            ) if recorded.pod_uid() == Some(current_uid) => Ok(()),
            // No view of the pod, but there is one now.
            (ObservedPod::Unobserved | ObservedPod::Absent, _) => {
                Err(PodFenceMismatch::PodIdentityUnverified)
            }
            (
                ObservedPod::Seen {
                    pod_uid,
                    resource_version,
                    terminal: _,
                },
                ObservedPod::Seen {
                    pod_uid: current_uid,
                    resource_version: current_version,
                    terminal: _,
                },
            ) => {
                if pod_uid != current_uid {
                    return Err(PodFenceMismatch::PodUidChanged);
                }
                if resource_version.is_some() && resource_version != current_version {
                    return Err(PodFenceMismatch::PodResourceVersionChanged);
                }
                Ok(())
            }
            (ObservedPod::Seen { .. }, ObservedPod::Unobserved) => {
                Err(PodFenceMismatch::PodIdentityUnverified)
            }
        }
    }
}

impl PodIdentityFence {
    /// A fence on the pod name alone — supersession only. Prefer
    /// [`Self::with_pod_uid`] whenever a UID is recorded.
    pub fn new(pod_name: PodName) -> Self {
        Self {
            pod_name,
            pod_uid: None,
            resource_version: None,
            activation_attempt_id: None,
            generation: None,
            recorded: RecordedExecution::Unread,
        }
    }

    /// Record what the caller's execution row said about itself, as read at
    /// select time.
    #[must_use]
    pub fn with_recorded(mut self, recorded: RecordedExecution) -> Self {
        self.recorded = recorded;
        self
    }

    #[must_use]
    pub fn with_generation(mut self, generation: i64) -> Self {
        self.generation = Some(generation);
        self
    }

    /// The fence as an [`ExecutionIdentity`] — the one shape every guard on
    /// this path compares and binds from.
    pub fn observed(&self) -> ExecutionIdentity {
        ExecutionIdentity {
            pod_name: self.pod_name.clone(),
            generation: self.generation,
            activation_attempt_id: self.activation_attempt_id,
            recorded: self.recorded.clone(),
            pod: match self.pod_uid.clone() {
                Some(pod_uid) => ObservedPod::Seen {
                    pod_uid,
                    resource_version: self.resource_version.clone(),
                    // A caller's observation comes from the filtered reflector,
                    // which drops terminal pods, so it is never one.
                    terminal: false,
                },
                None => ObservedPod::Unobserved,
            },
        }
    }

    #[must_use]
    pub fn with_activation_attempt(
        mut self,
        activation_attempt_id: SandboxActivationAttemptId,
    ) -> Self {
        self.activation_attempt_id = Some(activation_attempt_id);
        self
    }

    #[must_use]
    pub fn with_pod_uid(mut self, pod_uid: PodUid) -> Self {
        self.pod_uid = Some(pod_uid);
        self
    }

    #[must_use]
    pub fn with_resource_version(mut self, resource_version: PodResourceVersion) -> Self {
        self.resource_version = Some(resource_version);
        self
    }
}

/// What the caller observed about the sandbox's execution, and therefore what
/// the substrate has to re-prove before tearing anything down.
///
/// "I saw no execution" is a real observation and gets its own variant rather
/// than collapsing into "no fence". Collapsing them is a fail-open: a row that
/// had no execution when it was selected is exactly the row a concurrent
/// create or wake is most likely to give one, and the unfenced teardown is the
/// most destructive path there is — it deletes every tracked pod name and
/// releases tracking unconditionally. Having seen nothing must not authorize
/// that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetireFence {
    /// Nothing observed; retire whatever is tracked. Correct only for a caller
    /// that has already made the pod provably irrelevant — it deleted the pod
    /// itself under its own precondition first — not for one that simply has
    /// no identity to hand.
    Unobserved,
    /// The caller observed the sandbox with *no* active execution. The retire
    /// must refuse if one exists by the time it runs.
    NoExecution,
    /// The caller observed this pod.
    Pod(PodIdentityFence),
}

/// A pod's identity as the substrate currently sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedPodIdentity {
    pub pod_uid: PodUid,
    pub resource_version: Option<PodResourceVersion>,
}

/// Arguments for [`SubstrateAdapter::retire_execution`]. Grouped rather than
/// positional for the same reason [`CreateExecutionRequest`] is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireExecutionRequest {
    pub sandbox_id: SandboxId,
    pub fence: RetireFence,
}

impl RetireExecutionRequest {
    pub fn fenced(sandbox_id: SandboxId, pod_fence: PodIdentityFence) -> Self {
        Self {
            sandbox_id,
            fence: RetireFence::Pod(pod_fence),
        }
    }
}

/// Which half of a [`PodIdentityFence`] the substrate found violated. Carried
/// so callers can log and meter *why* they deferred without re-deriving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodFenceMismatch {
    /// The fenced pod's execution row is no longer the sandbox's active
    /// execution: a newer generation took over. Retiring now would delete the
    /// successor's pod, which the caller never observed.
    ExecutionSuperseded,
    /// No execution row records the fenced pod name at all — or, for a caller
    /// that observed no execution, one exists now.
    ExecutionMissing,
    /// A live pod exists at the fenced name with a different UID: the pod was
    /// replaced.
    PodUidChanged,
    /// A different activation attempt owns the execution now. Same generation,
    /// same pod name — a retry or a redrive replaced the attempt the caller
    /// selected.
    ActivationAttemptChanged,
    /// The live pod is the same object but has moved on (bound, status
    /// changed) since the caller observed it.
    PodResourceVersionChanged,
    /// The apiserver rejected the preconditioned delete (409). The pod changed
    /// between the check above and the delete itself.
    DeletePreconditionRejected,
    /// The fence carries no pod UID and a live pod exists at that name, so
    /// there is nothing to prove the object is the one the caller observed.
    /// A name-only fence may only act on an absent pod.
    PodIdentityUnverified,
}

impl PodFenceMismatch {
    /// Whether something the in-VM stop did *not* reach may now own the
    /// sandbox.
    ///
    /// This decides whether [`TeardownProgress::RuntimeStopped`] lets a caller
    /// leave its own state transition standing. The stop is addressed to the
    /// execution the fence observed, so it proves the sandbox is down only
    /// where the mismatch describes *that same object* having moved. Anything
    /// else — a new execution, a new attempt, a different pod at the same
    /// name, or an identity that could not be proven at all — may be running
    /// untouched, and leaving a dormant or terminal lifecycle over it is the
    /// stranded state nothing revisits.
    ///
    /// Exactly one variant is safe to answer `false` to, so the default is
    /// `true` and each exception has to earn it. Answering `true` when the
    /// sandbox happens to be down costs one re-evaluation on the next tick;
    /// answering `false` when it is up strands a live sandbox.
    #[must_use]
    pub fn successor_took_over(self) -> bool {
        match self {
            // The same pod object, at a later resourceVersion: a Pending pod
            // coming up is the canonical case. The stop reached it.
            Self::PodResourceVersionChanged => false,
            // A different pod at that name — the stop reached the old one and
            // this one is untouched.
            Self::PodUidChanged
            // A different execution or attempt owns the sandbox now.
            | Self::ExecutionSuperseded
            | Self::ActivationAttemptChanged
            | Self::ExecutionMissing
            // The apiserver rejected the delete's precondition, which it
            // returns as one 409 whether the pod moved or was replaced. Not
            // worth another read to disambiguate: the conservative answer only
            // costs a re-evaluation.
            | Self::DeletePreconditionRejected
            // Nothing could be proven about what is there.
            | Self::PodIdentityUnverified => true,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionSuperseded => "execution_superseded",
            Self::ExecutionMissing => "execution_missing",
            Self::PodUidChanged => "pod_uid_changed",
            Self::ActivationAttemptChanged => "activation_attempt_changed",
            Self::PodResourceVersionChanged => "pod_resource_version_changed",
            Self::DeletePreconditionRejected => "delete_precondition_rejected",
            Self::PodIdentityUnverified => "pod_identity_unverified",
        }
    }
}

impl std::fmt::Display for PodFenceMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of [`SubstrateAdapter::retire_execution`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetireExecutionOutcome {
    /// The execution was retired: node-local state preserved, pod(s) deleted
    /// or already absent, pod tracking released.
    Retired,
    /// A supplied fence no longer matched. This is *defer*, not failure: the
    /// row changed under the caller, and whatever changed it owns the sandbox
    /// now. `progress` says whether anything had already been done by the time
    /// that surfaced, which is what a caller needs to know before undoing a
    /// transition it made to enable this call.
    PodIdentityChanged {
        mismatch: PodFenceMismatch,
        progress: TeardownProgress,
    },
}

impl RetireExecutionOutcome {
    /// Whether a caller may undo a state transition it made to enable this
    /// retire.
    ///
    /// One place, because three sweeper passes ask it and a subset that drifts
    /// apart is how this fence's bugs have always looked. Two facts decide it,
    /// and both have to hold: nothing destructive happened yet, *or* whatever
    /// owns the sandbox now is something this teardown never reached.
    ///
    /// A retire that succeeded is not an undo case at all — the caller's
    /// transition is exactly what it wanted.
    #[must_use]
    pub fn permits_undo(&self) -> bool {
        match self {
            Self::Retired => false,
            Self::PodIdentityChanged { mismatch, progress } => {
                !progress.mutated() || mismatch.successor_took_over()
            }
        }
    }
}

/// How far a refused teardown got before the mismatch surfaced.
///
/// Not every refusal is free. The fence is re-proven at several points and
/// only the earliest run before anything is mutated — the pod delete's
/// precondition, in particular, is evaluated by the apiserver a round trip
/// after the in-VM stop. A caller that compensates its own state transition on
/// a refusal has to distinguish the two, or it will restore a lifecycle
/// claiming a running VM for a sandbox that is already stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeardownProgress {
    /// No in-VM stop, no pod delete, no tracking release.
    NothingMutated,
    /// The observed pod was deleted under its precondition.
    PodDeleted,
    /// The in-VM stop already ran and its disk backup was enqueued.
    RuntimeStopped,
}

impl TeardownProgress {
    #[must_use]
    pub fn mutated(self) -> bool {
        !matches!(self, Self::NothingMutated)
    }
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

pub struct PooledExecutionClaimRequest<'a> {
    pub metric_identity: SandboxApiMetricIdentity,
    pub actor: &'a ActorRef,
    pub template: &'a str,
    pub content_hash: Option<&'a str>,
    /// Pre-resolved template release id. When supplied, `claim_pooled_execution`
    /// skips the `template_releases` lookup the caller already performed.
    pub template_release_id: Option<Uuid>,
    pub runtime: &'a str,
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub env: &'a BTreeMap<String, String>,
    pub auto_lifecycle: &'a AutoLifecyclePolicy,
    pub network_policy_override: Option<&'a NetworkPolicyV1>,
    pub network_policy_generation: u64,
    pub template_network_policy: &'a NetworkPolicyV1,
    pub template_network_policy_generation: u64,
    /// Routes the claim to the matching encryptable pool partition. Encrypted
    /// creates bypass the pool until claim-time re-homing can stamp and apply
    /// the flag at adoption.
    pub volume_encryption: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PooledExecutionPoolShape {
    pub vcpu: u32,
    pub memory_mib: u32,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
}

impl PooledExecutionPoolShape {
    pub const fn new(
        vcpu: u32,
        memory_mib: u32,
        disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    ) -> Self {
        Self {
            vcpu,
            memory_mib,
            disk_mib: None,
            disk_io_limits,
        }
    }

    pub const fn with_disk_mib(
        vcpu: u32,
        memory_mib: u32,
        disk_mib: Option<u32>,
        disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    ) -> Self {
        Self {
            vcpu,
            memory_mib,
            disk_mib,
            disk_io_limits,
        }
    }
}

pub struct PooledExecutionClaim {
    pub claim_id: Uuid,
    pub pool_slot_id: Option<Uuid>,
    pub sandbox_id: SandboxId,
    pub content_hash: String,
    pub pod_name: String,
    pub pod_uid: Option<PodUid>,
    pub execution_generation: u64,
    pub pod_ip: String,
    pub vcpu: Option<u32>,
    pub memory_mib: Option<u32>,
    pub disk_mib: Option<u32>,
    pub disk_io_limits: pplx_space_common_core::types::DiskIoLimits,
    pub active_node_id: Option<NodeId>,
    /// Authoritative route identity returned while claiming the execution.
    /// Substrates without route identity leave it unset.
    pub route_identity: Option<SandboxRouteIdentity>,
}

/// A warm-pool slot claimed for a create when the create was admitted, before
/// it was queued. The create's public sandbox ID is that slot's reserved ID,
/// so the executor must adopt exactly this slot (or build cold under the same
/// ID) and never claim a different one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmittedPoolClaim {
    pub claim_id: Uuid,
    pub pool_slot_id: Uuid,
}

/// A ready warm-pool slot claimed while a create is being admitted, in the
/// transaction that records the create. `sandbox_id` is the slot's reserved
/// ID, which becomes the create's public ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionPoolClaim {
    pub claim: AdmittedPoolClaim,
    pub sandbox_id: SandboxId,
}

/// What the substrate found for an [`AdmittedPoolClaim`].
pub enum AdmittedPoolClaimLookup {
    /// The admitted slot still carries this claim and passed the same pod
    /// and placement checks an ordinary claim makes. The caller must persist
    /// it with `persist_pooled_execution_claim` or compensate it with
    /// `rollback_pooled_execution_claim`.
    Adoptable {
        claim: Box<PooledExecutionClaim>,
        pool_shape: PooledExecutionPoolShape,
    },
    /// No unbound slot owns a pod under this sandbox ID any more (the slot
    /// was collected, or was bound and retired). A cold create under the same
    /// ID is safe once it skips the generation the slot's pod used.
    Released,
    /// A slot still owns a pod under this sandbox ID that this create will
    /// not adopt. It has been taken out of the claimable inventory, and the
    /// pool's own cleanup collects it; until then a cold create under this ID
    /// would leave that pod with nothing to collect it, so the caller waits.
    ReleasePending,
}

pub struct PooledExecutionClaimAttempt {
    pub claim: Option<PooledExecutionClaim>,
    /// Exact release-and-shape pool selected by this attempt. A miss can have
    /// no shape when no pool exists for the resolved release; callers must not
    /// manufacture one from optional create-request fields in that case.
    pub pool_shape: Option<PooledExecutionPoolShape>,
    /// Why this attempt missed, when the reason is one a retry cannot change.
    ///
    /// `None` for an ordinary miss — no slot, a stale slot, a pod that could
    /// not be read. Those are all "try again later", which is what the pool's
    /// own miss handling already assumes.
    ///
    /// The substrate does not decide what a refusal means for the caller,
    /// because that depends on the boot mode it does not see: a fall-through
    /// create builds cold and resolves the same boundary again, while a strict
    /// `boot_mode=pool` create has nowhere to fall through to and would
    /// otherwise report a permanent refusal as "no slot available yet".
    pub refusal: Option<PooledClaimRefusal>,
}

/// A pooled-claim miss that no retry can turn into a hit.
///
/// The variant carries the *class* of the answer, so the controller renders it
/// rather than deciding it — only this module knows which condition occurred.
/// The rule is the status the condition warrants, which for every condition the
/// cold path *also* fails on means matching what the cold path answers. One
/// producer has no cold-path counterpart at all; see below.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PooledClaimRefusal {
    /// The sandbox's node-group boundary admits no pooled pod. A conflict: the
    /// request is incompatible with this sandbox's placement.
    ///
    /// Two producers, and they reach the same status differently:
    ///
    /// * An **empty** candidate set (`NoCompatibleNodeGroup` — an org-specific
    ///   template whose org has no compatible dedicated group). The cold path
    ///   maps this to `Conflict`, so the two agree and the caller sees the same
    ///   409 either way.
    /// * A **dedicated-only** set. Here the cold path does not fail at all: it
    ///   renders the dedicated affinity and the create succeeds. There is no
    ///   cold-path status to match, and 409 is the answer specific to a strict
    ///   `boot_mode=pool` request — a pooled pod is built for no org and lands
    ///   on shared capacity, so no slot can ever satisfy this sandbox and no
    ///   retry changes that.
    PlacementIncompatible { message: String },
    /// The boundary could not be computed at all, for a reason an operator has
    /// to repair — a reaped `templates` row, or a predicate disagreement. The
    /// cold path maps both to `Failed`, so this renders as a 500: it is a
    /// control-plane fault, not something the caller expressed, and reporting
    /// it as a conflict would take the only signal of a missing row out of 5xx
    /// alerting. Terminal, so *not* a 503 either — a strict-pool client must
    /// not be told to retry what only an operator can fix.
    PlacementUnresolvable { message: String },
}

impl std::fmt::Debug for PooledExecutionClaimAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledExecutionClaimAttempt")
            .field("claimed", &self.claim.is_some())
            .field("pool_shape", &self.pool_shape)
            .field("refusal", &self.refusal)
            .finish()
    }
}

impl PooledExecutionClaimAttempt {
    pub const fn claimed(
        claim: PooledExecutionClaim,
        pool_shape: PooledExecutionPoolShape,
    ) -> Self {
        Self {
            claim: Some(claim),
            pool_shape: Some(pool_shape),
            refusal: None,
        }
    }

    pub const fn missed(pool_shape: Option<PooledExecutionPoolShape>) -> Self {
        Self {
            claim: None,
            pool_shape,
            refusal: None,
        }
    }

    /// A miss the caller may need to surface differently. See
    /// [`PooledClaimRefusal`].
    pub const fn refused(
        pool_shape: Option<PooledExecutionPoolShape>,
        refusal: PooledClaimRefusal,
    ) -> Self {
        Self {
            claim: None,
            pool_shape,
            refusal: Some(refusal),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PooledPrepareFailureCleanup {
    FinalizeSandboxDbDelete,
    DeferredToReconciler,
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
/// [`SubstrateError::Timeout`] so consumers (timeout messages, the
/// `node_drain.failures{phase}` metric) attribute the timeout to the step
/// that actually stalled instead of a blanket "node-mgr unavailable" label —
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
    /// (`begin_restore_lifecycle(require_evacuation_marker = true)`) lost its
    /// CAS because the `evacuation_restore_target` marker was cleared by a
    /// concurrent user restore/takeover. Distinct from [`Self::Conflict`] so
    /// the node-drain cohort restore can treat it as a clean deferral (the
    /// user owns the sandbox now) rather than a restore failure — without
    /// swallowing genuine `Conflict`s (e.g. a real wake-RPC failure on the
    /// rebuilt pod) that must stay visible to `node_drain.failures`.
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

/// Capacity classes understood by the substrate admission boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivationAdmissionClass {
    /// A fresh execution without snapshot restoration.
    Cold,
    /// An execution restored from a durable sandbox snapshot.
    Restore,
}

/// Adapter-neutral rejection returned by activation admission.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ActivationAdmissionRejection {
    message: String,
    retry_after: Option<Duration>,
    rate_limited: bool,
}

impl ActivationAdmissionRejection {
    pub(crate) fn new(message: String, retry_after: Option<Duration>, rate_limited: bool) -> Self {
        Self {
            message,
            retry_after,
            rate_limited,
        }
    }
}

/// Boxed admission decision future used by [`ActivationAdmission`].
pub type ActivationAdmissionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), ActivationAdmissionRejection>> + Send + 'a>>;

/// Capacity admission capability supplied to substrate adapters.
pub trait ActivationAdmission: Send + Sync {
    /// Admit or reject one capacity-spending activation.
    fn admit(&self, class: ActivationAdmissionClass) -> ActivationAdmissionFuture<'_>;
}

/// Cluster-activation charge policy for a substrate data-plane call that may
/// recreate a missing pod. Explicit at the substrate boundary so a
/// recovery-capable path can't silently bypass the limiter by omitting a handle,
/// and so "limiter unconfigured" is distinguished from "intentionally exempt".
///
/// Recovery-capable paths (data-plane access, control-wake, rollback, resume)
/// pass [`RecoveryCharge::Enforce`]. Teardown and already-paid paths pass
/// [`RecoveryCharge::Exempt`]. Read paths that must
/// never recreate a pod should call `reachable_runtime_client` instead of
/// passing a charge policy at all.
// Owned (`Arc`) rather than borrowed: the wake path hands the charge to a
// detached coalesced run that outlives the borrowing request, so the policy
// must not carry a caller lifetime.
#[derive(Clone)]
pub enum RecoveryCharge {
    /// Charge activation admission when this path wins ownership of capacity-
    /// spending work. Carries `None` only when the process has no limiter
    /// configured (then it's a no-op).
    Enforce(Option<Arc<dyn ActivationAdmission>>),
    /// Never charge here: a teardown/cleanup path, or one whose activation was
    /// already charged upstream.
    Exempt,
}

impl RecoveryCharge {
    pub(crate) fn enforce<T>(admission: Option<&Arc<T>>) -> Self
    where
        T: ActivationAdmission + 'static,
    {
        Self::Enforce(
            admission.map(|admission| Arc::clone(admission) as Arc<dyn ActivationAdmission>),
        )
    }

    async fn charge(self, class: ActivationAdmissionClass) -> Result<(), SubstrateError> {
        let RecoveryCharge::Enforce(Some(admission)) = self else {
            return Ok(());
        };
        admission
            .admit(class)
            .await
            .map_err(|error| SubstrateError::AdmissionRejected {
                retry_after: error.retry_after,
                rate_limited: error.rate_limited,
                message: error.to_string(),
            })
    }

    /// Charge a Restore activation for a freshly-won restore claim. No-op
    /// for [`RecoveryCharge::Exempt`] or an unconfigured limiter. Exactly
    /// the claim-winner pays: joining followers never call this, and a
    /// lease takeover pays even when its request entered while another
    /// restore was in flight.
    pub(crate) async fn charge_restore_claim(self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Restore).await
    }

    /// Charge a Cold activation if this policy enforces one. No-op for
    /// [`RecoveryCharge::Exempt`] or an unconfigured limiter. Only the
    /// Kubernetes substrate spends deferred cold admission, so this is unused
    /// without that feature.
    #[cfg(feature = "kubernetes")]
    pub(crate) async fn charge_cold_activation(self) -> Result<(), SubstrateError> {
        self.charge(ActivationAdmissionClass::Cold).await
    }
}

#[allow(async_fn_in_trait)]
pub trait SubstrateAdapter: Send + Sync {
    /// Stable label written to control-plane metadata for this substrate.
    ///
    /// This is descriptive metadata. Runtime routing should use the active
    /// execution binding, not this label.
    fn managed_by(&self) -> &'static str;

    /// Whether create requests may carry a Kubernetes node-pool placement pin.
    fn supports_node_pool_placement(&self) -> bool {
        false
    }

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
    /// Issues an in-VM stop, explicitly enqueues a stopped-disk backup, and
    /// releases substrate-owned pod/binding state, but never sends the sbx-mgr
    /// delete that removes the sandbox's filesystem and local-artifact rows.
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
    ) -> Result<PublishedDurableSnapshot, SubstrateError> {
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
            .map_err(|error| SubstrateError::Failed {
                message: format!("node-api durable snapshot creation error: {error}"),
            })?
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

    /// Return a dialable sandbox IP for arbitrary service-port traffic.
    ///
    /// Service proxying owns the destination port, so substrates only expose
    /// the execution IP here. Substrates without a routable sandbox IP can
    /// return `None`.
    async fn sandbox_ip(&self, sandbox_id: SandboxId) -> Result<Option<String>, ControllerError>;

    /// Return a dialable sandbox IP for guest service traffic.
    ///
    /// Like `access_runtime_client`, this may recreate missing execution
    /// objects, but must honor implicit-wake policy when the sandbox is
    /// dormant. `admission` charges the cluster limiter when a live sandbox's
    /// pod has to be recreated (a real activation the stored-lifecycle
    /// entrypoint charge misses).
    async fn access_sandbox_ip(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        charge: RecoveryCharge,
    ) -> Result<Option<String>, ControllerError>;

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

fn retrying_worker_node_api(
    endpoint_addr: &str,
    request_timeout: Duration,
) -> Result<NodeApiClient, SubstrateError> {
    let endpoint = node_api_endpoint(endpoint_addr, Duration::from_secs(3), request_timeout)?;
    Ok(NodeApiClient::with_service_name(
        "worker-node-api",
        endpoint,
    ))
}

#[derive(Clone)]
pub enum Substrate {
    Direct(Box<direct::DirectSubstrate>),
    #[cfg(feature = "kubernetes")]
    Kubernetes(Box<kubernetes::KubernetesSubstrate>),
}

impl Substrate {
    pub fn live_node_names(&self) -> Option<std::collections::HashSet<String>> {
        match self {
            Self::Direct(_) => None,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.live_node_names(),
        }
    }

    pub async fn try_resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.resolve_node_api_endpoint(target).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.try_resolve_node_api_endpoint(target),
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
                Ok(Some(substrate)) => return Ok(Self::Kubernetes(Box::new(substrate))),
                Ok(None) => {}
                Err(err) => return Err(err),
            }
        }
        #[cfg(not(feature = "kubernetes"))]
        drop((shutdown_token, task_tracker));

        Ok(Self::Direct(Box::new(direct::DirectSubstrate::new(
            pool.clone(),
        ))))
    }

    pub(crate) async fn reserve_rollback_route_identity(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>, SubstrateError>
    {
        match self {
            Self::Direct(substrate) => substrate
                .reserve_rollback_route_identity(sandbox_id)
                .await
                .map(Some),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(_) => Ok(None),
        }
    }

    pub(crate) async fn persist_rollback_route_identity(
        &self,
        sandbox_id: SandboxId,
        route_identity: Option<pplx_space_sbx_mgr_client::proto::SpaceApiRouteIdentity>,
    ) -> Result<(), SubstrateError> {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .persist_rollback_route_identity(sandbox_id, route_identity)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(_) => Ok(()),
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
        match self {
            Self::Direct(_) => ReflectorReady::always_ready(),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.readiness(),
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
        match self {
            Self::Direct(substrate) => Ok(Some(substrate.eligible_start_node_count().await?)),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => Ok(substrate.eligible_start_node_count()),
        }
    }

    /// Set the underlying scheduling primitive on the given node. The
    /// concrete substrate decides how (or whether) to honor this; substrates
    /// that have no scheduling primitive return `SubstrateError::Unsupported`.
    ///
    /// Keeping this on `Substrate` means admin handlers stay free of
    /// substrate-specific clients and the substrate boundary owns the
    /// scheduling primitive.
    pub async fn set_node_schedulability(
        &self,
        _request: NodeSchedulabilityRequest<'_>,
        _desired: NodeSchedulability,
    ) -> Result<(), SubstrateError> {
        match self {
            Self::Direct(_) => Err(SubstrateError::Unsupported {
                message: "the active substrate does not support setting node schedulability"
                    .to_string(),
            }),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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

pub async fn connect_node_api_client_for_target<A>(
    substrate: &A,
    target: &NodeApiTarget<'_>,
    request_timeout: Duration,
) -> Result<(String, RuntimeClient), SubstrateError>
where
    A: SubstrateAdapter + ?Sized,
{
    let endpoint_addr = substrate.resolve_node_api_endpoint(target).await?;
    let endpoint = node_api_endpoint(&endpoint_addr, Duration::from_secs(3), request_timeout)?;
    let client = connect_worker(endpoint)
        .await
        .map_err(|err| SubstrateError::Unavailable {
            message: format!("node-api not reachable at {endpoint_addr}: {err}"),
        })?;
    Ok((endpoint_addr, client))
}

/// Per-sandbox liveness/salvage summary observed by one node-api
/// `WorkerApi/SandboxState` probe (which proxies the on-node sbx-mgr `State`
/// RPC). Verify-before-destroy reconcilers use this to decide whether a stale
/// control-plane row still has a live VM or salvageable node-local state
/// behind it before taking any destructive action. Live observations carry
/// the node-local lifecycle status when it parses, so repair loops can tell a
/// stale control-plane `failed` row from a node-local `failed` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedSandboxState {
    /// The Firecracker process is alive on the node — the VM is running.
    VmAlive {
        node_status: Option<SandboxLifecycle>,
    },
    /// Known to the on-node sbx-mgr, VM not running, and a current-instance
    /// local artifact pointer is recorded: the node holds salvageable state
    /// that may not be backed up to durable storage yet.
    DormantWithArtifact {
        node_status: Option<SandboxLifecycle>,
    },
    /// Known to the on-node sbx-mgr, VM not running, and no current-instance
    /// local artifact pointer.
    DormantNoArtifact {
        node_status: Option<SandboxLifecycle>,
    },
    /// The on-node sbx-mgr has no record of the sandbox (`missing_ids`).
    Missing,
}

impl ObservedSandboxState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VmAlive { .. } => "vm_alive",
            Self::DormantWithArtifact { .. } => "dormant_with_artifact",
            Self::DormantNoArtifact { .. } => "dormant_no_artifact",
            Self::Missing => "missing",
        }
    }

    pub const fn node_status(self) -> Option<SandboxLifecycle> {
        match self {
            Self::VmAlive { node_status }
            | Self::DormantWithArtifact { node_status }
            | Self::DormantNoArtifact { node_status } => node_status,
            Self::Missing => None,
        }
    }
}

impl std::fmt::Display for ObservedSandboxState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Batch-probe the liveness of `sandbox_ids` on one node via the node-api
/// `WorkerApi/SandboxState` RPC. Read-only: nothing on the node changes.
///
/// Ids the node answered for (present or explicitly missing) appear in the
/// returned map; an id absent from the map means the node's answer did not
/// cover it (a malformed id echo, or a skew where the node returned neither
/// presence nor absence) and callers must treat it as inconclusive rather
/// than missing. The latency contract is the channel timeout the caller
/// configured on `client` — this function adds no waiting of its own.
pub async fn sandbox_states_with_client(
    client: &mut RuntimeClient,
    endpoint_addr: &str,
    sandbox_ids: &[SandboxId],
) -> Result<BTreeMap<SandboxId, ObservedSandboxState>, SubstrateError> {
    let response = client
        .sandbox_state(pplx_space_sbx_mgr_client::proto::StateRequest {
            sandbox_ids: sandbox_ids.iter().map(SandboxId::to_string).collect(),
        })
        .await
        .map_err(|err| SubstrateError::Unavailable {
            message: format!("node-api sandbox-state probe at {endpoint_addr} failed: {err}"),
        })?
        .into_inner();

    Ok(observed_sandbox_states(response))
}

fn observed_sandbox_states(
    response: pplx_space_sbx_mgr_client::proto::StateResponse,
) -> BTreeMap<SandboxId, ObservedSandboxState> {
    let mut states = BTreeMap::new();
    for info in response.sandboxes {
        let Ok(sandbox_id) = info.sandbox_id.parse::<SandboxId>() else {
            // Not one of ours (sbx-mgr ids are UUIDs); leave it out of the
            // map so the caller classifies the requested id as inconclusive.
            continue;
        };
        let node_status = match info.observed_lifecycle() {
            Ok(status) => status.label().parse::<SandboxLifecycle>().ok(),
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    lifecycle = ?info.lifecycle,
                    error = %error,
                    "node-api sandbox-state probe returned unparseable sandbox status"
                );
                // Unknown vocabulary is not evidence of a dead sandbox. Omit
                // it so the requested-id probe remains inconclusive.
                continue;
            }
        };
        let state = if info.firecracker_alive {
            ObservedSandboxState::VmAlive { node_status }
        } else if info.current_local_artifact_id.is_some() {
            ObservedSandboxState::DormantWithArtifact { node_status }
        } else {
            ObservedSandboxState::DormantNoArtifact { node_status }
        };
        states.insert(sandbox_id, state);
    }
    for missing in response.missing_ids {
        if let Ok(sandbox_id) = missing.parse::<SandboxId>() {
            states.insert(sandbox_id, ObservedSandboxState::Missing);
        }
    }
    states
}

pub async fn connect_retrying_node_api_client_for_target<A>(
    substrate: &A,
    target: &NodeApiTarget<'_>,
    request_timeout: Duration,
) -> Result<(String, NodeApiClient), SubstrateError>
where
    A: SubstrateAdapter + ?Sized,
{
    let endpoint_addr = substrate.resolve_node_api_endpoint(target).await?;
    let client = retrying_worker_node_api(&endpoint_addr, request_timeout)?;
    Ok((endpoint_addr, client))
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
        match self {
            Self::Direct(substrate) => substrate.managed_by(),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.managed_by(),
        }
    }

    fn supports_node_pool_placement(&self) -> bool {
        match self {
            Self::Direct(substrate) => substrate.supports_node_pool_placement(),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.supports_node_pool_placement(),
        }
    }

    async fn create_execution(
        &self,
        request: CreateExecutionRequest<'_>,
    ) -> Result<CreateExecutionOutcome, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.create_execution(request).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.create_execution(request).await,
        }
    }

    async fn complete_activation_lease(
        &self,
        sandbox_id: SandboxId,
        completion: Option<ActivationLeaseCompletion>,
    ) {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .complete_activation_lease(sandbox_id, completion)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
                substrate
                    .complete_activation_lease(sandbox_id, completion)
                    .await
            }
        }
    }

    async fn delete_execution(&self, sandbox_id: SandboxId) -> Result<(), SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.delete_execution(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.delete_execution(sandbox_id).await,
        }
    }

    async fn retire_execution(
        &self,
        request: RetireExecutionRequest,
    ) -> Result<RetireExecutionOutcome, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.retire_execution(request).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.retire_execution(request).await,
        }
    }

    async fn retire_fence_mismatch(
        &self,
        sandbox_id: SandboxId,
        fence: &RetireFence,
    ) -> Result<Option<PodFenceMismatch>, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.retire_fence_mismatch(sandbox_id, fence).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.retire_fence_mismatch(sandbox_id, fence).await,
        }
    }

    async fn observe_pod_identities(
        &self,
        pod_names: &[PodName],
    ) -> HashMap<PodName, ObservedPodIdentity> {
        match self {
            // No pod objects, so nothing to observe; the default applies.
            Self::Direct(_) => HashMap::new(),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.observe_pod_identities(pod_names).await,
        }
    }

    async fn delete_observed_execution(
        &self,
        sandbox_id: SandboxId,
        target: &NodeApiTarget<'_>,
    ) -> Result<(), SubstrateError> {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .delete_observed_execution(sandbox_id, target)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => {
                substrate
                    .start_execution(metric_identity, sandbox_id, options, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => {
                substrate
                    .restore_execution(metric_identity, sandbox_id, options, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
                substrate
                    .restore_execution(metric_identity, sandbox_id, options, charge)
                    .await
            }
        }
    }

    async fn suspend_execution(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<PublishedLocalArtifact>, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.suspend_execution(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.suspend_execution_tracked(sandbox_id).await,
        }
    }

    async fn enqueue_artifact_backup(
        &self,
        sandbox_id: SandboxId,
        expected_kind: ArtifactKind,
        source: BackupSource,
        target: &NodeApiTarget<'_>,
    ) -> Result<EnqueuedArtifactBackup, SubstrateError> {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .enqueue_artifact_backup(sandbox_id, expected_kind, source, target)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
    ) -> Result<PublishedDurableSnapshot, SubstrateError> {
        match self {
            Self::Direct(substrate) => {
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
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => {
                substrate
                    .wake_execution(metric_identity, sandbox, timeout, wake_activity, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => substrate.runtime_client(sandbox_id, charge).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.runtime_client(sandbox_id, charge).await,
        }
    }

    async fn reachable_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.reachable_runtime_client(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.reachable_runtime_client(sandbox_id).await,
        }
    }

    async fn probe_runtime_client(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<RuntimeClient>, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.probe_runtime_client(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.probe_runtime_client(sandbox_id).await,
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
        match self {
            Self::Direct(substrate) => {
                substrate
                    .access_runtime_client(metric_identity, sandbox, activity, timeout, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
                substrate
                    .access_runtime_client(metric_identity, sandbox, activity, timeout, charge)
                    .await
            }
        }
    }

    async fn sandbox_ip(&self, sandbox_id: SandboxId) -> Result<Option<String>, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.sandbox_ip(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.sandbox_ip(sandbox_id).await,
        }
    }

    async fn access_sandbox_ip(
        &self,
        metric_identity: SandboxApiMetricIdentity,
        sandbox: &Sandbox,
        timeout: Duration,
        charge: RecoveryCharge,
    ) -> Result<Option<String>, ControllerError> {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .access_sandbox_ip(metric_identity, sandbox, timeout, charge)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
                substrate
                    .access_sandbox_ip(metric_identity, sandbox, timeout, charge)
                    .await
            }
        }
    }

    async fn builder_client(&self) -> Result<BuilderClient, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.builder_client().await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.builder_client().await,
        }
    }

    async fn builder_client_for_node(
        &self,
        node_id: NodeId,
    ) -> Result<BuilderClient, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.builder_client_for_node(node_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.builder_client_for_node(node_id).await,
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
        match self {
            Self::Direct(substrate) => substrate.pool_eligible_for_request(
                boot_mode,
                backend,
                wait_until_ready,
                disk_mib,
                file_changes_enabled,
                implicit_wake_enabled,
            ),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.pool_eligible_for_request(
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
        match self {
            Self::Direct(substrate) => substrate.pool_supports_request_env(),
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.pool_supports_request_env(),
        }
    }

    async fn claim_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
    ) -> Result<PooledExecutionClaimAttempt, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.claim_pooled_execution(request).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.claim_pooled_execution(request).await,
        }
    }

    async fn lookup_admitted_pooled_execution(
        &self,
        request: &PooledExecutionClaimRequest<'_>,
        admitted: AdmittedPoolClaim,
        sandbox_id: SandboxId,
    ) -> Result<AdmittedPoolClaimLookup, ControllerError> {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .lookup_admitted_pooled_execution(request, admitted, sandbox_id)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => substrate.skip_admitted_pool_generation(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
                substrate.skip_admitted_pool_generation(sandbox_id).await
            }
        }
    }

    async fn admitted_pool_cleanup_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.admitted_pool_cleanup_pending(sandbox_id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
                substrate.admitted_pool_cleanup_pending(sandbox_id).await
            }
        }
    }

    async fn claim_admission_pool_slot(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        template_release_id: Uuid,
    ) -> Result<Option<AdmissionPoolClaim>, ControllerError> {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .claim_admission_pool_slot(tx, template_release_id)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => {
                substrate
                    .persist_pooled_execution_claim(tx, request, claim)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => substrate.rollback_pooled_execution_claim(claim).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.rollback_pooled_execution_claim(claim).await,
        }
    }

    async fn cleanup_failed_pooled_prepare(
        &self,
        claim: &PooledExecutionClaim,
    ) -> Result<PooledPrepareFailureCleanup, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.cleanup_failed_pooled_prepare(claim).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.cleanup_failed_pooled_prepare(claim).await,
        }
    }

    async fn apply_pooled_execution_auto_lifecycle(
        &self,
        claim: &PooledExecutionClaim,
        policy: &AutoLifecyclePolicy,
    ) -> Result<(), ControllerError> {
        match self {
            Self::Direct(substrate) => {
                substrate
                    .apply_pooled_execution_auto_lifecycle(claim, policy)
                    .await
            }
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => {
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
            Self::Kubernetes(substrate) => {
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
        match self {
            Self::Direct(substrate) => substrate.list_pool_configs().await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.list_pool_configs().await,
        }
    }

    async fn upsert_pool_config(
        &self,
        request: ClusterPoolConfigRequest,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.upsert_pool_config(request).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.upsert_pool_config(request).await,
        }
    }

    async fn disable_pool_config(
        &self,
        id: Uuid,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        match self {
            Self::Direct(substrate) => substrate.disable_pool_config(id).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.disable_pool_config(id).await,
        }
    }

    async fn resolve_node_api_endpoint(
        &self,
        target: &NodeApiTarget<'_>,
    ) -> Result<String, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.resolve_node_api_endpoint(target).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.resolve_node_api_endpoint(target).await,
        }
    }

    async fn select_ready_worker_node_api_target(
        &self,
    ) -> Result<OwnedNodeApiTarget, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.select_ready_worker_node_api_target().await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.select_ready_worker_node_api_target().await,
        }
    }

    async fn list_inventory_nodes(
        &self,
        role_filter: Option<&str>,
    ) -> Result<Option<Vec<NodeInventoryEntry>>, SubstrateError> {
        match self {
            Self::Direct(substrate) => substrate.list_inventory_nodes(role_filter).await,
            #[cfg(feature = "kubernetes")]
            Self::Kubernetes(substrate) => substrate.list_inventory_nodes(role_filter).await,
        }
    }
}

/// Wrap a connected node-api `Channel` with the bearer-token interceptor (so a
/// local token read/mount failure fails the call with `Status::unavailable`, not a
/// header-less request the node rejects as a 401) and the client trace layer.
/// Every node-api client is built through this so auth is attached in one place.
pub fn node_api_channel(
    channel: Channel,
    kind: SpaceBearerToken,
) -> GrpcClientTrace<InterceptedService<Channel, SpaceBearerInterceptor>> {
    wrap_grpc_client_channel(InterceptedService::new(
        channel,
        SpaceBearerInterceptor::new(kind),
    ))
}

/// [`node_api_transport`] for the TCP connect helpers, carrying the TLS
/// handshake bound the transport applies.
async fn node_api_connect_endpoint(
    endpoint: Endpoint,
) -> Result<pplx_space_common_grpc::internal_transport::ConnectEndpoint, ControllerError> {
    pplx_space_common_grpc::internal_transport::client_connect_endpoint(
        pplx_space_common_grpc::internal_transport::Service::NodeApi,
        endpoint,
    )
    .await
    .map_err(ControllerError::node_api_transport)
}

async fn node_api_transport(endpoint: Endpoint) -> Result<Endpoint, ControllerError> {
    pplx_space_common_grpc::internal_transport::client_endpoint(
        pplx_space_common_grpc::internal_transport::Service::NodeApi,
        endpoint,
    )
    .await
    .map_err(ControllerError::node_api_transport)
}

pub(super) async fn require_activation_policy_support(
    transport: RuntimeTransport,
    timeout: Duration,
) -> Result<(), ControllerError> {
    let mut health = tonic_health::pb::health_client::HealthClient::new(transport);
    let mut request = tonic::Request::new(tonic_health::pb::HealthCheckRequest {
        service: pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE.to_owned(),
    });
    request.set_timeout(timeout);
    match tokio::time::timeout(timeout, health.check(request)).await {
        Ok(Ok(response))
            if response.get_ref().status
                == tonic_health::pb::health_check_response::ServingStatus::Serving as i32 =>
        {
            Ok(())
        }
        Ok(Ok(_)) => Err(ControllerError::service_unavailable(
            "worker does not support activation policy",
        )),
        Ok(Err(error)) => Err(ControllerError::service_unavailable(format!(
            "worker activation policy capability check failed: {error}"
        ))),
        Err(_) => Err(ControllerError::service_unavailable(
            "worker activation policy capability check timed out",
        )),
    }
}

pub async fn connect_worker(endpoint: Endpoint) -> Result<RuntimeClient, ControllerError> {
    let channel = node_api_connect_endpoint(endpoint)
        .await?
        .connect_with_retry(
            "worker-node-api",
            "worker-node-api",
            pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
        })?;
    Ok(RuntimeClient::new(channel))
}

pub async fn connect_worker_internal(
    endpoint: Endpoint,
) -> Result<RuntimeInternalClient, ControllerError> {
    let channel = node_api_connect_endpoint(endpoint)
        .await?
        .connect_with_retry(
            "worker-node-api-internal",
            "worker-node-api-internal",
            pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
        })?;
    Ok(WorkerApiInternalClient::new(wrap_grpc_client_channel(
        InterceptedService::new(
            channel,
            SpaceBearerInterceptor::new(SpaceBearerToken::NodeApiInternal),
        ),
    )))
}

pub async fn connect_builder(endpoint: Endpoint) -> Result<BuilderClient, ControllerError> {
    let channel = node_api_connect_endpoint(endpoint)
        .await?
        .connect_with_retry(
            "builder-node-api",
            "builder-node-api",
            pplx_space_common_grpc::DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|err| {
            ControllerError::service_unavailable(format!("node-api not reachable: {err}"))
        })?;
    Ok(BuilderApiClient::new(wrap_grpc_client_channel(
        InterceptedService::new(
            channel,
            SpaceBearerInterceptor::new(SpaceBearerToken::NodeApi),
        ),
    )))
}

#[cfg(test)]
mod tests;
