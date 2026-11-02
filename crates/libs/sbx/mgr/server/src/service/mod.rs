mod environment_selection;
use pplx_space_common_core::types::NodeId;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwapOption;
use pplx_space_common_core::host::linux::identities::user_names;
use pplx_space_common_core::host::linux::paths::{
    TEMPLATE_CACHE_DIR_MODE, normalize_template_cache_tree as normalize_template_cache_tree_io,
    set_template_cache_permissions as set_template_cache_permissions_io,
};
use pplx_space_common_core::types::{
    AutoLifecycleActivitySource, AutoLifecycleIdleAction, AutoLifecyclePolicy,
    AutoLifecycleRuntimeState, AutoLifecycleTransition, ExecutionGeneration, GuestUserName,
    IncompleteSandboxRouteIdentityError, LOCAL_OPERATOR_SANDBOX_OWNER_KINDS, NodeManagedBy,
    NodeRole, ORPHANED_SANDBOX_OWNER, ORPHANED_SANDBOX_TEMPLATE, RuntimeKind,
    SandboxCleanupEpisodeId, SandboxId, SandboxOwner, SandboxOwnerKind,
    SandboxQuarantineReasonCode, SandboxRouteIdentity, SandboxRuntimeLabel, SpaceApiRouteIdentity,
    TEMPLATE_MANAGER_SANDBOX_OWNER, format_reserved_sandbox_owner_kind_labels, parse_grpc_timeout,
};
use pplx_space_common_svc::space_api_route_ed25519::{
    SpaceApiRouteEd25519VerificationBundleUpdate, SpaceApiRouteEd25519VerificationBundleUpdateError,
};
use pplx_space_common_svc::{
    AuthorizedUdsPeer, LocalRole, PeerIdentity, UdsPeerAccount, UdsPeerAuthorizer,
    UdsPeerPrincipal,
    types::{ApplyDecisionKind, FACET_NETWORK_POLICY},
};
use pplx_space_net_common::{NetworkPolicyV1, proto::NetworkPolicyApplyDecision};
use pplx_space_net_gateway_client::proto::{ServicePortIngressFrame, service_port_ingress_frame};
use pplx_space_node_mgr_client::NodeManagerGrpcClient;
use pplx_space_node_mgr_client::proto::{
    AllocateRequest, CaptureVmStateRequest, CleanupSandboxRequest, LaunchRequest,
    LaunchSandboxRequest, NetworkPoolStatusRequest, PauseSandboxRequest,
    PrepareSandboxArtifactDirRequest, PrepareSandboxRequest, ReleaseRequest, RestoreVmStateRequest,
    ResumeSandboxRequest, StopProcessRequest, SyncGuestRequest,
};
use pplx_space_spaced_common::proto::host_control::host_control_server::HostControl;
use pplx_space_spaced_common::proto::host_control::{
    ActiveForAccepted as HostControlActiveForAccepted, ActivityState as HostControlActivityState,
    Denied as HostControlDenied, GetNetworkPolicyRequest as HostControlGetNetworkPolicyRequest,
    GetNetworkPolicyResponse as HostControlGetNetworkPolicyResponse,
    IssueGatewayIntermediateCertificateRequest, IssueGatewayIntermediateCertificateResponse,
    RequestActiveForRequest as HostControlRequestActiveForRequest,
    RequestActiveForResponse as HostControlRequestActiveForResponse,
    RequestShutdownRequest as HostControlShutdownRequest,
    RequestShutdownResponse as HostControlShutdownResponse,
    SetActivityStateRequest as HostControlSetActivityStateRequest,
    SetActivityStateResponse as HostControlSetActivityStateResponse,
    request_active_for_response::Outcome as HostControlActiveForOutcome,
    request_shutdown_response::Outcome as HostControlShutdownOutcome,
};
use pplx_space_spaced_common::{validate_duration, validate_optional_reason};
use pplx_space_spaced_host_client::proto as host_proto;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::{Request, Response, Status};
use tracing::Instrument;
use uuid::Uuid;

use crate::auto_lifecycle::{
    AutoLifecycleCoordinator, apply_observed_access_touch, apply_observed_command_and_access_touch,
    clear_error, disarm_guest_activity_on_external_access, due_stage,
    extend_guest_activity_idle_epoch, initial_runtime_state, mark_error,
    mark_error_and_disarm_conditions, mark_transition, notify_guest_activity_state, now_secs,
    policy_from_proto, policy_to_proto, recompute_runtime_state, status_to_proto, touch_access,
    touch_command_and_access, validate_v1_policy,
};
mod execution_startup;
pub(crate) mod fork_artifact;
mod gateway_authority;
pub(crate) mod network_policy;
mod restart_admission;
mod runtime_upgrade;
mod suspend_snapshot_publication;
mod template_apps;

/// Enables the network-credential authority path (off by default).
const NETWORK_CREDENTIALS_ENABLED_ENV: &str = "SPACE_SBXMGR_NETWORK_CREDENTIALS_ENABLED";

use self::drain::DrainState;
pub use self::drain::{
    EnableDependencyRestartDrainError, ReleaseDependencyRestartDrainError,
    StartupRecoveryAdmissionError, StartupRecoveryLease,
};
use self::network_policy::{
    NetworkPolicyReconcileQueue, TemplateNetworkPolicyCache, effective_network_policy_ref,
    network_policy_from_proto, network_policy_override_from_set_request, network_policy_to_proto,
};
pub use self::recovery::StartupRecoveryError;
pub use self::restart_admission::{
    SandboxManagerRestartAdmissionLayer, handoff_cancellable_response_stream_name,
};
use self::start_support::ActivationAdmissionLimiter;
use crate::node_manager_transport::NodeManagerConnector;
use crate::node_state_sync::{mark_sandbox_dirty, mark_sandboxes_dirty, should_skip_node_publish};
use crate::proto::sandbox_manager_server::SandboxManager;
use crate::proto::{
    AcknowledgeAutoLifecycleDeleteReceiptsRequest, AcknowledgeAutoLifecycleDeleteReceiptsResponse,
    AdoptPoolSandboxRequest, AdoptPoolSandboxResponse, AutoLifecycleDeleteReceipt, BootMode,
    CleanupRequest, CleanupResponse, CloneFromDonorRequest, CloneFromDonorResponse, CreateRequest,
    CreateResponse, CreateTemplateBaseSnapshotRequest, CreateTemplateBaseSnapshotResponse,
    DeleteRequest, DeleteResponse, DeleteSnapshotsRequest, DeleteSnapshotsResponse,
    DisableSnapshotsRequest, DisableSnapshotsResponse, EnableSnapshotsRequest,
    EnableSnapshotsResponse, EnqueueArtifactBackupRequest, EnqueueArtifactBackupResponse,
    ExportSandboxChunk, ExportSandboxRequest, ExportSnapshotChunk, ExportSnapshotRequest,
    GetDiskUsageRequest, GetDiskUsageResponse, GetEnvRequest, GetEnvResponse,
    GetNetworkPolicyRequest, GetNetworkPolicyResponse, GetSandboxHealthRequest,
    GetSandboxHealthResponse, ImportSandboxRequest, ImportSandboxResponse, ImportSnapshotRequest,
    ImportSnapshotResponse, KillRequest, KillResponse, ListAutoLifecycleDeleteReceiptsRequest,
    ListAutoLifecycleDeleteReceiptsResponse, ListBuildReferencingSandboxesRequest,
    ListBuildReferencingSandboxesResponse, ListPendingLocalBackupsRequest,
    ListPendingLocalBackupsResponse, ListRequest, ListResponse, ListSnapshotsRequest,
    ListSnapshotsResponse, ListTemplatesRequest, ListTemplatesResponse,
    MissingDiskUsageId as SbxMissingDiskUsageId,
    MissingDiskUsageReason as SbxMissingDiskUsageReason, NetworkPolicyDetail, PauseRequest,
    PauseResponse, PendingLocalBackup as ProtoPendingLocalBackup, PrepareTeardownSuspendRequest,
    PrepareTeardownSuspendResponse, ResolveSandboxMemoryRequest, ResolveSandboxMemoryResponse,
    ResumeRequest, ResumeResponse, RollbackRequest, RollbackResponse, SandboxInfo,
    SandboxListElement, SetAutoLifecyclePolicyRequest, SetAutoLifecyclePolicyResponse,
    SetNetworkPolicyRequest, SetNetworkPolicyResponse, SnapshotDetail,
    SpacedCloseLoopbackBridgeRequest, SpacedCloseLoopbackBridgeResponse, SpacedCloseStdinRequest,
    SpacedCloseStdinResponse, SpacedConnectProcessRequest, SpacedCreateWatcherRequest,
    SpacedCreateWatcherResponse, SpacedDisableFileChangesRequest, SpacedDisableFileChangesResponse,
    SpacedEnableFileChangesRequest, SpacedEnableFileChangesResponse, SpacedFileChunk,
    SpacedFileEvent, SpacedFileEventKind, SpacedFileInfo, SpacedGetModifiedFilesRequest,
    SpacedGetModifiedFilesResponse, SpacedGetStatusRequest, SpacedGetStatusResponse,
    SpacedGetWatcherEventsRequest, SpacedGetWatcherEventsResponse, SpacedKillProcessRequest,
    SpacedKillProcessResponse, SpacedListProcessesRequest, SpacedListProcessesResponse,
    SpacedListRequest, SpacedMkdirRequest, SpacedMkdirResponse, SpacedModifiedFile,
    SpacedNormalizeWatchRootRequest, SpacedNormalizeWatchRootResponse,
    SpacedOpenLoopbackBridgeRequest, SpacedOpenLoopbackBridgeResponse, SpacedPingGuestRequest,
    SpacedPingGuestResponse, SpacedProcessConnected, SpacedProcessEvent, SpacedProcessExit,
    SpacedProcessExited, SpacedProcessInfo, SpacedProcessOutput, SpacedProcessSignaled,
    SpacedProcessStarted, SpacedProcessTimedOut, SpacedProcessUnknown, SpacedReadRequest,
    SpacedRemoveRequest, SpacedRemoveResponse, SpacedRemoveWatcherRequest,
    SpacedRemoveWatcherResponse, SpacedRenameRequest, SpacedRenameResponse, SpacedResizePtyRequest,
    SpacedResizePtyResponse, SpacedSendInputRequest, SpacedSendInputResponse,
    SpacedSendSignalRequest, SpacedSendSignalResponse, SpacedStartProcessRequest,
    SpacedStatRequest, SpacedStatResponse, SpacedVersionInfo, SpacedWaitReadyRequest,
    SpacedWaitReadyResponse, SpacedWatchRequest, SpacedWriteBatchEntry, SpacedWriteBatchRequest,
    SpacedWriteBatchResponse, SpacedWriteRequest, SpacedWriteResponse, StartRequest, StartResponse,
    StateRequest, StateResponse, StatsRequest, StatsResponse, StopRequest, StopResponse,
    SuspendRequest, SuspendResponse, TeardownSuspendAction, TeardownSuspendOutcome, TemplateInfo,
    TemplateNetworkPolicyUpdatedRequest, TemplateNetworkPolicyUpdatedResponse,
    TouchAutoLifecycleRequest, TouchAutoLifecycleResponse, UpdateRequest, UpdateResponse,
    WaitRequest, WaitResponse, WakeRequest, WakeResponse, WakeTransition, spaced_write_request,
};
use crate::runtime_reconciliation::{mark_quarantined_best_effort, observe_vm_state};
use crate::state::SandboxState;
use pplx_space_node_runtime_core::vm_state::VmState;

type BoxedStatusStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;
const CLEANUP_RETRY_INITIAL_DELAY_SECS: u64 = 1;
const CLEANUP_RETRY_MAX_DELAY_SECS: u64 = 30;
const TEARDOWN_SUSPEND_DRIVER_CONCURRENCY: usize = 4;
const FORK_COPY_CONCURRENCY: usize = 1;
const FORK_COPY_MAX_QUEUED: usize = 4;
const FORK_COPY_MAX_WAIT: Duration = Duration::from_secs(5);

const RESUME_CLOCK_SYNC_TIMEOUT: Duration = Duration::from_secs(5);
// Foreground Stats should promptly fall back to recorded counters when the
// gateway is slow. Background observations tolerate a longer 2s probe.
const NET_GATEWAY_FOREGROUND_STATS_TIMEOUT: Duration = Duration::from_secs(1);
// Stats' node-manager inspect, its kept channel's dial included, gets as long
// as the gateway probe in the same call. A merely slow inspect must still
// answer: without it Stats reports zero CPU time and memory, which kubelet
// reads as a counter reset and an empty sandbox.
const STATS_INSPECT_TIMEOUT: Duration = Duration::from_secs(1);
const NETWORK_POLICY_SYNC_REFRESH_TIMEOUT: Duration = Duration::from_secs(2);
// End-to-end network-readiness budget, including connection, RPCs and backoff.
// Startup dependencies may need tens of seconds even after policy hydration.
// The gateway owns those dependencies; the manager only waits for Ready.
const NETWORK_GATEWAY_READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const NETWORK_POLICY_RECONCILE_BATCH_DEBOUNCE: Duration = Duration::from_millis(200);
const NETWORK_POLICY_RECONCILE_CONCURRENCY: usize = 32;
const NETWORK_POLICY_RECONCILE_RETRY_DELAY: Duration = Duration::from_secs(5);
/// Cadence of the periodic reconcile sweep that re-enqueues live sandboxes.
/// The network-policy reconcile is otherwise event-driven, so this is the
/// backstop that converges config with no event trigger of its own — notably
/// SPACE-API route identity — onto already-running gateways.
const NETWORK_POLICY_RECONCILE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const NETWORK_POLICY_PUSH_RETRY_DELAYS: [Duration; 11] = [
    Duration::from_millis(0),
    Duration::from_millis(10),
    Duration::from_millis(20),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

fn request_sandbox_ids(sandbox_ids: &[String]) -> Vec<String> {
    let mut ids = Vec::with_capacity(sandbox_ids.len());
    let mut seen = BTreeSet::new();
    for sandbox_id in sandbox_ids.iter().map(|id| id.trim()) {
        if !sandbox_id.is_empty() && seen.insert(sandbox_id.to_owned()) {
            ids.push(sandbox_id.to_owned());
        }
    }
    ids
}

const RUN_DIR: &str = pplx_space_common_core::host::linux::paths::SANDBOX_RUN_DIR;
const CACHE_DIR: &str = pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR;

/// Answers whether a sandbox's rootfs is on this node's disk; an error means
/// presence couldn't be verified.
pub type RootfsPresenceProbe = Arc<dyn Fn(&str) -> std::io::Result<bool> + Send + Sync>;
const TEMPLATE_DIR: &str = pplx_space_common_core::host::linux::paths::TEMPLATE_DIR;
const NET_GATEWAY_SIDECAR_PID_NAME: &str = "space-netgtw-svc";

const MAX_WRITE_BATCH_ENTRIES: usize = pplx_space_common_core::spaced::MAX_WRITE_BATCH_ENTRIES;
const DEFAULT_BLOCKING_READY_TIMEOUT_SECS: u32 = 30;
const DEFAULT_RESUME_READY_TIMEOUT_SECS: u32 = 5;
const GRPC_TIMEOUT_METADATA_KEY: &str = "grpc-timeout";
const SPACE_TMPLMGR_ACCOUNT: &str = user_names::SPACE_TMPLMGR;

const ALLOW_SANDBOX_OBSERVER: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];
const ALLOW_SANDBOX_OBSERVER_OR_TMPLMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Observer),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];
// Direct sandbox/snapshot import and export streams require root or a local
// operator; node-api retains separate authority for lifecycle and backup RPCs.
const ALLOW_LOCAL_ARTIFACT_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
];
const ALLOW_SANDBOX_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
];
const ALLOW_SANDBOX_AUTO_LIFECYCLE_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];
const ALLOW_SANDBOX_NETWORK_POLICY_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];
const ALLOW_SANDBOX_SNAPSHOT_OPERATOR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];
const ALLOW_SANDBOX_OPERATOR_OR_TMPLMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
    UdsPeerPrincipal::Service(user_names::SPACE_SBXMGR),
];
const ALLOW_AUTO_LIFECYCLE_DELETE_RECEIPTS: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];
// The pending-backup inventory is the node's own answer to "what on this
// disk has no durable copy". Its only caller is node-ctl, which publishes it
// in the Full census; root covers break-glass inspection. Deliberately not
// the general observer set, which would put a node's data-loss exposure in
// front of every local reader.
const ALLOW_LOCAL_BACKUP_INVENTORY: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];
const ALLOW_SANDBOX_ADMIN_OR_TMPLMGR: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
];
const ALLOW_SANDBOX_POOL_ADOPT: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
    UdsPeerPrincipal::Service(user_names::SPACE_TMPLMGR),
];
const ALLOW_SANDBOX_START_CONTEXT: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Admin),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
];
/// The callers that run lifecycle work for the control plane. Every one is
/// also allowed Start and Stop, which an upgrade runs as its caller.
const ALLOW_SANDBOX_RUNTIME_UPGRADE: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
];
// Cluster-deleted evictions are driven by node-ctl's desired-state facet
// sync, which carries the authoritative "no cluster row" signal. Root and
// the local operator role cover break-glass tooling; deliberately not the
// general sandbox-operator set.
const ALLOW_SANDBOX_CLUSTER_DELETED_EVICT: &[UdsPeerPrincipal] = &[
    UdsPeerPrincipal::Root,
    UdsPeerPrincipal::Role(LocalRole::Operator),
    UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
];

type SbxExportSnapshotStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<ExportSnapshotChunk, Status>> + Send>>;
type SbxExportSandboxStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<ExportSandboxChunk, Status>> + Send>>;
type TeardownSuspendDrivers = Arc<dashmap::DashMap<SandboxId, tokio::sync::OwnedSemaphorePermit>>;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TeardownSuspendDriverAdmission {
    Deferred,
    Started,
    Existing,
    Saturated,
}

impl TeardownSuspendDriverAdmission {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Deferred => "deferred",
            Self::Started => "started",
            Self::Existing => "existing",
            Self::Saturated => "saturated",
        }
    }
}

impl std::fmt::Display for TeardownSuspendDriverAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
struct TeardownSuspendDriverSlot {
    drivers: TeardownSuspendDrivers,
    sandbox_id: SandboxId,
}

impl Drop for TeardownSuspendDriverSlot {
    fn drop(&mut self) {
        self.drivers.remove(&self.sandbox_id);
    }
}

#[derive(Clone)]
pub struct SandboxManagerService {
    db: crate::db::SharedConnection,
    local_node_id: Option<NodeId>,
    local_artifacts: crate::local_artifacts::LocalArtifacts,
    watchdog: Arc<crate::watchdog::SandboxWatchdog>,
    snapshot_scheduler: Arc<crate::snapshot_scheduler::SnapshotScheduler>,
    auto_lifecycle: AutoLifecycleCoordinator,
    template_network_policies: TemplateNetworkPolicyCache,
    network_policy_reconcile_queue: NetworkPolicyReconcileQueue,
    network_policy_reconciler_started: Arc<AtomicBool>,
    cleanup_locks: CleanupLockMap,
    cluster_deleted_eviction_quiescence: Duration,
    /// Shared attempt capacity for foreground cleanup and durable retries.
    /// Excess work remains in SQLite until a slot is available.
    cleanup_sem: Arc<tokio::sync::Semaphore>,
    /// Shared blocking cleanup I/O budget, acquired before task allocation.
    cleanup_io: Arc<tokio::sync::Semaphore>,
    cleanup_target_streams: Arc<tokio::sync::Semaphore>,
    artifact_io: Arc<tokio::sync::Semaphore>,
    /// Network-paced exports cannot consume cleanup/rollback disk-work slots.
    export_io: Arc<tokio::sync::Semaphore>,
    cleanup_executor: Arc<cleanup_executor::CleanupExecutor>,
    /// Node-wide cap on legacy quarantine-recovery orphan restores. These run
    /// outside the serial inventory reconciler, but retain the old path's
    /// effective one-at-a-time I/O bound.
    quarantine_recovery_restore_sem: Arc<tokio::sync::Semaphore>,
    /// Node-wide admission for Firecracker process launches. Cold starts and
    /// restores share one capacity bucket.
    activation_admission_limiter: Arc<ActivationAdmissionLimiter>,
    /// At most one in-process driver may advance a persisted teardown suspend
    /// for a sandbox. The durable operation itself lives in SQLite; this map
    /// only bounds concurrent reattachment attempts from shim polling.
    teardown_suspend_drivers: TeardownSuspendDrivers,
    /// Node-wide admission for background durable-suspend drivers. A full
    /// semaphore returns `Pending` to the shim, whose next poll retries
    /// admission without leaving an unbounded queue of tasks on this node.
    teardown_suspend_driver_sem: Arc<tokio::sync::Semaphore>,
    /// Copies share one active slot and a bounded, deadline-limited queue.
    /// Busy callers retry their durable operation before staging any data.
    fork_copy_slots: pplx_space_common_grpc::QueuedSlots,
    fork_copy_waiting: Arc<AtomicUsize>,
    fork_copy_active: Arc<AtomicUsize>,
    /// In-flight operation staging roots owned by this service instance. The
    /// snapshot reaper shares this map so it cannot collect an active copy.
    active_fork_copy_operations: Arc<fork_artifact::CopyOperationReservations>,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
    node_role: Option<NodeRole>,
    node_managed_by: Option<NodeManagedBy>,
    cleanup_metrics: Arc<CleanupMetrics>,
    config: crate::config::SandboxConfig,
    peer_authorizer: UdsPeerAuthorizer,
    host_control_socket_path: PathBuf,
    credential_manager_endpoint: pplx_space_credmgr_client::CredmgrEndpoint,
    credential_manager_client:
        Arc<tokio::sync::OnceCell<pplx_space_credmgr_client::CredentialManagerGrpcClient>>,
    network_credentials_enabled: bool,
    node_manager: NodeManagerConnector,
    /// See [`SandboxManagerServiceOptions::volume_manager_socket_path`].
    volume_manager_socket_path: PathBuf,
    template_resolver: Arc<dyn crate::template_resolver::TemplateResolver>,
    /// See [`SandboxManagerServiceOptions::cleanup_run_dir`].
    #[cfg(test)]
    cleanup_run_dir: Option<PathBuf>,
    #[cfg(test)]
    create_cache_dir: Option<PathBuf>,
    gateway_caller_client: pplx_space_node_mgr_client::NodeManagerGrpcClient,
    gateway_authority: gateway_authority::GatewayAuthority,
    /// S3 upload config for suspend snapshots. `None` means uploads are disabled.
    snapshot_upload_config: Option<Arc<crate::snapshot_upload::SnapshotUploadConfig>>,
    /// Durable worker that drives the poll → metadata → register tail of the
    /// suspend-snapshot pipeline. Persists in sbx-mgr's local SQLite so a
    /// mid-flight process restart resumes the upload instead of orphaning it.
    snapshot_upload_worker: pplx_space_common_jobs::BackgroundWorker<
        crate::snapshot_upload_jobs::SnapshotUploadExecutor,
    >,
    /// Probe for "does this sandbox have a live Firecracker runtime on this
    /// node right now". Injected so tests can drive the suspend-checkpoint
    /// reattach path without a real `fc.pid` symlink on disk. Production uses
    /// [`is_sandbox_running`].
    sandbox_runtime_live: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    /// Probe for "is this sandbox's rootfs on this node's disk". The stopped
    /// materialization boundary checks it before vouching for a row, so a row
    /// whose bytes are gone is refused rather than reported as placed; an
    /// error means presence couldn't be verified. Injected so tests can answer
    /// it without `/space/cache`. Production uses
    /// [`create_support::sandbox_rootfs_exists_in`] over `CACHE_DIR`.
    sandbox_rootfs_present: RootfsPresenceProbe,
    /// Node-manager's answers about retained btrfs send sources. Injected so
    /// tests can drive recovery without a btrfs node-manager.
    retained_source_inspector: Arc<dyn crate::retained_source::RetainedSourceInspector>,
    drain_state: Arc<DrainState>,
    /// Eppo client for capability rollout. Disabled (every flag
    /// resolves to its default) when `EPPO_SDK_KEY` is unset.
    feature_flags: pplx_space_feature_flags::FeatureFlagClient,
    route_ed25519_verification_bundle: Arc<
        ArcSwapOption<
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationBundle,
        >,
    >,
    route_ed25519_verification_refresh_lock: Arc<Mutex<()>>,
    route_stream_epochs: Arc<Mutex<BTreeMap<SandboxId, RouteStreamEpoch>>>,
    service_port_ingress_admission: ServicePortIngressAdmission,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RouteStreamEpochIdentity(SandboxRouteIdentity);

impl RouteStreamEpochIdentity {
    fn from_runtime_update(
        sandbox_id: SandboxId,
        mut identity: SpaceApiRouteIdentity,
    ) -> Result<Option<Self>, Status> {
        if identity.is_absent() {
            return Ok(None);
        }
        identity.sandbox_id = Some(sandbox_id);
        Ok(Some(Self(match identity.try_into_complete() {
            Ok(identity) => identity,
            Err(IncompleteSandboxRouteIdentityError::InvalidProfile(error)) => {
                return Err(Status::failed_precondition(format!(
                    "runtime route identity has an invalid substrate profile: {error}"
                )));
            }
            Err(error) => {
                return Err(Status::failed_precondition(format!(
                    "runtime route identity is incomplete: {error}"
                )));
            }
        })))
    }

    fn from_stream_attachment(
        sandbox_id: SandboxId,
        mut identity: SpaceApiRouteIdentity,
    ) -> Result<Self, Status> {
        identity.sandbox_id = Some(sandbox_id);
        match identity.try_into_complete() {
            Ok(identity) => Ok(Self(identity)),
            Err(IncompleteSandboxRouteIdentityError::InvalidProfile(error)) => {
                Err(Status::failed_precondition(format!(
                    "sandbox route identity has an invalid substrate profile: {error}"
                )))
            }
            Err(error) => Err(Status::failed_precondition(format!(
                "sandbox route identity is incomplete for stream fencing: {error}"
            ))),
        }
    }

    fn wire_identity(&self) -> SpaceApiRouteIdentity {
        self.0.clone().into()
    }

    fn execution_generation(&self) -> Option<ExecutionGeneration> {
        Some(self.0.execution_generation())
    }
}

fn execution_only_provenance(identity: &SpaceApiRouteIdentity) -> bool {
    identity.execution_generation.is_some()
        && identity.route_generation.is_none()
        && identity.current_node_id.is_none()
        && identity.substrate_kind.is_none()
        && identity.pod_uid.is_none()
        && identity.activation_attempt_id.is_none()
}

fn validate_cold_start_provenance(
    sandbox: &crate::db::SandboxRow,
    incoming: &RouteStreamEpochIdentity,
) -> Result<(), Status> {
    let retained = &sandbox.space_api_route_identity;
    if sandbox.state == crate::state::SandboxState::Starting
        && execution_only_provenance(retained)
        && incoming.execution_generation() <= retained.execution_generation
    {
        return Err(Status::aborted(
            "cold activation must advance the retained execution generation",
        ));
    }
    Ok(())
}

fn persisted_route_stream_epoch(
    sandbox_id: SandboxId,
    sandbox: &crate::db::SandboxRow,
    activation_replacement_authority: ActivationRouteReplacementAuthority,
) -> Result<Option<RouteStreamEpochIdentity>, Status> {
    let identity = &sandbox.space_api_route_identity;
    let has_suspend_artifact = sandbox.current_local_artifact_id.is_some()
        && sandbox.current_local_artifact_kind
            == Some(pplx_space_common_core::types::ArtifactKind::Full)
        && sandbox.current_local_artifact_origin
            == Some(pplx_space_common_core::types::ArtifactOrigin::Suspend);
    // A completed full suspend keeps the producing execution generation and
    // pod UID as artifact provenance after its live routing fields are cleared.
    // That exact partial shape is not an attachable route epoch; a subsequent
    // complete activation identity must be allowed to replace it.
    let is_suspend_publication_provenance = matches!(
        sandbox.state,
        crate::state::SandboxState::Suspended | crate::state::SandboxState::Restoring
    ) && has_suspend_artifact
        && identity.route_generation.is_none()
        && identity.current_node_id.is_none()
        && identity.substrate_kind
            == Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes)
        && identity.execution_generation.is_some()
        && identity.pod_uid.is_some()
        && identity.activation_attempt_id.is_none();
    if is_suspend_publication_provenance {
        return Ok(None);
    }
    // A matching cleanup episode proves that this restore owns the runtime
    // publication boundary. Legacy suspended rows can lack today's artifact
    // pointer while retaining Kubernetes execution provenance, which is not an
    // attachable strict route epoch and must not fence out its owned replacement.
    let is_owned_legacy_restore_provenance = activation_replacement_authority
        == ActivationRouteReplacementAuthority::PersistedOwner
        && sandbox.state == crate::state::SandboxState::Restoring
        && identity.route_generation.is_none()
        && identity.execution_generation.is_some()
        && identity.substrate_kind
            == Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes);
    if is_owned_legacy_restore_provenance {
        return Ok(None);
    }
    // Orphan recovery publishes a suspend artifact with only its producing
    // execution generation; it cannot recover the old pod or live route.
    // A complete replacement may supersede that exact provenance shape only
    // when it owns the persisted restore. The runtime write still checks the
    // producing execution generation, so this does not admit an older restore.
    let is_owned_recovered_execution_provenance = activation_replacement_authority
        == ActivationRouteReplacementAuthority::PersistedOwner
        && sandbox.state == crate::state::SandboxState::Restoring
        && has_suspend_artifact
        && execution_only_provenance(identity);
    if is_owned_recovered_execution_provenance {
        return Ok(None);
    }
    // Stop clears the live route while retaining the producing execution as
    // cleanup provenance. An owned cold activation may replace that exact
    // partial identity after validate_cold_start_provenance requires a newer
    // execution, independently of the process-local route cache.
    let is_owned_cold_start_provenance = activation_replacement_authority
        == ActivationRouteReplacementAuthority::PersistedOwner
        && sandbox.state == crate::state::SandboxState::Starting
        && execution_only_provenance(identity);
    if is_owned_cold_start_provenance {
        return Ok(None);
    }
    RouteStreamEpochIdentity::from_runtime_update(sandbox_id, identity.clone())
}

#[derive(Clone)]
pub(super) struct RouteStreamFence {
    token: CancellationToken,
    close_reason: Arc<Mutex<Option<pplx_space_metrics::sandbox::worker::RouteStreamCloseReason>>>,
}

impl RouteStreamFence {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            close_reason: Arc::new(Mutex::new(None)),
        }
    }

    fn child(&self) -> Self {
        Self {
            token: self.token.child_token(),
            close_reason: Arc::clone(&self.close_reason),
        }
    }

    pub(super) fn child_token(&self) -> CancellationToken {
        self.token.child_token()
    }

    fn cancel(&self, reason: pplx_space_metrics::sandbox::worker::RouteStreamCloseReason) {
        if let Ok(mut close_reason) = self.close_reason.lock()
            && close_reason.is_none()
        {
            *close_reason = Some(reason);
        }
        self.token.cancel();
    }

    pub(super) async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    pub(super) fn close_reason(
        &self,
    ) -> pplx_space_metrics::sandbox::worker::RouteStreamCloseReason {
        self.close_reason
            .lock()
            .ok()
            .and_then(|reason| *reason)
            .unwrap_or(pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged)
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

#[derive(Clone)]
struct RouteStreamEpoch {
    identity: RouteStreamEpochIdentity,
    fence: RouteStreamFence,
    ingress: ingress_protection::Protection,
}

impl RouteStreamEpoch {
    fn new(identity: RouteStreamEpochIdentity) -> Self {
        Self {
            identity,
            fence: RouteStreamFence::new(),
            ingress: ingress_protection::Protection::default(),
        }
    }
}

#[derive(Clone, Default)]
struct ServicePortIngressAdmission {
    slots: Arc<dashmap::DashMap<SandboxId, Arc<tokio::sync::Semaphore>>>,
}

impl ServicePortIngressAdmission {
    fn try_acquire(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<ServicePortIngressAdmissionPermit, Status> {
        let slots = self
            .slots
            .entry(sandbox_id)
            .or_insert_with(|| {
                Arc::new(tokio::sync::Semaphore::new(
                    pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS,
                ))
            })
            .clone();
        let permit = slots.clone().try_acquire_owned().map_err(|_| {
            pplx_space_metrics::sandbox::worker::service_port_ingress_rejected(
                pplx_space_metrics::sandbox::worker::ServicePortIngressRejectionReason::Capacity,
            );
            pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status(
                "ServicePortIngress per-sandbox concurrency limit exceeded",
            )
        })?;
        Ok(ServicePortIngressAdmissionPermit {
            permit: Some(permit),
            slots: Some(slots),
            admission: self.clone(),
            sandbox_id,
        })
    }

    #[cfg(test)]
    fn available_permits(&self, sandbox_id: SandboxId) -> usize {
        self.slots.get(&sandbox_id).map_or(
            pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS,
            |slots| slots.available_permits(),
        )
    }

    #[cfg(test)]
    fn tracked_sandboxes(&self) -> usize {
        self.slots.len()
    }
}

struct ServicePortIngressAdmissionPermit {
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    slots: Option<Arc<tokio::sync::Semaphore>>,
    admission: ServicePortIngressAdmission,
    sandbox_id: SandboxId,
}

impl Drop for ServicePortIngressAdmissionPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        let Some(slots_arc) = self.slots.take() else {
            return;
        };
        let slots = Arc::downgrade(&slots_arc);
        drop(slots_arc);
        self.admission
            .slots
            .remove_if(&self.sandbox_id, |_, current| {
                std::sync::Weak::ptr_eq(&Arc::downgrade(current), &slots)
                    && current.available_permits()
                        == pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS
                    && Arc::strong_count(current) == 1
            });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RouteVerificationBundleRefresh {
    Updated(
        pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
    ),
    Unchanged(
        pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
    ),
    Absent,
}

#[derive(Debug, thiserror::Error)]
enum RouteVerificationBundleRefreshError {
    #[error("route verification bundle reload failed: {0}")]
    Invalid(#[source] pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error),
    #[error("route verification bundle disappeared after generation {generation} was installed")]
    Missing {
        generation: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
    },
    #[error(
        "route verification bundle generation moved backwards: current={current}, incoming={incoming}"
    )]
    Stale {
        current: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
        incoming: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
    },
    #[error("route verification bundle changed without increasing generation {generation}")]
    Conflict {
        generation: pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationGeneration,
    },
    #[error("{0}")]
    Internal(&'static str),
}

impl RouteVerificationBundleRefreshError {
    fn metric_result(
        &self,
    ) -> pplx_space_metrics::sandbox::worker::RouteVerificationBundleReloadResult {
        use pplx_space_metrics::sandbox::worker::RouteVerificationBundleReloadResult;
        match self {
            Self::Invalid(_) => RouteVerificationBundleReloadResult::Invalid,
            Self::Missing { .. } => RouteVerificationBundleReloadResult::Missing,
            Self::Stale { .. } => RouteVerificationBundleReloadResult::Stale,
            Self::Conflict { .. } => RouteVerificationBundleReloadResult::Conflict,
            Self::Internal(_) => RouteVerificationBundleReloadResult::Invalid,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActivationRouteReplacementAuthority {
    Unowned,
    PersistedOwner,
    /// The identity arrived on a peer-authorized activation RPC from the
    /// control plane, which mints the activation attempt and signs route
    /// tokens against it. It may therefore rotate the attempt within one
    /// execution generation; the runtime has no basis to refuse.
    ControlPlaneActivation,
}

fn validate_authoritative_route_epoch_transition(
    current: &RouteStreamEpochIdentity,
    incoming: &RouteStreamEpochIdentity,
    activation_replacement_authority: ActivationRouteReplacementAuthority,
) -> Result<(), Status> {
    if current == incoming {
        return Ok(());
    }
    let current = current.wire_identity();
    let incoming = incoming.wire_identity();
    let route_generation_advanced = match (current.route_generation, incoming.route_generation) {
        (Some(current), Some(incoming)) if incoming.get() < current.get() => {
            return Err(Status::aborted(
                "runtime route generation is older than the published stream epoch",
            ));
        }
        (Some(current), Some(incoming)) => incoming.get() > current.get(),
        (Some(_), None) => {
            return Err(Status::failed_precondition(
                "runtime route identity cannot remove a published route generation",
            ));
        }
        _ => false,
    };

    let execution_generation_advanced =
        match (current.execution_generation, incoming.execution_generation) {
            (Some(current_execution), Some(incoming_execution))
                if incoming_execution.get() < current_execution.get() =>
            {
                return Err(Status::aborted(
                    "runtime execution generation is older than the published stream epoch",
                ));
            }
            (Some(current_execution), Some(incoming_execution)) => {
                incoming_execution.get() > current_execution.get()
            }
            (Some(_), None) => {
                return Err(Status::failed_precondition(
                    "runtime route identity cannot remove a published execution generation",
                ));
            }
            _ => false,
        };

    if current.activation_attempt_id.is_some() && incoming.activation_attempt_id.is_none() {
        return Err(Status::failed_precondition(
            "runtime route identity cannot remove a published activation attempt",
        ));
    }
    if route_generation_advanced && execution_generation_advanced {
        return Ok(());
    }
    if execution_generation_advanced && current.route_generation.is_none() {
        // Legacy direct/runtime rows can predate route-generation
        // persistence. Their execution generation is still an
        // authoritative monotonic replacement fence.
        return Ok(());
    }

    if current.execution_generation == incoming.execution_generation
        && matches!(
            (current.activation_attempt_id, incoming.activation_attempt_id),
            (Some(current), Some(incoming)) if current != incoming
        )
    {
        if !optional_identity_field_is_enrichment(&current.substrate_kind, &incoming.substrate_kind)
            || !optional_identity_field_is_enrichment(&current.pod_uid, &incoming.pod_uid)
        {
            return Err(Status::failed_precondition(
                "runtime route identity cannot change substrate ownership at the same generation",
            ));
        }
        return match activation_replacement_authority {
            ActivationRouteReplacementAuthority::PersistedOwner
            | ActivationRouteReplacementAuthority::ControlPlaneActivation => Ok(()),
            ActivationRouteReplacementAuthority::Unowned => Err(Status::failed_precondition(
                "same-generation route identity replacement does not own the persisted activation attempt",
            )),
        };
    }
    if route_identity_is_enrichment(&current, &incoming) {
        return Ok(());
    }
    Err(Status::failed_precondition(
        "runtime route identity conflicts with the current stream epoch",
    ))
}

fn route_identity_is_enrichment(
    current: &SpaceApiRouteIdentity,
    incoming: &SpaceApiRouteIdentity,
) -> bool {
    optional_identity_field_is_enrichment(&current.sandbox_id, &incoming.sandbox_id)
        && optional_identity_field_is_enrichment(
            &current.route_generation,
            &incoming.route_generation,
        )
        && optional_identity_field_is_enrichment(
            &current.current_node_id,
            &incoming.current_node_id,
        )
        && optional_identity_field_is_enrichment(&current.substrate_kind, &incoming.substrate_kind)
        && optional_identity_field_is_enrichment(
            &current.execution_generation,
            &incoming.execution_generation,
        )
        && optional_identity_field_is_enrichment(
            &current.activation_attempt_id,
            &incoming.activation_attempt_id,
        )
        && optional_identity_field_is_enrichment(&current.pod_uid, &incoming.pod_uid)
}

fn optional_identity_field_is_enrichment<T: Eq>(current: &Option<T>, incoming: &Option<T>) -> bool {
    match current {
        Some(current) => incoming.as_ref() == Some(current),
        None => true,
    }
}

fn persisted_activation_owns_route_identity(
    db: &crate::db::Connection,
    sandbox_id: SandboxId,
    cleanup_id: SandboxCleanupEpisodeId,
    incoming: &RouteStreamEpochIdentity,
) -> Result<bool, Status> {
    let sandbox_id = sandbox_id.to_string();
    let Some(episode) = crate::db::read::sandbox_view(db, &sandbox_id)
        .cleanup_episode()
        .strict()
        .map_err(|error| Status::internal(format!("db error: {error}")))?
    else {
        return Ok(false);
    };
    let Some(sandbox) = crate::db::read::sandbox(db, &sandbox_id)
        .map_err(|error| Status::internal(format!("db error: {error}")))?
    else {
        return Ok(false);
    };
    let incoming = incoming.wire_identity();
    Ok(episode.id == cleanup_id
        && episode.requested_at.is_none()
        && episode.execution_generation == incoming.execution_generation
        && episode.activation_attempt_id == incoming.activation_attempt_id
        && episode.activation_state == sandbox.state
        && sandbox.cleanup_completion == Some(episode.completion)
        && sandbox.cleanup_keep_filesystem == Some(episode.keep_filesystem))
}

fn map_runtime_fields_update_error(error: crate::db::Error) -> Status {
    if crate::db::auto_lifecycle_delete_receipts::is_pending_receipt_execution_fence(&error) {
        return Status::aborted("pending auto-lifecycle delete receipt owns the sandbox execution");
    }
    if crate::db::auto_lifecycle_delete_receipts::is_lifecycle_execution_fence(&error) {
        return Status::aborted("sandbox lifecycle or cleanup owns the sandbox execution");
    }
    match error {
        e if e.is_invalid_query() => Status::aborted("runtime execution generation is stale"),
        e if e.is_no_rows() => Status::not_found("sandbox not found"),
        error => Status::internal(format!("db error: {error}")),
    }
}

fn route_identity_adoption_write_rejection_reason(
    error: &crate::db::Error,
) -> pplx_space_metrics::sandbox::worker::RouteIdentityAdoptionReason {
    use pplx_space_metrics::sandbox::worker::RouteIdentityAdoptionReason;
    if crate::db::auto_lifecycle_delete_receipts::is_pending_receipt_execution_fence(error) {
        RouteIdentityAdoptionReason::PendingReceiptOwnsExecution
    } else if crate::db::auto_lifecycle_delete_receipts::is_lifecycle_execution_fence(error) {
        RouteIdentityAdoptionReason::LifecycleOwnsExecution
    } else {
        match error {
            error if error.is_invalid_query() => RouteIdentityAdoptionReason::StaleOrConflict,
            error if error.is_no_rows() => RouteIdentityAdoptionReason::NotFound,
            _ => RouteIdentityAdoptionReason::Internal,
        }
    }
}

fn install_authoritative_route_epoch(
    epochs: &mut BTreeMap<SandboxId, RouteStreamEpoch>,
    sandbox_id: SandboxId,
    identity: RouteStreamEpochIdentity,
) {
    match epochs.get_mut(&sandbox_id) {
        Some(current) if current.identity == identity => {}
        Some(current) => {
            current.fence.cancel(
                pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged,
            );
            *current = RouteStreamEpoch::new(identity);
        }
        None => {
            epochs.insert(sandbox_id, RouteStreamEpoch::new(identity));
        }
    }
}

#[derive(Clone)]
pub struct SandboxManagerServiceOptions {
    pub host_control_socket_path: PathBuf,
    pub credential_manager_socket_path: PathBuf,
    pub sandbox_runtime_live: Arc<dyn Fn(&str) -> bool + Send + Sync>,
    pub sandbox_rootfs_present: RootfsPresenceProbe,
    pub shutdown_token: CancellationToken,
    pub task_tracker: TaskTracker,
    /// Stable identity of this node. Tests can inject availability explicitly;
    /// production resolves it once when constructing the service.
    pub local_node_id: Option<NodeId>,
    pub node_role: Option<NodeRole>,
    /// Trusted deployment substrate; absence is not evidence of Direct ownership.
    pub node_managed_by: Option<NodeManagedBy>,
    pub route_ed25519_verification_bundle: Option<
        Arc<pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519VerificationBundle>,
    >,
    /// Every node-manager client the service and its subsystems use.
    pub node_manager: NodeManagerConnector,
    /// The volume-manager socket that snapshot uploads stage their transfers
    /// with and observe them through. Restore, fork, cleanup and the
    /// btrfs-send capability probe still dial the fixed volume-manager socket.
    pub volume_manager_socket_path: PathBuf,
    /// Every template-manager `GetTemplate` the service and its subsystems ask.
    pub template_resolver: Arc<dyn crate::template_resolver::TemplateResolver>,
    /// Run directory whose `fc.pid` links stop and delete target selection
    /// reads in test builds. `None` reads the fixed sandbox run directory.
    #[cfg(test)]
    pub(crate) cleanup_run_dir: Option<PathBuf>,
    /// Cache root for logical Create and checkpoint admission tests; later VM work uses its own fixtures.
    #[cfg(test)]
    pub(crate) create_cache_dir: Option<PathBuf>,
}

impl Default for SandboxManagerServiceOptions {
    fn default() -> Self {
        Self {
            host_control_socket_path: PathBuf::from(
                pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET,
            ),
            credential_manager_socket_path: PathBuf::from(
                pplx_space_credmgr_client::CREDMGR_SOCKET,
            ),
            sandbox_runtime_live: Arc::new(is_sandbox_running),
            sandbox_rootfs_present: Arc::new(|id: &str| {
                create_support::sandbox_rootfs_exists_in(std::path::Path::new(CACHE_DIR), id)
            }),
            shutdown_token: CancellationToken::new(),
            task_tracker: TaskTracker::new(),
            local_node_id: local_space_node_id(),
            node_role: None,
            node_managed_by: None,
            route_ed25519_verification_bundle: None,
            node_manager: NodeManagerConnector::host(),
            volume_manager_socket_path: PathBuf::from(pplx_space_volmgr_client::VOLMGR_SOCKET),
            template_resolver: Arc::new(crate::template_resolver::HostTemplateManager),
            #[cfg(test)]
            cleanup_run_dir: None,
            #[cfg(test)]
            create_cache_dir: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RefundInterruptedSnapshotUploadAttemptsError {
    #[error("sandbox database mutex is poisoned")]
    DatabaseMutexPoisoned,
    #[error("failed to refund interrupted snapshot upload attempts: {0}")]
    Database(#[from] crate::db::Error),
}

impl SandboxManagerService {
    /// The sandbox cache root: [`CACHE_DIR`], or a test's own root.
    pub(super) fn cache_root(&self) -> &Path {
        #[cfg(test)]
        if let Some(root) = self.create_cache_dir.as_deref() {
            return root;
        }
        Path::new(CACHE_DIR)
    }

    pub(crate) fn db_conn(&self) -> Result<crate::db::ConnectionGuard<'_>, Status> {
        self.db.lock().map_err(|_| {
            // Poison is not recoverable connection health. Keep ownership
            // through the normal drain and let the supervisor restart us.
            self.fail_poisoned_database();
            Status::internal("database mutex poisoned")
        })
    }

    fn refresh_route_ed25519_verification_bundle(
        &self,
    ) -> Result<RouteVerificationBundleRefresh, RouteVerificationBundleRefreshError> {
        use pplx_space_common_svc::space_api_route_ed25519::load_space_api_route_ed25519_verification_bundle;

        let _refresh_guard = self
            .route_ed25519_verification_refresh_lock
            .lock()
            .map_err(|_| {
                RouteVerificationBundleRefreshError::Internal(
                    "route verification bundle refresh mutex poisoned",
                )
            })?;
        let incoming = load_space_api_route_ed25519_verification_bundle()
            .map_err(RouteVerificationBundleRefreshError::Invalid)?;
        let current = self.route_ed25519_verification_bundle.load_full();
        match (current.as_deref(), incoming) {
            (None, None) => Ok(RouteVerificationBundleRefresh::Absent),
            (Some(current), None) => Err(RouteVerificationBundleRefreshError::Missing {
                generation: current.generation,
            }),
            (None, Some(incoming)) => {
                let generation = incoming.generation;
                self.route_ed25519_verification_bundle
                    .store(Some(Arc::new(incoming)));
                Ok(RouteVerificationBundleRefresh::Updated(generation))
            }
            (Some(current), Some(incoming)) => match current.classify_update(&incoming) {
                Ok(SpaceApiRouteEd25519VerificationBundleUpdate::Unchanged) => Ok(
                    RouteVerificationBundleRefresh::Unchanged(current.generation),
                ),
                Ok(SpaceApiRouteEd25519VerificationBundleUpdate::Updated) => {
                    let generation = incoming.generation;
                    self.route_ed25519_verification_bundle
                        .store(Some(Arc::new(incoming)));
                    Ok(RouteVerificationBundleRefresh::Updated(generation))
                }
                Err(SpaceApiRouteEd25519VerificationBundleUpdateError::StaleGeneration {
                    current,
                    incoming,
                }) => Err(RouteVerificationBundleRefreshError::Stale { current, incoming }),
                Err(SpaceApiRouteEd25519VerificationBundleUpdateError::GenerationConflict {
                    generation,
                }) => Err(RouteVerificationBundleRefreshError::Conflict { generation }),
            },
        }
    }

    pub fn refresh_route_ed25519_verification_bundle_now(&self) -> bool {
        match self.refresh_route_ed25519_verification_bundle() {
            Ok(RouteVerificationBundleRefresh::Updated(generation)) => {
                self.watchdog
                    .set_expected_route_verification_key_generation(generation.get());
                pplx_space_metrics::sandbox::worker::route_verification_bundle_reload(
                    pplx_space_metrics::sandbox::worker::RouteVerificationBundleReloadResult::Updated,
                );
                pplx_space_metrics::sandbox::worker::route_verification_bundle_generation(
                    generation.get(),
                );
                tracing::info!(
                    generation = %generation,
                    "installed refreshed SPACE API route verification bundle"
                );
                true
            }
            Ok(RouteVerificationBundleRefresh::Unchanged(generation)) => {
                pplx_space_metrics::sandbox::worker::route_verification_bundle_reload(
                    pplx_space_metrics::sandbox::worker::RouteVerificationBundleReloadResult::Unchanged,
                );
                pplx_space_metrics::sandbox::worker::route_verification_bundle_generation(
                    generation.get(),
                );
                false
            }
            Ok(RouteVerificationBundleRefresh::Absent) => {
                pplx_space_metrics::sandbox::worker::route_verification_bundle_reload(
                    pplx_space_metrics::sandbox::worker::RouteVerificationBundleReloadResult::Absent,
                );
                false
            }
            Err(error) => {
                pplx_space_metrics::sandbox::worker::route_verification_bundle_reload(
                    error.metric_result(),
                );
                tracing::warn!(
                    %error,
                    "retaining last-known-good SPACE API route verification bundle"
                );
                false
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn with_retained_source_inspector(
        mut self,
        inspector: Arc<dyn crate::retained_source::RetainedSourceInspector>,
    ) -> Self {
        self.retained_source_inspector = inspector;
        self
    }

    #[cfg(test)]
    pub(super) fn update_runtime_fields(
        &self,
        sandbox_id: SandboxId,
        fields: crate::db::RuntimeFields<'_>,
    ) -> Result<crate::db::SupersededTeardownGeneration, Status> {
        self.update_runtime_fields_with_activation_owner(sandbox_id, None, fields)
    }

    /// Republish the route identity of an already-running VM from an
    /// authoritative activation RPC.
    ///
    /// Wake against a sandbox that is already `Started` (a claimed warm-pool
    /// pod) or `Paused` (resume) performs no runtime transition, so nothing
    /// on those paths writes runtime fields — the activation attempt the
    /// control plane just minted would be dropped, and every route token it
    /// signs afterwards would be rejected against the runtime's older
    /// identity. Adopt it here instead.
    pub(super) fn adopt_authoritative_route_identity(
        &self,
        sandbox_id: SandboxId,
        incoming: &SpaceApiRouteIdentity,
    ) -> Result<(), Status> {
        use pplx_space_metrics::sandbox::worker::{
            RouteIdentityAdoptionOutcome, RouteIdentityAdoptionReason, route_identity_adoption,
        };
        if incoming.is_absent() {
            route_identity_adoption(
                RouteIdentityAdoptionOutcome::Inert,
                RouteIdentityAdoptionReason::Absent,
            );
            return Ok(());
        }
        let db = self.db_conn().inspect_err(|_| {
            route_identity_adoption(
                RouteIdentityAdoptionOutcome::Rejected,
                RouteIdentityAdoptionReason::Internal,
            )
        })?;
        let mut epochs = self
            .route_stream_epochs
            .lock()
            .map_err(|error| {
                Status::internal(format!("route stream epoch mutex poisoned: {error}"))
            })
            .inspect_err(|_| {
                route_identity_adoption(
                    RouteIdentityAdoptionOutcome::Rejected,
                    RouteIdentityAdoptionReason::Internal,
                )
            })?;
        let Some(sandbox) = crate::db::read::sandbox(&db, &sandbox_id.to_string())
            .map_err(|error| Status::internal(format!("db error: {error}")))
            .inspect_err(|_| {
                route_identity_adoption(
                    RouteIdentityAdoptionOutcome::Rejected,
                    RouteIdentityAdoptionReason::Internal,
                )
            })?
        else {
            route_identity_adoption(
                RouteIdentityAdoptionOutcome::Rejected,
                RouteIdentityAdoptionReason::NotFound,
            );
            return Err(Status::not_found("sandbox not found"));
        };
        let current = sandbox.space_api_route_identity;
        let mut local = current.clone();
        if incoming.effective_substrate_kind() == Some(NodeManagedBy::Space)
            && local.effective_substrate_kind() != Some(NodeManagedBy::Kubernetes)
        {
            local.sandbox_id.get_or_insert(sandbox_id);
            local.current_node_id = local.current_node_id.or(self.local_node_id);
            local.substrate_kind.get_or_insert(NodeManagedBy::Space);
        }
        if local.effective_substrate_kind() == Some(NodeManagedBy::Kubernetes) {
            // The runtime, not the incoming request, owns this dimension.
            // Older Kubernetes rows can lack it even on a resident execution.
            // Keep any existing value: hydration must not hide a node mismatch.
            local.current_node_id = local.current_node_id.or(self.local_node_id);
        }
        let merged = merge_authoritative_route_identity(&local, incoming);
        let metadata = crate::db::read::route_identity_adoption(&db, sandbox_id)
            .map_err(|error| Status::internal(format!("inspect route identity metadata: {error}")))
            .inspect_err(|_| {
                route_identity_adoption(
                    RouteIdentityAdoptionOutcome::Rejected,
                    RouteIdentityAdoptionReason::Internal,
                );
            })?;
        let candidate_provenance =
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::for_authoritative_adoption(
                &local,
                incoming,
            );
        let provenance = if merged == local
            && metadata.current_provenance
                == Some(
                    crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced,
                ) {
            crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance::ClusterFenced
        } else {
            candidate_provenance
        };
        // Kubernetes hydration must persist/install the repaired epoch even
        // when it changes only runtime-owned fields. Direct keeps its existing
        // metadata-only hydration path: comparing its incomplete persisted row
        // as an epoch would reject the very fields being repaired here.
        let metadata_only_adoption =
            if local.effective_substrate_kind() == Some(NodeManagedBy::Kubernetes) {
                merged == current
            } else {
                merged == local
            };
        if metadata_only_adoption {
            // Equality of the route fields does not prove the migration-only
            // provenance and receipt mode are current. Persist an authoritative
            // observation even when it does not advance the stream epoch.
            let pending_receipt = crate::db::read::typed_sandbox_view(&db, sandbox_id)
                .delete_receipts()
                .pending()
                .map_err(|error| {
                    route_identity_adoption(
                        RouteIdentityAdoptionOutcome::Rejected,
                        RouteIdentityAdoptionReason::Internal,
                    );
                    Status::internal(format!(
                        "inspect pending auto-lifecycle delete receipt: {error}"
                    ))
                })?;
            let supersedes_pending_receipt = pending_receipt.is_some_and(|receipt| {
                merged
                    .execution_generation
                    .is_some_and(|generation| generation > receipt.execution_generation)
            });
            if metadata.is_current(&merged, provenance) && !supersedes_pending_receipt {
                route_identity_adoption(
                    RouteIdentityAdoptionOutcome::Inert,
                    RouteIdentityAdoptionReason::Unchanged,
                );
                return Ok(());
            }
            crate::db::update_space_api_route_identity(&db, sandbox_id, &merged, provenance)
                .map_err(|error| {
                    route_identity_adoption(
                        RouteIdentityAdoptionOutcome::Rejected,
                        route_identity_adoption_write_rejection_reason(&error),
                    );
                    map_runtime_fields_update_error(error)
                })?;
            route_identity_adoption(
                RouteIdentityAdoptionOutcome::Adopted,
                RouteIdentityAdoptionReason::ControlPlaneMetadata,
            );
            return Ok(());
        }
        // A wake whose target lifecycle is already satisfied performed no route
        // work before this adoption existed, so an identity that cannot be
        // published as an epoch leaves the runtime as it was instead of
        // failing the wake. Only a stale identity that would move a published
        // epoch backwards is rejected, below.
        let Ok(Some(merged_epoch)) =
            RouteStreamEpochIdentity::from_runtime_update(sandbox_id, merged.clone())
        else {
            route_identity_adoption(
                RouteIdentityAdoptionOutcome::Inert,
                RouteIdentityAdoptionReason::Unpublishable,
            );
            return Ok(());
        };
        let current_epoch = match epochs.get(&sandbox_id) {
            Some(current) => Some(current.identity.clone()),
            None => {
                // A legacy Kubernetes row may never have had a complete
                // epoch. Retain every known control-plane dimension as the
                // comparison floor; fill only absent ones from the candidate.
                // Runtime-owned dimensions were preserved/hydrated above and
                // merged_epoch already proved that profile complete. Never
                // discard a known generation just because another field was
                // missing. A published in-memory epoch always wins above.
                let previous =
                    if local.effective_substrate_kind() == Some(NodeManagedBy::Kubernetes) {
                        merge_authoritative_route_identity(&merged, &local)
                    } else {
                        current
                    };
                RouteStreamEpochIdentity::from_runtime_update(sandbox_id, previous)?
            }
        };
        if let Some(current_epoch) = current_epoch {
            validate_authoritative_route_epoch_transition(
                &current_epoch,
                &merged_epoch,
                ActivationRouteReplacementAuthority::ControlPlaneActivation,
            )
            .inspect_err(|_| {
                route_identity_adoption(
                    RouteIdentityAdoptionOutcome::Rejected,
                    RouteIdentityAdoptionReason::StaleOrConflict,
                )
            })?;
        } else {
            // Deliberate: an incomplete local row has never published a stream
            // epoch, so there is no monotonic epoch to protect. The merge still
            // accepts only control-plane-minted dimensions and preserves every
            // runtime-owned dimension.
        }
        crate::db::update_space_api_route_identity(&db, sandbox_id, &merged, provenance).map_err(
            |error| {
                route_identity_adoption(
                    RouteIdentityAdoptionOutcome::Rejected,
                    route_identity_adoption_write_rejection_reason(&error),
                );
                map_runtime_fields_update_error(error)
            },
        )?;
        install_authoritative_route_epoch(&mut epochs, sandbox_id, merged_epoch);
        self.enqueue_network_policy_reconcile(sandbox_id.to_string());
        route_identity_adoption(
            RouteIdentityAdoptionOutcome::Adopted,
            RouteIdentityAdoptionReason::ControlPlaneActivation,
        );
        Ok(())
    }

    pub(super) fn update_runtime_fields_for_activation(
        &self,
        sandbox_id: SandboxId,
        cleanup_id: SandboxCleanupEpisodeId,
        fields: crate::db::RuntimeFields<'_>,
    ) -> Result<crate::db::SupersededTeardownGeneration, Status> {
        self.update_runtime_fields_with_activation_owner(sandbox_id, Some(cleanup_id), fields)
    }

    fn update_runtime_fields_with_activation_owner(
        &self,
        sandbox_id: SandboxId,
        cleanup_id: Option<SandboxCleanupEpisodeId>,
        fields: crate::db::RuntimeFields<'_>,
    ) -> Result<crate::db::SupersededTeardownGeneration, Status> {
        let route_identity = RouteStreamEpochIdentity::from_runtime_update(
            sandbox_id,
            fields.space_api_route_identity.clone(),
        )?;
        let db = self.db_conn()?;
        let mut epochs = self.route_stream_epochs.lock().map_err(|error| {
            Status::internal(format!("route stream epoch mutex poisoned: {error}"))
        })?;
        if let Some(route_identity) = route_identity.as_ref() {
            let activation_replacement_authority = match cleanup_id {
                Some(cleanup_id)
                    if persisted_activation_owns_route_identity(
                        &db,
                        sandbox_id,
                        cleanup_id,
                        route_identity,
                    )? =>
                {
                    ActivationRouteReplacementAuthority::PersistedOwner
                }
                _ => ActivationRouteReplacementAuthority::Unowned,
            };
            let persisted = crate::db::read::sandbox(&db, &sandbox_id.to_string())
                .map_err(|error| Status::internal(format!("db error: {error}")))?;
            if let Some(sandbox) = persisted.as_ref() {
                validate_cold_start_provenance(sandbox, route_identity)?;
            }
            let current_identity = match epochs.get(&sandbox_id) {
                Some(current) => Some(current.identity.clone()),
                None => persisted
                    .map(|sandbox| {
                        persisted_route_stream_epoch(
                            sandbox_id,
                            &sandbox,
                            activation_replacement_authority,
                        )
                    })
                    .transpose()?
                    .flatten(),
            };
            if let Some(current_identity) = current_identity {
                validate_authoritative_route_epoch_transition(
                    &current_identity,
                    route_identity,
                    activation_replacement_authority,
                )?;
            }
        }
        let result = match cleanup_id {
            Some(cleanup_id) => {
                crate::db::update_runtime_fields_for_activation(&db, sandbox_id, cleanup_id, fields)
            }
            // Only the test-only `update_runtime_fields` wrapper passes no owner.
            #[cfg(test)]
            None => crate::db::update_runtime_fields(&db, sandbox_id, fields),
            #[cfg(not(test))]
            None => return Err(Status::internal("runtime fields update has no owner")),
        }
        .map_err(map_runtime_fields_update_error)?;
        if let Some(route_identity) = route_identity {
            install_authoritative_route_epoch(&mut epochs, sandbox_id, route_identity);
            // A just-launched gateway starts without process-local Ed25519
            // verification state. Reconcile immediately after publishing its
            // authoritative runtime identity so it receives the same current
            // bundle as already-running gateways refreshed after rotation.
            self.enqueue_network_policy_reconcile(sandbox_id.to_string());
        }
        Ok(result)
    }

    fn route_stream_identity_token(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<RouteStreamFence, Status> {
        let sandbox_id = sandbox.id.parse::<SandboxId>().map_err(|error| {
            Status::internal(format!("persisted sandbox id is invalid: {error}"))
        })?;
        let expected_identity = RouteStreamEpochIdentity::from_stream_attachment(
            sandbox_id,
            sandbox.space_api_route_identity.clone(),
        )?;
        let db = self.db_conn()?;
        let current = crate::db::read::sandbox(&db, &sandbox.id)
            .map_err(|error| Status::internal(format!("db error: {error}")))?
            .ok_or_else(|| Status::not_found(format!("sandbox not found: {}", sandbox.id)))?;
        let current_identity = RouteStreamEpochIdentity::from_stream_attachment(
            sandbox_id,
            current.space_api_route_identity,
        )?;
        if current_identity != expected_identity {
            return Err(Status::aborted(
                "sandbox route identity changed while the stream was opening",
            ));
        }

        let mut epochs = self.route_stream_epochs.lock().map_err(|error| {
            Status::internal(format!("route stream epoch mutex poisoned: {error}"))
        })?;
        match epochs.get(&sandbox_id) {
            Some(epoch) if epoch.identity == current_identity => Ok(epoch.fence.child()),
            Some(_) => Err(Status::aborted(
                "sandbox route stream epoch changed while the stream was opening",
            )),
            None => {
                let epoch = RouteStreamEpoch::new(current_identity);
                let fence = epoch.fence.child();
                epochs.insert(sandbox_id, epoch);
                Ok(fence)
            }
        }
    }

    fn service_port_ingress_identity_lease(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<(RouteStreamFence, ServicePortIngressAdmissionPermit), Status> {
        let sandbox_id = sandbox.id.parse::<SandboxId>().map_err(|error| {
            Status::internal(format!("persisted sandbox id is invalid: {error}"))
        })?;
        let fence = self.route_stream_identity_token(sandbox)?;
        let admission = self
            .service_port_ingress_admission
            .try_acquire(sandbox_id)?;
        Ok((fence, admission))
    }

    fn ingress_protection(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<Option<ingress_protection::Protection>, Status> {
        let sandbox_id = sandbox
            .id
            .parse::<SandboxId>()
            .map_err(|error| Status::internal(error.to_string()))?;
        let epochs = self
            .route_stream_epochs
            .lock()
            .map_err(|error| Status::internal(error.to_string()))?;
        if !epochs.contains_key(&sandbox_id) {
            return Ok(None);
        }
        let expected = RouteStreamEpochIdentity::from_stream_attachment(
            sandbox_id,
            sandbox.space_api_route_identity.clone(),
        )?;
        Ok(epochs
            .get(&sandbox_id)
            .filter(|epoch| epoch.identity == expected)
            .map(|epoch| epoch.ingress.clone()))
    }

    pub(super) fn cancel_route_streams_for_generation(
        &self,
        sandbox_id: SandboxId,
        execution_generation: Option<ExecutionGeneration>,
    ) -> Result<(), Status> {
        let mut epochs = self.route_stream_epochs.lock().map_err(|error| {
            Status::internal(format!("route stream epoch mutex poisoned: {error}"))
        })?;
        let should_cancel =
            epochs
                .get(&sandbox_id)
                .is_some_and(|epoch| match execution_generation {
                    Some(execution_generation) => {
                        Some(execution_generation) == epoch.identity.execution_generation()
                    }
                    None => true,
                });
        if should_cancel && let Some(epoch) = epochs.remove(&sandbox_id) {
            epoch.fence.cancel(
                pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::LifecycleTeardown,
            );
        }
        Ok(())
    }

    pub(super) fn cancel_all_route_streams(&self, sandbox_id: SandboxId) -> Result<(), Status> {
        let mut epochs = self.route_stream_epochs.lock().map_err(|error| {
            Status::internal(format!("route stream epoch mutex poisoned: {error}"))
        })?;
        if let Some(epoch) = epochs.remove(&sandbox_id) {
            epoch.fence.cancel(
                pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::LifecycleTeardown,
            );
        }
        Ok(())
    }

    /// Run a durable route-identity mutation and retire every affected
    /// in-memory authorization epoch before exposing its result to the caller.
    pub(super) fn apply_route_identity_mutation<T, E>(
        &self,
        sandbox_id: SandboxId,
        operation: &'static str,
        mutation: impl FnOnce() -> Result<(T, bool), E>,
    ) -> Result<T, E> {
        self.apply_route_identity_mutations(operation, || {
            let (value, identity_changed) = mutation()?;
            let changed_ids = identity_changed.then_some(sandbox_id).into_iter().collect();
            Ok((value, changed_ids))
        })
    }

    pub(super) fn apply_route_identity_mutations<T, E>(
        &self,
        operation: &'static str,
        mutation: impl FnOnce() -> Result<(T, Vec<SandboxId>), E>,
    ) -> Result<T, E> {
        let (value, changed_ids) = mutation()?;
        for sandbox_id in changed_ids {
            if let Err(error) = self.cancel_all_route_streams(sandbox_id) {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    %error,
                    operation,
                    "failed to retire route stream epoch after runtime identity mutation"
                );
            }
        }
        Ok(value)
    }

    pub fn new_with_options(
        conn: crate::db::Connection,
        options: SandboxManagerServiceOptions,
    ) -> Self {
        Self::new_with_config(conn, options, crate::config::SandboxConfig::default())
    }

    pub fn new_with_config(
        conn: crate::db::Connection,
        options: SandboxManagerServiceOptions,
        config: crate::config::SandboxConfig,
    ) -> Self {
        let expected_route_verification_key_generation = options
            .route_ed25519_verification_bundle
            .as_ref()
            .map(|bundle| bundle.generation.get());
        if options.node_role != Some(NodeRole::Worker) {
            match crate::db::read::counts(&conn, crate::db::read::DeleteOutbox) {
                Ok(stats) if stats.pending_count.saturating_add(stats.completed_count) != 0 => {
                    tracing::warn!(
                        node_role = ?options.node_role,
                        pending = stats.pending_count,
                        completed = stats.completed_count,
                        "non-worker sandbox-manager has auto-lifecycle delete receipts that cannot be published"
                    );
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(
                    %error,
                    node_role = ?options.node_role,
                    "failed to inspect non-worker auto-lifecycle delete receipt backlog"
                ),
            }
        }
        let db = crate::db::SharedConnection::new(conn);
        let auto_lifecycle = AutoLifecycleCoordinator::default();
        let network_policy_reconcile_queue = NetworkPolicyReconcileQueue::default();
        let feature_flags = pplx_space_feature_flags::FeatureFlagClient::from_env();
        // Force the lazy known-egress env parse here rather than on the first
        // gateway launch, so a rejected local list is an error in the startup
        // log instead of one line buried in an unrelated sandbox create.
        log_known_egress_configuration();
        let health_lifecycle = crate::watchdog::HealthLifecycleEpochs::default();
        let drain_state = Arc::new(DrainState::default());
        let checkpoint_ctx = Arc::new(crate::snapshot_scheduler::CheckpointContext {
            db: db.clone(),
            auto_lifecycle: auto_lifecycle.clone(),
            health_lifecycle: health_lifecycle.clone(),
            node_manager: options.node_manager.clone(),
        });
        let snapshot_scheduler = Arc::new(
            crate::snapshot_scheduler::SnapshotScheduler::new_with_ownership(
                checkpoint_ctx,
                Arc::clone(&drain_state),
                options.shutdown_token.clone(),
                &options.task_tracker,
            ),
        );
        let watchdog = Arc::new(crate::watchdog::SandboxWatchdog::new(
            db.clone(),
            network_policy_reconcile_queue.clone(),
            snapshot_scheduler.clone(),
            auto_lifecycle.clone(),
            health_lifecycle,
            feature_flags.clone(),
            options.shutdown_token.clone(),
            Arc::clone(&drain_state),
            &options.task_tracker,
            options.node_manager.clone(),
        ));
        if let Some(generation) = expected_route_verification_key_generation {
            watchdog.set_expected_route_verification_key_generation(generation);
        }
        let snapshot_upload_worker = crate::snapshot_upload_jobs::build_worker(
            db.clone(),
            Arc::clone(&drain_state),
            options.node_manager.clone(),
            options.volume_manager_socket_path.clone(),
        );
        let local_artifacts = crate::local_artifacts::LocalArtifacts::new(db.clone());
        let teardown_suspend_driver_concurrency = TEARDOWN_SUSPEND_DRIVER_CONCURRENCY;
        pplx_space_metrics::sandbox::worker::teardown_suspend_driver_capacity(
            teardown_suspend_driver_concurrency,
        );
        let credential_manager_endpoint =
            pplx_space_credmgr_client::CredmgrEndpoint::Uds(options.credential_manager_socket_path);
        let credential_manager_client = Arc::new(tokio::sync::OnceCell::new());
        let gateway_authority = gateway_authority::GatewayAuthority::new(
            credential_manager_endpoint.clone(),
            credential_manager_client.clone(),
        );
        Self {
            db,
            local_node_id: options.local_node_id,
            local_artifacts,
            watchdog,
            snapshot_scheduler,
            auto_lifecycle,
            template_network_policies: TemplateNetworkPolicyCache::new(
                options.shutdown_token.clone(),
                options.task_tracker.clone(),
                Arc::clone(&options.template_resolver),
            ),
            network_policy_reconcile_queue,
            network_policy_reconciler_started: Arc::new(AtomicBool::new(false)),
            cleanup_locks: Arc::new(Mutex::new(BTreeMap::new())),
            cluster_deleted_eviction_quiescence: eviction::cluster_deleted_eviction_quiescence(
                std::env::var("SPACE_SBX_CLUSTER_DELETED_QUIESCENCE_SECS")
                    .ok()
                    .as_deref(),
            ),
            cleanup_sem: Arc::new(tokio::sync::Semaphore::new(DEFAULT_CLEANUP_CONCURRENCY_CAP)),
            cleanup_io: Arc::new(tokio::sync::Semaphore::new(CLEANUP_IO_CONCURRENCY_CAP)),
            cleanup_target_streams: Arc::new(tokio::sync::Semaphore::new(4)),
            artifact_io: Arc::new(tokio::sync::Semaphore::new(
                storage::ARTIFACT_IO_CONCURRENCY,
            )),
            cleanup_executor: Arc::new(cleanup_executor::CleanupExecutor::default()),
            export_io: Arc::new(tokio::sync::Semaphore::new(storage::EXPORT_IO_CONCURRENCY)),
            quarantine_recovery_restore_sem: Arc::new(tokio::sync::Semaphore::new(
                recovery::QUARANTINE_RECOVERY_ORPHAN_RESTORE_PERMITS,
            )),
            activation_admission_limiter: Arc::new(ActivationAdmissionLimiter::from_env()),
            teardown_suspend_drivers: Arc::new(dashmap::DashMap::new()),
            teardown_suspend_driver_sem: Arc::new(tokio::sync::Semaphore::new(
                teardown_suspend_driver_concurrency,
            )),
            fork_copy_slots: pplx_space_common_grpc::QueuedSlots::new(Arc::new(
                tokio::sync::Semaphore::new(FORK_COPY_CONCURRENCY),
            ))
            .with_queue(FORK_COPY_MAX_QUEUED, FORK_COPY_MAX_WAIT),
            fork_copy_waiting: Arc::new(AtomicUsize::new(0)),
            fork_copy_active: Arc::new(AtomicUsize::new(0)),
            active_fork_copy_operations: Arc::new(
                fork_artifact::CopyOperationReservations::default(),
            ),
            shutdown_token: options.shutdown_token,
            task_tracker: options.task_tracker,
            node_role: options.node_role,
            node_managed_by: options.node_managed_by,
            cleanup_metrics: Arc::new(CleanupMetrics::default()),
            config,
            peer_authorizer: sandbox_peer_authorizer(),
            host_control_socket_path: options.host_control_socket_path,
            credential_manager_endpoint,
            credential_manager_client,
            network_credentials_enabled: pplx_space_common_core::env::env_flag(
                std::env::var(NETWORK_CREDENTIALS_ENABLED_ENV)
                    .ok()
                    .as_deref(),
                false,
            ),
            gateway_caller_client: options.node_manager.lazy(),
            gateway_authority,
            snapshot_upload_config: {
                let cfg = crate::snapshot_upload::SnapshotUploadConfig::from_env();
                if cfg.is_none() {
                    tracing::info!(
                        "snapshot S3 upload disabled \
                         (set SPACE_SNAPSHOT_BUCKET and SPACE_SNAPSHOT_REGION to enable)"
                    );
                }
                cfg.map(Arc::new)
            },
            snapshot_upload_worker,
            sandbox_runtime_live: options.sandbox_runtime_live,
            sandbox_rootfs_present: options.sandbox_rootfs_present,
            retained_source_inspector: Arc::new(
                crate::retained_source::NodeManagerSourceInspector(options.node_manager.clone()),
            ),
            node_manager: options.node_manager,
            volume_manager_socket_path: options.volume_manager_socket_path,
            template_resolver: options.template_resolver,
            #[cfg(test)]
            cleanup_run_dir: options.cleanup_run_dir,
            #[cfg(test)]
            create_cache_dir: options.create_cache_dir,
            drain_state,
            feature_flags,
            route_ed25519_verification_bundle: Arc::new(ArcSwapOption::from(
                options.route_ed25519_verification_bundle,
            )),
            route_ed25519_verification_refresh_lock: Arc::new(Mutex::new(())),
            route_stream_epochs: Arc::new(Mutex::new(BTreeMap::new())),
            service_port_ingress_admission: ServicePortIngressAdmission::default(),
        }
    }

    /// Handle to the `local_artifacts` typed-access layer. Use this
    /// for all reads and writes of `local_artifacts` rows; do not
    /// call `crate::db::*_local_artifact*` directly from service
    /// code. See [`crate::local_artifacts`] for the state machine
    /// and lease shapes.
    pub fn local_artifacts(&self) -> &crate::local_artifacts::LocalArtifacts {
        &self.local_artifacts
    }

    /// Resolve `local_artifacts` rows stranded in non-ready states by
    /// a prior process death. Must run before any background task or
    /// RPC path can acquire a fresh local-artifact lease.
    pub fn recover_local_artifact_stuck_states(
        &self,
    ) -> crate::db::Result<crate::local_artifacts::StuckStateRecovery> {
        self.local_artifacts.recover_stuck_states()
    }

    /// Handle to the durable snapshot-upload worker, for the service binary
    /// to call `recover_and_log` at startup and `shutdown` at exit.
    pub fn snapshot_upload_worker(
        &self,
    ) -> pplx_space_common_jobs::BackgroundWorker<crate::snapshot_upload_jobs::SnapshotUploadExecutor>
    {
        self.snapshot_upload_worker.clone()
    }

    pub fn start_snapshot_upload_maintenance(
        &self,
        shutdown: tokio_util::sync::CancellationToken,
        tracker: &tokio_util::task::TaskTracker,
    ) {
        let db = self.db.clone();
        let worker = self.snapshot_upload_worker.clone();
        let drain = Arc::clone(&self.drain_state);
        let critical = self.critical_task_guard("snapshot-upload-maintenance", &shutdown);
        tracker.spawn(async move {
            let _critical = critical;
            crate::snapshot_upload_jobs::run_maintenance(db, worker, drain, shutdown).await;
        });
    }

    pub fn refund_interrupted_snapshot_upload_attempts(
        &self,
    ) -> Result<usize, RefundInterruptedSnapshotUploadAttemptsError> {
        let conn = self
            .db
            .lock()
            .map_err(|_| RefundInterruptedSnapshotUploadAttemptsError::DatabaseMutexPoisoned)?;
        Ok(crate::snapshot_upload_jobs::refund_interrupted_running_attempts(&conn)?)
    }

    /// Spawn the on-node snapshot LRU reaper. No-op when
    /// `SPACE_SBX_GC_ENABLED` is unset. Wired alongside the other
    /// reconcilers in main.rs.
    pub fn start_snapshot_reaper(
        &self,
        shutdown_token: tokio_util::sync::CancellationToken,
        task_tracker: &tokio_util::task::TaskTracker,
    ) {
        let db = self.db.clone();
        let artifacts = self.local_artifacts.clone();
        let active_fork_copy_operations = Arc::clone(&self.active_fork_copy_operations);
        let cfg = crate::snapshot_gc::GcConfig::from_env();
        let ownership = crate::snapshot_gc::GcOwnership {
            coordinator: self.auto_lifecycle.clone(),
            drain: Arc::clone(&self.drain_state),
            shutdown: shutdown_token,
            node_manager: self.node_manager.clone(),
        };
        task_tracker.spawn(async move {
            crate::snapshot_gc::run_loop(
                artifacts,
                db,
                active_fork_copy_operations,
                cfg,
                ownership,
            )
            .await;
        });
    }

    pub fn start_local_artifact_metadata_reaper(
        &self,
        shutdown_token: tokio_util::sync::CancellationToken,
        task_tracker: &tokio_util::task::TaskTracker,
    ) {
        let artifacts = self.local_artifacts.clone();
        let config = LocalArtifactMetadataReaperConfig::production();
        task_tracker.spawn(async move {
            run_local_artifact_metadata_reaper(artifacts, config, shutdown_token).await;
        });
    }

    pub fn start_lifecycle_metrics_sampler(
        &self,
        shutdown_token: tokio_util::sync::CancellationToken,
        task_tracker: &tokio_util::task::TaskTracker,
    ) {
        task_tracker.spawn(crate::lifecycle_metrics::run(
            self.db.clone(),
            shutdown_token,
        ));
    }
}

#[derive(Debug, Clone, Copy)]
struct LocalArtifactMetadataReaperConfig {
    interval: Duration,
    max_records_per_tick: usize,
}

impl LocalArtifactMetadataReaperConfig {
    fn production() -> Self {
        Self {
            interval: Duration::from_secs(300),
            max_records_per_tick: 1_000,
        }
    }
}

async fn run_local_artifact_metadata_reaper(
    artifacts: crate::local_artifacts::LocalArtifacts,
    config: LocalArtifactMetadataReaperConfig,
    shutdown_token: tokio_util::sync::CancellationToken,
) {
    tracing::info!(
        interval_secs = config.interval.as_secs(),
        max_records_per_tick = config.max_records_per_tick,
        "starting local artifact metadata reaper"
    );
    prune_local_artifact_metadata_once(artifacts.clone(), config.max_records_per_tick).await;

    let first_tick = tokio::time::Instant::now() + config.interval;
    let mut interval = tokio::time::interval_at(first_tick, config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => return,
            _ = interval.tick() => {
                prune_local_artifact_metadata_once(
                    artifacts.clone(),
                    config.max_records_per_tick,
                ).await;
            }
        }
    }
}

async fn prune_local_artifact_metadata_once(
    artifacts: crate::local_artifacts::LocalArtifacts,
    max_records_per_tick: usize,
) {
    match tokio::task::spawn_blocking(move || {
        artifacts.prune_missing_orphan_rows(max_records_per_tick)
    })
    .await
    {
        Ok(Ok(pruned)) => {
            if pruned > 0 {
                pplx_space_metrics::sandbox::worker::local_artifact_metadata_pruned(pruned as u64);
                tracing::info!(pruned, "pruned stale local artifact metadata rows");
            }
        }
        Ok(Err(error)) => {
            tracing::warn!(
                %error,
                "failed to prune stale local artifact metadata rows"
            );
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "local artifact metadata reaper task failed"
            );
        }
    }
}

mod activation;
mod activation_support;
pub(crate) use activation_support::NodeIdentitySource;
mod adapter;
mod auth_lookup;
#[cfg(test)]
mod auth_lookup_tests;
pub(crate) mod cache_cleanup;
mod cleanup;
mod cleanup_executor;
mod cleanup_support;
mod cleanup_targets;
mod create;
mod create_support;
mod guest_image;
pub(crate) use create_support::template_lookup_scope;
pub(crate) mod drain;
mod eviction;
pub(crate) mod snapshot_capture;
pub(crate) use eviction::EvictionClaim;
mod grpc_auto_lifecycle_delete_receipts;
mod grpc_core;
mod grpc_lifecycle;
mod grpc_local_backup_inventory;
mod grpc_memory_policy;
mod grpc_net_gateway;
mod grpc_pool_wait_clone;
mod grpc_sandbox_transfer;
mod grpc_snapshot_transfer;
mod grpc_spaced_files;
mod grpc_spaced_process_snapshots;
mod grpc_state_snapshot;
mod host_control;
mod network_runtime;
pub(crate) use network_runtime::{
    log_known_egress_configuration, net_gateway_launch_config, netgtw_launch_args,
};
pub(crate) mod gateway_identity;
mod ingress_protection;
mod reconcile;
mod recovery;
mod recovery_cross_node;
#[cfg(test)]
mod route_identity_tests;
mod spaced_bridge;
mod spaced_bridge_policy;
mod start;
mod start_support;
mod startup_resident;
mod storage;
mod tail_support;
mod template_snapshot_support;
#[cfg(test)]
pub(crate) mod test_options;
#[cfg(test)]
pub(crate) mod tests;
mod validation;

use activation_support::{
    local_space_node_id, merge_authoritative_route_identity, sandbox_peer_authorizer,
};
use cache_cleanup::is_sandbox_running;
use cleanup_support::{
    CLEANUP_IO_CONCURRENCY_CAP, CleanupLockMap, CleanupMetrics, DEFAULT_CLEANUP_CONCURRENCY_CAP,
};
use spaced_bridge_policy::SpacedRpc;
