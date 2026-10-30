use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use time::OffsetDateTime;

use futures_util::{TryStreamExt, pin_mut};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Api, Client, Error as KubeError,
    api::{DeleteParams, PostParams, WatchEvent, WatchParams},
    runtime::reflector::{ObjectRef, Store},
};
use pplx_space_common_core::{
    kubernetes::{
        DEFAULT_SANDBOX_PROXY_PORT, ForkDiskRestorePodContext,
        MAX_SANDBOX_NETWORK_POLICY_ANNOTATION_BYTES, NodeGroupPlacement,
        NodeSelectorLabelsParseError, SANDBOX_NETWORK_POLICY_ANNOTATION,
        SANDBOX_NETWORK_POLICY_AUTHORITY_ANNOTATION, SANDBOX_NETWORK_POLICY_GENERATION_ANNOTATION,
        SandboxPodCommon, SandboxPodConfig, SandboxPodConfigValues, SandboxPodRequest,
        SandboxRuntimePodMetadata, build_sandbox_pod, node_group_label,
    },
    types::{
        NodeRole as SpaceNodeRole, PodName, PodResourceVersion, PodUid, SandboxId, StorageUri,
    },
};
use pplx_space_common_svc::types::k8s::space_k8s_node_id;
use pplx_space_node_api_client::{
    NodeApiClient, WorkerApiClient as RetryingWorkerApiClient,
    WorkerApiInternalClient as RetryingWorkerApiInternalClient,
    proto::worker_api_internal_client::WorkerApiInternalClient,
};
use pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest;
use sqlx::PgPool;
use tokio::{
    net::TcpStream,
    time::{Instant, sleep, timeout, timeout_at},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tonic::{Code, transport::Channel};
use tracing::Instrument;
use uuid::Uuid;

mod budgets;
mod caches;
mod cancel_rate_tracker;
mod pod_reflector;
mod pod_worker_channels;
mod restore_waiters;
mod sandbox_pod_view;
mod store;

use budgets::SubstrateBudgets;
pub use pod_reflector::ReflectorReady;
use sandbox_pod_view::NodeNetworkSlots;
pub use sandbox_pod_view::{
    SandboxContainerState, SandboxContainerStatuses, SandboxContainerTerminated,
    SandboxContainerWaiting, SandboxPodKey, SandboxPodView,
};
// Nameable so out-of-crate reconcilers can hold the cache bundle and consult
// `nodes_ready()`, not just read `caches.nodes`. Without this the type is
// reachable (the field is public) but not writable in a struct or signature,
// which pushes callers into scanning a bare `Store` and losing the
// cold-vs-genuinely-empty distinction the readiness flag exists to make.
pub use caches::K8sCaches;
use cancel_rate_tracker::CancelRateTracker;
use restore_waiters::RestoreWaiters;
use store::{
    ActiveExecutionState, K8sStore, NodeLifecycleState, NodeStateWrite, PlacementClock,
    PreparedWakeSandbox, RestoreClaim, RestoreClaimRevert, SandboxExecutionRow,
    StartedRecoveryClaim, StartedRecoveryCompletion, StartedRecoveryObservation,
    UserRestoreObservation,
};

use crate::{
    controllers::ControllerError, models::Sandbox, node_group_placement::NodeGroupPlacementResolver,
};

use super::{
    ActivationCompletion, ActivationOptions, BuilderClient, CreateExecutionOutcome,
    CreateExecutionRequest, ExecutionActivation, ExecutionActivityKind, ExecutionBinding,
    ExecutionIdentity, ExecutionRoutabilityCheck, NODE_RPC_TIMEOUT, NodeApiTarget,
    NodeInventoryEntry, ObservedPod, ObservedPodIdentity, OwnedNodeApiTarget, PodFenceMismatch,
    PodIdentityFence, RecordedExecution, RetireExecutionOutcome, RetireExecutionRequest,
    RetireFence, RuntimeClient, SubstrateAdapter, SubstrateCapabilities, SubstrateError,
    TeardownProgress, WaitReadyTimeoutPhase, connect_worker, node_api_endpoint,
};

const START_WAIT_FAST_POLL_INTERVAL: Duration = Duration::from_millis(250);
const START_WAIT_SLOW_POLL_INTERVAL: Duration = Duration::from_secs(1);
const START_WAIT_FAST_POLL_WINDOW: Duration = Duration::from_secs(10);
const COMMAND_READY_MAX_POLL_INTERVAL: Duration = Duration::from_secs(10);
const COMMAND_READY_POLL_JITTER_FACTOR: f64 = 0.2;
const START_WAIT_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
/// A resident route is already installed, so its no-op Wake is an
/// observability/lifecycle hint rather than an authorization completion
/// barrier. Keep a hung manager from consuming the command-readiness budget.
const RESIDENT_WAKE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const START_WAIT_COMMAND_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const POD_WORKER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// The first dial to a sandbox pod's kept channel from a Native data-plane
/// request. An in-cluster pod answers a SYN within milliseconds, so one that
/// has not within a second has most likely gone, or lost the SYN; either
/// way, the pod is resolved again before a longer dial, rather than waiting
/// out [`POD_WORKER_CONNECT_TIMEOUT`] on the old address first. A lost SYN
/// costs about what the kernel's own first retransmit, at one second, would.
const POD_WORKER_FIRST_DIAL_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_CREATE_DATA_PLANE_READY_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_CREATE_COMMAND_READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Overall wall-clock budget for the create path's combined readiness wait.
/// `wait_for_pod_proxy_reachable` (≤ `create_data_plane_ready_timeout`) and
/// `poll_until_command_ready` (≤ `create_command_ready_timeout`) run
/// **sequentially**, so their per-phase defaults can sum past the node-RPC
/// channel timeout. This outer budget — held below it via
/// [`crate::controllers::substrate::NODE_RPC_BLOCKING_BUDGET`] — caps the
/// combined wait so a slow create returns a structured, retryable response
/// (sandbox id preserved, pod left running to converge) before the channel
/// timeout fires. It is the authoritative bound regardless of per-phase env
/// overrides.
const CREATE_READY_BUDGET: Duration = crate::controllers::substrate::NODE_RPC_BLOCKING_BUDGET;
/// Margin reserved inside the [`CREATE_READY_BUDGET`]-bounded waits so the
/// inner readiness wait — which knows which phase stalled and records the
/// timeout metrics — fires strictly before the outer budget cancels it with
/// a generic phase-less `Timeout`. Without the margin, an inner deadline at
/// or above the budget (the default `create_data_plane_ready_timeout` of
/// 120s, or the ~600s evacuation command-ready wait) never fires, and every
/// budget-bounded readiness timeout loses its phase attribution. One slow
/// probe round costs ~1.5s (500ms TCP connect + 1s command probe), so 5s
/// covers a full round plus the poll sleep.
const READY_WAIT_PHASE_MARGIN: Duration = Duration::from_secs(5);
const _: () = assert!(
    READY_WAIT_PHASE_MARGIN.as_secs() < CREATE_READY_BUDGET.as_secs(),
    "phase margin must leave readiness-wait room inside the outer budget",
);
const _: () = assert!(
    RESIDENT_WAKE_PROBE_TIMEOUT.as_secs() < CREATE_READY_BUDGET.as_secs(),
    "resident Wake probe must leave time for command readiness",
);
const _: () = assert!(
    PRESERVE_DECISION_BUDGET.as_secs()
        < crate::controllers::substrate::NODE_RPC_TIMEOUT
            .as_secs()
            .saturating_sub(CREATE_READY_BUDGET.as_secs()),
    "preserve decision budget must fit inside the post-budget response margin"
);
/// Bound on the slow-create *preserve decision* (`preservable_create_node`),
/// which runs only after `CREATE_READY_BUDGET` has elapsed — i.e. inside the
/// ~15s margin reserved for flushing the structured response before the
/// node-RPC channel timeout fires. The decision reads the execution row, may
/// fall through to an unbounded apiserver `GET /pods/{name}` on a cache miss,
/// and runs a 500ms proxy TCP probe; left unbounded, a slow GET could itself
/// consume the margin and re-trip the very latency cliff this path exists to
/// avoid. Capping it well below the margin means a slow decision degrades to
/// teardown (the pre-preserve behavior) rather than overrunning the response.
const PRESERVE_DECISION_BUDGET: Duration = Duration::from_secs(5);
// The preserve decision must finish well within the response margin
// (`NODE_RPC_TIMEOUT − CREATE_READY_BUDGET`) so it can never push the create
// past the node-RPC channel timeout. Leave room for the response flush too,
// so require strictly less than the full margin.
const DEFAULT_K8S_BLOCKING_DELETE_TIMEOUT: Duration = Duration::from_secs(10);
/// A materialized restore execution owns downstream work for longer than any
/// API-to-node request can remain active. The same window decides when a
/// missing creating Pod is stale enough for generation replacement. Derived
/// from `common-core`'s `restore_ownership` module (which compile-asserts it
/// outlives the node-RPC timeout) so this window and the placement crate's
/// cannot drift apart.
const RESTORE_EXECUTION_OWNERSHIP_TTL_SECS: i64 =
    pplx_space_common_core::restore_ownership::RESTORE_EXECUTION_OWNERSHIP_TTL.as_secs() as i64;
/// Activation ownership must outlive every bounded API-to-node operation. A
/// crashed owner is reclaimable after this window; a healthy owner never has
/// to heartbeat while it is waiting on the node.
const ACTIVATION_ATTEMPT_LEASE_TTL_SECS: i64 = RESTORE_EXECUTION_OWNERSHIP_TTL_SECS;
/// Bounded timeout for the single direct kube-apiserver `GET /pods/{name}` that
/// runs as the correctness fallback once the reflector wait has used the full
/// blocking-delete budget without confirming deletion (reflector lag / watch
/// failure). One GET at the deadline, never a poll loop. The whole operation
/// can therefore run up to this long past `k8s_blocking_delete_timeout`; that
/// timeout is a soft outcome (a timeout after an accepted delete still
/// finalizes the suspend), so the bounded overrun is acceptable.
const K8S_BLOCKING_DELETE_FALLBACK_GET_TIMEOUT: Duration = Duration::from_secs(2);
const POD_IP_WATCH_TIMEOUT_SECS: u32 = 55;
/// Wall-clock budget for `read_sandbox_pod` to wait on a reflector event for
/// the target pod before falling through to a direct `GET /pods/{name}`.
/// A short wait can avoid an apiserver GET when the reflector is catching up,
/// while bounding the delay before falling back to a direct read.
const REFLECTOR_WAIT_BUDGET: Duration = Duration::from_millis(250);

/// How long a source-pinned restore waits to observe the scheduler's first
/// placement decision before it re-drives cross-node in-request. A required
/// single-node pin hits kube-scheduler's PreFilter NodeNames shortcut, so the
/// bind-or-Unschedulable decision lands in one scheduling cycle (sub-second);
/// this bound only guards against a wedged scheduler or reflector lag, and
/// stays well under the API restore/wake readiness budget so the extra watch
/// can never dominate request latency.
const SCHEDULING_WATCH_BUDGET: Duration = Duration::from_secs(5);

/// Fallback candidates added after the source node in the restore shortlist —
/// 64 nodes total. Anything comfortably under kube-scheduler's 100-node
/// prefilter floor guarantees the full list survives into scoring (the
/// adaptive `percentageOfNodesToScore` sampling never engages), which is what
/// makes the weight-100 preferred source term decisive whenever the source is
/// feasible.
const RESTORE_NODE_SHORTLIST_FALLBACKS: usize = 63;

/// How many times the cross-node re-drive re-reads a fresh UID/resourceVersion
/// and retries its preconditioned delete when a 409 turns out to be a
/// status-only RV bump on the same still-pinned, still-unschedulable pod (not
/// a bind, a replacement, or a deletion). Small and self-limiting: legitimate
/// status writes are rare, so a handful of attempts absorbs realistic churn
/// without letting a pathological writer spin the request.
const REDRIVE_PRECONDITION_RETRY_ATTEMPTS: usize = 3;

/// Pure tail of [`KubernetesSubstrate::restore_node_shortlist`]: source node
/// first, then up to [`RESTORE_NODE_SHORTLIST_FALLBACKS`] candidates in a
/// per-sandbox deterministic shuffle (hash of sandbox id + node name) —
/// different sandboxes spread across different fallback subsets while retries
/// of the same sandbox see a stable list. The source is excluded from the
/// candidate tail so it never appears twice.
fn shortlist_from_candidates(
    sandbox_id: SandboxId,
    source_node: &str,
    candidates: Vec<String>,
) -> Vec<String> {
    let mut candidates: Vec<String> = candidates
        .into_iter()
        .filter(|name| name != source_node)
        .collect();
    candidates.sort_by_cached_key(|name| {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sandbox_id.hash(&mut hasher);
        name.hash(&mut hasher);
        hasher.finish()
    });
    candidates.truncate(RESTORE_NODE_SHORTLIST_FALLBACKS);
    let mut shortlist = Vec::with_capacity(candidates.len() + 1);
    shortlist.push(source_node.to_string());
    shortlist.extend(candidates);
    shortlist
}

/// Share of the eligible fleet that may be labelled
/// `node-services-ready=false` before the signal is treated as unusable and
/// ignored for candidate *filtering*.
///
/// A worker DaemonSet rolls at `maxUnavailable: 10%`, and two node-local
/// DaemonSets rolling over disjoint nodes at once still land well under this,
/// so a genuine rollout never trips it. Half the fleet reading as degraded is
/// far more likely a *signal* failure — the writer wedged with stale `false`
/// labels, a DaemonSet renamed out from under the component list, a watch that
/// never recovered — than half the fleet actually being down.
///
/// The valve exists because filtering *removes* candidates: a fleet-wide stale
/// `false` would shrink the restore shortlist to the source node alone. The
/// soft affinity term needs no equivalent guard — it only re-ranks, so even a
/// fleet-wide penalty leaves every node schedulable, and the operator switch
/// (`node_services_ready_affinity`) is the kill switch for it.
const MAX_NOT_READY_NODE_SERVICES_FRACTION: f64 = 0.5;

/// Whether the `node-services-ready` labels over the eligible node set are
/// trustworthy enough to filter placement candidates on. See
/// [`MAX_NOT_READY_NODE_SERVICES_FRACTION`]; an empty set is never usable
/// (nothing to filter, and the fraction is undefined).
fn node_services_signal_usable(eligible: &[std::sync::Arc<Node>]) -> bool {
    // Ahead of the emission, and it has to stay there. An empty set is unusable
    // but it is *not* a suppression: there is no signal to distrust and no
    // fleet to describe. Booking the fleet-scoped counter here would report a
    // wedged readiness writer for a cluster that merely has no eligible node at
    // this instant — a full-fleet drain, an incident, a one-node kind cluster —
    // and the warn would print `eligible = 0` while asserting the labels "cover
    // too much of the eligible fleet". This guard predates the split into
    // `node_services_signal_trustworthy`, which also returns `false` on empty;
    // that is why it looked redundant, and it is not.
    if eligible.is_empty() {
        return false;
    }
    let usable = node_services_signal_trustworthy(eligible);
    if !usable {
        let not_ready = eligible
            .iter()
            .filter(|node| adapter::node_services_not_ready(node))
            .count();
        pplx_space_metrics::node_service_readiness::placement_signal_suppressed();
        tracing::warn!(
            not_ready,
            eligible = eligible.len(),
            "node-services-ready labels cover too much of the eligible fleet to \
             be a rollout; ignoring the signal for placement"
        );
    }
    usable
}

/// [`node_services_signal_usable`] without the metric or the log.
///
/// The emitting version's counter and warn are both fleet-scoped — the counter
/// is untagged and the message says "fleet" — so a second reading over a
/// *narrowed* population must not book them, or a small candidate set's
/// ordinary DaemonSet roll would trip a monitor whose meaning is fleet-wide.
/// The narrowed reading is a placement decision, not a fleet observation.
fn node_services_signal_trustworthy(eligible: &[std::sync::Arc<Node>]) -> bool {
    if eligible.is_empty() {
        return false;
    }
    let not_ready = eligible
        .iter()
        .filter(|node| adapter::node_services_not_ready(node))
        .count();
    (not_ready as f64) <= MAX_NOT_READY_NODE_SERVICES_FRACTION * eligible.len() as f64
}

fn restore_snapshot_id_from_storage_uri(storage_uri: Option<&str>) -> Option<String> {
    let storage_uri = storage_uri?;
    let parsed = StorageUri::parse(storage_uri).ok()?;
    parsed
        .key_prefix
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn sandbox_template_event_details<'a>(
    sandbox: &'a Sandbox,
    restore_storage_uri: Option<&'a str>,
    details: pplx_space_events::SandboxEventDetails,
) -> pplx_space_events::SandboxEventDetails {
    let mut details = details.template_release_tags(sandbox.release_tags.join(","));
    if let Some(template_id) = sandbox.template_id.as_deref() {
        details = details.template_id(template_id);
    }
    if let Some(content_hash) = sandbox.content_hash.as_deref() {
        details = details.template_content_hash(content_hash);
    }
    if restore_storage_uri.is_some() {
        details = details.bool_field(
            pplx_space_events::SandboxEventBoolField::RestoreStorageUriPresent,
            true,
        );
    }
    if let Some(snapshot_id) = restore_snapshot_id_from_storage_uri(restore_storage_uri) {
        details = details.restored_snapshot_id(snapshot_id);
    }
    details
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct PodIp(IpAddr);

impl PodIp {
    fn parse(value: &str) -> Result<Self, SubstrateError> {
        value
            .parse()
            .map(Self)
            .map_err(|err| SubstrateError::Failed {
                message: format!("invalid pod IP {value}: {err}"),
            })
    }

    fn socket_addr(self, port: u16) -> SocketAddr {
        SocketAddr::new(self.0, port)
    }
}

/// A sandbox pod's address and the pod instance it belongs to.
#[derive(Clone, Debug)]
struct ResolvedPodInstance {
    ip: PodIp,
    pod_uid: Option<PodUid>,
    generation: i64,
    /// The sandbox row read with the execution was `restoring`. A path that
    /// serves a user request must not dial such a pod (see
    /// `KubernetesSubstrate::join_restore_before_dial`).
    sandbox_restoring: bool,
}

impl ResolvedPodInstance {
    /// Whether `other` is the same pod instance at the same address.
    fn same_instance(&self, other: &Self) -> bool {
        self.ip == other.ip && self.pod_uid == other.pod_uid && self.generation == other.generation
    }
}

impl std::fmt::Display for PodIp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

fn duration_to_proto(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(duration.subsec_nanos()).expect("subsecond nanos fit i32"),
    }
}

#[derive(Clone)]
pub struct KubernetesState {
    pub client: Client,
    pub sandbox_namespace: String,
    pub sandbox_proxy_port: u16,
    /// gRPC port the node-api DaemonSet listens on. Distinct from
    /// `sandbox_proxy_port` (which targets the sandbox proxy inside Firecracker
    /// guests); the node-api chart's `port` and any per-role override could in
    /// principle differ from the sandbox proxy port.
    pub node_api_port: u16,
    pub sandbox_pod_config: SandboxPodConfigValues,
    /// Wall-clock budget for the post-readiness `poll_until_command_ready`
    /// loop on the create path. Configurable via
    /// `SPACE_CREATE_COMMAND_READY_TIMEOUT_SECS` so deployments and the
    /// native-recovery smoke can shrink the budget without a rebuild. Other
    /// lifecycle paths (start, wake, access) carry their own per-call timeouts.
    pub create_command_ready_timeout: Duration,
    /// Wall-clock budget for the create path to wait for the sandbox pod's
    /// dataplane proxy to become reachable. This includes kubelet runtime
    /// sandbox setup, where an uncached template may be pulled by
    /// template-manager through the shim path.
    pub create_data_plane_ready_timeout: Duration,
    /// Wall-clock budget for a Kubernetes DELETE followed by observation that
    /// the object is gone from the apiserver. Used on suspend so the control
    /// plane does not publish `suspended` while kubelet/containerd teardown for
    /// the old sandbox pod can still run later.
    pub k8s_blocking_delete_timeout: Duration,
    /// Whether a failed restore claim's rollback retires its execution
    /// (`SPACE_ABANDONED_RESTORE_RETIREMENT_ENABLED`, default off).
    pub abandoned_restore_retirement: super::AbandonedRestoreRetirement,
    /// Whether resume and start decide a runtime upgrade
    /// (`SPACE_RUNTIME_UPGRADE_AT_ACTIVATION_ENABLED`, default off).
    pub runtime_upgrade_at_activation: super::runtime_upgrade::RuntimeUpgradeAtActivation,
    pub caches: Arc<K8sCaches>,
}

/// Failure initializing [`KubernetesState`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum KubernetesStateInitError {
    #[error(transparent)]
    Kube(#[from] KubeError),
    #[error(transparent)]
    NodeSelectorLabels(#[from] NodeSelectorLabelsParseError),
}

impl KubernetesState {
    pub async fn try_from_env() -> Result<Option<Self>, KubernetesStateInitError> {
        if !pplx_space_common_core::env::env_flag_var("SPACE_K8S_CONTROL_PLANE", false) {
            return Ok(None);
        }

        let client = pplx_space_common_svc::kube_client::traced_kube_client_from_env().await?;

        let sandbox_namespace = std::env::var("SPACE_SANDBOX_NAMESPACE")
            .or_else(|_| std::env::var("POD_NAMESPACE"))
            .unwrap_or_else(|_| "default".to_string());
        let node_api_namespace = std::env::var("SPACE_NODE_API_NAMESPACE")
            .or_else(|_| std::env::var("POD_NAMESPACE"))
            .unwrap_or_else(|_| sandbox_namespace.clone());

        let caches = Arc::new(K8sCaches::spawn(client.clone(), &node_api_namespace));
        // Wait briefly for first list, but don't fail startup on a slow/unhealthy
        // API server — the stores will backfill once the watch reconnects.
        if let Err(err) = caches.wait_until_ready().await {
            tracing::warn!(error = %err, "k8s caches did not reach ready before timeout; continuing with cold cache");
        }

        let create_command_ready_timeout = parse_create_command_ready_timeout(
            std::env::var("SPACE_CREATE_COMMAND_READY_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        );
        let create_data_plane_ready_timeout = parse_create_data_plane_ready_timeout(
            std::env::var("SPACE_CREATE_DATA_PLANE_READY_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        );

        Ok(Some(Self {
            client,
            sandbox_namespace,
            caches,
            create_command_ready_timeout,
            create_data_plane_ready_timeout,
            k8s_blocking_delete_timeout: DEFAULT_K8S_BLOCKING_DELETE_TIMEOUT,
            abandoned_restore_retirement: super::AbandonedRestoreRetirement::from_env(),
            runtime_upgrade_at_activation:
                super::runtime_upgrade::RuntimeUpgradeAtActivation::from_env(),
            sandbox_proxy_port: DEFAULT_SANDBOX_PROXY_PORT,
            node_api_port: DEFAULT_SANDBOX_PROXY_PORT,
            sandbox_pod_config: SandboxPodConfigValues::from_env()?,
        }))
    }
}

fn sandbox_pod_config(k8s: &KubernetesState) -> SandboxPodConfig<'_> {
    k8s.sandbox_pod_config.for_namespace(&k8s.sandbox_namespace)
}

fn sandbox_template_name(sandbox: &Sandbox) -> &str {
    sandbox.template_id.as_deref().unwrap_or("seed")
}

fn sandbox_vcpu_option(sandbox: &Sandbox) -> Option<u32> {
    sandbox.vcpu.and_then(|value| u32::try_from(value).ok())
}

fn sandbox_memory_mib_option(sandbox: &Sandbox) -> Option<u32> {
    sandbox
        .memory_mib
        .and_then(|value| u32::try_from(value).ok())
}

fn sandbox_disk_mib_option(sandbox: &Sandbox) -> Option<u32> {
    sandbox.disk_mib.and_then(|value| u32::try_from(value).ok())
}

/// The generation a cold pod seeds sandbox-manager's network policy with.
///
/// The pod annotation always carries the effective policy, so the sandbox
/// enforces it from its first boot. An override is seeded at the sandbox's own
/// generation, the lineage node-ctl reconciles it on. An inherited policy is
/// seeded at 0, which sandbox-manager keeps as a cluster placeholder: the first
/// reconcile carries the sandbox's generation, never below 1, and replaces it
/// with template inheritance. Seeding the template's generation, an unrelated
/// counter, made that reconcile collide with the placeholder at the same
/// generation, or be skipped below it, so the sandbox followed neither its
/// template nor a later override.
fn cold_pod_network_policy_generation(has_override: bool, sandbox_generation: u64) -> u64 {
    if has_override { sandbox_generation } else { 0 }
}

fn execution_generation_to_u64(generation: i64) -> Result<u64, SubstrateError> {
    u64::try_from(generation).map_err(|err| SubstrateError::Failed {
        message: format!("invalid Kubernetes execution generation {generation}: {err}"),
    })
}

fn duration_millis_to_u64(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn ensure_network_policy_annotation_size(network_policy_json: &str) -> Result<(), SubstrateError> {
    let bytes = network_policy_json.len();
    if bytes <= MAX_SANDBOX_NETWORK_POLICY_ANNOTATION_BYTES {
        return Ok(());
    }

    Err(SubstrateError::Failed {
        message: format!(
            "network policy annotation is {bytes} bytes, exceeds {} byte limit",
            MAX_SANDBOX_NETWORK_POLICY_ANNOTATION_BYTES
        ),
    })
}

fn normalize_lifecycle(lifecycle: &str) -> String {
    lifecycle.trim().to_ascii_lowercase()
}

fn is_dormant_lifecycle(lifecycle: &str) -> bool {
    matches!(
        normalize_lifecycle(lifecycle).as_str(),
        "created" | "stopped" | "paused" | "suspended"
    )
}

fn is_suspended_lifecycle(lifecycle: &str) -> bool {
    normalize_lifecycle(lifecycle) == "suspended"
}

fn is_restoring_lifecycle(lifecycle: &str) -> bool {
    normalize_lifecycle(lifecycle) == "restoring"
}

fn is_recoverable_live_lifecycle(lifecycle: &str) -> bool {
    matches!(
        normalize_lifecycle(lifecycle).as_str(),
        "creating"
            | "created"
            | "starting"
            | "started"
            | "pausing"
            | "paused"
            | "resuming"
            | "checkpointing"
            | "suspending"
            | "suspended"
            | "restoring"
            | "stopping"
            | "stopped"
    )
}

fn is_wake_recovery_lifecycle(lifecycle: &str) -> bool {
    matches!(
        normalize_lifecycle(lifecycle).as_str(),
        "creating"
            | "created"
            | "starting"
            | "started"
            | "pausing"
            | "paused"
            | "resuming"
            | "checkpointing"
            | "suspending"
            | "suspended"
            | "restoring"
    )
}

async fn create_sandbox_pod(
    k8s: &KubernetesState,
    mut request: SandboxPodRequest<'_>,
) -> Result<SandboxPodView, KubeError> {
    let common = match &mut request {
        SandboxPodRequest::OnDemand { common, .. }
        | SandboxPodRequest::Restore { common, .. }
        | SandboxPodRequest::Pooled { common, .. } => common,
    };
    let span = tracing::info_span!(
        "substrate.k8s.materialize_pod",
        otel.kind = "producer",
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
        sandbox_id = %common.sandbox_id,
        pod_name = common.pod_name,
        namespace = %k8s.sandbox_namespace,
    );
    // Durable operation dispatch and detached wake tasks may have no request
    // span. The Pod carries the context of the operation that materializes it,
    // captured before rendering so the containerd shim can resume this trace.
    common.traceparent = pplx_space_observability::trace_propagation::traceparent_for_span(&span);
    let pods: Api<Pod> = Api::namespaced(k8s.client.clone(), &k8s.sandbox_namespace);
    let result = pods
        .create(
            &PostParams::default(),
            &build_sandbox_pod(&sandbox_pod_config(k8s), request),
        )
        .instrument(span.clone())
        .await;
    if let Err(error) = &result {
        span.record("otel.status_code", "ERROR");
        span.record("otel.status_description", tracing::field::display(error));
    }
    // Callers read the create response through the same view the cache holds.
    result.map(SandboxPodView::from)
}

async fn delete_sandbox_pod(k8s: &KubernetesState, pod_name: &str) -> Result<(), KubeError> {
    let pods: Api<Pod> = Api::namespaced(k8s.client.clone(), &k8s.sandbox_namespace);
    let _ = pods.delete(pod_name, &DeleteParams::default()).await?;
    Ok(())
}

/// Outcome of a preconditioned pod delete. Mirrors the mapping the blocking
/// delete already uses: 404 is benign (someone else removed it), 409 means the
/// precondition was rejected and the caller must not proceed as if it had
/// deleted the pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FencedPodDeleteOutcome {
    Deleted,
    NotFound,
    PreconditionFailed,
}

/// Classify a `DELETE` error from a preconditioned pod delete. Kept separate
/// from the call so the mapping is unit-testable without an apiserver.
fn classify_fenced_pod_delete_error(err: &KubeError) -> Option<FencedPodDeleteOutcome> {
    if is_not_found_error(err) {
        return Some(FencedPodDeleteOutcome::NotFound);
    }
    matches!(err, KubeError::Api(response) if response.code == 409)
        .then_some(FencedPodDeleteOutcome::PreconditionFailed)
}

/// Delete the fenced pod under its identity preconditions, so the removal is
/// atomic with respect to the identity check: the apiserver, not this process,
/// decides whether the object is still the one the caller observed.
///
/// Takes the UID as its own argument rather than reading it off the fence,
/// because a fence without one must never reach here — such a fence holds only
/// when the pod is *absent*, and between that check and this call the pod can
/// materialize. An unconditional delete would then remove the live pod the
/// fence explicitly declined to identify.
async fn delete_sandbox_pod_fenced(
    k8s: &KubernetesState,
    pod_name: &PodName,
    pod_uid: &PodUid,
    resource_version: Option<&PodResourceVersion>,
) -> Result<FencedPodDeleteOutcome, KubeError> {
    let pods: Api<Pod> = Api::namespaced(k8s.client.clone(), &k8s.sandbox_namespace);
    let params = pod_delete_params(pod_uid, resource_version);
    match pods.delete(pod_name.as_str(), &params).await {
        Ok(_) => Ok(FencedPodDeleteOutcome::Deleted),
        Err(err) => match classify_fenced_pod_delete_error(&err) {
            Some(outcome) => Ok(outcome),
            None => Err(err),
        },
    }
}

/// `DeleteParams` preconditioned on a pod UID, and on a resourceVersion when
/// the caller has one. The UID is not optional: a delete with no identity
/// precondition is an unconditional delete, and the one place that would be
/// tempted to issue one — a name-only fence — must not delete at all.
fn pod_delete_params(
    pod_uid: &PodUid,
    resource_version: Option<&PodResourceVersion>,
) -> DeleteParams {
    DeleteParams {
        preconditions: Some(kube::api::Preconditions {
            uid: Some(pod_uid.to_string()),
            resource_version: resource_version.map(PodResourceVersion::to_string),
        }),
        ..Default::default()
    }
}

async fn delete_sandbox_pod_if_uid(
    k8s: &KubernetesState,
    pod_name: &str,
    pod_uid: &PodUid,
) -> Result<(), KubeError> {
    let pods: Api<Pod> = Api::namespaced(k8s.client.clone(), &k8s.sandbox_namespace);
    let params = pod_delete_params(pod_uid, None);
    let _ = pods.delete(pod_name, &params).await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum K8sBlockingDeleteOutcome {
    Deleted,
    NotFound,
    TimedOut,
    ObserveError,
    /// A UID-preconditioned delete was rejected because the live pod's UID no
    /// longer matches — the inspected pod was replaced (same name, new pod)
    /// between the caller's read and the delete. Never returned by the
    /// precondition-less wrappers.
    PreconditionFailed,
}

impl K8sBlockingDeleteOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deleted => "deleted",
            Self::NotFound => "not_found",
            Self::TimedOut => "timeout",
            Self::ObserveError => "observe_error",
            Self::PreconditionFailed => "precondition_failed",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum K8sBlockingDeleteError {
    #[error("timed out waiting {timeout_secs}s for Kubernetes pod {pod_name} delete request")]
    DeleteTimeout { pod_name: String, timeout_secs: u64 },
    #[error("failed to delete Kubernetes pod {pod_name}: {source}")]
    Delete {
        pod_name: String,
        #[source]
        source: Box<KubeError>,
    },
}

impl K8sBlockingDeleteError {
    fn result_tag(&self) -> &'static str {
        match self {
            Self::DeleteTimeout { .. } => "delete_timeout",
            Self::Delete { .. } => "delete_error",
        }
    }
}

/// Direct kube-apiserver `GET /pods/{name}` fallback. Used only on rare paths
/// (e.g. the post-409 conflict recovery in `ensure_pod_exists`) where the
/// reflector cache has not yet seen a freshly-created pod. The hot read path
/// goes through [`KubernetesSubstrate::lookup_sandbox_pod`]. Returns the same
/// [`SandboxPodView`] the cache holds, so cached and uncached callers read the
/// same fields.
async fn fetch_sandbox_pod_uncached(
    k8s: &KubernetesState,
    pod_name: &str,
) -> Result<Option<Arc<SandboxPodView>>, KubeError> {
    let pods: Api<Pod> = Api::namespaced(k8s.client.clone(), &k8s.sandbox_namespace);
    Ok(pods
        .get_opt(pod_name)
        .await?
        .map(|pod| Arc::new(SandboxPodView::from(pod))))
}

fn is_not_found_error(err: &KubeError) -> bool {
    matches!(err, kube::Error::Api(resp) if resp.code == 404)
}

fn parse_positive_seconds_timeout(value: Option<&str>, default: Duration) -> Duration {
    value
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(default)
}

/// Parse `SPACE_CREATE_COMMAND_READY_TIMEOUT_SECS` env-var content into a
/// `Duration`. Falls back to `DEFAULT_CREATE_COMMAND_READY_TIMEOUT` when
/// the value is missing, empty, non-numeric, or zero — a zero deadline would
/// make every create attempt time out instantly, which is never what an
/// operator means and is more likely a misconfiguration than an opt-in.
fn parse_create_command_ready_timeout(value: Option<&str>) -> Duration {
    parse_positive_seconds_timeout(value, DEFAULT_CREATE_COMMAND_READY_TIMEOUT)
}

/// Parse `SPACE_CREATE_DATA_PLANE_READY_TIMEOUT_SECS` for the pre-command
/// create wait. Defaults to 120s so slow-but-healthy uncached template pulls
/// get a larger window before API create gives up.
fn parse_create_data_plane_ready_timeout(value: Option<&str>) -> Duration {
    parse_positive_seconds_timeout(value, DEFAULT_CREATE_DATA_PLANE_READY_TIMEOUT)
}

#[derive(Clone)]
pub struct KubernetesSubstrate {
    pub k8s: KubernetesState,
    store: K8sStore,
    /// Reflector-backed cache of sandbox pods. Populated by a background
    /// watcher filtered on `space.pplx.net/managed-by=space-api`. Reads are
    /// synchronous and do not hit the kube apiserver, eliminating the
    /// per-request `GET /pods/{name}` traffic that previously saturated APF.
    /// Holds the slim [`SandboxPodView`], not the full `Pod`.
    pod_store: Store<SandboxPodView>,
    /// Per-node active network-slot totals, written from the same reflector
    /// events as `pod_store`. Read by the create-path capacity preflight.
    pod_network_slots: NodeNetworkSlots,
    /// Per-pod-name event signaller fed by the same reflector watch. Lets
    /// `read_sandbox_pod` wait briefly for the next reflector update before
    /// falling through to a direct `GET` during the brief window between pod
    /// creation/scheduling and the reflector observing the bind event.
    pod_events: pod_reflector::PodEvents,
    /// Flipped to `true` once the reflector's initial `LIST` populates the
    /// store. Exposed via `Substrate::readiness()` so the HTTP `/readyz`
    /// probe can keep a freshly-spawned replica out of the Service endpoint
    /// rotation until its cache is warm.
    pod_reflector_ready: pod_reflector::ReflectorReady,
    /// Cascade-amplifier backpressure: bounds the rate at which we accept
    /// new `create_execution`s while cancellations are spiking, and lets
    /// `CancelGuard` defer cleanup when node-mgr is the cause. See
    /// `cancel_rate_tracker` module docs for the rationale.
    cancel_tracker: Arc<CancelRateTracker>,
    /// Per-process batched Postgres observer and per-sandbox fan-out for
    /// restore followers. This keeps request futures off the database polling
    /// path and bounds the number of sandbox channels retained by one pod.
    restore_waiters: RestoreWaiters,
    /// Pod-local coalescer for user wakes: at most one live wake run per
    /// sandbox per process. Concurrent callers join the run in memory
    /// instead of each executing the restore-lease claim CAS, whose
    /// `FOR UPDATE` row-lock queue holds one pooled Postgres connection per
    /// contender — the mechanism that let a per-sandbox connect stampede
    /// expand every replica's pool to its cap. Cross-pod arbitration stays
    /// with the DB lease; this only bounds contenders to one per pod.
    wake_coalescer: WakeCoalescer,
    /// Node-api channels kept per sandbox pod instance for runtime RPCs.
    pod_worker_channels: Arc<pod_worker_channels::PodWorkerChannels>,
    /// Resolves each pod build's node-group candidate set. Gated by
    /// `sandbox_pod_config.node_group_placement`
    /// (`SPACE_SANDBOX_NODE_GROUP_PLACEMENT`, default off), in which case it
    /// renders no constraints and issues no query.
    node_group_placement: NodeGroupPlacementResolver,
    /// Poll pacing for the two follower joins (started recovery and the user
    /// restore lease). Carried here rather than read from the constants
    /// directly so a join's re-check interval is settable in tests;
    /// production takes the default, which is those constants.
    recovery_join_backoff: execution::RecoveryJoinBackoffPolicy,
    /// The runtime environment policy the resume- and start-time upgrade
    /// decision reads, shared by clones.
    runtime_policy: super::runtime_upgrade::RuntimePolicyCache,
    /// Readiness, preserve-decision, suspend and connect budgets. Production
    /// takes the default, which is the constants each one names.
    budgets: SubstrateBudgets,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

/// Shared across substrate clones so every request on a pod sees one gate.
type WakeCoalescer = Arc<
    pplx_space_common_work_coalescer::WorkCoalescer<
        SandboxId,
        super::ExecutionActivation,
        SubstrateError,
    >,
>;

impl KubernetesSubstrate {
    pub async fn try_from_env(
        pool: &PgPool,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Result<Option<Self>, SubstrateError> {
        match KubernetesState::try_from_env().await {
            Ok(Some(k8s)) => {
                let (pod_store, pod_network_slots, pod_events, pod_reflector_ready) =
                    pod_reflector::start(k8s.client.clone(), k8s.sandbox_namespace.clone());
                let store = K8sStore::new(pool.clone(), k8s.sandbox_namespace.clone());
                // Read before `k8s` is moved into the struct below.
                let node_group_placement = NodeGroupPlacementResolver::new(
                    pool.clone(),
                    k8s.sandbox_pod_config.node_group_placement,
                );
                Ok(Some(Self {
                    restore_waiters: RestoreWaiters::new(
                        store.clone(),
                        shutdown_token.clone(),
                        task_tracker.clone(),
                    ),
                    pod_worker_channels: Arc::default(),
                    wake_coalescer: Arc::new(
                        pplx_space_common_work_coalescer::WorkCoalescer::with_tracker(
                            task_tracker.clone(),
                        ),
                    ),
                    store,
                    pod_store,
                    pod_network_slots,
                    pod_events,
                    pod_reflector_ready,
                    k8s,
                    node_group_placement,
                    cancel_tracker: CancelRateTracker::with_default_budget(),
                    recovery_join_backoff: execution::RecoveryJoinBackoffPolicy::default(),
                    runtime_policy: super::runtime_upgrade::RuntimePolicyCache::default(),
                    budgets: SubstrateBudgets::default(),
                    shutdown_token,
                    task_tracker,
                }))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(SubstrateError::Failed {
                message: format!("substrate initialization failed: {err}"),
            }),
        }
    }

    /// Reconcile kube Nodes carrying the sandbox selector into the SPACE
    /// `nodes` / `node_k8s` tables. Selected nodes are upserted; stale ready
    /// rows absent from the full watcher-backed Node membership are demoted
    /// within the supplied grace and per-tick bound.
    pub async fn sync_labeled_nodes_to_db(
        &self,
        missing_grace: Duration,
        missing_max_per_tick: u32,
    ) -> Result<Vec<String>, node_sync::SyncNodeInventoryError> {
        node_sync::sync_labeled_nodes_to_db(self, missing_grace, missing_max_per_tick).await
    }

    pub fn readiness(&self) -> pod_reflector::ReflectorReady {
        self.pod_reflector_ready.clone()
    }

    /// Shorten the follower-join poll pacing for a test that needs a join to
    /// notice a published outcome promptly. Production never calls this and
    /// keeps [`Default`](execution::RecoveryJoinBackoffPolicy::default).
    #[cfg(test)]
    pub(crate) fn with_recovery_join_backoff(mut self, initial: Duration, max: Duration) -> Self {
        self.recovery_join_backoff =
            execution::RecoveryJoinBackoffPolicy::with_bounds(initial, max);
        self
    }

    pub fn sandbox_pod_store(&self) -> Store<SandboxPodView> {
        self.pod_store.clone()
    }

    /// Count sandbox-start-capable worker nodes from the **live node
    /// reflector**, not SPACE's Postgres inventory. kube-scheduler places
    /// sandbox pods off the live cache, and `node_k8s`/`node_status_snapshots`
    /// lag it on fresh startup / delayed heartbeats — so a DB count would turn
    /// that lag into a false `empty_cluster` reject.
    ///
    /// A cold cache (node reflector's initial LIST not yet complete) reports
    /// `None` so admission fails open — an empty store there means "capacity
    /// unknown", not "zero nodes". A warm cache (LIST done) with zero surviving
    /// candidates (all NotReady / cordoned / placement-blocked / outside the
    /// sandbox selector — taints are the scheduler's job, not counted here) is
    /// a confident `Some(0)` the limiter rejects. Gating on reflector readiness
    /// rather than `nodes.is_empty()` is what keeps a total node outage from
    /// silently failing open forever.
    pub fn eligible_start_node_count(&self) -> Option<u64> {
        if !self.k8s.caches.nodes_ready() {
            return None;
        }
        let config = &self.k8s.sandbox_pod_config;
        let selector = config.base_node_selector();
        // `build_sandbox_pod` only stamps the `ssd-placement-blocked` NotIn
        // affinity when the SSD-headroom-tier bias is enabled; with it off those
        // nodes stay schedulable, so the candidate filter must follow the same
        // switch or admission would report a false `empty_cluster`.
        let exclude_placement_blocked = config.ssd_headroom_tier_affinity;
        let count = self
            .k8s
            .caches
            .nodes
            .state()
            .iter()
            .filter(|node| adapter::node_schedulable(node))
            .filter(|node| selector.matches(node))
            .filter(|node| !exclude_placement_blocked || adapter::node_not_placement_blocked(node))
            .count();
        Some(count as u64)
    }

    /// Source-aware restore shortlist: the source node first, then up to
    /// [`RESTORE_NODE_SHORTLIST_FALLBACKS`] schedulable fallback candidates
    /// (Ready, matching the sandbox node selector, not placement-blocked —
    /// the same filter chain as [`Self::eligible_start_node_count`], plus
    /// nodes whose node-local service pods are not `Ready` when that bias is
    /// enabled). A shortlisted restore suppresses the soft scoring terms so
    /// the source preference stays decisive, which is why service readiness
    /// applies here as a candidate filter rather than as a weight. The source
    /// itself is always kept — it holds the local suspend bytes, and its own
    /// `Unschedulable` outcome is what the in-request cross-node re-drive
    /// exists for. The
    /// whole list stays under kube-scheduler's 100-node prefilter floor, so
    /// every listed node is *scored* and the weight-100 preferred source term
    /// decides placement whenever the source is feasible; a busy source lands
    /// the pod on a fallback in the same scheduling pass instead of going
    /// `Unschedulable`.
    ///
    /// Fallback order is a per-sandbox deterministic shuffle (hash of sandbox
    /// id + node name): different sandboxes spread across different fallback
    /// subsets, while retries of the same sandbox see a stable list. A cold
    /// node cache returns just the source — the bare-pin mode, whose
    /// `Unschedulable` outcome the in-request watch already handles.
    pub(super) fn restore_node_shortlist(
        &self,
        sandbox_id: SandboxId,
        source_node: &str,
        node_group_placement: Option<&NodeGroupPlacement>,
    ) -> Vec<String> {
        if !self.k8s.caches.nodes_ready() {
            return vec![source_node.to_string()];
        }
        let config = &self.k8s.sandbox_pod_config;
        let selector = config.base_node_selector();
        let exclude_placement_blocked = config.ssd_headroom_tier_affinity;
        let eligible: Vec<std::sync::Arc<Node>> = self
            .k8s
            .caches
            .nodes
            .state()
            .into_iter()
            .filter(|node| adapter::node_schedulable(node))
            .filter(|node| selector.matches(node))
            .filter(|node| !exclude_placement_blocked || adapter::node_not_placement_blocked(node))
            .collect();
        // Node-group membership is a narrowing of the same eligible set as
        // every filter above it, so it belongs in this chain rather than only
        // in the rendered pod. The pod builder repeats the required node-group
        // term into *every* ORed `metadata.name` term, so an out-of-set node in
        // this list renders as a term no node can satisfy: the slot is consumed
        // and can never be chosen, and a busy source whose fallbacks are all
        // out-of-set would go `Unschedulable` instead of landing on one — the
        // cross-node drift this shortlist exists to prevent, with nothing in
        // the metrics telling it apart from a genuinely full fleet.
        //
        // A node with no node-group label is excluded for the same reason an
        // `In (…)` term excludes it. `None` (gate off, or no generic group)
        // narrows nothing, so the pre-gate shortlist is unchanged.
        //
        // **The source node is exempt from this**, because
        // `shortlist_from_candidates` prepends it unconditionally below. A
        // portable restore whose source has left the set therefore still
        // carries it, and relies on an in-set fallback being present; if every
        // fallback is filtered out the shortlist degenerates to the bare source
        // pin and the restore goes `Unschedulable`. The pre-bound (local-only)
        // case is handled earlier and explicitly, by
        // `check_local_restore_source`.
        // Borrowed, not cloned, when there is no placement — which is every
        // cluster while the gate is off. The filter is a no-op there by
        // construction, so building a second `Vec<Arc<Node>>` of the whole
        // fleet (plus the refcount traffic, plus a second readiness pass over
        // it below) bought nothing on the path every restore takes today.
        let narrowed: Option<Vec<std::sync::Arc<Node>>> = node_group_placement.map(|placement| {
            eligible
                .iter()
                .filter(|node| {
                    node_group_label(node).is_some_and(|group| placement.admits_group(group))
                })
                .cloned()
                .collect()
        });
        let in_set: &[std::sync::Arc<Node>] = narrowed.as_deref().unwrap_or(&eligible);
        // Booked when the narrowing removed every fallback, which is the one
        // outcome the comment above says this chain exists to prevent and the
        // only narrowing decision in this feature that would otherwise be
        // silent. Without it, "the candidate set excluded every fallback" and
        // "the fleet is genuinely full" produce the same bare source pin with
        // nothing to tell them apart — and the second is normal while the first
        // is a provisioning gap.
        if node_group_placement.is_some() && in_set.is_empty() && !eligible.is_empty() {
            pplx_space_metrics::node_group_placement::restore_shortlist_emptied();
            tracing::warn!(
                sandbox_id = %sandbox_id,
                eligible = eligible.len(),
                "node-group narrowing removed every restore fallback; the shortlist is the \
                 bare source pin, so a busy source cannot be re-placed"
            );
        }
        // The readiness valve is asked about **both** populations, and the
        // filter runs only if the signal looks trustworthy in each. That is the
        // conservative conjunction, not a way to have it both ways, so be
        // precise about what it buys and what it gives up.
        //
        // What it closes: a fleet-wide-only reading lets a wedged writer — or a
        // DaemonSet rolling just one group — leave every node of a small
        // candidate set labeled not-ready while the other hundreds read ready.
        // The fleet fraction stays under the threshold, the filter runs, and it
        // removes *every* in-set candidate, collapsing the shortlist to the
        // bare source pin. That is exactly the outcome
        // `MAX_NOT_READY_NODE_SERVICES_FRACTION` exists to prevent, reproduced
        // at candidate-set scale.
        //
        // What it gives up: the threshold was calibrated against the fleet (a
        // worker DaemonSet rolls at a small `maxUnavailable` share of it), and
        // requiring the narrowed reading too applies that fleet number to a
        // possibly four-node group — so on a small group an ordinary rollout
        // discards a signal that is telling the truth, and not-ready nodes stay
        // in the shortlist. That is the cheaper error of the two: keeping a
        // not-ready node costs a slower restore, while dropping every candidate
        // costs the restore. The conjunction picks the cheaper error
        // deliberately; it does not preserve the calibration.
        //
        // `&&` short-circuits, so at most one fleet-scoped suppression is
        // booked per call. With no placement the two populations are identical
        // and the second reading cannot change the answer — though it still
        // runs: a second pass over the `Arc<Node>` handles, one label lookup
        // each. Cheap enough not to special-case, but not free.
        let exclude_not_ready_services = config.node_services_ready_affinity
            && node_services_signal_usable(&eligible)
            && node_services_signal_trustworthy(in_set);
        let candidates: Vec<String> = in_set
            .iter()
            .filter(|node| !exclude_not_ready_services || !adapter::node_services_not_ready(node))
            .filter_map(|node| node.metadata.name.clone())
            .collect();
        shortlist_from_candidates(sandbox_id, source_node, candidates)
    }

    /// Kubernetes Nodes in the sandbox placement set — those carrying every
    /// label of the sandbox pod `nodeSelector` — from the watcher-backed
    /// reflector. `None` while the node cache is cold, so callers can tell
    /// "membership unknown" from "no worker nodes".
    ///
    /// Deliberately **not** filtered on Ready/cordon/placement-block, unlike
    /// [`Self::eligible_start_node_count`]: this is the set whose node-local
    /// state is published back onto the Node object, and a cordoned or
    /// briefly-NotReady worker should keep an accurate label rather than have
    /// it flap as the node's own condition changes.
    pub fn sandbox_selector_nodes(&self) -> Option<Vec<std::sync::Arc<Node>>> {
        if !self.k8s.caches.nodes_ready() {
            return None;
        }
        let selector = self.k8s.sandbox_pod_config.base_node_selector();
        Some(
            self.k8s
                .caches
                .nodes
                .state()
                .into_iter()
                .filter(|node| selector.matches(node))
                .collect(),
        )
    }

    /// Live Kubernetes node names from the watcher-backed reflector, or `None`
    /// when the node cache has not completed its initial `LIST` (cold). A
    /// reflector read, not a naked `LIST` — see the substrate `AGENTS.md`.
    ///
    /// A cold cache returns `None` so callers fail closed: an empty store there
    /// means "cluster membership unknown", not "no nodes exist". Reaper paths
    /// that key off node absence must treat `None` as "cannot decide" and skip,
    /// never as "every node is gone". A warm cache returns the exact set of
    /// `metadata.name`s currently backing schedulable and unschedulable Nodes
    /// alike, so a cordoned-but-present node is still reported live.
    ///
    /// Membership is keyed on the Node name (the reflector's own store key),
    /// not on `InternalIP`: every registered Node always has a name, whereas
    /// `status.addresses` can transiently lack an `InternalIP` during
    /// registration — so a name set can't misreport a present-but-not-yet-fully
    /// -statused Node as absent.
    pub fn live_node_names(&self) -> Option<std::collections::HashSet<String>> {
        if !self.k8s.caches.nodes_ready() {
            return None;
        }
        let names = self
            .k8s
            .caches
            .nodes
            .state()
            .iter()
            .filter_map(|node| node.metadata.name.clone())
            .collect();
        Some(names)
    }

    /// Confirm one Node's current API-server presence after a destructive
    /// reconciler has observed sustained reflector absence. This bounded GET
    /// is deliberately separate from the reflector snapshot: a stalled watch
    /// can omit a Node created after its last event, and absence is not safe to
    /// act on until the API server also reports the object missing.
    pub async fn kube_node_exists(&self, node_name: &str) -> Result<bool, KubeError> {
        let nodes: Api<Node> = Api::all(self.k8s.client.clone());
        nodes.get_opt(node_name).await.map(|node| node.is_some())
    }

    /// Patch `spec.unschedulable` on the core.v1.Node identified by
    /// `request.substrate_node_id`. Used by the admin cordon/drain
    /// endpoints; kept on `KubernetesSubstrate` rather than exposing the
    /// `kube::Client` directly so callers can't bypass the substrate boundary.
    pub async fn set_node_schedulability(
        &self,
        request: super::NodeSchedulabilityRequest<'_>,
        desired: super::NodeSchedulability,
    ) -> Result<(), SubstrateError> {
        use kube::api::{Patch, PatchParams};
        let unschedulable = matches!(desired, super::NodeSchedulability::Unschedulable);
        // RFC 7396 JSON merge patch on a single field — same shape `kubectl
        // cordon` uses. Avoids the apiVersion/kind/name metadata that
        // server-side apply would require.
        let patch = serde_json::json!({"spec": {"unschedulable": unschedulable}});
        let nodes: Api<Node> = Api::all(self.k8s.client.clone());
        nodes
            .patch(
                request.substrate_node_id,
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
            .map_err(|err| SubstrateError::Failed {
                message: format!("kubernetes node patch failed: {err}"),
            })?;
        Ok(())
    }
}

mod adapter;
#[cfg(test)]
pub(crate) mod adapter_characterization_tests;
mod execution;
mod node_sync;
mod per_sandbox_setup;
mod pod_ops;
mod pool;
mod runtime_upgrade;
#[cfg(test)]
pub(crate) mod scripted_test_doubles;
mod support;
#[cfg(any(test, feature = "test-hooks"))]
mod test_substrate;
#[cfg(test)]
pub(crate) mod tests;
mod wait_ready;

pub use node_sync::SyncNodeInventoryError;
use pod_ops::ExecutionHydrationOutcome;
use pod_ops::{ActivationAttemptAssignment, PodMaterializationOwnership, StartedRecoveryOwnership};
#[cfg(any(test, feature = "test-hooks"))]
pub use test_substrate::TEST_SANDBOX_NAMESPACE;
