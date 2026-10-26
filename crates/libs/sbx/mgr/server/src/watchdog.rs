//! Unified sandbox watchdog.
//!
//! Combines cached runtime and sidecar health monitoring into a single module.
//!
//! A jittered background sweep records bounded histories for Firecracker,
//! spaced-host, spaced-guest, and net-gateway. Read paths consume that cache
//! without issuing probes. Sidecar liveness and restart remain observationally
//! separate from the sandbox lifecycle.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use pplx_space_common_core::types::{LinuxPid, SandboxId, SemVer};
use pplx_space_metrics::sandbox::worker::{
    NetGatewayRouteVerificationStatus, NetGatewayTlsInterceptionMode,
};
use pplx_space_node_runtime_core::vm_state::VmState;

use crate::auto_lifecycle::AutoLifecycleCoordinator;
use crate::db::guest_health_scan::SandboxGuestHealthRow;
use crate::node_manager_transport::NodeManagerConnector;
use crate::node_state_sync::mark_sandbox_dirty;
use crate::service::network_policy::NetworkPolicyReconcileQueue;
use crate::sidecar::{sidecar_alive, sidecar_live_pid};
use crate::snapshot_scheduler::SnapshotScheduler;

mod diagnostics;

/// Health transition lines keep the watchdog's target, which log searches
/// already select on, though `diagnostics` writes them.
const LOG_TARGET: &str = module_path!();

const SIDECAR_CHECK_INTERVAL: Duration = Duration::from_secs(5);
// Background observations tolerate a slower reply than foreground Stats,
// without extending the caller's response latency.
const NET_GATEWAY_BACKGROUND_STATS_TIMEOUT: Duration = Duration::from_secs(2);
const NET_GATEWAY_STATS_TICK_STRIDE: u64 = 6;
const NET_GATEWAY_ROUTE_VERIFICATION_GRACE: Duration = Duration::from_secs(60);
const NET_GATEWAY_STATS_PROBE_SEED: u64 = 0xc2b2_ae3d_27d4_eb4f;
const NET_GATEWAY_STATS_CONCURRENCY: usize = 32;
const HEALTH_SCHEDULER_TICK: Duration = Duration::from_secs(5);
const HEALTH_PROBE_MIN_INTERVAL: Duration = Duration::from_secs(24);
const HEALTH_PROBE_JITTER_RANGE_SECS: u64 = 13;
const HEALTH_HISTORY_CAPACITY: usize = 5;
const HEALTH_FAILURE_THRESHOLD: usize = 2;
const HEALTH_SUCCESS_THRESHOLD: usize = 2;
const GUEST_HEALTH_FAILURE_THRESHOLD: u32 = 3;
const HEALTH_STALE_AFTER: Duration = Duration::from_secs(72);
const HEALTH_PROBE_CONCURRENCY: usize = 32;
const SPACED_HOST_HEALTH_TIMEOUT: Duration = Duration::from_millis(250);
const SPACED_GUEST_HEALTH_TIMEOUT: Duration = Duration::from_millis(500);
const NET_GATEWAY_HEALTH_TIMEOUT: Duration = Duration::from_millis(250);
const SIDECAR_RESTART_TIMEOUT: Duration = Duration::from_secs(10);
const SIDECAR_RESTART_CONCURRENCY: usize = 16;
const RUN_DIR: &str = pplx_space_common_core::host::linux::paths::SANDBOX_RUN_DIR;
const GUEST_HEALTH_LIFECYCLES: [crate::state::SandboxState; 9] = [
    crate::state::SandboxState::Starting,
    crate::state::SandboxState::Started,
    crate::state::SandboxState::Stopping,
    crate::state::SandboxState::Pausing,
    crate::state::SandboxState::Paused,
    crate::state::SandboxState::Resuming,
    crate::state::SandboxState::Checkpointing,
    crate::state::SandboxState::Suspending,
    crate::state::SandboxState::Restoring,
];
const COMPOSED_HEALTH_LIFECYCLES: [crate::state::SandboxState; 3] = [
    crate::state::SandboxState::Started,
    crate::state::SandboxState::Paused,
    crate::state::SandboxState::Quarantined,
];
const GUEST_HEALTH_CATEGORIES: [GuestHealthCategory; 3] = [
    GuestHealthCategory::Healthy,
    GuestHealthCategory::Degraded,
    GuestHealthCategory::Unreachable,
];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum NetGatewayLifecycle {
    Known(crate::state::SandboxState),
    Unknown,
}

impl NetGatewayLifecycle {
    fn as_str(self) -> &'static str {
        match self {
            Self::Known(state) => state.as_str(),
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct NetGatewayInventoryBucket {
    tls_interception_mode: NetGatewayTlsInterceptionMode,
    gateway_sha: String,
    template_content_hash: String,
    lifecycle: NetGatewayLifecycle,
}

fn net_gateway_tls_interception_mode(value: i32) -> NetGatewayTlsInterceptionMode {
    match pplx_space_net_common::proto::TlsInterception::try_from(value) {
        Ok(pplx_space_net_common::proto::TlsInterception::Off) => {
            NetGatewayTlsInterceptionMode::Off
        }
        Ok(pplx_space_net_common::proto::TlsInterception::On) => NetGatewayTlsInterceptionMode::On,
        Ok(pplx_space_net_common::proto::TlsInterception::Unspecified) | Err(_) => {
            NetGatewayTlsInterceptionMode::Unknown
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum NetGatewayRelaunchArgsError {
    #[error("sandbox database mutex poisoned: {0}")]
    DatabaseMutexPoisoned(String),
    #[error("failed to load sandbox {sandbox_id} from local DB: {source}")]
    LoadSandbox {
        sandbox_id: String,
        #[source]
        source: crate::db::Error,
    },
    #[error("sandbox {0} missing from local DB")]
    MissingSandbox(String),
}

#[derive(Debug, thiserror::Error)]
enum SpacedHostRestartError {
    #[error("connect to node-manager for spaced-host restart: {0}")]
    NodeManagerConnect(#[from] pplx_space_node_mgr_client::ConnectError),
    #[error("missing sandbox uid for spaced-host restart")]
    MissingUid,
    #[error("missing vsock UDS path for spaced-host restart")]
    MissingVsockUdsPath,
    #[error("launch spaced-host replacement: {0}")]
    Launch(#[source] tonic::Status),
    #[error("spaced-host replacement pid {0} exceeds i32")]
    PidOutOfRange(u32),
    #[error("connect to spaced-host replacement: {0}")]
    SpacedHostConnect(#[source] pplx_space_spaced_host_client::ConnectError),
    #[error("configure spaced-host replacement: {0}")]
    Configure(#[source] tonic::Status),
    #[error("sandbox left watchdog during spaced-host restart")]
    SandboxLeftWatchdog,
    #[error("sandbox authority changed during spaced-host restart")]
    AuthorityChanged,
    #[error("sandbox database mutex poisoned")]
    DatabaseMutexPoisoned,
    #[error("persist spaced-host replacement pid: {0}")]
    Persist(#[source] crate::db::Error),
}

#[derive(Debug, thiserror::Error)]
enum WatchdogTimeoutError<E>
where
    E: fmt::Debug + fmt::Display,
{
    #[error("{operation} timed out after {timeout:?}")]
    Timeout {
        operation: &'static str,
        timeout: Duration,
    },
    #[error("{0}")]
    Operation(E),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GuestHealthProbe {
    runtime_alive: bool,
    guest_healthy: bool,
    spaced_host_alive: bool,
    consecutive_guest_probe_failures: u32,
}

impl From<&SandboxHealth> for GuestHealthProbe {
    fn from(health: &SandboxHealth) -> Self {
        Self {
            runtime_alive: health.runtime_alive,
            guest_healthy: health.guest_healthy,
            spaced_host_alive: health.spaced_host_alive,
            consecutive_guest_probe_failures: health.consecutive_guest_probe_failures,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HealthGaugeSnapshot {
    guest: GuestHealthProbe,
    composed: ComposedHealthStatus,
}

impl From<&SandboxHealth> for HealthGaugeSnapshot {
    fn from(health: &SandboxHealth) -> Self {
        Self {
            guest: GuestHealthProbe::from(health),
            composed: health.current_status(),
        }
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum GuestHealthCategory {
    Healthy,
    Degraded,
    Unreachable,
}

impl GuestHealthCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unreachable => "unreachable",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GuestHealthGaugeValue {
    lifecycle: &'static str,
    health: &'static str,
    count: usize,
}

#[derive(Clone)]
pub struct SidecarInfo {
    pub guest_image: pplx_space_common_core::types::guest_image::GuestImageContract,
    pub node_execution_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
    pub sandbox_id: String,
    pub uid: Option<u32>,
    pub runtime_pid: Option<u32>,
    pub spaced_host_pid: Option<u32>,
    pub netgtw_pid: Option<u32>,
    pub vsock_uds_path: Option<String>,
    pub process_env: BTreeMap<String, String>,
    pub network_tx_bytes: u64,
    pub network_rx_bytes: u64,
}

fn sidecar_info_from_row(row: &crate::db::SandboxRow) -> SidecarInfo {
    SidecarInfo {
        guest_image: row.guest_image,
        node_execution_id: row.node_execution_id,
        sandbox_id: row.id.clone(),
        uid: row.uid,
        runtime_pid: row.pid,
        spaced_host_pid: row.spaced_host_pid,
        netgtw_pid: row.netgtw_pid,
        vsock_uds_path: row.vsock_uds_path.clone(),
        process_env: row.process_env.clone(),
        network_tx_bytes: row.network_tx_bytes,
        network_rx_bytes: row.network_rx_bytes,
    }
}

enum SidecarRestartRequest {
    SpacedHost(SidecarInfo),
    NetGateway(SidecarInfo),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SidecarKind {
    SpacedHost,
    NetGateway,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SidecarRestartKey {
    sandbox_id: String,
    kind: SidecarKind,
}

impl SidecarRestartRequest {
    fn sandbox_id(&self) -> &str {
        match self {
            Self::SpacedHost(info) | Self::NetGateway(info) => &info.sandbox_id,
        }
    }

    fn operation(&self) -> &'static str {
        match self {
            Self::SpacedHost(_) => "spaced-host restart",
            Self::NetGateway(_) => "net-gateway restart",
        }
    }

    fn key(&self) -> SidecarRestartKey {
        SidecarRestartKey {
            sandbox_id: self.sandbox_id().to_string(),
            kind: match self {
                Self::SpacedHost(_) => SidecarKind::SpacedHost,
                Self::NetGateway(_) => SidecarKind::NetGateway,
            },
        }
    }

    fn still_has_authority(&self, current: &SidecarInfo) -> bool {
        match self {
            Self::SpacedHost(observed) => {
                current.uid == observed.uid
                    && current.spaced_host_pid == observed.spaced_host_pid
                    && current.vsock_uds_path == observed.vsock_uds_path
                    && current.process_env == observed.process_env
            }
            Self::NetGateway(observed) => {
                current.uid == observed.uid && current.netgtw_pid == observed.netgtw_pid
            }
        }
    }

    fn still_needs_restart(&self, current: &SidecarInfo) -> bool {
        if !self.still_has_authority(current) || !runtime_alive(self.sandbox_id()) {
            return false;
        }
        match self {
            Self::SpacedHost(_) => !sidecar_pid_is_committed(
                current.spaced_host_pid,
                sidecar_live_pid(self.sandbox_id(), "space-spaced-host-svc"),
            ),
            Self::NetGateway(_) => !sidecar_pid_is_committed(
                current.netgtw_pid,
                sidecar_live_pid(self.sandbox_id(), "space-netgtw-svc"),
            ),
        }
    }
}

#[derive(Default)]
struct SidecarRestartQueue {
    pending: VecDeque<SidecarRestartRequest>,
    scheduled: HashSet<SidecarRestartKey>,
}

impl SidecarRestartQueue {
    fn schedule(&mut self, request: SidecarRestartRequest) -> bool {
        if !self.scheduled.insert(request.key()) {
            return false;
        }
        self.pending.push_back(request);
        true
    }

    fn pop_front(&mut self) -> Option<SidecarRestartRequest> {
        self.pending.pop_front()
    }

    fn retain_pending(&mut self, mut retain: impl FnMut(&SidecarRestartRequest) -> bool) {
        let scheduled = &mut self.scheduled;
        self.pending.retain(|request| {
            if retain(request) {
                true
            } else {
                scheduled.remove(&request.key());
                false
            }
        });
    }

    fn complete(&mut self, key: &SidecarRestartKey) {
        self.scheduled.remove(key);
    }
}

struct SidecarRestartOutcome {
    key: SidecarRestartKey,
    sandbox_id: String,
    operation: &'static str,
    result: Result<(), String>,
}

type SidecarRestartFuture = BoxFuture<'static, SidecarRestartOutcome>;

struct NetGatewayStatsObservation {
    network_tx_bytes: u64,
    network_rx_bytes: u64,
    build_git_sha: String,
    tls_interception_mode: NetGatewayTlsInterceptionMode,
    identity_profile_version: u32,
    route_verification_key_generation: u64,
}

#[derive(Clone, Debug)]
pub struct SandboxHealth {
    pub guest_uptime_secs: u64,
    pub guest_healthy: bool,
    pub guest_ready: bool,
    pub consecutive_guest_probe_failures: u32,
    pub runtime_alive: bool,
    pub vsock_raw_ping_ok: bool,
    pub spaced_host_alive: bool,
    pub netgtw_alive: bool,
    pub network_tx_bytes: u64,
    pub network_rx_bytes: u64,
    pub last_guest_ready_at: Option<SystemTime>,
    pub last_guest_heartbeat_at: Option<SystemTime>,
    pub spaced_version: Option<SemVer>,
    /// Build git sha reported by the running net-gateway (`GetStats`), `None`
    /// until the first successful stats probe. `Some("")` means the gateway
    /// answered but its binary predates sha reporting — a definitively stale
    /// gateway, distinct from `None` (not probed yet). Compared against this
    /// sandbox-manager's own build sha to detect gateways still running a
    /// pre-deploy binary.
    pub netgtw_git_sha: Option<String>,
    netgtw_stats_pid: Option<u32>,
    netgtw_tls_interception_mode: NetGatewayTlsInterceptionMode,
    /// Stable identity/token capability level reported by `GetStats`. `None`
    /// means no successful stats probe has completed since this gateway
    /// process started.
    pub netgtw_identity_profile_version: Option<u32>,
    /// Verification-key generation atomically accepted by the live gateway.
    /// Zero is a valid observed value meaning a capable gateway has not yet
    /// installed a bundle; `None` means the gateway has not been probed.
    pub netgtw_route_verification_key_generation: Option<u64>,
    netgtw_route_verification_window: NetGatewayRouteVerificationWindow,
    health_status: ComposedHealthStatus,
    health_observed_at: Option<SystemTime>,
    health_components: HealthComponents,
    lifecycle_epoch: u64,
    next_health_probe_at: Instant,
    health_probe_sequence: u64,
    /// When a degraded transition last carried console tails, so a flapping
    /// sandbox cannot repeat them more often than
    /// `diagnostics::CONSOLE_TAIL_MIN_INTERVAL`.
    last_console_tail_at: Option<Instant>,
}

#[derive(Clone, Copy, Debug)]
struct NetGatewayRouteVerificationWindow {
    gateway_started_at: Instant,
    expected_generation: u64,
    expected_generation_changed_at: Instant,
}

impl NetGatewayRouteVerificationWindow {
    fn new(now: Instant, expected_generation: u64) -> Self {
        Self {
            gateway_started_at: now,
            expected_generation,
            expected_generation_changed_at: now,
        }
    }

    fn observe_expected_generation(&mut self, generation: u64, now: Instant) {
        if self.expected_generation != generation {
            self.expected_generation = generation;
            self.expected_generation_changed_at = now;
        }
    }

    fn restart_gateway(&mut self, now: Instant) {
        self.gateway_started_at = now;
    }

    fn is_within_grace(self, now: Instant) -> bool {
        let started_at = self
            .gateway_started_at
            .max(self.expected_generation_changed_at);
        now.saturating_duration_since(started_at) < NET_GATEWAY_ROUTE_VERIFICATION_GRACE
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum ComposedHealthStatus {
    Unspecified,
    Initializing,
    Healthy,
    Degraded,
}

impl ComposedHealthStatus {
    fn as_proto(self) -> i32 {
        match self {
            Self::Unspecified => crate::proto::SandboxHealthStatus::Unspecified as i32,
            Self::Initializing => crate::proto::SandboxHealthStatus::Initializing as i32,
            Self::Healthy => crate::proto::SandboxHealthStatus::Healthy as i32,
            Self::Degraded => crate::proto::SandboxHealthStatus::Degraded as i32,
        }
    }

    fn as_metric(self) -> pplx_space_metrics::sandbox::worker::SandboxHealthStatus {
        match self {
            Self::Unspecified => {
                pplx_space_metrics::sandbox::worker::SandboxHealthStatus::Unspecified
            }
            Self::Initializing => {
                pplx_space_metrics::sandbox::worker::SandboxHealthStatus::Initializing
            }
            Self::Healthy => pplx_space_metrics::sandbox::worker::SandboxHealthStatus::Healthy,
            Self::Degraded => pplx_space_metrics::sandbox::worker::SandboxHealthStatus::Degraded,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HealthComponent {
    Firecracker,
    SpacedHost,
    SpacedGuest,
    NetGateway,
}

impl HealthComponent {
    const ALL: [Self; 4] = [
        Self::Firecracker,
        Self::SpacedHost,
        Self::SpacedGuest,
        Self::NetGateway,
    ];

    fn as_proto(self) -> i32 {
        match self {
            Self::Firecracker => crate::proto::SandboxHealthComponent::Runtime as i32,
            Self::SpacedHost => crate::proto::SandboxHealthComponent::SpacedHost as i32,
            Self::SpacedGuest => crate::proto::SandboxHealthComponent::SpacedGuest as i32,
            Self::NetGateway => crate::proto::SandboxHealthComponent::NetGateway as i32,
        }
    }

    fn as_metric(self) -> pplx_space_metrics::sandbox::worker::SandboxHealthComponent {
        match self {
            Self::Firecracker => {
                pplx_space_metrics::sandbox::worker::SandboxHealthComponent::Firecracker
            }
            Self::SpacedHost => {
                pplx_space_metrics::sandbox::worker::SandboxHealthComponent::SpacedHost
            }
            Self::SpacedGuest => {
                pplx_space_metrics::sandbox::worker::SandboxHealthComponent::SpacedGuest
            }
            Self::NetGateway => {
                pplx_space_metrics::sandbox::worker::SandboxHealthComponent::NetGateway
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HealthProbeOutcome {
    Success,
    Timeout,
    Unavailable,
    UnexpectedState,
    NotApplicable,
}

impl HealthProbeOutcome {
    fn is_success(self) -> bool {
        matches!(self, Self::Success | Self::NotApplicable)
    }

    fn as_proto(self) -> i32 {
        match self {
            Self::Success => crate::proto::SandboxHealthProbeOutcome::Success as i32,
            Self::Timeout => crate::proto::SandboxHealthProbeOutcome::Timeout as i32,
            Self::Unavailable => crate::proto::SandboxHealthProbeOutcome::Unavailable as i32,
            Self::UnexpectedState => {
                crate::proto::SandboxHealthProbeOutcome::UnexpectedState as i32
            }
            Self::NotApplicable => crate::proto::SandboxHealthProbeOutcome::NotApplicable as i32,
        }
    }

    fn as_metric(self) -> pplx_space_metrics::sandbox::worker::SandboxHealthProbeOutcome {
        match self {
            Self::Success => {
                pplx_space_metrics::sandbox::worker::SandboxHealthProbeOutcome::Success
            }
            Self::Timeout => {
                pplx_space_metrics::sandbox::worker::SandboxHealthProbeOutcome::Timeout
            }
            Self::Unavailable => {
                pplx_space_metrics::sandbox::worker::SandboxHealthProbeOutcome::Unavailable
            }
            Self::UnexpectedState => {
                pplx_space_metrics::sandbox::worker::SandboxHealthProbeOutcome::UnexpectedState
            }
            Self::NotApplicable => {
                pplx_space_metrics::sandbox::worker::SandboxHealthProbeOutcome::NotApplicable
            }
        }
    }
}

#[derive(Clone, Debug)]
struct HealthObservation {
    observed_at: SystemTime,
    latency_micros: u64,
    outcome: HealthProbeOutcome,
    detail: String,
    firecracker_state: Option<VmState>,
    lifecycle_epoch: u64,
}

struct ComposedProbeResult {
    observations: [HealthObservation; 4],
    guest_status: Option<pplx_space_spaced_host_client::proto::GetStatusResponse>,
}

#[derive(Clone, Debug, Default)]
struct ComponentHealth {
    observations: VecDeque<HealthObservation>,
    degraded: bool,
    /// Failed probes since the last success or not-applicable probe. Unlike
    /// the bounded history, this keeps counting for as long as the outage
    /// lasts.
    consecutive_failures: u32,
    last_success_at: Option<SystemTime>,
}

impl ComponentHealth {
    fn push(&mut self, observation: HealthObservation) {
        match observation.outcome {
            HealthProbeOutcome::Success => {
                self.consecutive_failures = 0;
                self.last_success_at = Some(observation.observed_at);
            }
            HealthProbeOutcome::NotApplicable => self.consecutive_failures = 0,
            HealthProbeOutcome::Timeout
            | HealthProbeOutcome::Unavailable
            | HealthProbeOutcome::UnexpectedState => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            }
        }
        if self.observations.len() == HEALTH_HISTORY_CAPACITY {
            self.observations.pop_front();
        }
        self.observations.push_back(observation);
    }
}

#[derive(Clone, Debug, Default)]
struct HealthComponents {
    firecracker: ComponentHealth,
    spaced_host: ComponentHealth,
    spaced_guest: ComponentHealth,
    net_gateway: ComponentHealth,
}

impl HealthComponents {
    fn get(&self, component: HealthComponent) -> &ComponentHealth {
        match component {
            HealthComponent::Firecracker => &self.firecracker,
            HealthComponent::SpacedHost => &self.spaced_host,
            HealthComponent::SpacedGuest => &self.spaced_guest,
            HealthComponent::NetGateway => &self.net_gateway,
        }
    }

    fn get_mut(&mut self, component: HealthComponent) -> &mut ComponentHealth {
        match component {
            HealthComponent::Firecracker => &mut self.firecracker,
            HealthComponent::SpacedHost => &mut self.spaced_host,
            HealthComponent::SpacedGuest => &mut self.spaced_guest,
            HealthComponent::NetGateway => &mut self.net_gateway,
        }
    }

    fn push(&mut self, component: HealthComponent, observation: HealthObservation) {
        self.get_mut(component).push(observation);
    }
}

pub struct SandboxWatchdog {
    state: Arc<Mutex<HashMap<String, SandboxHealth>>>,
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    db: crate::db::SharedConnection,
    network_policy_reconcile_queue: NetworkPolicyReconcileQueue,
    expected_route_verification_key_generation: Arc<AtomicU64>,
    feature_flags: pplx_space_feature_flags::FeatureFlagClient,
    node_manager: NodeManagerConnector,
    vm_outputs: crate::vm_diagnostics::VmOutputReaders,
    restored_guests: crate::vm_diagnostics::RestoredGuests,
}

#[derive(Clone, Default)]
pub struct HealthLifecycleEpochs {
    state: Arc<Mutex<HashMap<String, SandboxHealth>>>,
}

impl HealthLifecycleEpochs {
    pub async fn record_edge(&self, sandbox_id: &str) {
        let mut map = self.state.lock().await;
        let Some(health) = map.get_mut(sandbox_id) else {
            return;
        };
        health.lifecycle_epoch = health.lifecycle_epoch.wrapping_add(1);
        health.consecutive_guest_probe_failures = 0;
        for component in HealthComponent::ALL {
            health
                .health_components
                .get_mut(component)
                .consecutive_failures = 0;
        }
    }
}

impl SandboxWatchdog {
    // These are the watchdog's long-lived dependencies; grouping them into an
    // opaque context would only move the constructor contract elsewhere.
    #[expect(
        clippy::too_many_arguments,
        reason = "the constructor names each long-lived watchdog dependency explicitly"
    )]
    pub(crate) fn new(
        db: crate::db::SharedConnection,
        network_policy_reconcile_queue: NetworkPolicyReconcileQueue,
        snapshot_scheduler: Arc<SnapshotScheduler>,
        auto_lifecycle: AutoLifecycleCoordinator,
        lifecycle_epochs: HealthLifecycleEpochs,
        feature_flags: pplx_space_feature_flags::FeatureFlagClient,
        shutdown_token: CancellationToken,
        drain_state: Arc<crate::service::drain::DrainState>,
        task_tracker: &TaskTracker,
        node_manager: NodeManagerConnector,
    ) -> Self {
        let state = lifecycle_epochs.state.clone();
        let sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let task_state = state.clone();
        let task_sidecars = sidecars.clone();
        let task_db = db.clone();
        let task_network_policy_reconcile_queue = network_policy_reconcile_queue.clone();
        let expected_route_verification_key_generation = Arc::new(AtomicU64::new(0));
        let task_expected_route_verification_key_generation =
            Arc::clone(&expected_route_verification_key_generation);
        let task_feature_flags = feature_flags.clone();
        let task_snapshot_scheduler = snapshot_scheduler.clone();
        let task_node_manager = node_manager.clone();
        let sidecar_shutdown = shutdown_token.child_token();
        let sidecar_guard = crate::service::drain::CriticalTaskGuard::new(
            "sidecar watchdog",
            sidecar_shutdown.clone(),
            shutdown_token.clone(),
            Arc::clone(&drain_state),
        );
        task_tracker.spawn(async move {
            let _guard = sidecar_guard;
            sidecar_loop(
                task_state,
                task_sidecars,
                task_db,
                task_network_policy_reconcile_queue,
                task_expected_route_verification_key_generation,
                task_feature_flags,
                sidecar_shutdown,
                task_node_manager,
            )
            .await;
        });
        let health_guard = crate::service::drain::CriticalTaskGuard::new(
            "composed health watchdog",
            shutdown_token.clone(),
            shutdown_token.clone(),
            drain_state,
        );
        let restored_guests = crate::vm_diagnostics::RestoredGuests::default();
        let health = composed_health_loop(
            state.clone(),
            sidecars.clone(),
            db.clone(),
            task_snapshot_scheduler,
            auto_lifecycle,
            lifecycle_epochs,
            shutdown_token.child_token(),
            node_manager.clone(),
            restored_guests.clone(),
        );
        task_tracker.spawn(async move {
            let _guard = health_guard;
            health.await;
        });
        Self {
            state,
            sidecars,
            db,
            network_policy_reconcile_queue,
            expected_route_verification_key_generation,
            feature_flags,
            node_manager,
            vm_outputs: crate::vm_diagnostics::VmOutputReaders::new(shutdown_token.child_token()),
            restored_guests,
        }
    }

    #[cfg(test)]
    fn new_for_tests(
        db: crate::db::SharedConnection,
        network_policy_reconcile_queue: NetworkPolicyReconcileQueue,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) -> Self {
        let auto_lifecycle = AutoLifecycleCoordinator::default();
        let lifecycle_epochs = HealthLifecycleEpochs::default();
        // The loops tick immediately; their node-manager is an absent socket.
        let node_manager = crate::node_manager_transport::fake::unbound_in_absent_dir();
        let snapshot_scheduler = Arc::new(SnapshotScheduler::new(Arc::new(
            crate::snapshot_scheduler::CheckpointContext {
                db: db.clone(),
                auto_lifecycle: auto_lifecycle.clone(),
                health_lifecycle: lifecycle_epochs.clone(),
                node_manager: node_manager.clone(),
            },
        )));
        Self::new(
            db,
            network_policy_reconcile_queue,
            snapshot_scheduler,
            auto_lifecycle,
            lifecycle_epochs,
            pplx_space_feature_flags::FeatureFlagClient::disabled(),
            shutdown_token,
            Arc::new(crate::service::drain::DrainState::default()),
            task_tracker,
            node_manager,
        )
    }

    pub async fn add_sandbox(&self, info: SidecarInfo) {
        let id = info.sandbox_id.clone();
        self.vm_outputs.attach(&id, info.runtime_pid).await;
        let network_tx_bytes = info.network_tx_bytes;
        let network_rx_bytes = info.network_rx_bytes;
        {
            let mut map = self.sidecars.lock().await;
            map.insert(id.clone(), info);
        }
        let mut map = self.state.lock().await;
        let now = Instant::now();
        let expected_generation = self
            .expected_route_verification_key_generation
            .load(Ordering::Acquire);
        map.entry(id.clone()).or_insert(SandboxHealth {
            guest_uptime_secs: 0,
            guest_healthy: true,
            guest_ready: false,
            consecutive_guest_probe_failures: 0,
            runtime_alive: runtime_alive(&id),
            vsock_raw_ping_ok: false,
            spaced_host_alive: sidecar_alive(&id, "space-spaced-host-svc"),
            netgtw_alive: sidecar_alive(&id, "space-netgtw-svc"),
            network_tx_bytes,
            network_rx_bytes,
            last_guest_ready_at: None,
            last_guest_heartbeat_at: None,
            spaced_version: None,
            netgtw_git_sha: None,
            netgtw_stats_pid: None,
            netgtw_tls_interception_mode: NetGatewayTlsInterceptionMode::Unknown,
            netgtw_identity_profile_version: None,
            netgtw_route_verification_key_generation: None,
            netgtw_route_verification_window: NetGatewayRouteVerificationWindow::new(
                now,
                expected_generation,
            ),
            health_status: ComposedHealthStatus::Initializing,
            health_observed_at: None,
            health_components: HealthComponents::default(),
            lifecycle_epoch: 0,
            next_health_probe_at: now + initial_health_probe_delay(&id),
            health_probe_sequence: 0,
            last_console_tail_at: None,
        });
    }

    /// Re-arm monitoring from current durable endpoint authority. Lifecycle
    /// guards call this after a fallible teardown because their original row
    /// snapshot may predate a concurrent sidecar restart or operator cycle.
    pub(crate) async fn rearm_sandbox_from_db(&self, sandbox_id: &str) {
        let info = {
            let conn = match self.db.lock() {
                Ok(conn) => conn,
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        %error,
                        "failed to re-arm watchdog: sandbox database mutex poisoned"
                    );
                    return;
                }
            };
            match crate::db::read::sandbox(&conn, sandbox_id) {
                Ok(Some(row)) => sidecar_info_from_row(&row),
                Ok(None) => {
                    tracing::debug!(sandbox_id, "sandbox disappeared before watchdog re-arm");
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        sandbox_id,
                        %error,
                        "failed to load current sandbox authority for watchdog re-arm"
                    );
                    return;
                }
            }
        };
        self.add_sandbox(info).await;
    }

    #[cfg(test)]
    pub(crate) fn vm_outputs_for_test(&self) -> &crate::vm_diagnostics::VmOutputReaders {
        &self.vm_outputs
    }

    #[cfg(test)]
    pub(crate) async fn sidecar_info_for_test(&self, sandbox_id: &str) -> Option<SidecarInfo> {
        self.sidecars.lock().await.get(sandbox_id).cloned()
    }

    /// Note that this sandbox's guest was just restored from a snapshot, so its
    /// next diagnostics drain is the snapshot's ([`crate::vm_diagnostics::RestoredGuests`]).
    pub(crate) fn mark_restored_from_snapshot(&self, sandbox_id: &str) {
        self.restored_guests.mark(sandbox_id);
    }

    pub async fn remove_sandbox(&self, sandbox_id: &str) {
        self.vm_outputs.detach(sandbox_id);
        self.restored_guests.forget(sandbox_id);
        let committed_pid = self
            .sidecars
            .lock()
            .await
            .get(sandbox_id)
            .and_then(|info| info.netgtw_pid);
        if let Some(live_pid) = sidecar_live_pid(sandbox_id, "space-netgtw-svc")
            && committed_pid == Some(live_pid)
            && observe_authoritative_net_gateway_process(
                &self.state,
                &self.sidecars,
                sandbox_id,
                committed_pid,
                Some(live_pid),
            )
            .await
        {
            probe_net_gateway_stats(sandbox_id, live_pid, &self.state, &self.db).await;
        }
        {
            let mut map = self.sidecars.lock().await;
            map.remove(sandbox_id);
        }
        // Clients are kept only under ids that parse.
        if let Ok(sandbox_id) = sandbox_id.parse::<SandboxId>() {
            crate::net_gateway_clients::forget(sandbox_id);
        }
        let mut map = self.state.lock().await;
        map.remove(sandbox_id);
    }

    /// Reserve registration retirement before a synchronous inventory CAS.
    /// Dropping this closure without calling it leaves both maps unchanged.
    pub(crate) fn try_retirement<'a>(
        &'a self,
        sandbox_id: &str,
    ) -> Option<impl FnOnce() + use<'a>> {
        let mut health = self.state.try_lock().ok()?;
        let mut sidecars = self.sidecars.try_lock().ok()?;
        let vm_outputs = &self.vm_outputs;
        let restored_guests = &self.restored_guests;
        let kept_gateway_id = sandbox_id.parse::<SandboxId>().ok();
        let sandbox_id = sandbox_id.to_owned();
        Some(move || {
            health.remove(&sandbox_id);
            sidecars.remove(&sandbox_id);
            vm_outputs.detach(&sandbox_id);
            restored_guests.forget(&sandbox_id);
            if let Some(sandbox_id) = kept_gateway_id {
                crate::net_gateway_clients::forget(sandbox_id);
            }
        })
    }

    pub async fn record_lifecycle_edge(&self, sandbox_id: &str) {
        self.lifecycle_epochs().record_edge(sandbox_id).await;
    }

    pub fn lifecycle_epochs(&self) -> HealthLifecycleEpochs {
        HealthLifecycleEpochs {
            state: Arc::clone(&self.state),
        }
    }

    pub fn set_expected_route_verification_key_generation(&self, generation: u64) {
        self.expected_route_verification_key_generation
            .store(generation, Ordering::Release);
    }

    pub async fn get_health(&self, sandbox_id: &str) -> Option<SandboxHealth> {
        let map = self.state.lock().await;
        map.get(sandbox_id).cloned()
    }

    pub async fn get_composed_health(
        &self,
        sandbox_id: &str,
    ) -> Option<crate::proto::GetSandboxHealthResponse> {
        let health = {
            let map = self.state.lock().await;
            map.get(sandbox_id)?.clone()
        };
        Some(health.to_proto(sandbox_id))
    }

    /// Relaunch the sandbox's net-gateway via node-manager and enforce the
    /// paired cache invariant: the cached build sha is dropped on success so
    /// State/`--stale` report the new process as "unknown" until its first
    /// stats probe, instead of re-advertising it under the old process' sha
    /// (which would invite an immediate pointless re-cycle). This is the only
    /// relaunch entry point for callers outside the watchdog loop — the clear
    /// cannot be forgotten because it is not the caller's job.
    pub async fn relaunch_netgtw(&self, sandbox_id: &str, uid: u32) -> Result<u32, String> {
        let info = self
            .sidecars
            .lock()
            .await
            .get(sandbox_id)
            .cloned()
            .ok_or_else(|| format!("sandbox {sandbox_id} is not monitored"))?;
        if info.uid != Some(uid) {
            return Err(format!(
                "sandbox {sandbox_id} sidecar authority changed before net-gateway relaunch"
            ));
        }
        relaunch_netgtw(
            &info,
            &self.db,
            &self.sidecars,
            &self.network_policy_reconcile_queue,
            &self.state,
            &self.feature_flags,
            &self.node_manager,
        )
        .await
    }

    pub async fn update_network_counters(
        &self,
        sandbox_id: &str,
        network_tx_bytes: u64,
        network_rx_bytes: u64,
    ) {
        let mut map = self.state.lock().await;
        if let Some(health) = map.get_mut(sandbox_id) {
            health.network_tx_bytes = network_tx_bytes;
            health.network_rx_bytes = network_rx_bytes;
        }
    }

    /// Recover monitoring for sandboxes that survived a sandbox-manager
    /// restart.
    ///
    /// Discovery walks the run directory: a live `fc.pid` symlink is the
    /// liveness source of truth for "a VM is running here" — the same
    /// predicate every other liveness check in this crate uses — and the run
    /// dir's ownership guarantees this service user can always read it. The
    /// cache directory must not gate this decision: it answers "what data
    /// exists on disk", its per-sandbox subtrees are not guaranteed readable
    /// by this user, and `Path::exists()` reads "permission denied" as
    /// "absent". A running VM silently dropped here is never probed for
    /// gateway staleness and becomes invisible to
    /// `cycle-net-gateway --stale` until the next restart.
    pub async fn recover_all(&self) {
        self.recover_all_in(Path::new(RUN_DIR)).await;
    }

    async fn recover_all_in(&self, run_dir: &Path) {
        for sandbox_id in crate::service::cache_cleanup::list_live_sandbox_run_dirs_in(run_dir) {
            let id = sandbox_id.to_string();
            // Read actual uid/vsock_uds_path from DB so sidecar restart works.
            let db_row = {
                let conn = match self.db.lock() {
                    Ok(conn) => conn,
                    Err(error) => {
                        tracing::warn!(sandbox_id = %id, %error, "sandbox database mutex poisoned during watchdog recovery");
                        continue;
                    }
                };
                match crate::db::read::sandbox(&conn, &id) {
                    Ok(row) => row,
                    Err(error) => {
                        tracing::warn!(sandbox_id = %id, %error, "failed to load live sandbox during watchdog recovery");
                        continue;
                    }
                }
            };
            let Some(row) = db_row else {
                // Disk says a VM is running here but the DB has no row: that
                // is orphan evidence worth surfacing, not debris to pass over
                // silently.
                tracing::warn!(
                    sandbox_id = %id,
                    "running VM with no sandbox DB row; not recovering monitoring"
                );
                continue;
            };
            self.add_sandbox(sidecar_info_from_row(&row)).await;
            self.network_policy_reconcile_queue.enqueue(id);
        }
    }
}

impl SandboxHealth {
    fn observe_net_gateway_process(&mut self, process_pid: Option<u32>) {
        if self.netgtw_stats_pid == process_pid {
            return;
        }
        self.netgtw_stats_pid = process_pid;
        self.clear_net_gateway_stats();
    }

    fn clear_net_gateway_stats(&mut self) {
        self.netgtw_git_sha = None;
        self.netgtw_tls_interception_mode = NetGatewayTlsInterceptionMode::Unknown;
        self.netgtw_identity_profile_version = None;
        self.netgtw_route_verification_key_generation = None;
        self.netgtw_route_verification_window
            .restart_gateway(Instant::now());
    }

    fn apply_net_gateway_stats(
        &mut self,
        process_pid: u32,
        observation: NetGatewayStatsObservation,
    ) -> Option<(u64, u64)> {
        if self.netgtw_stats_pid != Some(process_pid) {
            return None;
        }
        self.network_tx_bytes = observation.network_tx_bytes.max(self.network_tx_bytes);
        self.network_rx_bytes = observation.network_rx_bytes.max(self.network_rx_bytes);
        self.netgtw_git_sha = Some(observation.build_git_sha);
        self.netgtw_tls_interception_mode = observation.tls_interception_mode;
        self.netgtw_identity_profile_version = Some(observation.identity_profile_version);
        self.netgtw_route_verification_key_generation =
            Some(observation.route_verification_key_generation);
        Some((self.network_tx_bytes, self.network_rx_bytes))
    }

    pub(crate) fn status_proto(&self) -> i32 {
        self.current_status().as_proto()
    }

    pub(crate) fn observed_at_proto(&self) -> Option<prost_types::Timestamp> {
        self.health_observed_at.and_then(system_time_to_timestamp)
    }

    fn current_status(&self) -> ComposedHealthStatus {
        if self
            .health_observed_at
            .and_then(|observed| observed.elapsed().ok())
            .is_none_or(|age| age > HEALTH_STALE_AFTER)
        {
            ComposedHealthStatus::Initializing
        } else {
            self.health_status
        }
    }

    fn to_proto(&self, sandbox_id: &str) -> crate::proto::GetSandboxHealthResponse {
        let status = self.current_status();
        let degraded_components = HealthComponent::ALL
            .into_iter()
            .filter(|component| self.component_is_degraded(*component))
            .map(HealthComponent::as_proto)
            .collect();
        let components = HealthComponent::ALL
            .into_iter()
            .map(|component| crate::proto::SandboxComponentHealth {
                component: component.as_proto(),
                observations: self
                    .health_components
                    .get(component)
                    .observations
                    .iter()
                    .map(|observation| crate::proto::SandboxHealthObservation {
                        observed_at: system_time_to_timestamp(observation.observed_at),
                        latency_micros: observation.latency_micros,
                        outcome: observation.outcome.as_proto(),
                        detail: observation.detail.clone(),
                    })
                    .collect(),
            })
            .collect();
        crate::proto::GetSandboxHealthResponse {
            sandbox_id: sandbox_id.to_string(),
            status: status.as_proto(),
            evaluated_at: self.health_observed_at.and_then(system_time_to_timestamp),
            degraded_components,
            components,
        }
    }

    fn component_is_degraded(&self, component: HealthComponent) -> bool {
        self.health_components.get(component).degraded
    }

    fn set_component_degraded(&mut self, component: HealthComponent, degraded: bool) {
        self.health_components.get_mut(component).degraded = degraded;
    }

    pub(crate) fn cached_firecracker_state(&self) -> Option<VmState> {
        if self
            .health_observed_at
            .and_then(|observed| observed.elapsed().ok())
            .is_none_or(|age| age > HEALTH_STALE_AFTER)
        {
            return None;
        }
        self.health_components
            .firecracker
            .observations
            .iter()
            .rev()
            .find(|observation| observation.lifecycle_epoch == self.lifecycle_epoch)
            .and_then(|observation| observation.firecracker_state)
    }
}

async fn observe_authoritative_net_gateway_process(
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    sidecars: &Arc<Mutex<HashMap<String, SidecarInfo>>>,
    sandbox_id: &str,
    observed_committed_pid: Option<u32>,
    observed_live_pid: Option<u32>,
) -> bool {
    // Replacement commits take these locks in the same order. Rechecking the
    // current sidecar authority while both are held prevents a delayed sweep
    // or removal probe from restoring the previous PID's stats fence.
    let mut health = state.lock().await;
    let sidecars = sidecars.lock().await;
    let Some(current) = sidecars.get(sandbox_id) else {
        return false;
    };
    if current.netgtw_pid != observed_committed_pid {
        return false;
    }
    let Some(health) = health.get_mut(sandbox_id) else {
        return false;
    };
    health.netgtw_alive = health.runtime_alive && observed_live_pid.is_some();
    health.observe_net_gateway_process(observed_live_pid);
    true
}

fn system_time_to_timestamp(value: SystemTime) -> Option<prost_types::Timestamp> {
    let duration = value.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    Some(prost_types::Timestamp {
        seconds: i64::try_from(duration.as_secs()).ok()?,
        nanos: i32::try_from(duration.subsec_nanos()).ok()?,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the loop receives each long-lived watchdog dependency from the constructor"
)]
async fn sidecar_loop(
    state: Arc<Mutex<HashMap<String, SandboxHealth>>>,
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    db: crate::db::SharedConnection,
    network_policy_reconcile_queue: NetworkPolicyReconcileQueue,
    expected_route_verification_key_generation: Arc<AtomicU64>,
    feature_flags: pplx_space_feature_flags::FeatureFlagClient,
    shutdown_token: CancellationToken,
    node_manager: NodeManagerConnector,
) {
    let mut interval = tokio::time::interval(SIDECAR_CHECK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut tick = 0_u64;
    let mut published_net_gateway_buckets: HashSet<(String, NetGatewayLifecycle)> = HashSet::new();
    let mut published_net_gateway_inventory_buckets = HashSet::new();
    let mut restart_queue = SidecarRestartQueue::default();
    let mut restart_futures = FuturesUnordered::<SidecarRestartFuture>::new();
    loop {
        let run_sweep = tokio::select! {
            _ = shutdown_token.cancelled() => return,
            _ = interval.tick() => true,
            outcome = restart_futures.next(), if !restart_futures.is_empty() => {
                if let Some(outcome) = outcome {
                    finish_sidecar_restart(&mut restart_queue, outcome);
                }
                fill_sidecar_restart_slots(
                    &mut restart_queue,
                    &mut restart_futures,
                    &sidecars,
                    &db,
                    &network_policy_reconcile_queue,
                    &state,
                    &feature_flags,
                    &node_manager,
                )
                .await;
                false
            }
        };
        if !run_sweep {
            continue;
        }
        tick = tick.wrapping_add(1);

        // Snapshot sidecar info for iteration
        let sidecar_snapshot: Vec<SidecarInfo> = {
            let map = sidecars.lock().await;
            // Drop queued work as soon as its sandbox disappears, its
            // authority changes, or another actor finishes the repair. This
            // keeps the FIFO bounded by current work and lets a fresh request
            // for the same sidecar replace a stale queued generation.
            restart_queue.retain_pending(|request| {
                map.get(request.sandbox_id())
                    .is_some_and(|current| request.still_has_authority(current))
            });
            map.values().cloned().collect()
        };
        let mut net_gateway_stats_pending = tokio::task::JoinSet::new();
        for info in &sidecar_snapshot {
            let id = &info.sandbox_id;

            let runtime_alive = runtime_alive(id);

            // Check sidecar PID symlinks. These only count as alive while the VM
            // itself is still alive; otherwise the sidecars are not useful even
            // if a stale PID symlink exists.
            let spaced_live_pid = sidecar_live_pid(id, "space-spaced-host-svc");
            let netgtw_live_pid = sidecar_live_pid(id, "space-netgtw-svc");
            // A replacement is usable only after its exact PID is committed to
            // watchdog/DB authority. A launch that succeeded before a later
            // configure or persist failure is retried and adopted instead of
            // being mistaken for a healthy completed restart forever.
            let spaced_alive =
                runtime_alive && sidecar_pid_is_committed(info.spaced_host_pid, spaced_live_pid);
            let netgtw_alive =
                runtime_alive && sidecar_pid_is_committed(info.netgtw_pid, netgtw_live_pid);

            let observed_netgtw_pid = if netgtw_alive { netgtw_live_pid } else { None };
            {
                let mut map = state.lock().await;
                if let Some(health) = map.get_mut(id.as_str()) {
                    health.runtime_alive = runtime_alive;
                    if !runtime_alive {
                        // The composed health loop owns normal updates for this
                        // observability-only bit.
                        health.vsock_raw_ping_ok = false;
                    }
                    health.spaced_host_alive = spaced_alive;
                }
            }
            let netgtw_observation_is_authoritative = observe_authoritative_net_gateway_process(
                &state,
                &sidecars,
                id,
                info.netgtw_pid,
                observed_netgtw_pid,
            )
            .await;

            // If FC is not alive, no point checking sidecars or the guest bridge.
            if !runtime_alive {
                continue;
            }

            if let Some(netgtw_pid) = observed_netgtw_pid
                && netgtw_observation_is_authoritative
                && should_run_staggered_probe(
                    tick,
                    id,
                    NET_GATEWAY_STATS_PROBE_SEED,
                    NET_GATEWAY_STATS_TICK_STRIDE,
                )
            {
                spawn_net_gateway_stats_probe(
                    &mut net_gateway_stats_pending,
                    id.clone(),
                    netgtw_pid,
                    state.clone(),
                    db.clone(),
                )
                .await;
            }

            // Restart dead sidecars and finish partially committed replacements.
            if info.guest_image.expects_agent()
                && !spaced_alive
                && info.uid.is_some()
                && info.vsock_uds_path.is_some()
                && restart_queue.schedule(SidecarRestartRequest::SpacedHost(info.clone()))
            {
                tracing::warn!(
                    sandbox_id = %id,
                    live_pid = ?spaced_live_pid,
                    committed_pid = ?info.spaced_host_pid,
                    "spaced-host missing or not committed; scheduling restart"
                );
            }
            if !netgtw_alive
                && info.uid.is_some()
                && restart_queue.schedule(SidecarRestartRequest::NetGateway(info.clone()))
            {
                tracing::warn!(
                    sandbox_id = %id,
                    live_pid = ?netgtw_live_pid,
                    committed_pid = ?info.netgtw_pid,
                    "net-gateway missing or not committed; scheduling restart"
                );
            }
        }

        fill_sidecar_restart_slots(
            &mut restart_queue,
            &mut restart_futures,
            &sidecars,
            &db,
            &network_policy_reconcile_queue,
            &state,
            &feature_flags,
            &node_manager,
        )
        .await;

        while !net_gateway_stats_pending.is_empty() {
            drain_net_gateway_stats_probe(&mut net_gateway_stats_pending).await;
        }

        let guest_health_rows = load_guest_health_rows(&db);
        if let Some(rows) = guest_health_rows.as_deref() {
            emit_health_gauges(&state, rows).await;
        }
        emit_auto_lifecycle_delete_gauges(&db);
        emit_net_gateway_live_gauges(
            &state,
            guest_health_rows.as_deref().unwrap_or_default(),
            &mut published_net_gateway_buckets,
            &mut published_net_gateway_inventory_buckets,
        )
        .await;
        emit_net_gateway_route_verification_gauges(
            &state,
            expected_route_verification_key_generation.load(Ordering::Acquire),
        )
        .await;
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "each restart task needs the sidecar loop's long-lived dependencies"
)]
async fn fill_sidecar_restart_slots(
    queue: &mut SidecarRestartQueue,
    futures: &mut FuturesUnordered<SidecarRestartFuture>,
    sidecars: &Arc<Mutex<HashMap<String, SidecarInfo>>>,
    db: &crate::db::SharedConnection,
    network_policy_reconcile_queue: &NetworkPolicyReconcileQueue,
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
    node_manager: &NodeManagerConnector,
) {
    while futures.len() < SIDECAR_RESTART_CONCURRENCY {
        let Some(request) = queue.pop_front() else {
            return;
        };
        let key = request.key();
        let current = {
            let current = sidecars.lock().await;
            current.get(request.sandbox_id()).cloned()
        };
        // `/run` and `/proc` probes are synchronous filesystem operations.
        // Keep them outside the shared async mutex; the final commit path
        // rechecks authority after every fallible launch/configuration step.
        let should_run = current
            .as_ref()
            .is_some_and(|current| request.still_needs_restart(current));
        if !should_run {
            queue.complete(&key);
            continue;
        }

        let sandbox_id = request.sandbox_id().to_string();
        let operation = request.operation();
        let db = db.clone();
        let sidecars = Arc::clone(sidecars);
        let network_policy_reconcile_queue = network_policy_reconcile_queue.clone();
        let state = Arc::clone(state);
        let feature_flags = feature_flags.clone();
        let node_manager = node_manager.clone();
        let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            match request {
                SidecarRestartRequest::SpacedHost(info) => watchdog_timeout(
                    SIDECAR_RESTART_TIMEOUT,
                    operation,
                    restart_spaced_host(&info, db, sidecars, &node_manager),
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
                SidecarRestartRequest::NetGateway(info) => watchdog_timeout(
                    SIDECAR_RESTART_TIMEOUT,
                    operation,
                    restart_netgtw(
                        &info,
                        db,
                        sidecars,
                        network_policy_reconcile_queue,
                        &state,
                        &feature_flags,
                        &node_manager,
                    ),
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            }
        }));
        futures.push(
            async move {
                let result = task.await.unwrap_or_else(|error| {
                    Err(format!("restart task failed before completion: {error}"))
                });
                SidecarRestartOutcome {
                    key,
                    sandbox_id,
                    operation,
                    result,
                }
            }
            .boxed(),
        );
    }
}

fn finish_sidecar_restart(queue: &mut SidecarRestartQueue, outcome: SidecarRestartOutcome) {
    queue.complete(&outcome.key);
    if let Err(error) = outcome.result {
        tracing::error!(
            sandbox_id = outcome.sandbox_id,
            %error,
            operation = outcome.operation,
            "sidecar restart failed"
        );
    }
}

fn emit_auto_lifecycle_delete_gauges(db: &crate::db::SharedConnection) {
    use pplx_space_metrics::sandbox::worker::AutoLifecycleDeleteOutboxState;

    let (outbox, blocked) = {
        let db = match db.lock() {
            Ok(db) => db,
            Err(error) => {
                tracing::warn!(%error, "sandbox database mutex poisoned while emitting delete-receipt gauges");
                return;
            }
        };
        let outbox = match crate::db::read::counts(&db, crate::db::read::DeleteOutbox) {
            Ok(stats) => stats,
            Err(error) => {
                tracing::warn!(%error, "failed to inspect auto-lifecycle delete outbox gauges");
                return;
            }
        };
        let blocked = match crate::db::read::counts(&db, crate::db::read::DeleteBlocked) {
            Ok(stats) => stats,
            Err(error) => {
                tracing::warn!(%error, "failed to inspect auto-lifecycle delete block gauges");
                return;
            }
        };
        (outbox, blocked)
    };

    pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_outbox_depth(
        outbox.pending_count,
        AutoLifecycleDeleteOutboxState::Pending,
    );
    pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_outbox_depth(
        outbox.completed_count,
        AutoLifecycleDeleteOutboxState::Completed,
    );
    pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_outbox_oldest_age_secs(
        outbox.oldest_pending_age_secs,
        AutoLifecycleDeleteOutboxState::Pending,
    );
    pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_outbox_oldest_age_secs(
        outbox.oldest_completed_age_secs,
        AutoLifecycleDeleteOutboxState::Completed,
    );
    pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_outbox_max_attempt_count(
        outbox.max_attempt_count,
    );
    for reason in crate::auto_lifecycle_delete_receipts::AutoLifecycleDeleteBlockReason::ALL {
        let stats = blocked.iter().find(|stats| stats.reason == reason);
        pplx_space_metrics::sandbox::worker::auto_lifecycle_delete_blocked(
            stats.map_or(0, |stats| stats.count),
            stats.map_or(0, |stats| stats.oldest_age_secs),
            reason.metric_reason(),
        );
    }
}

fn load_guest_health_rows(db: &crate::db::SharedConnection) -> Option<Vec<SandboxGuestHealthRow>> {
    let db = match db.lock() {
        Ok(db) => db,
        Err(error) => {
            tracing::warn!(%error, "sandbox database mutex poisoned while loading sandbox lifecycle gauges");
            return None;
        }
    };
    match crate::db::scan::all_sandboxes(&db, crate::db::scan::GuestHealthRows) {
        Ok(rows) => Some(rows),
        Err(error) => {
            tracing::warn!(%error, "failed to list sandboxes for lifecycle gauges");
            None
        }
    }
}

async fn emit_health_gauges(
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    rows: &[SandboxGuestHealthRow],
) {
    let health_snapshot = {
        let state = state.lock().await;
        state
            .iter()
            .map(|(sandbox_id, health)| (sandbox_id.clone(), HealthGaugeSnapshot::from(health)))
            .collect()
    };
    let guest_counts =
        guest_health_gauge_counts(rows, &health_snapshot, guest_health_probe_from_processes);
    let composed_counts = composed_health_gauge_counts(rows, &health_snapshot);
    for value in guest_health_gauge_values(&guest_counts) {
        pplx_space_metrics::sandbox::worker::guest_health(
            value.count,
            value.lifecycle,
            value.health,
        );
    }
    for lifecycle in COMPOSED_HEALTH_LIFECYCLES {
        for status in [
            ComposedHealthStatus::Unspecified,
            ComposedHealthStatus::Initializing,
            ComposedHealthStatus::Healthy,
            ComposedHealthStatus::Degraded,
        ] {
            pplx_space_metrics::sandbox::worker::sandbox_health_status(
                composed_counts
                    .get(&(lifecycle.as_str(), status))
                    .copied()
                    .unwrap_or_default(),
                lifecycle.as_str(),
                status.as_metric(),
            );
        }
    }
}

fn composed_health_gauge_counts(
    rows: &[SandboxGuestHealthRow],
    state: &HashMap<String, HealthGaugeSnapshot>,
) -> HashMap<(&'static str, ComposedHealthStatus), usize> {
    let mut counts = HashMap::<(&'static str, ComposedHealthStatus), usize>::new();
    for row in rows {
        if !COMPOSED_HEALTH_LIFECYCLES.contains(&row.state) {
            continue;
        }
        let status = state
            .get(&row.id)
            .map_or(ComposedHealthStatus::Unspecified, |snapshot| {
                snapshot.composed
            });
        *counts.entry((row.state.as_str(), status)).or_default() += 1;
    }
    counts
}

/// Count live net-gateways by the build sha each one reports and the sandbox's
/// current lifecycle, then emit one gauge per bucket. Detection only — nothing
/// here restarts anything; cycling gateways is an explicit operator action
/// (`space-sbxmgr cycle-net-gateway`).
///
/// `published` retains only the previous sample's buckets. A retired bucket
/// receives zero once, then is forgotten instead of accumulating build SHAs.
/// The higher-cardinality inventory remembers only the prior sweep, emits one
/// final zero for drained buckets, then drops them from process memory.
async fn emit_net_gateway_live_gauges(
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    rows: &[SandboxGuestHealthRow],
    published: &mut HashSet<(String, NetGatewayLifecycle)>,
    published_inventory: &mut HashSet<NetGatewayInventoryBucket>,
) {
    let rows_by_id = rows
        .iter()
        .map(|row| (row.id.as_str(), row))
        .collect::<HashMap<_, _>>();
    let (live_counts, inventory_counts) = {
        let map = state.lock().await;
        let live_counts = net_gateway_live_counts(map.iter().map(|(sandbox_id, health)| {
            (
                health,
                rows_by_id
                    .get(sandbox_id.as_str())
                    .map_or(NetGatewayLifecycle::Unknown, |row| {
                        NetGatewayLifecycle::Known(row.state)
                    }),
            )
        }));
        let inventory_counts =
            net_gateway_inventory_counts(map.iter().map(|(sandbox_id, health)| {
                (health, rows_by_id.get(sandbox_id.as_str()).copied())
            }));
        (live_counts, inventory_counts)
    };
    published.extend(live_counts.keys().cloned());
    for bucket in published.iter() {
        pplx_space_metrics::sandbox::worker::net_gateway_live(
            live_counts.get(bucket).copied().unwrap_or(0),
            &bucket.0,
            bucket.1.as_str(),
        );
    }
    published.retain(|bucket| live_counts.contains_key(bucket));

    let current_inventory = inventory_counts.keys().cloned().collect::<HashSet<_>>();
    for bucket in published_inventory.difference(&current_inventory) {
        emit_net_gateway_inventory_gauge(0, bucket);
    }
    for (bucket, count) in &inventory_counts {
        emit_net_gateway_inventory_gauge(*count, bucket);
    }
    *published_inventory = current_inventory;
}

/// Live gateways per reported build sha and current sandbox lifecycle. Dead
/// gateways are excluded: the watchdog restarts them and the new process
/// reports anew. A gateway that has not been probed yet, or that runs a binary
/// predating sha reporting, counts under `NET_GATEWAY_SHA_UNKNOWN`.
fn net_gateway_live_counts<'health>(
    healths: impl Iterator<Item = (&'health SandboxHealth, NetGatewayLifecycle)>,
) -> HashMap<(String, NetGatewayLifecycle), usize> {
    let mut counts = HashMap::new();
    for (health, lifecycle) in healths {
        if !health.netgtw_alive {
            continue;
        }
        let gateway_sha = health
            .netgtw_git_sha
            .as_deref()
            .filter(|sha| !sha.is_empty())
            .unwrap_or(pplx_space_metrics::sandbox::worker::NET_GATEWAY_SHA_UNKNOWN);
        *counts
            .entry((gateway_sha.to_string(), lifecycle))
            .or_default() += 1;
    }
    counts
}

fn emit_net_gateway_inventory_gauge(value: usize, bucket: &NetGatewayInventoryBucket) {
    pplx_space_metrics::sandbox::worker::net_gateway_inventory(
        value,
        bucket.tls_interception_mode,
        &bucket.gateway_sha,
        &bucket.template_content_hash,
        bucket.lifecycle.as_str(),
    );
}

/// Count every live gateway once. Missing gateway or sandbox-row metadata is
/// retained under explicit unknown values rather than excluding the process.
fn net_gateway_inventory_counts<'health, 'row>(
    healths: impl Iterator<Item = (&'health SandboxHealth, Option<&'row SandboxGuestHealthRow>)>,
) -> HashMap<NetGatewayInventoryBucket, usize> {
    let mut counts = HashMap::new();
    for (health, row) in healths {
        if !health.netgtw_alive {
            continue;
        }
        let gateway_sha = health
            .netgtw_git_sha
            .as_deref()
            .filter(|sha| !sha.is_empty())
            .unwrap_or(pplx_space_metrics::sandbox::worker::NET_GATEWAY_SHA_UNKNOWN);
        let template_content_hash = row
            .and_then(|row| row.template_content_hash.as_deref())
            .filter(|hash| !hash.is_empty())
            .unwrap_or(pplx_space_metrics::sandbox::worker::NET_GATEWAY_INVENTORY_UNKNOWN);
        let lifecycle = row.map_or(NetGatewayLifecycle::Unknown, |row| {
            NetGatewayLifecycle::Known(row.state)
        });
        let bucket = NetGatewayInventoryBucket {
            tls_interception_mode: health.netgtw_tls_interception_mode,
            gateway_sha: gateway_sha.to_string(),
            template_content_hash: template_content_hash.to_string(),
            lifecycle,
        };
        *counts.entry(bucket).or_default() += 1;
    }
    counts
}

fn net_gateway_route_verification_status(
    health: &mut SandboxHealth,
    expected_generation: u64,
    now: Instant,
) -> NetGatewayRouteVerificationStatus {
    health
        .netgtw_route_verification_window
        .observe_expected_generation(expected_generation, now);
    if expected_generation == 0 {
        return NetGatewayRouteVerificationStatus::Disabled;
    }
    let (Some(identity_profile_version), Some(applied_generation)) = (
        health.netgtw_identity_profile_version,
        health.netgtw_route_verification_key_generation,
    ) else {
        return if health.netgtw_route_verification_window.is_within_grace(now) {
            NetGatewayRouteVerificationStatus::Probing
        } else {
            NetGatewayRouteVerificationStatus::Unknown
        };
    };
    if identity_profile_version
        < pplx_space_net_gateway_client::MIN_IDENTITY_PROFILE_VERSION_FOR_VERIFICATION_KEYS
    {
        return NetGatewayRouteVerificationStatus::Unsupported;
    }
    if applied_generation == expected_generation {
        NetGatewayRouteVerificationStatus::Matched
    } else if health.netgtw_route_verification_window.is_within_grace(now) {
        NetGatewayRouteVerificationStatus::Converging
    } else {
        NetGatewayRouteVerificationStatus::Mismatched
    }
}

fn net_gateway_route_verification_counts<'health>(
    healths: impl Iterator<Item = &'health mut SandboxHealth>,
    expected_generation: u64,
    now: Instant,
) -> HashMap<NetGatewayRouteVerificationStatus, usize> {
    let mut counts = HashMap::new();
    for health in healths {
        if !health.netgtw_alive {
            continue;
        }
        *counts
            .entry(net_gateway_route_verification_status(
                health,
                expected_generation,
                now,
            ))
            .or_default() += 1;
    }
    counts
}

async fn emit_net_gateway_route_verification_gauges(
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    expected_generation: u64,
) {
    let counts = {
        let mut map = state.lock().await;
        net_gateway_route_verification_counts(map.values_mut(), expected_generation, Instant::now())
    };
    for status in [
        NetGatewayRouteVerificationStatus::Disabled,
        NetGatewayRouteVerificationStatus::Matched,
        NetGatewayRouteVerificationStatus::Probing,
        NetGatewayRouteVerificationStatus::Converging,
        NetGatewayRouteVerificationStatus::Mismatched,
        NetGatewayRouteVerificationStatus::Unsupported,
        NetGatewayRouteVerificationStatus::Unknown,
    ] {
        pplx_space_metrics::sandbox::worker::net_gateway_route_verification(
            counts.get(&status).copied().unwrap_or_default(),
            status,
        );
    }
}

fn sandbox_states_for_ids(
    db: &crate::db::SharedConnection,
    sandbox_ids: &[String],
) -> Option<HashMap<String, crate::state::SandboxState>> {
    let db = match db.lock() {
        Ok(db) => db,
        Err(error) => {
            tracing::warn!(%error, "sandbox database mutex poisoned while loading watchdog probe states");
            return None;
        }
    };
    let labels = match crate::db::read::sandbox_views(&db, sandbox_ids).state_labels() {
        Ok(labels) => labels,
        Err(error) => {
            tracing::warn!(%error, "failed to prepare watchdog probe-state lookup");
            return None;
        }
    };
    let mut states = HashMap::with_capacity(sandbox_ids.len());
    for (sandbox_id, label) in labels {
        let state = match label {
            Ok(Some(state)) => state,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(sandbox_id, %error, "failed to load watchdog probe state");
                continue;
            }
        };
        let Ok(state) = state.parse::<crate::state::SandboxState>() else {
            tracing::warn!(sandbox_id, state, "watchdog found an unknown sandbox state");
            continue;
        };
        states.insert(sandbox_id.clone(), state);
    }
    Some(states)
}

fn guest_health_gauge_counts(
    rows: &[SandboxGuestHealthRow],
    state: &HashMap<String, HealthGaugeSnapshot>,
    probe_missing_health: impl Fn(&str) -> GuestHealthProbe,
) -> HashMap<(&'static str, GuestHealthCategory), usize> {
    let mut counts = HashMap::new();
    for row in rows {
        if !lifecycle_has_guest_health_metric(row.state) {
            continue;
        }
        let probe = state
            .get(&row.id)
            .map(|snapshot| snapshot.guest)
            .unwrap_or_else(|| probe_missing_health(&row.id));
        let category = classify_guest_health(probe);
        *counts.entry((row.state.as_str(), category)).or_insert(0) += 1;
    }
    counts
}

fn guest_health_gauge_values(
    counts: &HashMap<(&'static str, GuestHealthCategory), usize>,
) -> Vec<GuestHealthGaugeValue> {
    let mut values = Vec::with_capacity(GUEST_HEALTH_LIFECYCLES.len() * 3);
    for lifecycle in GUEST_HEALTH_LIFECYCLES {
        for health in GUEST_HEALTH_CATEGORIES {
            values.push(GuestHealthGaugeValue {
                lifecycle: lifecycle.as_str(),
                health: health.as_str(),
                count: counts
                    .get(&(lifecycle.as_str(), health))
                    .copied()
                    .unwrap_or_default(),
            });
        }
    }
    values
}

fn lifecycle_has_guest_health_metric(state: crate::state::SandboxState) -> bool {
    GUEST_HEALTH_LIFECYCLES.contains(&state)
}

fn guest_health_probe_from_processes(sandbox_id: &str) -> GuestHealthProbe {
    let runtime_alive = runtime_alive(sandbox_id);
    GuestHealthProbe {
        runtime_alive,
        guest_healthy: false,
        spaced_host_alive: runtime_alive && sidecar_alive(sandbox_id, "space-spaced-host-svc"),
        consecutive_guest_probe_failures: GUEST_HEALTH_FAILURE_THRESHOLD,
    }
}

fn classify_guest_health(probe: GuestHealthProbe) -> GuestHealthCategory {
    if !probe.runtime_alive {
        return GuestHealthCategory::Unreachable;
    }
    if !probe.guest_healthy
        || !probe.spaced_host_alive
        || probe.consecutive_guest_probe_failures >= GUEST_HEALTH_FAILURE_THRESHOLD
    {
        return GuestHealthCategory::Degraded;
    }
    GuestHealthCategory::Healthy
}

fn should_run_staggered_probe(
    tick: u64,
    sandbox_id: &str,
    schedule_seed: u64,
    stride: u64,
) -> bool {
    stride <= 1
        || tick
            .wrapping_add(stable_probe_bucket(sandbox_id, schedule_seed, stride))
            .is_multiple_of(stride)
}

fn stable_probe_bucket(sandbox_id: &str, schedule_seed: u64, stride: u64) -> u64 {
    if stride <= 1 {
        return 0;
    }

    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ schedule_seed;
    for byte in sandbox_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash % stride
}

async fn spawn_net_gateway_stats_probe(
    pending: &mut tokio::task::JoinSet<()>,
    sandbox_id: String,
    netgtw_pid: u32,
    state: Arc<Mutex<HashMap<String, SandboxHealth>>>,
    db: crate::db::SharedConnection,
) {
    if pending.len() >= NET_GATEWAY_STATS_CONCURRENCY {
        drain_net_gateway_stats_probe(pending).await;
    }

    pending.spawn(async move {
        probe_net_gateway_stats(&sandbox_id, netgtw_pid, &state, &db).await;
    });
}

async fn drain_net_gateway_stats_probe(pending: &mut tokio::task::JoinSet<()>) {
    if let Some(Err(error)) = pending.join_next().await {
        tracing::warn!(%error, "net-gateway stats probe task failed");
    }
}

async fn watchdog_timeout<T, E>(
    timeout: Duration,
    operation: &'static str,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, WatchdogTimeoutError<E>>
where
    E: fmt::Debug + fmt::Display,
{
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| WatchdogTimeoutError::Timeout { operation, timeout })?
        .map_err(WatchdogTimeoutError::Operation)
}

async fn probe_net_gateway_stats(
    sandbox_id: &str,
    netgtw_pid: u32,
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    db: &crate::db::SharedConnection,
) {
    // Every registered sandbox has the UUID id create assigned it; an id that
    // does not parse fails the probe as an absent gateway socket would.
    let kept_gateway = sandbox_id
        .parse::<SandboxId>()
        .map(|id| (id, LinuxPid::try_from_u32(netgtw_pid).ok()));
    let result = watchdog_timeout(
        NET_GATEWAY_BACKGROUND_STATS_TIMEOUT,
        "net-gateway stats probe",
        async {
            let (id, gateway_pid) = kept_gateway.clone().map_err(|error| error.to_string())?;
            let mut client = crate::net_gateway_clients::client(id, gateway_pid)
                .await
                .map_err(|error| error.to_string())?;
            let stats = client
                .get_stats(pplx_space_net_gateway_client::proto::GetStatsRequest {})
                .await
                .map_err(|error| error.to_string())?
                .into_inner();
            Ok::<_, String>(NetGatewayStatsObservation {
                network_tx_bytes: stats.network_tx_bytes,
                network_rx_bytes: stats.network_rx_bytes,
                build_git_sha: stats.build_git_sha,
                tls_interception_mode: net_gateway_tls_interception_mode(
                    stats.tls_interception_mode,
                ),
                identity_profile_version: stats.identity_profile_version,
                route_verification_key_generation: stats.route_verification_key_generation,
            })
        },
    )
    .await;

    let observation = match result {
        Ok(observation) => observation,
        Err(error) => {
            if let Ok((id, gateway_pid)) = kept_gateway {
                crate::net_gateway_clients::discard(id, gateway_pid);
            }
            tracing::debug!(
                sandbox_id = %sandbox_id,
                %error,
                "net-gateway stats probe failed"
            );
            return;
        }
    };

    // The per-sandbox UDS can be rebound while the RPC is in flight. This PID
    // fences the observation to unchanged sidecar authority; it is not proof
    // of which process answered the mutable endpoint.
    let Some((network_tx_bytes, network_rx_bytes)) = ({
        let mut map = state.lock().await;
        map.get_mut(sandbox_id)
            .and_then(|health| health.apply_net_gateway_stats(netgtw_pid, observation))
    }) else {
        tracing::debug!(
            sandbox_id = %sandbox_id,
            netgtw_pid,
            "discarding stats from a replaced net-gateway process"
        );
        return;
    };

    let db = match db.lock() {
        Ok(db) => db,
        Err(error) => {
            tracing::warn!(sandbox_id = %sandbox_id, %error, "sandbox database mutex poisoned while persisting network counters");
            return;
        }
    };
    if let Err(error) =
        crate::db::update_network_counters(&db, sandbox_id, network_tx_bytes, network_rx_bytes)
    {
        tracing::warn!(
            sandbox_id = %sandbox_id,
            %error,
            "failed to persist network counters"
        );
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the loop receives each long-lived watchdog dependency from the constructor"
)]
async fn composed_health_loop(
    state: Arc<Mutex<HashMap<String, SandboxHealth>>>,
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    db: crate::db::SharedConnection,
    snapshot_scheduler: Arc<SnapshotScheduler>,
    auto_lifecycle: AutoLifecycleCoordinator,
    lifecycle_epochs: HealthLifecycleEpochs,
    shutdown_token: CancellationToken,
    node_manager: NodeManagerConnector,
    restored_guests: crate::vm_diagnostics::RestoredGuests,
) {
    let mut interval = tokio::time::interval(HEALTH_SCHEDULER_TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Evidence for degrading transitions is gathered off the probe path by a
    // fixed set of collectors, so a burst of them can delay neither the next
    // sweep nor the transition lines. Dropping the set on return aborts them.
    let evidence_queue = Arc::new(diagnostics::admission::EvidenceQueue::new(
        diagnostics::admission::EVIDENCE_QUEUE_CAPACITY,
    ));
    let host_reads = diagnostics::host_reads();
    let mut evidence_collectors = tokio::task::JoinSet::new();
    for _ in 0..diagnostics::admission::EVIDENCE_COLLECTORS {
        evidence_collectors.spawn(diagnostics::run_evidence_collector(
            Arc::clone(&evidence_queue),
            Arc::clone(&sidecars),
            host_reads.clone(),
            shutdown_token.clone(),
        ));
    }
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        let now = Instant::now();
        let due_ids = {
            let mut map = state.lock().await;
            map.iter_mut()
                .filter_map(|(sandbox_id, health)| {
                    if health.next_health_probe_at > now {
                        return None;
                    }
                    let sequence = health.health_probe_sequence;
                    health.health_probe_sequence = sequence.wrapping_add(1);
                    health.next_health_probe_at =
                        now + next_health_probe_delay(sandbox_id, sequence);
                    Some((sandbox_id.clone(), health.lifecycle_epoch))
                })
                .collect::<Vec<_>>()
        };
        let due_sidecars = {
            let map = sidecars.lock().await;
            due_ids
                .into_iter()
                .filter_map(|(sandbox_id, lifecycle_epoch)| {
                    map.get(&sandbox_id)
                        .cloned()
                        .map(|info| (sandbox_id, lifecycle_epoch, info))
                })
                .collect::<Vec<_>>()
        };
        let due_ids = due_sidecars
            .iter()
            .map(|(sandbox_id, _, _)| sandbox_id.clone())
            .collect::<Vec<_>>();
        let Some(sandbox_states) = sandbox_states_for_ids(&db, &due_ids) else {
            let retry_at = Instant::now() + HEALTH_SCHEDULER_TICK;
            let mut map = state.lock().await;
            for sandbox_id in due_ids {
                if let Some(health) = map.get_mut(&sandbox_id) {
                    health.next_health_probe_at = retry_at;
                }
            }
            continue;
        };

        let mut pending = tokio::task::JoinSet::new();
        for (sandbox_id, lifecycle_epoch, info) in due_sidecars {
            let lifecycle = sandbox_states.get(&sandbox_id).copied();
            if !health_probe_lifecycle_is_eligible(lifecycle) {
                continue;
            }
            if pending.len() >= HEALTH_PROBE_CONCURRENCY
                && let Some(transition) = apply_and_reconcile_composed_health_result(
                    &state,
                    &db,
                    snapshot_scheduler.as_ref(),
                    &auto_lifecycle,
                    &lifecycle_epochs,
                    pending.join_next().await,
                )
                .await
            {
                diagnostics::report_transition(&evidence_queue, &transition);
            }
            let node_manager = node_manager.clone();
            let restored_guests = restored_guests.clone();
            pending.spawn(async move {
                let result = probe_composed_health(
                    &sandbox_id,
                    &info,
                    lifecycle,
                    &node_manager,
                    &restored_guests,
                )
                .await;
                (sandbox_id, lifecycle, lifecycle_epoch, result)
            });
        }
        while !pending.is_empty() {
            if let Some(transition) = apply_and_reconcile_composed_health_result(
                &state,
                &db,
                snapshot_scheduler.as_ref(),
                &auto_lifecycle,
                &lifecycle_epochs,
                pending.join_next().await,
            )
            .await
            {
                diagnostics::report_transition(&evidence_queue, &transition);
            }
        }
    }
}

fn health_probe_lifecycle_is_eligible(state: Option<crate::state::SandboxState>) -> bool {
    matches!(
        state,
        Some(
            crate::state::SandboxState::Started
                | crate::state::SandboxState::Paused
                | crate::state::SandboxState::Quarantined
        )
    )
}

fn initial_health_probe_delay(sandbox_id: &str) -> Duration {
    Duration::from_secs(stable_probe_bucket(
        sandbox_id,
        0x94d0_49bb_1331_11eb,
        HEALTH_PROBE_MIN_INTERVAL.as_secs() + HEALTH_PROBE_JITTER_RANGE_SECS,
    ))
}

fn next_health_probe_delay(sandbox_id: &str, sequence: u64) -> Duration {
    let schedule_seed = 0x517c_c1b7_2722_0a95_u64 ^ sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    HEALTH_PROBE_MIN_INTERVAL
        + Duration::from_secs(stable_probe_bucket(
            sandbox_id,
            schedule_seed,
            HEALTH_PROBE_JITTER_RANGE_SECS,
        ))
}

async fn probe_composed_health(
    sandbox_id: &str,
    info: &SidecarInfo,
    lifecycle: Option<crate::state::SandboxState>,
    node_manager: &NodeManagerConnector,
    restored_guests: &crate::vm_diagnostics::RestoredGuests,
) -> ComposedProbeResult {
    let firecracker = probe_firecracker_health(sandbox_id, lifecycle, node_manager);
    let spaced_host = async {
        if info.guest_image.expects_agent() {
            probe_spaced_host_health(sandbox_id).await
        } else {
            health_observation(
                Instant::now(),
                HealthProbeOutcome::NotApplicable,
                "guest image has no agent",
            )
        }
    };
    let spaced_guest = probe_spaced_guest_health(info, lifecycle, restored_guests);
    let net_gateway = probe_net_gateway_health(sandbox_id);
    let (firecracker, spaced_host, spaced_guest, net_gateway) =
        tokio::join!(firecracker, spaced_host, spaced_guest, net_gateway);
    let guest_status = if spaced_host.outcome == HealthProbeOutcome::Success {
        read_guest_status_via_spaced_host(sandbox_id).await
    } else {
        None
    };
    ComposedProbeResult {
        observations: [firecracker, spaced_host, spaced_guest, net_gateway],
        guest_status,
    }
}

async fn read_guest_status_via_spaced_host(
    sandbox_id: &str,
) -> Option<pplx_space_spaced_host_client::proto::GetStatusResponse> {
    tokio::time::timeout(SPACED_HOST_HEALTH_TIMEOUT, async {
        let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
            .with_socket_path(spaced_host_socket_path(sandbox_id));
        let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
            .await
            .ok()?;
        client.get_status().await.ok()
    })
    .await
    .ok()
    .flatten()
}

async fn probe_firecracker_health(
    sandbox_id: &str,
    lifecycle: Option<crate::state::SandboxState>,
    node_manager: &NodeManagerConnector,
) -> HealthObservation {
    let started = Instant::now();
    if !runtime_alive(sandbox_id) {
        return health_observation(
            started,
            HealthProbeOutcome::Unavailable,
            "Firecracker process is not present",
        );
    }
    let sandbox_id = match sandbox_id.parse::<SandboxId>() {
        Ok(sandbox_id) => sandbox_id,
        Err(error) => {
            return health_observation(started, HealthProbeOutcome::Unavailable, error.to_string());
        }
    };
    match crate::runtime_via_nodemgr::get_vm_state(sandbox_id, node_manager).await {
        Err(crate::runtime_via_nodemgr::RuntimeStateReadError::Timeout { .. }) => {
            health_observation(
                started,
                HealthProbeOutcome::Timeout,
                "Firecracker state request timed out",
            )
        }
        Err(error) => health_observation(
            started,
            HealthProbeOutcome::Unavailable,
            bounded_health_detail(error.to_string()),
        ),
        Ok(vm_state) => {
            let expected = match lifecycle {
                Some(crate::state::SandboxState::Started) => Some(VmState::Running),
                Some(crate::state::SandboxState::Paused) => Some(VmState::Paused),
                _ => None,
            };
            match expected {
                Some(expected) if expected != vm_state => firecracker_health_observation(
                    started,
                    HealthProbeOutcome::UnexpectedState,
                    format!("observed {vm_state}, expected {}", expected),
                    vm_state,
                ),
                _ => firecracker_health_observation(
                    started,
                    HealthProbeOutcome::Success,
                    vm_state.to_string(),
                    vm_state,
                ),
            }
        }
    }
}

async fn probe_spaced_host_health(sandbox_id: &str) -> HealthObservation {
    let started = Instant::now();
    let result = tokio::time::timeout(SPACED_HOST_HEALTH_TIMEOUT, async {
        let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
            .with_socket_path(spaced_host_socket_path(sandbox_id));
        let client = pplx_space_spaced_host_client::SpacedHostClient::new_with_config(config)
            .await
            .map_err(|error| error.to_string())?;
        client.ping().await.map_err(|error| error.to_string())
    })
    .await;
    observation_from_probe_result(started, result, "pong")
}

async fn probe_spaced_guest_health(
    info: &SidecarInfo,
    lifecycle: Option<crate::state::SandboxState>,
    restored_guests: &crate::vm_diagnostics::RestoredGuests,
) -> HealthObservation {
    let started = Instant::now();
    if !info.guest_image.expects_agent() || lifecycle == Some(crate::state::SandboxState::Paused) {
        return health_observation(
            started,
            HealthProbeOutcome::NotApplicable,
            if info.guest_image.expects_agent() {
                "guest vCPUs are paused"
            } else {
                "guest image has no SPACE guest agent"
            },
        );
    }
    let Some(vsock_uds_path) = info.vsock_uds_path.as_deref() else {
        return health_observation(
            started,
            HealthProbeOutcome::Unavailable,
            "v.sock path is absent",
        );
    };
    let (Some(runtime_pid), Some(uid)) = (info.runtime_pid, info.uid) else {
        return health_observation(
            started,
            HealthProbeOutcome::Unavailable,
            "Firecracker peer credentials are absent",
        );
    };
    let Ok(runtime_pid) = i32::try_from(runtime_pid) else {
        return health_observation(
            started,
            HealthProbeOutcome::Unavailable,
            "Firecracker pid is outside the peer-credential range",
        );
    };
    let result = tokio::time::timeout(SPACED_GUEST_HEALTH_TIMEOUT, async {
        let (channel, _) =
            pplx_space_spaced_common::connect_firecracker_vsock_channel_with_expected_peer(
                "spaced guest health",
                Path::new(vsock_uds_path),
                pplx_space_spaced_common::BRIDGE_GUEST_VSOCK_PORT,
                pplx_space_spaced_common::ExpectedUdsPeer::new(runtime_pid, uid),
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut client =
            pplx_space_spaced_common::proto::bridge_guest::meta_client::MetaClient::new(channel)
                .max_decoding_message_size(
                    pplx_space_spaced_common::BRIDGE_GUEST_PING_MAX_DECODE_BYTES,
                );
        // This probe is the one host caller that drains the guest's
        // diagnostics; spaced-host's own pings leave them in place.
        client
            .ping(pplx_space_spaced_common::proto::bridge_guest::PingRequest {
                drain_diagnostics: true,
            })
            .await
            .map_err(|error| error.to_string())
    })
    .await;
    if let Ok(Ok(response)) = &result {
        crate::vm_diagnostics::forward_guest_diagnostics(
            &info.sandbox_id,
            response.get_ref(),
            restored_guests,
        );
    }
    observation_from_probe_result(started, result, "pong")
}

// The probe dials afresh, unlike the gateway's kept clients: a dial is what
// shows the gateway still accepts connections, which a kept connection could
// keep answering past.
async fn probe_net_gateway_health(sandbox_id: &str) -> HealthObservation {
    let started = Instant::now();
    let result = tokio::time::timeout(NET_GATEWAY_HEALTH_TIMEOUT, async {
        let mut client = pplx_space_net_gateway_client::connect(sandbox_id)
            .await
            .map_err(|error| error.to_string())?;
        client
            .ping(pplx_space_net_gateway_client::proto::GatewayPingRequest {})
            .await
            .map_err(|error| error.to_string())
    })
    .await;
    observation_from_probe_result(started, result, "pong")
}

fn observation_from_probe_result<T>(
    started: Instant,
    result: Result<Result<T, String>, tokio::time::error::Elapsed>,
    success_detail: &'static str,
) -> HealthObservation {
    match result {
        Err(_) => health_observation(started, HealthProbeOutcome::Timeout, "probe timed out"),
        Ok(Err(error)) => health_observation(
            started,
            HealthProbeOutcome::Unavailable,
            bounded_health_detail(error),
        ),
        Ok(Ok(_)) => health_observation(started, HealthProbeOutcome::Success, success_detail),
    }
}

fn health_observation(
    started: Instant,
    outcome: HealthProbeOutcome,
    detail: impl Into<String>,
) -> HealthObservation {
    HealthObservation {
        observed_at: SystemTime::now(),
        latency_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        outcome,
        detail: bounded_health_detail(detail.into()),
        firecracker_state: None,
        lifecycle_epoch: 0,
    }
}

fn firecracker_health_observation(
    started: Instant,
    outcome: HealthProbeOutcome,
    detail: impl Into<String>,
    state: VmState,
) -> HealthObservation {
    HealthObservation {
        firecracker_state: Some(state),
        ..health_observation(started, outcome, detail)
    }
}

fn bounded_health_detail(mut detail: String) -> String {
    const MAX_DETAIL_CHARS: usize = 256;
    if detail.chars().count() > MAX_DETAIL_CHARS {
        detail = detail.chars().take(MAX_DETAIL_CHARS).collect();
    }
    detail
}

type CompletedHealthProbe = Result<
    (
        String,
        Option<crate::state::SandboxState>,
        u64,
        ComposedProbeResult,
    ),
    tokio::task::JoinError,
>;

fn update_legacy_health_fields(
    health: &mut SandboxHealth,
    component: HealthComponent,
    outcome: HealthProbeOutcome,
) {
    match component {
        HealthComponent::Firecracker
        | HealthComponent::SpacedHost
        | HealthComponent::NetGateway => {}
        HealthComponent::SpacedGuest => {
            if outcome != HealthProbeOutcome::NotApplicable {
                health.vsock_raw_ping_ok = outcome == HealthProbeOutcome::Success;
            }
            if outcome == HealthProbeOutcome::Success {
                health.guest_healthy = true;
                health.last_guest_heartbeat_at = Some(SystemTime::now());
                health.consecutive_guest_probe_failures = 0;
            } else if outcome == HealthProbeOutcome::NotApplicable {
                health.consecutive_guest_probe_failures = 0;
            } else {
                health.consecutive_guest_probe_failures =
                    health.consecutive_guest_probe_failures.saturating_add(1);
                if health.consecutive_guest_probe_failures >= GUEST_HEALTH_FAILURE_THRESHOLD {
                    health.guest_healthy = false;
                    health.guest_ready = false;
                }
            }
        }
    }
}

async fn apply_and_reconcile_composed_health_result(
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    db: &crate::db::SharedConnection,
    snapshot_scheduler: &SnapshotScheduler,
    auto_lifecycle: &AutoLifecycleCoordinator,
    lifecycle_epochs: &HealthLifecycleEpochs,
    result: Option<CompletedHealthProbe>,
) -> Option<diagnostics::HealthTransition> {
    let pause_failed_recovery = match result.as_ref() {
        Some(Ok((
            sandbox_id,
            Some(crate::state::SandboxState::Quarantined),
            lifecycle_epoch,
            _,
        ))) => sandbox_id
            .parse::<SandboxId>()
            .ok()
            .map(|id| (id, *lifecycle_epoch)),
        _ => None,
    };
    let transition = apply_composed_health_result(state, db, result).await;
    if let Some((sandbox_id, lifecycle_epoch)) = pause_failed_recovery {
        reconcile_pause_failed_quarantine(
            db,
            snapshot_scheduler,
            auto_lifecycle,
            lifecycle_epochs,
            &sandbox_id,
            lifecycle_epoch,
        )
        .await;
    }
    transition
}

async fn apply_composed_health_result(
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    db: &crate::db::SharedConnection,
    result: Option<CompletedHealthProbe>,
) -> Option<diagnostics::HealthTransition> {
    let Some(Ok((sandbox_id, lifecycle, lifecycle_epoch, result))) = result else {
        return None;
    };
    let ComposedProbeResult {
        observations,
        guest_status,
    } = result;
    let mut map = state.lock().await;
    let health = map.get_mut(&sandbox_id)?;
    if health.lifecycle_epoch != lifecycle_epoch {
        tracing::debug!(
            sandbox_id,
            probe_epoch = lifecycle_epoch,
            current_epoch = health.lifecycle_epoch,
            "discarded health result from an older lifecycle epoch"
        );
        return None;
    }
    let previous_status = health.health_status;
    let previous_guest_health = classify_guest_health(GuestHealthProbe::from(&*health));
    for (component, mut observation) in HealthComponent::ALL.into_iter().zip(observations) {
        observation.lifecycle_epoch = lifecycle_epoch;
        let outcome = observation.outcome;
        pplx_space_metrics::sandbox::worker::sandbox_health_probe(
            component.as_metric(),
            outcome.as_metric(),
            observation.latency_micros,
        );
        health.health_components.push(component, observation);
        update_component_classification(health, component, lifecycle);
        update_legacy_health_fields(health, component, outcome);
    }
    health.health_observed_at = Some(SystemTime::now());
    let mut guest_uptime = None;
    if let Some(status) = guest_status {
        if let Some(uptime) = status.guest_uptime {
            let uptime_secs = uptime.seconds.max(0).cast_unsigned();
            health.guest_uptime_secs = uptime_secs;
            guest_uptime = Some(uptime_secs);
        }
        health.guest_ready = status.ready;
        if status.ready {
            health.last_guest_ready_at = Some(SystemTime::now());
        }
        if let Some(version) = status.guest_version {
            match SemVer::parse(&version.version) {
                Ok(version) => health.spaced_version = Some(version),
                Err(error) => tracing::warn!(
                    sandbox_id,
                    %error,
                    "guest reported invalid spaced version"
                ),
            }
        }
    }
    health.health_status = classify_composed_health(health, lifecycle);
    if health.health_status != previous_status {
        pplx_space_metrics::sandbox::worker::sandbox_health_transition(
            health.health_status.as_metric(),
        );
    }
    let transition = diagnostics::HealthTransition::observe(
        &sandbox_id,
        lifecycle,
        previous_status,
        previous_guest_health,
        health,
        Instant::now(),
    );
    drop(map);
    if let Some(uptime_secs) = guest_uptime {
        match db.lock() {
            Ok(db) => {
                if let Err(error) = crate::db::update_guest_uptime(&db, &sandbox_id, uptime_secs) {
                    tracing::warn!(sandbox_id, %error, "failed to persist guest uptime");
                }
            }
            Err(error) => {
                tracing::warn!(sandbox_id, %error, "sandbox database mutex poisoned while persisting watchdog state");
            }
        }
    }
    transition
}

async fn reconcile_pause_failed_quarantine(
    db: &crate::db::SharedConnection,
    snapshot_scheduler: &SnapshotScheduler,
    auto_lifecycle: &AutoLifecycleCoordinator,
    lifecycle_epochs: &HealthLifecycleEpochs,
    sandbox_id: &SandboxId,
    lifecycle_epoch: u64,
) {
    let sandbox_id_string = sandbox_id.to_string();
    let artifact_lock = auto_lifecycle.artifact_lock(&sandbox_id_string);
    let _artifact_guard = artifact_lock.lock().await;
    let target = {
        let map = lifecycle_epochs.state.lock().await;
        let Some(health) = map.get(&sandbox_id_string) else {
            return;
        };
        if health.lifecycle_epoch != lifecycle_epoch {
            return;
        }
        pause_failed_recovery_target(health, lifecycle_epoch)
    };
    let Some(target) = target else {
        return;
    };
    let recovered = match db.lock() {
        Ok(db) => match crate::db::recover_pause_failed_quarantine(&db, sandbox_id, target) {
            Ok(recovered) => recovered,
            Err(error) => {
                tracing::warn!(sandbox_id = %sandbox_id, %error, "failed to recover pause-failed quarantine");
                false
            }
        },
        Err(error) => {
            tracing::warn!(sandbox_id = %sandbox_id, %error, "sandbox database mutex poisoned during pause-failed recovery");
            false
        }
    };
    if recovered {
        lifecycle_epochs.record_edge(&sandbox_id_string).await;
        if target == crate::db::PauseFailedRecoveryTarget::Paused {
            snapshot_scheduler.remove_sandbox(&sandbox_id_string).await;
        }
        mark_sandbox_dirty(db, &sandbox_id_string);
        tracing::info!(
            sandbox_id = %sandbox_id,
            recovered_state = %target.sandbox_state(),
            "recovered pause-failed quarantine from repeated Firecracker evidence"
        );
    }
}

fn pause_failed_recovery_target(
    health: &SandboxHealth,
    lifecycle_epoch: u64,
) -> Option<crate::db::PauseFailedRecoveryTarget> {
    let mut observations = health
        .health_components
        .firecracker
        .observations
        .iter()
        .rev()
        .filter(|observation| observation.lifecycle_epoch == lifecycle_epoch)
        .take(HEALTH_SUCCESS_THRESHOLD);
    let first = observations.next()?;
    if first.outcome != HealthProbeOutcome::Success {
        return None;
    }
    let firecracker_state = first.firecracker_state?;
    let mut count = 1;
    for observation in observations {
        if observation.outcome != HealthProbeOutcome::Success
            || observation.firecracker_state != Some(firecracker_state)
        {
            return None;
        }
        count += 1;
    }
    if count != HEALTH_SUCCESS_THRESHOLD {
        return None;
    }
    match firecracker_state {
        VmState::Running => Some(crate::db::PauseFailedRecoveryTarget::Started),
        VmState::Paused => Some(crate::db::PauseFailedRecoveryTarget::Paused),
        VmState::NotStarted | VmState::Shutdown => None,
    }
}

fn update_component_classification(
    health: &mut SandboxHealth,
    component: HealthComponent,
    lifecycle: Option<crate::state::SandboxState>,
) {
    if component == HealthComponent::SpacedGuest
        && lifecycle == Some(crate::state::SandboxState::Paused)
    {
        health.set_component_degraded(component, false);
        return;
    }
    let applicable = health
        .health_components
        .get(component)
        .observations
        .iter()
        .rev()
        .filter(|sample| sample.lifecycle_epoch == health.lifecycle_epoch)
        .take_while(|sample| sample.outcome != HealthProbeOutcome::NotApplicable)
        .take(HEALTH_FAILURE_THRESHOLD.max(HEALTH_SUCCESS_THRESHOLD))
        .collect::<Vec<_>>();
    if applicable.len() >= HEALTH_FAILURE_THRESHOLD
        && applicable
            .iter()
            .take(HEALTH_FAILURE_THRESHOLD)
            .all(|sample| !sample.outcome.is_success())
    {
        health.set_component_degraded(component, true);
    } else if applicable.len() >= HEALTH_SUCCESS_THRESHOLD
        && applicable
            .iter()
            .take(HEALTH_SUCCESS_THRESHOLD)
            .all(|sample| sample.outcome.is_success())
    {
        health.set_component_degraded(component, false);
    }
}

fn classify_composed_health(
    health: &SandboxHealth,
    lifecycle: Option<crate::state::SandboxState>,
) -> ComposedHealthStatus {
    if health
        .health_observed_at
        .and_then(|observed| observed.elapsed().ok())
        .is_none_or(|age| age > HEALTH_STALE_AFTER)
    {
        return ComposedHealthStatus::Initializing;
    }
    let required = HealthComponent::ALL.into_iter().filter(|component| {
        !(*component == HealthComponent::SpacedGuest
            && lifecycle == Some(crate::state::SandboxState::Paused))
    });
    let mut insufficient_current_evidence = false;
    for component in required {
        let mut current_samples = health
            .health_components
            .get(component)
            .observations
            .iter()
            .rev()
            .filter(|sample| sample.lifecycle_epoch == health.lifecycle_epoch)
            .peekable();
        if matches!(
            component,
            HealthComponent::SpacedHost | HealthComponent::SpacedGuest
        ) && current_samples
            .peek()
            .is_some_and(|sample| sample.outcome == HealthProbeOutcome::NotApplicable)
        {
            continue;
        }
        let applicable_samples = current_samples
            .take_while(|sample| sample.outcome != HealthProbeOutcome::NotApplicable)
            .count();
        if applicable_samples < HEALTH_FAILURE_THRESHOLD {
            insufficient_current_evidence = true;
        }
        if health.component_is_degraded(component) {
            return ComposedHealthStatus::Degraded;
        }
    }
    if insufficient_current_evidence {
        health.health_status
    } else {
        ComposedHealthStatus::Healthy
    }
}

async fn restart_spaced_host(
    info: &SidecarInfo,
    db: crate::db::SharedConnection,
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    node_manager: &NodeManagerConnector,
) -> Result<(), SpacedHostRestartError> {
    let mut node_client = node_manager.connect().await?;

    let id = &info.sandbox_id;
    let uid = info.uid.ok_or(SpacedHostRestartError::MissingUid)?;
    let vsock_uds_path = info
        .vsock_uds_path
        .clone()
        .ok_or(SpacedHostRestartError::MissingVsockUdsPath)?;

    let resp = node_client
        .launch(pplx_space_node_mgr_client::proto::LaunchRequest {
            execution_fence: info.node_execution_id.map(|id| {
                pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(id, None).into()
            }),
            sandbox_id: id.clone(),
            program: pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN.to_string(),
            args: spaced_host_restart_args(id, vsock_uds_path),
            uid,
            gid: uid,
            env: crate::sidecar_env::telemetry_env_for_service("space-spaced-host"),
        })
        .await
        .map_err(SpacedHostRestartError::Launch)?;
    let pid = resp.get_ref().pid;

    let config =
        pplx_space_spaced_host_client::SpacedHostClientConfig::default().with_socket_path(format!(
            "{RUN_DIR}/{id}/{}",
            pplx_space_common_core::host::linux::paths::SPACED_HOST_SOCKET_FILE
        ));
    let spaced_host =
        pplx_space_spaced_host_client::SpacedHostClient::new_with_config_and_expected_peer(
            config,
            pplx_space_spaced_common::ExpectedUdsPeer::new(
                i32::try_from(pid).map_err(|_| SpacedHostRestartError::PidOutOfRange(pid))?,
                uid,
            ),
        )
        .await
        .map_err(SpacedHostRestartError::SpacedHostConnect)?;
    spaced_host
        .configure_process_env(info.process_env.clone().into_iter().collect())
        .await
        .map_err(SpacedHostRestartError::Configure)?;

    {
        let mut sidecars = sidecars.lock().await;
        let current = sidecars
            .get_mut(id)
            .ok_or(SpacedHostRestartError::SandboxLeftWatchdog)?;
        if current.uid != Some(uid)
            || (current.spaced_host_pid != info.spaced_host_pid
                && current.spaced_host_pid != Some(pid))
        {
            return Err(SpacedHostRestartError::AuthorityChanged);
        }
        if current.vsock_uds_path != info.vsock_uds_path || current.process_env != info.process_env
        {
            return Err(SpacedHostRestartError::AuthorityChanged);
        }
        let conn = db
            .lock()
            .map_err(|_| SpacedHostRestartError::DatabaseMutexPoisoned)?;
        crate::db::update_spaced_host_pid(&conn, id, uid, info.spaced_host_pid, pid)
            .map_err(SpacedHostRestartError::Persist)?;
        current.spaced_host_pid = Some(pid);
    }

    tracing::info!(
        sandbox_id = %id,
        pid,
        "spaced-host restarted"
    );

    Ok(())
}

fn spaced_host_restart_args(sandbox_id: &str, vsock_uds_path: String) -> Vec<String> {
    vec![
        "--socket".to_string(),
        pplx_space_common_core::host::linux::paths::spaced_host_socket(sandbox_id),
        "--sandbox-id".to_string(),
        sandbox_id.to_string(),
        "--vsock-uds".to_string(),
        vsock_uds_path,
        "--host-control-socket".to_string(),
        pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET.to_string(),
        "--boot-type".to_string(),
        "resume".to_string(),
    ]
}

async fn restart_netgtw(
    info: &SidecarInfo,
    db: crate::db::SharedConnection,
    sidecars: Arc<Mutex<HashMap<String, SidecarInfo>>>,
    network_policy_reconcile_queue: NetworkPolicyReconcileQueue,
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
    node_manager: &NodeManagerConnector,
) -> Result<(), String> {
    relaunch_netgtw(
        info,
        &db,
        &sidecars,
        &network_policy_reconcile_queue,
        state,
        feature_flags,
        node_manager,
    )
    .await?;
    Ok(())
}

/// Launch (or relaunch) the net-gateway sidecar for a sandbox via node-manager,
/// persist the new pid, enqueue a policy reconcile so the fresh gateway
/// re-hydrates immediately, and atomically drop the cached build sha when the
/// PID commit succeeds (the new process' sha is unknown until its first stats
/// probe; keeping the old one would misreport the replacement as stale/fresh).
/// The enqueued reconcile rehydrates policy and route state. If an earlier
/// launch succeeded but its PID was not committed, runtime-supervisor returns
/// that live process and this call finishes the interrupted commit instead of
/// forking a duplicate.
async fn relaunch_netgtw(
    info: &SidecarInfo,
    db: &crate::db::SharedConnection,
    sidecars: &Arc<Mutex<HashMap<String, SidecarInfo>>>,
    network_policy_reconcile_queue: &NetworkPolicyReconcileQueue,
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
    node_manager: &NodeManagerConnector,
) -> Result<u32, String> {
    let sandbox_id = &info.sandbox_id;
    let uid = info
        .uid
        .ok_or_else(|| "missing sandbox uid for net-gateway restart".to_string())?;
    let mut node_client = node_manager.connect().await.map_err(|e| e.to_string())?;

    let args = net_gateway_relaunch_args(db, sandbox_id, feature_flags)
        .map_err(|error| error.to_string())?;
    let process_uid =
        crate::service::gateway_identity::launch_uid(uid).map_err(|error| error.to_string())?;

    let resp = node_client
        .launch(pplx_space_node_mgr_client::proto::LaunchRequest {
            execution_fence: info.node_execution_id.map(|id| {
                pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(id, None).into()
            }),
            sandbox_id: sandbox_id.to_string(),
            program: pplx_space_common_core::host::linux::paths::NET_GATEWAY_SVC_BIN.to_string(),
            args,
            uid: process_uid,
            gid: process_uid,
            env: crate::sidecar_env::net_gateway_env(),
        })
        .await
        .map_err(|e| e.to_string())?;
    let pid = resp.get_ref().pid;

    commit_netgtw_replacement(sandbox_id, uid, info.netgtw_pid, pid, db, sidecars, state).await?;

    tracing::info!(
        sandbox_id = %sandbox_id,
        pid,
        "net-gateway restarted"
    );
    network_policy_reconcile_queue.enqueue(sandbox_id.to_string());
    Ok(pid)
}

async fn commit_netgtw_replacement(
    sandbox_id: &str,
    uid: u32,
    observed_netgtw_pid: Option<u32>,
    pid: u32,
    db: &crate::db::SharedConnection,
    sidecars: &Arc<Mutex<HashMap<String, SidecarInfo>>>,
    state: &Arc<Mutex<HashMap<String, SandboxHealth>>>,
) -> Result<(), String> {
    // Lock order for the only multi-map sidecar commit is health -> sidecars
    // -> SQLite. Other watchdog paths snapshot these maps under separate
    // guards; keep them that way so this order cannot form a cycle.
    let mut health = state.lock().await;
    let mut sidecars = sidecars.lock().await;
    let current = sidecars
        .get_mut(sandbox_id)
        .ok_or_else(|| "sandbox left watchdog during net-gateway restart".to_string())?;
    if current.uid != Some(uid) {
        return Err("sandbox authority changed during net-gateway restart".to_string());
    }
    if current.netgtw_pid != observed_netgtw_pid && current.netgtw_pid != Some(pid) {
        return Err("sandbox authority changed during net-gateway restart".to_string());
    }
    let expected_netgtw_pid = current.netgtw_pid;
    let conn = db
        .lock()
        .map_err(|error| format!("sandbox database mutex poisoned: {error}"))?;
    crate::db::replace_netgtw_pid(&conn, sandbox_id, uid, expected_netgtw_pid, pid)
        .map_err(|e| e.to_string())?;
    current.netgtw_pid = Some(pid);
    clear_netgtw_probe_state(&mut health, sandbox_id);
    Ok(())
}

fn net_gateway_relaunch_args(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    feature_flags: &pplx_space_feature_flags::FeatureFlagClient,
) -> Result<Vec<String>, NetGatewayRelaunchArgsError> {
    let conn = db
        .lock()
        .map_err(|error| NetGatewayRelaunchArgsError::DatabaseMutexPoisoned(error.to_string()))?;
    let sandbox = crate::db::read::sandbox(&conn, sandbox_id)
        .map_err(|source| NetGatewayRelaunchArgsError::LoadSandbox {
            sandbox_id: sandbox_id.to_string(),
            source,
        })?
        .ok_or_else(|| NetGatewayRelaunchArgsError::MissingSandbox(sandbox_id.to_string()))?;
    drop(conn);
    let config = crate::service::net_gateway_launch_config(feature_flags, sandbox_id);
    Ok(crate::service::netgtw_launch_args(
        sandbox_id,
        sandbox.pool_template.is_some(),
        Some(&sandbox.space_api_route_identity),
        &config,
    ))
}

/// Whether the sandbox's net-gateway pid symlink currently resolves to a live
/// process. Shared with the deliberate-cycle path, which must observe the old
/// gateway actually dead before relaunching (the runtime-supervisor returns
/// the existing pid otherwise).
pub(crate) fn netgtw_alive(sandbox_id: &str) -> bool {
    sidecar_alive(sandbox_id, "space-netgtw-svc")
}

fn clear_netgtw_probe_state(state: &mut HashMap<String, SandboxHealth>, sandbox_id: &str) {
    if let Some(health) = state.get_mut(sandbox_id) {
        health.netgtw_stats_pid = None;
        health.clear_net_gateway_stats();
    }
}

fn spaced_host_socket_path(sandbox_id: &str) -> String {
    format!(
        "{RUN_DIR}/{sandbox_id}/{}",
        pplx_space_common_core::host::linux::paths::SPACED_HOST_SOCKET_FILE
    )
}

fn runtime_alive(sandbox_id: &str) -> bool {
    Path::new(&format!("{RUN_DIR}/{sandbox_id}/fc.pid")).exists()
}

fn sidecar_pid_is_committed(expected: Option<u32>, live: Option<u32>) -> bool {
    live.is_some() && live == expected
}

#[cfg(test)]
mod tests;
