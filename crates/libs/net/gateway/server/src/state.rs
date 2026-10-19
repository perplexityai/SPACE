use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pplx_space_net_common::{
    EffectiveNetworkPolicyRef, NetworkPolicyV1, TlsInterception,
    effective_network_policy_ref_authority_label,
};
use tokio::sync::{Mutex, watch};
// Pending-hold deadlines share tokio's clock with the timers that enforce
// them, so paused-time tests observe the same timeout production does.
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::dns::DnsAllowCache;
use crate::egress::DnsAttributedTcpTracker;

use crate::proto::{NetworkPolicyErrorKind, NetworkState};

mod credentials;
pub use credentials::{InvalidNetworkCredentialSnapshot, NetworkCredentialSnapshot};
mod source;
pub(crate) use source::host_control_policy_loader;
pub use source::{
    HydrateNetworkStateError, NetworkPolicyFromHostControlError, PolicyLoader, PulledNetworkPolicy,
};
mod egress;
pub use egress::ReservedDnsCancellation;
mod startup_timing;
use startup_timing::StartupTiming;

pub type ReadinessObserver = Arc<dyn Fn(bool) + Send + Sync>;

// Credential-bearing policy hydration includes the node-agent and authority
// provider budgets. The state stays fail-closed while this deadline runs.
pub const HYDRATION_DEADLINE: Duration = Duration::from_secs(30);
pub const TCP_PENDING_TIMEOUT: Duration = HYDRATION_DEADLINE.saturating_add(Duration::from_secs(1));
pub const TCP_PENDING_WARN_AFTER: Duration = Duration::from_millis(500);
pub(crate) const INITIAL_HYDRATION_RETRY_INTERVAL: Duration = Duration::from_millis(250);
pub(crate) const READY_HYDRATION_RECHECK_INTERVAL: Duration = Duration::from_secs(5);
const POLICY_REF_MISMATCH_RECHECK_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const MAX_HYDRATION_RETRY_BACKOFF: Duration = Duration::from_secs(5);
pub(crate) const MAX_SELF_HYDRATION_RETRIES: u64 = 12;

#[derive(Clone, Debug)]
pub struct NetworkStateSnapshot {
    /// Policy lifecycle only. `Ready` means a policy is installed; it says
    /// nothing about TLS-interception dependencies (see
    /// [`Self::interception_readiness`]), so a missing CA never demotes a
    /// Ready gateway to `PendingPolicy` for its control-plane peers.
    pub mode: NetworkState,
    pub policy: Option<Arc<NetworkPolicyV1>>,
    pub installed_policy_ref: String,
    pub last_error: String,
    pub last_error_kind: NetworkPolicyErrorKind,
    pub last_hydration_latency_ms: u64,
    pub last_transition_unix_ms: u64,
    pub pending_tcp_connections: u64,
    pub current_retry_backoff_ms: u64,
    pub consecutive_hydration_failures: u64,
    /// Whether guest traffic under the installed policy can currently be
    /// admitted with respect to TLS-interception dependencies. Reported
    /// separately from `mode` so sandbox start never depends on it.
    pub interception_readiness: InterceptionReadiness,
}

/// Readiness of the gateway CA relative to the installed policy. Orthogonal to [`NetworkState`]:
/// a gateway is `Ready` with `DependenciesNotReady` while it denies traffic
/// until the CA is available.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InterceptionReadiness {
    /// No policy is installed, or the installed policy has TLS interception
    /// off. Nothing beyond policy hydration gates admission.
    NotRequired,
    /// The installed policy has TLS interception on and every dependency
    /// behind the traffic gate is usable.
    Ready,
    /// The installed policy has TLS interception on and the gateway CA is not
    /// usable yet. New TCP and UDP traffic is denied.
    DependenciesNotReady,
}

impl InterceptionReadiness {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Ready => "ready",
            Self::DependenciesNotReady => "dependencies_not_ready",
        }
    }

    /// Whether admission is currently blocked on interception dependencies.
    pub const fn blocks_admission(self) -> bool {
        matches!(self, Self::DependenciesNotReady)
    }
}

impl std::fmt::Display for InterceptionReadiness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str((*self).as_str())
    }
}

#[derive(Clone, Debug)]
pub enum TrafficPolicy {
    Ready(Arc<NetworkPolicyV1>),
    NotReady(PolicyNotReady),
}

/// Why no policy was available to evaluate a flow against. Doubles as the
/// `decision` tag on `gateway.policy.decision` for flows denied this way.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyNotReady {
    InterceptionNotReady,
    ReadyWithoutPolicy,
    PendingPolicyTimeout,
    NoIdentity,
    PolicyError,
    Draining,
    Stopped,
    Unspecified,
    /// Reached only by the UDP path, which buffers rather than drops here.
    PendingPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NetworkCredentialSnapshotError {
    TemporarilyUnavailable,
    Stale,
}

impl PolicyNotReady {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InterceptionNotReady => "interception_not_ready",
            Self::ReadyWithoutPolicy => "ready_without_policy",
            Self::PendingPolicyTimeout => "pending_policy_timeout",
            Self::NoIdentity => "no_identity",
            Self::PolicyError => "policy_error",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
            Self::Unspecified => "unspecified",
            Self::PendingPolicy => "pending_policy",
        }
    }
}

#[derive(Clone)]
pub struct GatewayNetworkState {
    state_tx: watch::Sender<Arc<NetworkStateInner>>,
    state_rx: watch::Receiver<Arc<NetworkStateInner>>,
    writer_lock: Arc<Mutex<()>>,
    refresh_lock: Arc<Mutex<()>>,
    pending_tcp_connections: Arc<AtomicU64>,
    dns_allow_cache: DnsAllowCache,
    tcp_connections: DnsAttributedTcpTracker,
    reserved_dns_cancellation: ReservedDnsCancellation,
    traffic_gate: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    readiness_observer: Option<Arc<StartupTiming>>,
    owner_contacted: Arc<watch::Sender<bool>>,
}

#[derive(Clone, Debug)]
struct NetworkStateInner {
    mode: NetworkState,
    policy: Option<Arc<NetworkPolicyV1>>,
    network_credentials: Option<Arc<NetworkCredentialSnapshot>>,
    installed_policy_ref: String,
    last_error: String,
    last_error_kind: NetworkPolicyErrorKind,
    last_hydration_latency_ms: u64,
    last_transition_unix_ms: u64,
    current_retry_backoff_ms: u64,
    consecutive_hydration_failures: u64,
}

struct TcpPendingWaiterGuard {
    pending_tcp_connections: Arc<AtomicU64>,
    active: bool,
}

impl TcpPendingWaiterGuard {
    fn finish(&mut self) {
        if self.active {
            let update_result = self.pending_tcp_connections.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(1)),
            );
            debug_assert!(update_result.is_ok());
            self.active = false;
        }
    }
}

impl Drop for TcpPendingWaiterGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkStateReason {
    Startup,
    SelfRetry,
    ControlRefresh,
    PoolAdoption,
    PoolAdoptionRollback,
    Reconcile,
    SnapshotRestore,
    Start,
    NoIdentity,
    Shutdown,
}

impl NetworkStateReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::SelfRetry => "self_retry",
            Self::ControlRefresh => "control_refresh",
            Self::PoolAdoption => "pool_adoption",
            Self::PoolAdoptionRollback => "pool_adoption_rollback",
            Self::Reconcile => "reconcile",
            Self::SnapshotRestore => "snapshot_restore",
            Self::Start => "start",
            Self::NoIdentity => "no_identity",
            Self::Shutdown => "shutdown",
        }
    }

    pub fn from_external(value: &str) -> Self {
        match value {
            "pool-adoption" | "pool_adoption" => Self::PoolAdoption,
            "pool-adoption-rollback"
            | "pool_adoption_rollback"
            | "pool-rollback"
            | "pool_rollback" => Self::PoolAdoptionRollback,
            "reconcile" => Self::Reconcile,
            "snapshot-restore" | "snapshot_restore" => Self::SnapshotRestore,
            "start" => Self::Start,
            "no_identity" | "no-identity" => Self::NoIdentity,
            "shutdown" => Self::Shutdown,
            "startup" => Self::Startup,
            "self_retry" | "self-retry" => Self::SelfRetry,
            _ => Self::ControlRefresh,
        }
    }
}

impl std::fmt::Display for NetworkStateReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str((*self).as_str())
    }
}

impl GatewayNetworkState {
    pub fn pending_policy() -> Self {
        Self::new(NetworkStateInner {
            mode: NetworkState::PendingPolicy,
            policy: None,
            network_credentials: None,
            installed_policy_ref: String::new(),
            last_error: String::new(),
            last_error_kind: NetworkPolicyErrorKind::None,
            last_hydration_latency_ms: 0,
            last_transition_unix_ms: unix_time_ms(),
            current_retry_backoff_ms: 0,
            consecutive_hydration_failures: 0,
        })
    }

    pub fn no_identity() -> Self {
        Self::new(NetworkStateInner {
            mode: NetworkState::NoIdentity,
            policy: None,
            network_credentials: None,
            installed_policy_ref: EffectiveNetworkPolicyRef::no_identity().to_string(),
            last_error: String::new(),
            last_error_kind: NetworkPolicyErrorKind::None,
            last_hydration_latency_ms: 0,
            last_transition_unix_ms: unix_time_ms(),
            current_retry_backoff_ms: 0,
            consecutive_hydration_failures: 0,
        })
    }

    fn new(inner: NetworkStateInner) -> Self {
        let (state_tx, state_rx) = watch::channel(Arc::new(inner));
        Self {
            traffic_gate: None,
            readiness_observer: None,
            state_tx,
            state_rx,
            writer_lock: Arc::new(Mutex::new(())),
            refresh_lock: Arc::new(Mutex::new(())),
            pending_tcp_connections: Arc::new(AtomicU64::new(0)),
            dns_allow_cache: DnsAllowCache::default(),
            tcp_connections: DnsAttributedTcpTracker::default(),
            reserved_dns_cancellation: ReservedDnsCancellation::default(),
            owner_contacted: Arc::new(watch::channel(false).0),
        }
    }

    /// Record that the owning sandbox-manager has called this gateway's control
    /// socket. The owner registers the gateway's process with host-control
    /// before it calls, so from now on a policy pull is authorized.
    pub fn record_owner_contact(&self) {
        self.owner_contacted.send_replace(true);
    }

    /// Whether, and when, the owner first called; see [`Self::record_owner_contact`].
    pub fn owner_contact(&self) -> watch::Receiver<bool> {
        self.owner_contacted.subscribe()
    }

    pub async fn snapshot(&self) -> NetworkStateSnapshot {
        let inner = self.state_rx.borrow().clone();
        self.snapshot_from_inner(inner.as_ref())
    }

    /// Attach once, before hydration or CA acquisition. Pool residency is not a
    /// readiness wait; timing begins when the policy leaves NoIdentity and
    /// stops at this process's first Ready.
    #[must_use]
    pub fn with_readiness_metrics(mut self) -> Self {
        self.readiness_observer =
            Some(Arc::new(StartupTiming::new(self.state_rx.borrow().clone())));
        self
    }

    /// Observer for the interception dependency (gateway CA).
    /// Feeds startup timing independently of policy readiness.
    pub fn interception_readiness_observer(&self) -> ReadinessObserver {
        // Capture only the policy receiver, not the gateway/authority graph.
        // Capturing self would create a cycle through traffic_gate.
        let state = self.state_rx.clone();
        let observer = self.readiness_observer.clone();
        Arc::new(move |ready| {
            if let Some(observer) = &observer {
                observer.observe(|| (state.borrow().clone(), ready));
            }
        })
    }

    /// Live interception readiness for `policy`, evaluated against the
    /// traffic gate at call time.
    fn interception_readiness_for(
        &self,
        policy: Option<&NetworkPolicyV1>,
    ) -> InterceptionReadiness {
        match policy {
            Some(policy) if policy.tls_interception == TlsInterception::On => {
                if self.traffic_gate_open() {
                    InterceptionReadiness::Ready
                } else {
                    InterceptionReadiness::DependenciesNotReady
                }
            }
            _ => InterceptionReadiness::NotRequired,
        }
    }

    // Hydration and admission must distinguish an installed policy from an
    // unavailable CA. In particular, CA startup must not spend policy retries.
    fn policy_snapshot(&self) -> NetworkStateSnapshot {
        let inner = self.state_rx.borrow();
        self.snapshot_from_inner(inner.as_ref())
    }

    /// Wait until the installed policy requires interception. Gateway CA
    /// preparation stays dormant for the default-off policy.
    pub(crate) async fn wait_for_interception_policy(&self, shutdown: &CancellationToken) -> bool {
        let mut state = self.state_rx.clone();
        tokio::select! {
            biased;
            () = shutdown.cancelled() => false,
            installed = state.wait_for(|inner| {
                inner.mode == NetworkState::Ready
                    && inner.policy.as_ref().is_some_and(|policy| {
                        policy.tls_interception == TlsInterception::On
                    })
            }) => installed.is_ok(),
        }
    }

    /// Additional admission requirement, separate from policy hydration so the
    /// control plane can initialize both without a circular readiness wait.
    pub fn with_traffic_gate(mut self, gate: Arc<dyn Fn() -> bool + Send + Sync>) -> Self {
        self.traffic_gate = Some(gate);
        self
    }

    fn traffic_gate_open(&self) -> bool {
        self.traffic_gate.as_ref().is_none_or(|gate| gate())
    }

    fn policy_traffic_ready(&self, policy: &NetworkPolicyV1) -> bool {
        policy.tls_interception == TlsInterception::Off || self.traffic_gate_open()
    }

    /// Whether `expected` is the exact policy generation currently ready for
    /// new TCP admission. Each successful install allocates a new policy, so
    /// pointer identity is the generation token without duplicating state.
    pub(crate) fn is_current_tcp_policy(&self, expected: &Arc<NetworkPolicyV1>) -> bool {
        let inner = self.state_rx.borrow();
        inner.mode == NetworkState::Ready
            && inner.policy.as_ref().is_some_and(|current| {
                Arc::ptr_eq(current, expected) && self.policy_traffic_ready(current)
            })
    }

    /// Keep raw values inside the checked snapshot borrow; callers may only
    /// return an owned representation prepared synchronously by this closure.
    pub(crate) fn with_network_credentials<T>(
        &self,
        expected_policy: &Arc<NetworkPolicyV1>,
        use_credentials: impl FnOnce(&NetworkCredentialSnapshot) -> T,
    ) -> Result<T, NetworkCredentialSnapshotError> {
        let inner = self.state_rx.borrow();
        let same_policy = inner
            .policy
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected_policy));
        if same_policy && inner.mode == NetworkState::PendingPolicy {
            return Err(NetworkCredentialSnapshotError::TemporarilyUnavailable);
        }
        if inner.mode != NetworkState::Ready
            || !same_policy
            || !self.policy_traffic_ready(expected_policy)
        {
            return Err(NetworkCredentialSnapshotError::Stale);
        }
        let credentials = inner
            .network_credentials
            .as_ref()
            .ok_or(NetworkCredentialSnapshotError::TemporarilyUnavailable)?;
        Ok(use_credentials(credentials))
    }

    /// Spawn the self-hydration loop. Its first pass, the startup pull, runs
    /// under `startup`, the gateway's startup span, which the loop releases
    /// after that pass whatever it finds: a gateway an external refresh made
    /// ready first never pulls under it. Retries and later rehydrations are
    /// the gateway's own and run under no span.
    pub fn spawn_hydration(
        &self,
        loader: PolicyLoader,
        shutdown_token: CancellationToken,
        startup: Option<tracing::Span>,
    ) -> tokio::task::JoinHandle<()> {
        let state = self.clone();
        tokio::spawn(async move {
            state
                .run_hydration_loop(loader, shutdown_token, startup)
                .await;
        })
    }

    async fn run_hydration_loop(
        self,
        loader: PolicyLoader,
        shutdown_token: CancellationToken,
        startup: Option<tracing::Span>,
    ) {
        use tracing::Instrument as _;
        let mut retry_delay = INITIAL_HYDRATION_RETRY_INTERVAL;
        let mut attempts = 0_u64;
        let mut startup = startup;

        loop {
            if shutdown_token.is_cancelled() {
                break;
            }

            let snapshot = self.policy_snapshot();
            // The startup span covers the first pass's pull only. A first
            // pass that finds the gateway ready, waiting on a newer policy or
            // stopping drops it here, before it waits.
            let startup_pass = startup.take().filter(|_| match snapshot.mode {
                NetworkState::PendingPolicy | NetworkState::Unspecified => true,
                NetworkState::PolicyError => {
                    snapshot.last_error_kind != NetworkPolicyErrorKind::PolicyRefMismatch
                }
                NetworkState::Ready
                | NetworkState::NoIdentity
                | NetworkState::Draining
                | NetworkState::Stopped => false,
            });
            match snapshot.mode {
                NetworkState::Ready => {
                    attempts = 0;
                    retry_delay = INITIAL_HYDRATION_RETRY_INTERVAL;
                    self.set_retry_backoff(Duration::ZERO).await;
                    if self
                        .wait_for_state_change_or_shutdown(
                            READY_HYDRATION_RECHECK_INTERVAL,
                            &shutdown_token,
                        )
                        .await
                    {
                        break;
                    }
                    continue;
                }
                NetworkState::PolicyError
                    if snapshot.last_error_kind == NetworkPolicyErrorKind::PolicyRefMismatch =>
                {
                    attempts = 0;
                    retry_delay = INITIAL_HYDRATION_RETRY_INTERVAL;
                    self.set_retry_backoff(POLICY_REF_MISMATCH_RECHECK_INTERVAL)
                        .await;
                    if self
                        .wait_for_state_change_or_shutdown(
                            POLICY_REF_MISMATCH_RECHECK_INTERVAL,
                            &shutdown_token,
                        )
                        .await
                    {
                        break;
                    }
                    continue;
                }
                NetworkState::NoIdentity | NetworkState::Draining | NetworkState::Stopped => break,
                NetworkState::PendingPolicy
                | NetworkState::PolicyError
                | NetworkState::Unspecified => {}
            }

            if attempts >= MAX_SELF_HYDRATION_RETRIES {
                self.mark_permanent_policy_error(attempts).await;
                break;
            }

            attempts = attempts.saturating_add(1);
            self.set_retry_backoff(retry_delay).await;
            pplx_space_metrics::gateway::policy_hydration_retry(
                NetworkStateReason::SelfRetry.as_str(),
            );
            let snapshot = self
                .refresh(loader.clone(), None, NetworkStateReason::SelfRetry)
                .instrument(startup_pass.unwrap_or_else(tracing::Span::none))
                .await;
            if self.policy_snapshot().mode == NetworkState::Ready {
                attempts = 0;
                retry_delay = INITIAL_HYDRATION_RETRY_INTERVAL;
                continue;
            }

            warn!(
                error_kind = snapshot.last_error_kind.as_str(),
                consecutive_hydration_failures = snapshot.consecutive_hydration_failures,
                current_retry_backoff_ms =
                    u64::try_from(retry_delay.as_millis()).unwrap_or(u64::MAX),
                "net-gateway policy hydration retry did not reach ready"
            );

            if wait_for_shutdown(retry_delay, &shutdown_token).await {
                break;
            }
            retry_delay = retry_delay
                .saturating_mul(2)
                .min(MAX_HYDRATION_RETRY_BACKOFF);
        }
    }

    async fn wait_for_state_change_or_shutdown(
        &self,
        duration: Duration,
        shutdown_token: &CancellationToken,
    ) -> bool {
        let mut state_rx = self.state_tx.subscribe();
        tokio::select! {
            _ = shutdown_token.cancelled() => true,
            _ = tokio::time::sleep(duration) => false,
            changed = state_rx.changed() => changed.is_err(),
        }
    }

    async fn update_inner(
        &self,
        update: impl FnOnce(&mut NetworkStateInner),
    ) -> Arc<NetworkStateInner> {
        let _guard = self.writer_lock.lock().await;
        let current = self.state_rx.borrow().clone();
        let mut inner = current.as_ref().clone();
        update(&mut inner);
        if current.mode == NetworkState::Ready && inner.mode != NetworkState::Ready {
            self.reserved_dns_cancellation.cancel_in_flight();
        }
        let policy_removed = current.policy.is_some() && inner.policy.is_none();
        let inner = Arc::new(inner);
        self.state_tx.send_replace(inner.clone());
        if let Some(observer) = &self.readiness_observer {
            observer.observe(|| (self.state_rx.borrow().clone(), self.traffic_gate_open()));
        }
        // Publish unavailability before sweeping. An old-policy admission racing
        // the transition then either fails its final generation check or was
        // already registered and is cancelled below.
        if policy_removed {
            let cancelled = self.tcp_connections.cancel_all();
            if cancelled > 0 {
                info!(
                    dns_attributed_cancelled_tcp_connections = cancelled,
                    "cancelled TCP connections after network policy invalidation"
                );
            }
        }
        inner
    }

    pub async fn set_retry_backoff(&self, retry_backoff: Duration) {
        self.update_inner(|inner| {
            inner.current_retry_backoff_ms =
                u64::try_from(retry_backoff.as_millis()).unwrap_or(u64::MAX);
        })
        .await;
    }

    async fn mark_permanent_policy_error(&self, attempts: u64) -> NetworkStateSnapshot {
        let inner = self
            .update_inner(|inner| {
                set_mode(
                    inner,
                    NetworkState::PolicyError,
                    NetworkStateReason::SelfRetry,
                );
                inner.policy = None;
                inner.network_credentials = None;
                inner.last_error =
                    format!("policy hydration exhausted retry budget after {attempts} attempts");
                inner.last_error_kind = NetworkPolicyErrorKind::LoaderError;
                inner.current_retry_backoff_ms = 0;
                inner.consecutive_hydration_failures =
                    inner.consecutive_hydration_failures.saturating_add(1);
                pplx_space_metrics::gateway::policy_hydration_error("permanent");
                record_hydration_failures(
                    inner.last_error_kind,
                    inner.consecutive_hydration_failures,
                );
            })
            .await;
        self.snapshot_from_inner(inner.as_ref())
    }

    pub async fn set_no_identity(&self, reason: NetworkStateReason) -> NetworkStateSnapshot {
        let inner = self
            .update_inner(|inner| {
                if inner.policy.is_some() {
                    info!(
                        cleared_policy_ref = %inner.installed_policy_ref,
                        reason = reason.as_str(),
                        "net-gateway policy cleared"
                    );
                }
                set_mode(inner, NetworkState::NoIdentity, reason);
                inner.policy = None;
                inner.network_credentials = None;
                inner.installed_policy_ref = EffectiveNetworkPolicyRef::no_identity().to_string();
                inner.last_error.clear();
                inner.last_error_kind = NetworkPolicyErrorKind::None;
                inner.current_retry_backoff_ms = 0;
                inner.consecutive_hydration_failures = 0;
                pplx_space_metrics::gateway::policy_ref("no_identity", inner.mode.as_str());
                record_hydration_failures(
                    inner.last_error_kind,
                    inner.consecutive_hydration_failures,
                );
            })
            .await;
        self.snapshot_from_inner(inner.as_ref())
    }

    /// Re-pull and reconcile the network policy. Skips the pull when the gateway
    /// is already `Ready` and the installed v0 policy ref satisfies the
    /// expectation.
    pub async fn refresh(
        &self,
        loader: PolicyLoader,
        expected_policy_ref: Option<EffectiveNetworkPolicyRef>,
        reason: NetworkStateReason,
    ) -> NetworkStateSnapshot {
        let _guard = self.refresh_lock.lock().await;

        let current = self.state_rx.borrow().clone();
        if current.mode == NetworkState::Ready
            && installed_ref_matches(&current.installed_policy_ref, expected_policy_ref.as_ref())
            && current
                .policy
                .as_ref()
                .is_none_or(|policy| policy.network_credential_rules.is_empty())
        {
            return self.snapshot_from_inner(current.as_ref());
        }

        let current = self
            .update_inner(|inner| {
                if matches!(inner.mode, NetworkState::Draining | NetworkState::Stopped) {
                    return;
                }
                set_mode(inner, NetworkState::PendingPolicy, reason);
                inner.last_error.clear();
                inner.last_error_kind = NetworkPolicyErrorKind::None;
            })
            .await;
        if matches!(current.mode, NetworkState::Draining | NetworkState::Stopped) {
            return self.snapshot_from_inner(current.as_ref());
        }

        let started_at = Instant::now();
        let result = tokio::time::timeout(HYDRATION_DEADLINE, (loader)()).await;
        let latency_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let expected_ref_for_log = expected_ref_display(expected_policy_ref.as_ref());

        let inner = self
            .update_inner(move |inner| {
                if refresh_result_must_not_mutate(inner.mode) {
                    return;
                }
                inner.last_hydration_latency_ms = latency_ms;
                match result {
                    Ok(Ok(pulled))
                        if pulled_ref_matches(&pulled.policy_ref, expected_policy_ref.as_ref()) =>
                    {
                        let policy = Arc::new(pulled.policy);
                        let network_credentials = Arc::new(pulled.network_credentials);
                        let (action, allow_counts, deny_counts) = policy_log_summary(&policy);
                        self.reconcile_policy(&policy);
                        set_mode(inner, NetworkState::Ready, reason);
                        inner.policy = Some(Arc::clone(&policy));
                        inner.network_credentials = Some(network_credentials);
                        inner.installed_policy_ref = pulled.policy_ref.to_string();
                        inner.last_error.clear();
                        inner.last_error_kind = NetworkPolicyErrorKind::None;
                        inner.current_retry_backoff_ms = 0;
                        inner.consecutive_hydration_failures = 0;
                        pplx_space_metrics::gateway::policy_hydration_latency(
                            "success",
                            latency_ms as f64,
                        );
                        pplx_space_metrics::gateway::policy_ref(
                            pulled.policy_ref.authority_label(),
                            inner.mode.as_str(),
                        );
                        info!(
                            installed_policy_ref = %inner.installed_policy_ref,
                            latency_ms,
                            policy_version = "v1",
                            action,
                            allow_rules = allow_counts.total,
                            allow_domains = allow_counts.domains,
                            allow_wildcard_domains = allow_counts.wildcard_domains,
                            allow_ips = allow_counts.ips,
                            allow_cidrs = allow_counts.cidrs,
                            deny_rules = deny_counts.total,
                            deny_domains = deny_counts.domains,
                            deny_wildcard_domains = deny_counts.wildcard_domains,
                            deny_ips = deny_counts.ips,
                            deny_cidrs = deny_counts.cidrs,
                            reason = reason.as_str(),
                            // Effective mode for this install. Every
                            // install (refresh, self-hydration, restore)
                            // logs this line, so it tracks mode changes.
                            tls_interception_mode = policy.tls_interception.as_str(),
                            // The rendered destination strings are NOT
                            // logged: policy destinations are tenant-
                            // authored and a hostname can carry arbitrary
                            // encoded data, so emitting them on every
                            // hydration would be an ungated log leak. The
                            // per-class counts above are the bounded,
                            // non-sensitive shape signal.
                            "net-gateway policy hydration ready"
                        );
                    }
                    Ok(Ok(pulled)) => {
                        set_mode(inner, NetworkState::PolicyError, reason);
                        inner.policy = None;
                        inner.network_credentials = None;
                        inner.last_error = format!(
                            "policy ref mismatch: expected {}, got {}",
                            expected_ref_for_log, pulled.policy_ref
                        );
                        inner.last_error_kind = NetworkPolicyErrorKind::PolicyRefMismatch;
                        inner.consecutive_hydration_failures =
                            inner.consecutive_hydration_failures.saturating_add(1);
                        pplx_space_metrics::gateway::policy_hydration_error("policy_ref_mismatch");
                        pplx_space_metrics::gateway::policy_ref(
                            pulled.policy_ref.authority_label(),
                            inner.mode.as_str(),
                        );
                        warn!(
                            expected_policy_ref = %expected_ref_for_log,
                            installed_policy_ref = %pulled.policy_ref,
                            reason = reason.as_str(),
                            "net-gateway policy hydration returned mismatched policy"
                        );
                    }
                    Ok(Err(error @ HydrateNetworkStateError::InvalidReference { .. })) => {
                        set_mode(inner, NetworkState::PolicyError, reason);
                        inner.policy = None;
                        inner.network_credentials = None;
                        inner.last_error = error.to_string();
                        inner.last_error_kind = NetworkPolicyErrorKind::LoaderError;
                        inner.consecutive_hydration_failures =
                            inner.consecutive_hydration_failures.saturating_add(1);
                        pplx_space_metrics::gateway::policy_hydration_error("invalid_policy_ref");
                        pplx_space_metrics::gateway::policy_hydration_latency(
                            "error",
                            latency_ms as f64,
                        );
                        warn!(
                            last_error = %inner.last_error,
                            latency_ms,
                            reason = reason.as_str(),
                            "net-gateway policy hydration returned invalid policy ref"
                        );
                    }
                    Ok(Err(error)) => {
                        set_mode(inner, NetworkState::PolicyError, reason);
                        inner.policy = None;
                        inner.network_credentials = None;
                        inner.last_error = error.to_string();
                        inner.last_error_kind = NetworkPolicyErrorKind::LoaderError;
                        inner.consecutive_hydration_failures =
                            inner.consecutive_hydration_failures.saturating_add(1);
                        pplx_space_metrics::gateway::policy_hydration_error("loader_error");
                        pplx_space_metrics::gateway::policy_hydration_latency(
                            "error",
                            latency_ms as f64,
                        );
                        warn!(
                            last_error = %inner.last_error,
                            latency_ms,
                            reason = reason.as_str(),
                            "net-gateway policy hydration failed"
                        );
                    }
                    Err(_) => {
                        set_mode(inner, NetworkState::PolicyError, reason);
                        inner.policy = None;
                        inner.network_credentials = None;
                        inner.last_error = format!(
                            "policy hydration timed out after {} ms",
                            HYDRATION_DEADLINE.as_millis()
                        );
                        inner.last_error_kind = NetworkPolicyErrorKind::Timeout;
                        inner.consecutive_hydration_failures =
                            inner.consecutive_hydration_failures.saturating_add(1);
                        pplx_space_metrics::gateway::policy_hydration_error("timeout");
                        pplx_space_metrics::gateway::policy_hydration_latency(
                            "timeout",
                            latency_ms as f64,
                        );
                        warn!(
                            latency_ms,
                            reason = reason.as_str(),
                            "net-gateway policy hydration timed out"
                        );
                    }
                }
                record_hydration_failures(
                    inner.last_error_kind,
                    inner.consecutive_hydration_failures,
                );
            })
            .await;
        self.snapshot_from_inner(inner.as_ref())
    }

    pub async fn wait_for_tcp_policy(&self) -> TrafficPolicy {
        let started_at = Instant::now();
        let mut warned = false;
        let mut pending_waiter = None;
        let deadline = Instant::now() + TCP_PENDING_TIMEOUT;
        let mut state_rx = self.state_rx.clone();

        loop {
            let snapshot = self.snapshot_from_inner(state_rx.borrow().as_ref());
            match snapshot.mode {
                NetworkState::Ready => {
                    if pending_waiter.take().is_some() {
                        record_tcp_pending_wait("ready", started_at);
                    }
                    if let Some(policy) = snapshot.policy {
                        if snapshot.interception_readiness.blocks_admission() {
                            record_not_ready_denial("tcp", PolicyNotReady::InterceptionNotReady);
                            return TrafficPolicy::NotReady(PolicyNotReady::InterceptionNotReady);
                        }
                        return TrafficPolicy::Ready(policy);
                    }
                    record_not_ready_denial("tcp", PolicyNotReady::ReadyWithoutPolicy);
                    return TrafficPolicy::NotReady(PolicyNotReady::ReadyWithoutPolicy);
                }
                NetworkState::PendingPolicy => {
                    if pending_waiter.is_none() {
                        pending_waiter = Some(self.start_tcp_pending_waiter());
                        pplx_space_metrics::gateway::pending_tcp_wait("started");
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        pending_waiter.take();
                        pplx_space_metrics::gateway::pending_timeout("tcp");
                        record_tcp_pending_wait("timeout", started_at);
                        return TrafficPolicy::NotReady(PolicyNotReady::PendingPolicyTimeout);
                    }
                    if !warned && started_at.elapsed() >= TCP_PENDING_WARN_AFTER {
                        warned = true;
                        record_pending_tcp_slow_wait(&snapshot, started_at);
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    let wait_for = if warned {
                        remaining
                    } else {
                        remaining.min(TCP_PENDING_WARN_AFTER.saturating_sub(started_at.elapsed()))
                    };
                    if tokio::time::timeout(wait_for, state_rx.changed())
                        .await
                        .is_err()
                    {
                        if !warned && started_at.elapsed() >= TCP_PENDING_WARN_AFTER {
                            warned = true;
                            record_pending_tcp_slow_wait(&snapshot, started_at);
                            continue;
                        }
                        pending_waiter.take();
                        pplx_space_metrics::gateway::pending_timeout("tcp");
                        record_tcp_pending_wait("timeout", started_at);
                        return TrafficPolicy::NotReady(PolicyNotReady::PendingPolicyTimeout);
                    }
                }
                NetworkState::NoIdentity => {
                    if pending_waiter.take().is_some() {
                        record_tcp_pending_wait("no_identity", started_at);
                    }
                    record_not_ready_denial("tcp", PolicyNotReady::NoIdentity);
                    return TrafficPolicy::NotReady(PolicyNotReady::NoIdentity);
                }
                NetworkState::PolicyError => {
                    if pending_waiter.take().is_some() {
                        record_tcp_pending_wait("policy_error", started_at);
                    }
                    record_not_ready_denial("tcp", PolicyNotReady::PolicyError);
                    return TrafficPolicy::NotReady(PolicyNotReady::PolicyError);
                }
                NetworkState::Draining => {
                    if pending_waiter.take().is_some() {
                        record_tcp_pending_wait("draining", started_at);
                    }
                    record_not_ready_denial("tcp", PolicyNotReady::Draining);
                    return TrafficPolicy::NotReady(PolicyNotReady::Draining);
                }
                NetworkState::Stopped => {
                    if pending_waiter.take().is_some() {
                        record_tcp_pending_wait("stopped", started_at);
                    }
                    record_not_ready_denial("tcp", PolicyNotReady::Stopped);
                    return TrafficPolicy::NotReady(PolicyNotReady::Stopped);
                }
                NetworkState::Unspecified => {
                    if pending_waiter.take().is_some() {
                        record_tcp_pending_wait("unspecified", started_at);
                    }
                    record_not_ready_denial("tcp", PolicyNotReady::Unspecified);
                    return TrafficPolicy::NotReady(PolicyNotReady::Unspecified);
                }
            }
        }
    }

    pub async fn udp_policy(&self) -> TrafficPolicy {
        let snapshot = self.policy_snapshot();
        if snapshot.mode == NetworkState::Ready
            && let Some(policy) = snapshot.policy
        {
            // One live gate read per datagram, taken by the snapshot.
            if snapshot.interception_readiness.blocks_admission() {
                // UDP has no connection to hold: a datagram either goes out
                // inspected or not at all. Deny per datagram.
                record_not_ready_denial("udp", PolicyNotReady::InterceptionNotReady);
                return TrafficPolicy::NotReady(PolicyNotReady::InterceptionNotReady);
            }
            return TrafficPolicy::Ready(policy);
        }
        debug!(state = ?snapshot.mode, "UDP dropped while network policy is not ready");
        let reason = match snapshot.mode {
            NetworkState::Ready => PolicyNotReady::ReadyWithoutPolicy,
            NetworkState::PendingPolicy => PolicyNotReady::PendingPolicy,
            NetworkState::NoIdentity => PolicyNotReady::NoIdentity,
            NetworkState::PolicyError => PolicyNotReady::PolicyError,
            NetworkState::Draining => PolicyNotReady::Draining,
            NetworkState::Stopped => PolicyNotReady::Stopped,
            NetworkState::Unspecified => PolicyNotReady::Unspecified,
        };
        record_not_ready_denial("udp", reason);
        TrafficPolicy::NotReady(reason)
    }

    pub async fn set_draining(&self, reason: NetworkStateReason) {
        self.update_inner(|inner| {
            set_mode(inner, NetworkState::Draining, reason);
            inner.last_error.clear();
            inner.last_error_kind = NetworkPolicyErrorKind::None;
            inner.current_retry_backoff_ms = 0;
        })
        .await;
    }

    pub async fn set_stopped(&self, reason: NetworkStateReason) {
        self.update_inner(|inner| {
            if inner.policy.is_some() {
                info!(
                    cleared_policy_ref = %inner.installed_policy_ref,
                    reason = reason.as_str(),
                    "net-gateway policy cleared"
                );
            }
            set_mode(inner, NetworkState::Stopped, reason);
            inner.policy = None;
            inner.network_credentials = None;
            inner.last_error.clear();
            inner.last_error_kind = NetworkPolicyErrorKind::None;
            inner.current_retry_backoff_ms = 0;
        })
        .await;
    }

    fn start_tcp_pending_waiter(&self) -> TcpPendingWaiterGuard {
        self.pending_tcp_connections.fetch_add(1, Ordering::Relaxed);
        TcpPendingWaiterGuard {
            pending_tcp_connections: self.pending_tcp_connections.clone(),
            active: true,
        }
    }

    fn snapshot_from_inner(&self, inner: &NetworkStateInner) -> NetworkStateSnapshot {
        let mut snapshot = NetworkStateSnapshot {
            mode: inner.mode,
            policy: inner.policy.clone(),
            installed_policy_ref: inner.installed_policy_ref.clone(),
            last_error: inner.last_error.clone(),
            last_error_kind: inner.last_error_kind,
            last_hydration_latency_ms: inner.last_hydration_latency_ms,
            last_transition_unix_ms: inner.last_transition_unix_ms,
            pending_tcp_connections: self.pending_tcp_connections.load(Ordering::Relaxed),
            current_retry_backoff_ms: inner.current_retry_backoff_ms,
            consecutive_hydration_failures: inner.consecutive_hydration_failures,
            // Evaluated live on every read; the gate is not part of the
            // watch channel.
            interception_readiness: self.interception_readiness_for(inner.policy.as_deref()),
        };
        // Ready always means an installed policy. TLS-interception
        // dependencies do not demote the state:
        // sandbox-manager keys sandbox start on Ready + matching ref, and a
        // CA the node cannot issue must fail closed per connection, never
        // fail the boot. Peers read `interception_readiness` for that.
        if snapshot.mode == NetworkState::Ready && snapshot.policy.is_none() {
            snapshot.mode = NetworkState::PendingPolicy;
            snapshot.last_error = "network policy is not installed".to_owned();
        }
        snapshot
    }
}

fn set_mode(inner: &mut NetworkStateInner, mode: NetworkState, reason: NetworkStateReason) {
    if inner.mode == mode {
        return;
    }
    pplx_space_metrics::gateway::network_state_transition(
        inner.mode.as_str(),
        mode.as_str(),
        reason.as_str(),
    );
    inner.last_transition_unix_ms = unix_time_ms();
    inner.mode = mode;
}

fn installed_ref_matches(installed: &str, expected: Option<&EffectiveNetworkPolicyRef>) -> bool {
    match expected {
        None => true,
        Some(expected) => installed
            .parse::<EffectiveNetworkPolicyRef>()
            .is_ok_and(|installed| &installed == expected),
    }
}

fn pulled_ref_matches(
    pulled: &EffectiveNetworkPolicyRef,
    expected: Option<&EffectiveNetworkPolicyRef>,
) -> bool {
    expected.is_none_or(|expected| pulled == expected)
}

fn refresh_result_must_not_mutate(mode: NetworkState) -> bool {
    matches!(
        mode,
        NetworkState::NoIdentity | NetworkState::Draining | NetworkState::Stopped
    )
}

/// The bounded, non-sensitive shape of a policy for the hydration log: the
/// action label plus per-class destination counts. Deliberately does NOT render
/// the destination strings — policy destinations are tenant-authored and a
/// hostname can carry arbitrary encoded data, so logging them on every
/// hydration would be an ungated log leak.
fn policy_log_summary(
    policy: &pplx_space_net_common::NetworkPolicyV1,
) -> (
    &'static str,
    pplx_space_net_common::EgressDestinationCounts,
    pplx_space_net_common::EgressDestinationCounts,
) {
    (
        match policy.default_action() {
            pplx_space_net_common::EgressDefaultAction::Allow => "default_allow",
            pplx_space_net_common::EgressDefaultAction::Deny => "default_deny",
        },
        policy.egress.allow_counts(),
        policy.egress.deny_counts(),
    )
}

fn expected_ref_display(expected: Option<&EffectiveNetworkPolicyRef>) -> String {
    expected
        .map(ToString::to_string)
        .unwrap_or_else(|| "<any>".to_string())
}

fn record_hydration_failures(active_kind: NetworkPolicyErrorKind, active_failures: u64) {
    for kind in [
        NetworkPolicyErrorKind::None,
        NetworkPolicyErrorKind::LoaderError,
        NetworkPolicyErrorKind::Timeout,
        NetworkPolicyErrorKind::PolicyRefMismatch,
        NetworkPolicyErrorKind::GatewayStopping,
    ] {
        let failures = if kind == active_kind {
            active_failures
        } else {
            0
        };
        pplx_space_metrics::gateway::policy_hydration_consecutive_failures(kind.as_str(), failures);
    }
}

async fn wait_for_shutdown(duration: Duration, shutdown_token: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown_token.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn record_tcp_pending_wait(result: &str, started_at: Instant) {
    pplx_space_metrics::gateway::pending_tcp_wait(result);
    pplx_space_metrics::gateway::pending_tcp_wait_ms(
        result,
        started_at.elapsed().as_millis() as f64,
    );
}

fn record_pending_tcp_slow_wait(snapshot: &NetworkStateSnapshot, started_at: Instant) {
    pplx_space_metrics::gateway::pending_tcp_slow_wait(
        effective_network_policy_ref_authority_label(&snapshot.installed_policy_ref),
    );
    let wait_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    // Only the hydration wait reaches here; unavailable interception
    // dependencies deny the connection instead of holding it. Carry the
    // readiness anyway so a responder is not sent hunting for a CA problem
    // that this wait is not evidence of.
    warn!(
        wait_ms,
        state = snapshot.mode.as_str(),
        interception_readiness = snapshot.interception_readiness.as_str(),
        "TCP connection waited for network policy hydration"
    );
}

fn record_not_ready_denial(protocol: &str, reason: PolicyNotReady) {
    match reason {
        PolicyNotReady::NoIdentity => pplx_space_metrics::gateway::no_identity_denied(protocol),
        PolicyNotReady::Draining | PolicyNotReady::Stopped => {
            pplx_space_metrics::gateway::draining_closed(protocol)
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
