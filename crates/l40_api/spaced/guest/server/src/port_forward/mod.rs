//! Loopback → external port bridge for explicitly authorized ports.
//!
//! SPACE's host-side `expose_port` DNATs incoming traffic to the VM's
//! external interface (`vpeer_ip`). A guest process bound only to
//! `127.0.0.1:N` is therefore invisible to the port-proxy and clients see
//! HTTP 502. This module makes `127.0.0.1`-bound listeners reachable on
//! `external_ip:N` for ports the host has explicitly authorized via the
//! `Networking` bridge-guest RPCs.
//!
//! ## Design
//!
//! For each authorized port, the manager spawns one driver task that binds
//! `external_ip:port`, retrying transient failures a bounded number of
//! times ([`BIND_RETRY_ATTEMPTS`] × [`BIND_RETRY_DELAY`] — the bind can
//! race the guest address coming up during boot or a wake):
//!
//! - **Bind succeeds** → run an accept loop that forwards each incoming TCP
//!   connection to `127.0.0.1:port` (a per-connection lazy `connect()`).
//!   Status becomes `LoopbackForwarded`. If the user app's loopback
//!   listener isn't up yet, the per-connection dial returns `ECONNREFUSED`,
//!   the inbound connection closes, and the next inbound connection
//!   re-tries — recovery happens at TCP-handshake time, no polling needed.
//! - **Bind fails with `EADDRINUSE`** → another process (the user app,
//!   bound on `0.0.0.0:port` or `external_ip:port` directly) already owns
//!   the address. Stand aside immediately — no retry; host-side DNAT lands
//!   on them. Status becomes `Direct`.
//! - **Bind fails for any other reason on every attempt** → `BindConflict`,
//!   with the final bind error preserved in the published detail so the
//!   exposure row records the real cause (typically a retry-exhausted
//!   `EADDRNOTAVAIL` while the guest address is still coming up — not a
//!   conflicting listener).
//!
//! `WaitingForListener` is published only as the seed value of the per-port
//! `watch::channel` while the bind-IP autodetect or the bounded bind retry
//! is still in flight (bounded by `MAX_OPEN_WAIT`). Once those resolve, the
//! status moves to one of the four terminal states above;
//! `WaitingForListener` never reappears as a steady state for the port's
//! lifetime.
//!
//! ## Steady-state classification and re-arm
//!
//! A **healthy** status (`Direct`, `LoopbackForwarded`) is decided once and
//! held until the port is explicitly torn down: subsequent
//! `OpenLoopbackBridge` calls short-circuit and re-read the cached status —
//! they do **not** retry the bind. This is a deliberate behavior change vs.
//! the previous `/proc/net/tcp` polling design, which re-classified every
//! tick and would have transitioned a port out of `Direct` once the user's
//! wildcard listener exited.
//!
//! A **failed** terminal status (`BindConflict`, `Failed`) is *not* sticky
//! across opens: a later `OpenLoopbackBridge` for the same port tears the
//! dead entry down and re-drives the bind. The exposure reconciler retries
//! `open` every tick precisely because it wants recovery — without the
//! re-arm, one lost bind race (address not yet up during a wake) would
//! leave the exposed port answering 502 for the remaining lifetime of the
//! spaced-guest-svc process no matter how often the reconciler retried.
//!
//! Concrete consequence of the healthy-status caching: if a user app holds
//! `0.0.0.0:port` at the moment of the first `OpenLoopbackBridge` (status →
//! `Direct`) and later exits, the host-side DNAT continues to land on
//! `external_ip:port` where nothing listens until one of:
//!
//! - The caller invokes `CloseLoopbackBridge` followed by a fresh
//!   `OpenLoopbackBridge` — explicit re-arm.
//! - `spaced-guest-svc` restarts (sandbox reboot, crash, etc.) — its
//!   per-process `service_generation` changes, the cluster reconciler
//!   detects the mismatch, and the next `OpenLoopbackBridge` lands on a
//!   fresh `state.ports` map with no cached entry.
//! - The sandbox migrates to a different worker — the cluster reconciler's
//!   `last_applied_node_id` check forces a re-apply on the new node,
//!   whose `LoopbackBridgeManager` has no cached entry.
//!
//! For SPACE's current workloads (code-runtime and e2b SDK
//! `expose`/`unexpose`) the user-app-exits-while-spaced-svc-stays-up
//! pattern does not occur in practice, so this trade was accepted to
//! eliminate the self-introspection bug. If a workload appears that needs
//! per-port `Direct` recovery within one spaced-svc lifetime, the cleanest
//! options are (a) have the caller invoke explicit close+open on detection,
//! or (b) extend the failed-status re-arm to `Direct` (no `/proc/net/tcp`
//! introspection — the new bind is the test).
//!
//! ## Cleanup
//!
//! Per-port lifecycle is wired through cancellation tokens:
//!
//! - `LoopbackBridgeManager::close(port)` cancels the per-port token →
//!   forwarder accept loop exits, listener drops, all in-flight
//!   `copy_bidirectional` futures complete.
//! - The parent `shutdown_token` (passed to `LoopbackBridgeManager::new`)
//!   is the ancestor of every per-port token, so service shutdown
//!   cascades to every running forwarder in one wave.
//!
//! ## History
//!
//! Earlier revisions ran a 1 Hz watcher loop that polled `/proc/net/tcp{,6}`
//! and re-classified the listener every tick (`Direct`,
//! `LoopbackForwarded`, `WaitingForListener`). That design had a
//! self-introspection bug: the watcher saw its own forwarder's listener on
//! `external_ip:port` and concluded "the user app is now bound directly,"
//! tore down the forwarder mid-flight, killed every live connection, then
//! re-installed the forwarder on the next tick. HTTP requests usually
//! completed inside one tick so they masked it; long-lived TCP
//! (WebSocket / SSE / gRPC streaming) hit it consistently with
//! `ConnectionClosedError code=1006` ~1 s after handshake.
//!
//! Replacing the polling loop with a one-shot bind + per-connection lazy
//! dial removes the entire `/proc/net/tcp` introspection path (the bug
//! source) while preserving the user-visible behavior — the per-connection
//! dial recovers as soon as the user's loopback listener appears, no
//! sandbox-internal polling required.

use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_spaced_common::proto::bridge_guest::LoopbackBridgeStatus;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore, watch};
use tokio_util::sync::CancellationToken;

mod forward;

/// Pluggable bind step for the per-port forwarder driver. Production code
/// binds a real [`TcpListener`]; tests inject factories that fail in
/// controlled ways to drive the bind-retry and re-arm paths.
type ListenerFactory = Arc<
    dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = std::io::Result<TcpListener>> + Send>>
        + Send
        + Sync,
>;

fn system_listener_factory() -> ListenerFactory {
    Arc::new(|addr| Box::pin(TcpListener::bind(addr)))
}

/// Bind attempts per [`port_forwarder::run`] invocation. The bind can race
/// the guest's link-local address coming up (cold boot, wake); a handful of
/// short retries absorbs that window without delaying a genuinely
/// conflicted port for long.
const BIND_RETRY_ATTEMPTS: usize = 4;

/// Delay between bind attempts. Combined with [`BIND_RETRY_ATTEMPTS`] the
/// retry window is ~1.5 s — comfortably inside [`MAX_OPEN_WAIT`] so a
/// caller that passes a generous `wait_timeout` still observes the terminal
/// status from this driver run.
const BIND_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Default service-side cap on how long `OpenLoopbackBridge` blocks waiting
/// for the per-port driver to publish a terminal status if the caller didn't
/// pass a timeout. Covers the common case: bind-IP autodetect resolving
/// within a second of guest boot (`autodetect_bind_ip` retries up to five
/// times at 1 s cadence) and the bind landing on its first attempt. It
/// deliberately does **not** cover the worst case — cold autodetect can run
/// ~5 s and the bounded bind retry ([`BIND_RETRY_ATTEMPTS`] ×
/// [`BIND_RETRY_DELAY`]) adds up to ~1.5 s more — in which case `open`
/// returns `WaitingForListener` at the deadline with the port still armed,
/// and the caller (the exposure reconciler) observes the terminal status on
/// a later tick.
const DEFAULT_OPEN_WAIT: Duration = Duration::from_millis(2_000);

/// Hard cap to keep request handlers well under the upstream RPC budget.
const MAX_OPEN_WAIT: Duration = Duration::from_secs(10);

/// Maximum number of concurrently proxied TCP connections across every
/// forwarded port. Each connection owns two stream FDs, so this is also the
/// dominant input to the spaced.service `LimitNOFILE` budget.
const MAX_ACTIVE_FORWARD_CONNECTIONS: usize = 256;

// Above the 128-exposure API admission limit to accommodate older templates'
// existing authorizations, but leave FD headroom for RPCs and process pipes.
const MAX_AUTHORIZED_PORTS: usize = 512;

/// Close a proxied connection when neither direction transfers a byte for
/// this long. Active WebSocket, SSE, and gRPC streams are unaffected.
const FORWARD_CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone)]
pub struct LoopbackBridgeManager {
    state: Arc<ManagerState>,
}

impl std::fmt::Debug for LoopbackBridgeManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoopbackBridgeManager")
            .field("shutting_down", &self.state.shutdown_token.is_cancelled())
            .finish_non_exhaustive()
    }
}

struct ManagerState {
    bind_ip: tokio::sync::OnceCell<IpAddr>,
    ports: Mutex<HashMap<u16, AuthorizedPort>>,
    shutdown_token: CancellationToken,
    listener_factory: ListenerFactory,
    active_connections: Arc<Semaphore>,
    connection_idle_timeout: Duration,
}

impl ManagerState {
    /// Resolve the bind IP, populating the `OnceCell` on first call. Returns
    /// `None` if autodetect concludes the guest has no usable non-loopback
    /// IPv4. Concurrent callers (the per-port watchers, `spawn_warm`, etc.)
    /// race on the same `get_or_try_init` and only one runs the autodetect.
    async fn bind_ip(&self) -> Option<IpAddr> {
        self.bind_ip
            .get_or_try_init(|| async {
                autodetect_bind_ip()
                    .await
                    .ok_or("no non-loopback IPv4 found on the guest")
            })
            .await
            .copied()
            .ok()
    }
}

/// Status + operator-facing detail published by the per-port driver. The
/// detail honors the proto contract that non-success outcomes carry a
/// free-text reason; it is produced where the outcome is decided so a
/// `BindConflict` records the *final bind error* (retry-exhausted
/// `EADDRNOTAVAIL` during a wake reads very differently from a genuinely
/// held address). Success statuses (`Direct`, `LoopbackForwarded`) carry
/// no detail.
#[derive(Clone, Debug)]
struct PortBridgeState {
    status: LoopbackBridgeStatus,
    detail: Option<String>,
}

impl PortBridgeState {
    fn new(status: LoopbackBridgeStatus, detail: Option<String>) -> Self {
        Self { status, detail }
    }
}

struct AuthorizedPort {
    status: watch::Sender<PortBridgeState>,
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl LoopbackBridgeManager {
    pub(crate) async fn shutdown(&self) {
        self.state.shutdown_token.cancel();
        let ports = std::mem::take(&mut *self.state.ports.lock().await);
        for (_, port) in ports {
            if let Err(error) = port.join.await {
                tracing::warn!(%error, "port forwarder failed during shutdown");
            }
        }
    }

    /// `shutdown_token` is the parent cancellation scope. Per-port forwarder
    /// tasks are spawned with descendants of this token so service shutdown
    /// cascades to all background work — no leaked listeners on
    /// reload/restart.
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            state: Arc::new(ManagerState {
                bind_ip: tokio::sync::OnceCell::new(),
                ports: Mutex::new(HashMap::new()),
                shutdown_token,
                listener_factory: system_listener_factory(),
                active_connections: Arc::new(Semaphore::new(MAX_ACTIVE_FORWARD_CONNECTIONS)),
                connection_idle_timeout: FORWARD_CONNECTION_IDLE_TIMEOUT,
            }),
        }
    }

    /// Test constructor: pre-resolves the bind IP (so no `eth0` scan runs)
    /// and injects the bind step.
    #[cfg(test)]
    fn for_tests(
        shutdown_token: CancellationToken,
        bind_ip: IpAddr,
        listener_factory: ListenerFactory,
        active_connection_limit: usize,
        connection_idle_timeout: Duration,
    ) -> Self {
        Self {
            state: Arc::new(ManagerState {
                bind_ip: tokio::sync::OnceCell::new_with(Some(bind_ip)),
                ports: Mutex::new(HashMap::new()),
                shutdown_token,
                listener_factory,
                active_connections: Arc::new(Semaphore::new(active_connection_limit)),
                connection_idle_timeout,
            }),
        }
    }

    /// Resolve `bind_ip` ahead of the first request. `autodetect_bind_ip`
    /// retries up to 5× with 1 s sleeps between attempts, so on a cold
    /// process it can take ~5 s. Doing that on the `OpenLoopbackBridge`
    /// request path would push worst-case RPC latency past
    /// [`MAX_OPEN_WAIT`]; warming on service start keeps the first call
    /// in line with the documented cap.
    pub(crate) fn spawn_warm(&self) {
        let state = self.state.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = state.shutdown_token.cancelled() => {}
                _ = state.bind_ip() => {}
            }
        });
    }

    /// Authorize and bridge `port`. Returns the status reached within
    /// `wait_timeout` (capped to [`MAX_OPEN_WAIT`]). The port stays armed
    /// regardless of the result; a `WaitingForListener` return is a
    /// "bind-IP autodetect or bind retry still in flight, retry shortly"
    /// signal, not a failure. Healthy terminal statuses (`Direct`,
    /// `LoopbackForwarded`) are cached and reused; failed terminal statuses
    /// (`BindConflict`, `Failed`) cause the next `open` for the port to
    /// re-drive the bind.
    pub async fn open(
        &self,
        port: u16,
        wait_timeout: Option<Duration>,
    ) -> (LoopbackBridgeStatus, Option<String>) {
        if self.state.shutdown_token.is_cancelled() {
            return (
                LoopbackBridgeStatus::Failed,
                Some("guest shutting down".into()),
            );
        }
        if port == 0 {
            return (
                LoopbackBridgeStatus::Failed,
                Some("port must be non-zero".into()),
            );
        }

        let wait = wait_timeout
            .map(|d| d.min(MAX_OPEN_WAIT))
            .unwrap_or(DEFAULT_OPEN_WAIT);
        let deadline = tokio::time::Instant::now() + wait;

        // Spawn the per-port forwarder task BEFORE resolving `bind_ip`.
        // The task resolves it itself (cancellation-aware) and the channel
        // seed is `WaitingForListener`, so callers polling status during
        // cold-start autodetect see a stable value — the port is registered
        // in `state.ports` and will reach a terminal status as soon as
        // autodetect finishes.
        let mut rx = {
            let mut guard = self.state.ports.lock().await;
            if self.state.shutdown_token.is_cancelled() {
                return (
                    LoopbackBridgeStatus::Failed,
                    Some("guest shutting down".into()),
                );
            }
            let reusable = guard
                .get(&port)
                .filter(|existing| authorized_port_is_reusable(existing));
            if let Some(existing) = reusable {
                existing.status.subscribe()
            } else {
                if !guard.contains_key(&port) && guard.len() >= MAX_AUTHORIZED_PORTS {
                    return (LoopbackBridgeStatus::Failed, Some("authorized port capacity exhausted; close an unused port before retrying".into()));
                }
                // Either the port is new, its cached status is a terminal
                // failure, or its tracked listener task ended unexpectedly.
                // Such an entry is re-armed rather than reused: the caller is
                // explicitly asking for the port again (the exposure
                // reconciler retries every tick), and the condition that
                // failed the bind — typically the guest address not yet
                // present during a wake — may have cleared. Healthy
                // entries (`Direct`/`LoopbackForwarded`, or a driver still
                // resolving) keep the cached classification.
                if let Some(stale) = guard.remove(&port) {
                    stale.cancel.cancel();
                    stale.join.abort();
                }
                // Per-port cancel as a child of the service shutdown so
                // `shutdown_token.cancel()` cascades to the forwarder.
                let cancel = self.state.shutdown_token.child_token();
                let (tx, rx) = watch::channel(PortBridgeState::new(
                    LoopbackBridgeStatus::WaitingForListener,
                    None,
                ));
                let state = self.state.clone();
                let task = port_forwarder::run(state, port, tx.clone(), cancel.clone());
                let join = tokio::spawn(task);
                guard.insert(
                    port,
                    AuthorizedPort {
                        status: tx,
                        cancel,
                        join,
                    },
                );
                rx
            }
        };

        loop {
            if self.state.shutdown_token.is_cancelled() || rx.has_changed().is_err() {
                return (
                    LoopbackBridgeStatus::Failed,
                    Some("port authorization closed".into()),
                );
            }
            let state = rx.borrow_and_update().clone();
            if status_is_terminal(state.status) {
                return (state.status, state.detail);
            }
            if tokio::time::Instant::now() >= deadline {
                // Final re-borrow before reporting Waiting: a terminal
                // send that landed between the borrow above and the
                // deadline check would otherwise be hidden by a stale
                // local copy.
                let final_state = rx.borrow().clone();
                if status_is_terminal(final_state.status) {
                    return (final_state.status, final_state.detail);
                }
                return (
                    LoopbackBridgeStatus::WaitingForListener,
                    Some(
                        "bind-IP autodetect or bind retries still in flight after wait_timeout; \
                         port stays armed"
                            .to_string(),
                    ),
                );
            }
            tokio::select! {
                result = rx.changed() => {
                    if result.is_err() {
                        return (LoopbackBridgeStatus::Failed, Some("port authorization closed".into()));
                    }
                }
                _ = self.state.shutdown_token.cancelled() => {
                    return (LoopbackBridgeStatus::Failed, Some("guest shutting down".into()));
                }
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Drop authorization for `port`. The forwarder task (and any in-flight
    /// `copy_bidirectional` it owns) are cancelled. Idempotent.
    pub async fn close(&self, port: u16) {
        let mut ports = self.state.ports.lock().await;
        if let Some(mut authorized) = ports.remove(&port) {
            authorized.cancel.cancel();
            if tokio::time::timeout(Duration::from_secs(1), &mut authorized.join)
                .await
                .is_err()
            {
                authorized.join.abort();
                let _ = authorized.join.await;
            }
        }
    }
}

fn status_is_terminal(status: LoopbackBridgeStatus) -> bool {
    matches!(
        status,
        LoopbackBridgeStatus::Direct
            | LoopbackBridgeStatus::LoopbackForwarded
            | LoopbackBridgeStatus::BindConflict
            | LoopbackBridgeStatus::Failed
    )
}

fn authorized_port_is_reusable(port: &AuthorizedPort) -> bool {
    match port.status.borrow().status {
        LoopbackBridgeStatus::Direct => true,
        LoopbackBridgeStatus::WaitingForListener | LoopbackBridgeStatus::LoopbackForwarded => {
            !port.join.is_finished()
        }
        LoopbackBridgeStatus::Unspecified
        | LoopbackBridgeStatus::BindConflict
        | LoopbackBridgeStatus::Failed => false,
    }
}

mod port_forwarder {
    use super::*;

    /// Outcome of the driver's bind step. Closed set: every arm maps to
    /// exactly one published [`LoopbackBridgeStatus`] (or, for `Cancelled`,
    /// to publishing nothing at all).
    pub(super) enum BindOutcome {
        Bound(TcpListener),
        AddrInUse,
        Failed(std::io::Error),
        Cancelled,
    }

    /// Per-port forwarder driver. Resolves the bind IP, binds
    /// `bind_ip:port` through the manager's listener factory, classifies
    /// the outcome, and publishes the resulting status. A successful driver
    /// then owns the accept loop until cancellation, so the manager's join
    /// handle is also the listener-lifetime boundary.
    pub(super) async fn run(
        state: Arc<ManagerState>,
        port: u16,
        status: watch::Sender<PortBridgeState>,
        cancel: CancellationToken,
    ) {
        // Resolve the bind IP first, cancellation-aware. The channel was
        // seeded with `WaitingForListener`, so callers polling status
        // during cold-start autodetect see a stable, contract-honoring
        // value — the port is armed regardless of how long autodetect
        // takes. `Failed` is published only if autodetect concludes the
        // guest has no usable IPv4 at all (caller should not retry).
        let bind_ip = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            ip = state.bind_ip() => match ip {
                Some(ip) => ip,
                None => {
                    status.send_replace(PortBridgeState::new(
                        LoopbackBridgeStatus::Failed,
                        Some(format!(
                            "no usable non-loopback IPv4 on the guest to bridge port {port}"
                        )),
                    ));
                    return;
                }
            }
        };
        let bind_addr = SocketAddr::new(bind_ip, port);
        // We always dial v4 loopback. `[::1]:port`-only listeners (no
        // matching v4 binding) are an extremely rare configuration and
        // explicitly outside the bridge's scope — the user can bind v4 or
        // a wildcard if they need bridge coverage.
        let dial_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

        let new_state =
            match bind_with_retry(state.listener_factory.clone(), bind_addr, &cancel).await {
                BindOutcome::Bound(listener) => {
                    // Keep the forwarder inside this tracked per-port task so
                    // `close` owns the listener's complete lifetime. The
                    // forwarder owns the accept loop and per-connection lazy
                    // `connect(127.0.0.1:port)` — if no listener is up yet,
                    // each accept dials, gets ECONNREFUSED, closes the inbound
                    // connection, and retries on the next accept. No
                    // /proc/net/tcp polling required.
                    let forwarder_cancel = cancel.child_token();
                    status.send_replace(PortBridgeState::new(
                        LoopbackBridgeStatus::LoopbackForwarded,
                        None,
                    ));
                    forward::run(
                        listener,
                        bind_addr,
                        dial_addr,
                        state.active_connections.clone(),
                        state.connection_idle_timeout,
                        forwarder_cancel,
                    )
                    .await;
                    return;
                }
                BindOutcome::AddrInUse => {
                    // Some other process — typically the user app bound on
                    // `0.0.0.0:port` or `bind_ip:port` — already owns the
                    // address. Stand aside; host-side DNAT lands on them
                    // directly.
                    tracing::info!(
                        %bind_addr,
                        "port-forward: external bind already in use, reporting Direct"
                    );
                    PortBridgeState::new(LoopbackBridgeStatus::Direct, None)
                }
                BindOutcome::Failed(error) => {
                    tracing::warn!(
                        %bind_addr,
                        %error,
                        attempts = BIND_RETRY_ATTEMPTS,
                        "port-forward: bind failed after bounded retries"
                    );
                    PortBridgeState::new(
                        LoopbackBridgeStatus::BindConflict,
                        Some(format!(
                            "{bind_addr} bind failed after {BIND_RETRY_ATTEMPTS} attempts: {error}"
                        )),
                    )
                }
                BindOutcome::Cancelled => return,
            };
        status.send_replace(new_state);
        // Driver task ends here; the spawned forwarder (if any) keeps
        // running on `forwarder_cancel`, which is a child of the per-port
        // `cancel` token held in `AuthorizedPort`. `close(port)` and
        // service shutdown both cascade to it.
    }

    /// Bind `addr` through `factory`, retrying transient failures up to
    /// [`BIND_RETRY_ATTEMPTS`] times with [`BIND_RETRY_DELAY`] between
    /// attempts. `EADDRINUSE` is terminal on the first hit (another process
    /// legitimately owns the address — that is the `Direct` contract, not a
    /// transient race). Every failed attempt is logged; cancellation aborts
    /// promptly, including mid-backoff.
    pub(super) async fn bind_with_retry(
        factory: ListenerFactory,
        addr: SocketAddr,
        cancel: &CancellationToken,
    ) -> BindOutcome {
        let mut last_error = None;
        for attempt in 1..=BIND_RETRY_ATTEMPTS {
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return BindOutcome::Cancelled,
                result = factory(addr) => result,
            };
            match result {
                Ok(listener) => return BindOutcome::Bound(listener),
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    return BindOutcome::AddrInUse;
                }
                Err(error) => {
                    tracing::warn!(
                        %addr,
                        %error,
                        attempt,
                        max_attempts = BIND_RETRY_ATTEMPTS,
                        "port-forward: bind attempt failed"
                    );
                    last_error = Some(error);
                    if attempt < BIND_RETRY_ATTEMPTS {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => return BindOutcome::Cancelled,
                            _ = tokio::time::sleep(BIND_RETRY_DELAY) => {}
                        }
                    }
                }
            }
        }
        BindOutcome::Failed(last_error.expect("loop ran at least one failed attempt"))
    }
}

/// Interface name SPACE attaches to firecracker guests. The host-side tap is
/// the only path traffic from outside reaches the guest on, so the bridge
/// must bind to its IP — never an unrelated secondary interface (docker0,
/// IPVLAN, secondary ENI) the guest might also have.
const EXPECTED_GUEST_IFACE: &str = "eth0";

/// Pick the IPv4 address bound on `eth0`. Firecracker guests run on a
/// link-local /30 (e.g., `169.254.0.21`) — the same destination SPACE's
/// host-side `expose_port` DNATs to. We deliberately accept any non-
/// loopback / non-unspecified / non-multicast IPv4 on `eth0` rather than
/// pattern-match on `169.254.0.0/16` so this still works on hypothetical
/// non-firecracker drivers that put the guest on a routable subnet.
async fn autodetect_bind_ip() -> Option<IpAddr> {
    for _ in 0..5 {
        if let Some(ip) = scan_eth0_ipv4() {
            return Some(IpAddr::V4(ip));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    scan_eth0_ipv4().map(IpAddr::V4)
}

fn scan_eth0_ipv4() -> Option<Ipv4Addr> {
    use nix::ifaddrs::getifaddrs;
    use nix::net::if_::InterfaceFlags;

    let iter = match getifaddrs() {
        Ok(iter) => iter,
        Err(err) => {
            tracing::trace!(%err, "port-forward: getifaddrs failed");
            return None;
        }
    };
    for ifaddr in iter {
        if ifaddr.interface_name != EXPECTED_GUEST_IFACE {
            continue;
        }
        if !ifaddr.flags.contains(InterfaceFlags::IFF_UP)
            || !ifaddr.flags.contains(InterfaceFlags::IFF_RUNNING)
            || ifaddr.flags.contains(InterfaceFlags::IFF_LOOPBACK)
        {
            continue;
        }
        let Some(addr) = ifaddr.address.and_then(|a| a.as_sockaddr_in().copied()) else {
            continue;
        };
        let ip = addr.ip();
        if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
            continue;
        }
        return Some(ip);
    }
    None
}

#[cfg(test)]
mod tests;
