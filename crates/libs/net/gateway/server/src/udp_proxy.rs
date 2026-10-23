//! Fully transparent UDP proxy using TPROXY.
//!
//! Uses a TPROXY listener for guest-originated packets only. Each allowed
//! guest flow gets a connected upstream socket bound to the guest source, plus
//! a response socket bound to the original destination so replies are spoofed
//! back to the guest with the expected source.
//!
//! The session identity is the full guest flow (`guest_src`, `orig_dst`), not
//! only `orig_dst`, so concurrent guest sockets talking to the same upstream
//! destination do not collide.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use lru::LruCache;
use pplx_space_metrics::gateway::{DnsAttributionCacheObservationSource, UdpDatagramRoute};
use pplx_space_net_common::{EgressDecision, NetworkPolicyV1};
use pplx_space_net_gateway_client::OUTBOUND_MARK;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::net::UdpSocket;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, warn};

use crate::UDP_MAPPING_TIMEOUT_SECS;
use crate::egress::{
    EgressContext, EgressPhase, EgressProtocol, admit_udp as evaluate_udp_egress, evaluate_egress,
};
use crate::known_egress::KnownEgressState;
use crate::orig_dst;
use crate::reserved_dns::{ReservedDnsCore, ReservedResolver, ResolveOutcome};
use crate::resource_budget::{MAX_ACTIVE_RESERVED_DNS_DATAGRAMS, MAX_ACTIVE_UDP_SESSIONS};
use crate::state::{GatewayNetworkState, PolicyNotReady, TrafficPolicy};
use crate::stats::ProxyStats;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FlowKey {
    guest_src: SocketAddr,
    orig_dst: SocketAddr,
}

fn evaluate_reserved_dns_egress(
    destination: SocketAddr,
    policy: &NetworkPolicyV1,
) -> EgressDecision {
    evaluate_egress(
        EgressContext::Admission(EgressProtocol::Udp),
        destination,
        false,
        policy,
    )
}

fn udp_datagram_route(destination: SocketAddr, is_reserved_dns: bool) -> UdpDatagramRoute {
    if is_reserved_dns {
        return UdpDatagramRoute::ReservedDns;
    }
    if destination.port() == 53 {
        return UdpDatagramRoute::GenericDns;
    }
    UdpDatagramRoute::Generic
}

const UDP_PENDING_MAX_DATAGRAMS: usize = 256;
const UDP_PENDING_MAX_DATAGRAMS_PER_FLOW: usize = 32;
const UDP_PENDING_MAX_BYTES: usize = 1024 * 1024;
/// How long a guest UDP datagram is buffered while the gateway is in a
/// transient `PendingPolicy` window (e.g. a mid-life policy refresh) before it
/// is dropped. Buffered datagrams are replayed once the policy reaches `Ready`,
/// so this is the UDP analog of the TCP egress path's `wait_for_tcp_policy`
/// grace — held to the hydration budget so it can absorb a full policy-hydration
/// cycle (`HYDRATION_DEADLINE`). DNS is the motivating case: a query held
/// across the refresh and replayed within this window still gets its answer,
/// whereas a shorter grace dropped it silently mid-refresh and surfaced as a
/// resolver timeout (gaierror) in the guest.
const UDP_PENDING_MAX_AGE: Duration = crate::state::TCP_PENDING_TIMEOUT;
const UDP_PENDING_REPLAY_INTERVAL: Duration = Duration::from_millis(25);
const UDP_PENDING_REPLAY_BATCH_DATAGRAMS: usize = 16;
/// A response task emits at most one terminal event, so a queue sized to the
/// session cap cannot grow without bound and cannot block healthy task exits.
const UDP_SESSION_EVENT_QUEUE_CAPACITY: usize = MAX_ACTIVE_UDP_SESSIONS;
const UDP_SESSION_CREATE_WARNING_INTERVAL: Duration = Duration::from_secs(60);
const UDP_SESSION_TASK_STOP_TIMEOUT: Duration = Duration::from_millis(10);

#[derive(Clone, Copy)]
enum UdpSessionSocketStage {
    Upstream,
    Response,
}

impl UdpSessionSocketStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Upstream => "upstream",
            Self::Response => "response",
        }
    }

    const fn socket_error_operation(self) -> &'static str {
        match self {
            Self::Upstream => "create_upstream_socket",
            Self::Response => "create_response_socket",
        }
    }
}

fn udp_session_create_failure_reason(stage: UdpSessionSocketStage, reason: &str) -> String {
    format!("{}: {reason}", stage.socket_error_operation())
}

fn udp_session_limit_reached(active_sessions: usize) -> bool {
    active_sessions >= MAX_ACTIVE_UDP_SESSIONS
}

#[derive(Debug)]
struct PendingUdpDatagram {
    flow: FlowKey,
    bytes: Vec<u8>,
    received_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingUdpDropAccounting {
    datagrams: u64,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingUdpClearReason {
    NonReadyPolicy,
}

impl PendingUdpClearReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NonReadyPolicy => "dropped_non_ready",
        }
    }
}

#[derive(Default)]
struct PendingUdpQueue {
    datagrams: VecDeque<PendingUdpDatagram>,
    bytes: usize,
    per_flow: HashMap<FlowKey, usize>,
    known_egress_state: KnownEgressState,
}

impl PendingUdpQueue {
    fn new(known_egress_state: KnownEgressState) -> Self {
        Self {
            known_egress_state,
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.datagrams.is_empty()
    }

    fn push(
        &mut self,
        flow: FlowKey,
        bytes: &[u8],
        now: Instant,
        sandbox_id: &'static str,
        stats: &ProxyStats,
    ) {
        self.evict_expired(now, sandbox_id, stats);
        let flow_count = self.per_flow.get(&flow).copied().unwrap_or_default();
        if flow_count >= UDP_PENDING_MAX_DATAGRAMS_PER_FLOW {
            pplx_space_metrics::gateway::udp_pending_buffer("drop_flow_limit");
            pplx_space_metrics::gateway::pending_udp_flow_dropped("datagram_limit");
            debug!(
                sandbox_id,
                ?flow,
                "dropped pending UDP datagram at per-flow limit"
            );
            return;
        }
        if self.datagrams.len() >= UDP_PENDING_MAX_DATAGRAMS {
            pplx_space_metrics::gateway::udp_pending_buffer("drop_gateway_datagram_limit");
            pplx_space_metrics::gateway::pending_udp_datagram("drop_gateway_datagram_limit");
            debug!(
                sandbox_id,
                "dropped pending UDP datagram at gateway datagram limit"
            );
            return;
        }
        if self.bytes.saturating_add(bytes.len()) > UDP_PENDING_MAX_BYTES {
            pplx_space_metrics::gateway::udp_pending_buffer("drop_gateway_byte_limit");
            pplx_space_metrics::gateway::pending_udp_bytes_dropped(
                "gateway_byte_limit",
                bytes.len() as u64,
            );
            debug!(
                sandbox_id,
                "dropped pending UDP datagram at gateway byte limit"
            );
            return;
        }

        self.bytes += bytes.len();
        *self.per_flow.entry(flow).or_default() += 1;
        self.datagrams.push_back(PendingUdpDatagram {
            flow,
            bytes: bytes.to_vec(),
            received_at: now,
        });
        pplx_space_metrics::gateway::udp_pending_buffer("buffered");
        pplx_space_metrics::gateway::pending_udp_datagram("queued");
        self.update_stats(stats);
    }

    fn pop_front(&mut self) -> Option<PendingUdpDatagram> {
        let datagram = self.datagrams.pop_front()?;
        self.bytes = self.bytes.saturating_sub(datagram.bytes.len());
        decrement_flow_count(&mut self.per_flow, datagram.flow);
        Some(datagram)
    }

    fn clear_dropped(
        &mut self,
        reason: PendingUdpClearReason,
        sandbox_id: &'static str,
        stats: &ProxyStats,
    ) -> PendingUdpDropAccounting {
        let dropped = PendingUdpDropAccounting {
            datagrams: self.datagrams.len() as u64,
            bytes: self.bytes as u64,
        };
        for datagram in &self.datagrams {
            self.known_egress_state.report_dns_failure(
                sandbox_id,
                datagram.flow.orig_dst,
                &datagram.bytes,
                reason.as_str(),
            );
        }
        self.datagrams.clear();
        self.per_flow.clear();
        self.bytes = 0;
        if dropped.datagrams > 0 {
            let reason = reason.as_str();
            pplx_space_metrics::gateway::udp_pending_buffer(reason);
            pplx_space_metrics::gateway::pending_udp_datagrams(reason, dropped.datagrams);
            pplx_space_metrics::gateway::pending_udp_bytes_dropped(reason, dropped.bytes);
        }
        self.update_stats(stats);
        dropped
    }

    fn evict_expired(&mut self, now: Instant, sandbox_id: &'static str, stats: &ProxyStats) {
        let mut evicted = 0_u64;
        while self
            .datagrams
            .front()
            .is_some_and(|datagram| now.duration_since(datagram.received_at) >= UDP_PENDING_MAX_AGE)
        {
            if let Some(datagram) = self.pop_front() {
                evicted = evicted.saturating_add(1);
                self.known_egress_state.report_dns_failure(
                    sandbox_id,
                    datagram.flow.orig_dst,
                    &datagram.bytes,
                    "pending_policy_timeout",
                );
                pplx_space_metrics::gateway::pending_timeout("udp");
                pplx_space_metrics::gateway::pending_udp_datagram("expired");
                pplx_space_metrics::gateway::pending_udp_bytes_dropped(
                    "expired",
                    datagram.bytes.len() as u64,
                );
                debug!(
                    sandbox_id,
                    flow = ?datagram.flow,
                    "expired pending UDP datagram before policy hydration"
                );
            }
        }
        if evicted > 0 {
            self.update_stats(stats);
        }
    }

    fn update_stats(&self, stats: &ProxyStats) {
        stats
            .udp_pending_datagrams
            .store(self.datagrams.len() as u64, Ordering::Relaxed);
        stats
            .udp_pending_bytes
            .store(self.bytes as u64, Ordering::Relaxed);
        pplx_space_metrics::gateway::udp_pending(self.datagrams.len() as u64, self.bytes as u64);
    }
}

fn decrement_flow_count(counts: &mut HashMap<FlowKey, usize>, flow: FlowKey) {
    match counts.get_mut(&flow) {
        Some(count) if *count > 1 => *count -= 1,
        Some(_) => {
            counts.remove(&flow);
        }
        None => {}
    }
}

struct Session {
    session_id: u64,
    upstream_socket: Arc<UdpSocket>,
    last_active_ms: Arc<AtomicU64>,
    cancel_token: CancellationToken,
    dead: Arc<AtomicBool>,
    completion: SessionCompletion,
    task: tokio::task::JoinHandle<()>,
}

impl Session {
    fn touch(&self, now_ms: u64) {
        self.last_active_ms.store(now_ms, Ordering::Relaxed);
    }

    fn is_idle(&self, now_ms: u64, timeout_ms: u64) -> bool {
        session_is_idle(
            now_ms,
            self.last_active_ms.load(Ordering::Relaxed),
            timeout_ms,
        )
    }

    fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    fn abort(self, outcome: UdpSessionOutcome) -> tokio::task::JoinHandle<()> {
        self.completion.mark(outcome);
        self.cancel_token.cancel();
        self.task.abort();
        self.task
    }

    async fn stop(self, outcome: UdpSessionOutcome) {
        let mut task = self.abort(outcome);
        let _ = tokio::time::timeout(UDP_SESSION_TASK_STOP_TIMEOUT, &mut task).await;
    }
}

/// Run the UDP transparent proxy on a pre-created TPROXY listener socket.
///
/// `sandbox_id` is recorded as a structured field on every log emitted by this
/// proxy and the per-flow tasks it spawns, so all proxy-side log lines can be
/// filtered to the owning sandbox. `&'static str` mirrors that the gateway is
/// a per-sandbox process: the id is set once at startup and lives until
/// process exit.
pub struct UdpProxyConfig {
    pub sandbox_id: &'static str,
    pub listener: std::net::UdpSocket,
    pub stats: Arc<ProxyStats>,
    pub network_state: GatewayNetworkState,
    pub known_egress_state: KnownEgressState,
    pub reserved_dns: ReservedDnsCore,
    pub shutdown_token: CancellationToken,
}

pub async fn run(mut config: UdpProxyConfig) -> std::io::Result<()> {
    let reserved_dns_tasks = TaskTracker::new();
    let proxy_shutdown = config.shutdown_token.child_token();
    config.shutdown_token = proxy_shutdown.clone();
    let mut proxy = UdpProxy::new(config, reserved_dns_tasks.clone())?;
    let result = proxy.run().await;
    proxy_shutdown.cancel();
    proxy.stop_all_sessions(UdpSessionOutcome::Shutdown).await;
    reserved_dns_tasks.close();
    reserved_dns_tasks.wait().await;
    result
}

struct UdpProxy {
    sandbox_id: &'static str,
    started_at: Instant,
    stats: Arc<ProxyStats>,
    network_state: GatewayNetworkState,
    known_egress_state: KnownEgressState,
    reserved_dns: ReservedDnsCore,
    shutdown_token: CancellationToken,
    reserved_dns_tasks: TaskTracker,
    reserved_dns_task_slots: Arc<Semaphore>,
    listener_ready: AsyncFd<std::net::UdpSocket>,
    // Guest datagrams promote a mapping in this cache. Response activity still
    // updates `last_active_ms` for the normal idle timeout, but under capacity
    // pressure admission favors flows the guest has used most recently.
    sessions: LruCache<FlowKey, Session>,
    response_sockets: HashMap<SocketAddr, Arc<UdpSocket>>,
    next_session_id: u64,
    session_event_tx: mpsc::Sender<SessionEvent>,
    session_event_rx: mpsc::Receiver<SessionEvent>,
    session_slots: Arc<Semaphore>,
    last_session_create_warning: Option<Instant>,
    pending_udp: PendingUdpQueue,
}

impl UdpProxy {
    fn new(config: UdpProxyConfig, reserved_dns_tasks: TaskTracker) -> io::Result<Self> {
        let UdpProxyConfig {
            sandbox_id,
            listener,
            stats,
            network_state,
            known_egress_state,
            reserved_dns,
            shutdown_token,
        } = config;
        let (session_event_tx, session_event_rx) =
            mpsc::channel::<SessionEvent>(UDP_SESSION_EVENT_QUEUE_CAPACITY);
        let session_capacity = NonZeroUsize::new(MAX_ACTIVE_UDP_SESSIONS).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP session limit must be non-zero",
            )
        })?;
        let pending_udp = PendingUdpQueue::new(known_egress_state.clone());
        pplx_space_metrics::gateway::udp_active_sessions(0);
        Ok(Self {
            sandbox_id,
            started_at: Instant::now(),
            stats,
            network_state,
            known_egress_state,
            reserved_dns,
            shutdown_token,
            reserved_dns_tasks,
            reserved_dns_task_slots: Arc::new(Semaphore::new(MAX_ACTIVE_RESERVED_DNS_DATAGRAMS)),
            // SAFETY: AsyncFd owns this open socket. The proxy only borrows it
            // for I/O and never replaces or closes its registered descriptor.
            listener_ready: unsafe { AsyncFd::register(listener)? },
            sessions: LruCache::new(session_capacity),
            response_sockets: HashMap::new(),
            next_session_id: 1,
            session_event_tx,
            session_event_rx,
            session_slots: Arc::new(Semaphore::new(MAX_ACTIVE_UDP_SESSIONS)),
            last_session_create_warning: None,
            pending_udp,
        })
    }

    async fn run(&mut self) -> io::Result<()> {
        info!(sandbox_id = self.sandbox_id, "UDP proxy running");

        let mut recv_buf = [0u8; 65536];
        let mut cmsg_buf = orig_dst::original_dst_cmsg_space();
        let evict_interval = Duration::from_secs(5);
        let mut eviction_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + evict_interval, evict_interval);
        eviction_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut pending_replay_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + UDP_PENDING_REPLAY_INTERVAL,
            UDP_PENDING_REPLAY_INTERVAL,
        );
        pending_replay_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // The listener is fed by the tap0 TPROXY rule, so readable packets
            // are guest-originated. Upstream responses are handled by per-flow
            // sockets.
            let result = tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    info!(sandbox_id = self.sandbox_id, "UDP proxy shutting down");
                    return Ok(());
                }
                _ = eviction_tick.tick() => {
                    self.evict_idle_sessions().await;
                    self.pending_udp.evict_expired(Instant::now(), self.sandbox_id, &self.stats);
                    continue;
                }
                _ = pending_replay_tick.tick() => {
                    self.replay_pending_udp().await;
                    continue;
                }
                event = self.session_event_rx.recv() => {
                    if let Some(event) = event {
                        self.handle_session_event(event).await;
                    }
                    continue;
                }
                result = self.listener_ready.async_io(Interest::READABLE, |inner| {
                    orig_dst::recvmsg_with_original_dst(
                        inner,
                        &mut recv_buf,
                        &mut cmsg_buf,
                    )
                }) => {
                    result
                }
            };

            match result {
                Ok((n, src_addr, orig_dst_v4)) => {
                    let Some(datagram) = recv_buf.get(..n) else {
                        debug!(
                            sandbox_id = self.sandbox_id,
                            bytes = n,
                            "recvmsg returned more UDP bytes than receive buffer capacity"
                        );
                        continue;
                    };
                    self.handle_guest_datagram(src_addr, orig_dst_v4, datagram)
                        .await;
                }
                Err(error) if error.kind() != io::ErrorKind::WouldBlock => {
                    debug!(
                        sandbox_id = self.sandbox_id,
                        %error,
                        "failed to receive UDP packet with original destination"
                    );
                }
                Err(_) => {}
            }
        }
    }

    async fn handle_guest_datagram(
        &mut self,
        src_addr: SocketAddr,
        orig_dst_v4: SocketAddrV4,
        bytes: &[u8],
    ) {
        let orig_dst = SocketAddr::V4(orig_dst_v4);
        let flow = FlowKey {
            guest_src: src_addr,
            orig_dst,
        };
        let reserved = self
            .reserved_dns
            .resolver_for(flow.orig_dst.ip())
            .map(|resolver| {
                (
                    resolver,
                    self.network_state
                        .reserved_dns_cancellation()
                        .operation_token(),
                )
            });

        let policy = match self.network_state.udp_policy().await {
            TrafficPolicy::Ready(policy) => policy,
            TrafficPolicy::NotReady(reason) => {
                if reason == PolicyNotReady::PendingPolicy {
                    self.pending_udp.push(
                        flow,
                        bytes,
                        Instant::now(),
                        self.sandbox_id,
                        &self.stats,
                    );
                    return;
                }
                if !self.pending_udp.is_empty() {
                    self.pending_udp.clear_dropped(
                        PendingUdpClearReason::NonReadyPolicy,
                        self.sandbox_id,
                        &self.stats,
                    );
                }
                self.known_egress_state.report_dns_failure(
                    self.sandbox_id,
                    flow.orig_dst,
                    bytes,
                    reason.as_str(),
                );
                debug!(
                    sandbox_id = self.sandbox_id,
                    guest = %src_addr,
                    %orig_dst,
                    reason = reason.as_str(),
                    "dropped UDP egress while network policy is not ready"
                );
                // No policy to run through the evaluator yet: this is a live
                // flow dropped because the policy is not ready.
                pplx_space_metrics::gateway::policy_decision(
                    pplx_space_metrics::gateway::PolicyDecisionKind::DnsAttributed,
                    EgressProtocol::Udp.as_str(),
                    EgressPhase::Admission.as_str(),
                    // No policy installed yet, so no version.
                    "unknown",
                    reason.as_str(),
                    crate::tcp_proxy::interception_for_not_ready(reason),
                );
                return;
            }
        };

        self.forward_udp_bytes(flow, bytes, policy, reserved).await;
    }

    async fn replay_pending_udp(&mut self) {
        if self.pending_udp.is_empty() {
            return;
        }
        self.pending_udp
            .evict_expired(Instant::now(), self.sandbox_id, &self.stats);
        let mut replayed = false;
        for _ in 0..UDP_PENDING_REPLAY_BATCH_DATAGRAMS {
            // Capture before admission so a concurrent policy install cancels
            // reserved-DNS work. Recheck the interception dependency gate for
            // each buffered send; a dependency can become unavailable between
            // two iterations of the same batch.
            let reserved_policy_cancellation = self
                .network_state
                .reserved_dns_cancellation()
                .operation_token();
            let policy = match self.network_state.udp_policy().await {
                TrafficPolicy::Ready(policy) => policy,
                TrafficPolicy::NotReady(PolicyNotReady::PendingPolicy) => break,
                TrafficPolicy::NotReady(_) => {
                    self.pending_udp.clear_dropped(
                        PendingUdpClearReason::NonReadyPolicy,
                        self.sandbox_id,
                        &self.stats,
                    );
                    break;
                }
            };
            let Some(datagram) = self.pending_udp.pop_front() else {
                break;
            };
            replayed = true;
            pplx_space_metrics::gateway::udp_pending_buffer("replayed");
            pplx_space_metrics::gateway::pending_udp_datagram("replayed");
            self.forward_udp_datagram(datagram, policy, &reserved_policy_cancellation)
                .await;
        }
        if replayed {
            self.pending_udp.update_stats(&self.stats);
        }
    }

    async fn forward_udp_datagram(
        &mut self,
        datagram: PendingUdpDatagram,
        policy: Arc<NetworkPolicyV1>,
        reserved_policy_cancellation: &CancellationToken,
    ) {
        let reserved = self
            .reserved_dns
            .resolver_for(datagram.flow.orig_dst.ip())
            .map(|resolver| (resolver, reserved_policy_cancellation.child_token()));
        self.forward_udp_bytes(datagram.flow, datagram.bytes.as_slice(), policy, reserved)
            .await;
    }

    async fn forward_udp_bytes(
        &mut self,
        flow: FlowKey,
        bytes: &[u8],
        policy: Arc<NetworkPolicyV1>,
        reserved: Option<(ReservedResolver, CancellationToken)>,
    ) {
        let route = udp_datagram_route(flow.orig_dst, reserved.is_some());
        let decision = if reserved.is_some() {
            evaluate_reserved_dns_egress(flow.orig_dst, policy.as_ref())
        } else {
            evaluate_udp_egress(
                self.network_state.dns_allow_cache(),
                flow.orig_dst,
                policy.as_ref(),
            )
        };
        pplx_space_metrics::gateway::udp_datagram(route, decision.is_allowed());
        if !decision.is_allowed() {
            self.known_egress_state.report_dns_failure(
                self.sandbox_id,
                flow.orig_dst,
                bytes,
                decision.label(),
            );
            debug!(
                sandbox_id = self.sandbox_id,
                guest = %flow.guest_src,
                orig_dst = %flow.orig_dst,
                reason = decision.label(),
                "denied UDP egress"
            );
            return;
        }

        if let Some((resolver, policy_cancellation)) = reserved {
            self.stats.add_network_tx_bytes(bytes.len() as u64);
            self.forward_reserved_dns_datagram(
                flow,
                resolver,
                bytes,
                policy,
                policy_cancellation,
                create_response_socket,
            )
            .await;
            return;
        }

        let Some(upstream_socket) = self.get_or_create_session(flow, bytes).await else {
            return;
        };

        match upstream_socket.send(bytes).await {
            Ok(sent) => {
                let requested = bytes.len();
                self.stats.add_network_tx_bytes(sent as u64);
                if sent != requested {
                    debug!(
                        sandbox_id = self.sandbox_id,
                        guest = %flow.guest_src,
                        orig_dst = %flow.orig_dst,
                        requested,
                        sent,
                        "UDP upstream send reported a short datagram"
                    );
                }
            }
            Err(error) => {
                pplx_space_metrics::gateway::udp_socket_error("send_upstream");
                self.known_egress_state.report_dns_failure(
                    self.sandbox_id,
                    flow.orig_dst,
                    bytes,
                    &format!("send_upstream: {error}"),
                );
                debug!(
                    sandbox_id = self.sandbox_id,
                    guest = %flow.guest_src,
                    orig_dst = %flow.orig_dst,
                    %error,
                    "failed to send UDP packet upstream"
                );
            }
        }
    }

    async fn forward_reserved_dns_datagram(
        &mut self,
        flow: FlowKey,
        resolver: ReservedResolver,
        wire: &[u8],
        policy: Arc<NetworkPolicyV1>,
        policy_cancellation: CancellationToken,
        response_socket_factory: impl FnOnce(SocketAddr) -> std::io::Result<UdpSocket>,
    ) {
        let permit = Arc::clone(&self.reserved_dns_task_slots).try_acquire_owned();
        pplx_space_metrics::gateway::reserved_dns_udp_in_flight(
            MAX_ACTIVE_RESERVED_DNS_DATAGRAMS - self.reserved_dns_task_slots.available_permits(),
        );
        let permit = match permit {
            Ok(permit) => permit,
            Err(_) => {
                let reply =
                    self.reserved_dns
                        .udp_capacity_response(self.sandbox_id, resolver, wire);
                if !reply.is_empty()
                    && let Ok(response_socket) = response_socket_factory(flow.orig_dst)
                    && let Ok(sent) = response_socket.try_send_to(&reply, flow.guest_src)
                {
                    self.stats.add_network_rx_bytes(sent as u64);
                }
                return;
            }
        };
        let response_socket = match response_socket_factory(flow.orig_dst) {
            Ok(socket) => socket,
            Err(error) => {
                pplx_space_metrics::gateway::udp_socket_error("reserved_dns_response");
                debug!(
                    sandbox_id = self.sandbox_id,
                    %error,
                    "failed to create reserved-DNS response socket"
                );
                return;
            }
        };

        let wire = wire.to_vec();
        let reserved_dns = self.reserved_dns.clone();
        let dns_allow_cache = self.network_state.dns_allow_cache().clone();
        let stats = Arc::clone(&self.stats);
        let gateway_shutdown = self.shutdown_token.clone();
        let sandbox_id = self.sandbox_id;
        drop(self.reserved_dns_tasks.spawn(async move {
            let _permit = permit;
            let outcome = tokio::select! {
                biased;
                () = gateway_shutdown.cancelled() => return,
                outcome = reserved_dns.resolve(
                    sandbox_id,
                    resolver,
                    &wire,
                    policy.as_ref(),
                    &policy_cancellation,
                ) => outcome,
            };
            let ResolveOutcome::Reply(reply) = outcome else {
                return;
            };
            if reply.is_empty() || policy_cancellation.is_cancelled() {
                return;
            }
            let accepted = dns_allow_cache.observe_response(&reply, policy.as_ref());
            pplx_space_metrics::gateway::dns_attribution_cache_observation(
                DnsAttributionCacheObservationSource::ReservedDns,
                accepted,
            );
            tokio::select! {
                biased;
                () = gateway_shutdown.cancelled() => {}
                () = policy_cancellation.cancelled() => {}
                result = tokio::time::timeout(
                    Duration::from_secs(1),
                    response_socket.send_to(&reply, flow.guest_src),
                ) => {
                    if let Ok(Ok(sent)) = result {
                        stats.add_network_rx_bytes(sent as u64);
                    }
                }
            }
        }));
    }

    async fn evict_idle_sessions(&mut self) {
        let timeout_ms = UDP_MAPPING_TIMEOUT_SECS.saturating_mul(1000);
        let now_ms = elapsed_millis(self.started_at);
        let expired: Vec<_> = self
            .sessions
            .iter()
            .filter_map(|(flow, session)| {
                let dead = session.is_dead();
                let idle = session.is_idle(now_ms, timeout_ms);
                (dead || idle).then_some((
                    *flow,
                    if dead {
                        UdpSessionOutcome::TaskExit
                    } else {
                        UdpSessionOutcome::Idle
                    },
                ))
            })
            .collect();
        let mut stopped_tasks = Vec::with_capacity(expired.len());
        for (flow, outcome) in expired {
            if let Some(session) = self.sessions.pop(&flow) {
                stopped_tasks.push(session.abort(outcome));
            }
        }
        wait_for_udp_session_tasks(stopped_tasks).await;
        self.refresh_session_stats();
    }

    async fn stop_all_sessions(&mut self, outcome: UdpSessionOutcome) {
        let mut stopped_tasks = Vec::with_capacity(self.sessions.len());
        while let Some((_flow, session)) = self.sessions.pop_lru() {
            stopped_tasks.push(session.abort(outcome.clone()));
        }
        wait_for_udp_session_tasks(stopped_tasks).await;
        self.refresh_session_stats();
    }

    async fn handle_session_event(&mut self, event: SessionEvent) {
        handle_udp_session_event(
            self.sandbox_id,
            &mut self.sessions,
            &mut self.response_sockets,
            &self.stats,
            event,
        )
        .await;
    }

    fn refresh_session_stats(&mut self) {
        refresh_udp_session_stats(&self.sessions, &mut self.response_sockets, &self.stats);
    }

    async fn get_or_create_session(
        &mut self,
        flow: FlowKey,
        bytes: &[u8],
    ) -> Option<Arc<UdpSocket>> {
        let now_ms = elapsed_millis(self.started_at);
        if let Some(socket) = lookup_live_udp_session(
            &mut self.sessions,
            &mut self.response_sockets,
            flow,
            now_ms,
            self.stats.as_ref(),
        )
        .await
        {
            return Some(socket);
        }

        if udp_session_limit_reached(self.sessions.len()) {
            if !evict_lru_udp_session(
                &mut self.sessions,
                &mut self.response_sockets,
                self.stats.as_ref(),
            )
            .await
            {
                self.record_session_limit_drop();
                return None;
            }
            pplx_space_metrics::gateway::udp_session_capacity_eviction();
            debug!(
                sandbox_id = self.sandbox_id,
                limit = MAX_ACTIVE_UDP_SESSIONS,
                "evicted least-recently-used UDP flow at active-session limit"
            );
        }

        let session_permit = match self.session_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                // A canceled task can retain its sockets and receive buffer
                // briefly after its map entry is removed. The permit follows
                // the task, so replacement waves cannot exceed the hard bound.
                self.record_session_limit_drop();
                return None;
            }
        };

        let upstream_socket = match create_connected_upstream_socket(flow.orig_dst) {
            Ok(socket) => Arc::new(socket),
            Err(error) => {
                let stage = UdpSessionSocketStage::Upstream;
                let reason = self.record_session_create_error(stage, &error);
                self.known_egress_state.report_dns_failure(
                    self.sandbox_id,
                    flow.orig_dst,
                    bytes,
                    &udp_session_create_failure_reason(stage, reason),
                );
                return None;
            }
        };
        let response_socket =
            match get_or_create_response_socket(&mut self.response_sockets, flow.orig_dst) {
                Ok(socket) => socket,
                Err(error) => {
                    let stage = UdpSessionSocketStage::Response;
                    let reason = self.record_session_create_error(stage, &error);
                    self.known_egress_state.report_dns_failure(
                        self.sandbox_id,
                        flow.orig_dst,
                        bytes,
                        &udp_session_create_failure_reason(stage, reason),
                    );
                    return None;
                }
            };

        let last_active_ms = Arc::new(AtomicU64::new(now_ms));
        let cancel_token = self.shutdown_token.child_token();
        let dead = Arc::new(AtomicBool::new(false));
        let completion = SessionCompletion::new();
        let session_id = self.take_next_session_id();
        let task = ResponseTask {
            sandbox_id: self.sandbox_id,
            flow,
            session_id,
            started_at: self.started_at,
            upstream_socket: upstream_socket.clone(),
            response_socket,
            last_active_ms: last_active_ms.clone(),
            stats: Arc::clone(&self.stats),
            cancel_token: cancel_token.clone(),
            dead: dead.clone(),
            completion: completion.clone(),
            session_event_tx: self.session_event_tx.clone(),
            session_permit,
        }
        .spawn();

        let evicted = self.sessions.push(
            flow,
            Session {
                session_id,
                upstream_socket: upstream_socket.clone(),
                last_active_ms,
                cancel_token,
                dead,
                completion,
                task,
            },
        );
        if let Some((_flow, replaced)) = evicted {
            // The key was absent and capacity was reclaimed above, so this is
            // defensive containment if those invariants ever diverge.
            replaced.stop(UdpSessionOutcome::CapacityEviction).await;
            pplx_space_metrics::gateway::udp_session_capacity_eviction();
        }
        update_udp_active_sessions(&self.sessions, &self.stats);
        Some(upstream_socket)
    }

    fn record_session_limit_drop(&self) {
        pplx_space_metrics::gateway::udp_session_limit_drop();
        debug!(
            sandbox_id = self.sandbox_id,
            limit = MAX_ACTIVE_UDP_SESSIONS,
            "dropped new UDP flow because response-task capacity is unavailable"
        );
    }

    fn record_session_create_error(
        &mut self,
        stage: UdpSessionSocketStage,
        error: &io::Error,
    ) -> &'static str {
        pplx_space_metrics::gateway::udp_socket_error(stage.socket_error_operation());
        let stage = stage.as_str();
        let reason = udp_session_create_error_reason(error);
        pplx_space_metrics::gateway::udp_session_create_error(stage, reason);
        let now = Instant::now();
        let should_warn = self.last_session_create_warning.is_none_or(|last_warning| {
            now.duration_since(last_warning) >= UDP_SESSION_CREATE_WARNING_INTERVAL
        });
        if should_warn {
            self.last_session_create_warning = Some(now);
            warn!(
                sandbox_id = self.sandbox_id,
                stage,
                reason,
                %error,
                "failed to create UDP session socket (further warnings throttled)"
            );
        } else {
            debug!(sandbox_id = self.sandbox_id, stage, reason, %error, "failed to create UDP session socket");
        }
        reason
    }

    fn take_next_session_id(&mut self) -> u64 {
        let session_id = self.next_session_id;
        self.next_session_id += 1;
        session_id
    }
}

#[derive(Debug)]
enum SessionEvent {
    Dead {
        flow: FlowKey,
        session_id: u64,
        error: String,
    },
}

async fn handle_udp_session_event(
    sandbox_id: &'static str,
    sessions: &mut LruCache<FlowKey, Session>,
    response_sockets: &mut HashMap<SocketAddr, Arc<UdpSocket>>,
    stats: &ProxyStats,
    event: SessionEvent,
) {
    match event {
        SessionEvent::Dead {
            flow,
            session_id,
            error,
        } => {
            let remove = sessions
                .peek(&flow)
                .is_some_and(|session| session.session_id == session_id);
            if !remove {
                debug!(
                    sandbox_id,
                    ?flow,
                    session_id,
                    %error,
                    "ignored stale UDP response task death"
                );
                return;
            }
            if let Some(session) = sessions.pop(&flow) {
                session
                    .stop(UdpSessionOutcome::ReceiveUpstreamFailed {
                        error: error.clone(),
                    })
                    .await;
                debug!(
                    sandbox_id,
                    ?flow,
                    session_id,
                    %error,
                    "removed failed UDP flow"
                );
                refresh_udp_session_stats(sessions, response_sockets, stats);
            }
        }
    }
}

fn refresh_udp_session_stats(
    sessions: &LruCache<FlowKey, Session>,
    response_sockets: &mut HashMap<SocketAddr, Arc<UdpSocket>>,
    stats: &ProxyStats,
) {
    let active_orig_dsts: HashSet<SocketAddr> =
        sessions.iter().map(|(flow, _)| flow.orig_dst).collect();
    response_sockets.retain(|addr, _| active_orig_dsts.contains(addr));
    update_udp_active_sessions(sessions, stats);
}

fn update_udp_active_sessions(sessions: &LruCache<FlowKey, Session>, stats: &ProxyStats) {
    stats
        .udp_active
        .store(sessions.len() as u64, Ordering::Relaxed);
    pplx_space_metrics::gateway::udp_active_sessions(sessions.len() as u64);
}

async fn wait_for_udp_session_tasks(tasks: Vec<tokio::task::JoinHandle<()>>) {
    let stop_deadline = tokio::time::Instant::now() + UDP_SESSION_TASK_STOP_TIMEOUT;
    for mut task in tasks {
        let _ = tokio::time::timeout_at(stop_deadline, &mut task).await;
    }
}

async fn lookup_live_udp_session(
    sessions: &mut LruCache<FlowKey, Session>,
    response_sockets: &mut HashMap<SocketAddr, Arc<UdpSocket>>,
    flow: FlowKey,
    now_ms: u64,
    stats: &ProxyStats,
) -> Option<Arc<UdpSocket>> {
    if sessions.peek(&flow).is_some_and(Session::is_dead) {
        if let Some(session) = sessions.pop(&flow) {
            session.stop(UdpSessionOutcome::TaskExit).await;
            refresh_udp_session_stats(sessions, response_sockets, stats);
        }
        None
    } else {
        sessions.get(&flow).map(|session| {
            session.touch(now_ms);
            session.upstream_socket.clone()
        })
    }
}

async fn evict_lru_udp_session(
    sessions: &mut LruCache<FlowKey, Session>,
    response_sockets: &mut HashMap<SocketAddr, Arc<UdpSocket>>,
    stats: &ProxyStats,
) -> bool {
    let Some((_flow, session)) = sessions.pop_lru() else {
        return false;
    };
    session.stop(UdpSessionOutcome::CapacityEviction).await;
    refresh_udp_session_stats(sessions, response_sockets, stats);
    true
}

/// Why one admitted UDP flow mapping's response task stopped.
#[derive(Clone, Debug, Eq, PartialEq)]
enum UdpSessionOutcome {
    Idle,
    CapacityEviction,
    Shutdown,
    ReceiveUpstreamFailed { error: String },
    TaskExit,
}

impl UdpSessionOutcome {
    const fn metric_tags(&self) -> (&'static str, &'static str) {
        match self {
            Self::Idle => ("relayed", "idle"),
            Self::CapacityEviction => ("relayed", "capacity_eviction"),
            Self::Shutdown => ("cancelled", "shutdown"),
            Self::ReceiveUpstreamFailed { .. } => ("relay_failed", "recv_upstream"),
            Self::TaskExit => ("relay_failed", "task_exit"),
        }
    }
}

fn record_udp_session(
    sandbox_id: &'static str,
    flow: FlowKey,
    session_id: u64,
    outcome: UdpSessionOutcome,
) {
    let (status, detail) = outcome.metric_tags();
    pplx_space_metrics::gateway::udp_connection(status, detail);

    match outcome {
        UdpSessionOutcome::Idle | UdpSessionOutcome::CapacityEviction => {
            debug!(
                sandbox_id,
                ?flow,
                session_id,
                reason = detail,
                "evicted UDP flow"
            );
        }
        UdpSessionOutcome::Shutdown => {
            debug!(
                sandbox_id,
                ?flow,
                session_id,
                "UDP response task shutting down"
            );
        }
        UdpSessionOutcome::ReceiveUpstreamFailed { error } => {
            pplx_space_metrics::gateway::udp_socket_error("recv_upstream");
            debug!(
                sandbox_id,
                ?flow,
                session_id,
                %error,
                "UDP upstream socket receive failed"
            );
        }
        UdpSessionOutcome::TaskExit => {
            debug!(
                sandbox_id,
                ?flow,
                session_id,
                "UDP response task exited without a classified outcome"
            );
        }
    }
}

#[derive(Clone)]
struct SessionCompletion(Arc<OnceLock<UdpSessionOutcome>>);

impl SessionCompletion {
    fn new() -> Self {
        Self(Arc::new(OnceLock::new()))
    }

    fn mark(&self, outcome: UdpSessionOutcome) -> bool {
        // The owner can abort the task while the task is independently
        // returning an error. Preserve whichever terminal cause happened first.
        self.0.set(outcome).is_ok()
    }

    fn outcome(&self) -> UdpSessionOutcome {
        self.0.get().cloned().unwrap_or(UdpSessionOutcome::TaskExit)
    }
}

struct SessionTaskGuard {
    sandbox_id: &'static str,
    flow: FlowKey,
    session_id: u64,
    dead: Arc<AtomicBool>,
    completion: SessionCompletion,
    _permit: OwnedSemaphorePermit,
}

impl Drop for SessionTaskGuard {
    fn drop(&mut self) {
        // Covers cancellation and unwinding as well as handled receive errors.
        // A map entry must never outlive its response task while appearing live.
        self.dead.store(true, Ordering::Release);
        record_udp_session(
            self.sandbox_id,
            self.flow,
            self.session_id,
            self.completion.outcome(),
        );
    }
}

struct ResponseTask {
    sandbox_id: &'static str,
    flow: FlowKey,
    session_id: u64,
    started_at: Instant,
    upstream_socket: Arc<UdpSocket>,
    response_socket: Arc<UdpSocket>,
    last_active_ms: Arc<AtomicU64>,
    stats: Arc<ProxyStats>,
    cancel_token: CancellationToken,
    dead: Arc<AtomicBool>,
    completion: SessionCompletion,
    session_event_tx: mpsc::Sender<SessionEvent>,
    session_permit: OwnedSemaphorePermit,
}

impl ResponseTask {
    fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let Self {
                sandbox_id,
                flow,
                session_id,
                started_at,
                upstream_socket,
                response_socket,
                last_active_ms,
                stats,
                cancel_token,
                dead,
                completion,
                session_event_tx,
                session_permit,
            } = self;
            let _task_guard = SessionTaskGuard {
                sandbox_id,
                flow,
                session_id,
                dead: dead.clone(),
                completion: completion.clone(),
                _permit: session_permit,
            };
            let mut buf = [0u8; 65536];
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        completion.mark(UdpSessionOutcome::Shutdown);
                        return;
                    }
                    result = upstream_socket.recv(&mut buf) => {
                        match result {
                            Ok(n) => {
                                last_active_ms.store(elapsed_millis(started_at), Ordering::Relaxed);
                                stats.add_network_rx_bytes(n as u64);
                                let Some(datagram) = buf.get(..n) else {
                                    debug!(
                                        sandbox_id,
                                        ?flow,
                                        session_id,
                                        bytes = n,
                                        "UDP upstream receive exceeded buffer capacity"
                                    );
                                    continue;
                                };
                                // Replies on a generic flow come from whatever
                                // the guest addressed, port 53 included, so they
                                // are relayed opaquely and never feed the DNS
                                // attribution cache. Only the gateway-mediated
                                // reserved resolvers mint egress evidence; a
                                // guest-chosen answer for an allowed name would
                                // otherwise bind any unguardrailed IP to it.
                                if let Err(error) =
                                    response_socket.send_to(datagram, flow.guest_src).await
                                {
                                    pplx_space_metrics::gateway::udp_socket_error("send_guest");
                                    debug!(
                                        sandbox_id,
                                        ?flow,
                                        %error,
                                        "failed to send UDP packet to guest"
                                    );
                                }
                            }
                            Err(error) => {
                                if is_recoverable_udp_recv_error(&error) {
                                    pplx_space_metrics::gateway::udp_socket_error(
                                        "recv_upstream_transient",
                                    );
                                    debug!(
                                        sandbox_id,
                                        ?flow,
                                        session_id,
                                        %error,
                                        "transient UDP upstream socket receive error"
                                    );
                                    continue;
                                }
                                let error_message = error.to_string();
                                completion.mark(UdpSessionOutcome::ReceiveUpstreamFailed {
                                    error: error_message.clone(),
                                });
                                dead.store(true, Ordering::Release);
                                if let Err(error) = session_event_tx.try_send(SessionEvent::Dead {
                                        flow,
                                        session_id,
                                        error: error_message,
                                    }) {
                                    let reason = match error {
                                        mpsc::error::TrySendError::Full(_) => "full",
                                        mpsc::error::TrySendError::Closed(_) => "closed",
                                    };
                                    pplx_space_metrics::gateway::udp_session_event_drop(reason);
                                    debug!(
                                        sandbox_id,
                                        ?flow,
                                        session_id,
                                        reason,
                                        "UDP session event receiver dropped before dead event"
                                    );
                                }
                                return;
                            }
                        }
                    }
                }
            }
        })
    }
}

fn udp_session_create_error_reason(error: &io::Error) -> &'static str {
    match error.kind() {
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::AddrInUse | io::ErrorKind::AddrNotAvailable => "address",
        io::ErrorKind::NetworkUnreachable | io::ErrorKind::HostUnreachable => "unreachable",
        _ if matches!(
            error.raw_os_error(),
            Some(libc::EMFILE) | Some(libc::ENFILE)
        ) =>
        {
            "fd_exhausted"
        }
        _ => "other",
    }
}

fn is_recoverable_udp_recv_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) || error.raw_os_error() == Some(libc::ECONNREFUSED)
}

fn get_or_create_response_socket(
    cache: &mut HashMap<SocketAddr, Arc<UdpSocket>>,
    addr: SocketAddr,
) -> std::io::Result<Arc<UdpSocket>> {
    match cache.entry(addr) {
        std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let socket = Arc::new(create_response_socket(addr)?);
            entry.insert(socket.clone());
            Ok(socket)
        }
    }
}

/// Create a connected upstream socket for packets leaving the node.
///
/// Do not bind this socket to the guest source address. Guest addresses are
/// node-local, so public upstreams such as DNS resolvers cannot route replies
/// back to them. The response socket below is the transparent leg that sends
/// replies back to the guest as if they came from the original destination.
fn create_connected_upstream_socket(orig_dst: SocketAddr) -> std::io::Result<UdpSocket> {
    let socket = create_udp_socket()?;
    orig_dst::set_so_mark(&socket, OUTBOUND_MARK)?;
    socket
        .connect(&socket2::SockAddr::from(orig_dst))
        .inspect_err(|_| {
            pplx_space_metrics::gateway::udp_socket_error("connect");
        })?;
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

/// Create a response socket bound to a non-local address with IP_TRANSPARENT.
fn create_response_socket(addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let socket = create_udp_socket()?;
    orig_dst::set_ip_transparent(&socket)?;
    orig_dst::set_so_mark(&socket, OUTBOUND_MARK)?;
    orig_dst::set_ip_freebind(&socket).inspect_err(|_| {
        pplx_space_metrics::gateway::udp_socket_error("setsockopt_ip_freebind");
    })?;
    socket.set_reuse_address(true).inspect_err(|_| {
        pplx_space_metrics::gateway::udp_socket_error("setsockopt_reuseaddr");
    })?;
    socket
        .bind(&socket2::SockAddr::from(addr))
        .inspect_err(|_| {
            pplx_space_metrics::gateway::udp_socket_error("bind");
        })?;
    let std_socket: std::net::UdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

fn create_udp_socket() -> std::io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .inspect_err(|_| {
        pplx_space_metrics::gateway::udp_socket_error("socket");
    })?;
    socket.set_nonblocking(true).inspect_err(|_| {
        pplx_space_metrics::gateway::udp_socket_error("set_nonblocking");
    })?;
    Ok(socket)
}

fn elapsed_millis(started_at: Instant) -> u64 {
    started_at
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn session_is_idle(now_ms: u64, last_active_ms: u64, timeout_ms: u64) -> bool {
    now_ms.saturating_sub(last_active_ms) >= timeout_ms
}

#[cfg(test)]
mod tests;
