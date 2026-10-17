//! TCP transparent proxy using TPROXY.
//!
//! One connection, in order: accept, peek, admit, relay. The peek consumes
//! nothing from the guest. Under TLS interception, a silent guest may first
//! be probed for a server greeting, which opens the destination connection
//! before the guest's bytes are admitted; see [`server_first`].

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_common_connection_admission::AcceptErrors;
use pplx_space_net_common::DomainName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::debug::GatewayDebugState;
use crate::egress::{EgressPhase, EgressProtocol};
use crate::known_egress::KnownEgressState;
use crate::reserved_dns::ReservedDnsCore;
use crate::resource_budget::MAX_ACTIVE_TCP_CONNECTIONS;
use crate::sniff;
use crate::state::{GatewayNetworkState, PolicyNotReady, TrafficPolicy};
use crate::stats::ProxyStats;
use crate::tls_interception::{TlsInterception, TlsInterceptionError};

mod accounting;
mod admission;
mod http_host;
mod inspected;
mod relay;
mod reserved_dns;
mod routing_headers;
mod server_first;
pub(crate) mod tls;
mod tls_http;
mod transport;
mod upstream;

use crate::egress::{AdmittedTcp, TcpAdmissionDenial};
pub use pplx_space_net_gateway_client::TcpDomainAuthorizationMode;

pub struct TcpProxyConfig {
    pub sandbox_id: &'static str,
    pub listener: TcpListener,
    pub stats: Arc<ProxyStats>,
    pub network_state: GatewayNetworkState,
    pub tls_interception: TlsInterception,
    pub reserved_dns: ReservedDnsCore,
    pub known_egress_state: KnownEgressState,
    /// Verbose debug-logging toggle. When enabled (via `EnableDebug`), the proxy
    /// emits a structured per-flow log of each TLS ClientHello SNI scan. Gated so
    /// the hot path stays quiet outside an operator's debug window.
    pub debug_state: GatewayDebugState,
    pub shutdown_token: CancellationToken,
}

/// Run the TCP transparent proxy on a pre-created TPROXY listener.
///
/// `sandbox_id` is recorded as a structured field on every log emitted by this
/// proxy and the per-connection tasks it spawns. `&'static str` mirrors that the
/// gateway is a per-sandbox process: the id is set once at startup and lives
/// until process exit.
pub async fn run(config: TcpProxyConfig) -> io::Result<()> {
    let TcpProxyConfig {
        sandbox_id,
        listener,
        stats,
        network_state,
        tls_interception,
        reserved_dns,
        known_egress_state,
        debug_state,
        shutdown_token,
    } = config;

    let proxy_shutdown_token = shutdown_token.child_token();
    let result = TcpProxy {
        sandbox_id,
        stats,
        network_state,
        tls_interception,
        reserved_dns,
        known_egress_state,
        debug_state,
        shutdown_token: proxy_shutdown_token.clone(),
        refusal_warnings: Arc::default(),
    }
    .accept_loop(listener)
    .await;

    proxy_shutdown_token.cancel();
    result
}

/// Per-sandbox proxy state, cloned into each connection task.
/// Minimum gap between interception refusal warnings for one cause. Matches
/// the listener accept-error budget so operators see a consistent cadence.
const REFUSAL_WARNING_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct TcpProxy {
    sandbox_id: &'static str,
    stats: Arc<ProxyStats>,
    network_state: GatewayNetworkState,
    tls_interception: TlsInterception,
    reserved_dns: ReservedDnsCore,
    known_egress_state: KnownEgressState,
    debug_state: GatewayDebugState,
    shutdown_token: CancellationToken,
    refusal_warnings: Arc<RefusalWarnings>,
}

/// Per-cause budget for interception refusal warnings.
///
/// A certificate-authority outage refuses every inspected connection, so an
/// unbudgeted WARN turns one incident into a log flood. Budgeting per cause
/// keeps a rare refusal visible while a common one is flooding, and the
/// suppressed count keeps the surviving line honest about what it stands for.
/// The metric is always emitted, so suppression costs no measurement.
#[derive(Default)]
struct RefusalWarnings {
    // Keys come from bounded enums, so this map cannot grow without bound.
    causes: Mutex<HashMap<(&'static str, Option<&'static str>), RefusalWarnState>>,
}

#[derive(Default)]
struct RefusalWarnState {
    last_logged_at: Option<Instant>,
    suppressed: u64,
}

impl RefusalWarnings {
    fn sample(&self, outcome: &'static str, detail: Option<&'static str>) -> (bool, u64) {
        self.sample_at(outcome, detail, Instant::now())
    }

    fn sample_at(
        &self,
        outcome: &'static str,
        detail: Option<&'static str>,
        now: Instant,
    ) -> (bool, u64) {
        let Ok(mut causes) = self.causes.lock() else {
            // A poisoned budget must not silence a fail-closed refusal.
            return (true, 0);
        };
        let state = causes.entry((outcome, detail)).or_default();
        let log = state
            .last_logged_at
            .is_none_or(|last| now.duration_since(last) >= REFUSAL_WARNING_INTERVAL);
        if log {
            state.last_logged_at = Some(now);
            return (true, std::mem::take(&mut state.suppressed));
        }
        state.suppressed = state.suppressed.saturating_add(1);
        (false, 0)
    }
}

/// One accepted connection, ready to serve.
struct Accepted {
    client: TcpStream,
    /// Where the guest was going. TPROXY preserves it as our local address.
    dst: SocketAddr,
    permit: OwnedSemaphorePermit,
}

/// What happened to one connection. Handled once, in [`TcpProxy::record`].
///
/// Four states; anything new lands as a reason under one of them.
enum FlowOutcome {
    /// Gateway shutting down, or a policy resweep cancelled this flow.
    Cancelled,
    Rejected(Rejection),
    Relayed(Relay),
    HandledDns {
        messages: u32,
        wire_tx: u64,
        wire_rx: u64,
    },
    InspectedTls,
    RelayFailed(RelayFailure),
}

impl FlowOutcome {
    fn metric_tags(&self) -> (&'static str, &'static str) {
        match self {
            Self::Cancelled => ("cancelled", "shutdown_or_policy_resweep"),
            Self::Rejected(Rejection::PolicyMissing(reason)) => ("rejected", reason.as_str()),
            Self::Rejected(Rejection::Admission(reason)) => ("rejected", reason.label()),
            Self::Rejected(Rejection::TlsInterception(_)) => ("rejected", "tls_interception"),
            Self::Relayed(Relay::Tunnelled { .. }) => ("relayed", "tunnelled"),
            Self::Relayed(Relay::HostBoundHttp { .. }) => ("relayed", "host_bound_http"),
            Self::HandledDns { .. } => ("handled", "reserved_dns"),
            Self::InspectedTls => ("relayed", "inspected_tls"),
            Self::RelayFailed(RelayFailure::Inspected(_)) => ("relay_failed", "inspected_http"),
            Self::RelayFailed(RelayFailure::Connect(_)) => ("relay_failed", "connect"),
            Self::RelayFailed(RelayFailure::Plaintext(error)) => ("relay_failed", error.reason()),
            Self::RelayFailed(RelayFailure::Transfer(_)) => ("relay_failed", "transfer"),
        }
    }

    fn failure_reason(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::RelayFailed(RelayFailure::Connect(error)) => {
                Some(format!("upstream_connect: {error}").into())
            }
            Self::RelayFailed(RelayFailure::Transfer(error)) => {
                Some(format!("transfer: {:?}: {error}", error.kind()).into())
            }
            Self::Rejected(_) | Self::RelayFailed(_) => Some(self.metric_tags().1.into()),
            Self::Cancelled | Self::Relayed(_) | Self::HandledDns { .. } | Self::InspectedTls => {
                None
            }
        }
    }
}

/// Who refused the flow, and why. The state says it was refused; these say by
/// what.
enum Rejection {
    /// No policy was installed yet, so there was nothing to evaluate against.
    PolicyMissing(PolicyNotReady),
    /// TCP admission denied the flow.
    Admission(TcpAdmissionDenial),
    TlsInterception(TlsInterceptionError),
}

/// Where relaying failed, named for the phase.
enum RelayFailure {
    Connect(transport::Error),
    Plaintext(http_host::Failure),
    /// Connected, then the byte stream failed (e.g. reset, broken pipe).
    Transfer(io::Error),
    Inspected(tls_http::HttpError),
}

/// How the bytes were carried.
enum Relay {
    /// CONNECT-tunnel semantics: bytes passed, never read.
    Tunnelled { wire_tx: u64, wire_rx: u64 },
    /// Plaintext HTTP whose every request host was bound to the DNS-derived
    /// permission that admitted this connection.
    HostBoundHttp {
        requests: u32,
        forwarded: u32,
        refused: u32,
        upstream_failed: u32,
        wire_tx: u64,
        wire_rx: u64,
    },
}

impl TcpProxy {
    async fn accept_loop(self, listener: TcpListener) -> io::Result<()> {
        info!(sandbox_id = self.sandbox_id, "TCP proxy running");

        let limit = Arc::new(Semaphore::new(MAX_ACTIVE_TCP_CONNECTIONS));
        let mut accept_errors = AcceptErrors::default();

        while let Some(accepted) = self.accept(&listener, &limit, &mut accept_errors).await? {
            tokio::spawn(self.clone().serve(accepted));
        }

        info!(sandbox_id = self.sandbox_id, "TCP proxy shutting down");
        Ok(())
    }

    /// The next connection to serve, or `None` once shutting down. Recoverable
    /// accept failures retry internally; fatal listener errors retain their
    /// source when they propagate through process supervision.
    async fn accept(
        &self,
        listener: &TcpListener,
        limit: &Arc<Semaphore>,
        accept_errors: &mut AcceptErrors,
    ) -> io::Result<Option<Accepted>> {
        loop {
            let Some(permit) = self.acquire_permit(limit).await else {
                return Ok(None);
            };
            let accepted = tokio::select! {
                _ = self.shutdown_token.cancelled() => return Ok(None),
                result = listener.accept() => result,
            };
            match accepted {
                Ok((client, _peer)) => {
                    accept_errors.reset();
                    match client.local_addr() {
                        Ok(dst) => {
                            return Ok(Some(Accepted {
                                client,
                                dst,
                                permit,
                            }));
                        }
                        Err(error) => {
                            warn!(sandbox_id = self.sandbox_id, %error, "TCP local_addr failed");
                        }
                    }
                }
                Err(error) => {
                    drop(permit);
                    let action = accept_errors.record(&error);
                    pplx_space_metrics::gateway::tcp_accept_error(action.reason.as_str());
                    if action.log {
                        warn!(
                            sandbox_id = self.sandbox_id,
                            %error,
                            reason = action.reason.as_str(),
                            suppressed = action.suppressed,
                            backoff_ms = u64::try_from(action.pause.as_millis()).unwrap_or(u64::MAX),
                            "TCP accept error"
                        );
                    }
                    if !action.retry {
                        return Err(error);
                    }
                    if !action.pause.is_zero() {
                        pplx_space_metrics::gateway::tcp_accept_backoff(action.reason.as_str());
                        tokio::select! {
                            _ = self.shutdown_token.cancelled() => return Ok(None),
                            _ = tokio::time::sleep(action.pause) => {}
                        }
                    }
                }
            }
        }
    }

    async fn acquire_permit(&self, limit: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
        let permit = limit.clone().try_acquire_owned();
        pplx_space_metrics::gateway::tcp_in_flight(
            MAX_ACTIVE_TCP_CONNECTIONS - limit.available_permits(),
        );
        match permit {
            Ok(permit) => Some(permit),
            // At capacity, stop accepting: connections queue in the kernel
            // rather than being accepted and then stalled.
            Err(TryAcquireError::NoPermits) => {
                pplx_space_metrics::gateway::tcp_connection_limit_wait();
                tokio::select! {
                    _ = self.shutdown_token.cancelled() => None,
                    permit = limit.clone().acquire_owned() => permit.ok(),
                }
            }
            Err(TryAcquireError::Closed) => None,
        }
    }

    async fn serve(self, accepted: Accepted) {
        let Accepted {
            client,
            dst,
            permit,
        } = accepted;
        let _permit = permit;

        self.stats.tcp_active.fetch_add(1, Ordering::Relaxed);
        let shutdown = self.shutdown_token.child_token();
        // Biased, with the connection polled first: on shutdown both arms are
        // ready, and an unbiased select would pick between them at random, so
        // the same event could report either this generic cancellation or the
        // specific cause the inspected session names. The inner boundary owns
        // the cause; this arm only covers a connection that cannot observe
        // cancellation itself.
        let (host, outcome) = tokio::select! {
            biased;
            outcome = self.handle(client, dst, &shutdown) => outcome,
            _ = shutdown.cancelled() => (None, FlowOutcome::Cancelled),
        };
        self.record(dst, host.as_ref(), outcome);
        self.stats.tcp_active.fetch_sub(1, Ordering::Relaxed);
    }

    async fn handle(
        &self,
        client: TcpStream,
        dst: SocketAddr,
        shutdown: &CancellationToken,
    ) -> (Option<DomainName>, FlowOutcome) {
        self.handle_with(client, dst, shutdown, transport::connect)
            .await
    }

    /// [`Self::handle`] with the dialer for pass-through, plaintext HTTP and the
    /// server-first probe. Tests pass one that needs no socket mark.
    async fn handle_with<F, Fut, U>(
        &self,
        client: TcpStream,
        dst: SocketAddr,
        shutdown: &CancellationToken,
        connect: F,
    ) -> (Option<DomainName>, FlowOutcome)
    where
        F: Fn(SocketAddr) -> Fut + Copy + Send + 'static,
        Fut: Future<Output = Result<U, transport::Error>> + Send + 'static,
        U: AsyncRead + AsyncWrite + transport::Close + Unpin + Send + 'static,
    {
        abort_on_close(&client);
        if let Some(resolver) = self.reserved_dns.resolver_for(dst.ip()) {
            // Take the cancellation token before reading policy. A concurrent
            // install therefore either cancels this connection or precedes
            // the policy snapshot used below.
            let policy_cancellation = self
                .network_state
                .reserved_dns_cancellation()
                .operation_token();
            let policy = tokio::select! {
                biased;
                () = policy_cancellation.cancelled() => return (None, FlowOutcome::Cancelled),
                policy = self.network_state.wait_for_tcp_policy() => policy,
            };
            let policy = match policy {
                TrafficPolicy::Ready(policy) => policy,
                TrafficPolicy::NotReady(reason) => {
                    return (
                        None,
                        FlowOutcome::Rejected(Rejection::PolicyMissing(reason)),
                    );
                }
            };
            let _admitted = match self.admit_reserved_dns_destination(dst, policy.as_ref()) {
                Ok(admitted) => admitted,
                Err(reason) => {
                    return (None, FlowOutcome::Rejected(Rejection::Admission(reason)));
                }
            };
            close_gracefully(&client);
            return (
                None,
                reserved_dns::serve(
                    self,
                    client,
                    resolver,
                    policy.as_ref(),
                    &policy_cancellation,
                )
                .await,
            );
        }

        let mut inspection = accounting::Attempt::inspection_admission();
        let policy = self.network_state.wait_for_tcp_policy().await;
        // Policy readiness remains first on the success path. A failed wait
        // still gets one bounded, non-consuming peek so its diagnostic can
        // identify a configured TLS destination.
        let missing_client_hello = self.missing_client_hello(dst, &policy);
        let mut observation = sniff::client_hello_with_detail(&client, missing_client_hello).await;
        // Probe only a guest that sent nothing; any byte or EOF takes the usual path.
        let mut probe_server_first = missing_client_hello
            == sniff::MissingClientHello::ProbesServer
            && observation.missing_sni_detail == Some(sniff::MissingSniDetail::FirstByteTimeout);
        // A probed flow's scan is recorded once the probe settles it.
        if !probe_server_first {
            self.record_scan(dst, &observation);
        }

        let mut policy = match policy {
            TrafficPolicy::Ready(policy) => policy,
            TrafficPolicy::NotReady(reason) => {
                return (
                    observation.scan.server_name().cloned(),
                    policy_not_ready(&mut inspection, reason),
                );
            }
        };

        // The unwritten probe connection, kept when the guest spoke first.
        let mut probed = None;
        // Runs a second time, on the guest's late bytes, when the guest spoke
        // first or the destination closed.
        let (observation, admitted, connection) = loop {
            let (admitted, mut decision) = if probe_server_first {
                let (admitted, decision) = self.admit_deferred(dst, &observation.scan, &policy);
                (admitted, Some(decision))
            } else {
                (self.admit(dst, &observation.scan, &policy), None)
            };
            let admitted = match admitted {
                Ok(admitted) => admitted,
                Err(reason) => {
                    // A probed flow has no SNI and needs no DNS evidence.
                    debug_assert!(!probe_server_first, "a probed flow was refused");
                    inspection.finish(
                        accounting::Outcome::Denied,
                        inspection_admission_reason(
                            &observation.scan,
                            observation.missing_sni_detail,
                            reason,
                        ),
                    );
                    if missing_client_hello == sniff::MissingClientHello::Denied
                        && matches!(
                            observation.scan,
                            crate::tls_sni::ClientHelloScan::Incomplete
                        )
                    {
                        self.warn_client_hello_missing(
                            dst,
                            observation.missing_sni_detail,
                            observation.elapsed,
                        );
                    }
                    return (
                        observation.scan.server_name().cloned(),
                        FlowOutcome::Rejected(Rejection::Admission(reason)),
                    );
                }
            };
            // Register before the final current-policy check so a concurrent
            // update either rejects this handoff or sees the flow in its resweep.
            let connection = self.network_state.tcp_connections().register(
                admitted.destination(),
                observation.scan.clone(),
                &policy,
                self.network_state.dns_allow_cache(),
                shutdown.clone(),
            );
            if let Err(reason) = self.validate_admission_policy(&policy) {
                if probe_server_first {
                    self.record_scan(dst, &observation);
                }
                inspection.finish(accounting::Outcome::Cancelled, reason.label());
                return (
                    observation.scan.server_name().cloned(),
                    FlowOutcome::Rejected(Rejection::Admission(reason)),
                );
            }
            if !std::mem::take(&mut probe_server_first) {
                break (observation, admitted, connection);
            }
            debug_assert!(!connection.interception_mode.is_off());
            let probe_started_at = Instant::now();
            let mut probe = server_first::probe(
                &client,
                admitted.destination(),
                connect,
                server_first::PROBE_WINDOW,
                shutdown,
            )
            .await;
            let probe_elapsed = probe_started_at.elapsed();
            if let server_first::Probe::GuestFirst(kept) = &mut probe {
                probed = kept.take();
            }
            let upstream_closed = matches!(probe, server_first::Probe::UpstreamClosed);
            // A flow whose guest may still speak is admitted again, which
            // counts its one admission decision.
            match decision.take() {
                Some(decision)
                    if upstream_closed || matches!(probe, server_first::Probe::GuestFirst(_)) =>
                {
                    decision.superseded();
                }
                decision => drop(decision),
            }
            // A greeting ends the wait for a ClientHello, so its delay is not
            // ClientHello time.
            if !matches!(probe, server_first::Probe::ServerFirst(_)) {
                observation.elapsed += probe_elapsed;
            }
            match probe {
                server_first::Probe::ServerFirst(greeted) => {
                    observation.missing_sni_detail = Some(sniff::MissingSniDetail::ServerFirst);
                    self.record_scan(dst, &observation);
                    if let Err(reason) = self.validate_admission_policy(&policy) {
                        inspection.finish(accounting::Outcome::Cancelled, reason.label());
                        return (None, FlowOutcome::Rejected(Rejection::Admission(reason)));
                    }
                    self.stats.tcp_total.fetch_add(1, Ordering::Relaxed);
                    close_gracefully(&client);
                    inspection.finish(accounting::Outcome::Skipped, "server_first");
                    debug!(
                        sandbox_id = self.sandbox_id,
                        destination_port = dst.port(),
                        waited_ms = probe_elapsed.as_millis(),
                        "relaying a server-speaks-first TCP flow under TLS interception"
                    );
                    let outcome = self.relay_probed(client, greeted).await;
                    drop(connection);
                    return (None, outcome);
                }
                server_first::Probe::GuestFirst(_) | server_first::Probe::UpstreamClosed => {
                    drop(connection);
                    let elapsed = observation.elapsed;
                    observation =
                        sniff::client_hello_with_detail(&client, sniff::MissingClientHello::Denied)
                            .await;
                    observation.elapsed += elapsed;
                    if upstream_closed
                        && observation.missing_sni_detail
                            == Some(sniff::MissingSniDetail::FirstByteTimeout)
                    {
                        observation.missing_sni_detail =
                            Some(sniff::MissingSniDetail::ServerFirstUpstreamClosed);
                    }
                    self.record_scan(dst, &observation);
                    // Decide the late bytes on the current policy, not the
                    // snapshot taken before the wait.
                    let current = self.network_state.wait_for_tcp_policy().await;
                    policy = match current {
                        TrafficPolicy::Ready(policy) => policy,
                        TrafficPolicy::NotReady(reason) => {
                            return (
                                observation.scan.server_name().cloned(),
                                policy_not_ready(&mut inspection, reason),
                            );
                        }
                    };
                }
                server_first::Probe::TimedOut => {
                    observation.missing_sni_detail =
                        Some(sniff::MissingSniDetail::ServerFirstTimeout);
                    self.record_scan(dst, &observation);
                    break (observation, admitted, connection);
                }
                server_first::Probe::ConnectFailed(error) => {
                    self.record_scan(dst, &observation);
                    self.stats.tcp_total.fetch_add(1, Ordering::Relaxed);
                    inspection.finish(accounting::Outcome::Error, "server_first_connect");
                    return (None, FlowOutcome::RelayFailed(RelayFailure::Connect(error)));
                }
                server_first::Probe::Cancelled => {
                    self.record_scan(dst, &observation);
                    inspection.finish(accounting::Outcome::Cancelled, "server_first_cancelled");
                    return (None, FlowOutcome::Cancelled);
                }
            }
        };
        let sniff::ClientHelloObservation {
            scan,
            client_alpn,
            missing_sni_detail,
            ..
        } = observation;
        let host = scan.server_name().cloned();
        let interception_mode = connection.interception_mode;
        self.stats.tcp_total.fetch_add(1, Ordering::Relaxed);

        let action = match self
            .tls_interception
            .before_relay(interception_mode, &scan, missing_sni_detail, dst.port())
            .await
        {
            Ok(action) => action,
            Err(error) => {
                let (outcome, reason) = interception_failure(&error);
                inspection.finish(outcome, reason);
                return (
                    host,
                    FlowOutcome::Rejected(Rejection::TlsInterception(error)),
                );
            }
        };
        if let Err(reason) = self.validate_admission_policy(&policy) {
            inspection.finish(accounting::Outcome::Cancelled, reason.label());
            return (host, FlowOutcome::Rejected(Rejection::Admission(reason)));
        }
        close_gracefully(&client);
        let outcome = match action {
            crate::tls_interception::InterceptionAction::PassThrough => {
                let reason = match policy.tls_interception {
                    pplx_space_net_common::TlsInterception::Off => "inspection_disabled",
                    pplx_space_net_common::TlsInterception::On if interception_mode.is_off() => {
                        "policy_exemption"
                    }
                    pplx_space_net_common::TlsInterception::On => "plaintext",
                };
                inspection.finish(accounting::Outcome::Skipped, reason);
                match (admitted, probed) {
                    // Some destinations accept one connection only, so the
                    // guest's bytes go over the probe's rather than a new dial.
                    (AdmittedTcp::Tunnel(destination), Some(probed)) if destination == dst => {
                        self.relay_probed(client, probed).await
                    }
                    (admitted, probed) => {
                        drop(probed);
                        self.relay(client, admitted, connect).await
                    }
                }
            }
            crate::tls_interception::InterceptionAction::Terminate(lease) => {
                drop(probed);
                inspection.finish(accounting::Outcome::Success, "admitted");
                match self
                    .terminate_tls(client, admitted, lease, client_alpn)
                    .await
                {
                    Ok(()) => FlowOutcome::InspectedTls,
                    Err(error) => FlowOutcome::RelayFailed(RelayFailure::Inspected(error)),
                }
            }
        };
        drop(connection);
        (host, outcome)
    }

    fn record_scan(&self, dst: SocketAddr, observation: &sniff::ClientHelloObservation) {
        sniff::record(
            self.sandbox_id,
            dst,
            &observation.scan,
            observation.client_alpn,
            observation.missing_sni_detail,
            observation.elapsed,
            &self.debug_state,
        );
    }

    /// A flow to a destination the guest resolved through an allowed domain,
    /// refused only because its ClientHello did not arrive in time, is a guest
    /// breakage rather than a policy verdict. Sampled like interception
    /// refusals so the cause survives a production log level.
    fn warn_client_hello_missing(
        &self,
        dst: SocketAddr,
        missing_sni_detail: Option<sniff::MissingSniDetail>,
        waited: Duration,
    ) {
        let detail = missing_sni_detail.map(sniff::MissingSniDetail::as_str);
        let (warn_now, suppressed) = self.refusal_warnings.sample("client_hello_missing", detail);
        if warn_now {
            warn!(
                sandbox_id = self.sandbox_id,
                destination_port = dst.port(),
                detail,
                suppressed,
                waited_ms = waited.as_millis(),
                "denied a DNS-attributed TCP flow whose ClientHello did not arrive in time"
            );
        }
    }

    async fn relay<F, Fut, U>(
        &self,
        client: TcpStream,
        admitted: AdmittedTcp,
        connect: F,
    ) -> FlowOutcome
    where
        F: FnOnce(SocketAddr) -> Fut + Send + 'static,
        Fut: Future<Output = Result<U, transport::Error>> + Send + 'static,
        U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        match admitted {
            AdmittedTcp::Tunnel(destination) => {
                self.pass_through(client, destination, connect).await
            }
            AdmittedTcp::HostBoundHttp(destination) => {
                self.host_bound_http(client, destination, connect).await
            }
        }
    }

    async fn pass_through<F, Fut, U>(
        &self,
        client: TcpStream,
        dst: SocketAddr,
        connect: F,
    ) -> FlowOutcome
    where
        F: FnOnce(SocketAddr) -> Fut,
        Fut: Future<Output = Result<U, transport::Error>>,
        U: AsyncRead + AsyncWrite + Unpin,
    {
        let upstream = match connect(dst).await {
            Ok(upstream) => upstream,
            Err(error) => return FlowOutcome::RelayFailed(RelayFailure::Connect(error)),
        };
        self.tunnel(client, upstream, 0).await
    }

    /// Forward what the probe read, then relay as a pass-through over the
    /// probe connection.
    async fn relay_probed<U>(
        &self,
        mut client: TcpStream,
        probed: server_first::Probed<U>,
    ) -> FlowOutcome
    where
        U: AsyncRead + AsyncWrite + transport::Close + Unpin,
    {
        use tokio::io::AsyncWriteExt as _;
        let server_first::Probed { upstream, received } = probed;
        if let Err(error) = client.write_all(&received).await {
            return FlowOutcome::RelayFailed(RelayFailure::Transfer(error));
        }
        // An abortive close at the end of the relay could discard data still
        // in flight, so refuse the flow rather than relay without an orderly one.
        if let Err(error) = upstream.set_abortive_close(false) {
            return FlowOutcome::RelayFailed(RelayFailure::Transfer(error));
        }
        let received_len = u64::try_from(received.len()).unwrap_or(u64::MAX);
        self.tunnel(client, upstream, received_len).await
    }

    /// Relay opaquely over an established upstream. `already_received` counts
    /// upstream bytes forwarded to the guest before the relay started.
    async fn tunnel<U>(&self, client: TcpStream, upstream: U, already_received: u64) -> FlowOutcome
    where
        U: AsyncRead + AsyncWrite + Unpin,
    {
        let report = relay::run(client, upstream, &self.shutdown_token, None).await;
        match report.result {
            Ok(()) => FlowOutcome::Relayed(Relay::Tunnelled {
                wire_tx: report.written.0,
                wire_rx: report.written.1.saturating_add(already_received),
            }),
            Err(relay::Error::Io(error)) => FlowOutcome::RelayFailed(RelayFailure::Transfer(error)),
            Err(relay::Error::Cancelled | relay::Error::Idle) => FlowOutcome::Cancelled,
        }
    }

    async fn host_bound_http<F, Fut, U>(
        &self,
        client: TcpStream,
        dst: SocketAddr,
        connect: F,
    ) -> FlowOutcome
    where
        F: FnOnce(SocketAddr) -> Fut + Send + 'static,
        Fut: Future<Output = Result<U, transport::Error>> + Send + 'static,
        U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let binding = http_host::HostBinding::new(self.sandbox_id, dst, self.network_state.clone());
        match http_host::serve(client, binding, connect).await {
            Ok(served) => FlowOutcome::Relayed(Relay::HostBoundHttp {
                requests: served.requests,
                forwarded: served.forwarded,
                refused: served.refused,
                upstream_failed: served.upstream_failed,
                wire_tx: served.wire_tx,
                wire_rx: served.wire_rx,
            }),
            Err(error) => FlowOutcome::RelayFailed(RelayFailure::Plaintext(error)),
        }
    }

    fn record(&self, dst: SocketAddr, host: Option<&DomainName>, outcome: FlowOutcome) -> bool {
        let (status, detail) = outcome.metric_tags();
        pplx_space_metrics::gateway::tcp_connection(status, detail);
        let reported = if let (Some(host), Some(reason)) = (host, outcome.failure_reason()) {
            self.known_egress_state
                .report_tcp_failure(self.sandbox_id, dst, host, reason.as_ref())
        } else {
            false
        };

        match outcome {
            // These phases have no branch-specific telemetry beyond the
            // terminal connection metric emitted above.
            FlowOutcome::Cancelled
            | FlowOutcome::InspectedTls
            | FlowOutcome::RelayFailed(RelayFailure::Connect(_) | RelayFailure::Transfer(_)) => {}
            FlowOutcome::RelayFailed(RelayFailure::Inspected(error)) => {
                pplx_space_metrics::gateway::domain_binding(
                    "tcp",
                    "tls_transport",
                    error.as_str(),
                    None,
                );
            }
            FlowOutcome::HandledDns {
                messages,
                wire_tx,
                wire_rx,
            } => {
                self.stats.tcp_total.fetch_add(1, Ordering::Relaxed);
                self.stats.add_network_bytes(wire_tx, wire_rx);
                pplx_space_metrics::gateway::tcp_bytes("tx", wire_tx);
                pplx_space_metrics::gateway::tcp_bytes("rx", wire_rx);
                debug!(
                    sandbox_id = self.sandbox_id,
                    %dst,
                    messages,
                    "handled policy-gated TCP DNS connection"
                );
            }
            FlowOutcome::RelayFailed(RelayFailure::Plaintext(error)) => match error {
                http_host::Failure::Setup(error)
                    if matches!(error.as_ref(), upstream::Error::Connect(_)) => {}
                http_host::Failure::Setup(_) => pplx_space_metrics::gateway::domain_binding(
                    "tcp",
                    "http_host",
                    "upstream_http_failed",
                    None,
                ),
                http_host::Failure::GuestHttp(_) => pplx_space_metrics::gateway::domain_binding(
                    "tcp",
                    "http_host",
                    "guest_http_failed",
                    None,
                ),
            },
            FlowOutcome::Rejected(Rejection::PolicyMissing(reason)) => {
                debug!(
                    sandbox_id = self.sandbox_id,
                    %dst,
                    reason = reason.as_str(),
                    "denied TCP egress while network policy is not ready"
                );
                // `evaluate_egress` meters every other verdict, but it cannot
                // run without a policy. No policy also means no version to tag.
                pplx_space_metrics::gateway::policy_decision(
                    pplx_space_metrics::gateway::PolicyDecisionKind::DnsAttributed,
                    EgressProtocol::Tcp.as_str(),
                    EgressPhase::Admission.as_str(),
                    "unknown",
                    reason.as_str(),
                    interception_for_not_ready(reason),
                );
                self.stats.tcp_denied.fetch_add(1, Ordering::Relaxed);
            }
            FlowOutcome::Rejected(Rejection::Admission(reason)) => {
                debug!(
                    sandbox_id = self.sandbox_id,
                    %dst,
                    reason = reason.label(),
                    "denied TCP egress"
                );
                self.stats.tcp_denied.fetch_add(1, Ordering::Relaxed);
            }
            FlowOutcome::Rejected(Rejection::TlsInterception(error)) => {
                let detail = interception_detail(&error);
                pplx_space_metrics::gateway::domain_binding(
                    "tcp",
                    "tls_transport",
                    error.as_str(),
                    detail,
                );
                // Every arm here is a fail-closed guest breakage, so the cause
                // has to survive a production log level. Certificate refusals
                // previously carried their gRPC code only in the message.
                let (warn_now, suppressed) = self.refusal_warnings.sample(error.as_str(), detail);
                if warn_now {
                    warn!(
                        sandbox_id = self.sandbox_id,
                        destination_port = dst.port(),
                        outcome = error.as_str(),
                        detail,
                        suppressed,
                        %error,
                        "experimental TLS interception closed the connection"
                    );
                } else {
                    debug!(
                        sandbox_id = self.sandbox_id,
                        destination_port = dst.port(),
                        outcome = error.as_str(),
                        detail,
                        %error,
                        "experimental TLS interception closed the connection"
                    );
                }
                self.stats.tcp_denied.fetch_add(1, Ordering::Relaxed);
            }
            FlowOutcome::Relayed(Relay::Tunnelled { wire_tx, wire_rx }) => {
                self.stats.add_network_bytes(wire_tx, wire_rx);
                pplx_space_metrics::gateway::tcp_bytes("tx", wire_tx);
                pplx_space_metrics::gateway::tcp_bytes("rx", wire_rx);
            }
            FlowOutcome::Relayed(Relay::HostBoundHttp {
                requests,
                forwarded,
                refused,
                upstream_failed,
                wire_tx,
                wire_rx,
            }) => {
                debug!(
                    sandbox_id = self.sandbox_id,
                    %dst,
                    requests,
                    forwarded,
                    refused,
                    upstream_failed,
                    "validated plaintext HTTP hosts on a DNS-authorized flow"
                );
                self.stats.add_network_bytes(wire_tx, wire_rx);
                pplx_space_metrics::gateway::tcp_bytes("tx", wire_tx);
                pplx_space_metrics::gateway::tcp_bytes("rx", wire_rx);
            }
        }
        reported
    }
}

/// A refusal must read as one. Closing a socket with nothing left to read
/// sends a FIN, which a guest cannot tell from an orderly shutdown: a flow
/// denied before its ClientHello arrived surfaced as an unexplained TLS EOF.
/// Every close before admission is therefore abortive (RST), and a serving
/// path restores the orderly close with [`close_gracefully`] before it takes
/// the socket.
fn abort_on_close(client: &TcpStream) {
    if let Err(error) = client.set_zero_linger() {
        debug!(%error, "could not make a pending TCP flow's close abortive");
    }
}

/// Undo [`abort_on_close`] once a flow is admitted, so its close is orderly.
fn close_gracefully(client: &TcpStream) {
    if let Err(error) = socket2::SockRef::from(client).set_linger(None) {
        warn!(%error, "could not restore an admitted TCP flow's orderly close");
    }
}

fn policy_not_ready(inspection: &mut accounting::Attempt, reason: PolicyNotReady) -> FlowOutcome {
    // Reserve `denied` for an admission decision. Policy readiness failures never
    // reached evaluation; only lifecycle shutdown is cancellation.
    let outcome = if matches!(reason, PolicyNotReady::Draining | PolicyNotReady::Stopped) {
        accounting::Outcome::Cancelled
    } else {
        accounting::Outcome::Error
    };
    inspection.finish(outcome, reason.as_str());
    FlowOutcome::Rejected(Rejection::PolicyMissing(reason))
}

fn inspection_admission_reason(
    scan: &crate::tls_sni::ClientHelloScan,
    missing_sni_detail: Option<crate::sniff::MissingSniDetail>,
    denial: TcpAdmissionDenial,
) -> &'static str {
    match scan {
        crate::tls_sni::ClientHelloScan::NoServerName
        | crate::tls_sni::ClientHelloScan::Incomplete
        | crate::tls_sni::ClientHelloScan::Malformed => missing_sni_detail
            .map(crate::sniff::MissingSniDetail::as_str)
            .unwrap_or_else(|| denial.label()),
        crate::tls_sni::ClientHelloScan::ServerName(_)
        | crate::tls_sni::ClientHelloScan::NotTls => denial.label(),
    }
}

/// Bounded detail for an interception refusal. Certificate refusals share one
/// `certificate_error` outcome, so the gRPC code is the only thing separating a
/// CA outage from a local capacity or lifetime refusal.
/// The interception choice to report for a flow refused before policy was
/// evaluated. `InterceptionNotReady` is only reachable with a policy installed
/// and interception enabled, because the readiness gate it fails is skipped
/// outright when interception is off; reporting `unknown` for it would drop
/// those denials out of the enabled-rollout rate. Every other reason genuinely
/// has no installed policy to read a choice from.
pub(crate) fn interception_for_not_ready(reason: crate::state::PolicyNotReady) -> &'static str {
    match reason {
        crate::state::PolicyNotReady::InterceptionNotReady => {
            pplx_space_net_common::TlsInterception::On.as_str()
        }
        _ => "unknown",
    }
}

fn interception_detail(error: &TlsInterceptionError) -> Option<&'static str> {
    match error {
        TlsInterceptionError::MissingServerName(detail) => Some(detail.as_str()),
        TlsInterceptionError::CertificateRequest(status) => Some(match status.code() {
            tonic::Code::Cancelled => "cancelled",
            tonic::Code::Aborted => "aborted",
            tonic::Code::ResourceExhausted => "resource_exhausted",
            tonic::Code::FailedPrecondition => "failed_precondition",
            tonic::Code::DeadlineExceeded => "deadline_exceeded",
            tonic::Code::InvalidArgument => "invalid_argument",
            tonic::Code::Unavailable => "unavailable",
            tonic::Code::Internal => "internal",
            _ => "other",
        }),
        TlsInterceptionError::AuthorityUnavailable | TlsInterceptionError::Timeout => None,
    }
}

fn interception_failure(error: &TlsInterceptionError) -> (accounting::Outcome, &'static str) {
    use accounting::Outcome;

    match error {
        TlsInterceptionError::AuthorityUnavailable => (Outcome::Error, "authority_unavailable"),
        TlsInterceptionError::MissingServerName(detail) => (Outcome::Denied, detail.as_str()),
        TlsInterceptionError::Timeout => (Outcome::Error, "certificate_timeout"),
        TlsInterceptionError::CertificateRequest(status) => match status.code() {
            tonic::Code::Cancelled => (Outcome::Cancelled, "certificate_cancelled"),
            tonic::Code::Aborted => (Outcome::Cancelled, "certificate_invalidated"),
            tonic::Code::ResourceExhausted => (Outcome::Error, "certificate_capacity"),
            tonic::Code::FailedPrecondition => (Outcome::Error, "certificate_configuration"),
            tonic::Code::DeadlineExceeded => (Outcome::Error, "certificate_timeout"),
            tonic::Code::Unavailable => (Outcome::Error, "certificate_unavailable"),
            _ => (Outcome::Error, "certificate_error"),
        },
    }
}

#[cfg(test)]
mod tests;
