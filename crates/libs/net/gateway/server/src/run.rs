//! The SPACE per-sandbox network gateway: the transparent L4 proxy that
//! enforces a sandbox's network policy, plus the Unix-socket gRPC control
//! surface sandbox-manager drives it through.
//!
//! This module is the whole service. The `space-netgtw-svc` binary is a thin
//! `main` that parses flags, initializes telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the service.

use pplx_space_common_core::types::{SandboxId, SpaceApiRouteIdentity};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_connection_admission::{
    AdmittedConnections, ConnectionAdmissionConfig, ConnectionAdmissionEvent,
    ConnectionAdmissionFailure, admit_connections,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_net_common::DomainName;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio_stream::Stream;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;

use crate::certificates::{CertificateContext, CertificateProvider, GatewayAuthority};
use crate::control::NetGatewayControlService;
use crate::debug::GatewayDebugState;
use crate::ingress::ServicePortIngressState;
use crate::known_egress::KnownEgressState;
use crate::proto::net_gateway_control_server::NetGatewayControlServer;
use crate::reserved_dns::{BuildReservedDnsCoreError, ReservedDnsCore};
use crate::resource_budget::MAX_CONTROL_CONNECTIONS;
use crate::state::{GatewayNetworkState, NetworkStateReason, host_control_policy_loader};
use crate::stats::ProxyStats;
use crate::tls_interception::TlsInterception;
use crate::{TCP_PROXY_PORT, UDP_PROXY_PORT, tcp_proxy, udp_proxy};

/// The service name the gateway reports to telemetry and names its
/// Unix-socket rate-limit surface with.
pub const SERVICE_NAME: &str = "space-net-gateway";

/// Initial policy availability selected by the process configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitialPolicyState {
    NoIdentity,
    PendingPolicy,
}

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. The launch arguments that are accepted purely for
/// compatibility with older sandbox-managers are no-ops and do not appear
/// here.
#[derive(Debug, Clone)]
pub struct NetGatewayRunConfig {
    /// Unix socket path for the control service (sbx-manager connects here).
    pub socket_path: PathBuf,
    /// Sandbox ID this gateway is managing.
    pub sandbox_id: SandboxId,
    /// Inherited TCP listener FD (created by node-manager with
    /// `IP_TRANSPARENT`). `None` builds a transparent fallback listener.
    pub tcp_fd: Option<i32>,
    /// Inherited UDP socket FD (created by node-manager with `IP_TRANSPARENT`
    /// + `IP_RECVORIGDSTADDR`). `None` builds a transparent fallback socket.
    pub udp_fd: Option<i32>,
    pub initial_policy_state: InitialPolicyState,
    /// Exact domains whose failed DNS or TCP flows should emit an error.
    pub known_egress_domains: Vec<DomainName>,
    /// May be partial while execution identity is being enriched.
    pub route_identity: Option<SpaceApiRouteIdentity>,
    /// The W3C `traceparent` of the traced launch that started this gateway,
    /// passed as `TRACEPARENT`; see [`startup_span`].
    pub launch_traceparent: Option<String>,
}

/// The root of this gateway's startup, when a traced request launched it.
///
/// The gateway outlives the request that launched it, so its startup is a
/// root linked to that launch and sampled as it was, as other work that
/// outlives a request is. Its startup events and the policy hydration until
/// the gateway first settles run under it, so they join the launching
/// request's work. A gateway launched with no trace context, such as one the
/// watchdog replaced, starts untraced.
fn startup_span(sandbox_id: &str, launch_traceparent: Option<&str>) -> Option<tracing::Span> {
    let launch = launch_traceparent?;
    let span = tracing::info_span!(
        parent: None,
        "netgtw.startup",
        otel.kind = "internal",
        otel.name = "NetGatewayStartup",
        "operation.name" = "netgtw.startup",
        sandbox_id,
        origin_trace_id = tracing::field::Empty,
        origin_sampled = tracing::field::Empty,
    );
    pplx_space_observability::trace_propagation::link_root_to_origins(&span, [launch]);
    Some(span)
}

/// Run `f` inside the startup span, when there is one.
fn in_startup(startup: Option<&tracing::Span>, f: impl FnOnce()) {
    match startup {
        Some(span) => span.in_scope(f),
        None => f(),
    }
}

/// Why [`run`] returned without a clean shutdown.
///
/// Every variant is fatal to the process: the gateway is a per-sandbox
/// sidecar, so a dead control surface or data-plane loop has to surface as a
/// non-zero exit for the sandbox watchdog to replace it.
#[derive(Debug, thiserror::Error)]
pub enum RunNetGatewayError {
    #[error("failed to initialize reserved-DNS resolution: {0}")]
    ReservedDns(#[source] BuildReservedDnsCoreError),
    #[error("failed to bind the net-gateway control socket {}: {source}", path.display())]
    BindControlSocket {
        path: PathBuf,
        #[source]
        source: pplx_space_common_svc::bootstrap::BindUnixSocketError,
    },
    #[error("failed to adopt the inherited TCP listener fd {fd}: {source}")]
    InheritedTcpListener {
        fd: i32,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to adopt the inherited UDP socket fd {fd}: {source}")]
    InheritedUdpSocket {
        fd: i32,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create a transparent TCP fallback listener on port {port}: {source}")]
    TransparentTcpListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to create a transparent UDP fallback listener on port {port}: {source}")]
    TransparentUdpListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    /// The control listener stopped accepting: the gateway is unreachable
    /// even though the data plane may still be up.
    #[error("net-gateway control listener failed: {0}")]
    ControlAdmission(#[source] ConnectionAdmissionFailure),
    #[error("net-gateway control server exited unexpectedly")]
    ControlServerExited,
    #[error("net-gateway control server failed: {0}")]
    ControlServer(#[source] tonic::transport::Error),
    /// The gateway CA owner returned while the service was live. Its outcome
    /// is rendered rather than chained because the task's own result carries
    /// no error type of its own.
    #[error("gateway CA owner exited unexpectedly: {outcome}")]
    AuthorityExited { outcome: String },
    #[error("{proxy} proxy exited unexpectedly")]
    ProxyExited { proxy: &'static str },
    #[error("{proxy} proxy failed: {source}")]
    ProxyFailed {
        proxy: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("{proxy} proxy task failed: {source}")]
    ProxyTaskFailed {
        proxy: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },
}

fn bounded_control_connections(
    listener: UnixListener,
    shutdown_token: CancellationToken,
) -> AdmittedConnections<UnixStream> {
    bounded_control_connections_from_stream(
        UnixListenerStream::new(listener),
        shutdown_token,
        pplx_space_metrics::gateway::control_accept_backoff,
    )
}

fn bounded_control_connections_from_stream<S, B>(
    incoming: S,
    shutdown_token: CancellationToken,
    record_backoff: B,
) -> AdmittedConnections<UnixStream>
where
    S: Stream<Item = Result<UnixStream, std::io::Error>> + Send + Unpin + 'static,
    B: Fn(&'static str) + Send + 'static,
{
    admit_connections(
        incoming,
        shutdown_token,
        ConnectionAdmissionConfig::new("net-gateway-control", MAX_CONTROL_CONNECTIONS),
        move |_listener, observation| match observation.event() {
            ConnectionAdmissionEvent::CapacityExhausted => {
                pplx_space_metrics::gateway::control_connection_limit_wait();
            }
            ConnectionAdmissionEvent::AcceptError(reason) => {
                pplx_space_metrics::gateway::control_accept_error(reason.as_str());
            }
            ConnectionAdmissionEvent::AcceptBackoff(reason) => {
                record_backoff(reason.as_str());
            }
        },
    )
}

const DNS_CACHE_SWEEP_INTERVAL: Duration = Duration::from_mins(1);
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

fn create_transparent_tcp_listener(port: u16) -> std::io::Result<TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    crate::orig_dst::set_ip_transparent(&socket)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    socket.listen(1024)?;

    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

fn create_transparent_udp_listener(port: u16) -> std::io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    crate::orig_dst::set_ip_transparent(&socket)?;
    crate::orig_dst::set_recvorigdstaddr(&socket)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;

    Ok(socket.into())
}

/// Run the net gateway until `shutdown_token` fires or a fatal condition ends
/// it. Telemetry must already be initialized by the caller.
pub async fn run(
    config: NetGatewayRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunNetGatewayError> {
    let short_id = config.sandbox_id.label();
    // Net-gateway is a per-sandbox process: the id is set once from the run
    // config and is valid until process exit. Leak it so spawned proxy tasks
    // can carry it as a Copy `&'static str` log field without any per-spawn
    // cloning or refcount machinery.
    let sandbox_id: &'static str = config.sandbox_id.to_string().leak();
    let startup = startup_span(sandbox_id, config.launch_traceparent.as_deref());

    tracing::info!(
        sandbox_id = %config.sandbox_id,
        no_identity = config.initial_policy_state == InitialPolicyState::NoIdentity,
        "starting net gateway"
    );

    let stats = Arc::new(ProxyStats::new());
    let reserved_dns = ReservedDnsCore::new().map_err(RunNetGatewayError::ReservedDns)?;
    let network_state = if config.initial_policy_state == InitialPolicyState::NoIdentity {
        in_startup(startup.as_ref(), || {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::NetworkPolicyEnforcementCompleted,
                SandboxEventSource::NetGateway,
                SandboxEventDetails::default()
                    .operation("initial_policy_hydration")
                    .network_state("no_identity")
                    .installed_network_policy_ref(
                        pplx_space_net_common::EffectiveNetworkPolicyRef::no_identity().to_string(),
                    )
                    .result("no_identity"),
            );
        });
        GatewayNetworkState::no_identity()
    } else {
        GatewayNetworkState::pending_policy()
    }
    .with_readiness_metrics();
    let policy_loader = host_control_policy_loader(
        config.sandbox_id,
        stats.clone(),
        network_state.owner_contact(),
    );
    let trusted_sandbox_id = config.sandbox_id;
    let service_port_ingress =
        ServicePortIngressState::new_for_sandbox(trusted_sandbox_id, config.route_identity.clone());
    let debug_state = GatewayDebugState::default();
    let known_egress_state = KnownEgressState::new(config.known_egress_domains.clone());
    let proxy_shutdown_token = CancellationToken::new();
    let authority = GatewayAuthority::new(
        pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET.into(),
        service_port_ingress.clone(),
        proxy_shutdown_token.clone(),
    )
    .with_readiness_observer(network_state.interception_readiness_observer());
    let network_state = {
        let authority = authority.clone();
        network_state.with_traffic_gate(Arc::new(move || authority.is_ready()))
    };
    let certificate_context =
        CertificateContext::new(network_state.clone(), proxy_shutdown_token.clone());
    let certificates = CertificateProvider::new(authority.clone(), certificate_context);
    // The control endpoint must exist before CA acquisition and guest traffic
    // startup. The owner never waits for a guest request or Started state.
    let control_listener = pplx_space_common_svc::bind_unix_socket(
        &config.socket_path,
        pplx_space_common_svc::UnixSocketOptions::new(0o770),
    )
    .map_err(|source| RunNetGatewayError::BindControlSocket {
        path: config.socket_path.clone(),
        source,
    })?;
    let hydration_task =
        (config.initial_policy_state != InitialPolicyState::NoIdentity).then(|| {
            network_state.spawn_hydration(
                policy_loader.clone(),
                proxy_shutdown_token.child_token(),
                startup.clone(),
            )
        });
    let dns_cache_shutdown_token = proxy_shutdown_token.child_token();
    let dns_cache = network_state.dns_allow_cache().clone();
    let certificate_cache = certificates.clone();
    let dns_cache_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(DNS_CACHE_SWEEP_INTERVAL);
        loop {
            tokio::select! {
                () = dns_cache_shutdown_token.cancelled() => break,
                _instant = interval.tick() => {
                    dns_cache.prune_expired();
                    if let Err(error) = certificate_cache.prune() {
                        tracing::error!(%error, "certificate cache sweep failed");
                    }
                },
            }
        }
    });

    // --- TCP proxy ---
    let tcp_listener = if let Some(fd) = config.tcp_fd {
        crate::inherited::tcp_listener_from_fd(fd)
            .map_err(|source| RunNetGatewayError::InheritedTcpListener { fd, source })?
    } else {
        tracing::warn!(
            sandbox_id,
            port = TCP_PROXY_PORT,
            "net gateway started without inherited TCP fd; creating transparent fallback listener"
        );
        create_transparent_tcp_listener(TCP_PROXY_PORT).map_err(|source| {
            RunNetGatewayError::TransparentTcpListener {
                port: TCP_PROXY_PORT,
                source,
            }
        })?
    };

    let tcp_stats = stats.clone();
    let tls_interception = TlsInterception::new(certificates.requester());
    let shutdown_tls_interception = tls_interception.clone();
    let tcp_network_state = network_state.clone();
    let tcp_reserved_dns = reserved_dns.clone();
    let tcp_debug_state = debug_state.clone();
    let tcp_known_egress_state = known_egress_state.clone();
    let tcp_shutdown_token = proxy_shutdown_token.child_token();
    let mut tcp_task = tokio::spawn(async move {
        let result = tcp_proxy::run(tcp_proxy::TcpProxyConfig {
            sandbox_id,
            listener: tcp_listener,
            stats: tcp_stats,
            network_state: tcp_network_state,
            tls_interception,
            reserved_dns: tcp_reserved_dns,
            known_egress_state: tcp_known_egress_state,
            debug_state: tcp_debug_state,
            shutdown_token: tcp_shutdown_token,
        })
        .await;
        if let Err(error) = &result {
            tracing::error!(sandbox_id, %error, "TCP proxy failed");
        }
        result
    });

    // --- UDP proxy ---
    let udp_listener = if let Some(fd) = config.udp_fd {
        crate::inherited::udp_socket_from_fd(fd)
            .map_err(|source| RunNetGatewayError::InheritedUdpSocket { fd, source })?
    } else {
        tracing::warn!(
            sandbox_id,
            port = UDP_PROXY_PORT,
            "net gateway started without inherited UDP fd; creating transparent fallback listener"
        );
        create_transparent_udp_listener(UDP_PROXY_PORT).map_err(|source| {
            RunNetGatewayError::TransparentUdpListener {
                port: UDP_PROXY_PORT,
                source,
            }
        })?
    };

    let udp_stats = stats.clone();
    let udp_network_state = network_state.clone();
    let udp_known_egress_state = known_egress_state.clone();
    let udp_reserved_dns = reserved_dns;
    let udp_shutdown_token = proxy_shutdown_token.child_token();
    let mut udp_task = tokio::spawn(async move {
        let result = Box::pin(udp_proxy::run(udp_proxy::UdpProxyConfig {
            sandbox_id,
            listener: udp_listener,
            stats: udp_stats,
            network_state: udp_network_state,
            known_egress_state: udp_known_egress_state,
            reserved_dns: udp_reserved_dns,
            shutdown_token: udp_shutdown_token,
        }))
        .await;
        if let Err(error) = &result {
            tracing::error!(sandbox_id, %error, "UDP proxy failed");
        }
        result
    });

    // --- Control socket ---
    // Node-manager prepares /space/run/{sandbox_id} as group space-sidecarclients
    // with the setgid bit, so this per-sandbox socket inherits that group even
    // though net-gateway runs as the sandbox uid. Do not chgrp here: after
    // privilege drop the sandbox uid cannot reliably assign service groups.
    tracing::info!(
        sandbox_id = %config.sandbox_id,
        short_id = %short_id,
        socket_path = %config.socket_path.display(),
        "net gateway listening"
    );
    let control_ready_snapshot = network_state.snapshot().await;
    in_startup(startup.as_ref(), || {
        emit_sandbox_event(
            sandbox_id,
            SandboxEventType::NetworkPolicyEnforcementCompleted,
            SandboxEventSource::NetGateway,
            SandboxEventDetails::default()
                .operation("control_socket_ready")
                .network_state(control_ready_snapshot.mode.to_string())
                .installed_network_policy_ref(control_ready_snapshot.installed_policy_ref)
                .result("ready_for_refresh"),
        );
    });
    // The hydration task holds the startup span until the gateway settles.
    drop(startup);

    let service = NetGatewayControlService::new(
        stats,
        network_state.clone(),
        policy_loader,
        debug_state,
        service_port_ingress,
        proxy_shutdown_token.child_token(),
    );
    let control_admission =
        bounded_control_connections(control_listener, proxy_shutdown_token.child_token());
    let control_server = async move {
        let result = control_admission
            .supervise(move |incoming| {
                pplx_space_common_svc::grpc_server_builder()
                    .layer(
                        pplx_space_common_svc::conservative_uds_grpc_rate_limiter_layer(
                            SERVICE_NAME,
                        ),
                    )
                    .layer(pplx_space_common_svc::grpc_trace_layer())
                    .layer(pplx_space_common_svc::SandboxSpanLayer::new(
                        trusted_sandbox_id,
                    ))
                    .add_service(NetGatewayControlServer::new(service))
                    .serve_with_incoming(incoming)
            })
            .await
            .map_err(RunNetGatewayError::ControlAdmission)?;
        unexpected_control_exit(result)
    };

    let mut authority_task = authority.spawn(network_state.clone());

    // A data-plane loop returning while the service is live is always fatal,
    // including a clean return or task panic. Otherwise the control socket can
    // remain healthy while guest networking is permanently blackholed, and the
    // sandbox watchdog has no reason to replace this process.
    let (service_result, tcp_finished, udp_finished, authority_finished) = tokio::select! {
        result = control_server => (result, false, false, false),
        () = shutdown_token.cancelled() => (Ok(()), false, false, false),
        result = &mut authority_task => (Err(RunNetGatewayError::AuthorityExited { outcome: format!("{result:?}") }), false, false, true),
        result = &mut tcp_task => (
            Err(unexpected_proxy_exit("TCP", result)),
            true,
            false,
            false,
        ),
        result = &mut udp_task => (
            Err(unexpected_proxy_exit("UDP", result)),
            false,
            true,
            false,
        ),
    };

    let network_state_for_shutdown = network_state.clone();
    network_state_for_shutdown
        .set_draining(NetworkStateReason::Shutdown)
        .await;
    proxy_shutdown_token.cancel();
    shutdown_tls_interception.close_handshakes();
    if !authority_finished {
        join_with_deadline("gateway_authority", authority_task).await;
    }
    if let Some(hydration_task) = hydration_task {
        join_with_deadline("policy_hydration", hydration_task).await;
    }
    if !tcp_finished {
        join_proxy_with_deadline("tcp_proxy", tcp_task).await;
    }
    if !udp_finished {
        join_proxy_with_deadline("udp_proxy", udp_task).await;
    }
    join_with_deadline("dns_cache", dns_cache_task).await;
    network_state_for_shutdown
        .set_stopped(NetworkStateReason::Shutdown)
        .await;
    let stopped_snapshot = network_state_for_shutdown.snapshot().await;
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::NetworkPathStopped,
        SandboxEventSource::NetGateway,
        SandboxEventDetails::default()
            .operation("shutdown")
            .network_state(stopped_snapshot.mode.to_string())
            .installed_network_policy_ref(stopped_snapshot.installed_policy_ref),
    );

    if let Err(error) = &service_result {
        tracing::error!(
            sandbox_id = %config.sandbox_id,
            short_id = %short_id,
            %error,
            "net-gateway service failed"
        );
    }

    service_result
}

fn unexpected_proxy_exit(
    proxy: &'static str,
    result: Result<std::io::Result<()>, tokio::task::JoinError>,
) -> RunNetGatewayError {
    match result {
        Ok(Ok(())) => RunNetGatewayError::ProxyExited { proxy },
        Ok(Err(source)) => RunNetGatewayError::ProxyFailed { proxy, source },
        Err(source) => RunNetGatewayError::ProxyTaskFailed { proxy, source },
    }
}

fn unexpected_control_exit(
    result: Result<(), tonic::transport::Error>,
) -> Result<(), RunNetGatewayError> {
    match result {
        Ok(()) => Err(RunNetGatewayError::ControlServerExited),
        Err(error) => Err(RunNetGatewayError::ControlServer(error)),
    }
}

async fn join_with_deadline<T>(name: &'static str, mut task: tokio::task::JoinHandle<T>) {
    match tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, &mut task).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::warn!(task = name, %error, "net-gateway task failed during shutdown");
        }
        Err(_) => {
            tracing::warn!(
                task = name,
                drain_timeout_ms =
                    u64::try_from(SHUTDOWN_DRAIN_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                "net-gateway task did not drain before deadline; aborting"
            );
            task.abort();
            // Do not await again: a future that failed to yield before the
            // drain deadline cannot observe abort, and waiting here would turn
            // a bounded shutdown into an indefinite one. Runtime teardown
            // drops the already-aborted async task.
        }
    }
}

async fn join_proxy_with_deadline(
    name: &'static str,
    mut task: tokio::task::JoinHandle<std::io::Result<()>>,
) {
    match tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, &mut task).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            tracing::warn!(task = name, %error, "net-gateway proxy failed during shutdown");
        }
        Ok(Err(error)) => {
            tracing::warn!(task = name, %error, "net-gateway task failed during shutdown");
        }
        Err(_) => {
            tracing::warn!(
                task = name,
                drain_timeout_ms =
                    u64::try_from(SHUTDOWN_DRAIN_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
                "net-gateway task did not drain before deadline; aborting"
            );
            task.abort();
            // See `join_with_deadline`: abort must remain the terminal bounded
            // action even when a broken proxy future never yields.
        }
    }
}

#[cfg(test)]
mod tests;
