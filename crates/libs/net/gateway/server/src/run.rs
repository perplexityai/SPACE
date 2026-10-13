//! The SPACE per-sandbox network gateway: the transparent L4 proxy that
//! enforces a sandbox's network policy, plus the Unix-socket gRPC control
//! surface sandbox-manager drives it through.
//!
//! This module is the whole service. The `space-netgtw-svc` binary is a thin
//! `main` that parses flags, initializes telemetry, and calls [`run`], so a
//! deployment that needs private wiring can supply its own `main` against this
//! library without copying any of the service.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_connection_admission::{
    AdmittedConnections, ConnectionAdmissionConfig, ConnectionAdmissionEvent,
    ConnectionAdmissionFailure, admit_connections,
};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_net_common::{
    DomainName, NetworkPolicyV1, proto::NetworkPolicyV1 as NetworkPolicyProto,
};
use pplx_space_spaced_common::proto::host_control::GetNetworkPolicyRequest;
use pplx_space_spaced_common::proto::host_control::host_control_client::HostControlClient;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio_stream::Stream;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;

use crate::certificates::{CertificateContext, CertificateProvider, GatewayAuthority};
use crate::control::NetGatewayControlService;
use crate::debug::GatewayDebugState;
use crate::dns::DnsAllowCache;
use crate::egress::DnsAttributedTcpTracker;
use crate::ingress::{
    ServicePortIngressState, route_identity_from_proto, status_for_route_identity_parse,
};
use crate::known_egress::KnownEgressState;
use crate::proto::net_gateway_control_server::NetGatewayControlServer;
use crate::reserved_dns::{BuildReservedDnsCoreError, ReservedDnsCore};
use crate::resource_budget::MAX_CONTROL_CONNECTIONS;
use crate::state::{
    GatewayNetworkState, NetworkCredentialSnapshot, NetworkStateReason, PolicyLoader,
    PulledNetworkPolicy, ReservedDnsCancellation,
};
use crate::stats::ProxyStats;
use crate::tls_interception::TlsInterception;
use crate::{TCP_PROXY_PORT, UDP_PROXY_PORT, tcp_proxy, udp_proxy};

/// The service name the gateway reports to telemetry and names its
/// Unix-socket rate-limit surface with.
pub const SERVICE_NAME: &str = "space-net-gateway";

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. The launch arguments that are accepted purely for
/// compatibility with older sandbox-managers are no-ops and do not appear
/// here.
#[derive(Debug, Clone)]
pub struct NetGatewayRunConfig {
    /// Unix socket path for the control service (sbx-manager connects here).
    pub socket_path: String,
    /// Sandbox ID this gateway is managing.
    pub sandbox_id: String,
    /// Inherited TCP listener FD (created by node-manager with
    /// `IP_TRANSPARENT`). `None` builds a transparent fallback listener.
    pub tcp_fd: Option<i32>,
    /// Inherited UDP socket FD (created by node-manager with `IP_TRANSPARENT`
    /// + `IP_RECVORIGDSTADDR`). `None` builds a transparent fallback socket.
    pub udp_fd: Option<i32>,
    /// Start closed without tenant identity. Used for warm-pool members.
    pub no_identity: bool,
    /// Exact domains whose failed DNS or TCP flows should emit an error.
    pub known_egress_domains: Vec<DomainName>,
    /// SPACE route generation this gateway is bound to for ServicePort ingress.
    pub route_generation: Option<u64>,
    /// SPACE node id this gateway is bound to for ServicePort ingress.
    pub current_node_id: Option<String>,
    /// Sandbox execution generation this gateway is bound to for ServicePort
    /// ingress.
    pub execution_generation: Option<u64>,
    /// Kubernetes pod uid this gateway is bound to for ServicePort ingress.
    pub pod_uid: Option<String>,
    /// Activation dispatch this gateway is bound to for ServicePort ingress.
    pub activation_attempt_id: Option<String>,
    /// Runtime substrate this gateway is bound to for ServicePort ingress.
    pub substrate_kind: Option<pplx_space_common_core::types::NodeManagedBy>,
}

/// Why [`run`] returned without a clean shutdown.
///
/// Every variant is fatal to the process: the gateway is a per-sandbox
/// sidecar, so a dead control surface or data-plane loop has to surface as a
/// non-zero exit for the sandbox watchdog to replace it.
#[derive(Debug, thiserror::Error)]
pub enum RunNetGatewayError {
    #[error("invalid sandbox id {sandbox_id:?}: {source}")]
    SandboxLabel {
        sandbox_id: String,
        #[source]
        source: pplx_space_common_core::ids::InvalidSandboxId,
    },
    #[error("sandbox id {sandbox_id:?} must be a UUID: {source}")]
    SandboxIdNotUuid {
        sandbox_id: String,
        #[source]
        source: pplx_space_common_core::types::SandboxIdParseError,
    },
    #[error("failed to initialize reserved-DNS resolution: {0}")]
    ReservedDns(#[source] BuildReservedDnsCoreError),
    #[error("invalid ServicePort route identity: {message}")]
    RouteIdentity { message: String },
    #[error("failed to bind the net-gateway control socket {path}: {source}")]
    BindControlSocket {
        path: String,
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

/// Why a host-control response did not yield a usable V1 network policy.
#[derive(Debug, thiserror::Error)]
pub enum NetworkPolicyFromHostControlError {
    #[error("host-control returned no V1 policy")]
    MissingV1Policy,
    #[error(transparent)]
    InvalidV1Policy(#[from] pplx_space_net_common::NetworkPolicyProtoError),
    #[error("host-control returned invalid network-credential material: {0}")]
    InvalidNetworkCredentials(#[from] crate::state::InvalidNetworkCredentialSnapshot),
}

/// Why the gateway could not hydrate its policy from host-control. Reported
/// through a sandbox event and, for a refresh, back to the caller as a string,
/// so each variant names the hop that failed.
#[derive(Debug, thiserror::Error)]
pub enum HydrateNetworkStateError {
    #[error("failed to connect to host-control while hydrating policy for {sandbox_id}: {source}")]
    ConnectHostControl {
        sandbox_id: String,
        #[source]
        source: pplx_space_spaced_common::UdsConnectError,
    },
    #[error("failed to pull initial network policy for {sandbox_id} from host-control: {source}")]
    GetNetworkPolicy {
        sandbox_id: String,
        #[source]
        source: tonic::Status,
    },
    #[error(transparent)]
    Policy(#[from] NetworkPolicyFromHostControlError),
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

fn network_policy_from_host_control_proto(
    policy: Option<&NetworkPolicyProto>,
) -> Result<NetworkPolicyV1, NetworkPolicyFromHostControlError> {
    let policy = policy.ok_or(NetworkPolicyFromHostControlError::MissingV1Policy)?;
    Ok(NetworkPolicyV1::try_from(policy)?)
}

struct InitialNetworkState {
    policy: NetworkPolicyV1,
    network_credentials: NetworkCredentialSnapshot,
    policy_ref: String,
    network_tx_bytes: u64,
    network_rx_bytes: u64,
}

/// How long after launch host-control may not know this gateway yet.
/// sandbox-manager registers a gateway's process only once its launch returns,
/// so a pull before then is refused as from an unknown caller. Within the
/// window, and up to [`MAX_PENDING_ATTEMPTS`] pulls, such a refusal is pending
/// registration; after either, a refusal is a failure.
const REGISTRATION_WINDOW: Duration = Duration::from_secs(10);
const MAX_PENDING_ATTEMPTS: u32 = 40;
/// The backoff between pending pulls: doubling from the first to the cap, each
/// wait drawn uniformly below it (full jitter). Every pull costs host-control a
/// connection's work and a database read behind the lock that registration
/// itself takes, so retries must thin out rather than compete with it.
const PENDING_RETRY_INITIAL: Duration = Duration::from_millis(10);
const PENDING_RETRY_CAP: Duration = Duration::from_millis(250);

impl HydrateNetworkStateError {
    /// Host-control refused the caller: it is not the registered gateway of
    /// any sandbox, which is also the answer before registration.
    fn is_unregistered_caller(&self) -> bool {
        matches!(
            self,
            Self::GetNetworkPolicy { source, .. } if source.code() == tonic::Code::PermissionDenied
        )
    }
}

/// When a refused pull is still pending registration, and what ends a wait.
struct RegistrationWait {
    launched_at: tokio::time::Instant,
    /// Set by the owner's first control call, which follows its registration
    /// of this gateway: a pending pull retries at once instead of waiting out
    /// its backoff.
    owner_contacted: tokio::sync::watch::Receiver<bool>,
    /// Draws a wait at or below a backoff bound: [`jittered`] in production.
    jitter: fn(Duration) -> Duration,
}

/// Run `pull`, retrying while host-control does not know this gateway yet,
/// within [`REGISTRATION_WINDOW`] and [`MAX_PENDING_ATTEMPTS`]. The first pull
/// is immediate; later ones back off with jitter, and the owner's first call
/// cuts the current wait short. Only an error the window does not cover is
/// returned.
async fn pull_once_registered<T, F, Fut>(
    mut wait: RegistrationWait,
    mut pull: F,
) -> Result<T, HydrateNetworkStateError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, HydrateNetworkStateError>>,
{
    let mut backoff = PENDING_RETRY_INITIAL;
    let mut attempts = 0_u32;
    loop {
        attempts += 1;
        match pull().await {
            Err(error)
                if error.is_unregistered_caller()
                    && attempts < MAX_PENDING_ATTEMPTS
                    && wait.launched_at.elapsed() < REGISTRATION_WINDOW =>
            {
                tracing::debug!(%error, attempts, "host-control does not know this net-gateway yet; retrying");
                let pause = (wait.jitter)(backoff);
                backoff = backoff.saturating_mul(2).min(PENDING_RETRY_CAP);
                if *wait.owner_contacted.borrow() {
                    tokio::time::sleep(pause).await;
                } else {
                    tokio::select! {
                        () = tokio::time::sleep(pause) => {}
                        _ = wait.owner_contacted.wait_for(|contacted| *contacted) => {}
                    }
                }
            }
            result => return result,
        }
    }
}

fn jittered(bound: Duration) -> Duration {
    use rand::RngExt as _;
    let micros = u64::try_from(bound.as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(rand::rng().random_range(0..=micros))
}

async fn pull_initial_network_state(
    sandbox_id: &str,
    wait: RegistrationWait,
) -> Result<InitialNetworkState, HydrateNetworkStateError> {
    hydrate_network_state(
        sandbox_id,
        wait,
        connect_host_control(sandbox_id),
        |client| pull_network_state(sandbox_id, client),
    )
    .await
}

/// Connect, then pull through [`pull_once_registered`] on that one connection,
/// reporting the outcome as sandbox events: one start, then one completion or
/// one failure. Every pending pull reuses the connection, since each new one
/// would cost host-control a handshake while it is registering this gateway.
async fn hydrate_network_state<C, Connect, F, Fut>(
    sandbox_id: &str,
    wait: RegistrationWait,
    connect: Connect,
    mut pull: F,
) -> Result<InitialNetworkState, HydrateNetworkStateError>
where
    C: Clone,
    Connect: std::future::Future<Output = Result<C, HydrateNetworkStateError>>,
    F: FnMut(C) -> Fut,
    Fut: std::future::Future<Output = Result<InitialNetworkState, HydrateNetworkStateError>>,
{
    let details = || SandboxEventDetails::default().operation("initial_policy_hydration");
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::NetworkPolicyEnforcementStarted,
        SandboxEventSource::NetGateway,
        details(),
    );
    let result = match connect.await {
        Ok(client) => pull_once_registered(wait, || pull(client.clone())).await,
        Err(error) => Err(error),
    };
    match &result {
        Ok(state) => emit_sandbox_event(
            sandbox_id,
            SandboxEventType::NetworkPolicyEnforcementCompleted,
            SandboxEventSource::NetGateway,
            details()
                .installed_network_policy_ref(state.policy_ref.clone())
                .result("policy_hydrated"),
        ),
        Err(error) => emit_sandbox_event(
            sandbox_id,
            SandboxEventType::NetworkPolicyEnforcementFailed,
            SandboxEventSource::NetGateway,
            details().error(error.to_string()),
        ),
    }
    result
}

type HostControl =
    HostControlClient<pplx_space_common_grpc::GrpcClientTrace<tonic::transport::Channel>>;

async fn connect_host_control(sandbox_id: &str) -> Result<HostControl, HydrateNetworkStateError> {
    let socket = std::path::Path::new(
        pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET,
    );
    let channel = pplx_space_spaced_common::connect_uds_channel("sandbox-host-control", socket)
        .await
        .map_err(|source| HydrateNetworkStateError::ConnectHostControl {
            sandbox_id: sandbox_id.to_string(),
            source,
        })?;
    Ok(HostControlClient::new(
        pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
    ))
}

async fn pull_network_state(
    sandbox_id: &str,
    mut client: HostControl,
) -> Result<InitialNetworkState, HydrateNetworkStateError> {
    let response = client
        .get_network_policy(GetNetworkPolicyRequest {})
        .await
        .map_err(|source| HydrateNetworkStateError::GetNetworkPolicy {
            sandbox_id: sandbox_id.to_string(),
            source,
        })?
        .into_inner();
    let policy = network_policy_from_host_control_proto(response.effective_policy.as_ref())?;
    let network_credentials =
        NetworkCredentialSnapshot::from_wire(response.network_credentials, &policy)
            .map_err(NetworkPolicyFromHostControlError::InvalidNetworkCredentials)?;
    Ok(InitialNetworkState {
        policy,
        network_credentials,
        policy_ref: response.effective_policy_ref,
        network_tx_bytes: response.network_tx_bytes,
        network_rx_bytes: response.network_rx_bytes,
    })
}

fn make_policy_loader(
    sandbox_id: String,
    stats: Arc<ProxyStats>,
    owner_contacted: tokio::sync::watch::Receiver<bool>,
) -> PolicyLoader {
    let launched_at = tokio::time::Instant::now();
    Arc::new(move || {
        let sandbox_id = sandbox_id.clone();
        let stats = stats.clone();
        let wait = RegistrationWait {
            launched_at,
            owner_contacted: owner_contacted.clone(),
            jitter: jittered,
        };
        Box::pin(async move {
            let pulled = pull_initial_network_state(&sandbox_id, wait)
                .await
                .map_err(|error| error.to_string())?;
            stats.merge_persisted_network_bytes(pulled.network_tx_bytes, pulled.network_rx_bytes);
            Ok(PulledNetworkPolicy {
                policy: pulled.policy,
                policy_ref: pulled.policy_ref,
                network_credentials: pulled.network_credentials,
            })
        })
    })
}

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

fn route_identity_from_config(
    config: &NetGatewayRunConfig,
) -> Result<Option<pplx_space_common_core::types::SpaceApiRouteIdentity>, RunNetGatewayError> {
    if config.route_generation.is_none()
        && config.current_node_id.is_none()
        && config.execution_generation.is_none()
        && config.pod_uid.is_none()
        && config.activation_attempt_id.is_none()
        && config.substrate_kind.is_none()
    {
        return Ok(None);
    }
    route_identity_from_proto(Some(crate::proto::SpaceApiRouteIdentity {
        sandbox_id: Some(config.sandbox_id.clone()),
        route_generation: config.route_generation,
        current_node_id: config.current_node_id.clone(),
        substrate_kind: match config.substrate_kind {
            None => crate::proto::SandboxSubstrateKind::Unspecified as i32,
            Some(pplx_space_common_core::types::NodeManagedBy::Space) => {
                crate::proto::SandboxSubstrateKind::Direct as i32
            }
            Some(pplx_space_common_core::types::NodeManagedBy::Kubernetes) => {
                crate::proto::SandboxSubstrateKind::Kubernetes as i32
            }
        },
        execution_generation: config
            .execution_generation
            .map(i64::try_from)
            .transpose()
            .map_err(|error| RunNetGatewayError::RouteIdentity {
                message: error.to_string(),
            })?,
        pod_uid: config.pod_uid.clone(),
        activation_attempt_id: config.activation_attempt_id.clone(),
    }))
    .map_err(status_for_route_identity_parse)
    .map_err(|status| RunNetGatewayError::RouteIdentity {
        message: status.message().to_string(),
    })
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
    let short_id =
        pplx_space_common_core::ids::sandbox_label(&config.sandbox_id).map_err(|source| {
            RunNetGatewayError::SandboxLabel {
                sandbox_id: config.sandbox_id.clone(),
                source,
            }
        })?;
    // Net-gateway is a per-sandbox process: the id is set once from the run
    // config and is valid until process exit. Leak it so spawned proxy tasks
    // can carry it as a Copy `&'static str` log field without any per-spawn
    // cloning or refcount machinery.
    let sandbox_id: &'static str = config.sandbox_id.clone().leak();

    tracing::info!(
        sandbox_id = %config.sandbox_id,
        no_identity = config.no_identity,
        "starting net gateway"
    );

    let stats = Arc::new(ProxyStats::new());
    let reserved_dns = ReservedDnsCore::new().map_err(RunNetGatewayError::ReservedDns)?;
    let reserved_dns_cancellation = ReservedDnsCancellation::default();
    // Built before the network state so policy installation can reconcile
    // them on every successful policy install (RPC refresh and self-hydration
    // alike), not just when the control-plane RPC drives the refresh.
    let dns_allow_cache = DnsAllowCache::default();
    let dns_attributed_connections = DnsAttributedTcpTracker::default();
    let network_state = if config.no_identity {
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
        GatewayNetworkState::no_identity()
    } else {
        GatewayNetworkState::pending_policy()
    }
    .with_egress(
        dns_allow_cache.clone(),
        dns_attributed_connections.clone(),
        reserved_dns_cancellation.clone(),
    )
    .with_readiness_metrics();
    let policy_loader = make_policy_loader(
        config.sandbox_id.clone(),
        stats.clone(),
        network_state.owner_contact(),
    );
    let trusted_sandbox_id =
        config
            .sandbox_id
            .parse()
            .map_err(|source| RunNetGatewayError::SandboxIdNotUuid {
                sandbox_id: config.sandbox_id.clone(),
                source,
            })?;
    let service_port_ingress = ServicePortIngressState::new_for_sandbox(
        trusted_sandbox_id,
        route_identity_from_config(&config)?,
    );
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
        std::path::Path::new(&config.socket_path),
        pplx_space_common_svc::UnixSocketOptions::new(0o770),
    )
    .map_err(|source| RunNetGatewayError::BindControlSocket {
        path: config.socket_path.clone(),
        source,
    })?;
    let hydration_task = (!config.no_identity).then(|| {
        network_state.spawn_hydration(policy_loader.clone(), proxy_shutdown_token.child_token())
    });
    let dns_cache_shutdown_token = proxy_shutdown_token.child_token();
    let dns_cache = dns_allow_cache.clone();
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
    let tcp_dns_allow_cache = dns_allow_cache.clone();
    let tcp_reserved_dns = reserved_dns.clone();
    let tcp_reserved_dns_cancellation = reserved_dns_cancellation.clone();
    let tcp_debug_state = debug_state.clone();
    let tcp_known_egress_state = known_egress_state.clone();
    let tcp_shutdown_token = proxy_shutdown_token.child_token();
    let mut tcp_task = tokio::spawn(async move {
        let result = tcp_proxy::run(tcp_proxy::TcpProxyConfig {
            sandbox_id,
            listener: tcp_listener,
            stats: tcp_stats,
            network_state: tcp_network_state,
            dns_allow_cache: tcp_dns_allow_cache,
            dns_attributed_connections,
            tls_interception,
            reserved_dns: tcp_reserved_dns,
            reserved_dns_cancellation: tcp_reserved_dns_cancellation,
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
    let udp_dns_allow_cache = dns_allow_cache.clone();
    let udp_known_egress_state = known_egress_state.clone();
    let udp_reserved_dns = reserved_dns;
    let udp_reserved_dns_cancellation = reserved_dns_cancellation;
    let udp_shutdown_token = proxy_shutdown_token.child_token();
    let mut udp_task = tokio::spawn(async move {
        let result = Box::pin(udp_proxy::run(udp_proxy::UdpProxyConfig {
            sandbox_id,
            listener: udp_listener,
            stats: udp_stats,
            network_state: udp_network_state,
            dns_allow_cache: udp_dns_allow_cache,
            known_egress_state: udp_known_egress_state,
            reserved_dns: udp_reserved_dns,
            reserved_dns_cancellation: udp_reserved_dns_cancellation,
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
        socket_path = %config.socket_path,
        "net gateway listening"
    );
    let control_ready_snapshot = network_state.snapshot().await;
    emit_sandbox_event(
        &config.sandbox_id,
        SandboxEventType::NetworkPolicyEnforcementCompleted,
        SandboxEventSource::NetGateway,
        SandboxEventDetails::default()
            .operation("control_socket_ready")
            .network_state(control_ready_snapshot.mode.to_string())
            .installed_network_policy_ref(control_ready_snapshot.installed_policy_ref)
            .result("ready_for_refresh"),
    );

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
        &config.sandbox_id,
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
