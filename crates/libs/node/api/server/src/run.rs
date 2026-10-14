//! The node-api service body: role resolution, node identity, the local health
//! and sandbox-manager capability gates, the listener, and the per-role server
//! assembly. This is what the `space-node-api-svc` binary's `main` used to
//! hold; the binary now only parses flags and calls [`run`].

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::auth;
use crate::builder::BuilderApiService;
use crate::clients::{BackendClientError, BackendClients};
use crate::drain::NodeApiDrainLayer;
use crate::node_info::NodeInfoService;
use crate::worker::WorkerApiService;
use pplx_space_common_core::types::{K8sNodeNameParseError, NodeId, NodeRole as SpaceNodeRole};
use pplx_space_common_svc::bearer_token::SPACE_INTERNAL_TOKEN_ENV;
use pplx_space_common_svc::types::k8s::space_node_id;
use pplx_space_node_api_client::proto::NodeRole;
use pplx_space_node_api_client::proto::node_info_server::NodeInfoServer;
use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternalServer;
use pplx_space_node_api_client::proto::worker_api_server::WorkerApiServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;
use tonic_health::ServingStatus as ReporterServingStatus;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as HealthStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::server::HealthReporter;

// WorkerApiInternal is served on the same listener as WorkerApi because the
// API substrate reaches it remotely on worker pods. Keep it off the normal
// worker auth surface with a distinct bearer token.
const SPACE_INTERNAL_CONTROL_TOKEN_ENV: &str = "SPACE_INTERNAL_CONTROL_TOKEN";
const SPACE_NODE_ID_ENV: &str = "SPACE_NODE_ID";
const NODE_ID_PATH: &str = "/space/etc/node-id";
/// The service name node-api reports to telemetry and names its local
/// health surface with.
pub const SERVICE_NAME: &str = "space-node-api";
const READINESS_SERVICE: &str = "readiness";
const LIVENESS_SERVICE: &str = "liveness";
/// Default ceiling on the post-SIGTERM drain, exposed so the binary's
/// `--shutdown-grace-secs` default lives next to the code that spends it.
pub const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 150;
type LocalHealthServer = (HealthReporter, JoinHandle<()>);

const SANDBOX_MANAGER_CAPABILITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(12);
const SANDBOX_MANAGER_CAPABILITY_INITIAL_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_secs(1);
const SANDBOX_MANAGER_CAPABILITY_MAX_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_secs(15);
#[cfg(not(test))]
const SANDBOX_MANAGER_CAPABILITY_REFRESH_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5);
#[cfg(test)]
const SANDBOX_MANAGER_CAPABILITY_REFRESH_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(25);

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. Environment-driven configuration (bearer tokens,
/// Kubernetes token auth, backend socket paths) stays with the code that reads
/// it.
#[derive(Debug, Clone)]
pub struct NodeApiRunConfig {
    /// TCP address to listen on.
    pub listen: String,
    /// Node role: determines which API surface is exposed. `worker` or
    /// `builder`; the cluster surface is `space-internal-api-svc`.
    pub role: String,
    /// Instance-local health Unix socket for Kubernetes probes.
    pub health_socket: Option<PathBuf>,
    /// Maximum time to wait for in-flight gRPC requests after SIGTERM.
    pub shutdown_grace: std::time::Duration,
}

/// Why [`run`] returned without a clean shutdown.
#[derive(Debug, thiserror::Error)]
pub enum RunNodeApiError {
    #[error(transparent)]
    InternalTransport(#[from] pplx_space_common_grpc::internal_transport::TransportError),
    #[error(
        "unknown role '{role}' -- valid roles: worker, builder (cluster is now space-internal-api-svc)"
    )]
    UnsupportedRole { role: String },
    /// The static `SPACE_INTERNAL_TOKEN` boot precondition was not met.
    #[error("{message}")]
    BearerToken { message: String },
    #[error(
        "failed to read {SPACE_INTERNAL_CONTROL_TOKEN_ENV} for worker role to protect WorkerApiInternal: {0}"
    )]
    InternalControlToken(#[source] std::env::VarError),
    #[error(transparent)]
    NodeId(#[from] NodeIdError),
    #[error("invalid builder node ID '{node_id}': {source}")]
    BuilderNodeId {
        node_id: String,
        #[source]
        source: K8sNodeNameParseError,
    },
    #[error(transparent)]
    BindListener(#[from] BindListenerError),
    #[error("failed to bind the node-api health socket: {0}")]
    HealthSocket(#[source] pplx_space_common_svc::bootstrap::BindUnixSocketError),
    #[error("failed to connect node-api backends for {role} role: {source}")]
    BackendClients {
        role: String,
        #[source]
        source: BackendClientError,
    },
    #[error("failed to configure node-api auth: {0}")]
    Auth(#[from] auth::AuthConfigError),
    #[error("failed to configure builder submission auth: {0}")]
    BuildAuth(#[from] auth::build::BuildAuthError),
    #[error("failed to build the node-api reflection service: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),
    #[error("node-api server exited with an error: {0}")]
    Serve(#[from] tonic::transport::Error),
}

/// Why node-api could not resolve or persist the node identity it serves under.
#[derive(Debug, thiserror::Error)]
pub enum NodeIdError {
    #[error("failed to create node-id directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write temporary node-id {path}: {source}")]
    WriteTemporary {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to replace node-id at {path}: {source}")]
    Replace {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "failed to write node-id at {path} after atomic write failed ({atomic_error}): {source}"
    )]
    WriteAfterAtomicFailure {
        path: PathBuf,
        atomic_error: Box<NodeIdError>,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read node-id {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid node-id in {path}: {source}")]
    Invalid {
        path: PathBuf,
        #[source]
        source: uuid::Error,
    },
}

/// Why the node-api TCP listener could not be bound.
#[derive(Debug, thiserror::Error)]
pub enum BindListenerError {
    #[error("invalid node-api listen address '{listen}': {source}")]
    InvalidAddress {
        listen: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("failed to create node-api TCP socket for {addr}: {source}")]
    CreateSocket {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set {option} on the node-api listener for {addr}: {source}")]
    SetOption {
        option: &'static str,
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind node-api listener at {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to listen on node-api socket at {addr}: {source}")]
    Listen {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to register the node-api listener with the Tokio reactor: {source}")]
    Register {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

/// Why a sandbox-manager capability probe produced no verdict. Never fatal:
/// startup retries until the capability appears, and the receipt bridge
/// withholds node-api's own capability while a probe is failing.
#[derive(Debug, thiserror::Error)]
enum CapabilityProbeError {
    #[error("failed to connect to sandbox-manager while checking {capability}: {source}")]
    Connect {
        capability: &'static str,
        #[source]
        source: pplx_space_common_grpc::UdsConnectError,
    },
    #[error("sandbox-manager health check for {capability} failed: {source}")]
    Check {
        capability: &'static str,
        #[source]
        source: tonic::Status,
    },
    #[error("timed out checking sandbox-manager capability {capability}")]
    Timeout { capability: &'static str },
    #[error("sandbox-manager capability {capability} is not serving")]
    NotServing { capability: &'static str },
}

/// The roles node-api serves, spelled as `--role` accepts them. `cluster` is
/// deliberately absent: that surface is `space-internal-api-svc` now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NodeApiRole {
    Worker,
    Builder,
}

impl NodeApiRole {
    fn parse(value: &str) -> Result<Self, RunNodeApiError> {
        match value {
            "worker" => Ok(Self::Worker),
            "builder" => Ok(Self::Builder),
            other => Err(RunNodeApiError::UnsupportedRole {
                role: other.to_string(),
            }),
        }
    }

    fn auth_role(self) -> auth::NodeRole {
        match self {
            Self::Worker => auth::NodeRole::Worker,
            Self::Builder => auth::NodeRole::Builder,
        }
    }
}

fn sandbox_manager_capability_retry_delay(retry_index: usize) -> std::time::Duration {
    let multiplier = 1_u32.checked_shl(retry_index as u32).unwrap_or(u32::MAX);
    SANDBOX_MANAGER_CAPABILITY_INITIAL_RETRY_DELAY
        .saturating_mul(multiplier)
        .min(SANDBOX_MANAGER_CAPABILITY_MAX_RETRY_DELAY)
}

async fn backend_clients(role: &str) -> Result<BackendClients, RunNodeApiError> {
    tracing::info!(
        role,
        "connecting backend clients (sandbox-manager, node-manager, template-manager)"
    );
    BackendClients::connect()
        .await
        .map_err(|source| RunNodeApiError::BackendClients {
            role: role.to_string(),
            source,
        })
}

const REQUIRED_SANDBOX_MANAGER_CAPABILITIES: [&str; 1] =
    [pplx_space_sbx_mgr_client::PROCESS_OUTPUT_CURSOR_HEALTH_SERVICE];

async fn sandbox_manager_capability_serving(
    socket_path: &Path,
    capability: &'static str,
) -> Result<bool, CapabilityProbeError> {
    tokio::time::timeout(SANDBOX_MANAGER_CAPABILITY_TIMEOUT, async {
        let channel = pplx_space_common_grpc::connect_uds_channel_with_retry(
            "sandbox-manager",
            socket_path,
            pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY,
        )
        .await
        .map_err(|source| CapabilityProbeError::Connect { capability, source })?;
        HealthClient::new(pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(
            channel,
        ))
        .check(HealthCheckRequest {
            service: capability.to_string(),
        })
        .await
        .map(|response| response.into_inner().status == HealthStatus::Serving as i32)
        .or_else(|status| {
            if matches!(
                status.code(),
                tonic::Code::NotFound | tonic::Code::Unimplemented
            ) {
                Ok(false)
            } else {
                Err(CapabilityProbeError::Check {
                    capability,
                    source: status,
                })
            }
        })
    })
    .await
    .map_err(|_| CapabilityProbeError::Timeout { capability })?
}

async fn check_sandbox_manager_capabilities(
    socket_path: &Path,
) -> Result<(), CapabilityProbeError> {
    for capability in REQUIRED_SANDBOX_MANAGER_CAPABILITIES {
        if !sandbox_manager_capability_serving(socket_path, capability).await? {
            return Err(CapabilityProbeError::NotServing { capability });
        }
        tracing::info!(
            capability,
            "verified sandbox-manager capability before binding node-api"
        );
    }
    Ok(())
}

fn spawn_sandbox_manager_capability_bridge(
    capability: &'static str,
    health_reporter: HealthReporter,
    sandbox_manager_socket: PathBuf,
    shutdown_token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_serving = None;
        loop {
            let serving = tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => break,
                result = sandbox_manager_capability_serving(&sandbox_manager_socket, capability) => {
                    match result {
                        Ok(serving) => serving,
                        Err(error) => {
                            tracing::warn!(
                                %error,
                                capability,
                                "sandbox-manager capability probe failed; withholding node-api capability"
                            );
                            false
                        }
                    }
                }
            };
            if last_serving != Some(serving) {
                health_reporter
                    .set_service_status(
                        capability,
                        if serving {
                            ReporterServingStatus::Serving
                        } else {
                            ReporterServingStatus::NotServing
                        },
                    )
                    .await;
                tracing::info!(
                    capability,
                    serving,
                    "updated node-api capability from sandbox-manager"
                );
                last_serving = Some(serving);
            }
            tokio::select! {
                biased;
                _ = shutdown_token.cancelled() => break,
                _ = tokio::time::sleep(SANDBOX_MANAGER_CAPABILITY_REFRESH_INTERVAL) => {}
            }
        }
    })
}

async fn wait_for_sandbox_manager_capabilities(socket_path: &Path) {
    let mut retry_index = 0;
    loop {
        match check_sandbox_manager_capabilities(socket_path).await {
            Ok(()) => return,
            Err(error) => {
                let retry_delay = sandbox_manager_capability_retry_delay(retry_index);
                tracing::warn!(
                    %error,
                    capabilities = ?REQUIRED_SANDBOX_MANAGER_CAPABILITIES,
                    retry_in = ?retry_delay,
                    "sandbox-manager capability is unavailable; node-api remains not ready"
                );
                tokio::time::sleep(retry_delay).await;
                retry_index = retry_index.saturating_add(1);
            }
        }
    }
}

fn role_requires_sandbox_manager_capabilities(role: auth::NodeRole) -> bool {
    role == auth::NodeRole::Worker
}

fn write_node_id_atomically(path: &Path, node_id: NodeId) -> Result<(), NodeIdError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| NodeIdError::CreateDirectory {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let temp_path = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temp_path, format!("{node_id}\n")).map_err(|source| {
        NodeIdError::WriteTemporary {
            path: temp_path.clone(),
            source,
        }
    })?;
    std::fs::rename(&temp_path, path).map_err(|source| NodeIdError::Replace {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn write_node_id(path: &Path, node_id: NodeId) -> Result<(), NodeIdError> {
    match write_node_id_atomically(path, node_id) {
        Ok(()) => Ok(()),
        Err(atomic_error) if path.exists() => {
            std::fs::write(path, format!("{node_id}\n")).map_err(|source| {
                NodeIdError::WriteAfterAtomicFailure {
                    path: path.to_path_buf(),
                    atomic_error: Box::new(atomic_error),
                    source,
                }
            })
        }
        Err(error) => Err(error),
    }
}

fn read_node_id(path: &Path) -> Result<Option<uuid::Uuid>, NodeIdError> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(NodeIdError::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    let content = content.trim();
    if content.is_empty() {
        return Ok(None);
    }

    uuid::Uuid::parse_str(content)
        .map(Some)
        .map_err(|source| NodeIdError::Invalid {
            path: path.to_path_buf(),
            source,
        })
}

fn resolve_node_id_with_env(
    path: &Path,
    env_node_id: Option<String>,
) -> Result<(String, String), NodeIdError> {
    if let Some(node_id) = env_node_id
        .map(|node_id| node_id.trim().to_string())
        .filter(|node_id| !node_id.is_empty())
    {
        return Ok((node_id, SPACE_NODE_ID_ENV.to_string()));
    }

    if let Some(node_id) = read_node_id(path)? {
        return Ok((node_id.to_string(), path.display().to_string()));
    }

    let node_id = NodeId::new();
    write_node_id(path, node_id)?;
    Ok((node_id.to_string(), path.display().to_string()))
}

fn resolve_node_id(path: &Path) -> Result<(String, String), NodeIdError> {
    resolve_node_id_with_env(path, std::env::var(SPACE_NODE_ID_ENV).ok())
}

fn builder_node_id(value: &str) -> Result<NodeId, RunNodeApiError> {
    space_node_id(value, SpaceNodeRole::Builder).map_err(|source| RunNodeApiError::BuilderNodeId {
        node_id: value.to_string(),
        source,
    })
}

fn bind_node_api_listener(listen: &str) -> Result<TcpListener, BindListenerError> {
    let addr: SocketAddr = listen
        .parse()
        .map_err(|source| BindListenerError::InvalidAddress {
            listen: listen.to_string(),
            source,
        })?;
    bind_node_api_listener_addr(addr)
}

fn bind_node_api_listener_addr(addr: SocketAddr) -> Result<TcpListener, BindListenerError> {
    let std_listener = bind_node_api_std_listener(addr)?;
    TcpListener::from_std(std_listener)
        .map_err(|source| BindListenerError::Register { addr, source })
}

#[cfg(target_os = "linux")]
fn bind_node_api_std_listener(
    addr: SocketAddr,
) -> Result<std::net::TcpListener, BindListenerError> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
        .map_err(|source| BindListenerError::CreateSocket { addr, source })?;
    socket
        .set_reuse_address(true)
        .map_err(|source| BindListenerError::SetOption {
            option: "SO_REUSEADDR",
            addr,
            source,
        })?;
    socket
        .set_reuse_port(true)
        .map_err(|source| BindListenerError::SetOption {
            option: "SO_REUSEPORT",
            addr,
            source,
        })?;
    socket
        .set_nonblocking(true)
        .map_err(|source| BindListenerError::SetOption {
            option: "O_NONBLOCK",
            addr,
            source,
        })?;
    socket
        .bind(&addr.into())
        .map_err(|source| BindListenerError::Bind { addr, source })?;
    socket
        .listen(1024)
        .map_err(|source| BindListenerError::Listen { addr, source })?;
    Ok(socket.into())
}

#[cfg(not(target_os = "linux"))]
fn bind_node_api_std_listener(
    addr: SocketAddr,
) -> Result<std::net::TcpListener, BindListenerError> {
    let listener = std::net::TcpListener::bind(addr)
        .map_err(|source| BindListenerError::Bind { addr, source })?;
    listener
        .set_nonblocking(true)
        .map_err(|source| BindListenerError::SetOption {
            option: "O_NONBLOCK",
            addr,
            source,
        })?;
    Ok(listener)
}

async fn spawn_local_health(
    health_socket: Option<&Path>,
    shutdown_token: CancellationToken,
) -> Result<Option<LocalHealthServer>, RunNodeApiError> {
    let Some(health_socket) = health_socket else {
        return Ok(None);
    };

    let listener = pplx_space_common_svc::bind_unix_socket(
        health_socket,
        pplx_space_common_svc::UnixSocketOptions::new(0o660),
    )
    .map_err(RunNodeApiError::HealthSocket)?;
    let (health_reporter, health_service) = pplx_space_common_svc::grpc_health_service();
    health_reporter
        .set_service_status("", ReporterServingStatus::NotServing)
        .await;
    health_reporter
        .set_service_status(READINESS_SERVICE, ReporterServingStatus::NotServing)
        .await;
    health_reporter
        .set_service_status(LIVENESS_SERVICE, ReporterServingStatus::Serving)
        .await;
    let handle = tokio::spawn(async move {
        let result = pplx_space_common_svc::grpc_server_builder()
            .layer(pplx_space_common_svc::grpc_trace_layer())
            .add_service(health_service)
            .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                shutdown_token.cancelled().await
            })
            .await;
        if let Err(error) = result {
            tracing::error!(%error, "node-api local health server exited with error");
        }
    });

    Ok(Some((health_reporter, handle)))
}

async fn set_local_readiness(
    health_server: Option<&LocalHealthServer>,
    status: ReporterServingStatus,
) {
    if let Some((health_reporter, _)) = health_server {
        health_reporter.set_service_status("", status).await;
        health_reporter
            .set_service_status(READINESS_SERVICE, status)
            .await;
    }
}

fn mark_local_readiness_not_serving_on_shutdown(
    health_server: Option<&LocalHealthServer>,
    shutdown_token: CancellationToken,
) {
    let Some((health_reporter, _)) = health_server else {
        return;
    };

    let health_reporter = health_reporter.clone();
    tokio::spawn(async move {
        shutdown_token.cancelled().await;
        health_reporter
            .set_service_status("", ReporterServingStatus::NotServing)
            .await;
        health_reporter
            .set_service_status(READINESS_SERVICE, ReporterServingStatus::NotServing)
            .await;
    });
}

async fn stop_local_health(health_server: Option<LocalHealthServer>) {
    if let Some((_health_reporter, handle)) = health_server
        && let Err(error) = handle.await
    {
        tracing::warn!(%error, "node-api local health server task ended unexpectedly");
    }
}

enum StartupPrelude {
    Started {
        health_server: Option<LocalHealthServer>,
    },
    Shutdown,
}

async fn start_local_health_and_await_capabilities(
    role: auth::NodeRole,
    health_socket: Option<&Path>,
    sandbox_manager_socket: &Path,
    shutdown_token: CancellationToken,
) -> Result<StartupPrelude, RunNodeApiError> {
    let health_server = spawn_local_health(health_socket, shutdown_token.clone()).await?;
    mark_local_readiness_not_serving_on_shutdown(health_server.as_ref(), shutdown_token.clone());

    if role_requires_sandbox_manager_capabilities(role) {
        tokio::select! {
            biased;
            _ = shutdown_token.cancelled() => {
                stop_local_health(health_server).await;
                return Ok(StartupPrelude::Shutdown);
            }
            () = wait_for_sandbox_manager_capabilities(sandbox_manager_socket) => {}
        }
    }

    Ok(StartupPrelude::Started { health_server })
}

/// Mirror `shutdown_token` onto the token the service itself runs off, raising
/// `drain_signal` before cancelling it.
///
/// Ordering is the point: the drain layer starts rejecting node-api RPCs and
/// the local readiness gate flips to NOT_SERVING the instant the flag is set,
/// and both have to be true *before* anything begins its drain. The caller's
/// token only says that shutdown started. The task also exits when the service
/// token is cancelled directly, which is how [`run`] stops it on the way out.
fn drain_before_shutdown(
    drain_signal: pplx_space_common_svc::DrainSignal,
    shutdown_token: CancellationToken,
) -> CancellationToken {
    let service_token = CancellationToken::new();
    tokio::spawn({
        let service_token = service_token.clone();
        async move {
            tokio::select! {
                biased;
                () = shutdown_token.cancelled() => {
                    drain_signal.begin();
                    service_token.cancel();
                }
                () = service_token.cancelled() => {}
            }
        }
    });
    service_token
}

/// Run node-api until `shutdown_token` fires or a fatal condition ends it.
/// Telemetry must already be initialized by the caller.
pub async fn run(
    config: NodeApiRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunNodeApiError> {
    let role = NodeApiRole::parse(&config.role)?;
    let auth_role = role.auth_role();

    let (node_id, node_id_source) = resolve_node_id(Path::new(NODE_ID_PATH))?;
    tracing::info!(
        node_id = %node_id,
        source = %node_id_source,
        "resolved node identity"
    );

    let token = auth::required_bearer_token(SPACE_INTERNAL_TOKEN_ENV).map_err(|status| {
        RunNodeApiError::BearerToken {
            message: status.message().to_string(),
        }
    })?;

    // WorkerApiInternal carries start-context recovery inputs and must remain
    // isolated from the normal worker surface even when both services share the
    // same listener. Do not fall back to SPACE_INTERNAL_TOKEN here.
    let internal_control_token = match role {
        NodeApiRole::Worker => Some(
            std::env::var(SPACE_INTERNAL_CONTROL_TOKEN_ENV)
                .map_err(RunNodeApiError::InternalControlToken)?,
        ),
        NodeApiRole::Builder => None,
    };

    let drain_signal = pplx_space_common_svc::DrainSignal::new();
    let shutdown_token = drain_before_shutdown(drain_signal.clone(), shutdown_token);
    let drain_timeout = config.shutdown_grace;
    let health_server = match start_local_health_and_await_capabilities(
        auth_role,
        config.health_socket.as_deref(),
        Path::new(pplx_space_sbx_mgr_client::SOCKET_PATH),
        shutdown_token.clone(),
    )
    .await?
    {
        StartupPrelude::Started { health_server } => health_server,
        StartupPrelude::Shutdown => return Ok(()),
    };
    let listener = bind_node_api_listener(&config.listen)?;
    tracing::info!(
        listen = %config.listen,
        role = %config.role,
        "space-node-api-svc listening"
    );

    let run_result = async {
        let auth_config = auth::NodeApiAuthConfig::from_env(
            auth_role,
            token.clone(),
            internal_control_token.clone(),
        )?;
        let auth_layer = auth::NodeApiAuthLayer::from_config(auth_config).await?;
        let mut builder = pplx_space_common_grpc::internal_transport::server_builder()?
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_mins(1)))
            // Trace first (outermost) so denied requests still produce
            // server spans; auth runs after tracing but before any routed
            // service. The drain gate runs after auth and before WorkerApi /
            // BuilderApi dispatch, so rejected drain-time requests never reach
            // node-local backends.
            .layer(pplx_space_common_svc::grpc_trace_layer())
            .layer(auth_layer)
            .layer(NodeApiDrainLayer::new(drain_signal.clone()));

        match role {
            NodeApiRole::Worker => {
                let clients = backend_clients(&config.role).await?;
                let reflection = tonic_reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(
                        pplx_space_node_api_client::proto::WORKER_FILE_DESCRIPTOR_SET,
                    )
                    .build_v1()?;
                let (health_reporter, health_service) =
                    pplx_space_common_svc::grpc_health_service();
                health_reporter
                    .set_service_status(
                        pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                        ReporterServingStatus::NotServing,
                    )
                    .await;
                let activation_policy_capability_task = spawn_sandbox_manager_capability_bridge(
                    pplx_space_sbx_mgr_client::ACTIVATION_POLICY_HEALTH_SERVICE,
                    health_reporter.clone(),
                    PathBuf::from(pplx_space_sbx_mgr_client::SOCKET_PATH),
                    shutdown_token.clone(),
                );
                let operation_receipt_capability_task = spawn_sandbox_manager_capability_bridge(
                    pplx_space_sbx_mgr_client::OPERATION_ACTIVATION_RECEIPT_HEALTH_SERVICE,
                    health_reporter.clone(),
                    PathBuf::from(pplx_space_sbx_mgr_client::SOCKET_PATH),
                    shutdown_token.clone(),
                );
                let encrypted_pool_capability_task = spawn_sandbox_manager_capability_bridge(
                    pplx_space_sbx_mgr_client::ENCRYPTED_POOL_ADOPTION_HEALTH_SERVICE,
                    health_reporter,
                    PathBuf::from(pplx_space_sbx_mgr_client::SOCKET_PATH),
                    shutdown_token.clone(),
                );
                let resource_status_reader = crate::NodeResourceStatusReader::new();
                let svc = WorkerApiService::with_resource_status_reader_and_shutdown_token(
                    clients.clone(),
                    resource_status_reader.clone(),
                    shutdown_token.clone(),
                );
                let internal_svc = WorkerApiService::with_resource_status_reader_and_shutdown_token(
                    clients.clone(),
                    resource_status_reader.clone(),
                    shutdown_token.clone(),
                );
                let node_info = NodeInfoService::with_resource_status_reader(
                    NodeRole::Worker,
                    node_id.clone(),
                    Some(clients),
                    resource_status_reader,
                );
                let server = builder
                    .add_service(health_service)
                    .add_service(reflection)
                    .add_service(WorkerApiServer::new(svc))
                    .add_service(WorkerApiInternalServer::new(internal_svc))
                    .add_service(NodeInfoServer::new(node_info));
                set_local_readiness(health_server.as_ref(), ReporterServingStatus::Serving).await;
                let serve_result = server.serve_with_incoming_shutdown(
                    pplx_space_common_grpc::internal_transport::Incoming::new(
                        TcpIncoming::from(listener).with_nodelay(Some(true)),
                        shutdown_token.clone(),
                    ),
                    std::future::pending(),
                );
                let serve_result = pplx_space_common_svc::drain_server(
                    SERVICE_NAME,
                    &shutdown_token,
                    drain_timeout,
                    serve_result,
                )
                .await;
                set_local_readiness(health_server.as_ref(), ReporterServingStatus::NotServing)
                    .await;
                shutdown_token.cancel();
                let _ = operation_receipt_capability_task.await;
                if let Err(error) = encrypted_pool_capability_task.await {
                    tracing::warn!(%error, "encrypted pool capability bridge failed");
                }
                if let Err(error) = activation_policy_capability_task.await {
                    tracing::warn!(%error, "activation policy capability bridge failed");
                }
                stop_local_health(health_server).await;
                if let Some(result) = serve_result {
                    result?;
                }
            }
            NodeApiRole::Builder => {
                let clients = backend_clients(&config.role).await?;
                let reflection = tonic_reflection::server::Builder::configure()
                    .register_encoded_file_descriptor_set(
                        pplx_space_node_api_client::proto::BUILDER_FILE_DESCRIPTOR_SET,
                    )
                    .build_v1()?;
                let (_, health_service) = pplx_space_common_svc::grpc_health_service();
                // One budget is shared by all clones of the routed service.
                // Additional builder listeners must reuse this same pool.
                let upload_slots = std::sync::Arc::new(tokio::sync::Semaphore::new(32));
                let svc = BuilderApiService::new(clients.clone(), builder_node_id(&node_id)?)
                    .into_grpc_service(upload_slots);
                let node_info = NodeInfoService::with_resource_status_reader(
                    NodeRole::Builder,
                    node_id.clone(),
                    Some(clients),
                    crate::NodeResourceStatusReader::new(),
                );
                let build_auth = auth::build::BuildAuthLayer::for_node_api(token.clone()).await?;
                let server = builder
                    .layer(build_auth)
                    .add_service(health_service)
                    .add_service(reflection)
                    .add_service(svc)
                    .add_service(NodeInfoServer::new(node_info));
                set_local_readiness(health_server.as_ref(), ReporterServingStatus::Serving).await;
                let serve_result = server.serve_with_incoming_shutdown(
                    pplx_space_common_grpc::internal_transport::Incoming::new(
                        TcpIncoming::from(listener).with_nodelay(Some(true)),
                        shutdown_token.clone(),
                    ),
                    std::future::pending(),
                );
                let serve_result = pplx_space_common_svc::drain_server(
                    SERVICE_NAME,
                    &shutdown_token,
                    drain_timeout,
                    serve_result,
                )
                .await;
                set_local_readiness(health_server.as_ref(), ReporterServingStatus::NotServing)
                    .await;
                shutdown_token.cancel();
                stop_local_health(health_server).await;
                if let Some(result) = serve_result {
                    result?;
                }
            }
        }

        Ok::<_, RunNodeApiError>(())
    }
    .await;

    shutdown_token.cancel();
    run_result?;

    Ok(())
}
#[cfg(test)]
mod tests;
