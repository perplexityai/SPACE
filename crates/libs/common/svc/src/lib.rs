//! Shared service-side helpers for SPACE daemons.
//!
//! This crate sits one layer above `pplx-space-common-core`: it re-exports the
//! core crate and adds service boundary helpers for auth context, endpoints,
//! transport errors, Kubernetes-safe labels, and Unix socket paths.
//!
//! ```
//! # use pplx_space_common_svc::types::ServiceName;
//! let service = ServiceName::parse("space-api").unwrap();
//! assert_eq!(service.as_str(), "space-api");
//! ```

pub use pplx_space_common_core as core;
pub use pplx_space_common_core::sql;
pub use pplx_space_observability::{self as observability, LogContext, ObservabilityError};

pub mod auth;
pub use pplx_space_bearer_token as bearer_token;
pub mod bootstrap;
pub use pplx_space_dependency_drain as dependency_drain;
pub mod disk_admission;
pub use pplx_space_ecw_credential as ecw_credential;
pub use pplx_space_federated_identity as federated_identity;
pub use pplx_space_http_server::http_listener;
#[cfg(feature = "k8s")]
pub use pplx_space_kube_client as kube_client;
pub use pplx_space_service_lifecycle as lifecycle;
pub mod local_health;
pub use pplx_space_api_route as space_api_route;
pub use pplx_space_api_route::capability as space_api_route_capability;
pub use pplx_space_api_route::ed25519 as space_api_route_ed25519;
pub use pplx_space_grpc_rate_limiter as rate_limiter;
pub use pplx_space_page_cache as page_cache;
pub use pplx_space_service_mutex as service_mutex;
pub use pplx_space_uds_peer_auth as peer_auth;
pub use pplx_space_valkey::redis;
pub use pplx_space_valkey::shared_valkey;
pub mod sql;
pub mod template_lookup;
pub use pplx_space_systemd_watchdog as systemd;
pub use pplx_space_tokio_runtime as tokio_runtime;
pub mod types;

pub use dependency_drain::{
    DependencyAdmissionLease, DependencyDrainError, DependencyDrainParticipant, DependencyWorkLease,
};
pub use lifecycle::{DEFAULT_DRAIN_TIMEOUT, DEFAULT_SERVICE_MUTEX_TIMEOUT};
pub use local_health::{
    LocalHealth, LocalHealthActivator, LocalHealthLiveness, LocalHealthServer, local_health_pair,
};
pub use service_mutex::{
    AcquireError as ServiceMutexAcquireError, AcquireOutcome, SERVICE_MUTEX_POLL_INTERVAL,
    ServiceMutex,
};

/// Guard returned by service telemetry initialization.
///
/// Keep the guard alive for as long as process-level tracing and metrics
/// exporters should remain installed.
///
/// ```no_run
/// # use pplx_space_common_svc::{init_service, TelemetryGuard};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let _guard: TelemetryGuard = init_service("space-api")?;
/// # Ok(())
/// # }
/// ```
pub type TelemetryGuard = pplx_space_observability::TelemetryGuard;

/// Flush telemetry while the service runtime can still drive exporter I/O.
/// A telemetry failure must not change the outcome of the service shutdown.
pub async fn shutdown_telemetry(guard: TelemetryGuard) {
    if let Err(error) = guard.shutdown_async().await {
        tracing::warn!(%error, "telemetry shutdown failed");
    }
}

/// Initializes process-wide service telemetry.
///
/// Use this once during daemon startup before serving requests. This also
/// installs SPACE's process-wide rustls crypto provider before telemetry or
/// service clients can construct TLS state.
///
/// ```no_run
/// # use pplx_space_common_svc::init_service;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let telemetry = init_service("space-node")?;
/// // Run the service inside its Tokio runtime.
/// pplx_space_common_svc::shutdown_telemetry(telemetry).await;
/// # Ok(())
/// # }
/// ```
pub fn init_service(service_name: &str) -> Result<TelemetryGuard, ObservabilityError> {
    let guard = init_service_subprocess(service_name)?;
    systemd::install_watchdog(service_name);
    Ok(guard)
}

pub fn init_service_with_log_context(
    service_name: &str,
    log_context: LogContext,
) -> Result<TelemetryGuard, ObservabilityError> {
    let guard = init_service_subprocess_with_log_context(service_name, log_context)?;
    systemd::install_watchdog(service_name);
    Ok(guard)
}

/// Initializes telemetry for a subprocess serving service work.
///
/// Use this for service-owned subprocesses that emit service telemetry but do
/// not own the parent daemon's systemd watchdog contract.
pub fn init_service_subprocess(service_name: &str) -> Result<TelemetryGuard, ObservabilityError> {
    init_service_subprocess_with_log_context(service_name, LogContext::default())
}

pub fn init_service_subprocess_with_log_context(
    service_name: &str,
    log_context: LogContext,
) -> Result<TelemetryGuard, ObservabilityError> {
    install_default_rustls_crypto_provider();
    bootstrap::set_current_service_name(service_name);
    bootstrap::initialize_failed_grpc_request_logging();
    pplx_space_observability::init_with_log_context(service_name, log_context)
}

pub use pplx_space_rustls_provider::install_default_rustls_crypto_provider;

pub use bootstrap::{
    CLIENT_SERVICE_METADATA_KEY, GRPC_HTTP2_KEEPALIVE_INTERVAL, GRPC_HTTP2_KEEPALIVE_TIMEOUT,
    GRPC_REQUEST_TIMEOUT, HttpServerMakeSpan, HttpServerTraceLayer, HttpSpanMetadata,
    REDACTED_HTTP_PATH_VALUE, SandboxSpanLayer, SandboxSpanService, SocketGroup, StopTokens,
    TraceparentResponseLayer, UnixSocketOptions, apply_unix_socket_permissions, bind_unix_socket,
    current_service_name, format_grpc_request_params, grpc_health_service, grpc_server_builder,
    grpc_server_builder_with_request_timeout, grpc_trace_layer, http_trace_layer, is_probe_request,
    log_failed_grpc_request_params, record_safe_grpc_request_params, request_id_from_headers,
    set_current_service_name, shutdown_token, shutdown_token_with_drain, sigterm_shutdown,
    stop_tokens, truncate_utf8_for_log, unix_group_exists, wait_for_sigterm,
};

pub use lifecycle::{DrainSignal, drain_server};

pub use pplx_space_common_grpc::record_sandbox_id;

pub use rate_limiter::{
    GrpcRateLimitBucket, GrpcRateLimiterConfig, GrpcRateLimiterLayer, GrpcRateLimiterLimits,
    GrpcRateLimiterMode, UnknownPeerPolicy, conservative_uds_grpc_rate_limiter_layer,
    grpc_rate_limiter_layer,
};

pub use peer_auth::{
    AuthorizedUdsPeer, LocalRole, PEER_PROCESS_CHECK_ENV, ParsePeerProcessCheckModeError,
    PeerIdentity, PeerProcessCheckMode, PeerProcessCheckModeEnvError, UdsPeerAccount,
    UdsPeerAuthorizer, UdsPeerPrincipal, capability, peer_identity_from_tonic_request,
    peer_may_set_local_authority, require_local_authority_peer,
};

pub use pplx_space_ingress_admission as ingress_admission;
