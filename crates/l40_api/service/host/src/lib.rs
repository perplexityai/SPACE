//! Process setup for SPACE service binaries: telemetry initialization and
//! shutdown, with the process-wide rustls provider, the current service name
//! and the systemd watchdog installed alongside.
//!
//! ```no_run
//! # use pplx_space_service_host::init_service;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let telemetry = init_service("space-node")?;
//! pplx_space_service_host::shutdown_telemetry(telemetry).await;
//! # Ok(())
//! # }
//! ```

use pplx_space_cluster_id::ClusterId;
use pplx_space_observability::{LogContext, ObservabilityError};

/// Guard returned by service telemetry initialization.
///
/// Keep the guard alive for as long as process-level tracing and metrics
/// exporters should remain installed.
///
/// ```no_run
/// # use pplx_space_service_host::{init_service, TelemetryGuard};
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
/// # use pplx_space_service_host::init_service;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let telemetry = init_service("space-node")?;
/// // Run the service inside its Tokio runtime.
/// pplx_space_service_host::shutdown_telemetry(telemetry).await;
/// # Ok(())
/// # }
/// ```
pub fn init_service(service_name: &str) -> Result<TelemetryGuard, ObservabilityError> {
    let guard = init_service_subprocess(service_name)?;
    pplx_space_systemd_watchdog::install_watchdog(service_name);
    Ok(guard)
}

pub fn init_service_with_log_context(
    service_name: &str,
    log_context: LogContext,
) -> Result<TelemetryGuard, ObservabilityError> {
    let guard = init_service_subprocess_with_log_context(service_name, log_context)?;
    pplx_space_systemd_watchdog::install_watchdog(service_name);
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
    pplx_space_rustls_provider::install_default_rustls_crypto_provider();
    pplx_space_common_grpc::set_current_service_name(service_name);
    pplx_space_grpc_server::initialize_failed_grpc_request_logging();
    // A malformed process cluster id is dropped by its owner. Its warning is
    // emitted on the first read, normally this one, before the subscriber
    // below is installed, so it is not logged.
    let log_context =
        with_process_cluster_id(log_context, pplx_space_cluster_id::process_cluster_id());
    pplx_space_observability::init(service_name, log_context)
}

/// `log_context` with the process-global cluster id as its only cluster id,
/// replacing or clearing any the caller set. That id has one owner, which the
/// sandbox-event stream also reads, so logs, metrics and events never
/// disagree about it.
fn with_process_cluster_id(log_context: LogContext, cluster_id: Option<&ClusterId>) -> LogContext {
    match cluster_id {
        Some(cluster_id) => log_context.with_cluster_id(cluster_id.as_str()),
        None => log_context.without_cluster_id(),
    }
}

#[cfg(test)]
mod tests;
