//! The API service body: surface selection, the startup configuration
//! validations, both database pools, the [`AppState`] assembly, and the axum
//! server with its drain supervisor.
//!
//! This is what the `space-api-svc` binary's `main` used to hold; the binary
//! now parses flags, initializes telemetry, and calls [`run`].

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::routes::AppState;
use pplx_space_common_svc::sql::{CredentialsPool, connect_space_user_pool_lazy};

/// Telemetry and log name for a process serving the public API, on either the
/// dedicated public host or the shared host.
pub const PUBLIC_SERVICE_NAME: &str = "space-api";
/// Telemetry and log name for a process serving only the admin API.
pub const ADMIN_SERVICE_NAME: &str = "space-admin-api";
/// Default ceiling on the post-SIGTERM drain, exposed so the binary's
/// `--shutdown-grace-secs` default lives next to the code that spends it.
pub const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 150;

/// The API surface a process serves. It selects the service name, which
/// startup validations are required, and the router.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiSurface {
    /// Dedicated public API host plus host-gated E2B compatibility API.
    Public,
    /// Single-host topology with `/api`, `/admin/api`, and `/e2b/api` roots.
    Shared,
    /// Dedicated admin API only.
    Admin,
}

impl ApiSurface {
    /// The service name this surface reports to telemetry and drain logs.
    pub fn service_name(self) -> &'static str {
        match self {
            Self::Public | Self::Shared => PUBLIC_SERVICE_NAME,
            Self::Admin => ADMIN_SERVICE_NAME,
        }
    }

    const fn requires_public_cursor_secret(self) -> bool {
        matches!(self, Self::Public | Self::Shared)
    }
}

/// Everything [`run`] needs from the process that hosts it: the binary's
/// flags, already parsed. Every other input — the route-signing bundle, the
/// ECW keyring, the public cursor secret, the database passwords, the
/// federated-identity and credential-injection settings — stays in the
/// environment, read by the code that owns it, so a private `main` only has
/// to supply what the binary's flags do.
#[derive(Clone)]
pub struct ApiRunConfig {
    /// Listen address for the HTTP server.
    pub listen: String,
    /// Maximum time to wait for in-flight HTTP requests after SIGTERM.
    pub shutdown_grace: Duration,
    /// API surface to serve from this process.
    pub surface: ApiSurface,
    /// PostgreSQL connection string for the `space_user` and `space_creds`
    /// pools.
    pub database_url: String,
}

impl ApiRunConfig {
    /// The service name this configuration runs under, so the caller can
    /// initialize telemetry before calling [`run`] without reimplementing the
    /// surface-to-name mapping.
    pub fn service_name(&self) -> &'static str {
        self.surface.service_name()
    }
}

// `database_url` can carry a password, and the binary already hides it from
// `--help` with clap's `hide_env_values`. `Debug` is written by hand rather
// than derived so the same string cannot reach a log through a config dump.
impl std::fmt::Debug for ApiRunConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiRunConfig")
            .field("listen", &self.listen)
            .field("shutdown_grace", &self.shutdown_grace)
            .field("surface", &self.surface)
            .field("database_url", &"<redacted>")
            .finish()
    }
}

/// Why [`run`] returned without a clean shutdown. Every variant is fatal to
/// the process: each names a startup step that leaves the API unable to serve
/// its surface, or the server itself failing, so none of them is something to
/// log and continue past.
#[derive(Debug, thiserror::Error)]
pub enum RunApiError {
    #[error("invalid or missing SPACE API route signing configuration: {0}")]
    RouteSigningConfiguration(
        #[source] pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error,
    ),
    #[error("{0}")]
    PublicCursorSecret(String),
    #[error("invalid or missing ECW credential signing configuration: {0}")]
    EcwCredentialConfiguration(#[source] pplx_space_common_svc::ecw_credential::EcwCredentialError),
    #[error("failed to bind the API listener on {listen}: {source}")]
    BindListener {
        listen: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to initialize the API space_user pool: {0}")]
    SpaceUserPool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error("failed to initialize the API space_creds pool: {0}")]
    CredentialsPool(#[source] pplx_space_common_svc::sql::DatabaseConnectOptionsError),
    #[error(transparent)]
    AppState(#[from] crate::routes::AppStateError),
    #[error("ingress activation service: {0}")]
    Activation(#[from] tonic::Status),
    #[error("the API server exited with an error: {0}")]
    Serve(#[source] std::io::Error),
}

/// Mirror `shutdown` onto the token the service itself runs off, raising
/// `drain_signal` before cancelling it.
///
/// Ordering is the point: the readiness route reports NOT_SERVING the instant
/// the flag is set, and that has to be true *before* the axum graceful
/// shutdown stops accepting, so a load balancer sees the instance leave
/// rotation rather than having connections cut. `shutdown_token_with_drain`
/// gave the pre-split `main` that ordering by raising the flag inside the
/// signal handler; the caller's token only says that shutdown started. The
/// task also exits when the service token is cancelled directly, which is how
/// [`run`] stops it on the way out.
fn drain_before_shutdown(
    drain_signal: pplx_space_common_svc::DrainSignal,
    shutdown: CancellationToken,
) -> CancellationToken {
    let service_token = CancellationToken::new();
    tokio::spawn({
        let service_token = service_token.clone();
        async move {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    drain_signal.begin();
                    service_token.cancel();
                }
                () = service_token.cancelled() => {}
            }
        }
    });
    service_token
}

/// Serve the configured API surface until `shutdown` fires or a fatal
/// condition ends it. Telemetry must already be initialized by the caller.
pub async fn run(config: ApiRunConfig, shutdown: CancellationToken) -> Result<(), RunApiError> {
    let service_name = config.surface.service_name();
    let route_ed25519_authority = match config.surface {
        ApiSurface::Admin => None,
        ApiSurface::Public | ApiSurface::Shared => Some(
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority::load_required_from_env()
                .map_err(RunApiError::RouteSigningConfiguration)?,
        ),
    };

    if config.surface.requires_public_cursor_secret() {
        crate::routes::validate_public_cursor_secret().map_err(RunApiError::PublicCursorSecret)?;
    }
    if !matches!(config.surface, ApiSurface::Admin) {
        pplx_space_common_svc::ecw_credential::EcwCredentialKeyring::load_required_from_env()
            .map_err(RunApiError::EcwCredentialConfiguration)?;
    }

    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .map_err(|source| RunApiError::BindListener {
            listen: config.listen.clone(),
            source,
        })?;
    let pool = connect_space_user_pool_lazy(&config.database_url, service_name)
        .map_err(RunApiError::SpaceUserPool)?;
    let credentials_pool = CredentialsPool::connect_lazy(&config.database_url, service_name)
        .map_err(RunApiError::CredentialsPool)?;
    tracing::info!("prepared lazy space_user and space_creds API database pools");
    let drain_signal = pplx_space_common_svc::DrainSignal::new();
    let shutdown_token = drain_before_shutdown(drain_signal.clone(), shutdown);
    let task_tracker = TaskTracker::new();
    let state = AppState::from_pools_with_lifecycle(
        pool,
        credentials_pool,
        drain_signal,
        shutdown_token.clone(),
        task_tracker.clone(),
        route_ed25519_authority.map(std::sync::Arc::new),
    )
    .await?;

    let activation_server =
        crate::ingress_activation::server(&state, task_tracker.clone(), shutdown_token.clone())
            .await?;

    let app = match config.surface {
        ApiSurface::Public => crate::routes::router(state),
        ApiSurface::Shared => crate::routes::shared_host_router(state),
        ApiSurface::Admin => crate::routes::dedicated_admin_router(state),
    };

    tracing::info!(
        listen = %config.listen,
        surface = ?config.surface,
        "space-api-svc listening"
    );
    let shutdown = {
        let shutdown_token = shutdown_token.clone();
        let task_tracker = task_tracker.clone();
        async move {
            shutdown_token.cancelled().await;
            // Closing allows `wait` to complete but still tracks work spawned
            // by HTTP handlers that are already inside the server drain.
            task_tracker.close();
        }
    };
    // Connect info gives request handlers the TCP peer address — the
    // ECW request log falls back to it when `X-Forwarded-For` is absent.
    let server = axum::serve(
        pplx_space_common_svc::http_listener::nodelay(listener),
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown);
    let tracked_server = {
        let shutdown_token = shutdown_token.clone();
        async move {
            let http = async { server.await.map_err(RunApiError::Serve) };
            let activation = async { activation_server.await.map_err(RunApiError::Activation) };
            tokio::pin!(http, activation);
            let result = tokio::select! {
                result = &mut http => {
                    shutdown_token.cancel();
                    let other = activation.await;
                    result.and(other)
                }
                result = &mut activation => {
                    shutdown_token.cancel();
                    let other = http.await;
                    result.and(other)
                }
            };
            task_tracker.close();
            task_tracker.wait().await;
            result
        }
    };

    let drain_outcome = pplx_space_common_svc::drain_server(
        service_name,
        &shutdown_token,
        config.shutdown_grace,
        tracked_server,
    )
    .await;
    // Nothing is left to drain, so release the mirroring task whether the
    // server returned on its own or the drain budget ran out.
    shutdown_token.cancel();
    if let Some(result) = drain_outcome {
        result?;
    }

    Ok(())
}

#[cfg(test)]
mod tests;
