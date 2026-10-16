//! Per-replica stream load, served on an internal listener for the cluster
//! controller's scale-in ordering.
//!
//! The report is read-only and carries one aggregate count, nothing scoped to
//! an org, sandbox, or session. It is deliberately unauthenticated: the only
//! caller is the cluster controller dialing the pod IP over plain HTTP, and
//! sending `SPACE_INTERNAL_TOKEN` there would expose a credential that
//! authorizes node-api calls in exchange for protecting one integer. Keep the
//! listener off the public Service, and keep anything sensitive off this
//! router.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use pplx_space_common_core::data_plane_load::{DATA_PLANE_STREAM_LOAD_PATH, DataPlaneStreamLoad};
use tokio_util::sync::CancellationToken;

use crate::state::AttachedStreamLimiter;

pub(crate) fn stream_load(limiter: &AttachedStreamLimiter) -> DataPlaneStreamLoad {
    DataPlaneStreamLoad {
        active_streams: limiter.active_total() as u64,
    }
}

pub(crate) fn internal_router(limiter: std::sync::Arc<AttachedStreamLimiter>) -> Router {
    Router::new()
        .route(DATA_PLANE_STREAM_LOAD_PATH, get(stream_load_handler))
        .with_state(limiter)
}

async fn stream_load_handler(
    State(limiter): State<std::sync::Arc<AttachedStreamLimiter>>,
) -> Json<DataPlaneStreamLoad> {
    Json(stream_load(&limiter))
}

/// Serve the internal router on its own task until `shutdown` fires.
///
/// If the serve task ends before `shutdown` (with graceful shutdown,
/// `axum::serve` retries failed accepts and only returns once the signal
/// fires, so in practice that means a panic), the supervisor logs it and the
/// replica keeps serving. The cluster controller then can't read this
/// replica's load and ranks it as busy after a few failed probes, which only
/// errs toward removing it last; shutting the replica down instead would cut
/// every stream it holds. The supervisor watches the task's `JoinHandle` rather
/// than the serve result so a panic is caught too.
pub(crate) fn spawn_internal(
    listener: tokio::net::TcpListener,
    limiter: std::sync::Arc<AttachedStreamLimiter>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<InternalListenerEnd> {
    let stopped = shutdown.clone();
    let serve = tokio::spawn(async move {
        axum::serve(listener, internal_router(limiter))
            .with_graceful_shutdown(async move { stopped.cancelled().await })
            .await
    });
    tokio::spawn(supervise_internal(serve, shutdown))
}

/// How the internal listener's serve task ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InternalListenerEnd {
    /// Stopped cleanly because `shutdown` fired.
    Shutdown,
    /// Ended on its own: returned early, errored, or panicked. Logged; the
    /// replica keeps serving.
    Early,
}

/// Classify and log the serve task's end. Never touches `shutdown`.
pub(crate) async fn supervise_internal(
    serve: tokio::task::JoinHandle<std::io::Result<()>>,
    shutdown: CancellationToken,
) -> InternalListenerEnd {
    let ended = serve.await;
    if shutdown.is_cancelled()
        && let Ok(Ok(())) = ended
    {
        return InternalListenerEnd::Shutdown;
    }
    let error = match ended {
        Ok(Ok(())) => "stopped before shutdown".to_string(),
        Ok(Err(error)) => error.to_string(),
        Err(join) => join.to_string(),
    };
    tracing::error!(
        %error,
        "data-plane internal stream-load listener ended; this replica keeps serving \
         and the cluster controller will rank it as busy until it restarts"
    );
    InternalListenerEnd::Early
}

#[cfg(test)]
mod tests;
