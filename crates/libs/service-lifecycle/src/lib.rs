//! Shared default durations and helpers for the SPACE service lifecycle.
//!
//! Each daemon can override these via its own CLI flags, but the defaults
//! sit here so the SPACE rollout shape (`terminationGracePeriodSeconds`,
//! mutex acquire budgets, drain budgets) stays consistent across services.
//!
//! It is its own crate so a service can share these defaults and the drain
//! helpers, or the stop-signal handling in [`shutdown`], without the rest of
//! `pplx-space-common-svc`.

pub mod shutdown;

use std::future::IntoFuture;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// Default upper bound on how long a passive instance waits for the
/// predecessor to release the service mutex before exiting non-zero. 10 s
/// of headroom above the chart's `terminationGracePeriodSeconds: 60` so a
/// predecessor SIGKILLed at the grace ceiling still releases the mutex
/// before our timer expires.
pub const DEFAULT_SERVICE_MUTEX_TIMEOUT: Duration = Duration::from_secs(70);

/// Default upper bound on the active-instance drain on SIGTERM. Covers
/// reconciler exit, background-worker shutdown, and tonic in-flight RPCs.
/// Designed to fit comfortably under
/// `terminationGracePeriodSeconds: 60`-or-larger so the kernel
/// SIGKILL is a backstop, not the expected path.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// Shared in-process drain flag for readiness gates.
///
/// Services flip this as soon as they receive a shutdown signal. Readiness
/// probes can then fail while the server continues draining in-flight work.
#[derive(Clone, Debug, Default)]
pub struct DrainSignal {
    draining: Arc<AtomicBool>,
}

impl DrainSignal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }
}

/// Wait for a server future to finish normally, or after shutdown starts wait
/// up to `drain_timeout` for in-flight requests to complete.
///
/// Returns `None` when the drain budget is exhausted. Dropping the server
/// future lets process termination continue so Kubernetes' SIGKILL remains a
/// last-resort backstop instead of the normal shutdown path.
pub async fn drain_server<S>(
    service_name: &'static str,
    shutdown_token: &CancellationToken,
    drain_timeout: Duration,
    server: S,
) -> Option<S::Output>
where
    S: IntoFuture,
{
    let server = server.into_future();
    tokio::pin!(server);

    tokio::select! {
        output = &mut server => Some(output),
        _ = shutdown_token.cancelled() => {
            tracing::info!(
                service_name,
                drain_timeout_secs = drain_timeout.as_secs(),
                "shutdown signal received; draining server"
            );
            match tokio::time::timeout(drain_timeout, &mut server).await {
                Ok(output) => Some(output),
                Err(_) => {
                    tracing::warn!(
                        service_name,
                        drain_timeout_secs = drain_timeout.as_secs(),
                        "server drain budget exhausted; exiting"
                    );
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
