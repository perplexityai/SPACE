//! Request-concurrency observation for SPACE gRPC servers.
//!
//! tower-http's in-flight middleware owns the counting: a request holds its
//! slot from dispatch until its response body is consumed, dropped, or errors,
//! so long-lived streaming RPCs stay counted. A per-process sampler reads that
//! counter on an interval, which keeps the emission rate independent of the
//! request rate and leaves nothing on the request path but an atomic
//! increment.

use std::sync::OnceLock;
use std::time::Duration;

use tower_http::metrics::InFlightRequestsLayer;
use tower_http::metrics::in_flight_requests::InFlightRequestsCounter;

/// Sampled well inside a DogStatsD flush, so `max` still sees a burst shorter
/// than a flush.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Every listener in a process shares one counter, so the sampled depth is
/// process concurrency rather than a single listener's.
pub(crate) fn process_layer() -> InFlightRequestsLayer {
    static COUNTER: OnceLock<InFlightRequestsCounter> = OnceLock::new();
    let counter = COUNTER.get_or_init(InFlightRequestsCounter::new);
    start_sampler(counter);
    InFlightRequestsLayer::new(counter.clone())
}

/// Claiming the slot only after a runtime is found leaves a later call — one
/// made from inside a runtime — free to start sampling, and keeps the spawn to
/// once however many listeners a process builds. A process that exports no
/// metrics (its sink is off, as in a sandbox guest) does not sample: the
/// samples would go nowhere, and the sampler would wake the process every
/// second for its whole life.
fn start_sampler(counter: &InFlightRequestsCounter) {
    static SAMPLER: OnceLock<()> = OnceLock::new();
    if SAMPLER.get().is_some() {
        return;
    }
    if !pplx_space_observability::metrics_enabled() {
        tracing::debug!("metrics are not exported; gRPC active-request sampling not started");
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        tracing::debug!("no tokio runtime; gRPC active-request sampling not started");
        return;
    };
    if SAMPLER.set(()).is_err() {
        return;
    }
    runtime.spawn(sample_forever(counter.clone()));
}

/// Runs for the life of the process: it holds no state to drain, and a
/// listener that stopped serving is still worth reporting zero depth for.
async fn sample_forever(counter: InFlightRequestsCounter) {
    counter
        .run_emitter(SAMPLE_INTERVAL, |depth| async move {
            pplx_space_grpc_metrics::server_active_requests(depth);
        })
        .await;
}

#[cfg(test)]
mod tests;
