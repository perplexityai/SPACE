//! Traffic-independent liveness ticks for the two admission limiters.
//!
//! Both [`crate::cluster_admission`] and [`crate::client_concurrency`] write
//! their decision counter only when there is a decision to record, so a silent
//! counter means either "nothing arrived to admit" or "this emitter is gone",
//! and no query of that counter can tell the two apart. Each limiter therefore
//! also ticks a heartbeat counter on the timer below, from its own module, so
//! the heartbeat and the decision counter share everything beneath the metric
//! helper: the process, the metrics sink, and the `counter` path through it.
//!
//! What that proves, exactly: a heartbeat visible in Datadog means the process
//! is running and the whole chain from that `counter` call to ingestion works.
//! It does not prove the decision call sites are reachable — whether a code
//! path executes is not something a metric from a different call site can show
//! — and it reports nothing about the limiter's configuration or verdicts.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// Heartbeat cadence, shared by both limiters so one number sets the absence
/// window a liveness monitor can safely assume.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

// The metrics catalog and the rate-limiting RFC both state this cadence, and a
// liveness monitor's evaluation window is sized from it. No test pins it — the
// loop's tests supply their own interval — so pin it here, and a change fails
// the build rather than the prose.
const _: () = assert!(HEARTBEAT_INTERVAL.as_secs() == 60);

/// Call `emit` every `interval` until `shutdown_token` fires.
///
/// The first tick is due immediately rather than one interval in, so a
/// short-lived loop still reports — though a loop whose token is already
/// cancelled when it is first polled returns without emitting, because the
/// `select!` below is biased toward shutdown.
///
/// When the loop starts is the caller's choice, and it is neither the
/// process's start nor its serving boundary — space-api spawns it partway
/// through `AppState` construction, after the substrate is built but before the
/// server listens. So a silent replica may simply not have got there yet, and a
/// ticking one is not thereby serving requests. What the cluster controller's
/// silence means has more cases than that, and they are enumerated at its spawn
/// site in `run_reconcilers` rather than summarized here.
///
/// Granularity, because it bounds what any monitor over this can claim: the
/// counter carries no call-site tags at all, so **there is no replica
/// dimension**. A group reports whether *some* process is emitting, and one
/// replica whose metric path is broken while its peers are healthy lowers the
/// group's count without ever making it absent. Detecting that would need a
/// per-pod tag, which is a cardinality decision and a tag-configuration
/// change, not a code one.
///
/// Which of the sink's process-global tags a query can actually group by is a
/// separate question, and not one this file can answer — unlisted tags are
/// dropped at ingestion until the deployment's tag configuration allows them.
///
/// `MissedTickBehavior::Delay` is a choice about count fidelity, not about
/// absence: tokio's default discharges every missed tick at once, so a stalled
/// runtime would leave its stall window empty and then over-report the next
/// one. No test distinguishes the two — the behaviors diverge only once a
/// deadline is actually missed — and an absence monitor cannot tell them apart
/// either, so this matters only to a reader who thresholds on the count. The
/// `biased;` below is likewise unpinned: the branches only race when a
/// shutdown coincides with a due tick, which no test arranges.
pub(crate) async fn run_heartbeat(
    interval: Duration,
    shutdown_token: CancellationToken,
    mut emit: impl FnMut(),
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // Biased so a shutdown that coincides with a due tick ends the loop
            // instead of emitting one more point at random.
            biased;
            _ = shutdown_token.cancelled() => return,
            _ = ticker.tick() => {}
        }
        emit();
    }
}

#[cfg(test)]
mod tests;
