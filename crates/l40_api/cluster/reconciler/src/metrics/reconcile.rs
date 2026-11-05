//! Reconcile queue lag, stale claims and trigger fan-out, which the cluster
//! reconciler records.

use std::fmt;
use std::time::Instant;

use pplx_space_metrics_emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};

pub fn queue_stale_claims(facet: &str, count: u64) {
    gauge(
        "reconcile.queue.stale_claims",
        count as f64,
        &[tag("facet", facet)],
    );
}

pub fn trigger_depth(state: &str, count: u64) {
    gauge(
        "reconcile.trigger.depth",
        count as f64,
        &[tag("state", state)],
    );
}

pub fn trigger_fanout(result: &str, error_class: &str) {
    counter(
        "reconcile.trigger.fanout",
        1,
        &[tag("result", result), tag("error_class", error_class)],
    );
}

/// One processed trigger-fanout claim batch: how many triggers the claim held
/// and the wall-clock time from claiming them to writing their completions —
/// target load, live-node filtering, endpoint resolution, the concurrent
/// node-api pokes, and the claim completion/dead-letter writes. A batch that
/// fails to load its targets still emits, so a degraded Postgres shows up
/// here rather than as a silent gap.
pub fn trigger_fanout_batch(claimed: u64, started_at: Instant) {
    distribution(
        "reconcile.trigger.fanout.batch.claimed",
        claimed as f64,
        &[],
    );
    timing_ms(
        "reconcile.trigger.fanout.batch.duration_ms",
        elapsed_ms(started_at),
        &[],
    );
}

/// Which clock a `reconcile.queue.lag_seconds` series runs. The age of a queue
/// ref answers two different questions — "is anything picking work up?" and "is
/// this element able to converge?" — and one number for both misreports queue
/// health, because a single unconvergeable element ages past the entire retry
/// backoff schedule while claim throughput is perfectly healthy.
///
/// The two series are not a partition of the rows; they are two clocks, and a
/// row can appear on both.
///
/// Neither is the same partition as `reconcile.queue.depth{state:…}`, whose
/// `claimed` means "a node currently holds the claim token".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueLagStage {
    /// How long the oldest *claimable* ref has been waiting for a node to pick
    /// it up, measured from when it became claimable — `requested_at` for a ref
    /// that has never failed, or the expiry of its backoff window for one that
    /// has. Rows still inside a backoff window are excluded: nothing is
    /// supposed to claim them yet.
    ///
    /// This is the queue-health signal, and it holds for a ref at any `attempts`
    /// count. A ref that failed once and then sat past an expired backoff
    /// because its node's node-ctl wedged is a claim stall, not a retry, and it
    /// surfaces here — the case a discriminator of `attempts = 0` would hide
    /// forever, since `attempts` only resets on a completion that never comes.
    ///
    /// Bounded by the node-ctl poll interval in a healthy fleet.
    Claim,
    /// Total lifetime of the oldest ref that has failed at least one apply
    /// (`attempts > 0`), measured from `requested_at`, which survives every
    /// re-drive.
    ///
    /// Dominated by the capped-exponential-backoff schedule, not by how fast
    /// nodes claim work: a ref that retries to the attempts cap ages past the
    /// whole backoff sum before it dead-letters, regardless of queue health.
    /// Read it as "some element cannot converge", never as latency.
    Retry,
}

impl QueueLagStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Retry => "retry",
        }
    }
}

impl fmt::Display for QueueLagStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn queue_lag_seconds(facet: &str, stage: QueueLagStage, lag_seconds: f64) {
    gauge(
        "reconcile.queue.lag_seconds",
        lag_seconds.max(0.0),
        &[tag("facet", facet), tag("stage", stage.as_str())],
    );
}
