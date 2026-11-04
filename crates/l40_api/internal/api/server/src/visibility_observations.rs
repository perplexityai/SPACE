//! Trailing-hour count of alertable template-visibility degradations, per
//! reason, published as a distribution on every tick.
//!
//! The `template.visibility_lookup.degraded` counter only reports when an event
//! happens, so an hour with no degradations has no points and a persistence
//! monitor can't tell it from missing data. This series is dense: each replica
//! emits every reason on every tick, zeros included, so Datadog can evaluate
//! persistence with `min(last_4h)`. Datadog's `min` skips missing points, so a
//! reporting gap inside the window reads as neither clean nor degraded; the
//! monitor needs its own no-data or gap handling, which also covers a
//! cluster whose replicas all stop emitting.
//!
//! The window is process-local and monotonic-clock based. A restart starts from
//! zero. Under `max:` that reads as recovery when the restarted replica was the
//! worst one, and a rollout of every replica resets persistence fleet-wide;
//! that's the accepted cost of keeping evaluation state out of the service.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pplx_space_metrics::template::VisibilityLookupDegradation;
use tokio_util::sync::CancellationToken;

/// Trailing window each sample covers.
const WINDOW: Duration = Duration::from_secs(3600);
/// Bucket width.
const BUCKET: Duration = Duration::from_secs(60);
/// Buckets per reason inside [`WINDOW`]. [`prune`] keeps at most one more, so
/// memory stays bounded no matter how fast degradations arrive.
const RETAINED_BUCKETS: u64 = WINDOW.as_secs() / BUCKET.as_secs();
/// Emission period. Two samples per minute keep the series dense well inside
/// the monitor's window.
const EMIT_PERIOD: Duration = Duration::from_secs(30);

const ALERTABLE: [VisibilityLookupDegradation; 3] = VisibilityLookupDegradation::ALERTABLE;

/// `(bucket index, count)` pairs in ascending bucket order.
type Buckets = VecDeque<(u64, u64)>;

pub(crate) struct Observations {
    started: Instant,
    /// Each alertable reason paired with its buckets.
    buckets: Mutex<[(VisibilityLookupDegradation, Buckets); ALERTABLE.len()]>,
}

impl Observations {
    pub(crate) fn new() -> Self {
        Self::starting_at(Instant::now())
    }

    fn starting_at(started: Instant) -> Self {
        Self {
            started,
            buckets: Mutex::new(ALERTABLE.map(|reason| (reason, VecDeque::new()))),
        }
    }

    pub(crate) fn record(&self, reason: VisibilityLookupDegradation) {
        self.record_at(reason, Instant::now());
    }

    fn record_at(&self, reason: VisibilityLookupDegradation, now: Instant) {
        let mut buckets = self
            .buckets
            .lock()
            .expect("visibility observations poisoned");
        // `template_missing` has no entry, so it falls through here.
        let Some((_, reason_buckets)) = buckets
            .iter_mut()
            .find(|(alertable, _)| *alertable == reason)
        else {
            return;
        };
        // `now` is read before the lock, so a handler preempted across a
        // minute boundary can arrive after a newer bucket exists. Counting it in
        // the newest bucket keeps the deque ascending and only lengthens the
        // event's stay in the window.
        let bucket = match reason_buckets.back() {
            Some((last, _)) => self.bucket(now).max(*last),
            None => self.bucket(now),
        };
        match reason_buckets.back_mut() {
            Some((last, count)) if *last == bucket => *count += 1,
            _ => reason_buckets.push_back((bucket, 1)),
        }
        prune(reason_buckets, bucket);
    }

    /// Alertable degradations per reason in the trailing [`WINDOW`], zeros
    /// included.
    fn trailing_counts(
        &self,
        now: Instant,
    ) -> [(VisibilityLookupDegradation, u64); ALERTABLE.len()] {
        let bucket = self.bucket(now);
        let mut buckets = self
            .buckets
            .lock()
            .expect("visibility observations poisoned");
        buckets.each_mut().map(|(reason, reason_buckets)| {
            prune(reason_buckets, bucket);
            (*reason, reason_buckets.iter().map(|(_, count)| count).sum())
        })
    }

    fn bucket(&self, now: Instant) -> u64 {
        let elapsed = now.saturating_duration_since(self.started);
        // Whole minutes since process start; u64 covers any realistic uptime.
        elapsed.as_secs() / BUCKET.as_secs()
    }
}

/// Drop buckets that lie entirely outside the window ending at `current`.
///
/// A bucket stays until its *end* is an hour old, so an event late in a minute
/// is never dropped early; the window errs up to one bucket long. Dropping on
/// the bucket's start would emit a premature zero, which a `min(last_4h)`
/// persistence test reads as a clean sample.
fn prune(buckets: &mut Buckets, current: u64) {
    while buckets
        .front()
        .is_some_and(|(bucket, _)| *bucket + RETAINED_BUCKETS < current)
    {
        buckets.pop_front();
    }
}

/// Publish [`Observations::trailing_counts`] every [`EMIT_PERIOD`] until
/// `shutdown` fires.
pub(crate) async fn emit(observations: Arc<Observations>, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(EMIT_PERIOD);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            // Shutdown first: the first tick is immediate, and a replica
            // cancelled during startup must not publish a round of zeros.
            biased;
            () = shutdown.cancelled() => return,
            _ = interval.tick() => {}
        }
        for (reason, count) in observations.trailing_counts(Instant::now()) {
            pplx_space_metrics::template::visibility_lookup_alertable_last_hour(reason, count);
        }
    }
}

#[cfg(test)]
#[path = "visibility_observations_tests.rs"]
mod tests;
