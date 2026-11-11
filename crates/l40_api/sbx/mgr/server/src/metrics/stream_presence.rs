//! Metric helpers for sbx-mgr's stream-presence touches.
//!
//! Names live in the `sandbox.stream_presence.*` namespace. While a foreground guest process stream stays
//! open, the stream forward loop periodically refreshes the sandbox's
//! auto-lifecycle idle clock so an actively-used sandbox is not suspended
//! as idle mid-command; these helpers make that loop observable in prod
//! (are touches landing, how often are they skipped, is a fleet of streams
//! hitting the per-stream cap).

use pplx_space_metrics_emit::{counter, tag};

/// One per presence-touch tick. `result` is a closed set owned by the
/// sbx-mgr `StreamPresenceTouch` outcome enum (plus the loop-level
/// `cap_reached` terminal tick):
///
/// - `applied` — the touch refreshed the idle clock.
/// - `skipped_lock` — the per-sandbox wake lock was contended (a wake or
///   suspend is in flight); the tick was skipped, not queued.
/// - `skipped_state` — the sandbox is not `Started`; a background task
///   never re-arms a suspending/suspended row.
/// - `cap_reached` — the per-stream presence cap elapsed; touches stop for
///   this stream and idle reclaim resumes (one per stream).
/// - `failed` — the touch errored (sandbox row deleted under an open
///   stream, or a persistence failure); the stream keeps forwarding.
pub fn touch(result: &str) {
    counter("sandbox.stream_presence.touch", 1, &[tag("result", result)]);
}
