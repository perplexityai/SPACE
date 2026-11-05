//! Metric helpers for the cluster-controller's snapshot-storage sweeper.
//!
//! Names match the `snapshot_storage_sweeper.*` namespace.

use std::time::Instant;

use pplx_space_metrics_emit::{counter, elapsed_ms, gauge, tag, timing_ms};

/// Storage-location rows surfaced as candidates in one tick: the page the
/// scan returned, which is `2 * max_per_tick` rows, BEFORE the per-tick
/// cap selects which of them to act on. Written on every tick whose scan
/// completes, zero included, which makes it the loop's liveness signal.
pub fn candidates_scanned(count: u64) {
    gauge(
        "snapshot_storage_sweeper.candidates_scanned",
        count as f64,
        &[],
    );
}

/// Candidates this tick listed but shed because the per-tick cap was
/// already spent. Written on every tick whose scan completes, zero
/// included, so a sustained non-zero value is a direct measurement of the
/// sweeper running at its ceiling — no comparison against a separately
/// configured cap required.
pub fn rate_limited(count: u64) {
    gauge("snapshot_storage_sweeper.rate_limited", count as f64, &[]);
}

/// One successful sweep outcome. `outcome` is one of:
///
/// - `"deleted"` — S3 prefix successfully cleared and the DB row removed.
/// - `"already_gone"` — S3 reported zero objects under the prefix; the
///   blob was already gone and the DB row was removed idempotently.
pub fn deleted(outcome: &str) {
    counter(
        "snapshot_storage_sweeper.deleted",
        1,
        &[tag("outcome", outcome)],
    );
}

/// One failed sweep attempt. `reason` is bounded to coarse categories so
/// the metric stays a useful dashboard signal — detail goes in the log
/// line.
///
/// Reasons:
///
/// - `"parse_uri"` — `storage_connection_string` failed to parse as a
///   valid `s3://` URI. Terminal: same row will re-fail every tick until
///   manually repaired.
/// - `"encrypted_unsupported"` — the row has an encryption sidecar; the
///   sweeper doesn't yet decrypt. Soft-failure that leaves the row in
///   place for a future encryption-aware sweep.
/// - `"bucket_out_of_scope"` — the URI points at a bucket the cluster
///   isn't scoped to (e.g. an org-level
///   `snapshot_base_object_storage_uri` override). Terminal from
///   this reconciler's perspective until cross-bucket sweep is added.
/// - `"s3_delete"` — the S3 list/delete call failed (throttled, network,
///   permissions, ...). Transient; next tick retries.
/// - `"db_delete"` — the `DELETE FROM snapshot_storage_locations` failed
///   after the S3 blob was cleared. Transient; next tick re-checks S3
///   (idempotent) and retries the DB delete.
/// - `"drift"` — the conditional DELETE returned `rows_affected = 0`
///   (orphan state cleared between scan and delete). Expected to stay
///   at zero today since no production writer resets `orphaned_at`
///   or restores `snapshot_id`; a non-zero series means an admin tool
///   is racing the sweep or a future schema change introduced a
///   writer worth investigating.
pub fn failed(reason: &str) {
    counter(
        "snapshot_storage_sweeper.failed",
        1,
        &[tag("reason", reason)],
    );
}

/// Wall-clock time for one reconciler tick (all candidates processed or
/// rate-limited).
pub fn loop_latency(started_at: Instant) {
    timing_ms(
        "snapshot_storage_sweeper.loop_latency_ms",
        elapsed_ms(started_at),
        &[],
    );
}

/// Per-object S3 prefix delete latency. Captures list + batch-delete
/// round-trip for one storage-location prefix.
pub fn s3_delete_latency(started_at: Instant) {
    timing_ms(
        "snapshot_storage_sweeper.s3_delete_latency_ms",
        elapsed_ms(started_at),
        &[],
    );
}
