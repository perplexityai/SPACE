//! Metric helpers for the cluster-controller's snapshot-retention sweeper.
//!
//! Names match the `snapshot_retention_sweeper.*` namespace. Sibling to [`crate::snapshot_storage_sweeper`]
//! (which reaps S3 blobs orphaned by sandbox delete); this namespace
//! covers the keep-newest-N retention sweep over still-live sandboxes'
//! durable snapshots.

use std::time::Instant;

use crate::emit::{counter, elapsed_ms, gauge, tag, timing_ms};

/// Fresh eligible rows returned before the commitment cap. Emitted after
/// every tick with a successful fresh discovery page, including zero.
/// A later page failure is reported separately with `failed{reason:fresh_scan}`.
pub fn candidates_scanned(count: u64) {
    gauge(
        "snapshot_retention_sweeper.candidates_scanned",
        count as f64,
        &[],
    );
}

/// Durable claims returned by recovery. Emitted after every successful scan.
pub fn claimed_scanned(count: u64) {
    gauge(
        "snapshot_retention_sweeper.claimed_scanned",
        count as f64,
        &[],
    );
}

/// Fresh and recovery rows observed during one complete reconciler tick.
/// Emitted once per tick, including when either scan fails or both return zero.
pub fn scan_volume(count: u64) {
    gauge("snapshot_retention_sweeper.scan_volume", count as f64, &[]);
}

/// Fresh candidates shed after the shared recovery/fresh budget was consumed.
/// Recovery rows are not themselves counted. Emitted after every successful
/// fresh scan, including zero.
pub fn rate_limited(count: u64) {
    gauge("snapshot_retention_sweeper.rate_limited", count as f64, &[]);
}

/// One snapshot successfully pruned. `outcome` is one of:
///
/// - `"deleted"` — S3 prefix cleared and the snapshot + storage-location
///   DB rows removed.
/// - `"already_gone"` — S3 reported zero objects under the prefix (a
///   prior tick reaped it before the DB delete landed); the DB rows were
///   still removed idempotently.
pub fn pruned(outcome: &str) {
    counter(
        "snapshot_retention_sweeper.pruned",
        1,
        &[tag("outcome", outcome)],
    );
}

/// Dry-run candidate that passed route validation. No claim or delete ran.
pub fn would_prune() {
    counter("snapshot_retention_sweeper.would_prune", 1, &[]);
}

/// Failed phase, tagged by cause; logs carry the specific error.
pub fn failed(reason: &str) {
    counter(
        "snapshot_retention_sweeper.failed",
        1,
        &[tag("reason", reason)],
    );
}

/// Post-claim external deletion left the sandbox below keep-N. Expected zero.
pub fn floor_breached() {
    counter("snapshot_retention_sweeper.floor_breached", 1, &[]);
}

/// Marker writer selected the claimed artifact before finalize. This is a
/// lower bound for marked-restore mode and is expected to remain zero.
pub fn current_artifact_breached() {
    counter(
        "snapshot_retention_sweeper.current_artifact_breached",
        1,
        &[],
    );
}

/// Claimed prune completed by a later recovery pass.
pub fn resumed() {
    counter("snapshot_retention_sweeper.resumed", 1, &[]);
}

/// Wall-clock time for one reconciler tick (all candidates processed or
/// rate-limited).
pub fn loop_latency(started_at: Instant) {
    timing_ms(
        "snapshot_retention_sweeper.loop_latency_ms",
        elapsed_ms(started_at),
        &[],
    );
}

/// Backend list and delete latency for the snapshot's selected prefix.
pub fn backend_delete_latency(started_at: Instant) {
    timing_ms(
        "snapshot_retention_sweeper.backend_delete_latency_ms",
        elapsed_ms(started_at),
        &[],
    );
}

/// First failing claim guard, in store evaluation order. IDs belong only in logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimRefusalReason {
    MissingSandboxOrSnapshot,
    ActiveForkSource,
    MissingSnapshot,
    SnapshotNotReady,
    StorageLocationUnusable,
    StorageLocationChanged,
    KeepNFloor,
    ProtectedRestoreSource,
    AgeGate,
    ConditionalUpdate,
}

impl ClaimRefusalReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingSandboxOrSnapshot => "missing_sandbox_or_snapshot",
            Self::ActiveForkSource => "active_fork_source",
            Self::MissingSnapshot => "missing_snapshot",
            Self::SnapshotNotReady => "snapshot_not_ready",
            Self::StorageLocationUnusable => "storage_location_unusable",
            Self::StorageLocationChanged => "storage_location_changed",
            Self::KeepNFloor => "keep_n_floor",
            Self::ProtectedRestoreSource => "protected_restore_source",
            Self::AgeGate => "age_gate",
            Self::ConditionalUpdate => "conditional_update",
        }
    }
}

impl std::fmt::Display for ClaimRefusalReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One refused claim; preserves the existing drift aggregate without double counting.
pub fn claim_refused(reason: ClaimRefusalReason) {
    counter(
        "snapshot_retention_sweeper.failed",
        1,
        &[
            tag("reason", "drift"),
            tag("refusal_reason", reason.as_str()),
        ],
    );
}
