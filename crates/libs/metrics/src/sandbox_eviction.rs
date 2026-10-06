//! Metric helpers for sbx-mgr's cold-sandbox eviction loop.
//!
//! Names live in the `sandbox.evict.*` namespace. The loop removes a suspended sandbox's entire
//! local presence (cache directory including the rootfs instance, plus
//! node-local DB rows) once its state is durably restorable from a ready
//! remote snapshot, freeing `/space/cache` held by parked sandboxes.

use std::time::Instant;

use crate::emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};

/// One per tick of the eviction loop. Emits the `sandbox.evict.tick`
/// counter and the `sandbox.evict.tick_duration_ms` timing under a single
/// `result:ok|error` tag — alert on absence of
/// `sandbox.evict.tick{result:ok}` to detect a stuck loop. `candidates` is
/// the number of cold suspended sandboxes the local pre-filter selected
/// this tick (before the per-sandbox cluster checks).
pub fn tick(result: &str, started_at: Instant, candidates: u64) {
    let tags = [tag("result", result)];
    counter("sandbox.evict.tick", 1, &tags);
    timing_ms(
        "sandbox.evict.tick_duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
    gauge("sandbox.evict.candidates", candidates as f64, &[]);
}

/// One per sandbox whose local presence was fully evicted. `reason` is
/// the eviction proof that fired — deliberately a single value, since
/// every eviction must carry a proof tied to the exact bytes deleted:
///
/// - `latest_backed_idle` — the bytes on disk are provably backed (the
///   pinned artifact's durability confirmed for its exact immutable
///   id, or — legacy unpinned rows — the newest local generation
///   equals the cluster's latest ready snapshot with the recency
///   witness holding), and the sandbox is past the minimum age floor.
pub fn evicted(reason: &str) {
    counter("sandbox.evict.evicted", 1, &[tag("reason", reason)]);
}

/// Age of the sandbox's last local transition when its local presence
/// was evicted. Every eviction is retained as a distribution sample.
/// Time between the evicted sandbox's suspend (its proof artifact's
/// creation) and the eviction, tagged by which proof dated it
/// (`proof:pinned|legacy_upload`). The premature-eviction canary for
/// the activity-witness idle gate: a healthy fleet sits at or above the
/// retention floor; a mass near zero means suspends are being evicted
/// immediately — the activity clocks stopped advancing for live
/// sandboxes and the touch plumbing needs investigation.
pub fn evicted_suspend_age_secs(age_secs: u64, proof: &str) {
    distribution(
        "sandbox.evict.evicted_suspend_age_secs",
        age_secs as f64,
        &[tag("proof", proof)],
    );
}

pub fn evicted_age_secs(age_secs: u64) {
    distribution("sandbox.evict.evicted_age_secs", age_secs as f64, &[]);
}

/// An eviction crossed the retention warning threshold. This is an
/// alertable early signal that disk pressure is forcing the node to
/// trade away recently used wake-cache.
pub fn retention_pressure(evictions: u64) {
    counter("sandbox.evict.retention_pressure", evictions, &[]);
}

/// Suspended sandbox rows excluded from eviction because `updated_at` is
/// empty or non-numeric. These rows require operator repair; guessing a
/// replacement timestamp could make live customer state prematurely
/// evictable.
pub fn malformed_updated_at(rows: u64) {
    gauge("sandbox.evict.malformed_updated_at", rows as f64, &[]);
}

/// One per candidate the loop examined and declined to evict. `reason`
/// enumerates the closed set of skip labels:
///
/// - `no_ready_backup` — the cluster has no ready full suspend snapshot
///   for the sandbox.
/// - `local_newer_than_backup` — the newest local generation is newer than
///   the cluster's latest ready snapshot (e.g. the last suspend's upload
///   failed); evicting would lose the newest state.
/// - `unknown_local_generation` — the node has no upload-job record to
///   identify its local generation; direct eviction is skipped. The repair
///   path may queue a fresh upload so a later tick can prove the bytes.
/// - `current_artifact_unproven` — the pinned current artifact's
///   durability could not be proven: no local `backup_completed_at`
///   marker and the exact-artifact cluster lookup found no ready
///   backup. The bytes on disk may be the only copy; kept.
/// - `generation_unproven` — legacy (unpinned) row whose generation
///   equality could not be established: the cluster's latest ready
///   snapshot differs from the newest local upload job, or the sandbox
///   transitioned after that job was staged (a later suspend may have
///   no job at all).
/// - `retry_backoff` — a durable proof skip or candidate-specific failure
///   is still within its local retry backoff, so no cluster RPC was made
///   for that candidate.
/// - `not_suspended_at_execute` — the sandbox left `suspended` (or its
///   pin changed from the proven identity) between planning and the
///   eviction claim (e.g. a resume raced us).
/// - `local_read_error` — the node-local SQLite read of one candidate failed;
///   that row is backed off while the scan continues.
/// - `rpc_error` — a candidate-specific cluster response failed or was
///   malformed; that row is backed off while the scan continues.
/// - `rpc_transport_error` — the cluster dependency is unavailable; the scan
///   stops and retries from the same cursor next tick.
pub fn skipped(reason: &str) {
    counter("sandbox.evict.skipped", 1, &[tag("reason", reason)]);
}

/// One per eviction that was claimed but whose local cleanup failed. The
/// local row is left in `deleting` and the standard cleanup machinery
/// retries; the matching `local_eviction_failed` sandbox event carries the
/// error.
pub fn failed() {
    counter("sandbox.evict.failed", 1, &[]);
}

/// Cold legacy rows without any local upload generation selected for the
/// repair pass. These rows are not evicted directly; the repair pass queues a
/// fresh upload so a later tick can prove the exact local bytes.
pub fn legacy_repair_candidates(count: u64) {
    gauge("sandbox.evict.legacy_repair.candidates", count as f64, &[]);
}

/// A legacy repair upload was queued on the durable snapshot-upload worker.
pub fn legacy_repair_queued() {
    counter("sandbox.evict.legacy_repair.queued", 1, &[]);
}

/// A legacy repair candidate was declined without queueing a reupload.
/// `reason` is a bounded skip vocabulary owned by sbx-mgr.
pub fn legacy_repair_skipped(reason: &str) {
    counter(
        "sandbox.evict.legacy_repair.skipped",
        1,
        &[tag("reason", reason)],
    );
}

/// A legacy repair attempt hit a retryable failure after selection.
pub fn legacy_repair_failed(reason: &str) {
    counter(
        "sandbox.evict.legacy_repair.failed",
        1,
        &[tag("reason", reason)],
    );
}

/// **Logical** bytes released by one eviction: a recursive file-size
/// walk of the sandbox cache dir before deletion. Named logical
/// deliberately — btrfs CoW sharing with template subvolumes means the
/// physically reclaimed SSD space can be much lower. For physical reclaim,
/// watch `sandbox.evict.disk_usage_fraction` move between ticks.
pub fn logical_bytes_freed(reason: &str, bytes: u64) {
    counter(
        "sandbox.evict.logical_bytes_freed",
        bytes,
        &[tag("reason", reason)],
    );
}

/// `/space/cache` filesystem occupancy (`used/total` from statvfs)
/// observed at the start of each tick. Plot against the configured
/// engage/target fractions to see where each node sits on the
/// reclaim ladder.
pub fn disk_usage_fraction(fraction: f64) {
    gauge("sandbox.evict.disk_usage_fraction", fraction, &[]);
}

/// Local `sandboxes` rows on the node at the start of each tick, whatever
/// their state. node-ctl's full census publishes one entry per row in a
/// single `PublishNodeState`, so the census size grows with this count and
/// it flags nodes approaching cluster-api's decode limit. Every tick of an
/// enabled loop emits it once its row counts succeed.
pub fn resident_rows(rows: u64) {
    gauge("sandbox.evict.resident_rows", rows as f64, &[]);
}
