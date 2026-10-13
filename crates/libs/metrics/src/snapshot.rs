//! Metric helpers for the suspend-snapshot create and restore lifecycles.
//!
//! Names are emitted under
//! `snapshot.create.*`, `snapshot.restore.*`, and `snapshot.fork_copy.*`.
//!
//! ## Codec dimension
//!
//! Every metric here carries a `codec:` tag whose vocabulary mirrors the
//! suspend-snapshot rootfs codec used for the operation:
//!
//! - `codec:full` — the historical Full codec; rootfs uploaded/downloaded
//!   as a complete `rootfs.ext4` image. Used unconditionally by the API
//!   substrate's SuspendSandbox path; selectable via
//!   `--format=full` on `space-sbxmgr suspend`.
//! - `codec:btrfs_send_v1` — the btrfs-send codec; rootfs is uploaded as
//!   an incremental `btrfs send -p` stream against the pinned template
//!   subvolume. Per-request opt-in via `--format=btrfs-send`; not yet
//!   emitted by the substrate.
//!
//! ## Encryption dimension
//!
//! Every metric here also carries `encrypted:true|false`, sourced from the
//! sandbox's immutable `volume_encryption` stamp rather than from any
//! runtime detail, so a suspend and its later restore always agree. It is
//! what makes "how many encrypted suspends/restores happened, and do they
//! fail or take longer than plaintext ones" answerable at all — without it
//! BYOK traffic is indistinguishable in the lifecycle metrics.
//!
//! Callers pick the codec value once at the start of the operation
//! ([`codec_tag_from_diff_format`] for the restore side, which derives
//! it from the sidecar's `rootfs_diff_format` field), then thread that
//! string through every metric call so a single suspend or restore
//! emits a consistent codec dimension across its `started`, per-step
//! `step.duration_ms`, terminal `completed` / `failed`, and per-artifact
//! `bytes` records.

use std::time::Instant;

use crate::emit::{counter, elapsed_ms, gauge, tag, timing_ms};

/// A fresh submission attempt, not an observation or an individual vol-mgr retry.
pub fn backup_redrive() {
    counter("snapshot.backup.redrive", 1, &[]);
}

pub fn delivery_duration_ms(
    stage: &str,
    origin: &str,
    artifact: Option<&str>,
    result: &str,
    duration_ms: f64,
) {
    let mut tags = vec![
        tag("stage", stage),
        tag("origin", origin),
        tag("result", result),
    ];
    if let Some(artifact) = artifact {
        tags.push(tag("artifact", artifact));
    }
    timing_ms("snapshot.delivery.duration_ms", duration_ms.max(0.0), &tags);
}

/// Tag value used for the historical full-rootfs codec. Emitted with
/// no `rootfs_diff_format` key in the snapshot sidecar.
pub const CODEC_FULL: &str = "full";

/// Tag value for the btrfs-send-v1 codec — sidecar's
/// `rootfs_diff_format = "btrfs-send-v1"`. Note the underscore for the
/// metric tag (Datadog convention) versus the hyphen in the wire
/// vocabulary.
pub const CODEC_BTRFS_SEND_V1: &str = "btrfs_send_v1";

/// Map a snapshot sidecar's optional `rootfs_diff_format` field to the
/// `codec:` tag value used in metrics. The sidecar omits the field for
/// Full snapshots and writes the codec name for diff formats; this
/// helper normalizes both into the closed vocabulary
/// ([`CODEC_FULL`], [`CODEC_BTRFS_SEND_V1`]).
///
/// Unrecognized values fall through to [`CODEC_FULL`] so callers can't
/// accidentally emit a high-cardinality tag if the sidecar drifts;
/// callers that need to detect this case should validate the codec
/// independently before emitting metrics.
pub fn codec_tag_from_diff_format(diff_format: Option<&str>) -> &'static str {
    match diff_format {
        Some("btrfs-send-v1") => CODEC_BTRFS_SEND_V1,
        _ => CODEC_FULL,
    }
}

fn create_kind_tag(kind: &str) -> String {
    tag("kind", kind)
}

fn restore_kind_tag(kind: &str) -> String {
    tag("kind", kind)
}

fn codec_tag(codec: &str) -> String {
    tag("codec", codec)
}

/// `encrypted:true|false` from the sandbox's immutable `volume_encryption`
/// stamp. Threaded through every metric in an operation the same way `codec`
/// is, so one suspend or restore emits a consistent dimension across its
/// `started`, per-step, terminal, and per-artifact records.
fn encrypted_tag(encrypted: bool) -> String {
    tag("encrypted", if encrypted { "true" } else { "false" })
}

/// Emit at the start of a create flow. Pair with exactly one of
/// [`create_completed`] or [`create_failed`].
pub fn create_started(kind: &str, codec: &str, encrypted: bool) {
    counter(
        "snapshot.create.started",
        1,
        &[
            create_kind_tag(kind),
            codec_tag(codec),
            encrypted_tag(encrypted),
        ],
    );
}

/// Emit at the end of a named step within create. See the catalog for the
/// step vocabulary (`pause`, `sync_guest`, `fc_create`, `upload_queue`,
/// `cleanup`).
pub fn create_step(kind: &str, codec: &str, encrypted: bool, step: &str, started_at: Instant) {
    let tags = [
        create_kind_tag(kind),
        codec_tag(codec),
        encrypted_tag(encrypted),
        tag("step", step),
    ];
    timing_ms(
        "snapshot.create.step.duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Emit on successful completion of the create flow.
pub fn create_completed(kind: &str, codec: &str, encrypted: bool, started_at: Instant) {
    let tags = [
        create_kind_tag(kind),
        codec_tag(codec),
        encrypted_tag(encrypted),
        tag("result", "success"),
    ];
    counter("snapshot.create.completed", 1, &tags);
    timing_ms("snapshot.create.duration_ms", elapsed_ms(started_at), &tags);
}

pub fn fork_copy_queue_depth(state: &str, depth: usize) {
    gauge(
        "snapshot.fork_copy.queue_depth",
        depth as f64,
        &[tag("state", state)],
    );
}

pub fn fork_copy_queue_wait(started_at: Instant) {
    timing_ms(
        "snapshot.fork_copy.queue_wait_ms",
        elapsed_ms(started_at),
        &[],
    );
}

pub fn fork_copy_transfer(operation: &str, outcome: &str, started_at: Instant) {
    timing_ms(
        "snapshot.fork_copy.transfer_duration_ms",
        elapsed_ms(started_at),
        &[tag("operation", operation), tag("outcome", outcome)],
    );
}

/// Emit when the create flow fails. `step` shares the vocabulary of
/// [`create_step`]; `reason` is a short low-cardinality label.
pub fn create_failed(
    kind: &str,
    codec: &str,
    encrypted: bool,
    step: &str,
    reason: &str,
    started_at: Instant,
) {
    let tags = [
        create_kind_tag(kind),
        codec_tag(codec),
        encrypted_tag(encrypted),
        tag("step", step),
        tag("reason", reason),
    ];
    counter("snapshot.create.failed", 1, &tags);
    timing_ms(
        "snapshot.create.duration_ms",
        elapsed_ms(started_at),
        &[
            create_kind_tag(kind),
            codec_tag(codec),
            encrypted_tag(encrypted),
            tag("result", "failure"),
        ],
    );
}

/// Bytes uploaded to S3 per artifact (`vmstate`, `memory`, `rootfs`,
/// `rootfs.btrfs-send`, `metadata`). The artifact set differs per
/// codec — Full produces `rootfs`, btrfs-send produces
/// `rootfs.btrfs-send` — so dashboards comparing per-codec bytes
/// should split on both `artifact:` and `codec:` to avoid pretending
/// they're the same series.
pub fn create_bytes(kind: &str, codec: &str, encrypted: bool, artifact: &str, bytes: u64) {
    let tags = [
        create_kind_tag(kind),
        codec_tag(codec),
        encrypted_tag(encrypted),
        tag("artifact", artifact),
    ];
    counter("snapshot.create.bytes", bytes, &tags);
}

/// Startup recovery outcome for an interrupted snapshot-create publication.
pub fn create_recovery(kind: &str, outcome: &str) {
    counter(
        "snapshot.create.recovery",
        1,
        &[create_kind_tag(kind), tag("outcome", outcome)],
    );
}

/// A checkpoint restore retry found cleanup intent owned by another operation.
pub fn restore_retry_preserved_cleanup_intent() {
    counter(
        "snapshot.restore.self_heal",
        1,
        &[
            tag("path", "restoring_retry"),
            tag("outcome", "retirement_preserved_intent"),
        ],
    );
}

/// One uploaded rootfs diff stream (`rootfs.btrfs-send`) reclaimed from
/// the published snapshot directory after its backup finalized ready.
/// `bytes` is the logical file size at unlink time (CoW sharing makes
/// the physical reclaim lower).
pub fn stream_reclaimed(bytes: u64) {
    counter("snapshot.stream_reclaim.reclaimed", 1, &[]);
    counter("snapshot.stream_reclaim.bytes_freed", bytes, &[]);
}

/// One post-backup stream reclaim that did not delete anything. The
/// `reason` enumerates the closed set of skip labels:
///
/// - `not_pinned_suspended` — the sandbox row is no longer `suspended`
///   with the uploaded artifact pinned (resumed, re-suspended, or
///   deleted since the upload), so the file on disk is not provably the
///   uploaded bytes.
/// - `missing` — the stream file was already gone (prior reclaim or
///   manual cleanup); benign.
/// - `io_error` — the unlink itself failed; retried implicitly by the
///   next backup of the sandbox.
/// - `db_error` — the node-local post-finalize bookkeeping (marker
///   stamp or gate read) failed before the unlink could be evaluated.
/// - `admission_refused` — local writer admission was denied;
///   durable completed-job evidence retains the marker repair obligation.
pub fn stream_reclaim_skipped(reason: &str) {
    counter(
        "snapshot.stream_reclaim.skipped",
        1,
        &[tag("reason", reason)],
    );
}

/// One node-manager inspection of a retained btrfs send source. `outcome` is
/// `present`, `absent`, `invalid`, or `unavailable`. Unavailable outcomes carry
/// the gRPC `code`; a persistent `unimplemented` means an older node-manager,
/// and `permission_denied` a socket identity mismatch.
pub fn retained_source_inspection(outcome: &str, code: Option<&str>) {
    let mut tags = vec![tag("outcome", outcome)];
    if let Some(code) = code {
        tags.push(tag("code", code));
    }
    counter("snapshot.retained_source.inspection", 1, &tags);
}

/// Retained sources that periodic recovery still cannot resolve after a pass,
/// and the age of the oldest. Their artifacts are preserved meanwhile.
pub fn retained_source_pending(count: usize, oldest_age_seconds: f64) {
    gauge("snapshot.retained_source.pending", count as f64, &[]);
    gauge(
        "snapshot.retained_source.pending_oldest_age_seconds",
        oldest_age_seconds,
        &[],
    );
}

/// Unidentifiable completed-upload evidence observed by a bounded repair pass.
/// Reasons are `malformed_payload` and `missing_artifact_id`. Repeated sweeps
/// count a retained row again; this is neither unique jobs nor total backlog.
pub fn backup_marker_repair_unidentifiable(reason: &str, count: u64) {
    counter(
        "snapshot.backup_marker_repair.unidentifiable",
        count,
        &[tag("reason", reason)],
    );
}

/// Emit at the start of a restore flow. Pair with exactly one of
/// [`restore_completed`] or [`restore_failed`].
pub fn restore_started(kind: &str, codec: &str, encrypted: bool) {
    counter(
        "snapshot.restore.started",
        1,
        &[
            restore_kind_tag(kind),
            codec_tag(codec),
            encrypted_tag(encrypted),
        ],
    );
}

/// Emit at the end of a named step within restore. See the catalog for the
/// step vocabulary.
pub fn restore_step(kind: &str, codec: &str, encrypted: bool, step: &str, started_at: Instant) {
    let tags = [
        restore_kind_tag(kind),
        codec_tag(codec),
        encrypted_tag(encrypted),
        tag("step", step),
    ];
    timing_ms(
        "snapshot.restore.step.duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Emit on successful completion of the restore flow.
pub fn restore_completed(kind: &str, codec: &str, encrypted: bool, started_at: Instant) {
    let tags = [
        restore_kind_tag(kind),
        codec_tag(codec),
        encrypted_tag(encrypted),
        tag("result", "success"),
    ];
    counter("snapshot.restore.completed", 1, &tags);
    timing_ms(
        "snapshot.restore.duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Emit when the restore flow fails. `step` shares the vocabulary of
/// [`restore_step`]; `reason` is a short low-cardinality label.
pub fn restore_failed(
    kind: &str,
    codec: &str,
    encrypted: bool,
    step: &str,
    reason: &str,
    started_at: Instant,
) {
    let tags = [
        restore_kind_tag(kind),
        codec_tag(codec),
        encrypted_tag(encrypted),
        tag("step", step),
        tag("reason", reason),
    ];
    counter("snapshot.restore.failed", 1, &tags);
    timing_ms(
        "snapshot.restore.duration_ms",
        elapsed_ms(started_at),
        &[
            restore_kind_tag(kind),
            codec_tag(codec),
            encrypted_tag(encrypted),
            tag("result", "failure"),
        ],
    );
}

// ── On-node snapshot reaper (LRU GC for /space/cache local artifacts) ──
//
// Mirrors the `template.gc.*` shape so dashboards can compare
// template eviction against snapshot eviction side-by-side.

/// One per tick of the sandbox-manager snapshot LRU loop. Emits the
/// `snapshot.gc.tick` counter and the `snapshot.gc.tick_duration_ms`
/// timing under a single `result:ok|error` tag so dashboards stay in
/// sync — alert on absence of `snapshot.gc.tick{result:ok}` to detect
/// a stuck loop.
pub fn gc_tick(result: &str, started_at: Instant) {
    let tags = [tag("result", result)];
    counter("snapshot.gc.tick", 1, &tags);
    timing_ms(
        "snapshot.gc.tick_duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Per-engaged-tick work metrics for the snapshot reaper. These are
/// emitted only when the disk gate opens and the planner runs; below
/// the engage fraction the loop intentionally skips the directory-size
/// walk, so emitting zeros would falsely imply that no local artifacts
/// exist. Bytes before covers only directories sampled from the current
/// bounded artifact page, never the total cache footprint or backlog.
pub fn gc_tick_work(sampled_bytes_before: u64, bytes_freed: u64, candidates: u64) {
    gauge("snapshot.gc.bytes_before", sampled_bytes_before as f64, &[]);
    counter("snapshot.gc.bytes_freed", bytes_freed, &[]);
    counter("snapshot.gc.candidates", candidates, &[]);
}

/// `/space/cache` filesystem occupancy (`used/total` from statvfs)
/// observed at the start of each reaper tick. Plot against the
/// configured engage/target fractions to see where each node sits on
/// the reclaim ladder.
pub fn gc_disk_usage_fraction(fraction: f64) {
    gauge("snapshot.gc.disk_usage_fraction", fraction, &[]);
}

/// One per local artifact deleted by the reaper. Tagged by origin and
/// kind so dashboards can split "stop disk artifacts" from "suspend
/// full snapshots" from "named user snapshots". The sandbox id is
/// intentionally NOT a tag (unbounded cardinality) but is logged on
/// the same path.
pub fn gc_deleted(origin: &str, kind: &str) {
    counter(
        "snapshot.gc.deleted",
        1,
        &[tag("origin", origin), tag("kind", kind)],
    );
}

/// One per candidate the reaper skipped at execute time. The
/// `reason` enumerates the closed set of skip labels:
///
/// - `pinned_current_at_execute` — `sandboxes.current_local_artifact_id`
///   started pointing at the candidate between plan and execute.
/// - `not_idle_at_execute` — `last_touched_at` was bumped between
///   plan and execute (an in-flight restore touched the artifact).
/// - `shared_local_path` — more than one `local_artifacts` row
///   points at the candidate's `local_path`; acting would corrupt
///   the live sibling's bytes, so the delete checkout refuses.
/// - `not_ready` — the row is in `restoring` or `deleting`; another
///   operation holds the lease. Retried next tick.
/// - `row_missing` — the local_artifacts row vanished between plan
///   and execute (concurrent delete path raced us).
/// - `error` — the disk delete or the finalize transaction failed.
///   The artifact stays on disk for the next tick.
pub fn gc_skipped(reason: &str) {
    counter("snapshot.gc.skipped", 1, &[tag("reason", reason)]);
}
