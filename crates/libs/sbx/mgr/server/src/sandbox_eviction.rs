//! Cold-sandbox eviction: free `/space/cache` held by parked suspended
//! sandboxes whose state is durably restorable from a ready remote
//! snapshot.
//!
//! The snapshot reaper (`crate::snapshot_gc`) only deletes unpinned
//! artifact residue; a suspended sandbox resident on the node pins its
//! current suspend artifact and keeps its whole cache directory —
//! `instance/` rootfs subvolume included — even after a restore elsewhere.
//! On long-lived nodes that population dominates
//! disk usage. This loop removes the **entire local presence** of such a
//! sandbox (cache dir + node-local DB rows) when it is safe:
//!
//! 1. **Suspended and past the configured minimum idle.** Local
//!    `sandboxes.state = 'suspended'` and `updated_at` older than
//!    `SPACE_SBX_EVICT_MIN_IDLE_SECS` (default 25 hours). The default
//!    preserves local wake-cache across a daily recurrence plus scheduling
//!    jitter; 25 hours is a recommended floor, not a clamp, so operators
//!    may set a lower value (down to 0) when aggressive reclamation is
//!    needed. Disk pressure determines the effective retention window above
//!    that minimum. The local pre-filter is conservative and the cluster
//!    check below is decisive.
//! 2. **Local proof seed and no in-flight upload.** The row has either
//!    an immutable current-artifact pin or a legacy upload generation to
//!    compare against the cluster. Rows with neither are repaired first:
//!    while the disk gate is engaged, a bounded repair pass queues a fresh
//!    reupload of the local suspended bytes. Once that durable upload
//!    completes, the sandbox becomes a normal pinned candidate. No
//!    pending/running `snapshot_upload` job for the sandbox — deleting
//!    local files mid-upload would corrupt the backup being created.
//! 3. **Provably backed — proof tied to the exact bytes.** Two proof
//!    shapes, selected by whether the sandbox pins a current artifact:
//!
//!    **Pinned rows (identity proof).** The pin
//!    (`sandboxes.current_local_artifact_id`) immutably names the bytes
//!    on disk: it is written in the same transaction as the suspend and
//!    every re-suspend mints a new artifact id. The artifact is proven
//!    backed by its `backup_completed_at` marker (stamped by the upload
//!    worker for exactly this id, only after the cluster confirmed
//!    `FinalizeArtifactBackup(READY)`, which requires every per-file
//!    transfer to have completed) — or, when the marker is absent (e.g.
//!    the bytes were restored *from* S3 and never re-uploaded), by a
//!    `GetArtifactBackupLocation` readiness query keyed on the exact artifact
//!    id. Job journals and timestamps are never consulted for pinned
//!    rows — they are not evidence about these bytes.
//!
//!    **Unpinned rows (legacy, equality proof + recency witness).**
//!    Pre-artifact-tracking sandboxes have no pin, so the best identity
//!    is the newest `snapshot_upload` job's `payload.snapshot_id`.
//!    Evictable only when that **equals** the cluster's latest ready
//!    snapshot (confirmed by a readiness query) AND the sandbox's last
//!    state transition is not later than that job's creation (plus
//!    slack) — proving no later suspend exists whose backup enqueue
//!    silently failed. UUIDv7 *ordering* is never eviction evidence:
//!    `cluster < local` means the newest local state has no ready
//!    backup (failed upload — evicting would roll the sandbox back),
//!    and `cluster > local` cannot prove the local bytes are not newer
//!    still.
//!
//! 4. **Proof re-asserted at delete time.** The eviction claim CAS
//!    (`db::claim_suspended_sandbox_for_eviction`) requires the pin to
//!    still equal the proven identity (or still be NULL for the legacy
//!    class), so a resume + re-suspend racing the decision can never
//!    get its fresh, unproven bytes deleted.
//!
//! ## Why no cluster-side write
//!
//! Postgres tracks per-node presence only via the placement pointers on
//! the `sandboxes` row. An evicted sandbox's later resume routed to this
//! node finds no local copy and restores from the remote snapshot via
//! the orphan-adopt path (the same path cross-node restores use). The
//! node's inventory facet sync reports the sandbox as absent after the
//! local delete; the cluster keeps the `sandboxes` row `suspended` and
//! restorable.
//!
//! ## Race protection
//!
//! Eviction claims the local row with a CAS `suspended → deleting`
//! (`db::claim_suspended_sandbox_for_eviction`); a concurrent resume that
//! already flipped the local state wins and the candidate is skipped. A
//! resume that arrives *after* the claim is rejected by the lifecycle
//! state machine like any other operation on a deleting sandbox, and its
//! retry restores from the remote snapshot. The actual teardown is the
//! standard `CleanupCompletion::DeleteRow` machinery, which treats the
//! `deleting` row as a current cleanup — a crash mid-eviction therefore
//! converges through the existing cleanup recovery paths.

use pplx_space_common_core::types::{LocalArtifactId, SnapshotId};

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::db::Connection;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

pub(crate) use crate::auto_lifecycle::now_secs;
use crate::db::eviction_scan::{count_malformed_suspended_updated_at, count_resident_rows};
use crate::service::SandboxManagerService;

/// Runtime configuration for the eviction loop. Loaded from env on
/// startup; disabled unless `SPACE_SBX_EVICT_ENABLED` is `1`/`true`.
///
/// The disk-fraction gate (see [`crate::disk_usage`]) decides *when*
/// eviction acts — trading away wake-cache is a capacity decision, so
/// the loop engages when `/space/cache` usage reaches `engage_fraction`
/// and keeps evicting until usage falls below `target_fraction`.
/// `engage_fraction = 0` makes it always-on. The *what* is decided by
/// the proof rules in [`decide`] regardless of disk state.
#[derive(Debug, Clone, Copy)]
pub struct EvictionConfig {
    pub enabled: bool,
    pub engage_fraction: f64,
    pub target_fraction: f64,
    pub min_idle: Duration,
    pub interval: Duration,
    pub max_per_tick: usize,
    pub reupload_max_per_tick: usize,
}

impl EvictionConfig {
    const ENV_ENABLED: &'static str = "SPACE_SBX_EVICT_ENABLED";
    const ENV_ENGAGE_FRACTION: &'static str = "SPACE_SBX_EVICT_ENGAGE_FRACTION";
    const ENV_TARGET_FRACTION: &'static str = "SPACE_SBX_EVICT_TARGET_FRACTION";
    const ENV_MIN_IDLE_SECS: &'static str = "SPACE_SBX_EVICT_MIN_IDLE_SECS";
    const ENV_INTERVAL_SECS: &'static str = "SPACE_SBX_EVICT_INTERVAL_SECS";
    const ENV_REUPLOAD_MAX_PER_TICK: &'static str = "SPACE_SBX_EVICT_REUPLOAD_MAX_PER_TICK";

    /// Cold-sandbox wake-cache is the cheapest thing on the node to
    /// lose once pressure remains after artifact GC, so this loop
    /// engages after the artifact reaper (0.50) and drains to the
    /// desired sandbox-retention target.
    const DEFAULT_ENGAGE_FRACTION: f64 = 0.75;
    const DEFAULT_TARGET_FRACTION: f64 = 0.70;
    /// Below this the loop still honors an operator-set
    /// `SPACE_SBX_EVICT_MIN_IDLE_SECS`, but logs a warning at startup:
    /// evicting a sandbox idle for less than roughly a day risks
    /// reclaiming wake-cache that a daily recurrence would have resumed.
    /// This is a guidance threshold, not a clamp — operators who need
    /// aggressive reclamation (e.g. relieving a wedged worker) may set any
    /// value, including 0.
    const RECOMMENDED_MIN_IDLE_SECS: u64 = 25 * 60 * 60;
    const DEFAULT_MIN_IDLE_SECS: u64 = Self::RECOMMENDED_MIN_IDLE_SECS;
    const RETENTION_WARNING_SECS: u64 = 48 * 60 * 60;
    const DEFAULT_INTERVAL_SECS: u64 = 600;
    /// Per-tick candidate cap bounds cluster-api call volume and therefore
    /// also upper-bounds node-mgr cleanup load. The cursor prevents skipped
    /// candidates from permanently consuming the window.
    const DEFAULT_MAX_PER_TICK: usize = 100;
    const ENV_MAX_PER_TICK: &'static str = "SPACE_SBX_EVICT_MAX_PER_TICK";

    /// Whether an operator-configured min-idle is below the recommended
    /// safety floor. The value is honored regardless; this only gates the
    /// startup warning.
    fn is_below_recommended_min_idle(secs: u64) -> bool {
        secs < Self::RECOMMENDED_MIN_IDLE_SECS
    }

    pub fn from_env() -> Self {
        let enabled = std::env::var(Self::ENV_ENABLED)
            .map(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "True"))
            .unwrap_or(false);
        let engage = std::env::var(Self::ENV_ENGAGE_FRACTION)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(Self::DEFAULT_ENGAGE_FRACTION);
        let target = std::env::var(Self::ENV_TARGET_FRACTION)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(Self::DEFAULT_TARGET_FRACTION);
        let (engage_fraction, target_fraction) = crate::disk_usage::validate_fraction_pair(
            "sandbox_eviction",
            engage,
            target,
            Self::ENV_ENGAGE_FRACTION,
            Self::ENV_TARGET_FRACTION,
        );
        let min_idle_secs = std::env::var(Self::ENV_MIN_IDLE_SECS)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(Self::DEFAULT_MIN_IDLE_SECS);
        if Self::is_below_recommended_min_idle(min_idle_secs) {
            warn!(
                env_var = Self::ENV_MIN_IDLE_SECS,
                configured_secs = min_idle_secs,
                recommended_secs = Self::RECOMMENDED_MIN_IDLE_SECS,
                "sandbox eviction minimum idle is below the recommended safety floor; \
                 honoring the configured value — recently-idle sandboxes may be evicted",
            );
        }
        let interval_secs = std::env::var(Self::ENV_INTERVAL_SECS)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(Self::DEFAULT_INTERVAL_SECS);
        let max_per_tick = std::env::var(Self::ENV_MAX_PER_TICK)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(Self::DEFAULT_MAX_PER_TICK);
        let reupload_default = max_per_tick.min(5);
        let reupload_max_per_tick = std::env::var(Self::ENV_REUPLOAD_MAX_PER_TICK)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(reupload_default);
        Self {
            enabled,
            engage_fraction,
            target_fraction,
            min_idle: Duration::from_secs(min_idle_secs),
            interval: Duration::from_secs(interval_secs),
            max_per_tick,
            reupload_max_per_tick,
        }
    }
}

/// Why a candidate is evictable. Doubles as the `reason` tag on
/// `sandbox.evict.evicted` and the sandbox event detail. There is
/// deliberately only one reason: every eviction must carry a proof
/// tied to the exact bytes being deleted ("a newer generation exists
/// elsewhere" is not such a proof and was removed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictReason {
    /// The bytes on disk are provably backed: the pinned artifact's
    /// durability was confirmed for its exact immutable id, or (legacy
    /// rows) the newest local generation is exactly the cluster's
    /// latest ready snapshot, and the sandbox is past the minimum age
    /// floor.
    LatestBackedIdle,
    /// The cluster authoritatively reported that no `sandboxes` row
    /// exists for this id (rows are hard-deleted on delete). No backup
    /// proof applies: with the row gone the sandbox can never be
    /// restored, so its local presence is unreclaimable dead weight
    /// regardless of backup state. Driven by node-ctl's desired-state
    /// facet sync, not by this loop.
    ClusterRowAbsent,
}

impl EvictReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            EvictReason::LatestBackedIdle => "latest_backed_idle",
            EvictReason::ClusterRowAbsent => "cluster_row_absent",
        }
    }
}

/// Why a candidate was declined. Doubles as the `reason` tag on
/// `sandbox.evict.skipped` — keep in sync with the metric catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    NoReadyBackup,
    LocalNewerThanBackup,
    UnknownLocalGeneration,
    /// The pinned current artifact's durability could not be proven:
    /// no local `backup_completed_at` marker and the exact-artifact
    /// cluster lookup found no ready backup (or the pin points at a
    /// missing artifact row). The bytes on disk may be the only copy.
    CurrentArtifactUnproven,
    /// Legacy (unpinned) row whose generation equality could not be
    /// established: the cluster's latest ready snapshot differs from
    /// the newest local upload job, or the sandbox transitioned after
    /// that job was staged (a later suspend may have no job at all).
    GenerationUnproven,
    RetryBackoff,
    NotSuspendedAtExecute,
    /// The node-local SQLite read of the decision inputs failed.
    /// Distinct from [`Self::RpcError`] so a flaky cluster-api and a
    /// node-local DB problem don't share a dashboard signal.
    LocalReadError,
    /// A candidate-specific cluster response could not be used. The row is
    /// backed off so malformed remote data cannot monopolize the scan.
    RpcError,
    /// The cluster dependency is unavailable for the whole scan. Retain the
    /// cursor and retry the same oldest candidate next tick.
    RpcTransportError,
}

impl SkipReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SkipReason::NoReadyBackup => "no_ready_backup",
            SkipReason::LocalNewerThanBackup => "local_newer_than_backup",
            SkipReason::UnknownLocalGeneration => "unknown_local_generation",
            SkipReason::CurrentArtifactUnproven => "current_artifact_unproven",
            SkipReason::GenerationUnproven => "generation_unproven",
            SkipReason::RetryBackoff => "retry_backoff",
            SkipReason::NotSuspendedAtExecute => "not_suspended_at_execute",
            SkipReason::LocalReadError => "local_read_error",
            SkipReason::RpcError => "rpc_error",
            SkipReason::RpcTransportError => "rpc_transport_error",
        }
    }

    fn should_back_off(self) -> bool {
        matches!(
            self,
            Self::NoReadyBackup
                | Self::LocalNewerThanBackup
                | Self::UnknownLocalGeneration
                | Self::CurrentArtifactUnproven
                | Self::GenerationUnproven
                | Self::LocalReadError
                | Self::RpcError
        )
    }

    fn halts_scan(self) -> bool {
        matches!(self, Self::RpcTransportError)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyRepairSkipReason {
    MissingLocalFiles,
    NoUploadConfig,
    StateChanged,
}

impl LegacyRepairSkipReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LegacyRepairSkipReason::MissingLocalFiles => "missing_local_files",
            LegacyRepairSkipReason::NoUploadConfig => "no_upload_config",
            LegacyRepairSkipReason::StateChanged => "state_changed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyRepairFailureReason {
    LocalRead,
    Rpc,
    Submission,
}

impl LegacyRepairFailureReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LegacyRepairFailureReason::LocalRead => "local_read_error",
            LegacyRepairFailureReason::Rpc => "rpc_error",
            LegacyRepairFailureReason::Submission => "submission_error",
        }
    }
}

/// What the node knows locally about the sandbox's on-disk bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalEvidence {
    /// `sandboxes.current_local_artifact_id` is set: the bytes on
    /// disk are exactly the named immutable artifact (the pin is
    /// written in the same transaction as the suspend, and every
    /// re-suspend mints a new id).
    Pinned(PinnedArtifact),
    /// The pin is set but the `local_artifacts` row is gone —
    /// inconsistent state; never evict.
    PinnedRowMissing,
    /// No pin (pre-artifact-tracking sandbox). The best identity is
    /// the newest `snapshot_upload` job this node staged.
    Unpinned {
        latest_job: Option<UploadGeneration>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PinnedArtifact {
    pub(crate) artifact_id: LocalArtifactId,
    pub(crate) artifact_kind: pplx_space_common_core::types::ArtifactKind,
    /// `backup_completed_at IS NOT NULL` — stamped by the upload
    /// worker for exactly this artifact id, only after the cluster
    /// confirmed `FinalizeArtifactBackup(READY)` (which requires
    /// every per-file transfer to have completed).
    pub(crate) backup_marker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UploadGeneration {
    pub(crate) snapshot_id: SnapshotId,
    /// When the job row was created, for the recency witness.
    pub(crate) created_at_secs: u64,
}

/// Pre-decision: what the pure local/cluster comparison can conclude
/// before any per-artifact cluster proof RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreDecision {
    Evict(EvictReason),
    /// Pinned artifact without a local backup marker: the loop must
    /// ask the cluster for a ready backup of the exact artifact id
    /// (`GetArtifactBackupLocation`) before evicting.
    ProvePinned,
    Skip(SkipReason),
}

/// Slack for the legacy recency witness: suspend enqueues its upload
/// immediately when enqueue works, so the sandbox's last state
/// transition landing more than this long after the newest upload job
/// means a later suspend may exist with no job at all — unprovable,
/// skip. Generous to absorb clock/queue jitter; over-skipping is
/// safe.
pub(crate) const RECENCY_WITNESS_SLACK_SECS: u64 = 3_600;

/// Pure eviction decision. Eviction requires a proof tied to the
/// exact bytes on disk:
///
/// - **Pinned rows** (identity proof): the pinned artifact id names
///   the bytes immutably. Local `backup_marker` set → proven. Marker
///   unset → [`PreDecision::ProvePinned`]: the loop confirms a ready
///   backup of exactly that id with the cluster (covers restored
///   copies whose local marker was never stamped). No comparison of
///   job journals or timestamps is consulted — they are not evidence
///   about these bytes.
/// - **Unpinned rows** (legacy, equality proof): evict only when the
///   newest local upload job's generation **equals** the cluster's
///   latest ready snapshot AND the recency witness holds
///   (`sandbox_updated_at_secs` not later than the job's creation
///   plus slack — proving no later suspend exists whose enqueue
///   silently failed). UUIDv7 *ordering* is never used as eviction
///   evidence; `cluster < local` is the failed-upload trap (local
///   bytes newer than any backup) and `cluster > local` cannot prove
///   the local bytes are not newer still.
pub(crate) fn decide(
    cluster_latest: Option<SnapshotId>,
    evidence: &LocalEvidence,
    sandbox_updated_at_secs: u64,
) -> PreDecision {
    match evidence {
        LocalEvidence::Pinned(pinned) if pinned.backup_marker => {
            PreDecision::Evict(EvictReason::LatestBackedIdle)
        }
        LocalEvidence::Pinned(_) => PreDecision::ProvePinned,
        LocalEvidence::PinnedRowMissing => PreDecision::Skip(SkipReason::CurrentArtifactUnproven),
        LocalEvidence::Unpinned { latest_job: None } => {
            PreDecision::Skip(SkipReason::UnknownLocalGeneration)
        }
        LocalEvidence::Unpinned {
            latest_job: Some(job),
        } => match cluster_latest.map(|cluster| cluster.cmp(&job.snapshot_id)) {
            None => PreDecision::Skip(SkipReason::NoReadyBackup),
            Some(std::cmp::Ordering::Less) => PreDecision::Skip(SkipReason::LocalNewerThanBackup),
            Some(std::cmp::Ordering::Greater) => PreDecision::Skip(SkipReason::GenerationUnproven),
            Some(std::cmp::Ordering::Equal) => {
                if sandbox_updated_at_secs
                    <= job
                        .created_at_secs
                        .saturating_add(RECENCY_WITNESS_SLACK_SECS)
                {
                    PreDecision::Evict(EvictReason::LatestBackedIdle)
                } else {
                    PreDecision::Skip(SkipReason::GenerationUnproven)
                }
            }
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateCursor {
    idle_witness_secs: u64,
    sandbox_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvictionCandidate {
    sandbox_id: String,
    cursor: CandidateCursor,
}

const CANDIDATE_SKIP_BACKOFF: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Default)]
struct CandidateScanState {
    cursor: Option<CandidateCursor>,
    skipped_until: HashMap<String, Instant>,
}

impl CandidateScanState {
    fn begin_tick(&mut self, now: Instant) {
        self.skipped_until.retain(|_, retry_at| *retry_at > now);
    }

    fn is_backed_off(&self, sandbox_id: &str) -> bool {
        self.skipped_until.contains_key(sandbox_id)
    }

    fn back_off(&mut self, sandbox_id: &str, now: Instant) {
        self.skipped_until
            .insert(sandbox_id.to_string(), now + CANDIDATE_SKIP_BACKOFF);
    }

    fn complete_candidate(&mut self, cursor: CandidateCursor) {
        self.cursor = Some(cursor);
    }

    fn reset_cursor(&mut self) {
        self.cursor = None;
    }
}

fn query_candidates_after(
    conn: &Connection,
    idle_cutoff_epoch_secs: u64,
    cursor: Option<&CandidateCursor>,
    limit: usize,
) -> crate::db::Result<Vec<EvictionCandidate>> {
    let cursor_witness =
        cursor.map(|cursor| i64::try_from(cursor.idle_witness_secs).unwrap_or(i64::MAX));
    let cursor_sandbox_id = cursor.map(|cursor| cursor.sandbox_id.as_str());
    let cutoff = i64::try_from(idle_cutoff_epoch_secs).unwrap_or(i64::MAX);
    let rows = crate::db::eviction_scan::idle_suspended_candidates(
        conn,
        cutoff,
        cursor_witness,
        cursor_sandbox_id,
        limit,
    )?;
    Ok(rows
        .into_iter()
        .map(|(sandbox_id, idle_witness_secs)| EvictionCandidate {
            cursor: CandidateCursor {
                idle_witness_secs,
                sandbox_id: sandbox_id.clone(),
            },
            sandbox_id,
        })
        .collect())
}

/// Cold suspended sandboxes eligible for the per-candidate cluster check:
/// suspended, idle past the cutoff, with no in-flight upload job, and
/// with a local proof seed. The keyset cursor advances across capped
/// ticks so permanently skipped old rows cannot monopolize every scan.
pub(crate) fn list_candidates(
    conn: &Connection,
    idle_cutoff_epoch_secs: u64,
    cursor: Option<&CandidateCursor>,
    limit: usize,
) -> crate::db::Result<Vec<EvictionCandidate>> {
    let candidates = query_candidates_after(conn, idle_cutoff_epoch_secs, cursor, limit)?;
    if candidates.is_empty() && cursor.is_some() {
        return query_candidates_after(conn, idle_cutoff_epoch_secs, None, limit);
    }
    Ok(candidates)
}

/// Cold legacy sandboxes whose local bytes have no generation proof yet.
/// The repair pass queues a fresh upload for these rows so a later eviction
/// tick can prove the exact artifact and delete normally. Callers fetch a
/// window wider than the per-tick attempt cap and pass over backed-off
/// rows, so an unrepairable head cannot starve the rows behind it.
pub(crate) fn list_legacy_reupload_candidates(
    conn: &Connection,
    idle_cutoff_epoch_secs: u64,
    limit: usize,
) -> crate::db::Result<Vec<String>> {
    crate::db::eviction_scan::legacy_reupload_candidates(conn, idle_cutoff_epoch_secs, limit)
}

/// Gather the local half of the eviction evidence plus the sandbox's
/// last-transition time (`updated_at`, epoch secs) for the recency
/// witness. The pin and its artifact row are read in one query so the
/// evidence is a consistent snapshot.
///
/// The artifact lookup is keyed on `(local_artifact_id, sandbox_id)`, not
/// on the id alone. A pin naming a row that belongs to a *different*
/// sandbox is an inconsistent state, and reading it by id alone would
/// return that sandbox's `backup_completed_at` — evicting these bytes on
/// somebody else's proof of durability. Requiring ownership turns the
/// mismatch into [`LocalEvidence::PinnedRowMissing`], which
/// [`decide`] already refuses to evict on.
pub(crate) fn gather_local_evidence(
    conn: &Connection,
    sandbox_id: &str,
) -> crate::db::Result<Option<(LocalEvidence, u64)>> {
    let row = crate::db::eviction_scan::artifact_pin_and_updated_at(conn, sandbox_id)?;
    let Some((pin, updated_at)) = row else {
        return Ok(None);
    };
    let updated_at_secs = updated_at.unsigned();

    if let Some(pin) = pin {
        let artifact = crate::db::eviction_scan::owned_artifact_evidence(conn, &pin, sandbox_id)?;
        let evidence = match artifact {
            Some((kind, id, backup_marker)) => {
                match (id.parse(), kind.parse()) {
                    (Ok(artifact_id), Ok(artifact_kind)) => LocalEvidence::Pinned(PinnedArtifact {
                        artifact_id,
                        artifact_kind,
                        backup_marker,
                    }),
                    // Unparseable id/kind: treat like a missing row —
                    // never evict what we can't identify.
                    _ => LocalEvidence::PinnedRowMissing,
                }
            }
            None => LocalEvidence::PinnedRowMissing,
        };
        return Ok(Some((evidence, updated_at_secs)));
    }

    // Unpinned: the newest upload job regardless of status (a failed
    // newest upload must still count — it proves newer-than-backed
    // local state exists).
    let job = crate::db::eviction_scan::latest_upload_job(conn, sandbox_id)?;
    let latest_job = job.and_then(|(snapshot_id, created_at)| {
        let snapshot_id = snapshot_id?.trim().parse::<SnapshotId>().ok()?;
        // Invalid time cannot prove recency, even when the sandbox itself
        // has a low but valid epoch timestamp.
        let created_at_secs = crate::db::timestamps::EpochSeconds::parse(&created_at)
            .ok()?
            .unsigned();
        Some(UploadGeneration {
            snapshot_id,
            created_at_secs,
        })
    });
    Ok(Some((
        LocalEvidence::Unpinned { latest_job },
        updated_at_secs,
    )))
}

#[derive(Debug, Default)]
struct LegacyRepairReport {
    candidates: usize,
    queued: usize,
    skipped: usize,
    failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyRepairOutcome {
    Queued,
    Skipped,
    Failed,
}

impl CandidateScanState {
    /// Queued: the pending upload excludes the row from candidate SQL,
    /// so drop any proof backoff and reconsider the row as soon as the
    /// upload completes. Skipped/Failed: back the row off, so a
    /// permanently-unrepairable head (e.g. a sandbox whose cluster row
    /// is gone) cannot re-consume the per-tick repair budget every tick
    /// and starve the repairable rows behind it. None (budget
    /// exhausted): leave any existing backoff untouched.
    fn apply_repair_outcome(&mut self, sandbox_id: &str, outcome: Option<LegacyRepairOutcome>) {
        match outcome {
            Some(LegacyRepairOutcome::Queued) => {
                self.skipped_until.remove(sandbox_id);
            }
            Some(LegacyRepairOutcome::Skipped | LegacyRepairOutcome::Failed) => {
                self.back_off(sandbox_id, Instant::now());
            }
            None => {}
        }
    }
}

impl LegacyRepairReport {
    fn attempts(&self) -> usize {
        self.queued
            .saturating_add(self.skipped)
            .saturating_add(self.failed)
    }
}

#[derive(Debug, Default)]
struct TickReport {
    candidates: usize,
    malformed_updated_at: usize,
    /// Local `sandboxes` rows at tick start.
    resident_rows: usize,
    evicted: usize,
    young_evictions: usize,
    youngest_eviction_age_secs: Option<u64>,
    skipped: usize,
    failed: usize,
    logical_bytes_freed: u64,
    legacy_repair: LegacyRepairReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFlow {
    Continue,
    Stop,
}

fn record_candidate_skip(
    report: &mut TickReport,
    scan_state: &mut CandidateScanState,
    sandbox_id: &str,
    cursor: CandidateCursor,
    reason: SkipReason,
    now: Instant,
) -> CandidateFlow {
    report.skipped += 1;
    pplx_space_metrics::sandbox_eviction::skipped(reason.as_str());
    if reason.should_back_off() {
        scan_state.back_off(sandbox_id, now);
    }
    if reason.halts_scan() {
        CandidateFlow::Stop
    } else {
        scan_state.complete_candidate(cursor);
        CandidateFlow::Continue
    }
}

fn record_continuing_candidate_skip(
    report: &mut TickReport,
    scan_state: &mut CandidateScanState,
    sandbox_id: &str,
    cursor: CandidateCursor,
    reason: SkipReason,
    now: Instant,
) {
    debug_assert!(!reason.halts_scan());
    let flow = record_candidate_skip(report, scan_state, sandbox_id, cursor, reason, now);
    debug_assert_eq!(flow, CandidateFlow::Continue);
}

fn classify_rpc_failure(status: &tonic::Status) -> SkipReason {
    match status.code() {
        // Codes that mean the cluster dependency cannot serve any candidate
        // right now: transport-level failures, plus the codes cluster-api
        // only produces scan-wide — `Internal` is its Postgres-error
        // mapping, and `Unauthenticated`/`PermissionDenied` mean the
        // node's internal token is rejected for every request alike.
        // Backing off individual rows for these would burn the full
        // candidate window against a failing dependency and suppress the
        // oldest evictable rows long after it recovers.
        tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::DeadlineExceeded
        | tonic::Code::ResourceExhausted
        | tonic::Code::Unavailable
        | tonic::Code::Internal
        | tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied => SkipReason::RpcTransportError,
        _ => SkipReason::RpcError,
    }
}

/// Which proof dated an eviction's suspend time, for the
/// `evicted_suspend_age_secs` metric tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SuspendAgeProof {
    Pinned,
    LegacyUpload,
}

impl SuspendAgeProof {
    fn as_str(self) -> &'static str {
        match self {
            SuspendAgeProof::Pinned => "pinned",
            SuspendAgeProof::LegacyUpload => "legacy_upload",
        }
    }
}

impl std::fmt::Display for SuspendAgeProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// When this sandbox was last suspended, as witnessed by the eviction
/// proof itself: the pinned artifact id is a UUIDv7 minted at suspend
/// time, and legacy rows carry their newest upload job's creation
/// time. Independent of both `updated_at` (churned by periodic
/// writers) and the activity clocks (the thing being monitored), so
/// `evicted_suspend_age_secs` can expose activity clocks that stopped
/// advancing: those evictions would cluster near zero suspend age.
fn suspend_time_witness(evidence: &LocalEvidence) -> Option<(u64, SuspendAgeProof)> {
    match evidence {
        LocalEvidence::Pinned(pinned) => uuid_v7_epoch_secs(pinned.artifact_id.as_uuid())
            .map(|secs| (secs, SuspendAgeProof::Pinned)),
        LocalEvidence::Unpinned {
            latest_job: Some(job),
        } => Some((job.created_at_secs, SuspendAgeProof::LegacyUpload)),
        LocalEvidence::PinnedRowMissing | LocalEvidence::Unpinned { latest_job: None } => None,
    }
}

fn uuid_v7_epoch_secs(id: Uuid) -> Option<u64> {
    id.get_timestamp().map(|ts| ts.to_unix().0)
}

pub(crate) fn sandbox_cache_dir(sandbox_id: &str) -> String {
    format!(
        "{}/{sandbox_id}",
        pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR
    )
}

pub(crate) fn emit_eviction_event(
    sandbox_id: &str,
    event_type: pplx_space_events::SandboxEventType,
    details: pplx_space_events::SandboxEventDetails,
) {
    pplx_space_events::emit_sandbox_event(
        sandbox_id,
        event_type,
        pplx_space_events::SandboxEventSource::SbxManager,
        details.trigger(pplx_space_events::SandboxEventTrigger::System),
    );
}

/// Check the cluster for a ready backup of **exactly** the pinned
/// artifact id without reading its storage location. On success the
/// local `backup_completed_at` marker is stamped best-effort so the
/// next tick proves locally — this is the path restored-from-S3 copies
/// take, since restore never stamps the marker. Returns `Ok(true)` on
/// proof, `Ok(false)` when the cluster has no ready backup of that
/// artifact, or the classified RPC failure when proof cannot be obtained.
async fn prove_pinned_artifact(
    cluster: &mut pplx_space_node_api_client::cluster::AuthenticatedClusterApiClient,
    service: &SandboxManagerService,
    sandbox_id: &str,
    pinned: &PinnedArtifact,
) -> Result<bool, SkipReason> {
    use pplx_space_node_api_client::proto::{
        LocalArtifactBackupIdentity, backup_readiness_query::Artifact,
    };
    let artifact = Artifact::LocalArtifact(LocalArtifactBackupIdentity {
        local_artifact_id: pinned.artifact_id.to_string(),
        artifact_kind: match pinned.artifact_kind {
            pplx_space_common_core::types::ArtifactKind::Disk => {
                pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32
            }
            pplx_space_common_core::types::ArtifactKind::Full => {
                pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32
            }
        },
    });
    if !backup_ready(cluster, sandbox_id, artifact).await? {
        return Ok(false);
    }
    let service = service.clone();
    let artifact_id = pinned.artifact_id;
    let stamp = tokio::task::spawn_blocking(move || {
        let conn = service.db_conn().map_err(|error| error.to_string())?;
        crate::db::mark_local_artifact_backup_completed(&conn, &artifact_id.to_string())
            .map_err(|error| error.to_string())
    })
    .await;
    let stamp_error = match stamp {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(join_error) => Some(join_error.to_string()),
    };
    if let Some(error) = stamp_error {
        warn!(
            sandbox_id,
            local_artifact_id = %pinned.artifact_id,
            error,
            "sandbox eviction: failed to stamp backup marker after exact-artifact proof; next tick re-proves via RPC",
        );
    }
    Ok(true)
}

/// The newest local upload generation of an unpinned cache, when the cluster
/// confirms it is still the latest ready suspend attempt. Pinned caches use
/// their own marker or exact-artifact proof and skip this query.
async fn ready_unpinned_suspend_generation(
    cluster: &mut pplx_space_node_api_client::cluster::AuthenticatedClusterApiClient,
    sandbox_id: &str,
    evidence: &LocalEvidence,
) -> Result<Option<SnapshotId>, SkipReason> {
    use pplx_space_node_api_client::proto::backup_readiness_query::Artifact;
    let LocalEvidence::Unpinned {
        latest_job: Some(job),
    } = evidence
    else {
        return Ok(None);
    };
    let artifact = Artifact::LatestReadySuspendSnapshotId(job.snapshot_id.to_string());
    let ready = backup_ready(cluster, sandbox_id, artifact).await?;
    Ok(ready.then_some(job.snapshot_id))
}

/// Ask the cluster whether an identity this node already holds is backed
/// up, without reading any storage location. The identity rides only in the
/// readiness query, so a server without readiness support rejects the empty
/// top-level artifact id instead of answering with a location. Only a bare
/// `ready` reply is evidence: an absent `ready` or any location payload is
/// unusable, and like an RPC failure it retains the cache.
async fn backup_ready(
    cluster: &mut pplx_space_node_api_client::cluster::AuthenticatedClusterApiClient,
    sandbox_id: &str,
    artifact: pplx_space_node_api_client::proto::backup_readiness_query::Artifact,
) -> Result<bool, SkipReason> {
    use pplx_space_node_api_client::proto::{
        BackupReadinessQuery, GetArtifactBackupLocationRequest, GetArtifactBackupLocationResponse,
    };
    let request = GetArtifactBackupLocationRequest {
        sandbox_id: sandbox_id.to_string(),
        readiness: Some(BackupReadinessQuery {
            artifact: Some(artifact),
        }),
        ..Default::default()
    };
    match cluster.get_artifact_backup_location(request).await {
        Ok(response) => {
            let response = response.into_inner();
            let bare = GetArtifactBackupLocationResponse {
                ready: response.ready,
                ..Default::default()
            };
            match response.ready {
                Some(ready) if response == bare => Ok(ready),
                _ => {
                    warn!(
                        sandbox_id,
                        "sandbox eviction: backup readiness reply is not a bare acknowledgement",
                    );
                    Err(SkipReason::RpcError)
                }
            }
        }
        Err(status) => {
            warn!(
                sandbox_id,
                code = %status.code(),
                message = %status.message(),
                "sandbox eviction: backup readiness query failed",
            );
            Err(classify_rpc_failure(&status))
        }
    }
}

fn classify_reupload_failure(
    status: &tonic::Status,
) -> Result<LegacyRepairSkipReason, LegacyRepairFailureReason> {
    match status.code() {
        tonic::Code::FailedPrecondition => {
            let message = status.message();
            if message.contains(
                pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER,
            ) {
                Ok(LegacyRepairSkipReason::MissingLocalFiles)
            } else if message.contains("no snapshot upload destination configured") {
                Ok(LegacyRepairSkipReason::NoUploadConfig)
            } else if message.contains("sandbox is ")
                || message.contains("no record of this sandbox")
            {
                Ok(LegacyRepairSkipReason::StateChanged)
            } else {
                Err(LegacyRepairFailureReason::Submission)
            }
        }
        tonic::Code::NotFound => Ok(LegacyRepairSkipReason::StateChanged),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            Err(LegacyRepairFailureReason::Rpc)
        }
        _ => Err(LegacyRepairFailureReason::Submission),
    }
}

fn record_reupload_failure(
    report: &mut LegacyRepairReport,
    sandbox_id: &str,
    status: &tonic::Status,
) -> LegacyRepairOutcome {
    match classify_reupload_failure(status) {
        Ok(reason) => {
            report.skipped += 1;
            pplx_space_metrics::sandbox_eviction::legacy_repair_skipped(reason.as_str());
            warn!(
                sandbox_id,
                code = %status.code(),
                message = %status.message(),
                reason = reason.as_str(),
                "sandbox eviction: legacy repair skipped",
            );
            LegacyRepairOutcome::Skipped
        }
        Err(reason) => {
            report.failed += 1;
            pplx_space_metrics::sandbox_eviction::legacy_repair_failed(reason.as_str());
            warn!(
                sandbox_id,
                code = %status.code(),
                message = %status.message(),
                reason = reason.as_str(),
                "sandbox eviction: legacy repair failed",
            );
            LegacyRepairOutcome::Failed
        }
    }
}

async fn enqueue_legacy_reupload(
    service: &SandboxManagerService,
    report: &mut LegacyRepairReport,
    sandbox_id: &str,
) -> LegacyRepairOutcome {
    match service
        .reupload_legacy_sandbox_for_eviction(sandbox_id)
        .await
    {
        Ok(job_id) => {
            report.queued += 1;
            pplx_space_metrics::sandbox_eviction::legacy_repair_queued();
            info!(
                sandbox_id,
                job_id = %job_id, "sandbox eviction: legacy repair reupload queued",
            );
            LegacyRepairOutcome::Queued
        }
        Err(status) => record_reupload_failure(report, sandbox_id, &status),
    }
}

/// How many of the oldest proof-less suspended rows the repair pass
/// reads this tick: the attempt budget plus every currently backed-off
/// row. At most `skipped_until.len()` fetched rows can be passed over,
/// so the fetch always reaches at least `attempt_cap` eligible rows
/// past a backed-off head, however large the head grows — a fixed
/// window would degenerate to an all-backed-off page and starve the
/// repairable rows behind it. The map may also hold rows outside this
/// scan's population (main-scan skips); the resulting over-fetch is a
/// slightly larger SQLite page, no RPCs.
fn repair_scan_window(scan_state: &CandidateScanState, attempt_cap: usize) -> usize {
    attempt_cap.saturating_add(scan_state.skipped_until.len())
}

/// Oldest-first repair attempts for this tick: backed-off rows are
/// passed over without consuming the attempt budget.
fn plan_repair_attempts(
    candidates: Vec<String>,
    scan_state: &CandidateScanState,
    attempt_cap: usize,
) -> Vec<String> {
    candidates
        .into_iter()
        .filter(|sandbox_id| !scan_state.is_backed_off(sandbox_id))
        .take(attempt_cap)
        .collect()
}

async fn repair_legacy_reupload_candidates(
    service: &SandboxManagerService,
    scan_state: &mut CandidateScanState,
    idle_cutoff_epoch_secs: u64,
    attempt_cap: usize,
) -> Result<LegacyRepairReport, String> {
    let scan_window = repair_scan_window(scan_state, attempt_cap);
    let service_for_list = service.clone();
    let candidates = tokio::task::spawn_blocking(move || {
        let conn = service_for_list
            .db_conn()
            .map_err(|error| error.to_string())?;
        list_legacy_reupload_candidates(&conn, idle_cutoff_epoch_secs, scan_window)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|e| {
        pplx_space_metrics::sandbox_eviction::legacy_repair_failed(
            LegacyRepairFailureReason::LocalRead.as_str(),
        );
        format!("list_legacy_reupload_candidates spawn_blocking: {e}")
    })?
    .map_err(|e| {
        pplx_space_metrics::sandbox_eviction::legacy_repair_failed(
            LegacyRepairFailureReason::LocalRead.as_str(),
        );
        format!("list_legacy_reupload_candidates: {e}")
    })?;

    let attempts = plan_repair_attempts(candidates, scan_state, attempt_cap);
    let mut report = LegacyRepairReport {
        candidates: attempts.len(),
        ..Default::default()
    };
    pplx_space_metrics::sandbox_eviction::legacy_repair_candidates(report.candidates as u64);

    for sandbox_id in attempts {
        let outcome = enqueue_legacy_reupload(service, &mut report, &sandbox_id).await;
        scan_state.apply_repair_outcome(&sandbox_id, Some(outcome));
    }

    Ok(report)
}

async fn evict_tick(
    service: &SandboxManagerService,
    cfg: EvictionConfig,
    disk: Option<crate::disk_usage::DiskUsage>,
    scan_state: &mut CandidateScanState,
) -> Result<TickReport, String> {
    let service_for_counts = service.clone();
    let (malformed_updated_at, resident_rows) = tokio::task::spawn_blocking(move || {
        let conn = service_for_counts
            .db_conn()
            .map_err(|error| format!("eviction row counts db_conn: {error}"))?;
        let malformed = count_malformed_suspended_updated_at(&conn)
            .map_err(|error| format!("count_malformed_suspended_updated_at: {error}"))?;
        let resident =
            count_resident_rows(&conn).map_err(|error| format!("count_resident_rows: {error}"))?;
        Ok::<_, String>((malformed, resident))
    })
    .await
    .map_err(|e| format!("eviction row counts spawn_blocking: {e}"))??;
    pplx_space_metrics::sandbox_eviction::malformed_updated_at(malformed_updated_at as u64);
    // Counted before the disk check so a tick that can't read disk usage
    // still reports the population.
    pplx_space_metrics::sandbox_eviction::resident_rows(resident_rows as u64);
    if malformed_updated_at > 0 {
        warn!(
            malformed_updated_at,
            "sandbox eviction: suspended rows have malformed updated_at and are not evictable",
        );
    }

    let disk = disk.ok_or("statvfs on the cache filesystem failed; cannot evaluate disk gate")?;
    // Hysteresis state: open when usage reached the engage fraction at
    // tick start; closed again mid-tick once evictions bring usage
    // below the target fraction.
    if !crate::disk_usage::gate_engaged(disk.fraction(), cfg.engage_fraction) {
        scan_state.reset_cursor();
        return Ok(TickReport {
            malformed_updated_at,
            resident_rows,
            ..Default::default()
        });
    }
    let scan_started = Instant::now();
    scan_state.begin_tick(scan_started);
    let now = now_secs();
    let cutoff = now.saturating_sub(cfg.min_idle.as_secs());
    let legacy_repair =
        repair_legacy_reupload_candidates(service, scan_state, cutoff, cfg.reupload_max_per_tick)
            .await?;
    let service_for_list = service.clone();
    let max = cfg.max_per_tick;
    let cursor_for_list = scan_state.cursor.clone();
    let candidates = tokio::task::spawn_blocking(move || {
        let conn = service_for_list
            .db_conn()
            .map_err(|error| error.to_string())?;
        list_candidates(&conn, cutoff, cursor_for_list.as_ref(), max)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|e| format!("list_candidates spawn_blocking: {e}"))?
    .map_err(|e| format!("list_candidates: {e}"))?;

    let mut report = TickReport {
        candidates: candidates.len(),
        malformed_updated_at,
        resident_rows,
        legacy_repair,
        ..Default::default()
    };
    if candidates.is_empty() {
        scan_state.cursor = None;
        return Ok(report);
    }

    if candidates
        .iter()
        .all(|candidate| scan_state.is_backed_off(&candidate.sandbox_id))
    {
        // This duplicates the backed-off branch in the main loop so an
        // all-backed-off tick avoids both the cluster connection and a
        // dependency on cluster-api availability.
        for candidate in candidates {
            record_continuing_candidate_skip(
                &mut report,
                scan_state,
                &candidate.sandbox_id,
                candidate.cursor,
                SkipReason::RetryBackoff,
                scan_started,
            );
        }
        return Ok(report);
    }

    let mut cluster = pplx_space_node_api_client::cluster::connect_from_env()
        .await
        .map_err(|e| format!("cluster-api connect: {e}"))?;

    for candidate in candidates {
        let EvictionCandidate { sandbox_id, cursor } = candidate;
        // The witness the candidate qualified on; `evicted_age_secs` and
        // the retention-pressure advisory measure the same idleness the
        // gate did, so a drained backlog of long-cold rows cannot read
        // as a wave of young evictions.
        let idle_witness_secs = cursor.idle_witness_secs;
        if scan_state.is_backed_off(&sandbox_id) {
            record_continuing_candidate_skip(
                &mut report,
                scan_state,
                &sandbox_id,
                cursor,
                SkipReason::RetryBackoff,
                scan_started,
            );
            continue;
        }
        // Gather the local decision inputs fresh per candidate.
        let service_for_inputs = service.clone();
        let id_for_inputs = sandbox_id.clone();
        let inputs = tokio::task::spawn_blocking(move || {
            let conn = service_for_inputs
                .db_conn()
                .map_err(|error| error.to_string())?;
            gather_local_evidence(&conn, &id_for_inputs).map_err(|error| error.to_string())
        })
        .await
        .map_err(|e| format!("inputs spawn_blocking: {e}"))?;
        let (evidence, updated_at_secs) = match inputs {
            Ok(Some(v)) => v,
            Ok(None) => {
                // Sandbox row vanished between planning and now.
                record_continuing_candidate_skip(
                    &mut report,
                    scan_state,
                    &sandbox_id,
                    cursor,
                    SkipReason::NotSuspendedAtExecute,
                    scan_started,
                );
                continue;
            }
            Err(error) => {
                warn!(sandbox_id = %sandbox_id, %error, "sandbox eviction: local input read failed");
                record_continuing_candidate_skip(
                    &mut report,
                    scan_state,
                    &sandbox_id,
                    cursor,
                    SkipReason::LocalReadError,
                    scan_started,
                );
                continue;
            }
        };

        if matches!(evidence, LocalEvidence::Unpinned { latest_job: None }) {
            record_continuing_candidate_skip(
                &mut report,
                scan_state,
                &sandbox_id,
                cursor,
                SkipReason::UnknownLocalGeneration,
                scan_started,
            );
            continue;
        }

        let cluster_latest =
            match ready_unpinned_suspend_generation(&mut cluster, &sandbox_id, &evidence).await {
                Ok(generation) => generation,
                Err(reason) => {
                    let flow = record_candidate_skip(
                        &mut report,
                        scan_state,
                        &sandbox_id,
                        cursor,
                        reason,
                        scan_started,
                    );
                    if flow == CandidateFlow::Stop {
                        break;
                    }
                    continue;
                }
            };

        let suspend_age = suspend_time_witness(&evidence);
        let reason = match decide(cluster_latest, &evidence, updated_at_secs) {
            PreDecision::Skip(reason) => {
                record_continuing_candidate_skip(
                    &mut report,
                    scan_state,
                    &sandbox_id,
                    cursor,
                    reason,
                    scan_started,
                );
                continue;
            }
            PreDecision::ProvePinned => {
                let LocalEvidence::Pinned(pinned) = &evidence else {
                    // decide() only returns ProvePinned for Pinned
                    // evidence.
                    record_continuing_candidate_skip(
                        &mut report,
                        scan_state,
                        &sandbox_id,
                        cursor,
                        SkipReason::CurrentArtifactUnproven,
                        scan_started,
                    );
                    continue;
                };
                match prove_pinned_artifact(&mut cluster, service, &sandbox_id, pinned).await {
                    Ok(true) => EvictReason::LatestBackedIdle,
                    Ok(false) => {
                        let repair_outcome =
                            if report.legacy_repair.attempts() < cfg.reupload_max_per_tick {
                                Some(
                                    enqueue_legacy_reupload(
                                        service,
                                        &mut report.legacy_repair,
                                        &sandbox_id,
                                    )
                                    .await,
                                )
                            } else {
                                None
                            };
                        record_continuing_candidate_skip(
                            &mut report,
                            scan_state,
                            &sandbox_id,
                            cursor,
                            SkipReason::CurrentArtifactUnproven,
                            scan_started,
                        );
                        // A pending upload excludes the row from candidate SQL.
                        // Once it completes, reconsider the new proof without
                        // waiting for the local backoff to expire. Skips and
                        // failures refresh the backoff; cap exhaustion leaves
                        // any existing backoff untouched.
                        scan_state.apply_repair_outcome(&sandbox_id, repair_outcome);
                        continue;
                    }
                    Err(reason) => {
                        let flow = record_candidate_skip(
                            &mut report,
                            scan_state,
                            &sandbox_id,
                            cursor,
                            reason,
                            scan_started,
                        );
                        if flow == CandidateFlow::Stop {
                            break;
                        }
                        continue;
                    }
                }
            }
            PreDecision::Evict(reason) => reason,
        };

        // The proven identity the eviction claim must re-assert: the
        // exact pinned artifact, or "still unpinned" for legacy rows.
        let proven_pin = match &evidence {
            LocalEvidence::Pinned(pinned) => Some(pinned.artifact_id),
            _ => None,
        };

        // Size estimate before the directory disappears.
        let cache_dir = sandbox_cache_dir(&sandbox_id);
        let dir_for_size = cache_dir.clone();
        let bytes = tokio::task::spawn_blocking(move || {
            crate::snapshot_gc::dir_size_bytes(Path::new(&dir_for_size))
        })
        .await
        .unwrap_or(0);

        match service
            .evict_local_sandbox(
                &sandbox_id,
                crate::service::EvictionClaim::SuspendedPinned(proven_pin),
                None,
            )
            .await
        {
            Ok(()) => {
                scan_state.complete_candidate(cursor);
                report.evicted += 1;
                report.logical_bytes_freed = report.logical_bytes_freed.saturating_add(bytes);
                let age_secs = now.saturating_sub(idle_witness_secs);
                pplx_space_metrics::sandbox_eviction::evicted(reason.as_str());
                pplx_space_metrics::sandbox_eviction::evicted_age_secs(age_secs);
                if let Some((suspended_at_secs, proof)) = suspend_age {
                    pplx_space_metrics::sandbox_eviction::evicted_suspend_age_secs(
                        now.saturating_sub(suspended_at_secs),
                        proof.as_str(),
                    );
                }
                pplx_space_metrics::sandbox_eviction::logical_bytes_freed(reason.as_str(), bytes);
                if age_secs < EvictionConfig::RETENTION_WARNING_SECS {
                    pplx_space_metrics::sandbox_eviction::retention_pressure(1);
                    report.young_evictions += 1;
                    report.youngest_eviction_age_secs = Some(
                        report
                            .youngest_eviction_age_secs
                            .map_or(age_secs, |youngest| youngest.min(age_secs)),
                    );
                }
                info!(
                    sandbox_id = %sandbox_id,
                    reason = reason.as_str(),
                    bytes,
                    remote_snapshot_id = ?cluster_latest,
                    "sandbox eviction: local presence evicted",
                );
                let mut details = pplx_space_events::SandboxEventDetails::default()
                    .reason(reason.as_str())
                    .local_path(cache_dir);
                if let Some(id) = cluster_latest {
                    details = details.snapshot_id(id.to_string());
                }
                emit_eviction_event(
                    &sandbox_id,
                    pplx_space_events::SandboxEventType::LocalEvictionCompleted,
                    details,
                );
                // Mid-tick hysteresis: once usage drops below the
                // target fraction, stop trading away wake-cache for
                // the rest of this tick. The next scheduled tick is
                // the authoritative re-evaluation.
                if let Some(current) = crate::disk_usage::read_cache_disk_usage()
                    && current.fraction() < cfg.target_fraction
                {
                    scan_state.reset_cursor();
                    break;
                }
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::FailedPrecondition | tonic::Code::NotFound
                ) =>
            {
                // A resume/restore claimed the row (or a delete removed
                // it) between planning and the eviction CAS — exactly
                // what the claim exists for.
                record_continuing_candidate_skip(
                    &mut report,
                    scan_state,
                    &sandbox_id,
                    cursor,
                    SkipReason::NotSuspendedAtExecute,
                    scan_started,
                );
            }
            Err(status) => {
                scan_state.complete_candidate(cursor);
                report.failed += 1;
                pplx_space_metrics::sandbox_eviction::failed();
                warn!(
                    sandbox_id = %sandbox_id,
                    code = %status.code(),
                    message = %status.message(),
                    "sandbox eviction: local delete failed; row left in deleting for cleanup retry",
                );
                emit_eviction_event(
                    &sandbox_id,
                    pplx_space_events::SandboxEventType::LocalEvictionFailed,
                    pplx_space_events::SandboxEventDetails::default()
                        .reason(reason.as_str())
                        .error(status.message().to_string())
                        .logged_by_caller(),
                );
            }
        }
    }

    Ok(report)
}

/// Drive the eviction loop on a recurring interval until `shutdown`
/// fires. No-op (one log line) when disabled.
pub async fn run_loop(
    service: SandboxManagerService,
    cfg: EvictionConfig,
    shutdown: CancellationToken,
) {
    if !cfg.enabled {
        info!(
            env_var = EvictionConfig::ENV_ENABLED,
            "sandbox eviction disabled; loop exiting",
        );
        return;
    }

    info!(
        engage_fraction = cfg.engage_fraction,
        target_fraction = cfg.target_fraction,
        min_idle_secs = cfg.min_idle.as_secs(),
        interval_secs = cfg.interval.as_secs(),
        max_per_tick = cfg.max_per_tick,
        reupload_max_per_tick = cfg.reupload_max_per_tick,
        "sandbox eviction loop starting",
    );

    let mut scan_state = CandidateScanState::default();
    loop {
        let tick_started = Instant::now();
        let disk = crate::disk_usage::read_cache_disk_usage();
        if let Some(disk) = disk {
            pplx_space_metrics::sandbox_eviction::disk_usage_fraction(disk.fraction());
        }
        let tick_result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            result = evict_tick(&service, cfg, disk, &mut scan_state) => result,
        };

        match tick_result {
            Ok(report) => {
                if report.young_evictions > 0 {
                    warn!(
                        young_evictions = report.young_evictions,
                        youngest_eviction_age_secs = report.youngest_eviction_age_secs,
                        warning_age_secs = EvictionConfig::RETENTION_WARNING_SECS,
                        target_fraction = cfg.target_fraction,
                        "sandbox eviction: disk pressure shortened local retention below warning age",
                    );
                }
                info!(
                    candidates = report.candidates,
                    evicted = report.evicted,
                    skipped = report.skipped,
                    failed = report.failed,
                    legacy_repair_candidates = report.legacy_repair.candidates,
                    legacy_repair_queued = report.legacy_repair.queued,
                    legacy_repair_skipped = report.legacy_repair.skipped,
                    legacy_repair_failed = report.legacy_repair.failed,
                    malformed_updated_at = report.malformed_updated_at,
                    resident_rows = report.resident_rows,
                    logical_bytes_freed = report.logical_bytes_freed,
                    "sandbox eviction tick complete",
                );
                pplx_space_metrics::sandbox_eviction::tick(
                    "ok",
                    tick_started,
                    report.candidates as u64,
                );
            }
            Err(error) => {
                warn!(%error, "sandbox eviction tick failed");
                pplx_space_metrics::sandbox_eviction::tick("error", tick_started, 0);
            }
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(cfg.interval) => {}
        }
    }
}

#[cfg(test)]
mod tests;
