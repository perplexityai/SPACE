//! Periodic reconciler that enforces a keep-newest-N retention policy
//! over still-live sandboxes' durable snapshots.
//!
//! ## Why this exists
//!
//! Each suspend/checkpoint writes a fresh durable snapshot (a new
//! `snapshots` row + at most one `snapshot_storage_locations` row pointing at an
//! S3 prefix holding `vmstate` / `memory` / `rootfs` / `metadata.json`).
//! Nothing deletes the *older* snapshots of a sandbox that stays alive —
//! `snapshot_storage_sweeper` only reaps blobs orphaned by a sandbox
//! DELETE, and the on-node btrfs tier scheduler only prunes node-local
//! btrfs snapshots, not the durable S3 copies. Left unbounded, a
//! long-lived sandbox that suspends/resumes repeatedly accumulates
//! durable snapshots forever.
//!
//! This reconciler keeps, per sandbox, the newest `keep_n` `ready`
//! `full` snapshots and prunes the rest (S3 prefix + the `snapshots` and
//! `snapshot_storage_locations` rows).
//!
//! ## Validity
//!
//! Only `state = 'ready'`, `type = 'full'` snapshots with a usable attached
//! storage location count toward `N`. `creating` (upload in flight), `failed`,
//! and route-less/orphaned rows are invisible to the sibling probe. A route-less
//! fresh row may still surface for repair telemetry, but it remains unclaimed.
//!
//! ### Recency is not sufficient — the restore-source guard
//!
//! The newest `ready` full snapshot can never have `keep_n` newer
//! siblings, so it is never a candidate while `keep_n >= 1`. That protects
//! the *markerless* restore path, which selects by recency. It does not
//! protect either of the two other shapes restore selection can take:
//!
//! - A sandbox carrying `current_local_artifact_id` restores by **exact
//!   artifact identity**, and once the row is `suspended` that selection has
//!   no newest-snapshot fallback. A sandbox pinned to an older artifact has
//!   its only restore source outside the keep-set.
//! - A row already in `restoring` retries from its **pre-restore source
//!   window**, deliberately ignoring the marker — a failed restore branch
//!   can publish later artifacts after the row entered `restoring`, and the
//!   retry must load the state restore originally started from. Those later
//!   artifacts still count as newer siblings, so once `keep_n` of them
//!   accumulate the retry's real source falls out of the keep-set.
//!
//! Both the candidate scan and the pre-reap authorization therefore exclude
//! any snapshot matching the shared
//! `sandbox_portability::snapshot_is_live_restore_source` fragment, which
//! composes both shapes. It is built from the same pieces the restore
//! selector itself is built from, so the "never delete this" and "this is
//! the one to load" answers cannot drift. The exclusion filters the
//! candidate row itself and never the newer-sibling probe, so a shielded
//! snapshot still counts toward `keep_n` and protecting it never promotes
//! an extra row into the candidate set.
//!
//! ### Claim and recovery
//!
//! A prune first validates its storage route, then calls
//! [`RetentionSink::claim_prune`]. The claim takes the shared fork-source lock
//! and then the sandbox lock, rechecks the floor, route, age, and restore-source
//! guards, and commits `ready -> deleting` before any byte deletion. Readers
//! treat `deleting` as gone; the state never moves back to `ready`.
//!
//! Each tick reserves part of one shared `max_per_tick` budget for existing
//! claims, then lends unused recovery capacity to fresh work. Odd units
//! alternate between recovery and fresh work, so `max_per_tick = 1` cannot
//! starve either side. Reap and finalize are idempotent: failures leave the
//! claim in place for a later tick.
//!
//! Both `enabled = false` and `dry_run = true` stop finish-forward recovery and
//! can therefore leave already-committed `deleting` claims parked. Dry run
//! reports those rows as `stuck_claim`. Recovery is to run again with
//! `enabled = true` and `dry_run = false`; there is deliberately no unsafe
//! `deleting -> ready` unclaim because a prior attempt may already have removed
//! some or all bytes.
//!
//! Finalize requires `deleting`, removes storage-location rows before the
//! snapshot row, and reports floor or marker drift observed after the claim.
//!
//! ## Hosting boundary
//!
//! This library owns one deterministic retention tick and its storage/database
//! contracts. The hosting application owns environment parsing, interval
//! scheduling, singleton authority, and shutdown. SPACE currently hosts it in
//! `space-cluster-controller`; API servers do not start background loops.

use pplx_space_common_core::types::SnapshotId;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{
    StreamExt,
    stream::{self, FuturesUnordered},
};
use pplx_space_common_core::types::SandboxId;
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::snapshot_retention_sweeper as sweeper_metrics;
use uuid::Uuid;

use backend::{DeletePrefixError, DeletePrefixOutcome, StorageBackendRegistry};

pub mod backend;
pub mod store;

#[cfg(test)]
mod tests;

pub use store::SnapshotRetentionDbStore;

/// Runtime-independent policy consumed by one retention state machine.
/// Environment parsing and process scheduling belong to the application
/// that hosts the reconciler.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// When `true`, the sweep runs its full candidate scan and per-candidate
    /// keep-N safety gate but performs **no** destructive action — it logs
    /// and meters (`would_prune`) what a live run would delete, and touches
    /// neither S3 nor Postgres. The cluster controller reads it from
    /// `SPACE_SNAPSHOT_RETENTION_SWEEPER_DRY_RUN` (off by default), so a new
    /// environment can enable the reconciler observe-only and turn dry run
    /// off once an operator has validated the keep-N filter against real data.
    pub dry_run: bool,
    /// Number of newest `ready` full snapshots to keep per sandbox.
    /// Hosting applications must validate this as non-zero before
    /// constructing the policy.
    pub keep_n: u32,
    /// Hard cap on how many snapshots a single tick may prune. Bounds
    /// blast radius if the keep-N filter ever regressed.
    pub max_per_tick: u32,
    /// Maximum concurrent candidate workflows within each phase, including
    /// claim, byte deletion, and finalize. Effective concurrency is also
    /// bounded by the phase's remaining shared work budget.
    pub max_concurrent_deletes: NonZeroU32,
    /// Minimum age (since the snapshot's creation `timestamp`) before a
    /// snapshot beyond the keep-set is prune-eligible. `None` disables
    /// the grace; see the user-CLAUDE.md rule on `Option<Duration>` vs a
    /// zero-sentinel for why this isn't a bare `Duration`.
    pub min_age: Option<Duration>,
}

/// One durable location belonging to a snapshot. Its route is intentionally
/// redacted from `Debug`: connection strings may contain endpoint userinfo.
#[derive(Clone, PartialEq, Eq)]
pub struct SnapshotStorageLocation {
    pub id: Uuid,
    connection_string: String,
    pub has_encrypted_fields: bool,
}

impl SnapshotStorageLocation {
    pub fn new(id: Uuid, connection_string: String, has_encrypted_fields: bool) -> Self {
        Self {
            id,
            connection_string,
            has_encrypted_fields,
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}

impl std::fmt::Debug for SnapshotStorageLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotStorageLocation")
            .field("id", &self.id)
            .field("connection_string_bytes", &self.connection_string.len())
            .field("has_encrypted_fields", &self.has_encrypted_fields)
            .finish()
    }
}

/// One prune candidate surfaced by the retention store. The database permits
/// zero or one storage location per snapshot. `None` means the catalog has no
/// usable route for the snapshot; whether durable bytes still exist is unknown.
/// A fresh row remains unclaimed, while an existing claim finishes forward.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneCandidate {
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub storage_location: Option<SnapshotStorageLocation>,
    /// Seconds since the snapshot's creation `timestamp`, for the
    /// emitted `snapshot_pruned` event.
    pub age_secs: i64,
}

/// A bounded discovery page. Progress is independent of eligibility.
#[derive(Debug, Default)]
pub struct CandidatePage {
    pub candidates: Vec<PruneCandidate>,
    /// Last raw row examined, or last returned candidate if projection hit its cap.
    pub scanned_through: Option<SnapshotId>,
    /// True only when both raw discovery and candidate projection reached the end.
    pub exhausted: bool,
}

/// Postgres-side surface. Production impl: [`SnapshotRetentionDbStore`].
pub trait RetentionSink: Send + Sync + 'static {
    /// Discover a bounded raw page and return at most `limit` eligible rows.
    /// Claimed rows are absent; an empty candidate page can still make progress.
    fn list_candidates(
        &self,
        keep_n: i64,
        limit: i64,
        after: Option<SnapshotId>,
        min_age: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<CandidatePage, sqlx::Error>> + Send;

    /// Fixed upper bound for one claimed-recovery epoch.
    fn claimed_scan_epoch(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<ClaimedScanEpoch>, sqlx::Error>> + Send;

    /// Page `deleting` rows independently within one bounded recovery epoch.
    fn list_claimed(
        &self,
        limit: i64,
        after: Option<SnapshotId>,
        through: SnapshotId,
    ) -> impl std::future::Future<Output = Result<Vec<PruneCandidate>, sqlx::Error>> + Send;

    /// Revalidate the complete candidate under the shared fork-source and
    /// sandbox locks, then commit `ready -> deleting`. The expected optional
    /// location and database-clock age are part of the guarded transition so
    /// route validation cannot race a writer.
    fn claim_prune(
        &self,
        candidate: PruneCandidate,
        keep_n: i64,
        min_age: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<ClaimOutcome, sqlx::Error>> + Send;

    /// After the reap, require `deleting`, report post-claim drift, and delete
    /// storage locations before the snapshot row.
    fn finalize_prune(
        &self,
        snapshot_id: SnapshotId,
        keep_n: i64,
    ) -> impl std::future::Future<Output = Result<PruneFinalize, sqlx::Error>> + Send;
}

pub use pplx_space_metrics::snapshot_retention_sweeper::ClaimRefusalReason;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The exact candidate authorized by the committed claim. Callers must
    /// delete bytes using this returned location, never their advisory scan
    /// object.
    Claimed(PruneCandidate),
    Refused(ClaimRefusalReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimedScanEpoch {
    pub through: SnapshotId,
}

/// Outcome of [`RetentionSink::finalize_prune`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneFinalize {
    /// The snapshot row + its storage locations were deleted.
    ///
    /// `floor_ok` is `false` if, at delete time, the sandbox had fewer
    /// than `keep_n` newer `ready` full snapshots — i.e. a concurrent
    /// deletion breached the retention floor after the reap was
    /// authorized.
    ///
    /// `marker_ok` is `false` if, at delete time, the snapshot had become
    /// its sandbox's tracked current artifact — i.e. a marker write landed
    /// in the claim→finalize window. This is the residual race the
    /// pre-reap guard cannot close on its own; see the module docs.
    ///
    /// In both cases the row was still removed (its blobs were already
    /// gone, so it was unrestorable), but the breach is worth surfacing.
    Pruned { floor_ok: bool, marker_ok: bool },
    /// The row still exists but is not `deleting`.
    NotClaimed,
    /// The snapshot row was already gone when finalize ran (e.g. a
    /// concurrent sandbox-delete cascade). Benign: reaping its blobs only
    /// removed data that was being deleted anyway.
    AlreadyGone,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    /// Fresh `ready` candidates returned by the scan.
    pub scanned: usize,
    /// `deleting` rows returned by the recovery scan.
    pub claimed_scanned: usize,
    pub pruned: usize,
    /// Candidates a live run would have pruned, observed during a dry run
    /// (`dry_run = true`). They passed the keep-N safety gate, but neither
    /// their S3 prefix nor their DB rows were touched. Always zero when
    /// `dry_run = false`.
    pub would_prune: usize,
    pub already_gone: usize,
    /// Candidates skipped because they carry an encryption sidecar.
    pub skipped_encrypted: usize,
    /// Fresh candidates left unclaimed because no storage location is
    /// attached. These require catalog repair before retention can proceed.
    pub missing_locations: usize,
    /// Candidates skipped because no backend is registered for the
    /// connection string's URI scheme.
    pub unsupported_scheme: usize,
    /// Candidates skipped because `storage_connection_string` failed to
    /// parse as a valid URI for its backend.
    pub parse_errors: usize,
    /// Candidates skipped because the URI targets a container the backend
    /// isn't scoped to.
    pub bucket_out_of_scope: usize,
    /// Backend list/delete failures. Transient; retried next tick.
    pub backend_errors: usize,
    /// Claim transaction failures before byte deletion.
    pub claim_errors: usize,
    /// Transactional DB delete failures *after* a successful reap. The
    /// blob is already cleared, so the next tick re-lists (idempotent) and
    /// retries the delete. Transient.
    pub db_errors: usize,
    /// Finalize calls refused because the row was not `deleting`.
    pub finalize_protocol_errors: usize,
    /// Refused claims or rows already gone at finalize.
    pub drift_skips: usize,
    /// Recovered claims driven to a terminal state this tick.
    pub resumed: usize,
    /// Claims left untouched because the sweeper is in dry-run mode.
    pub stuck_claims: usize,
    /// Snapshots that were pruned but left the sandbox below the keep-N
    /// floor because a newer snapshot was deleted concurrently in the
    /// reap→finalize window. The row was still removed (it was already
    /// unrestorable), but the breach is surfaced loudly. Expected zero.
    pub floor_breaches: usize,
    /// Snapshots that were pruned after becoming their sandbox's tracked
    /// current artifact in the claim→finalize window — a restore target
    /// was destroyed. The row was still removed (it was already
    /// unrestorable), but the breach is surfaced loudly. Expected zero.
    pub marker_breaches: usize,
    /// Candidates the per-tick cap rate-limited; the next tick picks
    /// them up.
    pub rate_limited: usize,
}

impl TickReport {
    fn accumulate(&mut self, other: Self) {
        let Self {
            scanned,
            claimed_scanned,
            pruned,
            would_prune,
            already_gone,
            skipped_encrypted,
            missing_locations,
            unsupported_scheme,
            parse_errors,
            bucket_out_of_scope,
            backend_errors,
            claim_errors,
            db_errors,
            finalize_protocol_errors,
            drift_skips,
            resumed,
            stuck_claims,
            floor_breaches,
            marker_breaches,
            rate_limited,
        } = other;
        self.scanned += scanned;
        self.claimed_scanned += claimed_scanned;
        self.pruned += pruned;
        self.would_prune += would_prune;
        self.already_gone += already_gone;
        self.skipped_encrypted += skipped_encrypted;
        self.missing_locations += missing_locations;
        self.unsupported_scheme += unsupported_scheme;
        self.parse_errors += parse_errors;
        self.bucket_out_of_scope += bucket_out_of_scope;
        self.backend_errors += backend_errors;
        self.claim_errors += claim_errors;
        self.db_errors += db_errors;
        self.finalize_protocol_errors += finalize_protocol_errors;
        self.drift_skips += drift_skips;
        self.resumed += resumed;
        self.stuck_claims += stuck_claims;
        self.floor_breaches += floor_breaches;
        self.marker_breaches += marker_breaches;
        self.rate_limited += rate_limited;
    }
}

pub struct Reconciler<S: RetentionSink> {
    policy: RetentionPolicy,
    sink: Arc<S>,
    registry: StorageBackendRegistry,
    /// Cursor for fresh candidates only.
    scan_after: Option<SnapshotId>,
    /// Independent cursor for claimed recovery.
    claimed_scan_after: Option<SnapshotId>,
    /// Fixed upper bound for the current recovery epoch. New higher-id claims
    /// wait for the next epoch, guaranteeing that this one wraps and retries
    /// older parked claims even while fresh work keeps arriving.
    claimed_scan_through: Option<SnapshotId>,
    /// Odd shared budgets alternate their extra unit between recovery and
    /// fresh work. In particular, `max_per_tick = 1` makes progress on both
    /// queues while the unused recovery unit is still borrowable by fresh
    /// work when there are no parked claims.
    recovery_gets_extra: bool,
}

impl<S: RetentionSink> Reconciler<S> {
    pub fn new<P: Into<RetentionPolicy>>(
        policy: P,
        sink: S,
        registry: StorageBackendRegistry,
    ) -> Self {
        let policy = policy.into();
        assert!(
            policy.keep_n > 0,
            "snapshot retention keep_n must be non-zero"
        );
        Self {
            policy,
            sink: Arc::new(sink),
            registry,
            scan_after: None,
            claimed_scan_after: None,
            claimed_scan_through: None,
            recovery_gets_extra: true,
        }
    }

    /// Retune work limits between sweeps without resetting discovery/recovery
    /// cursors, epoch bounds, or odd-budget fairness. The mutable borrow ensures
    /// an in-flight tick finishes before its limits can change.
    pub fn set_work_limits(
        &mut self,
        max_per_tick: NonZeroU32,
        max_concurrent_deletes: NonZeroU32,
    ) {
        self.policy.max_per_tick = max_per_tick.get();
        self.policy.max_concurrent_deletes = max_concurrent_deletes;
    }

    pub async fn tick(&mut self) -> TickReport {
        let started_at = Instant::now();
        let mut report = TickReport::default();
        let keep_n = i64::from(self.policy.keep_n);
        let total_budget = usize::try_from(self.policy.max_per_tick).unwrap_or(usize::MAX);
        let recovery_budget =
            total_budget / 2 + usize::from(total_budget % 2 == 1 && self.recovery_gets_extra);
        self.recovery_gets_extra = !self.recovery_gets_extra;

        // Recovery and fresh claims share one hard work budget so the metric
        // and monitor ceiling describe the actual number of destructive
        // attempts. Unused recovery capacity is borrowed by fresh work.
        let recovered_work = self
            .recover_claimed(keep_n, recovery_budget, &mut report)
            .await;
        self.sweep_fresh(
            keep_n,
            total_budget.saturating_sub(recovered_work),
            &mut report,
        )
        .await;
        sweeper_metrics::scan_volume(
            report
                .scanned
                .saturating_add(report.claimed_scanned)
                .try_into()
                .unwrap_or(u64::MAX),
        );
        sweeper_metrics::loop_latency(started_at);
        report
    }

    /// Finish one bounded page of durable claims. Every surfaced claim spends
    /// one shared work unit, including a parked/erroring row, so recovery can
    /// neither double the destructive budget nor generate unbounded failures.
    async fn recover_claimed(
        &mut self,
        keep_n: i64,
        budget: usize,
        report: &mut TickReport,
    ) -> usize {
        if budget == 0 {
            sweeper_metrics::claimed_scanned(0);
            return 0;
        }
        let limit = i64::try_from(budget).unwrap_or(i64::MAX);
        let Some(through) = self.claimed_epoch_through().await else {
            return 0;
        };
        let Some(claimed) = self.claimed_page(limit, through).await else {
            return 0;
        };
        report.claimed_scanned = claimed.len();
        sweeper_metrics::claimed_scanned(claimed.len() as u64);
        self.advance_claimed_cursor(&claimed, limit, through);

        let concurrency = self.phase_concurrency(claimed.len());
        let reconciler = &*self;
        let mut outcomes = stream::iter(claimed)
            .map(|candidate| async move {
                let mut outcome = TickReport::default();
                reconciler
                    .recover_candidate(candidate, keep_n, &mut outcome)
                    .await;
                outcome
            })
            .buffer_unordered(concurrency);
        while let Some(outcome) = outcomes.next().await {
            report.accumulate(outcome);
        }
        report.claimed_scanned
    }

    async fn claimed_epoch_through(&mut self) -> Option<SnapshotId> {
        if let Some(through) = self.claimed_scan_through {
            return Some(through);
        }
        match self.sink.claimed_scan_epoch().await {
            Ok(Some(epoch)) => {
                self.claimed_scan_through = Some(epoch.through);
                Some(epoch.through)
            }
            Ok(None) => {
                sweeper_metrics::claimed_scanned(0);
                None
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "snapshot_retention_sweeper: claimed high-watermark scan \
                     failed; skipping claimed recovery this tick"
                );
                sweeper_metrics::failed("claimed_scan");
                None
            }
        }
    }

    async fn claimed_page(&self, limit: i64, through: SnapshotId) -> Option<Vec<PruneCandidate>> {
        match self
            .sink
            .list_claimed(limit, self.claimed_scan_after, through)
            .await
        {
            Ok(rows) => Some(rows),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "snapshot_retention_sweeper: list_claimed failed; skipping \
                     claimed recovery this tick"
                );
                sweeper_metrics::failed("claimed_scan");
                None
            }
        }
    }

    fn advance_claimed_cursor(
        &mut self,
        claimed: &[PruneCandidate],
        limit: i64,
        through: SnapshotId,
    ) {
        // The fixed upper bound excludes claims created after the epoch began.
        // Reaching that row, or observing a short page, completes the epoch.
        let reached_epoch_end = claimed
            .last()
            .is_none_or(|candidate| candidate.snapshot_id == through);
        if (claimed.len() as i64) < limit || reached_epoch_end {
            self.claimed_scan_after = None;
            self.claimed_scan_through = None;
        } else {
            self.claimed_scan_after = claimed.last().map(|candidate| candidate.snapshot_id);
        }
    }

    async fn recover_candidate(
        &self,
        candidate: PruneCandidate,
        keep_n: i64,
        report: &mut TickReport,
    ) {
        if self.policy.dry_run {
            tracing::warn!(
                snapshot_id = %candidate.snapshot_id,
                sandbox_id = %candidate.sandbox_id,
                "snapshot_retention_sweeper: DRY RUN — row is already claimed \
                 'deleting'; return to live mode to finish forward"
            );
            sweeper_metrics::failed("stuck_claim");
            report.stuck_claims += 1;
            return;
        }
        if candidate
            .storage_location
            .as_ref()
            .is_some_and(|location| location.has_encrypted_fields)
        {
            tracing::error!(
                snapshot_id = %candidate.snapshot_id,
                "snapshot_retention_sweeper: claimed row has encrypted storage \
                 metadata; the claim stays parked until an operator repairs it"
            );
            sweeper_metrics::failed("parked_claim");
            report.skipped_encrypted += 1;
            return;
        }
        tracing::info!(
            snapshot_id = %candidate.snapshot_id,
            sandbox_id = %candidate.sandbox_id,
            "snapshot_retention_sweeper: resuming a claimed prune (finish-forward)"
        );
        self.reap_and_finalize(&candidate, keep_n, true, report)
            .await;
    }

    /// Fresh-work phase: scan `ready` candidates past the cursor and claim
    /// up to `max_per_tick` of them (or meter what a live run would claim,
    /// under dry run).
    async fn sweep_fresh(&mut self, keep_n: i64, budget: usize, report: &mut TickReport) {
        if budget == 0 {
            return;
        }
        // Empty keeper pages must not cost an entire controller interval. Bound
        // discovery independently of destructive work: at most 10M raw rows and
        // five seconds of page starts per tick. An in-flight query still obeys
        // the database statement timeout. Keep the existing 2x candidate cap.
        let mut discovery_time = Duration::ZERO;
        let candidate_cap = self.policy.max_per_tick.saturating_mul(2) as usize;
        let mut cap_consumed = 0;
        let mut successful_scan = false;
        for _ in 0..1_000 {
            let limit = candidate_cap.saturating_sub(report.scanned);
            if limit == 0 || cap_consumed == budget || discovery_time >= Duration::from_secs(5) {
                break;
            }
            let started = tokio::time::Instant::now();
            let page = self.fresh_candidates(keep_n, limit as i64).await;
            discovery_time += started.elapsed();
            let Some(page) = page else {
                break;
            };
            successful_scan = true;
            report.scanned += page.candidates.len();
            let concurrency = self.phase_concurrency(budget - cap_consumed);
            let mut candidates = page.candidates.into_iter();
            let mut pending = FuturesUnordered::new();
            let mut last_visited = None;
            loop {
                while pending.len() < concurrency && cap_consumed + pending.len() < budget {
                    let Some(candidate) = candidates.next() else {
                        break;
                    };
                    last_visited = Some(candidate.snapshot_id);
                    pending.push(async {
                        let mut outcome = TickReport::default();
                        let consumed = self
                            .process_fresh_candidate(candidate, keep_n, &mut outcome)
                            .await;
                        (consumed, outcome)
                    });
                }
                let Some((consumed, outcome)) = pending.next().await else {
                    break;
                };
                cap_consumed += usize::from(consumed);
                report.accumulate(outcome);
            }
            drop(pending);
            report.rate_limited += candidates.len();
            // Only cross the scanned high watermark after every returned
            // candidate was visited. Completion order cannot move this cursor.
            if candidates.len() != 0 {
                self.scan_after = last_visited;
                break;
            }
            self.scan_after = if page.exhausted {
                None
            } else {
                page.scanned_through
            };
            if page.exhausted {
                break;
            }
        }
        if successful_scan {
            sweeper_metrics::candidates_scanned(report.scanned as u64);
            sweeper_metrics::rate_limited(report.rate_limited as u64);
        }
    }

    fn phase_concurrency(&self, work: usize) -> usize {
        usize::try_from(self.policy.max_concurrent_deletes.get())
            .unwrap_or(usize::MAX)
            .min(work)
            .max(1)
    }

    async fn fresh_candidates(&self, keep_n: i64, limit: i64) -> Option<CandidatePage> {
        match self
            .sink
            .list_candidates(keep_n, limit, self.scan_after, self.policy.min_age)
            .await
        {
            Ok(rows) => Some(rows),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "snapshot_retention_sweeper: list_candidates failed"
                );
                sweeper_metrics::failed("fresh_scan");
                None
            }
        }
    }

    async fn process_fresh_candidate(
        &self,
        candidate: PruneCandidate,
        keep_n: i64,
        report: &mut TickReport,
    ) -> bool {
        if !self.fresh_candidate_is_routable(&candidate, report) {
            return false;
        }
        if self.policy.dry_run {
            record_dry_run_candidate(&candidate, report);
            return true;
        }
        let Some(claimed) = self.claim_fresh(candidate, keep_n, report).await else {
            return false;
        };
        self.reap_and_finalize(&claimed, keep_n, false, report)
            .await;
        true
    }

    fn fresh_candidate_is_routable(
        &self,
        candidate: &PruneCandidate,
        report: &mut TickReport,
    ) -> bool {
        let Some(storage_location) = candidate.storage_location.as_ref() else {
            tracing::error!(
                snapshot_id = %candidate.snapshot_id,
                "snapshot_retention_sweeper: fresh candidate has no storage location; leaving it unclaimed"
            );
            sweeper_metrics::failed("missing_location");
            report.missing_locations += 1;
            return false;
        };
        if storage_location.has_encrypted_fields {
            tracing::warn!(
                snapshot_id = %candidate.snapshot_id,
                "snapshot_retention_sweeper: skipping encrypted row"
            );
            sweeper_metrics::failed("encrypted_unsupported");
            report.skipped_encrypted += 1;
            return false;
        }
        let error = self
            .registry
            .validate_route(storage_location.connection_string())
            .err();
        match error {
            Some(error) => {
                record_fresh_route_error(candidate, error, report);
                false
            }
            None => true,
        }
    }

    async fn claim_fresh(
        &self,
        candidate: PruneCandidate,
        keep_n: i64,
        report: &mut TickReport,
    ) -> Option<PruneCandidate> {
        match self
            .sink
            .claim_prune(candidate.clone(), keep_n, self.policy.min_age)
            .await
        {
            Ok(ClaimOutcome::Claimed(claimed)) => {
                tracing::info!(
                    snapshot_id = %candidate.snapshot_id,
                    sandbox_id = %candidate.sandbox_id,
                    "snapshot_retention_sweeper: claimed fresh snapshot for prune"
                );
                Some(claimed)
            }
            Ok(ClaimOutcome::Refused(reason)) => {
                tracing::info!(
                    snapshot_id = %candidate.snapshot_id,
                    sandbox_id = %candidate.sandbox_id,
                    refusal_reason = %reason,
                    "snapshot_retention_sweeper: claim refused at re-validation"
                );
                sweeper_metrics::claim_refused(reason);
                report.drift_skips += 1;
                None
            }
            Err(err) => {
                tracing::warn!(
                    snapshot_id = %candidate.snapshot_id,
                    error = %err,
                    "snapshot_retention_sweeper: claim transaction failed before delete"
                );
                sweeper_metrics::failed("claim");
                report.claim_errors += 1;
                None
            }
        }
    }

    /// Reap and finalize an already-claimed candidate. Failures stay claimed.
    async fn reap_and_finalize(
        &self,
        candidate: &PruneCandidate,
        keep_n: i64,
        resumed: bool,
        report: &mut TickReport,
    ) {
        let Some(delete_outcome) = self.delete_storage(candidate, report).await else {
            return;
        };
        let finalize = self
            .sink
            .finalize_prune(candidate.snapshot_id, keep_n)
            .await;
        record_finalize_result(candidate, resumed, delete_outcome, finalize, report);
    }

    async fn delete_storage(
        &self,
        candidate: &PruneCandidate,
        report: &mut TickReport,
    ) -> Option<DeletePrefixOutcome> {
        let Some(location) = &candidate.storage_location else {
            tracing::error!(
                snapshot_id = %candidate.snapshot_id,
                "snapshot_retention_sweeper: claimed row has no usable storage location; durable bytes may be unaddressable, finalizing the catalog claim without a backend delete"
            );
            return Some(DeletePrefixOutcome::AlreadyGone);
        };
        let delete_started = Instant::now();
        match self
            .registry
            .delete_prefix(location.connection_string())
            .await
        {
            Ok(outcome) => {
                sweeper_metrics::backend_delete_latency(delete_started);
                log_delete_outcome(candidate, &outcome);
                Some(outcome)
            }
            Err(error) => {
                record_claimed_delete_error(candidate, location, error, report);
                None
            }
        }
    }
}

fn record_dry_run_candidate(candidate: &PruneCandidate, report: &mut TickReport) {
    tracing::info!(
        snapshot_id = %candidate.snapshot_id,
        sandbox_id = %candidate.sandbox_id,
        age_secs = candidate.age_secs,
        has_storage_location = candidate.storage_location.is_some(),
        "snapshot_retention_sweeper: DRY RUN — would prune this snapshot"
    );
    sweeper_metrics::would_prune();
    report.would_prune += 1;
}

fn record_fresh_route_error(
    candidate: &PruneCandidate,
    error: DeletePrefixError,
    report: &mut TickReport,
) {
    let failure = RouteFailure::from(error);
    tracing::warn!(
        snapshot_id = %candidate.snapshot_id,
        reason = failure.metric_reason(),
        "snapshot storage route is not eligible; leaving unclaimed"
    );
    sweeper_metrics::failed(failure.metric_reason());
    failure.increment(report);
}

fn record_claimed_delete_error(
    candidate: &PruneCandidate,
    location: &SnapshotStorageLocation,
    error: DeletePrefixError,
    report: &mut TickReport,
) {
    let failure = match &error {
        DeletePrefixError::UnsupportedScheme(_) => RouteFailure::UnsupportedScheme,
        DeletePrefixError::InvalidUri => RouteFailure::InvalidUri,
        DeletePrefixError::OutOfScope { .. } => RouteFailure::OutOfScope,
        DeletePrefixError::Backend(_) => RouteFailure::Backend,
    };
    match error {
        DeletePrefixError::UnsupportedScheme(scheme) => tracing::error!(
            snapshot_id = %candidate.snapshot_id,
            storage_location_id = %location.id,
            scheme = %scheme,
            "claimed snapshot URI scheme has no backend; the claim remains parked"
        ),
        DeletePrefixError::InvalidUri => tracing::error!(
            snapshot_id = %candidate.snapshot_id,
            storage_location_id = %location.id,
            "claimed snapshot URI is invalid; the claim remains parked"
        ),
        DeletePrefixError::OutOfScope { backend, container } => tracing::error!(
            snapshot_id = %candidate.snapshot_id,
            storage_location_id = %location.id,
            backend,
            container_len = container.len(),
            "claimed snapshot route is outside backend scope; the claim remains parked"
        ),
        DeletePrefixError::Backend(error) => tracing::warn!(
            snapshot_id = %candidate.snapshot_id,
            storage_location_id = %location.id,
            // Backend errors may embed endpoint userinfo from the storage
            // connection string. Preserve bounded failure metadata without
            // copying credentials into logs.
            error_bytes = error.len(),
            "claimed snapshot backend delete failed; the claim remains for retry"
        ),
    }
    sweeper_metrics::failed(if failure == RouteFailure::Backend {
        "backend_delete"
    } else {
        "parked_claim"
    });
    failure.increment(report);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteFailure {
    UnsupportedScheme,
    InvalidUri,
    OutOfScope,
    Backend,
}

impl From<DeletePrefixError> for RouteFailure {
    fn from(error: DeletePrefixError) -> Self {
        match error {
            DeletePrefixError::UnsupportedScheme(_) => Self::UnsupportedScheme,
            DeletePrefixError::InvalidUri => Self::InvalidUri,
            DeletePrefixError::OutOfScope { .. } => Self::OutOfScope,
            DeletePrefixError::Backend(_) => Self::Backend,
        }
    }
}

impl RouteFailure {
    fn metric_reason(self) -> &'static str {
        match self {
            Self::UnsupportedScheme => "unsupported_scheme",
            Self::InvalidUri => "parse_uri",
            Self::OutOfScope => "bucket_out_of_scope",
            Self::Backend => "backend_delete",
        }
    }

    fn increment(self, report: &mut TickReport) {
        match self {
            Self::UnsupportedScheme => report.unsupported_scheme += 1,
            Self::InvalidUri => report.parse_errors += 1,
            Self::OutOfScope => report.bucket_out_of_scope += 1,
            Self::Backend => report.backend_errors += 1,
        }
    }
}

fn log_delete_outcome(candidate: &PruneCandidate, outcome: &DeletePrefixOutcome) {
    match outcome {
        DeletePrefixOutcome::Deleted { count } => tracing::info!(
            snapshot_id = %candidate.snapshot_id,
            objects_deleted = count,
            "snapshot_retention_sweeper: cleared storage prefix"
        ),
        DeletePrefixOutcome::AlreadyGone => tracing::info!(
            snapshot_id = %candidate.snapshot_id,
            "snapshot_retention_sweeper: storage prefix was already empty"
        ),
    }
}

fn record_finalize_result(
    candidate: &PruneCandidate,
    resumed: bool,
    delete_outcome: DeletePrefixOutcome,
    finalize: Result<PruneFinalize, sqlx::Error>,
    report: &mut TickReport,
) {
    match finalize {
        Ok(PruneFinalize::Pruned {
            floor_ok,
            marker_ok,
        }) => {
            record_finalize_breaches(candidate, floor_ok, marker_ok, report);
            record_resumed(resumed, report);
            let label = match delete_outcome {
                DeletePrefixOutcome::Deleted { .. } => {
                    report.pruned += 1;
                    "deleted"
                }
                DeletePrefixOutcome::AlreadyGone => {
                    report.already_gone += 1;
                    "already_gone"
                }
            };
            sweeper_metrics::pruned(label);
            emit_prune_event(candidate);
        }
        Ok(PruneFinalize::AlreadyGone) => {
            tracing::info!(snapshot_id = %candidate.snapshot_id, "snapshot row already gone at finalize");
            record_resumed(resumed, report);
            report.drift_skips += 1;
        }
        Ok(PruneFinalize::NotClaimed) => {
            tracing::error!(snapshot_id = %candidate.snapshot_id, "finalize refused an unclaimed snapshot");
            sweeper_metrics::failed("finalize_not_claimed");
            report.finalize_protocol_errors += 1;
        }
        Err(error) => {
            tracing::warn!(snapshot_id = %candidate.snapshot_id, %error, "DB finalize failed after storage delete; will retry");
            sweeper_metrics::failed("db_delete");
            report.db_errors += 1;
        }
    }
}

fn record_finalize_breaches(
    candidate: &PruneCandidate,
    floor_ok: bool,
    marker_ok: bool,
    report: &mut TickReport,
) {
    if !marker_ok {
        tracing::error!(snapshot_id = %candidate.snapshot_id, sandbox_id = %candidate.sandbox_id, "current-artifact invariant breached during finalize");
        sweeper_metrics::current_artifact_breached();
        report.marker_breaches += 1;
    }
    if !floor_ok {
        tracing::error!(snapshot_id = %candidate.snapshot_id, sandbox_id = %candidate.sandbox_id, "keep-N floor breached during finalize");
        sweeper_metrics::floor_breached();
        report.floor_breaches += 1;
    }
}

fn record_resumed(resumed: bool, report: &mut TickReport) {
    if resumed {
        sweeper_metrics::resumed();
        report.resumed += 1;
    }
}

/// Emit a `snapshot_pruned` sandbox event for one pruned snapshot.
/// Pruning a durable snapshot of a live sandbox is an immutable
/// durable-artifact deletion; component identity lives in `source`
/// (`cluster_controller`) and causality in `trigger` (`system`).
fn emit_prune_event(candidate: &PruneCandidate) {
    let details = SandboxEventDetails::default()
        .reason("snapshot_retention_pruned")
        .operation("snapshot_retention_sweep")
        .trigger(SandboxEventTrigger::System)
        .snapshot_id(candidate.snapshot_id.to_string())
        .age_secs(candidate.age_secs.to_string());
    SandboxEvent::new(
        candidate.sandbox_id,
        SandboxEventType::SnapshotPruned,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}
