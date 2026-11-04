//! Warm-pool orphan pod sweep — a bounded actual→desired stage of the
//! warm-pool reconciler ([`crate::warm_pool`]).
//!
//! Unclaimed warm-pool pods that reach a terminal phase (`Succeeded`/`Failed`)
//! but whose slot the pool no longer tracks are never deleted by anything:
//! they are bare pods (`restartPolicy: Never`, no `ownerReferences`, no
//! finalizers), so Kubernetes won't auto-GC them, and the warm-pool
//! reconciler's other stages only walk *live* slot rows. They accumulate until
//! the cluster-wide terminated-pod ceiling. This stage is the reverse
//! (actual→desired) sweep that reclaims them, and drains the existing backlog.
//!
//! It runs as one stage of [`crate::warm_pool`]'s existing reconcile tick —
//! reusing that reconciler's shared pod reflector cache, [`WarmPoolStore`], kube
//! client, and cadence — rather than as a separate loop with its own interval,
//! rolling-window rate-limiter, or dependency-injection seam. Pacing is the
//! per-tick delete cap plus the warm-pool cadence; a wall-clock stage budget
//! keeps it from stalling the tick, and a persisted fairness cursor rotates the
//! (pod-name-sorted) backlog across ticks so the deferred tail isn't starved.
//!
//! ## What it deletes
//! A pod is deleted only when **all** hold: it carries
//! `space.pplx.net/pool-role=warm`; its phase is `Succeeded`/`Failed` and it is
//! not terminating and carries no restore annotation; it has been terminal for
//! at least [`SweepConfig::min_terminal_age`] (read off container `finishedAt`,
//! or `creationTimestamp` for a pod that never ran a container — no timestamp ⇒
//! un-ageable ⇒ skipped); its `pool-slot-id` has no `kubernetes_pool_slots`
//! row; and its `(sandbox_id, pod_name)` has no **active**
//! `sandbox_k8s_executions` row.
//!
//! ## Safety
//! On a *successful* pool claim the slot's `kubernetes_pool_slots` row is
//! deleted **row-only** while the pod lives on as an adopted user sandbox that
//! **keeps its `pool-role=warm` label**. So "warm + no slot row" alone would
//! match a live user sandbox — the active-execution check is the load-bearing
//! guard. It keys on the `active` flag of the `sandbox_k8s_executions` row:
//! suspend, delete, and replace retire the execution (`active = FALSE`), so a
//! suspended sandbox's leaked terminal pod is collectable rather than pinned
//! live forever. Every wake-in-place
//! state (`stopped`/`paused`) keeps its pod non-terminal *and* `active = TRUE`
//! (invisible to this terminal-only sweep and protected here); suspend / delete
//! / replace make a fresh generation on wake, so the retired pod is never
//! reused. The batch scan is a stale pre-filter; the authoritative guard is a
//! **single-statement** recheck ([`WarmPoolStore::orphan_candidate_still_unowned`])
//! run immediately before each delete, failing closed on both "owned again" and
//! query error, so a claim/bind that landed after the scan is caught before the
//! irreversible delete. The delete additionally re-reads the pod (still an
//! orphan-shaped warm pod, unchanged UID) and carries a UID + resourceVersion
//! precondition.
//!
//! Ships default-disabled; when enabled, starts in dry-run. Dry-run runs the
//! whole authoritative path (batch checks + fresh GET + ownership fence) and
//! diverges only at the DELETE, so its would-delete volume is a faithful
//! preview.
//!
//! The safety-bearing logic is pure ([`partition`], [`fresh_verdict`],
//! [`WarmPodSnapshot::from_pod`]) and unit-tested without kube or Postgres; the
//! async [`sweep`] is thin I/O glue over warm-pool's handles.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use jiff::{SignedDuration, Timestamp};
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, DeleteParams, Preconditions};
use pplx_space_api::controllers::substrate::kubernetes::{SandboxPodKey, SandboxPodView};
use pplx_space_cluster_pool::WarmPoolStore;
use pplx_space_common_core::kubernetes::SANDBOX_POOL_ROLE_WARM;
use pplx_space_common_core::types::{PodUid, SandboxId};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_metrics::cluster_reconciler as metrics;
use uuid::Uuid;

use crate::env_config;
use crate::warm_pool::PodCache;

pub(crate) const DEFAULT_MIN_TERMINAL_AGE: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_MAX_DELETES_PER_TICK: u32 = 20;
/// Overall wall-clock budget for the action phase. Kept modest so this stage,
/// which runs inside the warm-pool reconcile tick, can't starve slot cleanup /
/// replenishment while a backlog drains.
pub(crate) const DEFAULT_MAX_STAGE_DURATION: Duration = Duration::from_secs(10);

/// Per-call I/O timeouts, sourced from the warm-pool reconciler's own
/// `io_timeouts`. Bundled so [`sweep`] stays within clippy's argument ceiling.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StageTimeouts {
    pub k8s: Duration,
    pub postgres: Duration,
}

/// Deploy-time knobs for the sweep, rendered under the warm-pool Helm values.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SweepConfig {
    /// Master gate. Off by default everywhere.
    pub enabled: bool,
    /// When true (default), classify + report but never delete.
    pub dry_run: bool,
    /// Grace window a pod must be terminal before it is eligible.
    pub min_terminal_age: Duration,
    /// Ceiling on deletes attempted per warm-pool tick. With the warm-pool
    /// cadence this paces the backlog drain (interval × cap per minute) and
    /// bounds per-tick apiserver load; leftover candidates roll to next tick.
    pub max_deletes_per_tick: u32,
    /// Wall-clock budget for the action phase so a slow apiserver/Postgres
    /// can't stall the warm-pool tick; remaining candidates defer.
    pub max_stage_duration: Duration,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run: true,
            min_terminal_age: DEFAULT_MIN_TERMINAL_AGE,
            max_deletes_per_tick: DEFAULT_MAX_DELETES_PER_TICK,
            max_stage_duration: DEFAULT_MAX_STAGE_DURATION,
        }
    }
}

impl SweepConfig {
    pub(crate) fn from_env() -> Self {
        let default = Self::default();
        Self {
            enabled: env_config::truthy_default(
                "SPACE_WARM_POOL_ORPHAN_GC_ENABLED",
                default.enabled,
            ),
            dry_run: default.dry_run,
            min_terminal_age: default.min_terminal_age,
            max_deletes_per_tick: default.max_deletes_per_tick,
            max_stage_duration: default.max_stage_duration,
        }
    }

    fn mode(&self) -> metrics::Mode {
        if self.dry_run {
            metrics::Mode::DryRun
        } else {
            metrics::Mode::Live
        }
    }
}

/// Flattened, kube-type-free view of a warm-pool pod. Extracted once so the
/// classifier and tests never touch `k8s_openapi` types directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WarmPodSnapshot {
    pub pod_name: String,
    pub pod_uid: Option<PodUid>,
    pub resource_version: Option<String>,
    /// `space.pplx.net/pool-slot-id` → the `kubernetes_pool_slots.id`.
    pub slot_id: Option<Uuid>,
    /// `space.pplx.net/sandbox-id` → the slot's `reserved_sandbox_id`.
    pub sandbox_id: Option<SandboxId>,
    pub is_warm_pool: bool,
    pub is_restore: bool,
    pub terminating: bool,
    pub phase_terminal: bool,
    /// When the pod became terminal: the latest container
    /// `terminated.finishedAt`, or — for a pod that **never ran a container**
    /// (e.g. a `Failed` from kubelet admission, which carries no container
    /// status at all) — its `creationTimestamp`, which then approximates the
    /// failure time. A pod that *did* run a container but has no `finishedAt`
    /// keeps `None` (un-ageable, skipped): using `creationTimestamp` there would
    /// make a long-lived pod that just went terminal look terminal for its whole
    /// lifetime and bypass `min_terminal_age`. `None` ⇒ un-ageable ⇒ skipped.
    pub terminal_since: Option<Timestamp>,
}

impl WarmPodSnapshot {
    pub(crate) fn from_pod(pod: &SandboxPodView) -> Self {
        let slot_id = pod
            .annotation(SandboxPodKey::PoolSlotId)
            .and_then(|raw| Uuid::parse_str(raw).ok());
        let sandbox_id = pod
            .annotation_or_label(SandboxPodKey::SandboxId)
            .and_then(|raw| raw.parse::<SandboxId>().ok());
        let is_warm_pool = is_warm_pool_labelled(pod);
        // A warm slot pod carries no restore annotation; a pod that ever
        // restored is excluded conservatively.
        let is_restore = pod.has_restore_annotation();
        let phase_terminal = pod.is_terminal();

        Self {
            pod_name: pod.name().to_owned(),
            pod_uid: pod.uid().and_then(|uid| uid.parse().ok()),
            resource_version: pod.resource_version().map(str::to_owned),
            slot_id,
            sandbox_id,
            is_warm_pool,
            is_restore,
            terminating: pod.is_deleting(),
            phase_terminal,
            terminal_since: terminal_since(pod),
        }
    }

    /// Terminal warm pod the sweep may consider (age / ownership are the
    /// reconciler's checks, not this predicate).
    pub(crate) fn is_orphan_candidate_shaped(&self) -> bool {
        self.is_warm_pool && self.phase_terminal && !self.terminating && !self.is_restore
    }

    /// How long the pod has been terminal as of `now`, or `None` when it
    /// carries no `finishedAt`. A future timestamp clamps to zero (fails any
    /// positive grace — the safe direction).
    pub(crate) fn terminal_age(&self, now: Timestamp) -> Option<SignedDuration> {
        let since = self.terminal_since?;
        Some(now.duration_since(since).max(SignedDuration::ZERO))
    }
}

/// A shaped, aged candidate carried from the scan into the action loop.
#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub snapshot: WarmPodSnapshot,
    pub slot_id: Uuid,
    pub sandbox_id: SandboxId,
    pub pod_uid: PodUid,
    pub age_secs: i64,
}

/// Categorization of the candidate set against the batch liveness pre-filter.
#[derive(Debug, Default)]
pub(crate) struct Partition {
    /// No slot row and no live execution — the sweep owns these.
    pub orphans: Vec<Candidate>,
    /// Backs a live sandbox execution (active `sandbox_k8s_executions` row) — an
    /// adopted user sandbox. Checked first: the load-bearing guard.
    pub sandbox_bound: Vec<Candidate>,
    /// Slot still tracked by the pool — the pool's own paths own it.
    pub slot_row_live: Vec<Candidate>,
}

/// Pure split of candidates against the batch pre-filter sets. `sandbox_bound`
/// wins over `slot_row_live` when both apply (the stronger live-sandbox signal).
/// `live_pods` is the set of candidate pod names with an active execution.
pub(crate) fn partition(
    candidates: Vec<Candidate>,
    live_slots: &HashSet<Uuid>,
    live_pods: &HashSet<String>,
) -> Partition {
    let mut out = Partition::default();
    for candidate in candidates {
        if live_pods.contains(&candidate.snapshot.pod_name) {
            out.sandbox_bound.push(candidate);
        } else if live_slots.contains(&candidate.slot_id) {
            out.slot_row_live.push(candidate);
        } else {
            out.orphans.push(candidate);
        }
    }
    out
}

/// Verdict of the authoritative fresh pod re-read, against the observed UID.
/// Pure so every branch is unit-tested without a kube client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FreshVerdict {
    /// Still an orphan-shaped warm pod with the observed UID — proceed to the
    /// ownership fence.
    Proceed,
    /// The pod is gone (404).
    AlreadyGone,
    /// A same-name pod from a newer generation replaced it, or the UID changed.
    StaleIdentity,
    /// The same-UID pod recovered / is no longer an orphan-shaped warm pod.
    NoLongerEligible,
}

pub(crate) fn fresh_verdict(
    fresh: Option<&WarmPodSnapshot>,
    observed_uid: &PodUid,
) -> FreshVerdict {
    let Some(fresh) = fresh else {
        return FreshVerdict::AlreadyGone;
    };
    if fresh.pod_uid.as_ref() != Some(observed_uid) {
        return FreshVerdict::StaleIdentity;
    }
    if !fresh.is_orphan_candidate_shaped() {
        return FreshVerdict::NoLongerEligible;
    }
    FreshVerdict::Proceed
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SweepSummary {
    pub scanned_warm_pods: u32,
    pub candidates: u32,
    pub too_young: u32,
    pub deleted: u32,
    pub sandbox_bound: u32,
    pub slot_row_live: u32,
    pub deferred: u32,
    pub errors: u32,
    pub skipped_cache_cold: bool,
    pub db_error: bool,
}

/// Run one bounded orphan sweep as a warm-pool reconcile stage. Reads warm pods
/// from the shared reflector cache, screens them against the two batch DB
/// checks, then per orphan (up to `cfg.max_deletes_per_tick`) fresh-reads,
/// re-checks ownership authoritatively, and deletes — all fail-closed. `no-op`
/// when disabled or the reflector cache is cold.
pub(crate) async fn sweep(
    kube: &Client,
    cache: &PodCache,
    store: &WarmPoolStore,
    namespace: &str,
    cfg: &SweepConfig,
    cursor: &AtomicUsize,
    timeouts: StageTimeouts,
) -> SweepSummary {
    let mut report = SweepSummary::default();
    if !cfg.enabled {
        return report;
    }
    if !cache.ready.is_ready() {
        report.skipped_cache_cold = true;
        tracing::debug!("warm_pool_orphan_sweep: reflector cache not ready; skipping");
        return report;
    }

    let now = Timestamp::now();
    let min_age = SignedDuration::try_from(cfg.min_terminal_age).unwrap_or(SignedDuration::MAX);

    // Snapshot warm pods from the shared cache (label pre-filter before the
    // full extraction so a mostly-live namespace doesn't pay `from_pod` on
    // every non-pool pod).
    let mut candidates: Vec<Candidate> = Vec::new();
    let mut scanned = 0u32;
    for pod in cache.store.state() {
        if !is_warm_pool_labelled(&pod) {
            continue;
        }
        scanned = scanned.saturating_add(1);
        let snapshot = WarmPodSnapshot::from_pod(&pod);
        if !snapshot.is_orphan_candidate_shaped() {
            continue;
        }
        let (Some(slot_id), Some(sandbox_id), Some(pod_uid)) = (
            snapshot.slot_id,
            snapshot.sandbox_id,
            snapshot.pod_uid.clone(),
        ) else {
            tracing::debug!(
                pod_name = %snapshot.pod_name,
                "warm_pool_orphan_sweep: skipping terminal warm pod with missing identity"
            );
            continue;
        };
        match snapshot.terminal_age(now) {
            Some(age) if age >= min_age => {
                candidates.push(Candidate {
                    age_secs: age.as_secs(),
                    snapshot,
                    slot_id,
                    sandbox_id,
                    pod_uid,
                });
            }
            _ => report.too_young += 1,
        }
    }
    report.scanned_warm_pods = scanned;
    report.candidates = u32::try_from(candidates.len()).unwrap_or(u32::MAX);
    if candidates.is_empty() {
        return report;
    }

    // Two batch liveness checks — a stale pre-filter, fail-closed on error.
    let slot_ids: Vec<Uuid> = candidates.iter().map(|c| c.slot_id).collect();
    let sandbox_ids: Vec<SandboxId> = candidates.iter().map(|c| c.sandbox_id).collect();
    let pod_names: Vec<String> = candidates
        .iter()
        .map(|c| c.snapshot.pod_name.clone())
        .collect();
    let live_slots = match timed(timeouts.postgres, store.live_slot_ids(&slot_ids)).await {
        Ok(Ok(set)) => set,
        other => {
            report.db_error = true;
            tracing::warn!(error = ?db_err(other), "warm_pool_orphan_sweep: live-slot check failed; skipping");
            return report;
        }
    };
    let live_pods = match timed(
        timeouts.postgres,
        store.live_execution_pod_names(&sandbox_ids, &pod_names),
    )
    .await
    {
        Ok(Ok(set)) => set,
        other => {
            report.db_error = true;
            tracing::warn!(error = ?db_err(other), "warm_pool_orphan_sweep: live-execution check failed; skipping");
            return report;
        }
    };

    metrics::scanned(
        metrics::Loop::WarmPoolOrphanGc,
        u64::from(report.candidates),
    );
    let split = partition(candidates, &live_slots, &live_pods);
    report.sandbox_bound = u32::try_from(split.sandbox_bound.len()).unwrap_or(u32::MAX);
    report.slot_row_live = u32::try_from(split.slot_row_live.len()).unwrap_or(u32::MAX);
    for c in &split.sandbox_bound {
        emit(cfg, metrics::WarmPoolOrphanGcOutcome::SandboxBound, c, None);
    }
    for c in &split.slot_row_live {
        emit(cfg, metrics::WarmPoolOrphanGcOutcome::SlotRowLive, c, None);
    }

    // Fairness: `Store::state()` is an unordered map, so sort orphans by a
    // stable identity (pod name — unique per pod) before rotating by the
    // persisted cursor. Without the sort a relist could reorder the set and the
    // numeric cursor would not guarantee the deferred tail is ever visited.
    // Rotating this stable order across ticks covers the whole backlog even in
    // dry-run, where nothing is deleted and the set never shrinks.
    let mut orphans = split.orphans;
    orphans.sort_by(|a, b| a.snapshot.pod_name.cmp(&b.snapshot.pod_name));
    let total = orphans.len();
    if total > 0 {
        orphans.rotate_left(cursor.load(Ordering::Relaxed) % total);
    }
    let pods: Api<Pod> = Api::namespaced(kube.clone(), namespace);
    let cap = cfg.max_deletes_per_tick as usize;
    // Overall wall-clock budget for the action phase. This stage runs inside the
    // warm-pool reconcile tick, so an unbounded serial drain would stall slot
    // cleanup/replenishment; each candidate's GET/recheck/DELETE is additionally
    // bounded by this same deadline (see `act_on_orphan`), so the whole stage
    // honors `max_stage_duration` even when a single apiserver/Postgres call
    // hangs. Candidates past the cap or the deadline defer to the next tick.
    let deadline = tokio::time::Instant::now() + cfg.max_stage_duration;
    let mut processed = 0usize;
    for candidate in &orphans {
        if processed >= cap || tokio::time::Instant::now() >= deadline {
            break;
        }
        act_on_orphan(
            &pods,
            store,
            cfg,
            candidate,
            deadline,
            timeouts,
            &mut report,
        )
        .await;
        processed += 1;
    }
    if total > 0 {
        cursor.store(
            cursor.load(Ordering::Relaxed).wrapping_add(processed),
            Ordering::Relaxed,
        );
    }
    // Deferred tail (past the per-tick cap or the stage budget) is reported in
    // aggregate — `scanned` counts the whole surfaced backlog, resolved
    // candidates each emit one `outcome`, and `deferred = scanned - resolved`.
    // Emitting a per-pod outcome for each deferred candidate would be a
    // telemetry storm on a large backlog (up to ~12k, every tick), so it is a
    // single count on the summary log instead.
    let deferred = total.saturating_sub(processed);
    report.deferred = u32::try_from(deferred).unwrap_or(u32::MAX);
    if deferred > 0 {
        tracing::info!(
            deferred,
            processed,
            max_deletes_per_tick = cfg.max_deletes_per_tick,
            max_stage_duration_secs = cfg.max_stage_duration.as_secs(),
            "warm_pool_orphan_sweep: cap/budget reached; deferring rest to next tick"
        );
    }

    report
}

/// Fresh-read, authoritative ownership recheck, then delete one orphan. Every
/// exit is fail-closed: a delete happens only after the fresh read confirms an
/// unchanged orphan-shaped pod and the single-statement recheck confirms no
/// slot row and no binding.
async fn act_on_orphan(
    pods: &Api<Pod>,
    store: &WarmPoolStore,
    cfg: &SweepConfig,
    candidate: &Candidate,
    deadline: tokio::time::Instant,
    timeouts: StageTimeouts,
    report: &mut SweepSummary,
) {
    use metrics::WarmPoolOrphanGcOutcome as O;
    let name = &candidate.snapshot.pod_name;

    let fresh = match timed_at(deadline, timeouts.k8s, pods.get_opt(name)).await {
        Ok(Ok(pod)) => pod.map(|pod| WarmPodSnapshot::from_pod(&pod.into())),
        other => {
            report.errors += 1;
            emit(cfg, O::ReadError, candidate, Some(kube_err(other)));
            return;
        }
    };
    match fresh_verdict(fresh.as_ref(), &candidate.pod_uid) {
        FreshVerdict::Proceed => {}
        FreshVerdict::AlreadyGone => {
            emit(cfg, O::AlreadyGone, candidate, None);
            return;
        }
        FreshVerdict::StaleIdentity => {
            emit(cfg, O::StaleIdentity, candidate, None);
            return;
        }
        FreshVerdict::NoLongerEligible => {
            emit(cfg, O::NoLongerEligible, candidate, None);
            return;
        }
    }

    // Authoritative single-statement ownership fence, immediately before the
    // delete. Fail closed on both "owned again" and query error.
    let unowned = timed_at(
        deadline,
        timeouts.postgres,
        store.orphan_candidate_still_unowned(candidate.slot_id, candidate.sandbox_id, name),
    )
    .await;
    match unowned {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => {
            emit(cfg, O::OwnershipReappeared, candidate, None);
            return;
        }
        other => {
            report.errors += 1;
            emit(cfg, O::RecheckFailed, candidate, Some(db_err(other)));
            return;
        }
    }

    if cfg.dry_run {
        // Dry-run is not a real state transition: metric/log only, no
        // `space.sandbox` cleanup event.
        report.deleted += 1;
        emit(cfg, O::Deleted, candidate, None);
        return;
    }

    // Live deletion is a cleanup state transition, so it emits the required
    // `space.sandbox` events (keyed on the reserved sandbox id): a
    // `cleanup_requested` before the DELETE, then a matching terminal event.
    let sandbox_id = candidate.sandbox_id;
    emit_cleanup(sandbox_id, candidate, CleanupOutcome::Attempting);
    let resource_version = fresh.and_then(|f| f.resource_version);
    let params = DeleteParams {
        preconditions: Some(Preconditions {
            uid: Some(candidate.pod_uid.as_str().to_string()),
            resource_version,
        }),
        ..DeleteParams::default()
    };
    match timed_at(deadline, timeouts.k8s, pods.delete(name, &params)).await {
        Ok(Ok(_)) => {
            report.deleted += 1;
            emit(cfg, O::Deleted, candidate, None);
            emit_cleanup(sandbox_id, candidate, CleanupOutcome::Deleted);
        }
        Ok(Err(kube::Error::Api(err))) if err.code == 404 => {
            emit(cfg, O::AlreadyGone, candidate, None);
            emit_cleanup(sandbox_id, candidate, CleanupOutcome::AlreadyGone);
        }
        Ok(Err(kube::Error::Api(err))) if err.code == 409 => {
            emit(cfg, O::StaleIdentity, candidate, None);
            emit_cleanup(sandbox_id, candidate, CleanupOutcome::SupersededIdentity);
        }
        other => {
            report.errors += 1;
            let err = kube_err(other);
            emit(cfg, O::DeleteFailed, candidate, Some(err.clone()));
            emit_cleanup(sandbox_id, candidate, CleanupOutcome::Failed(err));
        }
    }
}

/// Emit one `cluster_reconciler.outcome` for a candidate plus its paired log.
/// On error outcomes the rendered error is attached to the event so the
/// dashboard→log drilldown carries it (no separate warning line).
fn emit(
    cfg: &SweepConfig,
    outcome: metrics::WarmPoolOrphanGcOutcome,
    candidate: &Candidate,
    error: Option<String>,
) {
    use metrics::WarmPoolOrphanGcOutcome as O;
    let mut event = metrics::OutcomeEvent::warm_pool_orphan_gc(outcome, cfg.mode())
        .pod_name(candidate.snapshot.pod_name.clone())
        .sandbox_id(candidate.sandbox_id)
        .age_secs(u64::try_from(candidate.age_secs).unwrap_or(0));
    if let Some(error) = error {
        event = event.error(error);
    }
    match outcome {
        O::DeleteFailed | O::ReadError | O::RecheckFailed => event.warn(),
        O::Deleted if cfg.mode() == metrics::Mode::Live => event.warn(),
        O::Deleted | O::AlreadyGone => event.info(),
        _ => event.debug(),
    }
}

/// Terminal result of a live orphan-pod delete, for the `space.sandbox`
/// cleanup event. Only live deletes emit these — dry-run stays metric-only.
enum CleanupOutcome {
    Attempting,
    Deleted,
    /// 404 — the pod was already gone; the cleanup goal is met.
    AlreadyGone,
    /// 409 — a delete precondition failed, so the object changed under us and
    /// **no deletion happened**. Not a completed cleanup: report it as failed
    /// and let a later sweep re-evaluate the (possibly superseded) pod fresh.
    SupersededIdentity,
    Failed(String),
}

/// Map a cleanup outcome to its `space.sandbox` event type. Only [`Deleted`]
/// and [`AlreadyGone`] are terminal *successes* (the pod is gone); a 409
/// precondition mismatch ([`SupersededIdentity`]) did not delete anything, so
/// it is a `cleanup_failed` a later sweep retries — emitting `cleanup_completed`
/// there would record a durable success for cleanup that never happened.
///
/// [`Deleted`]: CleanupOutcome::Deleted
/// [`AlreadyGone`]: CleanupOutcome::AlreadyGone
/// [`SupersededIdentity`]: CleanupOutcome::SupersededIdentity
fn cleanup_event_type(outcome: &CleanupOutcome) -> SandboxEventType {
    match outcome {
        CleanupOutcome::Attempting => SandboxEventType::CleanupRequested,
        CleanupOutcome::Deleted | CleanupOutcome::AlreadyGone => SandboxEventType::CleanupCompleted,
        CleanupOutcome::SupersededIdentity | CleanupOutcome::Failed(_) => {
            SandboxEventType::CleanupFailed
        }
    }
}

/// Emit the required `space.sandbox` cleanup event for a live orphan delete,
/// keyed on the reserved sandbox id: `cleanup_requested` (attempting) then a
/// matching `cleanup_completed` / `cleanup_failed`.
fn emit_cleanup(sandbox_id: SandboxId, candidate: &Candidate, outcome: CleanupOutcome) {
    let event_type = cleanup_event_type(&outcome);
    let mut details = SandboxEventDetails::default()
        .reason("orphaned_warm_pool_pod")
        .operation("warm_pool_orphan_gc")
        .trigger(SandboxEventTrigger::System)
        .age_secs(candidate.age_secs.to_string())
        .pod_name(candidate.snapshot.pod_name.clone());
    if let Some(pod_uid) = &candidate.snapshot.pod_uid {
        details = details.pod_uid(pod_uid.as_str());
    }
    details = match outcome {
        CleanupOutcome::Attempting => details.result("attempting"),
        CleanupOutcome::Deleted => details.result("deleted"),
        CleanupOutcome::AlreadyGone => details.result("already_gone"),
        CleanupOutcome::SupersededIdentity => details.result("stale_identity"),
        CleanupOutcome::Failed(error) => details.result("delete_failed").error(error),
    };
    SandboxEvent::new(
        sandbox_id,
        event_type,
        SandboxEventSource::ClusterController,
    )
    .emit_with(details);
}

/// Cheap `pool-role=warm` label check on the cached view.
fn is_warm_pool_labelled(pod: &SandboxPodView) -> bool {
    pod.label(SandboxPodKey::PoolRole) == Some(SANDBOX_POOL_ROLE_WARM)
}

/// When the pod became terminal (see [`WarmPodSnapshot::terminal_since`]): the
/// latest container `finishedAt`, else the `creationTimestamp` **only** for a
/// pod that never ran a container (creation ≈ failure time then), else `None`.
fn terminal_since(pod: &SandboxPodView) -> Option<Timestamp> {
    latest_terminated_at(pod).or_else(|| {
        if never_ran_container(pod) {
            pod.creation_timestamp()
        } else {
            None
        }
    })
}

fn latest_terminated_at(pod: &SandboxPodView) -> Option<Timestamp> {
    pod.container_statuses()
        .app_and_init()
        .filter_map(|state| state.terminated.as_ref()?.finished_at)
        .max()
}

/// True when the pod reports no app or init container status at all — it never
/// ran a container (e.g. a `Failed` from admission), so `creationTimestamp` is
/// a safe terminal-since. A pod that ran always carries container status, so
/// this stays `false` for the long-lived-then-terminal case.
fn never_ran_container(pod: &SandboxPodView) -> bool {
    let statuses = pod.container_statuses();
    statuses.containers.is_empty() && statuses.init_containers.is_empty()
}

/// Wrap one I/O future in a timeout so a hung apiserver / Postgres call can't
/// stall the warm-pool tick. `Ok(inner)` on completion, `Err(Elapsed)` on
/// timeout. Used for the two per-tick batch pre-filter queries.
async fn timed<T, F>(timeout: Duration, future: F) -> Result<T, tokio::time::error::Elapsed>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout(timeout, future).await
}

/// Like [`timed`], but bounded by the earlier of the per-op `timeout` and the
/// absolute stage `deadline` — so a per-candidate GET/recheck/DELETE can never
/// push the action phase past `max_stage_duration`, even if the op's own
/// timeout is larger than the remaining budget.
async fn timed_at<T, F>(
    deadline: tokio::time::Instant,
    timeout: Duration,
    future: F,
) -> Result<T, tokio::time::error::Elapsed>
where
    F: std::future::Future<Output = T>,
{
    let op_deadline = deadline.min(tokio::time::Instant::now() + timeout);
    tokio::time::timeout_at(op_deadline, future).await
}

/// Render the error side of a `Result<Result<_, E>, Elapsed>` for logging.
fn db_err<T>(
    outcome: Result<Result<T, pplx_space_cluster_pool::PoolError>, tokio::time::error::Elapsed>,
) -> String {
    match outcome {
        Ok(Ok(_)) => "none".to_string(),
        Ok(Err(err)) => err.to_string(),
        Err(elapsed) => elapsed.to_string(),
    }
}

fn kube_err<T>(outcome: Result<Result<T, kube::Error>, tokio::time::error::Elapsed>) -> String {
    match outcome {
        Ok(Ok(_)) => "none".to_string(),
        Ok(Err(err)) => err.to_string(),
        Err(elapsed) => elapsed.to_string(),
    }
}

#[cfg(test)]
mod tests;
