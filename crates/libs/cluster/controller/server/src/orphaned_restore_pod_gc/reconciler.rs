//! The reconciler core: the per-tick scan, the batched owning-row
//! classification, the continuous-orphan observation clock, the rolling-window
//! delete budget, the fairness cursor, the per-candidate verify-then-delete
//! action flow, and the `run` entrypoint spawned by the cluster-singleton
//! binary.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::types::{PodUid, SandboxId};
use pplx_space_metrics::cluster_reconciler::{self as metrics, OrphanedRestorePodOutcome as O};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::backend::{DeleteOutcome, KubernetesPodBackend, PodBackend, SandboxStore};
use super::classify::{
    Disposition, RestorePodSnapshot, SandboxRestoreState, SkipClass, classify_disposition,
};
use super::config::ReconcilerConfig;
use super::events::{CleanupOutcome, emit_cleanup_event};
use super::store::SandboxDbStore;

/// Rolling window the `max_deletes_per_minute` budget is measured over.
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
/// Stuck-teardown checks (one authoritative GET each) per tick. The rest stay
/// due and are checked on later ticks.
const STUCK_CHECKS_PER_TICK: usize = 32;
/// Concurrent stuck-teardown GETs.
const STUCK_CHECK_CONCURRENCY: usize = 4;
/// Minimum wall-clock budget for the stuck-teardown checks, even when the
/// tick's own budget is spent (e.g. after a Postgres timeout): they only need
/// Kubernetes, and a zero budget would time every read out unread.
const STUCK_CHECK_MIN_BUDGET: Duration = Duration::from_secs(5);

/// Monotonic clock for the observation age and the rate-limit window. Both
/// measure *elapsed in-process time*, so this must be monotonic — a wall-clock
/// source would let a forward host-clock step make pods look past-threshold
/// (premature reap) and expire rate-limit charges (burst), and a backward step
/// stall the loop. Production uses [`Instant::now`]; tests inject a
/// controllable clock so age and rate-limit windows are deterministic.
pub(crate) type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// One restore pod eligible for classification this tick: not terminating,
/// with a UID (delete precondition + observation-clock key), a sandbox id
/// (owning-row lookup), and an execution generation (the supersession fence).
struct EligiblePod {
    pod: RestorePodSnapshot,
    uid: PodUid,
    sandbox_id: SandboxId,
    generation: i64,
}

/// An orphan past the observation threshold — the actionable set the action
/// phase verifies and deletes.
#[derive(Debug, Clone)]
struct Candidate {
    pod: RestorePodSnapshot,
    uid: PodUid,
    sandbox_id: SandboxId,
    /// Execution generation the pod carries; the supersession fence compares it
    /// to the active execution's on the authoritative recheck.
    generation: i64,
    /// Continuous-orphan observation age, for the emitted event/log. The orphan *reason* is not
    /// carried from the scan: the action phase re-classifies authoritatively and emits that reason,
    /// so the scan-time one would be stale.
    observed_age: Duration,
}

/// A restore pod in teardown: this loop's DELETE was accepted, or the pod was
/// seen terminating. Tracked until the pod leaves the cache (a confirmed reap,
/// when this loop issued the DELETE) so a teardown that never finishes stays
/// visible instead of being skipped as "terminating" forever.
struct PendingTeardown {
    /// Owning sandbox, when the pod carries one (needed for sandbox events).
    sandbox_id: Option<SandboxId>,
    /// Latest snapshot of the pod, for the emitted log/event fields.
    pod: RestorePodSnapshot,
    /// Continuous-orphan age at the DELETE (zero for a pod this loop didn't
    /// delete), for the confirmation event.
    observed_age: Duration,
    /// When the DELETE was accepted, or when this loop first saw the pod
    /// terminating. A controller restart re-adopts terminating pods from the
    /// scan, so a stuck teardown is still reported after a restart.
    since: Instant,
    /// This loop issued the DELETE, so a `cleanup_requested` is open for it and
    /// its disappearance is a confirmed reap.
    requested_by_us: bool,
    /// `teardown_stuck` already reported for this pod (reported once).
    reported_stuck: bool,
    /// Last authoritative read attempted for the pod. Past the stuck threshold
    /// the pod is re-read every `teardown_stuck_after` — after the stuck
    /// report too, so a disappearance the cache missed still settles it, and
    /// after a failed read, so the per-tick cap rotates through the backlog.
    last_checked: Option<Instant>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    /// Restore pods present in the reflector cache this tick.
    pub scanned: u32,
    /// Orphans past the observation threshold (the actionable candidate set).
    pub orphans_detected: u32,
    /// Orphans not yet observed long enough continuously to be eligible.
    pub too_young: u32,
    /// Live: pods this loop deleted that are now confirmed gone. Dry-run:
    /// would-delete.
    pub reaped: u32,
    /// Live DELETEs the apiserver accepted this tick (the pod is now
    /// terminating; `reaped` follows once it's gone).
    pub delete_requested: u32,
    /// Restore pods newly reported as stuck terminating this tick.
    pub teardown_stuck: u32,
    pub delete_failed: u32,
    pub rate_limited: u32,
    /// Restore pods in teardown — terminating, or already deleted by this loop
    /// — tracked for completion instead of classified.
    pub terminating: u32,
    /// Restore pods missing a UID, a sandbox id, or a positive execution
    /// generation (never actionable).
    pub missing_identity: u32,
    /// Non-orphan dispositions by skip class (tallied, not per-pod metered).
    pub skipped: BTreeMap<SkipClass, u32>,
    /// Dry-run only: orphans past the threshold that were already reported as
    /// would-reap on an earlier tick and are not re-reported.
    pub already_previewed: u32,
    /// Candidates left unprocessed because the per-tick wall-clock budget
    /// elapsed; they roll to the next tick.
    pub deferred: u32,
    /// True when the tick returned early because the reflector cache was cold.
    pub cache_not_ready: bool,
}

impl TickReport {
    fn record_skip(&mut self, class: SkipClass) {
        *self.skipped.entry(class).or_default() += 1;
    }

    fn skipped_total(&self) -> u32 {
        self.skipped.values().copied().sum()
    }
}

/// Whether the per-candidate action finished within the tick's wall-clock
/// budget, or an in-flight apiserver call hit the deadline (so the loop should
/// stop and defer the remaining candidates).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFlow {
    Processed,
    /// Shutdown interrupted the wait for a rate-limit slot before the
    /// candidate was touched; it rolls to the deferred tail.
    Cancelled,
    DeadlineExceeded,
}

pub struct Reconciler<P: PodBackend, S: SandboxStore> {
    config: ReconcilerConfig,
    /// `pub(crate)` so the sibling test module can inspect the injected fakes.
    pub(crate) pod_backend: P,
    pub(crate) store: S,
    /// Instants of candidates committed to, within the rolling window, stamped at the moment of
    /// commitment so the cap holds over any real minute. Each committed candidate costs at most one
    /// apiserver GET (verify) plus one DELETE, so this bounds the reaping's apiserver work, not
    /// just successful deletes (stuck-teardown reads are capped separately). Trimmed to `RATE_LIMIT_WINDOW` each tick.
    delete_history: VecDeque<Instant>,
    /// Rotating start offset into the orphan list, advanced by the number of
    /// candidates committed to each tick, so a budget-sized prefix of
    /// always-failing candidates can't starve the ones behind it.
    scan_offset: usize,
    /// When each pod (keyed by UID) was *first observed as an orphan* on the monotonic clock. A pod
    /// is a candidate only once it has classified as an orphan continuously for `threshold`; the
    /// entry is dropped the moment the pod classifies as anything else (the active runtime, an
    /// in-flight restore, a concurrent op) or leaves the cache, so a pod that only transiently
    /// looks like an orphan never accrues deletable age. Resets on controller restart (orphans
    /// re-age over one threshold), the safe direction.
    first_seen_orphan: HashMap<PodUid, Instant>,
    /// Dry-run only: pods (by UID) already reported as would-reap and still
    /// orphaned. Keeps the preview at one would-reap per pod, as live mode
    /// deletes once. Pruned whenever a pod stops classifying as an orphan or
    /// leaves the cache.
    dry_run_previewed: HashSet<PodUid>,
    /// Restore pods in teardown, by UID (see [`PendingTeardown`]). Pruned as
    /// pods leave the cache.
    pending_teardown: HashMap<PodUid, PendingTeardown>,
    /// Cancels an in-tick wait for a rate-limit slot, so shutdown doesn't sit
    /// out the rest of a backlog tick's budget.
    shutdown: CancellationToken,
    clock: Clock,
}

impl<P: PodBackend, S: SandboxStore> Reconciler<P, S> {
    pub fn new(config: ReconcilerConfig, pod_backend: P, store: S) -> Self {
        Self {
            config,
            pod_backend,
            store,
            delete_history: VecDeque::new(),
            scan_offset: 0,
            first_seen_orphan: HashMap::new(),
            dry_run_previewed: HashSet::new(),
            pending_teardown: HashMap::new(),
            shutdown: CancellationToken::new(),
            clock: Arc::new(Instant::now),
        }
    }

    pub(crate) fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Seed the observation clock for a pod UID, so a single-tick test can
    /// treat a pod as already-observed-orphan for a given duration.
    #[cfg(test)]
    pub(crate) fn observe_since(&mut self, pod_uid: &str, since: Instant) {
        let uid = pod_uid.parse().expect("test pod uid must be non-empty");
        self.first_seen_orphan.insert(uid, since);
    }

    /// Number of pods currently on the observation clock — lets a test assert
    /// the clock was reset after an unverifiable tick.
    #[cfg(test)]
    pub(crate) fn observed_count(&self) -> usize {
        self.first_seen_orphan.len()
    }

    fn metric_mode(&self) -> metrics::Mode {
        if self.config.dry_run {
            metrics::Mode::DryRun
        } else {
            metrics::Mode::Live
        }
    }

    /// Emit one `cluster_reconciler.outcome` for a resolved orphan candidate,
    /// paired with a log carrying the identifiers. Destructive/error outcomes
    /// log at `warn`, benign resolutions at `info`, routine skips/defers at
    /// `debug`.
    fn emit_outcome(&self, outcome: O, candidate: &Candidate, pod: &RestorePodSnapshot) {
        self.emit_event(outcome, candidate, pod, None, None);
    }

    /// `pod` supplies the pod-derived log fields (`node`, `generation`); pass
    /// the authoritative fresh snapshot once a verify GET has run so a pod that
    /// bound between the cached scan and the GET is recorded as bound, and the
    /// cached scan snapshot only before then.
    fn emit_event(
        &self,
        outcome: O,
        candidate: &Candidate,
        pod: &RestorePodSnapshot,
        detail: Option<&str>,
        err: Option<&dyn std::fmt::Display>,
    ) {
        self.emit_pod_event(
            outcome,
            Some(candidate.sandbox_id),
            candidate.observed_age,
            pod,
            detail,
            err,
        );
    }

    fn emit_pod_event(
        &self,
        outcome: O,
        sandbox_id: Option<SandboxId>,
        age: Duration,
        pod: &RestorePodSnapshot,
        detail: Option<&str>,
        err: Option<&dyn std::fmt::Display>,
    ) {
        let mode = self.metric_mode();
        let mut event = metrics::OutcomeEvent::orphaned_restore_pod_gc(outcome, mode)
            .pod_name(pod.pod_name.clone())
            .age_secs(age.as_secs());
        if let Some(sandbox_id) = sandbox_id {
            event = event.sandbox_id(sandbox_id);
        }
        if let Some(generation) = pod.generation {
            event = event.generation(generation);
        }
        if let Some(node) = &pod.node_name {
            event = event.node(node);
        }
        if let Some(detail) = detail {
            event = event.detail(detail);
        }
        if let Some(err) = err {
            event = event.error(err);
        }
        match outcome {
            O::DeleteFailed | O::ReadError | O::TimedOut | O::TeardownStuck => event.warn(),
            // The accepted DELETE is the destructive action; its confirmation
            // and a dry-run would-reap are informational.
            O::DeleteRequested => event.warn(),
            O::Reaped => event.info(),
            _ => event.debug(),
        }
    }

    /// Drop rate-limit charges at or older than the rolling window relative to
    /// `now`.
    fn trim_rate_window(&mut self, now: Instant) {
        if let Some(cutoff) = now.checked_sub(RATE_LIMIT_WINDOW) {
            while self.delete_history.front().is_some_and(|ts| *ts <= cutoff) {
                self.delete_history.pop_front();
            }
        }
    }

    /// How long until the oldest rate-limit charge leaves the window.
    fn next_slot_in(&self, now: Instant) -> Option<Duration> {
        let oldest = *self.delete_history.front()?;
        Some((oldest + RATE_LIMIT_WINDOW).saturating_duration_since(now))
    }

    fn delete_budget_remaining(&self) -> usize {
        let cap = usize::try_from(self.config.max_deletes_per_minute).unwrap_or(usize::MAX);
        cap.saturating_sub(self.delete_history.len())
    }

    /// Classify the owning rows for `ids` in `batch_size` chunks so a single
    /// `ANY($1)` query never carries an unbounded id array. Returns the merged
    /// map; an error from any chunk aborts the tick (the whole classification
    /// pass retries next tick).
    async fn classify_all(
        &self,
        ids: &[SandboxId],
    ) -> Result<HashMap<SandboxId, SandboxRestoreState>, sqlx::Error> {
        let chunk = usize::try_from(self.config.batch_size)
            .unwrap_or(usize::MAX)
            .max(1);
        let mut states = HashMap::with_capacity(ids.len());
        for window in ids.chunks(chunk) {
            states.extend(self.store.classify_sandboxes(window).await?);
        }
        Ok(states)
    }

    /// Record a restore pod in teardown. Returns true when it has stayed
    /// terminating in the cache past `teardown_stuck_after` and hasn't been
    /// reported yet — the caller confirms that with an authoritative read
    /// ([`Self::report_stuck_teardowns`]) before reporting it.
    fn track_teardown(&mut self, uid: PodUid, pod: RestorePodSnapshot, now: Instant) -> bool {
        let stuck_after = self.config.teardown_stuck_after.get();
        let entry = self
            .pending_teardown
            .entry(uid)
            .or_insert_with(|| PendingTeardown {
                sandbox_id: pod.sandbox_id,
                pod: pod.clone(),
                observed_age: Duration::ZERO,
                since: now,
                requested_by_us: false,
                reported_stuck: false,
                last_checked: None,
            });
        entry.pod = pod;
        let past_threshold = now.duration_since(entry.since) >= stuck_after;
        let recheck_due = entry
            .last_checked
            .is_none_or(|checked| now.duration_since(checked) >= stuck_after);
        past_threshold && recheck_due
    }

    /// Report teardowns the cache still holds past the stuck threshold — after
    /// an authoritative read, since a stalled watch can keep a deleted pod in
    /// the cache indefinitely. Gone (or replaced by a different UID) settles it
    /// as completed; still there reports `teardown_stuck` once; a failed or
    /// timed-out read retries next tick.
    async fn report_stuck_teardowns(
        &mut self,
        mut due: Vec<PodUid>,
        now: Instant,
        deadline: tokio::time::Instant,
        report: &mut TickReport,
    ) {
        use futures_util::StreamExt;

        // First reports before periodic re-reads, so a backlog of pods already
        // reported stuck can't delay a new one.
        due.sort_by_key(|uid| {
            self.pending_teardown
                .get(uid)
                .is_some_and(|t| t.reported_stuck)
        });
        due.truncate(STUCK_CHECKS_PER_TICK);
        let deadline = deadline.max(tokio::time::Instant::now() + STUCK_CHECK_MIN_BUDGET);
        let targets: Vec<(PodUid, String)> = due
            .into_iter()
            .filter_map(|uid| {
                let pod_name = self.pending_teardown.get(&uid)?.pod.pod_name.clone();
                Some((uid, pod_name))
            })
            .collect();
        let backend = &self.pod_backend;
        let reads: Vec<_> = futures_util::stream::iter(targets)
            .map(|(uid, pod_name)| async move {
                let read =
                    tokio::time::timeout_at(deadline, backend.read_pod_fresh(&pod_name)).await;
                (uid, pod_name, read)
            })
            .buffer_unordered(STUCK_CHECK_CONCURRENCY)
            .collect()
            .await;
        for (uid, pod_name, read) in reads {
            // Every attempted read, whatever its result, moves this pod to the
            // back of the queue for a `teardown_stuck_after`, so a prefix of
            // failing reads can't hold the per-tick cap forever.
            if let Some(entry) = self.pending_teardown.get_mut(&uid) {
                entry.last_checked = Some(now);
            }
            match read {
                Ok(Ok(Some(fresh))) if fresh.pod_uid.as_ref() == Some(&uid) => {
                    let Some(entry) = self.pending_teardown.get_mut(&uid) else {
                        continue;
                    };
                    entry.pod = fresh;
                    if entry.reported_stuck {
                        // Still there on a periodic re-read; already reported.
                        continue;
                    }
                    entry.reported_stuck = true;
                    let (sandbox_id, pod, requested_by_us) =
                        (entry.sandbox_id, entry.pod.clone(), entry.requested_by_us);
                    let stuck_for = now.duration_since(entry.since);
                    report.teardown_stuck += 1;
                    if requested_by_us {
                        // This loop's DELETE hasn't finished: its own operation
                        // is failing, so it's an `error` outcome and closes the
                        // open `cleanup_requested`.
                        self.emit_pod_event(
                            O::TeardownStuck,
                            sandbox_id,
                            stuck_for,
                            &pod,
                            None,
                            None,
                        );
                        if let Some(sandbox_id) = sandbox_id {
                            emit_cleanup_event(
                                sandbox_id,
                                &pod,
                                stuck_for,
                                CleanupOutcome::TeardownStuck,
                            );
                        }
                    } else {
                        // Someone else's delete (or one from before a restart):
                        // an observation, not this loop's failure, so a log
                        // rather than an `error` outcome that would page on
                        // every node outage.
                        tracing::warn!(
                            pod_name = pod.pod_name,
                            sandbox_id = ?sandbox_id,
                            node = ?pod.node_name,
                            stuck_secs = stuck_for.as_secs(),
                            "orphaned_restore_pod_gc: restore pod stuck terminating"
                        );
                    }
                }
                // Absent, or the name now belongs to a different pod: this one
                // is gone even though the cache hasn't caught up.
                Ok(Ok(_)) => self.settle_teardown(&uid, report),
                // Unverifiable this tick (read error, or the tick's budget ran
                // out): checked again once its turn comes back round.
                Ok(Err(err)) => {
                    tracing::debug!(
                        error = %err,
                        pod_name,
                        "orphaned_restore_pod_gc: stuck-teardown read failed; retrying next tick"
                    );
                }
                Err(_elapsed) => {
                    tracing::debug!(
                        pod_name,
                        "orphaned_restore_pod_gc: tick budget ran out before the stuck-teardown read; retrying next tick"
                    );
                }
            }
        }
    }

    /// Settle teardowns whose pod has left the cache: for a pod this loop
    /// deleted, that's the confirmed reap (`cleanup_completed` + `reaped`).
    fn settle_teardowns(&mut self, present: &HashSet<PodUid>, report: &mut TickReport) {
        let gone: Vec<PodUid> = self
            .pending_teardown
            .keys()
            .filter(|uid| !present.contains(*uid))
            .cloned()
            .collect();
        for uid in gone {
            self.settle_teardown(&uid, report);
        }
    }

    /// The pod behind a tracked teardown is gone: drop it, and for a pod this
    /// loop deleted, confirm the reap.
    fn settle_teardown(&mut self, uid: &PodUid, report: &mut TickReport) {
        let Some(teardown) = self.pending_teardown.remove(uid) else {
            return;
        };
        let Some(sandbox_id) = teardown.sandbox_id.filter(|_| teardown.requested_by_us) else {
            return;
        };
        emit_cleanup_event(
            sandbox_id,
            &teardown.pod,
            teardown.observed_age,
            CleanupOutcome::Deleted,
        );
        self.emit_pod_event(
            O::Reaped,
            Some(sandbox_id),
            teardown.observed_age,
            &teardown.pod,
            None,
            None,
        );
        report.reaped += 1;
    }

    pub async fn tick(&mut self) -> TickReport {
        let now = (self.clock)();
        // One wall-clock deadline for the whole tick — the scan, the owning-row
        // classification, and the per-candidate action phase share it, so a
        // stalled Postgres can't hold the loop (or delay shutdown)
        // indefinitely. A classification that exceeds it is treated like a
        // failed one: nothing is verified, so nothing is acted on (fail closed).
        let deadline = tokio::time::Instant::now() + self.config.tick_budget;
        let mut report = TickReport::default();
        if !self.pod_backend.caches_ready() {
            report.cache_not_ready = true;
            tracing::debug!("orphaned_restore_pod_gc: reflector cache not ready; skipping tick");
            // Can't verify any disposition this tick, so drop the observation
            // clock: a cache outage must not count as continuous-orphan time,
            // or a legitimate pod still in its materialization window could
            // cross the threshold the instant the cache recovers. Orphans
            // re-age from scratch — the safe direction.
            self.first_seen_orphan.clear();
            return report;
        }
        self.trim_rate_window(now);

        let pods = self.pod_backend.list_restore_pods().await;
        report.scanned = u32::try_from(pods.len()).unwrap_or(u32::MAX);

        let mut eligible: Vec<EligiblePod> = Vec::new();
        let mut present: HashSet<PodUid> = HashSet::with_capacity(pods.len());
        let mut stuck_due: Vec<PodUid> = Vec::new();
        for pod in pods {
            if let Some(uid) = &pod.pod_uid {
                present.insert(uid.clone());
            }
            let in_teardown = pod
                .pod_uid
                .as_ref()
                .is_some_and(|uid| self.pending_teardown.contains_key(uid));
            if pod.terminating || in_teardown {
                // In teardown: track it to completion rather than classify it
                // again. A pod the cache still shows as not terminating right
                // after this loop's DELETE lands here too.
                report.terminating += 1;
                if let Some(uid) = pod.pod_uid.clone()
                    && self.track_teardown(uid.clone(), pod, now)
                {
                    stuck_due.push(uid);
                }
                continue;
            }
            let (Some(uid), Some(sandbox_id), Some(generation)) =
                (pod.pod_uid.clone(), pod.sandbox_id, pod.generation)
            else {
                report.missing_identity += 1;
                continue;
            };
            eligible.push(EligiblePod {
                pod,
                uid,
                sandbox_id,
                generation,
            });
        }

        self.settle_teardowns(&present, &mut report);

        // Batched owning-row classification. One id per sandbox (dedup so many
        // pods on one sandbox don't blow up the id array).
        let mut ids: Vec<SandboxId> = eligible.iter().map(|e| e.sandbox_id).collect();
        ids.sort_unstable();
        ids.dedup();
        let classified = tokio::time::timeout_at(deadline, self.classify_all(&ids)).await;
        let states = match classified {
            Ok(Ok(states)) => states,
            Err(_elapsed) => {
                tracing::warn!(
                    budget_ms = self.config.tick_budget.as_millis(),
                    "orphaned_restore_pod_gc: classify query exceeded the tick budget"
                );
                // Same as a failed query: no verified observation this tick.
                self.first_seen_orphan.clear();
                self.report_stuck_teardowns(stuck_due, now, deadline, &mut report)
                    .await;
                return report;
            }
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "orphaned_restore_pod_gc: classify query failed");
                // Same reasoning as the cold-cache return: a DB outage must not
                // count as continuous-orphan observation. Drop the clock so
                // orphans re-age from scratch after recovery. Stuck-teardown
                // checks need only Kubernetes, so they still run.
                self.first_seen_orphan.clear();
                self.report_stuck_teardowns(stuck_due, now, deadline, &mut report)
                    .await;
                return report;
            }
        };

        // Disposition + observation-clock management.
        let threshold = self.config.threshold.get();
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut still_orphan: HashSet<PodUid> = HashSet::new();
        for eligible in eligible {
            match classify_disposition(states.get(&eligible.sandbox_id), eligible.generation) {
                Disposition::Orphaned(_reason) => {
                    still_orphan.insert(eligible.uid.clone());
                    let first_seen = *self
                        .first_seen_orphan
                        .entry(eligible.uid.clone())
                        .or_insert(now);
                    let observed = now.duration_since(first_seen);
                    if observed >= threshold
                        && self.config.dry_run
                        && self.dry_run_previewed.contains(&eligible.uid)
                    {
                        report.already_previewed += 1;
                    } else if observed >= threshold {
                        candidates.push(Candidate {
                            pod: eligible.pod,
                            uid: eligible.uid,
                            sandbox_id: eligible.sandbox_id,
                            generation: eligible.generation,
                            observed_age: observed,
                        });
                    } else {
                        report.too_young += 1;
                    }
                }
                Disposition::Skip(class) => {
                    self.first_seen_orphan.remove(&eligible.uid);
                    report.record_skip(class);
                }
            }
        }
        // Drop observation-clock and preview entries for pods no longer orphan
        // or gone.
        self.first_seen_orphan
            .retain(|uid, _| still_orphan.contains(uid));
        self.dry_run_previewed
            .retain(|uid| still_orphan.contains(uid));

        report.orphans_detected = u32::try_from(candidates.len()).unwrap_or(u32::MAX);
        // Emit `scanned` only on ticks that surfaced actionable orphans,
        // matching the family convention that the series doesn't exist until a
        // loop first finds work; each surfaced orphan resolves to exactly one
        // `outcome` this tick (including `deferred` for the budget tail). An
        // accepted DELETE gets its confirmation (`reaped`) or `teardown_stuck`
        // on a later tick, outside this count.
        if report.orphans_detected > 0 {
            metrics::scanned(
                metrics::Loop::OrphanedRestorePodGc,
                u64::from(report.orphans_detected),
            );
        }

        let candidate_count = candidates.len();
        if candidate_count > 0 {
            // Stable order first: the reflector's iteration order is a hash
            // map's and shifts with churn, so rotating it wouldn't anchor the
            // cursor to the same pods across ticks.
            candidates.sort_unstable_by(|a, b| a.uid.as_str().cmp(b.uid.as_str()));
            candidates.rotate_left(self.scan_offset % candidate_count);
        }
        let mut committed = 0usize;
        let mut processed = 0usize;
        let mut last_committed_pos = 0usize;
        for candidate in &candidates {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let committed_before = committed;
            let flow = self
                .handle_orphan(candidate, deadline, &mut committed, &mut report)
                .await;
            if flow == CandidateFlow::Cancelled {
                // Untouched: leave it (and the rest) to the deferred tail.
                break;
            }
            processed += 1;
            if committed > committed_before {
                last_committed_pos = processed;
            }
            if flow == CandidateFlow::DeadlineExceeded {
                break;
            }
        }
        report.deferred =
            u32::try_from(candidate_count.saturating_sub(processed)).unwrap_or(u32::MAX);
        for candidate in candidates.iter().skip(processed) {
            self.emit_outcome(O::Deferred, candidate, &candidate.pod);
        }
        if report.deferred > 0 {
            tracing::warn!(
                deferred = report.deferred,
                tick_budget_secs = self.config.tick_budget.as_secs(),
                "orphaned_restore_pod_gc: tick budget elapsed; deferring remaining orphans"
            );
        }

        let advance = if report.deferred > 0 {
            processed
        } else {
            last_committed_pos
        };
        self.scan_offset = self.scan_offset.wrapping_add(advance);

        // Stuck-teardown checks come last, on whatever budget the orphan work
        // left, so a slow or stalled read can't starve reaping.
        self.report_stuck_teardowns(stuck_due, now, deadline, &mut report)
            .await;

        report
    }

    async fn handle_orphan(
        &mut self,
        candidate: &Candidate,
        deadline: tokio::time::Instant,
        committed: &mut usize,
        report: &mut TickReport,
    ) -> CandidateFlow {
        // Re-trim against the current clock so charges that crossed the window
        // free their slots before this budget check.
        self.trim_rate_window((self.clock)());
        if self.delete_budget_remaining() == 0 {
            // The budget is spent, but the oldest charge may expire moments
            // from now (the previous tick's commits were stamped slightly
            // later in its tick than this one has reached). Wait for it if it
            // frees before this tick's deadline, so a backlog drains at the
            // configured rate instead of losing whole ticks to that skew.
            if let Some(wait) = self.next_slot_in((self.clock)()) {
                let wake = tokio::time::Instant::now() + wait;
                if wake < deadline {
                    tokio::select! {
                        biased;
                        () = self.shutdown.cancelled() => return CandidateFlow::Cancelled,
                        () = tokio::time::sleep_until(wake) => {}
                    }
                    self.trim_rate_window((self.clock)());
                }
            }
        }
        if self.delete_budget_remaining() == 0 {
            self.emit_outcome(O::RateLimited, candidate, &candidate.pod);
            report.rate_limited += 1;
            return CandidateFlow::Processed;
        }
        // Charge a slot the moment we commit to touching the apiserver — each
        // committed candidate costs at most one GET plus one DELETE. Dry-run
        // charges too so its projected volume matches live mode.
        self.delete_history.push_back((self.clock)());
        *committed += 1;

        // Verify before destroy: authoritative GET, bounded by the deadline.
        let read = tokio::time::timeout_at(
            deadline,
            self.pod_backend.read_pod_fresh(&candidate.pod.pod_name),
        )
        .await;
        let fresh = match read {
            Ok(Ok(Some(fresh))) => fresh,
            Ok(Ok(None)) => {
                self.first_seen_orphan.remove(&candidate.uid);
                self.emit_outcome(O::AlreadyGone, candidate, &candidate.pod);
                return CandidateFlow::Processed;
            }
            // An unverifiable read, like an unverifiable recheck, must not
            // count as continuous-orphan time: drop the clock so the pod
            // re-ages a full threshold of verified observation.
            Ok(Err(err)) => {
                self.first_seen_orphan.remove(&candidate.uid);
                self.emit_event(O::ReadError, candidate, &candidate.pod, None, Some(&err));
                return CandidateFlow::Processed;
            }
            Err(_elapsed) => {
                self.first_seen_orphan.remove(&candidate.uid);
                self.emit_outcome(O::TimedOut, candidate, &candidate.pod);
                return CandidateFlow::DeadlineExceeded;
            }
        };
        // From here every emission uses `fresh`, the authoritative snapshot: a
        // pod that bound between the cached scan and this GET must be recorded
        // as bound (an unrouted VM started), not as the pre-bind inert pod the
        // cache saw.
        if fresh.pod_uid.as_ref() != Some(&candidate.uid) {
            // A same-name pod from a newer generation took this one's place;
            // its observation-clock entry is dead.
            self.first_seen_orphan.remove(&candidate.uid);
            self.emit_outcome(O::StaleIdentity, candidate, &fresh);
            return CandidateFlow::Processed;
        }
        if fresh.terminating {
            // Already being deleted by someone else; nothing left to reap.
            self.first_seen_orphan.remove(&candidate.uid);
            self.emit_outcome(O::AlreadyGone, candidate, &fresh);
            return CandidateFlow::Processed;
        }
        if !fresh.is_restore
            || fresh.sandbox_id != Some(candidate.sandbox_id)
            || fresh.generation != Some(candidate.generation)
        {
            // Same UID, but the identity the scan classified (restore marker,
            // owning sandbox, execution generation) no longer holds, so the
            // cached verdict says nothing about this object. Drop its clock so
            // it re-ages under whatever it now is.
            self.first_seen_orphan.remove(&candidate.uid);
            self.emit_outcome(O::StaleIdentity, candidate, &fresh);
            return CandidateFlow::Processed;
        }

        // Authoritative recheck against the owning row (unlocked — see below),
        // bounded by the tick deadline. Re-reads the active execution's
        // generation so the supersession verdict is current, and catches a row
        // that a concurrent operation moved back to `restoring` (a
        // same-generation re-drive) or that became this pod's active runtime
        // between the cached scan and now.
        let recheck = match tokio::time::timeout_at(
            deadline,
            self.store.classify_sandboxes(&[candidate.sandbox_id]),
        )
        .await
        {
            Ok(Ok(states)) => {
                classify_disposition(states.get(&candidate.sandbox_id), candidate.generation)
            }
            Ok(Err(err)) => {
                // An unverifiable recheck must not count as continuous-orphan
                // time (the same rule as a failed scan), or the pod could be
                // deleted on the next tick without a full threshold of
                // verified observation.
                self.first_seen_orphan.remove(&candidate.uid);
                self.emit_event(
                    O::ReadError,
                    candidate,
                    &fresh,
                    Some("db_recheck"),
                    Some(&err),
                );
                return CandidateFlow::Processed;
            }
            Err(_elapsed) => {
                self.first_seen_orphan.remove(&candidate.uid);
                self.emit_outcome(O::TimedOut, candidate, &fresh);
                return CandidateFlow::DeadlineExceeded;
            }
        };
        let reason = match recheck {
            Disposition::Orphaned(reason) => reason,
            Disposition::Skip(class) => {
                self.first_seen_orphan.remove(&candidate.uid);
                self.emit_event(
                    O::NotOrphanOnRecheck,
                    candidate,
                    &fresh,
                    Some(class.as_str()),
                    None,
                );
                return CandidateFlow::Processed;
            }
        };

        // Dry-run: report the would-reap. Not a real state transition, so metric
        // + log only, no `space.sandbox` cleanup event.
        if self.config.dry_run {
            self.emit_event(O::Reaped, candidate, &fresh, Some(reason.as_str()), None);
            self.dry_run_previewed.insert(candidate.uid.clone());
            report.reaped += 1;
            return CandidateFlow::Processed;
        }

        // Live delete. No lock is held across it: the recheck confirmed the pod
        // is *provably superseded* — a strictly-older execution generation, a
        // `suspended`/`failed` row with no active execution, or a deleted row —
        // and generations are monotonic, so no current or future restore can
        // adopt this pod. The delete is therefore safe under any concurrency: a
        // restore racing in, or a DELETE cancelled by the deadline yet still
        // applied by the apiserver, can only remove this unadoptable pod, never
        // a live runtime. `cleanup_requested` precedes the DELETE (evidence of
        // the attempt even on a mid-delete crash); the UID + resourceVersion
        // precondition makes a same-name newer pod or a status change a benign
        // conflict rather than a collateral removal.
        emit_cleanup_event(
            candidate.sandbox_id,
            &fresh,
            candidate.observed_age,
            CleanupOutcome::Attempting,
        );
        let delete = tokio::time::timeout_at(
            deadline,
            self.pod_backend.delete_pod(
                &candidate.pod.pod_name,
                &candidate.uid,
                fresh.resource_version.as_deref(),
            ),
        )
        .await;
        match delete {
            Ok(Ok(DeleteOutcome::Deleted)) => {
                // Accepted, not finished: the pod is now terminating and its VM
                // in node-side teardown. The open `cleanup_requested` stays
                // open until the pod is observed gone (`settle_teardowns`), or
                // is reported stuck (`track_teardown`).
                self.first_seen_orphan.remove(&candidate.uid);
                self.pending_teardown.insert(
                    candidate.uid.clone(),
                    PendingTeardown {
                        sandbox_id: Some(candidate.sandbox_id),
                        pod: fresh.clone(),
                        observed_age: candidate.observed_age,
                        since: (self.clock)(),
                        requested_by_us: true,
                        reported_stuck: false,
                        last_checked: None,
                    },
                );
                self.emit_event(
                    O::DeleteRequested,
                    candidate,
                    &fresh,
                    Some(reason.as_str()),
                    None,
                );
                report.delete_requested += 1;
            }
            Ok(Ok(DeleteOutcome::NotFound)) => {
                self.first_seen_orphan.remove(&candidate.uid);
                emit_cleanup_event(
                    candidate.sandbox_id,
                    &fresh,
                    candidate.observed_age,
                    CleanupOutcome::AlreadyGone,
                );
                self.emit_outcome(O::AlreadyGone, candidate, &fresh);
            }
            Ok(Ok(DeleteOutcome::StaleIdentity)) => {
                // UID+resourceVersion precondition failed (409): the pod at this
                // name/version is no longer the one we verified. Close the
                // `cleanup_requested` emitted above as `cleanup_failed` — the
                // same pod may still exist — (so retries don't strand
                // dangling in-progress cleanups in the timeline) and keep the
                // observation clock so the next tick re-reads the fresh
                // resourceVersion and deletes, rather than re-aging a full
                // threshold. The metric records the benign `cas_lost` outcome.
                emit_cleanup_event(
                    candidate.sandbox_id,
                    &fresh,
                    candidate.observed_age,
                    CleanupOutcome::StaleIdentity,
                );
                self.emit_outcome(O::StaleIdentity, candidate, &fresh);
            }
            Ok(Err(err)) => {
                emit_cleanup_event(
                    candidate.sandbox_id,
                    &fresh,
                    candidate.observed_age,
                    CleanupOutcome::Failed(err.to_string()),
                );
                self.emit_event(
                    O::DeleteFailed,
                    candidate,
                    &fresh,
                    Some(reason.as_str()),
                    Some(&err),
                );
                report.delete_failed += 1;
            }
            Err(_elapsed) => {
                // `cleanup_requested` was emitted, so close it with the timeout.
                emit_cleanup_event(
                    candidate.sandbox_id,
                    &fresh,
                    candidate.observed_age,
                    CleanupOutcome::TimedOut,
                );
                self.emit_outcome(O::TimedOut, candidate, &fresh);
                return CandidateFlow::DeadlineExceeded;
            }
        }
        CandidateFlow::Processed
    }
}

/// Run the orphaned-restore-pod GC loop until the shutdown token fires.
/// Spawned by the cluster-singleton `space-cluster-controller` binary.
///
/// Unlike the stuck-ContainerCreating GC, this reaper needs no DB-convergence
/// companion: it only reaps a pod the owning row has provably superseded (row
/// deleted, no active execution, or a strictly-higher active generation, on a
/// non-transient row), so the row already reflects the desired state and deleting the stranded pod
/// converges Kubernetes to it without leaving a lifecycle for another
/// reconciler to retire. Ships default-disabled; when enabled, starts in
/// dry-run.
pub async fn run(
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "orphaned_restore_pod_gc disabled \
             (SPACE_ORPHANED_RESTORE_POD_GC_ENABLED unset); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        interval_secs = config.interval.as_secs(),
        threshold_secs = config.threshold.get().as_secs(),
        max_deletes_per_minute = config.max_deletes_per_minute,
        tick_budget_secs = config.tick_budget.as_secs(),
        batch_size = config.batch_size,
        teardown_stuck_after_secs = config.teardown_stuck_after.get().as_secs(),
        dry_run = config.dry_run,
        "starting orphaned_restore_pod_gc"
    );

    let pod_backend = KubernetesPodBackend::new(kube);
    let store = SandboxDbStore::new(pool);
    let mut reconciler = Reconciler::new(config.clone(), pod_backend, store)
        .with_shutdown(shutdown_token.child_token());

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so startup doesn't race the reflector sync.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let report = reconciler.tick().await;
                if report.cache_not_ready {
                    tracing::info!("orphaned_restore_pod_gc: cache not ready; tick skipped");
                } else {
                    tracing::info!(
                        scanned = report.scanned,
                        orphans_detected = report.orphans_detected,
                        too_young = report.too_young,
                        already_previewed = report.already_previewed,
                        reaped_or_would_reap = report.reaped,
                        delete_requested = report.delete_requested,
                        teardown_stuck = report.teardown_stuck,
                        delete_failed = report.delete_failed,
                        rate_limited = report.rate_limited,
                        skipped = report.skipped_total(),
                        terminating = report.terminating,
                        missing_identity = report.missing_identity,
                        deferred = report.deferred,
                        dry_run = config.dry_run,
                        "orphaned_restore_pod_gc tick complete"
                    );
                }
                metrics::loop_latency(metrics::Loop::OrphanedRestorePodGc, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("orphaned_restore_pod_gc stopping");
                return;
            }
        }
    }
}
