//! The reconciler core: the per-tick scan, the rolling-window delete budget,
//! the fairness cursor, the per-candidate verify-then-delete action flow, and
//! the `run` entrypoint spawned by the cluster-singleton binary.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::types::{PodUid, SandboxId};
use pplx_space_metrics::cluster_reconciler as metrics;
use tokio_util::sync::CancellationToken;

use super::backend::{DeleteOutcome, GcBackend, KubernetesGcBackend};
use super::classify::{NodeEligibility, PodSnapshot, is_stuck_shaped};
use super::config::ReconcilerConfig;
use super::events::{CleanupOutcome, emit_cleanup_event};

/// Rolling window the `max_deletes_per_minute` budget is measured over.
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

/// Why a stuck candidate was not deleted this tick. Internal accounting only:
/// the [`TickReport::skipped`] tally that feeds the tick-complete log line's
/// skip count. The per-row metric vocabulary is
/// [`metrics::StuckPodGcOutcome`], which carries the `as_str` + `Display`
/// closed-set shape; this enum is never emitted as a metric tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SkipReason {
    MissingSandboxId,
    MissingNode,
    /// A user-restore pod — excluded from the GC (see `PodSnapshot::is_restore`).
    RestorePod,
    /// A warm-pool slot pod — excluded (see `PodSnapshot::is_warm_pool`).
    WarmPool,
    NodeNotReady,
    NodeCordoned,
    NodeUnknown,
    RateLimited,
    IdentityChanged,
    AlreadyGone,
    ProgressingOnRecheck,
    ReadError,
    /// An apiserver GET/DELETE for this candidate did not complete within the
    /// tick's wall-clock budget and was abandoned.
    TimedOut,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub scanned: u32,
    pub stuck_detected: u32,
    pub too_young: u32,
    pub deleted: u32,
    pub delete_failed: u32,
    pub skipped: BTreeMap<SkipReason, u32>,
    /// Candidates left unprocessed this tick because the per-tick wall-clock
    /// budget elapsed; they roll to the next tick.
    pub deferred: u32,
    /// True when the tick returned early because a reflector cache was still
    /// cold.
    pub cache_not_ready: bool,
}

impl TickReport {
    fn record_skip(&mut self, reason: SkipReason) {
        *self.skipped.entry(reason).or_default() += 1;
    }

    fn skipped_total(&self) -> u32 {
        self.skipped.values().copied().sum()
    }
}

/// Monotonic clock, sampled fresh at each rate-limit charge (and once per tick
/// for the age reference). Both the observation age and the rate-limit window
/// measure *elapsed in-process time*, so this must be monotonic — a wall-clock
/// (`jiff::Timestamp`) source would let a forward host-clock step make pods
/// look past-threshold (premature delete) and expire rate-limit charges (burst
/// delete), and a backward step stall GC. Production uses [`Instant::now`];
/// tests inject a controllable clock so age and rate-limit windows are
/// deterministic.
pub(crate) type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

pub struct Reconciler<B: GcBackend> {
    config: ReconcilerConfig,
    /// `pub(crate)` so the test module (a sibling of this one) can inspect the
    /// injected fake backend — the test/backend seam the split preserves.
    pub(crate) backend: B,
    /// Monotonic instants of candidates committed to, within the rolling
    /// window. Each committed candidate costs at most one apiserver GET (verify) plus one
    /// DELETE, so this bounds *all* apiserver work, not just successful
    /// deletes. Each entry is stamped at the moment of its commitment (not at
    /// tick start), so a tick that runs long under apiserver pressure can't
    /// back-date its late charges and let the next tick over-spend the real
    /// rolling minute. Trimmed to `RATE_LIMIT_WINDOW` each tick.
    delete_history: VecDeque<Instant>,
    /// Rotating start offset into the candidate list, advanced by the number of
    /// candidates committed to each tick. Without it, the reflector store's
    /// stable iteration order would let a budget-sized prefix of
    /// always-failing candidates consume every replenished window and starve
    /// the genuinely-stuck pods behind them.
    scan_offset: usize,
    /// When each still-present pod (keyed by UID) was *first observed* wedged in
    /// `ContainerCreating`, on the monotonic clock. The pod object has no
    /// reliable "in ContainerCreating since" timestamp (`status.startTime` is
    /// the pod's lifetime), so age is measured from continuous observation
    /// instead — a pod that just flipped into `ContainerCreating` starts its
    /// clock now. Reset when the pod stops being stuck-shaped or its UID
    /// changes; GC'd each tick to the pods still present. Resets on controller
    /// restart (the backlog then re-ages over one threshold), which is the safe
    /// direction.
    ///
    /// Limitation: keyed on UID and driven by the periodic scan of the
    /// watch-coalesced reflector store, so a pod that leaves and re-enters
    /// `ContainerCreating` entirely *between* scans (a sub-interval flap the
    /// store never surfaces as not-stuck) keeps its old first-seen time and can
    /// cross the threshold on age it didn't continuously hold. Recoveries the
    /// store or the fresh GET *do* observe already reset the clock; catching the
    /// unobserved flap needs a resourceVersion / watch-event signal rather than
    /// the store snapshot — a follow-up, and low-harm here (dry-run-first, and a
    /// pod flapping in/out of `ContainerCreating` is itself unhealthy).
    first_seen_stuck: HashMap<PodUid, Instant>,
    clock: Clock,
}

impl<B: GcBackend> Reconciler<B> {
    pub fn new(config: ReconcilerConfig, backend: B) -> Self {
        Self {
            config,
            backend,
            delete_history: VecDeque::new(),
            scan_offset: 0,
            first_seen_stuck: HashMap::new(),
            clock: Arc::new(Instant::now),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Seed the observation clock for a pod UID, so a single-tick test can
    /// treat a pod as already-observed-stuck for a given duration.
    #[cfg(test)]
    pub(crate) fn observe_since(&mut self, pod_uid: &str, since: Instant) {
        let uid = pod_uid.parse().expect("test pod uid must be non-empty");
        self.first_seen_stuck.insert(uid, since);
    }

    fn metric_mode(&self) -> metrics::Mode {
        metrics::Mode::Live
    }

    /// Emit one `cluster_reconciler.outcome` for a resolved candidate, paired
    /// with a log carrying the row identifiers. Destructive/error outcomes log
    /// at `warn`, benign resolutions at `info`, routine skips/defers at `debug`.
    fn emit_outcome(
        &self,
        outcome: metrics::StuckPodGcOutcome,
        pod: &PodSnapshot,
        sandbox_id: Option<SandboxId>,
        node: Option<&str>,
        age_secs: Option<i64>,
    ) {
        use metrics::StuckPodGcOutcome as O;
        let mode = self.metric_mode();
        let mut event = metrics::OutcomeEvent::stuck_container_creating_gc(outcome, mode)
            .pod_name(pod.pod_name.clone());
        if let Some(id) = sandbox_id {
            event = event.sandbox_id(id);
        }
        if let Some(node) = node {
            event = event.node(node);
        }
        if let Some(age) = age_secs {
            event = event.age_secs(u64::try_from(age).unwrap_or(0));
        }
        match outcome {
            O::DeleteFailed | O::ReadError | O::TimedOut => event.warn(),
            O::Deleted if mode == metrics::Mode::Live => event.warn(),
            O::Deleted | O::AlreadyGone => event.info(),
            _ => event.debug(),
        }
    }

    /// Like [`Self::emit_outcome`] for an error outcome, attaching the error
    /// string to the (warn-level) log. `node` is always known at the error call
    /// sites (post node-guard).
    fn emit_outcome_err(
        &self,
        outcome: metrics::StuckPodGcOutcome,
        pod: &PodSnapshot,
        sandbox_id: SandboxId,
        node: Option<&str>,
        age_secs: i64,
        err: &dyn std::fmt::Display,
    ) {
        let mut event =
            metrics::OutcomeEvent::stuck_container_creating_gc(outcome, self.metric_mode())
                .pod_name(pod.pod_name.clone())
                .sandbox_id(sandbox_id)
                .age_secs(u64::try_from(age_secs).unwrap_or(0))
                .error(err);
        if let Some(node) = node {
            event = event.node(node);
        }
        event.warn();
    }

    /// Drop rate-limit charges at or older than the rolling window relative to
    /// `now`. Uses `<=` (age `>= RATE_LIMIT_WINDOW` expires) so a charge stamped
    /// exactly one window ago frees its slot immediately rather than lingering a
    /// fraction longer. Called at tick start and again before each budget check
    /// with a fresh clock, so charges age out as real time advances instead of
    /// only at the next tick boundary.
    fn trim_rate_window(&mut self, now: Instant) {
        if let Some(cutoff) = now.checked_sub(RATE_LIMIT_WINDOW) {
            while self.delete_history.front().is_some_and(|ts| *ts <= cutoff) {
                self.delete_history.pop_front();
            }
        }
    }

    /// Number of candidates we may still commit to (GET + DELETE) this rolling
    /// minute.
    fn delete_budget_remaining(&self) -> usize {
        let cap = usize::try_from(self.config.max_deletes_per_minute).unwrap_or(usize::MAX);
        cap.saturating_sub(self.delete_history.len())
    }

    /// Maps the node reflector cache's current view of `node_name` to a skip
    /// reason + outcome, or `None` if the node is eligible (present, `Ready`,
    /// schedulable). Cheap (cache read, no apiserver), so it is called both as a
    /// pre-GET filter and as the authoritative recheck immediately before the
    /// DELETE — a node can cordon or go `NotReady` during the verify GET (up to
    /// the tick budget), and deleting then would race the node-drain /
    /// remediation path this guard defers to.
    fn node_gate(&self, node_name: &str) -> Option<(SkipReason, metrics::StuckPodGcOutcome)> {
        use metrics::StuckPodGcOutcome as Gc;
        match self.backend.node_eligibility(node_name) {
            NodeEligibility::Eligible => None,
            NodeEligibility::NotReady => Some((SkipReason::NodeNotReady, Gc::NodeNotReady)),
            NodeEligibility::Cordoned => Some((SkipReason::NodeCordoned, Gc::NodeCordoned)),
            NodeEligibility::Unknown => Some((SkipReason::NodeUnknown, Gc::NodeUnknown)),
        }
    }

    pub async fn tick(&mut self) -> TickReport {
        let now = (self.clock)();
        let mut report = TickReport::default();
        if !self.backend.caches_ready() {
            report.cache_not_ready = true;
            tracing::debug!(
                "stuck_container_creating_gc: reflector caches not ready; skipping tick"
            );
            return report;
        }

        // Trim the rate-limit window at tick start. It's re-trimmed against a
        // fresh clock before each budget check too (see `trim_rate_window`), so
        // charges stamped mid-tick age out as soon as they cross the window
        // rather than lingering until the *next* tick's start — otherwise, with
        // the default interval equal to the window, a burst of charges would
        // still look in-window at the next tick and halve effective throughput.
        self.trim_rate_window(now);

        let pods = self.backend.list_sandbox_pods().await;
        report.scanned = u32::try_from(pods.len()).unwrap_or(u32::MAX);

        // Build the actionable candidate list. Each stuck-shaped pod's age is
        // its *continuous* ContainerCreating age from the observation clock
        // (not the pod timestamp — see `first_seen_stuck`): a pod is a candidate
        // only once it has held the stuck shape for `threshold`. The list also
        // becomes the rotation-cursor's index space, so a budget-sized prefix of
        // repeatedly-failing candidates can't starve the ones behind it.
        let threshold = self.config.threshold;
        let mut candidates: Vec<(PodSnapshot, PodUid, i64)> = Vec::new();
        let mut still_stuck: HashSet<PodUid> = HashSet::new();
        for pod in pods {
            if !is_stuck_shaped(&pod) {
                if let Some(uid) = &pod.pod_uid {
                    self.first_seen_stuck.remove(uid);
                }
                continue;
            }
            let Some(uid) = pod.pod_uid.clone() else {
                // No UID to anchor the observation clock, so age is unknowable —
                // never act on a pod we can't age (real pods always carry one).
                report.too_young += 1;
                continue;
            };
            // Pause the observation clock while the pod's node is ineligible
            // (NotReady / cordoned / unknown). A pod stuck because its node is
            // down must not accrue deletable age during the outage and then be
            // deleted the instant the node recovers — kubelet has only just
            // resumed the create and deserves the full healthy-node threshold.
            // Dropping the entry re-ages it from scratch once the node is back.
            let node_ineligible = pod.node_name.as_deref().is_some_and(|node| {
                self.backend.node_eligibility(node) != NodeEligibility::Eligible
            });
            if node_ineligible {
                self.first_seen_stuck.remove(&uid);
                report.too_young += 1;
                continue;
            }
            still_stuck.insert(uid.clone());
            let first_seen = *self.first_seen_stuck.entry(uid.clone()).or_insert(now);
            let observed = now.duration_since(first_seen);
            if observed >= threshold {
                let age_secs = i64::try_from(observed.as_secs()).unwrap_or(i64::MAX);
                candidates.push((pod, uid, age_secs));
            } else {
                report.too_young += 1;
            }
        }
        // Drop observation-clock entries for pods no longer stuck-shaped or gone.
        self.first_seen_stuck
            .retain(|uid, _| still_stuck.contains(uid));

        report.stuck_detected = u32::try_from(candidates.len()).unwrap_or(u32::MAX);
        // `cluster_reconciler.scanned` counts the actionable candidates (stuck
        // pods past the threshold) surfaced this tick; each resolves to exactly
        // one `outcome` event below (including `deferred` for the budget tail).
        // Emitted only on ticks that surfaced at least one candidate, matching
        // the family convention that the series doesn't exist until a loop first
        // finds work — an idle tick emits nothing.
        if report.stuck_detected > 0 {
            metrics::scanned(
                metrics::Loop::StuckContainerCreatingGc,
                u64::from(report.stuck_detected),
            );
        }

        let candidate_count = candidates.len();
        if candidate_count > 0 {
            candidates.rotate_left(self.scan_offset % candidate_count);
        }
        // Count budget commitments explicitly rather than by `delete_history`
        // length delta: `trim_rate_window` runs mid-tick (before each budget
        // check), so the deque both shrinks (expiry) and grows (new charges)
        // during the action phase, and a net-length delta would mis-count
        // commitments and mis-advance the fairness cursor.
        let mut committed = 0usize;
        // Monotonic wall-clock deadline for the whole action phase. Bounds tick
        // duration when the apiserver is slow — each verify GET / DELETE is run
        // under this deadline via `timeout_at`, and it is re-checked between
        // candidates — so a single hung request can't block the loop (and
        // shutdown) for the kube client's multi-minute timeout. Remaining
        // candidates roll to the next tick; the cursor then advances past the
        // ones handled this tick (see below), so the deferred tail isn't
        // starved.
        let deadline = tokio::time::Instant::now() + self.config.tick_budget;

        let mut processed = 0usize;
        // 1-based index just past the last candidate that consumed budget. The
        // fairness cursor advances to here (not by the commitment *count*): a
        // skip-only prefix (restore / warm-pool / ineligible node) commits no
        // budget, so advancing by count would leave the cursor short of it and
        // re-process the same failing committed block ahead of the rate-limited
        // tail every tick, starving it.
        let mut last_committed_pos = 0usize;
        for (candidate, uid, observed_age) in &candidates {
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            let committed_before = committed;
            let flow = self
                .handle_stuck(
                    candidate,
                    uid,
                    *observed_age,
                    deadline,
                    &mut committed,
                    &mut report,
                )
                .await;
            processed += 1;
            if committed > committed_before {
                last_committed_pos = processed;
            }
            if flow == CandidateFlow::DeadlineExceeded {
                // The in-flight call for this candidate hit the deadline (it was
                // counted as `timed_out`); defer the rest.
                break;
            }
        }
        // Every candidate counted in `scanned` gets exactly one outcome: the
        // budget tail that never ran emits `deferred` so the metric contract
        // holds even under apiserver pressure.
        report.deferred =
            u32::try_from(candidate_count.saturating_sub(processed)).unwrap_or(u32::MAX);
        for (candidate, _uid, observed_age) in candidates.iter().skip(processed) {
            self.emit_outcome(
                metrics::StuckPodGcOutcome::Deferred,
                candidate,
                candidate.sandbox_id,
                candidate.node_name.as_deref(),
                Some(*observed_age),
            );
        }
        if report.deferred > 0 {
            tracing::warn!(
                deferred = report.deferred,
                tick_budget_secs = self.config.tick_budget.as_secs(),
                "stuck_container_creating_gc: tick budget elapsed; \
                 deferring remaining candidates to next tick"
            );
        }

        // Advance the fairness cursor. Two cases:
        // - A deadline deferred a tail (`deferred > 0`): advance past *every*
        //   candidate handled this tick, so the deferred tail runs first next
        //   tick.
        // - No deferral: advance to just past the last candidate that consumed
        //   budget, so the rate-limited tail behind it (and any repeatedly
        //   committing-but-failing block ahead of it) is retried first next
        //   tick. A skip-only prefix commits no budget, so `last_committed_pos`
        //   stays 0 and the cursor holds — nothing actionable is behind pure
        //   skips to starve.
        let advance = if report.deferred > 0 {
            processed
        } else {
            last_committed_pos
        };
        self.scan_offset = self.scan_offset.wrapping_add(advance);

        report
    }

    async fn handle_stuck(
        &mut self,
        pod: &PodSnapshot,
        pod_uid: &PodUid,
        observed_age: i64,
        deadline: tokio::time::Instant,
        committed: &mut usize,
        report: &mut TickReport,
    ) -> CandidateFlow {
        use metrics::StuckPodGcOutcome as Gc;

        // Restore pods carry durable snapshot state and land in `restoring`,
        // which the orphaned-sandbox convergence prerequisite doesn't handle;
        // deleting them here would strand the row. A restore-aware path owns
        // them, so exclude them (detected purely from the pod annotation).
        if pod.is_restore {
            report.record_skip(SkipReason::RestorePod);
            self.emit_outcome(
                Gc::RestorePod,
                pod,
                pod.sandbox_id,
                pod.node_name.as_deref(),
                Some(observed_age),
            );
            return CandidateFlow::Processed;
        }
        // Warm-pool slot pods are owned by the warm-pool reconciler and have no
        // logical `sandboxes` row for either convergence reconciler to retire;
        // deleting one here would leave a `warming` slot pinned against pool
        // capacity. Detected purely from the pool-role label.
        if pod.is_warm_pool {
            report.record_skip(SkipReason::WarmPool);
            self.emit_outcome(
                Gc::WarmPool,
                pod,
                pod.sandbox_id,
                pod.node_name.as_deref(),
                Some(observed_age),
            );
            return CandidateFlow::Processed;
        }
        let Some(sandbox_id) = pod.sandbox_id else {
            report.record_skip(SkipReason::MissingSandboxId);
            self.emit_outcome(
                Gc::MissingSandboxId,
                pod,
                None,
                pod.node_name.as_deref(),
                Some(observed_age),
            );
            return CandidateFlow::Processed;
        };
        let Some(node_name) = pod.node_name.clone() else {
            report.record_skip(SkipReason::MissingNode);
            self.emit_outcome(
                Gc::MissingNode,
                pod,
                Some(sandbox_id),
                None,
                Some(observed_age),
            );
            return CandidateFlow::Processed;
        };
        // Cheap pre-GET filter: skip an obviously-ineligible node before
        // spending budget + an apiserver GET on it. The authoritative recheck
        // happens again after the GET, just before the DELETE. The node went
        // ineligible after the scan-phase pause saw it healthy (a mid-tick
        // change), so reset the observation clock here too — otherwise a node
        // that recovers before the next scan would inherit this stale age and
        // skip the full healthy-node threshold.
        if let Some((skip, outcome)) = self.node_gate(&node_name) {
            self.first_seen_stuck.remove(pod_uid);
            report.record_skip(skip);
            self.emit_outcome(
                outcome,
                pod,
                Some(sandbox_id),
                Some(&node_name),
                Some(observed_age),
            );
            return CandidateFlow::Processed;
        }

        // Re-trim against the current clock so charges that crossed the window
        // since tick start free their slots before this budget check, rather
        // than waiting for the next tick.
        self.trim_rate_window((self.clock)());
        if self.delete_budget_remaining() == 0 {
            report.record_skip(SkipReason::RateLimited);
            self.emit_outcome(
                Gc::RateLimited,
                pod,
                Some(sandbox_id),
                Some(&node_name),
                Some(observed_age),
            );
            return CandidateFlow::Processed;
        }
        // Consume a budget slot the moment we commit to touching the apiserver
        // for this candidate. Each committed candidate costs at most one GET
        // (verify) plus one DELETE, so charging here — not only on a successful
        // delete — bounds *all* apiserver work by `max_deletes_per_minute`,
        // including the degenerate case where candidates repeatedly fail the
        // fresh recheck without ever landing a delete. A candidate skipped this
        // tick is re-evaluated next tick from a fresh cache scan, so nothing is
        // starved. Dry-run consumes a slot too so its projected volume matches
        // what live mode would actually do. The timestamp is sampled now, at
        // the point of commitment, so a long-running tick doesn't back-date its
        // late charges.
        self.delete_history.push_back((self.clock)());
        *committed += 1;

        let age_secs = observed_age;
        let node = Some(node_name.as_str());

        // Verify before destroy: re-read authoritatively and confirm the pod is
        // still the same object and still stuck. This runs in dry-run too so its
        // reported volume is a faithful preview of what live mode would delete —
        // dry-run and live share the whole authoritative decision path and
        // diverge only at the DELETE below. Bounded by the tick deadline so a
        // slow/hung apiserver can't stall the loop on this one call.
        let read =
            tokio::time::timeout_at(deadline, self.backend.read_pod_fresh(&pod.pod_name)).await;
        let fresh = match read {
            Ok(Ok(Some(fresh))) => fresh,
            Ok(Ok(None)) => {
                report.record_skip(SkipReason::AlreadyGone);
                self.emit_outcome(Gc::AlreadyGone, pod, Some(sandbox_id), node, Some(age_secs));
                return CandidateFlow::Processed;
            }
            Ok(Err(err)) => {
                report.record_skip(SkipReason::ReadError);
                self.emit_outcome_err(Gc::ReadError, pod, sandbox_id, node, age_secs, &err);
                return CandidateFlow::Processed;
            }
            Err(_elapsed) => {
                report.record_skip(SkipReason::TimedOut);
                self.emit_outcome(Gc::TimedOut, pod, Some(sandbox_id), node, Some(age_secs));
                return CandidateFlow::DeadlineExceeded;
            }
        };
        // Re-apply every destructive-eligibility guard to the authoritative
        // object, not just to the cached scan snapshot, so scan and verify
        // can't drift: a pod that reveals itself as a restore on the fresh read
        // (its annotation wasn't in the stale cache) must not be deleted.
        if fresh.is_restore {
            report.record_skip(SkipReason::RestorePod);
            self.emit_outcome(Gc::RestorePod, pod, Some(sandbox_id), node, Some(age_secs));
            return CandidateFlow::Processed;
        }
        if fresh.is_warm_pool {
            report.record_skip(SkipReason::WarmPool);
            self.emit_outcome(Gc::WarmPool, pod, Some(sandbox_id), node, Some(age_secs));
            return CandidateFlow::Processed;
        }
        if fresh.pod_uid.as_ref() != Some(pod_uid) {
            // The observed pod is gone (a same-name pod from a newer generation
            // took its place); its observation-clock entry is now dead. Drop it
            // so the stale age can't be inherited if this UID somehow recurs.
            self.first_seen_stuck.remove(pod_uid);
            report.record_skip(SkipReason::IdentityChanged);
            self.emit_outcome(
                Gc::StaleIdentity,
                pod,
                Some(sandbox_id),
                node,
                Some(age_secs),
            );
            return CandidateFlow::Processed;
        }
        if !is_stuck_shaped(&fresh) {
            // The authoritative read shows the same-UID pod recovered — the
            // cached scan was stale. Reset its observation clock: if it later
            // re-wedges before the reflector scan observes this recovery, the
            // next tick must re-age from the re-wedge, not inherit the old age
            // and delete immediately, bypassing the threshold.
            self.first_seen_stuck.remove(pod_uid);
            report.record_skip(SkipReason::ProgressingOnRecheck);
            self.emit_outcome(
                Gc::ProgressingOnRecheck,
                pod,
                Some(sandbox_id),
                node,
                Some(age_secs),
            );
            return CandidateFlow::Processed;
        }
        // Node recheck from the watch-backed node reflector cache: the node may
        // have cordoned or gone `NotReady` during the verify GET (up to the tick
        // budget). This is best-effort, not a fresh authoritative node GET — a
        // cordon the apiserver has accepted but the cache hasn't yet observed
        // can still read `Eligible` here; the pod-identity fence (UID +
        // resourceVersion on the DELETE) is what actually prevents removing a
        // healthy pod, and node-drain will re-delete a genuinely-stuck pod on a
        // cordoning node anyway. Deferring on a *seen* cordon still avoids
        // racing node-drain in the common case. Reset the observation clock (as
        // the scan-phase pause does) so a node that recovers before the next
        // scan re-ages from scratch rather than inheriting this age.
        if let Some((skip, outcome)) = self.node_gate(&node_name) {
            self.first_seen_stuck.remove(pod_uid);
            report.record_skip(skip);
            self.emit_outcome(outcome, pod, Some(sandbox_id), node, Some(age_secs));
            return CandidateFlow::Processed;
        }

        emit_cleanup_event(
            sandbox_id,
            pod,
            &node_name,
            age_secs,
            CleanupOutcome::Attempting,
        );

        let delete = tokio::time::timeout_at(
            deadline,
            self.backend
                .delete_pod(&pod.pod_name, pod_uid, fresh.resource_version.as_deref()),
        )
        .await;
        match delete {
            Ok(Ok(DeleteOutcome::Deleted)) => {
                emit_cleanup_event(
                    sandbox_id,
                    pod,
                    &node_name,
                    age_secs,
                    CleanupOutcome::Deleted,
                );
                self.emit_outcome(Gc::Deleted, pod, Some(sandbox_id), node, Some(age_secs));
                report.deleted += 1;
            }
            Ok(Ok(DeleteOutcome::NotFound)) => {
                // The pod vanished between the verify GET and the DELETE — the
                // cleanup goal is met. Close the `cleanup_requested` we emitted
                // above so the event timeline isn't left dangling.
                emit_cleanup_event(
                    sandbox_id,
                    pod,
                    &node_name,
                    age_secs,
                    CleanupOutcome::AlreadyGone,
                );
                self.emit_outcome(Gc::AlreadyGone, pod, Some(sandbox_id), node, Some(age_secs));
                report.record_skip(SkipReason::AlreadyGone);
            }
            Ok(Ok(DeleteOutcome::StaleIdentity)) => {
                // A precondition failed — the object changed under us, so we
                // correctly did not delete it. Close the dangling request.
                emit_cleanup_event(
                    sandbox_id,
                    pod,
                    &node_name,
                    age_secs,
                    CleanupOutcome::SupersededIdentity,
                );
                self.emit_outcome(
                    Gc::StaleIdentity,
                    pod,
                    Some(sandbox_id),
                    node,
                    Some(age_secs),
                );
                report.record_skip(SkipReason::IdentityChanged);
            }
            Ok(Err(err)) => {
                emit_cleanup_event(
                    sandbox_id,
                    pod,
                    &node_name,
                    age_secs,
                    CleanupOutcome::Failed(err.to_string()),
                );
                self.emit_outcome_err(Gc::DeleteFailed, pod, sandbox_id, node, age_secs, &err);
                report.delete_failed += 1;
            }
            Err(_elapsed) => {
                // The DELETE outcome is unknown (it may still land on the
                // apiserver). Close the `cleanup_requested` we emitted above so
                // the timeline doesn't dangle; the next tick re-attempts.
                emit_cleanup_event(
                    sandbox_id,
                    pod,
                    &node_name,
                    age_secs,
                    CleanupOutcome::TimedOut,
                );
                self.emit_outcome(Gc::TimedOut, pod, Some(sandbox_id), node, Some(age_secs));
                report.record_skip(SkipReason::TimedOut);
                return CandidateFlow::DeadlineExceeded;
            }
        }
        CandidateFlow::Processed
    }
}

/// Whether the per-candidate action finished within the tick's wall-clock
/// budget, or an in-flight apiserver call hit the deadline (so the loop should
/// stop and defer the remaining candidates).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFlow {
    Processed,
    DeadlineExceeded,
}

/// Whether the orphaned-sandbox reconciler — the convergence path that retires
/// a sandbox row once this GC deletes its pod — is configured to actively
/// mutate. Pure so the policy is unit-testable; the env wrapper feeds it.
///
/// A deleted stuck pod's sandbox row is retired by one of two reconcilers,
/// depending on the lifecycle the row is in when its pod vanishes:
/// - `creating` / `starting` / `started` → the orphaned-sandbox reconciler.
/// - `suspending` / `checkpointing` → the transient-lifecycle sweeper. A stuck
///   pod normally belongs to a `started` row (the started-recovery
///   `ensure_pod_exists` path), but a user suspend/checkpoint can race that row
///   into a transient state while its pod is still wedged, so this path must be
///   covered too.
///
/// `restoring` pods are excluded upstream (restore annotation). Live deletion
/// therefore requires **both** convergence paths live; if either is down a
/// drifted row would be stranded with no reconciler to retire it.
pub(crate) fn convergence_allows_live(orphan_live: bool, transient_live: bool) -> bool {
    orphan_live && transient_live
}

/// Reads the orphaned-sandbox reconciler's own config so this gate can't drift
/// from its env-var names or defaults — that reconciler is the single source of
/// truth and lives in the same binary.
fn orphaned_sandbox_reconciler_is_live() -> bool {
    let cfg = crate::orphaned_sandboxes::ReconcilerConfig::from_env();
    cfg.enabled && !cfg.dry_run
}

/// Upper bound on a transient lifecycle's *effective* sweep age for it to count
/// as a live convergence path. The sweeper shares one `default_age` across all
/// lifecycles, so a large default can effectively disable a lifecycle left on
/// `UseDefault` and leave a GC-deleted pod's row unretired. Anything
/// past this bound is treated as *not* converging. Generous (no legitimate
/// transient-sweep age approaches a week); it's a "clearly disabled" sentinel,
/// not a convergence SLA.
const MAX_TRANSIENT_CONVERGENCE_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Whether a transient lifecycle's effective sweep age makes it a live
/// convergence path: not [`LifecycleAge::Disabled`](crate::transient_lifecycle_sweeper::LifecycleAge::Disabled), and — resolving
/// `UseDefault` to the sweeper's `default_age` — bounded below
/// [`MAX_TRANSIENT_CONVERGENCE_AGE`]. Pure so the boundary is unit-testable.
pub(crate) fn transient_lifecycle_covered(
    age: crate::transient_lifecycle_sweeper::LifecycleAge,
    default_age: Duration,
) -> bool {
    use crate::transient_lifecycle_sweeper::LifecycleAge;
    let effective = match age {
        LifecycleAge::Disabled => return false,
        LifecycleAge::UseDefault => default_age,
        LifecycleAge::Override(d) => d.get(),
    };
    effective <= MAX_TRANSIENT_CONVERGENCE_AGE
}

/// Whether the transient-lifecycle sweeper will actually converge every row a
/// GC-deleted pod could leave behind that isn't the orphaned-sandbox
/// reconciler's. Unlike that reconciler it has no global dry-run; it acts when
/// its master gate is enabled *and* the relevant knob is on:
/// - `suspending` / `checkpointing` — a user action racing a stuck `started`
///   pod into a transient state; needs the per-lifecycle *effective* age to be
///   set (not [`LifecycleAge::Disabled`](crate::transient_lifecycle_sweeper::LifecycleAge::Disabled)) and bounded (see
///   [`transient_lifecycle_covered`]), not merely non-`Disabled` — `UseDefault`
///   inherits `default_age`, which can exceed the convergence bound.
/// - `failed` — the sweeper gave up on a wedged transition but its best-effort
///   execution retirement failed, so the pod lingers in `ContainerCreating`
///   with an active execution + placement still booked. The orphaned-sandbox
///   reconciler excludes `failed`, and only the sweeper's `failed_cleanup` pass
///   retires those, so live GC needs it enabled too.
///
/// Reads the sweeper's own config so this gate can't drift from its env-var
/// names or defaults.
fn transient_sweeper_covers_gc_lifecycles() -> bool {
    let cfg = crate::transient_lifecycle_sweeper::ReconcilerConfig::from_env();
    let default_age = cfg.default_age.get();
    cfg.enabled
        && transient_lifecycle_covered(cfg.suspending_age, default_age)
        && transient_lifecycle_covered(cfg.checkpointing_age, default_age)
        && cfg.failed_cleanup_enabled
}

/// Run the stuck-ContainerCreating pod GC loop until the shutdown token fires.
/// Spawned by the cluster-singleton `space-cluster-controller` binary.
pub async fn run(
    kube: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "stuck_container_creating_gc disabled \
             (SPACE_STUCK_CONTAINER_CREATING_GC_ENABLED unset); waiting for shutdown"
        );
        shutdown_token.cancelled().await;
        return;
    }

    // Fail closed: live deletion is only safe when both convergence paths are
    // live to retire the sandbox row once its pod is gone (this loop never
    // touches Postgres). The orphaned-sandbox reconciler covers
    // `creating`/`starting`/`started`; the transient-lifecycle sweeper covers a
    // row a user action raced into `suspending`/`checkpointing`, plus a `failed`
    // row whose execution retirement didn't finish. If either path is down,
    // force dry-run rather than strand a drifted row on a config mistake.
    //
    // This guards against *misconfiguration* (a convergence reconciler disabled
    // or left in dry-run), read once at startup from the same env the sibling
    // reconcilers read — their config can't change without a restart, so
    // re-reading each tick is moot. Runtime liveness is covered separately: all
    // loops are co-located in this single-replica process, and the reconciler
    // chassis (`run_reconcilers`) fails fast if *any* task exits before
    // shutdown, crashing the pod so it restarts and this gate re-evaluates —
    // so a convergence reconciler dying after startup can't leave this GC
    // deleting into a dead convergence path for long.
    if !convergence_allows_live(
        orphaned_sandbox_reconciler_is_live(),
        transient_sweeper_covers_gc_lifecycles(),
    ) {
        tracing::warn!(
            "stuck_container_creating_gc is enabled but a convergence path is \
             not live: the orphaned-sandbox reconciler needs \
             SPACE_ORPHANED_SANDBOX_RECONCILER_ENABLED=true and SPACE_API_RECONCILER_DRY_RUN=false, \
             and the transient-lifecycle sweeper needs SPACE_TRANSIENT_LIFECYCLE_SWEEPER_ENABLED=true \
             with the suspending/checkpointing effective ages bounded (not disabled, and not the \
             practically-disabled large default — set explicit bounded *_AGE_SECS overrides if the \
             default is large) and failed-sandbox cleanup must remain enabled; refusing to run"
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        interval_secs = config.interval.as_secs(),
        threshold_secs = config.threshold.as_secs(),
        max_deletes_per_minute = config.max_deletes_per_minute,
        "starting stuck_container_creating_gc"
    );

    let backend = KubernetesGcBackend::new(kube);
    let mut reconciler = Reconciler::new(config.clone(), backend);

    let mut interval = tokio::time::interval(config.interval);
    // Backoff, not burst catch-up, when a tick runs long under apiserver
    // pressure — the exact condition this loop is designed to relieve.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so startup doesn't race the reflector sync.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let report = reconciler.tick().await;
                if report.cache_not_ready {
                    tracing::info!("stuck_container_creating_gc: caches not ready; tick skipped");
                } else {
                    tracing::info!(
                        scanned = report.scanned,
                        stuck_detected = report.stuck_detected,
                        too_young = report.too_young,
                        deleted_or_would_delete = report.deleted,
                        delete_failed = report.delete_failed,
                        skipped = report.skipped_total(),
                        deferred = report.deferred,
                                                "stuck_container_creating_gc tick complete"
                    );
                }
                metrics::loop_latency(metrics::Loop::StuckContainerCreatingGc, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("stuck_container_creating_gc stopping");
                return;
            }
        }
    }
}
