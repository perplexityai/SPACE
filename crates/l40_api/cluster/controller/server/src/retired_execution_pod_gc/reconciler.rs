//! The reconciler core: the per-tick scan, the batched execution-row lookup,
//! the continuous-observation clock, the rolling-window delete budget, the
//! per-candidate verify-then-delete flow, and the `run` entrypoint.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_common_core::types::PodUid;
use pplx_space_metrics::cluster_reconciler::{
    self as metrics, Mode, OutcomeEvent, RetiredExecutionPodOutcome as O,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use super::backend::{DeleteOutcome, ExecutionStore, KubernetesPodBackend, PodBackend};
use super::classify::{Disposition, PodSnapshot, ReapReason, SkipReason, classify};
use super::config::ReconcilerConfig;
use super::store::ExecutionDbStore;

/// Rolling window the `max_deletes_per_minute` budget is measured over.
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);

/// What one tick saw and did. Counts, for the tick log line and tests.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub cache_not_ready: bool,
    /// The execution-row lookup failed; nothing was classified or acted on.
    pub lookup_failed: bool,
    pub scanned: u32,
    /// Pods that classified as reapable this tick, by reason.
    pub reapable: BTreeMap<&'static str, u32>,
    /// Reapable but not yet observed for the full threshold.
    pub too_young: u32,
    pub skipped: BTreeMap<&'static str, u32>,
    /// Live: DELETE accepted. Dry-run: would have been.
    pub reaped: u32,
    pub already_gone: u32,
    pub not_reapable_on_recheck: u32,
    pub stale_identity: u32,
    pub errors: u32,
    pub rate_limited: u32,
    pub deferred: u32,
}

fn skip_label(reason: SkipReason) -> &'static str {
    match reason {
        SkipReason::Terminating => "terminating",
        SkipReason::MissingIdentity => "missing_identity",
        SkipReason::NoExecutionRow => "no_execution_row",
        SkipReason::IdentityMismatch => "identity_mismatch",
        SkipReason::ActiveExecution => "active_execution",
        SkipReason::RetiredNotProvablyIdle => "retired_not_provably_idle",
    }
}

struct Candidate {
    pod: PodSnapshot,
    uid: PodUid,
    reason: ReapReason,
    observed_for: Duration,
}

pub struct Reconciler<P, S> {
    config: ReconcilerConfig,
    pods: P,
    store: S,
    /// First time each pod UID was seen classifying as reapable, continuously.
    first_seen: HashMap<PodUid, Instant>,
    /// Dry-run: UIDs already reported as would-reap, so each is logged once.
    previewed: HashSet<PodUid>,
    delete_history: VecDeque<Instant>,
    clock: Box<dyn Fn() -> Instant + Send + Sync>,
}

impl<P: PodBackend, S: ExecutionStore> Reconciler<P, S> {
    pub fn new(config: ReconcilerConfig, pods: P, store: S) -> Self {
        Self {
            config,
            pods,
            store,
            first_seen: HashMap::new(),
            previewed: HashSet::new(),
            delete_history: VecDeque::new(),
            clock: Box::new(Instant::now),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_clock(
        mut self,
        clock: impl Fn() -> Instant + Send + Sync + 'static,
    ) -> Self {
        self.clock = Box::new(clock);
        self
    }

    fn mode(&self) -> Mode {
        if self.config.dry_run {
            Mode::DryRun
        } else {
            Mode::Live
        }
    }

    fn outcome(&self, outcome: O, candidate: &Candidate) -> OutcomeEvent {
        let mut event =
            OutcomeEvent::retired_execution_pod_gc(outcome, self.mode(), candidate.reason.as_str())
                .pod_name(candidate.pod.pod_name.clone())
                .age_secs(candidate.observed_for.as_secs());
        if let Some(sandbox_id) = candidate.pod.sandbox_id {
            event = event.sandbox_id(sandbox_id);
        }
        if let Some(generation) = candidate.pod.generation {
            event = event.generation(generation);
        }
        if let Some(node) = candidate.pod.node_name.as_deref() {
            event = event.node(node);
        }
        event
    }

    fn trim_rate_window(&mut self, now: Instant) {
        while self
            .delete_history
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= RATE_LIMIT_WINDOW)
        {
            self.delete_history.pop_front();
        }
    }

    fn budget_remaining(&self) -> usize {
        (self.config.max_deletes_per_minute as usize).saturating_sub(self.delete_history.len())
    }

    pub async fn tick(&mut self) -> TickReport {
        let mut report = TickReport::default();
        if !self.pods.caches_ready() {
            // A cold cache proves nothing about continuity; restart the clocks.
            self.first_seen.clear();
            report.cache_not_ready = true;
            return report;
        }
        let deadline = tokio::time::Instant::now() + self.config.tick_budget;

        let pods: Vec<PodSnapshot> = self
            .pods
            .list_pods()
            .await
            .into_iter()
            .filter(PodSnapshot::worth_classifying)
            .collect();
        report.scanned = u32::try_from(pods.len()).unwrap_or(u32::MAX);

        let names: Vec<String> = pods.iter().map(|pod| pod.pod_name.clone()).collect();
        let mut rows = HashMap::new();
        for chunk in names.chunks(self.config.batch_size.max(1) as usize) {
            match self.store.rows_for_pods(chunk).await {
                Ok(found) => rows.extend(found),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        "retired_execution_pod_gc: execution-row lookup failed; tick skipped"
                    );
                    // A failed scan breaks continuity like a cold cache.
                    self.first_seen.clear();
                    report.lookup_failed = true;
                    return report;
                }
            }
        }

        let now = (self.clock)();
        let mut still_reapable = HashSet::new();
        let mut candidates = Vec::new();
        for pod in pods {
            match classify(&pod, rows.get(&pod.pod_name)) {
                Disposition::Skip(reason) => {
                    *report.skipped.entry(skip_label(reason)).or_default() += 1;
                }
                Disposition::Reap(reason) => {
                    *report.reapable.entry(reason.as_str()).or_default() += 1;
                    // `classify` reaps only pods with a UID.
                    let Some(uid) = pod.pod_uid.clone() else {
                        continue;
                    };
                    still_reapable.insert(uid.clone());
                    let first = *self.first_seen.entry(uid.clone()).or_insert(now);
                    let observed_for = now.saturating_duration_since(first);
                    if observed_for < self.config.threshold.get() {
                        report.too_young += 1;
                        continue;
                    }
                    candidates.push(Candidate {
                        pod,
                        uid,
                        reason,
                        observed_for,
                    });
                }
            }
        }
        // A pod that stopped classifying as reapable (or disappeared) restarts
        // its clock if it ever classifies again.
        self.first_seen
            .retain(|uid, _| still_reapable.contains(uid));
        self.previewed.retain(|uid| still_reapable.contains(uid));

        // Oldest observation first, so a backlog drains in arrival order.
        candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.observed_for));
        let mut candidates = candidates.into_iter();
        while let Some(candidate) = candidates.next() {
            if tokio::time::Instant::now() >= deadline {
                report.deferred += 1 + u32::try_from(candidates.len()).unwrap_or(u32::MAX);
                self.outcome(O::Deferred, &candidate).debug();
                break;
            }
            if self.config.dry_run && self.previewed.contains(&candidate.uid) {
                continue;
            }
            self.trim_rate_window((self.clock)());
            if self.budget_remaining() == 0 {
                report.rate_limited += 1;
                self.outcome(O::RateLimited, &candidate).debug();
                continue;
            }
            // Charge the slot when committing to touch the apiserver; dry-run
            // charges too so its projected volume matches live mode.
            self.delete_history.push_back((self.clock)());
            self.act(candidate, deadline, &mut report).await;
        }
        report
    }

    /// Verify-before-destroy, then the preconditioned DELETE.
    async fn act(
        &mut self,
        candidate: Candidate,
        deadline: tokio::time::Instant,
        report: &mut TickReport,
    ) {
        let fresh = match tokio::time::timeout_at(
            deadline,
            self.pods.read_pod_fresh(&candidate.pod.pod_name),
        )
        .await
        {
            Ok(Ok(Some(fresh))) => fresh,
            Ok(Ok(None)) => {
                self.first_seen.remove(&candidate.uid);
                report.already_gone += 1;
                self.outcome(O::AlreadyGone, &candidate).debug();
                return;
            }
            Ok(Err(err)) => {
                report.errors += 1;
                self.outcome(O::ReadError, &candidate).error(err).warn();
                return;
            }
            Err(_) => {
                report.errors += 1;
                self.outcome(O::TimedOut, &candidate).warn();
                return;
            }
        };
        if fresh.pod_uid.as_ref() != Some(&candidate.uid) {
            // Same name, different object: not the pod that aged in.
            self.first_seen.remove(&candidate.uid);
            report.stale_identity += 1;
            self.outcome(O::StaleIdentity, &candidate).info();
            return;
        }
        let row = match tokio::time::timeout_at(
            deadline,
            self.store
                .rows_for_pods(std::slice::from_ref(&fresh.pod_name)),
        )
        .await
        {
            Ok(Ok(mut rows)) => rows.remove(&fresh.pod_name),
            Ok(Err(err)) => {
                report.errors += 1;
                self.outcome(O::ReadError, &candidate).error(err).warn();
                return;
            }
            Err(_) => {
                report.errors += 1;
                self.outcome(O::TimedOut, &candidate).warn();
                return;
            }
        };
        if !matches!(classify(&fresh, row.as_ref()), Disposition::Reap(_)) {
            self.first_seen.remove(&candidate.uid);
            report.not_reapable_on_recheck += 1;
            self.outcome(O::NotReapableOnRecheck, &candidate).info();
            return;
        }

        if self.config.dry_run {
            self.previewed.insert(candidate.uid.clone());
            report.reaped += 1;
            self.outcome(O::Reaped, &candidate).info();
            return;
        }

        let delete = tokio::time::timeout_at(
            deadline,
            self.pods.delete_pod(
                &fresh.pod_name,
                &candidate.uid,
                fresh.resource_version.as_deref(),
            ),
        )
        .await;
        match delete {
            Ok(Ok(DeleteOutcome::Deleted)) => {
                self.first_seen.remove(&candidate.uid);
                report.reaped += 1;
                self.outcome(O::Reaped, &candidate).info();
            }
            Ok(Ok(DeleteOutcome::NotFound)) => {
                self.first_seen.remove(&candidate.uid);
                report.already_gone += 1;
                self.outcome(O::AlreadyGone, &candidate).debug();
            }
            Ok(Ok(DeleteOutcome::StaleIdentity)) => {
                report.stale_identity += 1;
                self.outcome(O::StaleIdentity, &candidate).info();
            }
            Ok(Err(err)) => {
                report.errors += 1;
                self.outcome(O::DeleteFailed, &candidate).error(err).warn();
            }
            Err(_) => {
                report.errors += 1;
                self.outcome(O::TimedOut, &candidate).warn();
            }
        }
    }
}

/// Entrypoint spawned by the cluster-singleton binary for the Kubernetes
/// substrate. Ships default-disabled; when enabled, starts in dry-run.
pub async fn run(
    pool: PgPool,
    kube: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "retired_execution_pod_gc disabled \
             (SPACE_RETIRED_EXECUTION_POD_GC_ENABLED unset); waiting for shutdown"
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
        dry_run = config.dry_run,
        "starting retired_execution_pod_gc"
    );

    let pods = KubernetesPodBackend::new(kube);
    let store = ExecutionDbStore::new(pool, pods.namespace().to_owned());
    let mut reconciler = Reconciler::new(config.clone(), pods, store);

    let mut interval = tokio::time::interval(config.interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so startup doesn't race the reflector sync.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let started = Instant::now();
                let report = tokio::select! {
                    biased;
                    () = shutdown_token.cancelled() => {
                        tracing::info!("retired_execution_pod_gc stopping");
                        return;
                    }
                    report = reconciler.tick() => report,
                };
                if report.cache_not_ready {
                    tracing::info!("retired_execution_pod_gc: cache not ready; tick skipped");
                } else {
                    tracing::info!(
                        scanned = report.scanned,
                        reapable = ?report.reapable,
                        too_young = report.too_young,
                        skipped = ?report.skipped,
                        reaped_or_would_reap = report.reaped,
                        already_gone = report.already_gone,
                        not_reapable_on_recheck = report.not_reapable_on_recheck,
                        stale_identity = report.stale_identity,
                        errors = report.errors,
                        rate_limited = report.rate_limited,
                        deferred = report.deferred,
                        lookup_failed = report.lookup_failed,
                        dry_run = config.dry_run,
                        "retired_execution_pod_gc tick complete"
                    );
                }
                metrics::loop_latency(metrics::Loop::RetiredExecutionPodGc, started);
            }
            _ = shutdown_token.cancelled() => {
                tracing::info!("retired_execution_pod_gc stopping");
                return;
            }
        }
    }
}
