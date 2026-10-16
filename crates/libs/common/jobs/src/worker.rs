use pplx_space_common_core::types::{JobId, SubjectId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::Instrument as _;

use crate::db;
use crate::scheduler::{PriorityPermit, PriorityScheduler, SchedulerBand};
use crate::{Job, JobError, JobExecutor, JobFailureKind, JobPriority};

mod completion;

use completion::CompletionWaiters;

/// Default cadence for [`BackgroundWorker::run_queue_metrics`].
const DEFAULT_QUEUE_METRICS_INTERVAL: Duration = Duration::from_secs(10);
const DB_MUTEX_POISONED: &str = "common jobs database mutex poisoned";
const AWAITING_SUBMITTERS_POISONED: &str = "common jobs awaiting-submitter mutex poisoned";

type AwaitingSubmitters = Arc<Mutex<HashMap<JobId, tracing::Span>>>;

tokio::task_local! {
    /// The registration of a [`BackgroundWorker::submit_and_wait`] call, for
    /// the jobs its submission creates on that worker.
    static AWAITING_SUBMITTER: Arc<AwaitingRegistration>;
}

/// One [`BackgroundWorker::submit_and_wait`] call's waiting span, registered
/// for each job its submission creates. Dropping the registration, when the
/// call returns, fails, or is cancelled, removes exactly the jobs it
/// registered; a deduplicated submission registers nothing, so it never
/// removes another waiter's job.
struct AwaitingRegistration {
    submitters: AwaitingSubmitters,
    span: tracing::Span,
    job_ids: Mutex<Vec<JobId>>,
}

impl AwaitingRegistration {
    /// Infallible: the job's row is already inserted, so a failure here would
    /// report a submitted job as rejected.
    fn register(&self, job_id: JobId) {
        let mut submitters = self
            .submitters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut job_ids = self
            .job_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        submitters.insert(job_id, self.span.clone());
        job_ids.push(job_id);
    }
}

impl Drop for AwaitingRegistration {
    fn drop(&mut self) {
        let job_ids = std::mem::take(
            self.job_ids
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let mut submitters = self
            .submitters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for job_id in &job_ids {
            submitters.remove(job_id);
        }
    }
}

pub const COMPLETED_JOB_RETENTION: Duration = Duration::from_secs(4 * 24 * 60 * 60);
pub const FAILED_JOB_RETENTION: Duration = Duration::from_secs(10 * 24 * 60 * 60);
pub const TERMINAL_JOB_PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60);

// Retry backoff schedule (seconds): attempt 1→5s, 2→30s, 3→120s, 4→300s, 5+→300s
const DEFAULT_RETRY_BACKOFF_SECS: [u64; 4] = [5, 30, 120, 300];

/// How long a failed attempt waits before it is retried, by attempt number.
///
/// Whole seconds because that is the granularity the schedule is stored at:
/// `next_attempt_at` is `datetime('now', '+N seconds')`, so a sub-second
/// delay is not representable in the row every other process reads.
///
/// Entry `i` is the delay after attempt `i + 1`, and the last entry repeats
/// for every attempt past the end — including the out-of-range attempt
/// numbers a caller should never produce, which take the longest delay
/// rather than the shortest.
#[derive(Clone, Debug)]
pub(crate) struct RetryBackoffSchedule(Arc<[u64]>);

impl Default for RetryBackoffSchedule {
    fn default() -> Self {
        Self(Arc::from(DEFAULT_RETRY_BACKOFF_SECS.as_slice()))
    }
}

impl RetryBackoffSchedule {
    /// Shorten the backoff for a test that has to observe a scheduled retry
    /// arrive. Production keeps [`Default`](Self::default).
    #[cfg(test)]
    pub(crate) fn from_secs(delays: impl IntoIterator<Item = u64>) -> Self {
        let delays: Arc<[u64]> = delays.into_iter().collect();
        assert!(
            !delays.is_empty(),
            "a retry schedule needs at least one delay"
        );
        Self(delays)
    }

    fn delay_secs(&self, attempt: i32) -> u64 {
        let last = *self
            .0
            .last()
            .expect("a retry schedule always holds at least one delay");
        usize::try_from(attempt)
            .ok()
            .and_then(|attempt| attempt.checked_sub(1))
            .and_then(|index| self.0.get(index).copied())
            .unwrap_or(last)
    }

    /// Sum of retry backoffs for a job with `max_attempts` attempts.
    fn total_backoff_secs(&self, max_attempts: i32) -> u64 {
        (1..max_attempts)
            .map(|attempt| self.delay_secs(attempt))
            .sum()
    }
}

/// Cap on permanent-failure rows scanned at shutdown. A queue accumulating
/// failures over many restarts should not turn shutdown into an O(N) log
/// spew; the operator gets a count plus the most recent entries.
const PERMANENT_FAILURE_LOG_CAP: u32 = 100;

/// Configuration for a [`BackgroundWorker`].
pub struct WorkerConfig {
    /// Job kinds this worker may requeue during startup recovery. Empty means
    /// all kinds, preserving the shared-worker default.
    pub recovery_job_kinds: Vec<String>,
    /// Concurrency limits per job kind. Kinds not listed use `default_concurrency`.
    pub kind_concurrency: Vec<(String, usize)>,
    /// Maximum active jobs per kind. Kinds not listed have no admission limit.
    pub kind_active_job_limits: Vec<(String, usize)>,
    /// Per-kind capacity bands protected above configurable priority thresholds.
    pub kind_priority_bands: Vec<(String, Vec<PriorityBand>)>,
    /// Fallback concurrency for job kinds not in `kind_concurrency`.
    pub default_concurrency: usize,
    /// Per-attempt timeout. Retriable error on expiry.
    pub job_timeout: Duration,
    /// Maximum attempts before a retriable job is permanently failed.
    pub max_attempts: i32,
    /// Optional multi-process lease configuration. Disabled by default, which
    /// preserves the legacy single-process worker behavior.
    pub lease: Option<LeaseConfig>,
    /// Retry backoff every worker shares. Private with a test-only setter:
    /// the production schedule is a property of the queue, not of the
    /// service configuring it.
    retry_backoff: RetryBackoffSchedule,
}

impl WorkerConfig {
    pub fn new(default_concurrency: usize, job_timeout: Duration, max_attempts: i32) -> Self {
        Self {
            recovery_job_kinds: Vec::new(),
            kind_concurrency: Vec::new(),
            kind_active_job_limits: Vec::new(),
            kind_priority_bands: Vec::new(),
            default_concurrency,
            job_timeout,
            max_attempts,
            lease: None,
            retry_backoff: RetryBackoffSchedule::default(),
        }
    }

    pub fn with_kind_concurrency(mut self, kind: impl Into<String>, concurrency: usize) -> Self {
        self.kind_concurrency.push((kind.into(), concurrency));
        self
    }

    pub fn with_recovery_job_kind(mut self, kind: impl Into<String>) -> Self {
        self.recovery_job_kinds.push(kind.into());
        self
    }

    pub fn with_kind_active_job_limit(mut self, kind: impl Into<String>, limit: usize) -> Self {
        self.kind_active_job_limits.push((kind.into(), limit));
        self
    }

    pub fn with_kind_priority_bands(
        mut self,
        kind: impl Into<String>,
        bands: impl IntoIterator<Item = PriorityBand>,
    ) -> Self {
        self.kind_priority_bands
            .push((kind.into(), bands.into_iter().collect()));
        self
    }

    pub fn with_lease(mut self, lease: LeaseConfig) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Shorten the retry backoff so a test can wait for a scheduled retry
    /// instead of sitting out the production schedule. Production workers
    /// never call this and keep [`RetryBackoffSchedule::default`].
    #[cfg(test)]
    pub(crate) fn with_retry_backoff_secs(mut self, delays: impl IntoIterator<Item = u64>) -> Self {
        self.retry_backoff = RetryBackoffSchedule::from_secs(delays);
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PriorityBand {
    /// Jobs at or above this value enter this band unless a higher band matches.
    pub min_priority: JobPriority,
    /// Additional executor slots that lower bands cannot consume.
    pub concurrency: usize,
    /// Maximum pending plus running jobs in this band.
    pub active_jobs: usize,
}

impl PriorityBand {
    pub const fn new(min_priority: JobPriority, concurrency: usize, active_jobs: usize) -> Self {
        Self {
            min_priority,
            concurrency,
            active_jobs,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LeaseConfig {
    pub worker_id: String,
    pub lease_ttl: Duration,
    pub heartbeat_interval: Duration,
    pub poll_interval: Duration,
    pub requeue_expired_leases: bool,
    pub subject_concurrency: SubjectConcurrency,
}

impl LeaseConfig {
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            lease_ttl: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(15),
            poll_interval: Duration::from_secs(1),
            requeue_expired_leases: true,
            subject_concurrency: SubjectConcurrency::Parallel,
        }
    }

    pub fn with_lease_ttl(mut self, lease_ttl: Duration) -> Self {
        self.lease_ttl = lease_ttl;
        self
    }

    pub fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_expired_lease_requeue(mut self, requeue_expired_leases: bool) -> Self {
        self.requeue_expired_leases = requeue_expired_leases;
        self
    }

    pub fn with_serialized_subjects(mut self) -> Self {
        self.subject_concurrency = SubjectConcurrency::Serialized;
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DuplicatePolicy {
    ReuseExisting,
    /// Reuse an active or successfully completed job with the same dedup key.
    /// Failed and cancelled jobs remain retryable through a new job.
    ReuseExistingOrCompleted,
    Reject,
}

#[derive(Clone, Copy)]
struct SubmitOptions {
    priority: JobPriority,
    duplicate_policy: DuplicatePolicy,
}

#[derive(Clone, Copy)]
struct AdmissionBand {
    min_priority: Option<JobPriority>,
    max_priority: Option<JobPriority>,
    active_jobs: usize,
}

impl AdmissionBand {
    fn select(default_active_jobs: usize, bands: &[PriorityBand], priority: JobPriority) -> Self {
        let index = bands.partition_point(|band| priority >= band.min_priority);
        let selected_band = index.checked_sub(1).and_then(|band| bands.get(band));
        Self {
            min_priority: selected_band.map(|band| band.min_priority),
            max_priority: bands.get(index).map(|band| band.min_priority),
            active_jobs: selected_band.map_or(default_active_jobs, |band| band.active_jobs),
        }
    }

    fn metric_label(self) -> String {
        priority_band_metric_label(self.min_priority)
    }
}

fn priority_band_metric_label(min_priority: Option<JobPriority>) -> String {
    min_priority.map_or_else(
        || "default".to_string(),
        |priority| priority.get().to_string(),
    )
}

/// The bare sandbox UUID a `sandbox:<uuid>` subject names: the canonical
/// `sandbox_id` that every service's spans and logs carry, so a sandbox's
/// jobs are found by the same value as the rest of its telemetry.
/// Name the job a deduplicated submission joined on the submitter's span. The
/// job runs in the trace of the submission that created it, so this is how
/// the submitter's trace finds it.
fn record_deduplicated_submission(job_id: &JobId) {
    let span = tracing::Span::current();
    pplx_space_observability::trace_propagation::set_span_attribute(
        &span,
        "job.id",
        &job_id.to_string(),
    );
    pplx_space_observability::trace_propagation::set_span_attribute(&span, "job.dedup", "hit");
}

fn subject_sandbox_id(subject_id: Option<&str>) -> Option<String> {
    match subject_id?.parse::<SubjectId>().ok()? {
        SubjectId::Sandbox(sandbox_id) => Some(sandbox_id.to_string()),
        SubjectId::Template(_) => None,
    }
}

/// How the lease claim treats jobs that share a `(kind, subject_id)`.
///
/// `Serialized` makes the claim skip a pending job while another job with the
/// same kind and subject is `running`, or while an older pending job for the
/// same subject exists (FIFO order is `(created_at, job_id)`; job ids are
/// UUIDv7, so the id tie-break preserves submission order within a second).
/// Jobs with a NULL subject are never serialized. Note an older pending job in
/// retry backoff still holds its FIFO slot until it reaches a terminal state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SubjectConcurrency {
    #[default]
    Parallel,
    Serialized,
}

/// A terminal-job prune that failed in the database. `Display` and `source`
/// are the SQLite error's; the error itself stays private to this crate, so a
/// worker's owner gets no database value back.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct PruneTerminalJobsError(rusqlite::Error);

/// A background job worker that persists jobs to SQLite and drives execution
/// with bounded concurrency, exponential-backoff retry, and graceful drain.
///
/// `Clone` is cheap — all internal state is `Arc`-wrapped.
#[derive(Clone)]
pub struct BackgroundWorker<E> {
    db: Arc<Mutex<rusqlite::Connection>>,
    executor: E,
    schedulers: Arc<HashMap<String, Arc<PriorityScheduler>>>,
    priority_bands: Arc<HashMap<String, Vec<PriorityBand>>>,
    active_job_limits: Arc<HashMap<String, usize>>,
    recovery_job_kinds: Arc<Vec<String>>,
    default_scheduler: Arc<PriorityScheduler>,
    inflight: Arc<AtomicUsize>,
    stopping: CancellationToken,
    completion_waiters: CompletionWaiters,
    cancellation_tokens: Arc<Mutex<HashMap<JobId, CancellationToken>>>,
    /// The waiting submitter's span for each job a
    /// [`submit_and_wait`](Self::submit_and_wait) call created, held only
    /// while it waits.
    awaiting_submitters: AwaitingSubmitters,
    job_timeout: Duration,
    max_attempts: i32,
    lease: Option<LeaseConfig>,
    retry_backoff: RetryBackoffSchedule,
}

impl<E: JobExecutor> BackgroundWorker<E> {
    pub fn new(db: Arc<Mutex<rusqlite::Connection>>, executor: E, config: WorkerConfig) -> Self {
        let mut priority_bands = HashMap::<String, Vec<PriorityBand>>::new();
        for (kind, mut bands) in config.kind_priority_bands {
            bands.sort_by_key(|band| band.min_priority);
            assert!(
                bands.iter().all(|band| band.concurrency > 0),
                "priority band concurrency must be greater than zero for {kind}"
            );
            assert!(
                bands.iter().all(|band| band.active_jobs > 0),
                "priority band active-job limit must be greater than zero for {kind}"
            );
            assert!(
                bands.windows(2).all(
                    |pair| matches!(pair, [lower, upper] if lower.min_priority < upper.min_priority)
                ),
                "priority bands must have unique minimum priorities for {kind}"
            );
            if !bands.is_empty() {
                assert!(
                    priority_bands.insert(kind.clone(), bands).is_none(),
                    "priority bands configured more than once for {kind}"
                );
            }
        }
        let schedulers: HashMap<String, Arc<PriorityScheduler>> = config
            .kind_concurrency
            .into_iter()
            .map(|(kind, concurrency)| {
                let bands = priority_bands.get(&kind).map_or_else(Vec::new, |bands| {
                    bands
                        .iter()
                        .map(|band| SchedulerBand {
                            min_priority: band.min_priority,
                            concurrency: band.concurrency,
                        })
                        .collect()
                });
                (kind, PriorityScheduler::new(concurrency, bands))
            })
            .collect();
        for kind in priority_bands.keys() {
            assert!(
                schedulers.contains_key(kind),
                "priority bands require explicit concurrency for {kind}"
            );
            assert!(
                config
                    .kind_active_job_limits
                    .iter()
                    .any(|(configured_kind, _)| configured_kind == kind),
                "priority bands require an active-job limit for {kind}"
            );
        }
        let active_job_limits = config.kind_active_job_limits.into_iter().collect();
        Self {
            db,
            executor,
            schedulers: Arc::new(schedulers),
            priority_bands: Arc::new(priority_bands),
            active_job_limits: Arc::new(active_job_limits),
            recovery_job_kinds: Arc::new(config.recovery_job_kinds),
            default_scheduler: PriorityScheduler::new(config.default_concurrency, Vec::new()),
            inflight: Arc::new(AtomicUsize::new(0)),
            stopping: CancellationToken::new(),
            completion_waiters: CompletionWaiters::default(),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
            awaiting_submitters: Arc::new(Mutex::new(HashMap::new())),
            job_timeout: config.job_timeout,
            max_attempts: config.max_attempts,
            lease: config.lease,
            retry_backoff: config.retry_backoff,
        }
    }

    /// Budget for executor attempts and their retry backoffs. Terminal-state
    /// persistence can exceed this budget while SQLite is unavailable: a
    /// synchronous waiter may time out, but the worker retains the successful
    /// result and execution ownership until it can commit or is superseded.
    pub fn worst_case_runtime(&self) -> Duration {
        let attempts = self.max_attempts.max(1).cast_unsigned();
        let timeouts = self
            .job_timeout
            .checked_mul(attempts)
            .unwrap_or(Duration::from_secs(u64::MAX / 2));
        timeouts + Duration::from_secs(self.retry_backoff.total_backoff_secs(self.max_attempts))
    }

    /// Convenience wrapper around [`recover`](Self::recover) that emits an
    /// info log when anything was re-queued. `worker_label` shows up in the
    /// log so multi-worker services (sbx-mgr, vol-mgr, …) can tell which
    /// queue the entries belong to.
    pub fn recover_and_log(&self, worker_label: &str) -> Vec<JobId> {
        let recovered = self.recover();
        if !recovered.is_empty() {
            tracing::info!(
                worker = worker_label,
                count = recovered.len(),
                "re-queued jobs interrupted by previous process restart"
            );
        }
        recovered
    }

    pub fn prune_terminal_jobs_once(&self, worker_label: &str) -> usize {
        match self.try_prune_terminal_jobs_once(worker_label) {
            Ok(pruned) => pruned,
            Err(error) => {
                tracing::warn!(
                    worker = worker_label,
                    %error,
                    "failed to prune old terminal jobs"
                );
                0
            }
        }
    }

    /// Preserve database failures so maintenance callers can retry without
    /// treating an unsuccessful pass as a successful zero-deletion pass.
    pub fn try_prune_terminal_jobs_once(
        &self,
        worker_label: &str,
    ) -> Result<usize, PruneTerminalJobsError> {
        let pruned = {
            let conn = self.lock_db_for_background();
            db::prune_terminal_jobs(&conn, COMPLETED_JOB_RETENTION, FAILED_JOB_RETENTION)
                .map_err(PruneTerminalJobsError)?
        };
        if pruned > 0 {
            tracing::info!(
                worker = worker_label,
                pruned,
                completed_retention_secs = COMPLETED_JOB_RETENTION.as_secs(),
                failed_retention_secs = FAILED_JOB_RETENTION.as_secs(),
                "pruned old terminal jobs"
            );
        }
        Ok(pruned)
    }

    pub async fn run_terminal_job_pruner(
        self,
        worker_label: &'static str,
        shutdown_token: CancellationToken,
    ) {
        self.prune_terminal_jobs_once(worker_label);
        let first_tick = tokio::time::Instant::now() + TERMINAL_JOB_PRUNE_INTERVAL;
        let mut interval = tokio::time::interval_at(first_tick, TERMINAL_JOB_PRUNE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.prune_terminal_jobs_once(worker_label);
                }
                _ = shutdown_token.cancelled() => return,
            }
        }
    }

    /// Shutdown helper that drains durable jobs and spawned executor tasks,
    /// then logs any permanent failures still in the queue. Replaces the
    /// `drain(..).await; log_permanent_failures();` pair every service
    /// binary used to write at the end of `main`. Returns whether the worker
    /// proved quiescence before the deadline.
    pub async fn shutdown(&self, drain_timeout: Duration) -> bool {
        let quiescent = self.drain_until_quiescent(drain_timeout).await;
        self.log_permanent_failures();
        quiescent
    }

    /// Stop launching work, ask in-flight executors to yield at a safe
    /// boundary, and wait until no executor remains. Yielded durable jobs
    /// remain pending for startup recovery by the successor.
    pub async fn yield_for_restart(&self, timeout: Duration) -> bool {
        self.stopping.cancel();
        for token in self
            .cancellation_tokens
            .lock()
            .expect("common jobs cancellation mutex poisoned")
            .values()
        {
            token.cancel();
        }

        let deadline = Instant::now() + timeout;
        loop {
            let inflight = self.inflight();
            if inflight == 0 {
                self.log_permanent_failures();
                return true;
            }
            if Instant::now() >= deadline {
                tracing::error!(
                    inflight,
                    "restart yield timed out before executor quiescence"
                );
                self.log_permanent_failures();
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits for the durable queue and every spawned executor task to become
    /// quiescent, returning false when either remains at the deadline.
    async fn drain_until_quiescent(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut last_log: Option<Instant> = None;

        loop {
            let active_jobs = {
                let conn = self.lock_db_for_background();
                db::count_active_jobs(&conn).unwrap_or(0)
            };
            let inflight = self.inflight();
            pplx_space_metrics::jobs::drain_outstanding(active_jobs);
            if active_jobs == 0 && inflight == 0 {
                return true;
            }
            if Instant::now() >= deadline {
                tracing::error!(
                    active_jobs,
                    inflight,
                    "SIGTERM drain timeout reached before executor quiescence"
                );
                return false;
            }
            if last_log.is_none_or(|logged| logged.elapsed() >= Duration::from_secs(5)) {
                tracing::warn!(
                    active_jobs,
                    inflight,
                    "SIGTERM received — waiting for durable jobs and executor tasks to quiesce"
                );
                last_log = Some(Instant::now());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Re-queue any jobs interrupted by a previous crash. Call once at startup
    /// before accepting new work. Honors `next_attempt_at`: jobs scheduled in
    /// the future are spawned via a delayed task so backoff isn't lost across
    /// restarts.
    pub fn recover(&self) -> Vec<JobId> {
        let cancelling = {
            let conn = self.lock_db_for_background();
            db::cancelling_jobs(&conn, &self.recovery_job_kinds).unwrap_or_else(|error| {
                tracing::error!(%error, "failed to recover cancelling jobs");
                Vec::new()
            })
        };
        for job in cancelling {
            let worker = self.clone();
            tokio::spawn(async move {
                worker.finish_cancelled_job(job).await;
            });
        }
        if self.lease.is_some() {
            self.spawn_due_leased_jobs();
            return Vec::new();
        }

        let jobs = {
            let conn = self.lock_db_for_background();
            db::recover_interrupted_jobs(&conn, &self.recovery_job_kinds).unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to recover interrupted jobs");
                vec![]
            })
        };
        let job_ids: Vec<JobId> = jobs.iter().map(|(id, _, _, _)| *id).collect();
        for (job_id, kind, _, delay_secs) in &jobs {
            pplx_space_metrics::jobs::recovered(kind);
            let delay = db::nonnegative_i64_to_u64(*delay_secs);
            if delay == 0 {
                self.spawn_job(*job_id, kind);
            } else {
                let worker = self.clone();
                let job_id = *job_id;
                let kind = kind.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    worker.spawn_job_for_mode(job_id, &kind);
                });
            }
        }
        job_ids
    }

    pub async fn run_queue_driver(self, shutdown_token: CancellationToken) {
        let Some(lease) = self.lease.clone() else {
            return;
        };
        let mut interval = tokio::time::interval(lease.poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => self.spawn_due_leased_jobs(),
                _ = shutdown_token.cancelled() => return,
            }
        }
    }

    /// Like [`submit`](Self::submit) but accepts any `Serialize` payload and
    /// JSON-encodes it for you. Saves callers from repeating the
    /// `serde_json::to_string(...)?` boilerplate at every submission site.
    /// A serialization failure surfaces as `INVALID_ARGUMENT`.
    pub fn submit_json<T: serde::Serialize>(
        &self,
        kind: &str,
        payload: &T,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
    ) -> Result<JobId, Status> {
        self.submit_json_with_duplicate_policy(
            kind,
            payload,
            subject_id,
            dedup_key,
            DuplicatePolicy::ReuseExisting,
        )
    }

    pub fn submit_json_with_priority<T: serde::Serialize>(
        &self,
        kind: &str,
        payload: &T,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
        priority: JobPriority,
    ) -> Result<JobId, Status> {
        let json = serde_json::to_string(payload)
            .map_err(|e| Status::invalid_argument(format!("failed to encode job payload: {e}")))?;
        self.submit_with_priority(kind, &json, subject_id, dedup_key, priority)
    }

    pub fn submit_json_with_duplicate_policy<T: serde::Serialize>(
        &self,
        kind: &str,
        payload: &T,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
        duplicate_policy: DuplicatePolicy,
    ) -> Result<JobId, Status> {
        let json = serde_json::to_string(payload)
            .map_err(|e| Status::invalid_argument(format!("failed to encode job payload: {e}")))?;
        self.submit_with_duplicate_policy(kind, &json, subject_id, dedup_key, duplicate_policy)
    }

    /// Submit a job. Returns the job_id (new or existing when deduplication fires).
    pub fn submit(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
    ) -> Result<JobId, Status> {
        self.submit_with_duplicate_policy(
            kind,
            payload,
            subject_id,
            dedup_key,
            DuplicatePolicy::ReuseExisting,
        )
    }

    pub fn submit_with_priority(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
        priority: JobPriority,
    ) -> Result<JobId, Status> {
        self.submit_with_options(
            kind,
            payload,
            subject_id,
            dedup_key,
            SubmitOptions {
                priority,
                duplicate_policy: DuplicatePolicy::ReuseExisting,
            },
        )
    }

    pub fn submit_with_duplicate_policy(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
        duplicate_policy: DuplicatePolicy,
    ) -> Result<JobId, Status> {
        self.submit_with_options(
            kind,
            payload,
            subject_id,
            dedup_key,
            SubmitOptions {
                priority: JobPriority::default(),
                duplicate_policy,
            },
        )
    }

    fn submit_with_options(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
        options: SubmitOptions,
    ) -> Result<JobId, Status> {
        let SubmitOptions {
            priority,
            duplicate_policy,
        } = options;
        let conn = self.lock_db_for_status()?;

        let existing = match (dedup_key, duplicate_policy) {
            (Some(dk), DuplicatePolicy::ReuseExistingOrCompleted) => {
                db::find_job_by_dedup_key(&conn, kind, dk)
                    .map_err(|e| Status::internal(format!("db error: {e}")))?
                    .filter(|job| {
                        matches!(
                            job.status.as_str(),
                            "pending" | "running" | "cancelling" | "completed"
                        )
                    })
                    .map(|job| job.job_id)
            }
            (Some(dk), _) => db::find_active_job(&conn, kind, dk)
                .map_err(|e| Status::internal(format!("db error: {e}")))?,
            (None, _) => None,
        };
        if let Some(existing) = existing {
            if duplicate_policy == DuplicatePolicy::Reject {
                return Err(Status::already_exists(format!(
                    "an active job already exists for {kind}/{}",
                    dedup_key.expect("existing jobs require a dedup key")
                )));
            }
            self.promote_deduplicated(&conn, kind, &existing, priority)?;
            tracing::debug!(job_id = %existing, kind, "job deduplicated");
            pplx_space_metrics::jobs::submitted(kind, "hit");
            record_deduplicated_submission(&existing);
            return Ok(existing);
        }

        let job_id = JobId::new();
        let traceparent = pplx_space_observability::trace_propagation::current_traceparent();
        let job = db::InsertJob {
            job_id: &job_id,
            kind,
            payload,
            subject_id,
            dedup_key,
            max_attempts: self.max_attempts,
            traceparent: traceparent.as_deref(),
            priority,
        };
        let admission_band = self.active_job_limits.get(kind).map(|default_active_jobs| {
            AdmissionBand::select(
                *default_active_jobs,
                self.priority_bands.get(kind).map_or(&[], Vec::as_slice),
                priority,
            )
        });
        let insert_result = match admission_band {
            Some(band) => db::insert_job_with_priority_band_active_limit(
                &conn,
                job,
                band.min_priority,
                band.max_priority,
                band.active_jobs,
            ),
            None => db::insert_job(&conn, job).map(|()| db::InsertJobOutcome::Inserted),
        };
        let insert_outcome = match insert_result {
            Ok(outcome) => outcome,
            Err(error) => {
                if let Some(dk) = dedup_key
                    && is_constraint_violation(&error)
                    && let Some(existing) = db::find_active_job(&conn, kind, dk)
                        .map_err(|e| Status::internal(format!("db error: {e}")))?
                {
                    if duplicate_policy == DuplicatePolicy::Reject {
                        return Err(Status::already_exists(format!(
                            "an active job already exists for {kind}/{dk}"
                        )));
                    }
                    self.promote_deduplicated(&conn, kind, &existing, priority)?;
                    tracing::debug!(job_id = %existing, kind, "job deduplicated after insert race");
                    pplx_space_metrics::jobs::submitted(kind, "hit");
                    record_deduplicated_submission(&existing);
                    return Ok(existing);
                }
                return Err(Status::internal(format!("db insert failed: {error}")));
            }
        };
        if insert_outcome == db::InsertJobOutcome::AtActiveLimit {
            let Some(admission_band) = admission_band else {
                return Err(Status::internal(
                    "job insert reported an active limit without a configured limit",
                ));
            };
            if let Some(dk) = dedup_key
                && let Some(existing) = db::find_active_job(&conn, kind, dk)
                    .map_err(|e| Status::internal(format!("db error: {e}")))?
            {
                if duplicate_policy == DuplicatePolicy::Reject {
                    return Err(Status::already_exists(format!(
                        "an active job already exists for {kind}/{dk}"
                    )));
                }
                self.promote_deduplicated(&conn, kind, &existing, priority)?;
                tracing::debug!(job_id = %existing, kind, "job deduplicated at admission limit");
                pplx_space_metrics::jobs::submitted(kind, "hit");
                record_deduplicated_submission(&existing);
                return Ok(existing);
            }
            let priority_band = admission_band.metric_label();
            tracing::debug!(
                kind,
                priority = priority.get(),
                priority_band,
                selected_limit = admission_band.active_jobs,
                subject_id,
                "job admission rejected"
            );
            pplx_space_metrics::jobs::admission_rejected(kind, &priority_band);
            return Err(Status::resource_exhausted(format!(
                "{kind} job admission limit reached ({} active jobs in priority band {priority_band})",
                admission_band.active_jobs
            )));
        }
        // Registered before the database lock is released, so no claim of the
        // new row can miss its waiting submitter.
        if let Ok(Some(registration)) = AWAITING_SUBMITTER.try_with(|registration| {
            Arc::ptr_eq(&registration.submitters, &self.awaiting_submitters)
                .then(|| registration.clone())
        }) {
            registration.register(job_id);
        }
        drop(conn);

        pplx_space_metrics::jobs::submitted(kind, "miss");
        tracing::info!(
            job_id = %job_id,
            kind,
            priority = priority.get(),
            subject_id,
            sandbox_id = subject_sandbox_id(subject_id),
            "job submitted"
        );
        self.spawn_job_for_mode(job_id, kind);
        Ok(job_id)
    }

    fn promote_deduplicated(
        &self,
        conn: &rusqlite::Connection,
        kind: &str,
        job_id: &JobId,
        priority: JobPriority,
    ) -> Result<(), Status> {
        db::raise_pending_job_priority(conn, job_id, priority)
            .map_err(|error| Status::internal(format!("db error: {error}")))?;
        self.scheduler_for(kind)
            .raise_waiting_priority(job_id, priority);
        Ok(())
    }

    /// Run `submission`, which submits one job through this worker, then
    /// [`wait_for_job`](Self::wait_for_job) for the job it returns.
    ///
    /// A job the submission creates runs as a child of the current span, a
    /// real in-process parent for work the caller is waiting on. Any other job
    /// starts its own trace with a link to its submitter (see
    /// `execute_claimed_job`), and so does a deduplicated submission, whose job
    /// another submitter created.
    pub async fn submit_and_wait<F>(&self, submission: F, timeout: Duration) -> Result<Job, Status>
    where
        F: Future<Output = Result<JobId, Status>>,
    {
        // Attempts started while the caller waited keep their parent. Once the
        // call ends, however it ends, a retry, or a job still queued when it
        // gave up, starts its own trace.
        let registration = Arc::new(AwaitingRegistration {
            submitters: self.awaiting_submitters.clone(),
            span: tracing::Span::current(),
            job_ids: Mutex::default(),
        });
        let job_id = AWAITING_SUBMITTER
            .scope(registration.clone(), submission)
            .await?;
        let job = self.wait_for_job(&job_id, timeout).await;
        drop(registration);
        job
    }

    /// Block until the persisted job reaches a terminal state or the timeout elapses.
    pub async fn wait_for_job(&self, job_id: &JobId, timeout: Duration) -> Result<Job, Status> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut completion_notified_at: Option<Instant> = None;
        loop {
            let mut completion = self.completion_waiters.register(job_id)?;
            let job = {
                let conn = self.lock_db_for_status()?;
                match db::get_job(&conn, job_id) {
                    Ok(Some(j)) => j,
                    Ok(None) => {
                        return Err(Status::not_found(format!("job not found: {job_id}")));
                    }
                    Err(e) => return Err(Status::internal(format!("db error: {e}"))),
                }
            };

            if job.is_terminal() {
                if let Some(notified_at) = completion_notified_at {
                    pplx_space_metrics::jobs::completion_observation_delay(
                        &job.kind,
                        notified_at.elapsed(),
                    );
                }
                return Ok(job);
            }

            completion_notified_at = tokio::select! {
                notified_at = completion.notified() => notified_at,
                _ = tokio::time::sleep(Duration::from_millis(500)) => None,
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(Status::deadline_exceeded(format!(
                        "timed out waiting for job {job_id}"
                    )));
                }
            };
        }
    }

    /// Durably request cancellation and wait until the executor has stopped
    /// writing and completed its cancellation cleanup.
    pub async fn cancel_job_and_wait(
        &self,
        job_id: &JobId,
        timeout: Duration,
    ) -> Result<Job, Status> {
        let (previous_status, job) = {
            let conn = self.lock_db_for_status()?;
            let previous = db::get_job(&conn, job_id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found(format!("job not found: {job_id}")))?;
            let job = db::request_job_cancellation(&conn, job_id)
                .map_err(|error| Status::internal(format!("db error: {error}")))?
                .ok_or_else(|| Status::not_found(format!("job not found: {job_id}")))?;
            (previous.status, job)
        };
        if job.is_terminal() {
            return Ok(job);
        }
        tracing::info!(
            job_id = %job.job_id,
            kind = %job.kind,
            previous_status,
            "job cancellation requested"
        );
        pplx_space_metrics::jobs::cancellation_requested(&job.kind);

        let token = self
            .cancellation_tokens
            .lock()
            .map_err(|_| Status::internal("common jobs cancellation mutex poisoned"))?
            .get(job_id)
            .cloned();
        if let Some(token) = token {
            token.cancel();
        } else if previous_status != "running" {
            let worker = self.clone();
            let job = job.clone();
            tokio::spawn(async move {
                worker.finish_cancelled_job(job).await;
            });
        }
        self.wait_for_job(job_id, timeout).await
    }

    /// Look up a job by id.
    pub fn get_job(&self, job_id: &JobId) -> Result<Option<Job>, Status> {
        let conn = self.lock_db_for_status()?;
        db::get_job(&conn, job_id).map_err(|e| Status::internal(format!("db error: {e}")))
    }

    /// Look up the newest job with the given `kind` and `dedup_key`, regardless
    /// of status. See [`db::find_job_by_dedup_key`] — used to recover the
    /// durable record of an idempotent batch (e.g. the persisted wrapped DEK)
    /// when a retry arrives after the original job already finished, which
    /// [`submit`](Self::submit)'s `find_active_job` dedup (pending/running
    /// only) would otherwise miss, minting a fresh job and key.
    pub fn find_job_by_dedup_key(
        &self,
        kind: &str,
        dedup_key: &str,
    ) -> Result<Option<Job>, Status> {
        let conn = self.lock_db_for_status()?;
        db::find_job_by_dedup_key(&conn, kind, dedup_key)
            .map_err(|e| Status::internal(format!("db error: {e}")))
    }

    /// List jobs, optionally filtered by subject and/or status.
    pub fn list_jobs(
        &self,
        subject_id: Option<&str>,
        status: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Job>, Status> {
        let conn = self.lock_db_for_status()?;
        db::list_jobs(&conn, subject_id, status, limit)
            .map_err(|e| Status::internal(format!("db error: {e}")))
    }

    /// Number of currently executing tasks.
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    pub fn lease_worker_id(&self) -> Option<&str> {
        self.lease.as_ref().map(|lease| lease.worker_id.as_str())
    }

    /// Log permanently failed jobs at ERROR level. Call before exit so
    /// operators can see which jobs need manual remediation. The number of
    /// per-job log lines is capped at [`PERMANENT_FAILURE_LOG_CAP`]; the count
    /// in the summary line is the full total.
    pub fn log_permanent_failures(&self) {
        let (jobs, total) = {
            let conn = self.lock_db_for_background();
            let total = db::count_permanent_failures(&conn).unwrap_or(0);
            let jobs =
                db::list_permanent_failures(&conn, PERMANENT_FAILURE_LOG_CAP).unwrap_or_default();
            (jobs, total)
        };
        if total == 0 {
            return;
        }
        tracing::error!(
            count = total,
            logged = jobs.len(),
            "permanently failed jobs in queue — these will NOT be retried and require \
             manual intervention:"
        );
        for job in &jobs {
            tracing::error!(
                job_id = %job.job_id,
                kind = %job.kind,
                subject_id = ?job.subject_id,
                error = ?job.error,
                attempts = job.attempts,
                completed_at = ?job.completed_at,
                "permanently failed job"
            );
        }
    }

    /// Wait until all pending and running jobs complete or the timeout elapses.
    /// Logs a warning every 5 seconds while jobs are outstanding (the first log
    /// fires immediately so operators see drain activity right away).
    pub async fn drain(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut last_log: Option<Instant> = None;

        loop {
            let active = {
                let conn = self.lock_db_for_background();
                db::count_active_jobs(&conn).unwrap_or(0)
            };

            pplx_space_metrics::jobs::drain_outstanding(active);

            if active == 0 {
                return;
            }

            if Instant::now() >= deadline {
                tracing::error!(
                    active_jobs = active,
                    "SIGTERM drain timeout reached — forcing exit with outstanding jobs; \
                     they will be recovered on next start"
                );
                return;
            }

            let should_log = last_log.is_none_or(|t| t.elapsed() >= Duration::from_secs(5));
            if should_log {
                tracing::warn!(
                    active_jobs = active,
                    "SIGTERM received — waiting for {} outstanding job(s) to complete \
                     before exiting",
                    active
                );
                last_log = Some(Instant::now());
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Emit periodic queue-state gauges (`jobs.queue.depth`,
    /// `jobs.queue.oldest_pending_age_seconds`, `jobs.permanent_failures`)
    /// every [`DEFAULT_QUEUE_METRICS_INTERVAL`] until the cancellation token
    /// fires. Caller `tokio::spawn`s this from service init alongside the
    /// other background loops; it exits cooperatively on shutdown.
    pub async fn run_queue_metrics(self, shutdown_token: CancellationToken) {
        let mut interval = tokio::time::interval(DEFAULT_QUEUE_METRICS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => self.emit_queue_metrics(),
                _ = shutdown_token.cancelled() => return,
            }
        }
    }

    fn emit_queue_metrics(&self) {
        let (depth, oldest, perm) = {
            let conn = self.lock_db_for_background();
            let depth = db::count_active_jobs_by_kind_and_status(&conn).unwrap_or_default();
            let oldest = db::oldest_pending_age_secs_by_kind(&conn).unwrap_or_default();
            let perm = db::count_permanent_failures_by_kind(&conn).unwrap_or_default();
            (depth, oldest, perm)
        };
        for (kind, status, count) in depth {
            pplx_space_metrics::jobs::queue_depth(
                &kind,
                &status,
                db::nonnegative_i64_to_u64(count),
            );
        }
        for (kind, age) in oldest {
            pplx_space_metrics::jobs::queue_oldest_pending_age_seconds(&kind, age.max(0) as f64);
        }
        for (kind, count) in perm {
            pplx_space_metrics::jobs::permanent_failures(&kind, db::nonnegative_i64_to_u64(count));
        }
        for (kind, scheduler) in self.schedulers.iter() {
            let snapshot = scheduler.snapshot();
            for band in snapshot.bands {
                let priority_band = priority_band_metric_label(band.min_priority);
                pplx_space_metrics::jobs::scheduler_depth(
                    kind,
                    pplx_space_metrics::jobs::SchedulerState::Running,
                    &priority_band,
                    band.running,
                );
                pplx_space_metrics::jobs::scheduler_depth(
                    kind,
                    pplx_space_metrics::jobs::SchedulerState::Waiting,
                    &priority_band,
                    band.waiting,
                );
            }
        }
    }

    fn scheduler_for(&self, kind: &str) -> Arc<PriorityScheduler> {
        self.schedulers
            .get(kind)
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.default_scheduler))
    }

    fn notify_completion(&self, job_id: &JobId) {
        self.completion_waiters.notify(job_id);
    }

    fn spawn_job(&self, job_id: JobId, kind: &str) {
        if self.stopping.is_cancelled() {
            return;
        }
        let scheduler = self.scheduler_for(kind);
        let Some(acquire) = self.with_current_job_priority(&job_id, kind, |priority| {
            scheduler.acquire(job_id, priority)
        }) else {
            return;
        };
        let worker = self.clone();
        tokio::spawn(async move {
            let permit = acquire.await;
            if !worker.stopping.is_cancelled() {
                worker.run_job(job_id, permit).await;
            }
        });
    }

    fn spawn_leased_job(&self, job_id: JobId, kind: &str) {
        if self.stopping.is_cancelled() {
            return;
        }
        let scheduler = self.scheduler_for(kind);
        let Some(acquire) = self.with_current_job_priority(&job_id, kind, |priority| {
            scheduler.acquire(job_id, priority)
        }) else {
            return;
        };
        let worker = self.clone();
        tokio::spawn(async move {
            let permit = acquire.await;
            if !worker.stopping.is_cancelled() {
                worker.run_leased_job(job_id, permit).await;
            }
        });
    }

    fn spawn_job_for_mode(&self, job_id: JobId, kind: &str) {
        if self.lease.is_some() {
            self.spawn_leased_job(job_id, kind);
        } else {
            self.spawn_job(job_id, kind);
        }
    }

    fn with_current_job_priority<T>(
        &self,
        job_id: &JobId,
        kind: &str,
        action: impl FnOnce(JobPriority) -> T,
    ) -> Option<T> {
        let conn = self.lock_db_for_background();
        match db::get_job(&conn, job_id) {
            Ok(Some(job)) => Some(action(job.priority)),
            Ok(None) => {
                tracing::warn!(job_id = %job_id, kind, "skipping job missing from database");
                self.notify_completion(job_id);
                None
            }
            Err(error) => {
                tracing::error!(job_id = %job_id, kind, %error, "failed to read job priority");
                self.notify_completion(job_id);
                None
            }
        }
    }

    fn spawn_due_leased_jobs(&self) {
        let due = {
            let conn = self.lock_db_for_background();
            if let Some(lease) = &self.lease
                && lease.requeue_expired_leases
            {
                match db::requeue_expired_running_jobs(&conn) {
                    Ok(count) if count > 0 => {
                        tracing::warn!(count, "re-queued jobs with expired leases")
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(%error, "failed to re-queue jobs with expired leases")
                    }
                }
            }
            let subject_concurrency = self
                .lease
                .as_ref()
                .map(|lease| lease.subject_concurrency)
                .unwrap_or_default();
            db::list_due_pending_jobs(&conn, 100, subject_concurrency).unwrap_or_else(|e| {
                tracing::error!(error = %e, "failed to list pending jobs");
                Vec::new()
            })
        };
        let mut priorities_by_kind = HashMap::<String, Vec<JobPriority>>::new();
        for (_, kind, priority) in &due {
            priorities_by_kind
                .entry(kind.clone())
                .or_default()
                .push(*priority);
        }
        let mut remaining_by_kind_and_band = HashMap::<(String, Option<JobPriority>), usize>::new();
        for (kind, priorities) in priorities_by_kind {
            let scheduler = self.scheduler_for(&kind);
            for budget in scheduler.spawn_budgets(&priorities) {
                remaining_by_kind_and_band
                    .insert((kind.clone(), budget.min_priority), budget.permits);
            }
        }
        for (job_id, kind, priority) in due {
            let scheduler = self.scheduler_for(&kind);
            let band = scheduler.priority_band(priority);
            let remaining = remaining_by_kind_and_band
                .get_mut(&(kind.clone(), band))
                .expect("scheduler budget must exist for every due priority band");
            if *remaining == 0 {
                continue;
            }
            *remaining -= 1;
            self.spawn_leased_job(job_id, &kind);
        }
    }

    async fn run_leased_job(&self, job_id: JobId, permit: PriorityPermit) {
        let Some(lease) = self.lease.clone() else {
            self.run_job(job_id, permit).await;
            return;
        };
        let _permit = permit;

        let job = {
            let conn = self.lock_db_for_background();
            match db::try_claim_pending_job(
                &conn,
                &job_id,
                &lease.worker_id,
                lease.lease_ttl,
                lease.subject_concurrency,
            ) {
                Ok(Some(job)) => job,
                Ok(None) => {
                    self.notify_completion(&job_id);
                    return;
                }
                Err(e) => {
                    tracing::error!(job_id = %job_id, error = %e, "failed to claim leased job");
                    self.notify_completion(&job_id);
                    return;
                }
            }
        };

        self.inflight.fetch_add(1, Ordering::SeqCst);
        let heartbeat_token = CancellationToken::new();
        let heartbeat = tokio::spawn(run_heartbeat(
            Arc::clone(&self.db),
            job.job_id,
            lease.clone(),
            heartbeat_token.clone(),
        ));

        self.execute_claimed_job(job, Some(lease.worker_id.clone()))
            .await;

        heartbeat_token.cancel();
        if let Err(error) = heartbeat.await {
            tracing::error!(job_id = %job_id, %error, "job lease heartbeat task panicked");
        }
        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }

    async fn run_job(&self, job_id: JobId, _permit: PriorityPermit) {
        self.inflight.fetch_add(1, Ordering::SeqCst);

        {
            let conn = self.lock_db_for_background();
            match db::mark_job_running(&conn, &job_id) {
                Ok(true) => {}
                Ok(false) => {
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                    self.notify_completion(&job_id);
                    return;
                }
                Err(e) => {
                    tracing::error!(job_id = %job_id, error = %e, "failed to mark job running");
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                    self.notify_completion(&job_id);
                    return;
                }
            }
        }

        let job = {
            let conn = self.lock_db_for_background();
            match db::get_job(&conn, &job_id) {
                Ok(Some(j)) => j,
                Ok(None) => {
                    tracing::error!(job_id = %job_id, "job vanished from db after mark_running");
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                    self.notify_completion(&job_id);
                    return;
                }
                Err(e) => {
                    tracing::error!(job_id = %job_id, error = %e, "db error reading job");
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                    self.notify_completion(&job_id);
                    return;
                }
            }
        };

        self.execute_claimed_job(job, None).await;

        self.inflight.fetch_sub(1, Ordering::SeqCst);
    }

    async fn execute_claimed_job(&self, job: Job, lease_owner: Option<String>) {
        pplx_space_metrics::jobs::started(&job.kind, job.attempts);

        let operation_name = format!("job.{}", job.kind);
        // A submitter waiting in `submit_and_wait` is the job's parent. Any
        // other job is a root linked to the trace context saved at submission.
        // Parenting it on that saved context would hold a still-running
        // submitter's trace open for work it does not wait for, and make a
        // submitter that has returned the parent of a span that outlives it.
        // The clone keeps the submitter's span, and so its ID, alive until the
        // child holds it.
        let submitter = self
            .awaiting_submitters
            .lock()
            .expect(AWAITING_SUBMITTERS_POISONED)
            .get(&job.job_id)
            .cloned();
        let parent = submitter.as_ref().and_then(tracing::Span::id);
        let job_span = tracing::info_span!(
            parent: parent.clone(),
            "job.execute",
            otel.kind = "internal",
            otel.name = %operation_name,
            "operation.name" = %operation_name,
            "resource.name" = %job.kind,
            job.id = %job.job_id,
            job.kind = %job.kind,
            job.priority = job.priority.get(),
            job.subject_id = job.subject_id.as_deref().unwrap_or(""),
            sandbox_id = subject_sandbox_id(job.subject_id.as_deref()),
            job.attempt = job.attempts,
            job.max_attempts = job.max_attempts,
            otel.status_code = tracing::field::Empty,
            error = tracing::field::Empty,
            origin_trace_id = tracing::field::Empty,
            origin_sampled = tracing::field::Empty,
        );
        drop(submitter);
        // The root records its submitter's trace id, which a link does not
        // make searchable, and is sampled as the submitter was.
        if parent.is_none()
            && let Some(traceparent) = job.traceparent.as_deref()
        {
            pplx_space_observability::trace_propagation::link_root_to_origins(
                &job_span,
                [traceparent],
            );
        }

        async move {
            let started_at = Instant::now();
            let cancellation = CancellationToken::new();
            self.cancellation_tokens
                .lock()
                .expect("common jobs cancellation mutex poisoned")
                .insert(job.job_id, cancellation.clone());
            if self.stopping.is_cancelled() {
                cancellation.cancel();
            }
            let cancellation_already_requested = {
                let conn = self.lock_db_for_background();
                db::get_job(&conn, &job.job_id)
                    .ok()
                    .flatten()
                    .is_some_and(|current| current.status == "cancelling")
            };
            if cancellation_already_requested {
                self.cancellation_tokens
                    .lock()
                    .expect("common jobs cancellation mutex poisoned")
                    .remove(&job.job_id);
                self.finish_cancelled_job(job).await;
                return;
            }
            let result =
                tokio::time::timeout(self.job_timeout, self.executor.execute(&job, cancellation))
                    .await;
            self.cancellation_tokens
                .lock()
                .expect("common jobs cancellation mutex poisoned")
                .remove(&job.job_id);
            let span = tracing::Span::current();

            let cancellation_requested = {
                let conn = self.lock_db_for_background();
                db::get_job(&conn, &job.job_id)
                    .ok()
                    .flatten()
                    .is_some_and(|current| current.status == "cancelling")
            };
            if cancellation_requested {
                self.finish_cancelled_job(job).await;
                return;
            }

            match result {
                Ok(Ok(bytes)) => {
                    if self.complete_job(&job, lease_owner.as_deref(), bytes).await {
                        span.record("otel.status_code", "OK");
                        tracing::info!(
                            job_id = %job.job_id,
                            kind = %job.kind,
                            subject_id = ?job.subject_id,
                            bytes,
                            elapsed_ms = started_at.elapsed().as_millis(),
                            "job completed"
                        );
                        pplx_space_metrics::jobs::completed(
                            &job.kind,
                            self.executor.completed_result(&job),
                            db::nonnegative_i64_to_u64(bytes),
                            started_at,
                        );
                        pplx_space_metrics::jobs::attempts(&job.kind, "completed", job.attempts);
                    }
                    self.notify_completion(&job.job_id);
                }
                Ok(Err(JobError::YieldedForRestart)) => {
                    tracing::info!(
                        job_id = %job.job_id,
                        kind = %job.kind,
                        subject_id = ?job.subject_id,
                        "job execution yielded for restart"
                    );
                    let yielded = {
                        let conn = self.lock_db_for_background();
                        match lease_owner.as_deref() {
                            Some(owner) => db::yield_leased_job(&conn, &job.job_id, owner),
                            None => db::yield_running_job(&conn, &job.job_id),
                        }
                    };
                    log_conditional_update_result(yielded, &job, "yield for restart");
                    if !self.stopping.is_cancelled() {
                        self.spawn_job_for_mode(job.job_id, &job.kind);
                    }
                }
                Ok(Err(e)) => {
                    span.record("otel.status_code", "ERROR");
                    span.record("error", e.message());
                    let error_kind_tag = match e.kind() {
                        JobFailureKind::Permanent => {
                            tracing::error!(
                                job_id = %job.job_id,
                                kind = %job.kind,
                                subject_id = ?job.subject_id,
                                error = e.message(),
                                attempts = job.attempts,
                                "job failed permanently — will not retry"
                            );
                            "permanent"
                        }
                        JobFailureKind::Retriable => {
                            tracing::warn!(
                                job_id = %job.job_id,
                                kind = %job.kind,
                                subject_id = ?job.subject_id,
                                error = e.message(),
                                attempts = job.attempts,
                                max_attempts = job.max_attempts,
                                "job failed (retriable)"
                            );
                            if job.attempts >= job.max_attempts {
                                "exhausted"
                            } else {
                                "retriable"
                            }
                        }
                    };
                    pplx_space_metrics::jobs::failed(&job.kind, error_kind_tag, started_at);
                    if matches!(error_kind_tag, "permanent" | "exhausted") {
                        pplx_space_metrics::jobs::attempts(&job.kind, "failed", job.attempts);
                    }
                    self.handle_failure(&job, lease_owner.as_deref(), e).await;
                }
                Err(_timeout) => {
                    let msg = format!("job timed out after {}s", self.job_timeout.as_secs());
                    span.record("otel.status_code", "ERROR");
                    span.record("error", msg.as_str());
                    tracing::warn!(
                        job_id = %job.job_id,
                        kind = %job.kind,
                        subject_id = ?job.subject_id,
                        timeout_secs = self.job_timeout.as_secs(),
                        attempts = job.attempts,
                        max_attempts = job.max_attempts,
                        "job timed out (retriable)"
                    );
                    pplx_space_metrics::jobs::failed(&job.kind, "timeout", started_at);
                    if job.attempts >= job.max_attempts {
                        pplx_space_metrics::jobs::attempts(&job.kind, "failed", job.attempts);
                    }
                    self.handle_failure(&job, lease_owner.as_deref(), JobError::Retriable(msg))
                        .await;
                }
            }
            let cancellation_requested = {
                let conn = self.lock_db_for_background();
                db::get_job(&conn, &job.job_id)
                    .ok()
                    .flatten()
                    .is_some_and(|current| current.status == "cancelling")
            };
            if cancellation_requested {
                self.finish_cancelled_job(job).await;
            }
        }
        .instrument(job_span)
        .await;
    }

    /// Keep the existing execution slot, inflight count, and optional lease
    /// heartbeat until the success is durable. Retrying execution would repeat
    /// remote work; abandoning this write strands nonleased running rows.
    /// A persistent database outage has no completion deadline. Bounded drain
    /// must report non-quiescence rather than dropping this ownership.
    async fn complete_job(&self, job: &Job, lease_owner: Option<&str>, bytes: i64) -> bool {
        let mut retry_delay = Duration::from_secs(1);
        loop {
            let result = {
                let conn = self.lock_db_for_background();
                match lease_owner {
                    Some(owner) => db::complete_leased_job(&conn, &job.job_id, owner, bytes),
                    None => db::complete_job_attempt(&conn, &job.job_id, job.attempts, bytes),
                }
            };
            match result {
                Ok(true) => return true,
                Ok(false) => {
                    tracing::warn!(job_id = %job.job_id, kind = %job.kind,
                        "skipped completing job because its claim changed");
                    return false;
                }
                Err(error) => tracing::error!(job_id = %job.job_id, kind = %job.kind,
                    %error, retry_after_secs = retry_delay.as_secs(),
                    "failed to persist job completion; retaining ownership and retrying only the local write"),
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
        }
    }

    async fn finish_cancelled_job(&self, job: Job) {
        let cleanup_attempts = job.max_attempts.max(1);
        for cleanup_attempt in 1..=cleanup_attempts {
            let error = match self.executor.cleanup_cancelled(&job).await {
                Ok(()) => {
                    let result = {
                        let conn = self.lock_db_for_background();
                        db::mark_job_cancelled(&conn, &job.job_id)
                    };
                    match result {
                        Ok(true) => {
                            tracing::info!(
                                job_id = %job.job_id,
                                kind = %job.kind,
                                "job cancellation completed"
                            );
                            pplx_space_metrics::jobs::cancellation_completed(
                                &job.kind,
                                pplx_space_metrics::jobs::CancellationOutcome::Cancelled,
                            );
                            self.notify_completion(&job.job_id);
                        }
                        Ok(false) => {}
                        Err(error) => tracing::error!(
                            job_id = %job.job_id,
                            kind = %job.kind,
                            %error,
                            "failed to mark job cancelled"
                        ),
                    }
                    return;
                }
                Err(error) => error,
            };

            if error.kind() == JobFailureKind::Retriable && cleanup_attempt < cleanup_attempts {
                let delay = self.retry_backoff.delay_secs(cleanup_attempt);
                tracing::warn!(
                    job_id = %job.job_id,
                    kind = %job.kind,
                    %error,
                    cleanup_attempt,
                    cleanup_attempts,
                    delay,
                    "cancelled job cleanup failed; retrying"
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
                continue;
            }

            tracing::error!(
                job_id = %job.job_id,
                kind = %job.kind,
                %error,
                cleanup_attempt,
                cleanup_attempts,
                "cancelled job cleanup failed after final attempt"
            );
            let result = {
                let conn = self.lock_db_for_background();
                db::fail_cancelling_job(
                    &conn,
                    &job.job_id,
                    error.message(),
                    error.kind(),
                    error.failure_reason(),
                )
            };
            match result {
                Ok(true) => {
                    pplx_space_metrics::jobs::cancellation_completed(
                        &job.kind,
                        pplx_space_metrics::jobs::CancellationOutcome::CleanupFailed,
                    );
                    self.notify_completion(&job.job_id);
                }
                Ok(false) => {}
                Err(error) => tracing::error!(
                    job_id = %job.job_id,
                    kind = %job.kind,
                    %error,
                    "failed to persist cancelled job cleanup failure"
                ),
            }
            return;
        }
    }

    async fn handle_failure(&self, job: &Job, lease_owner: Option<&str>, err: JobError) {
        let failure_kind = err.kind();
        let failure_reason = err.failure_reason();
        let message = err.message().to_string();
        match failure_kind {
            JobFailureKind::Permanent => {
                let result = {
                    let conn = self.lock_db_for_background();
                    match lease_owner {
                        Some(owner) => db::fail_leased_job_permanent(
                            &conn,
                            &job.job_id,
                            owner,
                            &message,
                            failure_reason,
                        ),
                        None => db::fail_running_job_permanent(
                            &conn,
                            &job.job_id,
                            &message,
                            failure_reason,
                        ),
                    }
                };
                log_conditional_update_result(result, job, "fail permanently");
                self.notify_completion(&job.job_id);
            }
            JobFailureKind::Retriable => {
                if job.attempts >= job.max_attempts {
                    tracing::error!(
                        job_id = %job.job_id,
                        kind = %job.kind,
                        subject_id = ?job.subject_id,
                        attempts = job.attempts,
                        error = %message,
                        "job failed after exhausting all retry attempts"
                    );
                    let result = {
                        let conn = self.lock_db_for_background();
                        match lease_owner {
                            Some(owner) => db::fail_leased_job_retriable_exhausted(
                                &conn,
                                &job.job_id,
                                owner,
                                &message,
                            ),
                            None => db::fail_running_job_retriable_exhausted(
                                &conn,
                                &job.job_id,
                                &message,
                            ),
                        }
                    };
                    let exhausted = conditional_update_succeeded(result, job, "fail after retries");
                    if exhausted {
                        self.executor.retries_exhausted(job, &message).await;
                    }
                    self.notify_completion(&job.job_id);
                } else {
                    let delay = self.retry_backoff.delay_secs(job.attempts);
                    let result = {
                        let conn = self.lock_db_for_background();
                        match lease_owner {
                            Some(owner) => {
                                db::retry_leased_job(&conn, &job.job_id, owner, &message, delay)
                            }
                            None => db::retry_running_job(&conn, &job.job_id, &message, delay),
                        }
                    };
                    log_conditional_update_result(result, job, "schedule retry");
                    let worker = self.clone();
                    let job_id = job.job_id;
                    let kind = job.kind.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(delay)).await;
                        worker.spawn_job_for_mode(job_id, &kind);
                    });
                }
            }
        }
    }

    fn lock_db_for_status(&self) -> Result<MutexGuard<'_, rusqlite::Connection>, Status> {
        self.db
            .lock()
            .map_err(|_poisoned| Status::internal(DB_MUTEX_POISONED))
    }

    fn lock_db_for_background(&self) -> MutexGuard<'_, rusqlite::Connection> {
        self.db.lock().expect(DB_MUTEX_POISONED)
    }
}

async fn run_heartbeat(
    db: Arc<Mutex<rusqlite::Connection>>,
    job_id: JobId,
    lease: LeaseConfig,
    shutdown_token: CancellationToken,
) {
    let mut interval = tokio::time::interval(lease.heartbeat_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let result = {
                    let conn = db.lock().expect(DB_MUTEX_POISONED);
                    db::heartbeat_job_lease(
                        &conn,
                        &job_id,
                        &lease.worker_id,
                        lease.lease_ttl,
                    )
                };
                match result {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(job_id = %job_id, worker_id = %lease.worker_id, "job lease heartbeat lost ownership");
                        return;
                    }
                    Err(error) => {
                        tracing::error!(job_id = %job_id, worker_id = %lease.worker_id, %error, "job lease heartbeat failed");
                    }
                }
            }
            _ = shutdown_token.cancelled() => return,
        }
    }
}

fn log_conditional_update_result(
    result: rusqlite::Result<bool>,
    job: &Job,
    operation: &'static str,
) {
    match result {
        Ok(true) => {}
        Ok(false) => tracing::warn!(
            job_id = %job.job_id,
            kind = %job.kind,
            operation,
            "skipped job state update because lease ownership was lost"
        ),
        Err(error) => tracing::error!(
            job_id = %job.job_id,
            kind = %job.kind,
            operation,
            %error,
            "failed to update job state"
        ),
    }
}

fn conditional_update_succeeded(
    result: rusqlite::Result<bool>,
    job: &Job,
    operation: &'static str,
) -> bool {
    match result {
        Ok(true) => true,
        other => {
            log_conditional_update_result(other, job, operation);
            false
        }
    }
}

fn is_constraint_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(sqlite_error, _)
            if sqlite_error.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

#[cfg(test)]
mod submit_and_wait_tests;

#[cfg(test)]
mod wait_for_job_tests;

#[cfg(test)]
mod tests;
