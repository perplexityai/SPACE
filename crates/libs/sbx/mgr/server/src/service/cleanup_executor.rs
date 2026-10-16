use pplx_space_common_core::types::JobId;

use super::cleanup::{AsyncCleanupRequest, CleanupAdmission, SynchronousCleanupRequest};
use super::cleanup_support::{
    CleanupLockGuard, CleanupLockTryResult, CleanupPermit, CleanupWaitGuard,
    DEFAULT_CLEANUP_CONCURRENCY_CAP,
};
use super::*;
use crate::db::cleanup_jobs::{
    self, CleanupEligibility, CleanupFailure, CleanupOutcome, CleanupWork,
};
use pplx_space_common_jobs::{JobPriority, JobStatus};

type AttemptResult = Result<(), Status>;
type CleanupResult = Result<CleanupOutcome, Status>;
type CompletionWaiters = BTreeMap<JobId, (tokio::sync::watch::Sender<()>, usize)>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CleanupWait {
    Attempt,
    Stop,
}

fn caller_result(outcome: CleanupOutcome) -> AttemptResult {
    match outcome {
        CleanupOutcome::Applied | CleanupOutcome::AlreadyApplied => Ok(()),
        CleanupOutcome::Superseded => Err(Status::aborted("cleanup superseded")),
        CleanupOutcome::Quarantined(message) | CleanupOutcome::Blocked(message) => {
            Err(Status::failed_precondition(message))
        }
    }
}

pub(super) struct CleanupExecutor {
    started: AtomicBool,
    #[cfg(test)]
    pub(super) dispatch: Mutex<Option<TestDispatch>>,
    #[cfg(test)]
    pub(super) admission_barrier: Mutex<Option<Arc<TestAdmissionBarrier>>>,
    #[cfg(test)]
    waiter_read_barrier: Mutex<Option<Arc<TestAdmissionBarrier>>>,
    #[cfg(test)]
    pub(super) cache_dir: Mutex<Option<PathBuf>>,
    wake: tokio::sync::Notify,
    pub(super) active: dashmap::DashMap<String, bool>,
    waiters: Mutex<CompletionWaiters>,
    subscriptions: Arc<tokio::sync::Semaphore>,
    admission: Arc<tokio::sync::Semaphore>,
}

impl CleanupExecutor {
    pub(super) fn notify(&self) {
        self.wake.notify_one();
    }
}

impl Default for CleanupExecutor {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            #[cfg(test)]
            dispatch: Mutex::new(None),
            #[cfg(test)]
            admission_barrier: Mutex::new(None),
            #[cfg(test)]
            waiter_read_barrier: Mutex::new(None),
            #[cfg(test)]
            cache_dir: Mutex::new(None),
            wake: tokio::sync::Notify::new(),
            active: dashmap::DashMap::new(),
            waiters: Mutex::new(BTreeMap::new()),
            subscriptions: Arc::new(tokio::sync::Semaphore::new(128)),
            admission: Arc::new(tokio::sync::Semaphore::new(128)),
        }
    }
}

struct ActiveAttempt {
    executor: Arc<CleanupExecutor>,
    subject: String,
    _permit: CleanupPermit,
}
impl Drop for ActiveAttempt {
    fn drop(&mut self) {
        self.executor.active.remove(&self.subject);
        self.executor.wake.notify_one();
    }
}

struct CompletionSubscription {
    executor: Arc<CleanupExecutor>,
    id: JobId,
}
impl Drop for CompletionSubscription {
    fn drop(&mut self) {
        #[allow(
            clippy::expect_used,
            reason = "Private waiter map is modified only by bounded subscription accounting and watch notifications; no caller callback runs under lock"
        )]
        let mut waiters = self
            .executor
            .waiters
            .lock()
            .expect("cleanup waiters poisoned");
        if let Some((_, count)) = waiters.get_mut(&self.id) {
            *count -= 1;
            if *count == 0 {
                waiters.remove(&self.id);
            }
        }
    }
}

impl SandboxManagerService {
    pub(super) fn persist_cleanup_request(
        &self,
        request: AsyncCleanupRequest,
        admission: Option<CleanupAdmission>,
    ) -> Result<Option<JobId>, Status> {
        let id = request.sandbox_id.to_string();
        tracing::debug!(sandbox_id = %id, context = request.context, "scheduling durable cleanup");
        let acquired;
        let expected_generation = match &admission {
            Some(admission) => Some(admission.expected_execution_generation(request.sandbox_id)?),
            None => {
                acquired = self.try_cleanup_fence(&id);
                if matches!(acquired, CleanupLockTryResult::Busy) {
                    return Ok(None);
                }
                None
            }
        };
        let db = self
            .db
            .lock()
            .map_err(|_| Status::internal("cleanup database poisoned"))?;
        let tx = crate::db::Transaction::deferred(&db).map_err(storage_status)?;
        if request.persist_intent
            && !crate::db::set_cleanup_intent(&tx, &id, request.keep_filesystem, request.completion)
                .map_err(storage_status)?
        {
            return Ok(None);
        }
        let generation =
            crate::db::runtime_execution_generation(&tx, &id).map_err(storage_status)?;
        if expected_generation.is_some_and(|expected| expected != generation) {
            return Ok(None);
        }
        let work = CleanupWork {
            version: 1,
            sandbox_id: request.sandbox_id,
            uuid8: request.uuid8,
            execution_generation: generation,
            cleanup_id: request.cleanup_id,
            completion: request.completion,
            keep_filesystem: request.keep_filesystem,
            immediate_kill: request.immediate_kill,
            guest_flush: request.guest_flush,
            published_artifact: None,
        };
        if !cleanup_jobs::current(&tx, &work).map_err(storage_status)? {
            return Ok(None);
        }
        let job = cleanup_jobs::persist(&tx, work)
            .map_err(|error| Status::internal(error.to_string()))?;
        tx.commit().map_err(storage_status)?;
        Ok(Some(job))
    }

    pub(super) async fn run_cleanup_to_completion_using_admission(
        &self,
        request: SynchronousCleanupRequest<'_>,
        admission: Option<CleanupAdmission>,
        cleanup_id: Option<SandboxCleanupEpisodeId>,
        wait: CleanupWait,
    ) -> AttemptResult {
        let sandbox_id: SandboxId = request
            .sandbox_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid cleanup id: {error}")))?;
        let fence = if admission.is_none() {
            Some(
                CleanupLockGuard::new(
                    Arc::clone(&self.cleanup_locks),
                    request.sandbox_id.to_owned(),
                    self.cleanup_lock(request.sandbox_id),
                )
                .lock_owned()
                .await,
            )
        } else {
            None
        };
        let uuid8 = request
            .uuid8
            .parse()
            .map_err(|error| Status::invalid_argument(format!("invalid runtime label: {error}")))?;
        let expected = admission
            .as_ref()
            .map(|admission| admission.expected_execution_generation(sandbox_id))
            .transpose()?;
        let mut work = CleanupWork {
            version: 1,
            sandbox_id,
            uuid8,
            execution_generation: None,
            cleanup_id,
            completion: request.completion,
            keep_filesystem: request.keep_filesystem,
            immediate_kill: request.immediate_kill,
            guest_flush: request.guest_flush,
            published_artifact: request.published_artifact,
        };
        let job = self
            .storage(move |db| {
                let _admission = admission;
                let _fence = fence;
                let tx = crate::db::Transaction::deferred(db).map_err(storage_status)?;
                let generation =
                    crate::db::runtime_execution_generation(&tx, &sandbox_id.to_string())
                        .map_err(storage_status)?;
                if expected.is_some_and(|expected| expected != generation) {
                    return Err(Status::aborted("cleanup execution superseded"));
                }
                work.execution_generation = generation;
                if let CleanupEligibility::Finished(outcome) =
                    cleanup_jobs::eligibility(&tx, &work).map_err(storage_status)?
                {
                    caller_result(outcome)?;
                    return Ok(None);
                }
                let job = cleanup_jobs::persist(&tx, work)
                    .map_err(|error| Status::internal(error.to_string()))?;
                tx.commit().map_err(storage_status)?;
                Ok(Some(job))
            })
            .await?;
        let Some(job) = job else {
            return Ok(());
        };
        self.ensure_cleanup_executor();
        match wait {
            CleanupWait::Attempt => self.wait_cleanup_job(job).await,
            CleanupWait::Stop => {
                self.wait_stop_cleanup_job(job, Duration::from_secs(30))
                    .await
            }
        }
    }

    pub(super) async fn cleanup_request_permit(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        if self.shutdown_token.is_cancelled() {
            return Err(Status::unavailable("cleanup admission closed"));
        }
        let _waiting = CleanupWaitGuard::new(Arc::clone(&self.cleanup_metrics));
        tokio::select! {
            biased;
            _ = self.shutdown_token.cancelled() => Err(Status::unavailable("cleanup admission closed")),
            permit = Arc::clone(&self.cleanup_executor.admission).acquire_owned() =>
                permit.map_err(|_| Status::unavailable("cleanup admission closed")),
        }
    }

    pub(super) fn ensure_cleanup_executor(&self) {
        self.cleanup_executor.wake.notify_one();
        if self.shutdown_token.is_cancelled()
            || self.cleanup_executor.started.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let service = self.clone();
        let guard = self.critical_task_guard("cleanup executor", &self.shutdown_token);
        self.task_tracker.spawn(async move {
            let _guard = guard;
            service.run_cleanup_executor().await;
        });
    }

    async fn run_cleanup_executor(self) {
        loop {
            match self
                .storage(|db| {
                    cleanup_jobs::reset_running(db)
                        .map_err(|error| Status::internal(error.to_string()))
                })
                .await
            {
                Ok(_) => break,
                Err(error) => tracing::warn!(%error, "cannot recover cleanup scheduling"),
            }
            tokio::select! {
                _ = self.shutdown_token.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
        let capacity = DEFAULT_CLEANUP_CONCURRENCY_CAP;
        let mut foreground_first = true;
        let mut recovery_cursor = None;
        let mut tasks = tokio::task::JoinSet::new();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_token.cancelled() => break,
                result = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(error)) = result { tracing::error!(%error, "cleanup attempt task failed"); }
                }
                _ = self.cleanup_executor.wake.notified() => {}
                _ = tick.tick() => {}
            }
            if self.is_draining_for_restart() {
                continue;
            }
            let after = recovery_cursor;
            match self
                .storage(move |db| {
                    crate::db::cleanup_recovery_page(db, after, 128).map_err(storage_status)
                })
                .await
            {
                Ok(page) => {
                    recovery_cursor = page.next_cursor;
                    let ids = page.ids;
                    if !ids.is_empty() {
                        let permit = Arc::clone(&self.cleanup_io).acquire_owned().await;
                        if let Ok(permit) = permit {
                            let service = self.clone();
                            if let Err(error) = tokio::task::spawn_blocking(move || {
                                let _permit = permit;
                                let ids = ids.into_iter().filter_map(|id| match id.parse() {
                                    Ok(id) => Some(id),
                                    Err(error) => { tracing::warn!(%id, %error, "invalid cleanup recovery identity"); None }
                                }).collect();
                                let targets = service.recover_requested_cleanup_targets_for_ids(ids);
                                service.schedule_cleanup_recovery_targets(targets);
                            }).await { tracing::warn!(%error, "cleanup discovery failed"); }
                        }
                    }
                }
                Err(error) => tracing::warn!(%error, "cleanup recovery discovery failed"),
            }
            let active: Vec<_> = self
                .cleanup_executor
                .active
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect();
            let available = self.cleanup_sem.available_permits();
            if available == 0 {
                continue;
            }
            let foreground = active.iter().filter(|(_, kill)| *kill).count();
            let background = active.len() - foreground;
            let classes = if foreground_first {
                [(1, foreground), (0, background)]
            } else {
                [(0, background), (1, foreground)]
            };
            foreground_first = !foreground_first;
            let jobs = self
                .storage(move |db| {
                    let tx = crate::db::Transaction::deferred(db).map_err(storage_status)?;
                    let mut subjects: Vec<_> = active.into_iter().map(|(id, _)| id).collect();
                    cleanup_jobs::reset_inactive_running(&tx, &subjects).map_err(storage_status)?;
                    let mut selected = Vec::new();
                    // Neither class can occupy the other's final slot.
                    for (priority, count) in classes {
                        let budget = (capacity - 1)
                            .max(1)
                            .saturating_sub(count)
                            .min(available - selected.len());
                        for id in cleanup_jobs::due_job_ids(
                            &tx,
                            JobPriority::new(priority),
                            &subjects,
                            budget,
                        )
                        .map_err(storage_status)?
                        {
                            let Some(job) =
                                cleanup_jobs::get_job(&tx, &id).map_err(storage_status)?
                            else {
                                continue;
                            };
                            let Some(subject) = job.subject_id.clone() else {
                                continue;
                            };
                            if subjects.contains(&subject) {
                                continue;
                            }
                            if cleanup_jobs::mark_running(&tx, &id).map_err(storage_status)? {
                                subjects.push(subject.clone());
                                selected.push((id, subject, priority == 1));
                            }
                        }
                    }
                    tx.commit().map_err(storage_status)?;
                    Ok(selected)
                })
                .await;
            match jobs {
                Ok(selected) => {
                    for (id, subject, kill) in selected {
                        let Ok(permit) = Arc::clone(&self.cleanup_sem).try_acquire_owned() else {
                            continue;
                        };
                        self.cleanup_executor.active.insert(subject.clone(), kill);
                        let guard = ActiveAttempt {
                            executor: Arc::clone(&self.cleanup_executor),
                            subject: subject.clone(),
                            _permit: CleanupPermit::new(permit, Arc::clone(&self.cleanup_metrics)),
                        };
                        let service = self.clone();
                        tasks.spawn(async move {
                            let _guard = guard;
                            if let Err(error) = service
                                .run_durable_cleanup_attempt(&id, &subject, kill)
                                .await
                            {
                                tracing::warn!(job_id = %id, %error, "cleanup attempt could not finish; durable recovery owns retry");
                            }
                            #[allow(clippy::expect_used, reason = "Private waiter map is modified only by bounded subscription accounting and watch notifications; no caller callback runs under lock")]
                            let waiters = service
                                .cleanup_executor
                                .waiters
                                .lock()
                                .expect("cleanup waiters poisoned");
                            if let Some((sender, _)) = waiters.get(&id) {
                                sender.send_replace(());
                            }
                        });
                    }
                }
                Err(error) => tracing::warn!(%error, "cannot dispatch cleanup batch"),
            }
        }
        self.cleanup_executor.admission.close();
        self.cleanup_executor.subscriptions.close();
        // Dropping a caller or shutdown signal must not abandon a host mutation.
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                tracing::error!(%error, "cleanup attempt failed during drain");
            }
        }
    }

    async fn run_durable_cleanup_attempt(
        &self,
        id: &JobId,
        subject: &str,
        reserved_kill: bool,
    ) -> AttemptResult {
        let _writer = match self.state_writer_lease("AsyncCleanup") {
            Ok(writer) => writer,
            Err(error) => {
                // A blocking caller still holds its outer writer. Persist this
                // failure so it returns and releases the dependency work gate;
                // the durable job retries after admission reopens.
                self.record_cleanup_attempt(id, &Err(error.clone())).await?;
                return Err(error);
            }
        };
        let fence = CleanupLockGuard::new(
            Arc::clone(&self.cleanup_locks),
            subject.to_owned(),
            self.cleanup_lock(subject),
        )
        .lock_owned()
        .await;
        let job_id = id.to_owned();
        let selected = self
            .storage(move |db| {
                let job = cleanup_jobs::get_job(db, &job_id)
                    .map_err(storage_status)?
                    .ok_or_else(|| Status::internal("cleanup job disappeared"))?;
                if job.status != JobStatus::Running.as_str() {
                    return Ok(None);
                }
                let work = cleanup_jobs::decode(&job.payload)
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                if let CleanupEligibility::Finished(outcome) =
                    cleanup_jobs::eligibility(db, &work).map_err(storage_status)?
                {
                    let tx = crate::db::Transaction::deferred(db).map_err(storage_status)?;
                    cleanup_jobs::finish(&tx, &job_id, &outcome).map_err(storage_status)?;
                    tx.commit().map_err(storage_status)?;
                    return Ok(None);
                }
                let receipt =
                    crate::db::auto_lifecycle_delete_receipts::pending_receipt_for_sandbox(
                        db,
                        work.sandbox_id,
                    )
                    .map_err(storage_status)?;
                Ok(Some((work, receipt)))
            })
            .await;
        let result = match selected {
            Ok(Some((work, receipt))) => {
                if work.immediate_kill != reserved_kill {
                    // A caller can promote a selected job while its attempt waits
                    // for this fence. Release the old class reservation before
                    // competing for capacity in the stronger class. This is not
                    // a failed attempt or a completion notification to callers.
                    let job_id = id.to_owned();
                    self.storage(move |db| {
                        cleanup_jobs::yield_running(db, &job_id).map_err(storage_status)?;
                        Ok(())
                    })
                    .await?;
                    return Ok(());
                }
                let mut admission = CleanupAdmission::persisted(
                    work.sandbox_id,
                    work.execution_generation,
                    receipt,
                    fence,
                );
                if let CleanupAdmission::Persisted {
                    dispatch_job_id, ..
                } = &mut admission
                {
                    *dispatch_job_id = Some(id.to_owned());
                }
                let request = SynchronousCleanupRequest {
                    sandbox_id: &work.sandbox_id.to_string(),
                    uuid8: work.uuid8.as_str(),
                    keep_filesystem: work.keep_filesystem,
                    completion: work.completion,
                    immediate_kill: work.immediate_kill,
                    guest_flush: work.guest_flush,
                    published_artifact: work.published_artifact,
                    context: "durable sandbox cleanup",
                };
                return self
                    .execute_cleanup_attempt(request, Some(admission), work.cleanup_id)
                    .await
                    .map(|_| ());
            }
            Ok(None) => return Ok(()),
            Err(error) => Err(error),
        };
        self.record_cleanup_attempt(id, &result).await?;
        result.map(|_| ())
    }

    pub(super) async fn record_cleanup_attempt(
        &self,
        id: &JobId,
        result: &CleanupResult,
    ) -> AttemptResult {
        let id = id.to_owned();
        let result = result.clone();
        self.storage(move |db| {
            match &result {
                Ok(outcome) => {
                    // Supersession has no lifecycle write. Every other terminal
                    // outcome must already share the lifecycle transaction;
                    // completing it here would reopen the crash ambiguity.
                    if matches!(outcome, CleanupOutcome::Superseded) {
                        let tx = crate::db::Transaction::deferred(db).map_err(storage_status)?;
                        cleanup_jobs::finish(&tx, &id, outcome).map_err(storage_status)?;
                        tx.commit().map_err(storage_status)?;
                    } else {
                        let stored = cleanup_jobs::get_job(db, &id)
                            .map_err(storage_status)?
                            .ok_or_else(|| Status::internal("cleanup job disappeared"))?;
                        if stored.status != outcome.job_status().as_str() {
                            return Err(Status::internal(
                                "cleanup outcome was not committed with lifecycle finalization",
                            ));
                        }
                    }
                }
                Err(error) => {
                    let attempts = cleanup_jobs::get_job(db, &id)
                        .map_err(storage_status)?
                        .map_or(1, |job| job.attempts);
                    let delay = CLEANUP_RETRY_INITIAL_DELAY_SECS
                        .saturating_mul(1_u64 << (attempts - 1).clamp(0, 5))
                        .min(CLEANUP_RETRY_MAX_DELAY_SECS);
                    // Stable per-obligation jitter avoids synchronized retries after
                    // a node-wide outage, without an additional persisted clock.
                    use std::hash::{Hash, Hasher};
                    let mut jitter = std::collections::hash_map::DefaultHasher::new();
                    id.hash(&mut jitter);
                    attempts.hash(&mut jitter);
                    let delay = delay - jitter.finish() % (delay / 2 + 1);
                    let failure = serde_json::to_string(&CleanupFailure {
                        code: error.code() as i32,
                        message: error.message().to_owned(),
                    })
                    .map_err(|error| Status::internal(format!("encode cleanup result: {error}")))?;
                    cleanup_jobs::retry_running(db, &id, &failure, delay)
                        .map_err(storage_status)?;
                    pplx_space_metrics::sandbox::worker::cleanup_retry();
                }
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn wait_cleanup_job(&self, id: JobId) -> AttemptResult {
        self.wait_cleanup_job_with_policy(id, CleanupWait::Attempt)
            .await
    }

    async fn wait_stop_cleanup_job(&self, id: JobId, budget: Duration) -> AttemptResult {
        // Timeout drops only the subscriber. The persisted, execution-fenced
        // job keeps its retry schedule and ownership until cleanup completes.
        tokio::time::timeout(
            budget,
            self.wait_cleanup_job_with_policy(id, CleanupWait::Stop),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!("Stop is not confirmed; cleanup job {id} continues"))
        })?
    }

    async fn wait_cleanup_job_with_policy(&self, id: JobId, wait: CleanupWait) -> AttemptResult {
        let _permit = tokio::select! {
            biased;
            _ = self.shutdown_token.cancelled() => return Err(Status::unavailable("cleanup waiter admission closed")),
            permit = Arc::clone(&self.cleanup_executor.subscriptions).acquire_owned() =>
                permit.map_err(|_| Status::unavailable("cleanup waiter admission closed"))?,
        };
        let mut receiver = {
            #[allow(
                clippy::expect_used,
                reason = "Private waiter map is modified only by bounded subscription accounting and watch notifications; no caller callback runs under lock"
            )]
            let mut waiters = self
                .cleanup_executor
                .waiters
                .lock()
                .expect("cleanup waiters poisoned");
            let (sender, count) = waiters
                .entry(id)
                .or_insert_with(|| (tokio::sync::watch::channel(()).0, 0));
            *count += 1;
            sender.subscribe()
        };
        let _subscription = CompletionSubscription {
            executor: Arc::clone(&self.cleanup_executor),
            id,
        };
        // Notifications carry no outcome. SQLite is the sole authority for
        // completion, including cancellation before dispatch and restart.
        // Polling also covers transitions made without a live subscriber.
        let mut poll = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let read_id = id;
            let stored = self
                .storage(move |db| cleanup_jobs::get_job(db, &read_id).map_err(storage_status))
                .await?
                .ok_or_else(|| Status::internal("cleanup job disappeared"))?;
            #[cfg(test)]
            {
                let barrier = self
                    .cleanup_executor
                    .waiter_read_barrier
                    .lock()
                    .unwrap()
                    .take();
                if let Some(barrier) = barrier {
                    barrier.entered.notify_one();
                    barrier.release.acquire().await.unwrap().forget();
                }
            }
            let status: JobStatus = stored.status.parse().map_err(|error| {
                Status::failed_precondition(format!("invalid cleanup job state: {error}"))
            })?;
            match status {
                JobStatus::Completed => return Ok(()),
                JobStatus::Cancelled | JobStatus::Cancelling => {
                    return Err(Status::aborted("cleanup superseded"));
                }
                JobStatus::Pending | JobStatus::Running | JobStatus::Failed => {
                    if let Some(error) = stored.error {
                        let failure = match serde_json::from_str::<CleanupFailure>(&error) {
                            Ok(failure) => failure,
                            // Preserve legacy plain-string failures, including their
                            // message. Unknown provenance cannot justify retrying.
                            Err(_) => return Err(Status::unavailable(error)),
                        };
                        let error =
                            Status::new(tonic::Code::from_i32(failure.code), failure.message);
                        let retryable = matches!(
                            error.code(),
                            tonic::Code::Unavailable
                                | tonic::Code::ResourceExhausted
                                | tonic::Code::DeadlineExceeded
                        );
                        if wait == CleanupWait::Attempt || status == JobStatus::Failed || !retryable
                        {
                            return Err(error);
                        }
                    }
                    if status == JobStatus::Failed {
                        return Err(Status::failed_precondition(
                            "cleanup failed without recorded details; operator repair required",
                        ));
                    }
                }
            }
            tokio::select! {
                _ = self.shutdown_token.cancelled() => return Err(Status::unavailable("cleanup continues after restart")),
                result = receiver.changed() => result.map_err(|_| Status::unavailable("cleanup waiter closed"))?,
                _ = poll.tick() => {},
            }
        }
    }
}

fn storage_status(error: crate::db::Error) -> Status {
    Status::internal(format!("cleanup storage: {error}"))
}

#[cfg(test)]
type TestDispatch = Arc<
    dyn Fn(String, bool) -> std::pin::Pin<Box<dyn Future<Output = AttemptResult> + Send>>
        + Send
        + Sync,
>;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) struct TestAdmissionBarrier {
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Semaphore,
}
