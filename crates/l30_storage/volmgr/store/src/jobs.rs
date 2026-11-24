//! volume-manager's job queue in this store's SQLite database.
//!
//! [`JobWorker`] is volume-manager's only path to the common-jobs worker that
//! runs its queue:
//! [`SharedConnection::background_worker`](crate::SharedConnection::background_worker)
//! builds one, and each of its methods forwards unchanged to the worker
//! method of the same name, which runs the queue's SQL. As this crate's
//! `AGENTS.md` says, a worker operation volume-manager does not call yet is a
//! new forwarding method here, and `BackgroundWorker` is never re-exported. A
//! read of job rows that must run under the caller's held connection goes in
//! the crate root's job censuses instead, as
//! [`count_active_jobs`](crate::count_active_jobs) does.
//!
//! The re-exported items are data and traits that run no SQL:
//!
//! - [`Job`] is a persisted job, which the worker's lookups and waits return.
//!   `Job::status` is a string, which callers parse into a [`JobStatus`];
//!   `Job::priority` is a [`JobPriority`], and `Job::failure` is a
//!   [`JobFailure`] whose permanent reason is a [`JobFailureReason`].
//! - [`JobExecutor`] is the trait volume-manager implements for the executor
//!   it hands to `background_worker`. Executors and [`Job::payload_as`] fail
//!   with [`JobError`], whose kind is a [`JobFailureKind`]. The worker's
//!   fallible methods fail with `tonic::Status`.
//! - [`WorkerConfig`] configures the worker, and [`PriorityBand`] is one of
//!   the priority bands it admits a kind's jobs in.
//! - [`DuplicatePolicy`] says what a submission with a `dedup_key` does when
//!   a job with that key exists: reuse an active one, also reuse a completed
//!   one (`ReuseExistingOrCompleted`), or fail while an active one exists
//!   (`Reject`).

use std::time::Duration;

use pplx_space_common_jobs::BackgroundWorker;
pub use pplx_space_common_jobs::{
    DuplicatePolicy, Job, JobError, JobExecutor, JobFailure, JobFailureKind, JobFailureReason,
    JobPriority, JobStatus, PriorityBand, WorkerConfig,
};
use pplx_space_job_id::JobId;
use tokio_util::sync::CancellationToken;
use tonic::Status;

/// A job worker over the store's connection. Each method is the common-jobs
/// worker method of the same name. The async ones return the worker's future
/// directly rather than awaiting it in another. `Clone` is cheap and shares
/// the worker's state.
#[derive(Clone)]
pub struct JobWorker<E>(BackgroundWorker<E>);

impl<E: JobExecutor> JobWorker<E> {
    pub(crate) fn new(worker: BackgroundWorker<E>) -> Self {
        Self(worker)
    }

    /// Budget for executor attempts and their retry backoffs.
    pub fn worst_case_runtime(&self) -> Duration {
        self.0.worst_case_runtime()
    }

    /// Re-queue jobs a previous process left interrupted, logging under
    /// `worker_label` when any were.
    pub fn recover_and_log(&self, worker_label: &str) -> Vec<JobId> {
        self.0.recover_and_log(worker_label)
    }

    /// Delete terminal jobs past their retention now and then periodically
    /// until `shutdown_token` fires.
    pub fn run_terminal_job_pruner(
        self,
        worker_label: &'static str,
        shutdown_token: CancellationToken,
    ) -> impl Future<Output = ()> {
        self.0.run_terminal_job_pruner(worker_label, shutdown_token)
    }

    /// Stop starting jobs, including jobs submitted from now on, and let
    /// running executors finish. Jobs that have not started stay pending for
    /// the next owner of the queue to recover.
    pub fn stop_starting_jobs(&self) {
        self.0.stop_starting_jobs()
    }

    /// Wait until no executor task remains, returning false at the deadline.
    pub fn wait_for_executors(&self, timeout: Duration) -> impl Future<Output = bool> {
        self.0.wait_for_executors(timeout)
    }

    /// Emit the queue's metrics until `shutdown_token` fires.
    pub fn run_queue_metrics(self, shutdown_token: CancellationToken) -> impl Future<Output = ()> {
        self.0.run_queue_metrics(shutdown_token)
    }

    /// Submit a job with a raw `payload`. Returns its id, or the existing
    /// job's when `dedup_key` deduplicates the submission.
    pub fn submit(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
    ) -> Result<JobId, Status> {
        self.0.submit(kind, payload, subject_id, dedup_key)
    }

    /// [`Self::submit`] at `priority`.
    pub fn submit_with_priority(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
        priority: JobPriority,
    ) -> Result<JobId, Status> {
        self.0
            .submit_with_priority(kind, payload, subject_id, dedup_key, priority)
    }

    /// [`Self::submit`], reusing an existing job as `duplicate_policy` says.
    pub fn submit_with_duplicate_policy(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
        duplicate_policy: DuplicatePolicy,
    ) -> Result<JobId, Status> {
        self.0
            .submit_with_duplicate_policy(kind, payload, subject_id, dedup_key, duplicate_policy)
    }

    /// Run `submission` and wait until the job it submitted is terminal or
    /// `timeout` elapses.
    pub fn submit_and_wait<F>(
        &self,
        submission: F,
        timeout: Duration,
    ) -> impl Future<Output = Result<Job, Status>>
    where
        F: Future<Output = Result<JobId, Status>>,
    {
        self.0.submit_and_wait(submission, timeout)
    }

    /// Wait until the job with `job_id` is terminal or `timeout` elapses.
    pub fn wait_for_job(
        &self,
        job_id: &JobId,
        timeout: Duration,
    ) -> impl Future<Output = Result<Job, Status>> {
        self.0.wait_for_job(job_id, timeout)
    }

    /// Durably request cancellation of the job with `job_id` and wait until
    /// its executor has stopped writing and completed its cancellation
    /// cleanup, or `timeout` elapses.
    pub fn cancel_job_and_wait(
        &self,
        job_id: &JobId,
        timeout: Duration,
    ) -> impl Future<Output = Result<Job, Status>> {
        self.0.cancel_job_and_wait(job_id, timeout)
    }

    /// The job with `job_id`.
    pub fn get_job(&self, job_id: &JobId) -> Result<Option<Job>, Status> {
        self.0.get_job(job_id)
    }

    /// The newest job of `kind` with `dedup_key`, whatever its status.
    pub fn find_job_by_dedup_key(
        &self,
        kind: &str,
        dedup_key: &str,
    ) -> Result<Option<Job>, Status> {
        self.0.find_job_by_dedup_key(kind, dedup_key)
    }

    /// Up to `limit` jobs, optionally filtered by subject and status.
    pub fn list_jobs(
        &self,
        subject_id: Option<&str>,
        status: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Job>, Status> {
        self.0.list_jobs(subject_id, status, limit)
    }

    /// Number of currently executing tasks.
    pub fn inflight(&self) -> usize {
        self.0.inflight()
    }
}
