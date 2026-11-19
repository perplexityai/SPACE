//! sbx-mgr's job queue in this store's SQLite database.
//!
//! [`JobWorker`] is sbx-mgr's only path to the common-jobs worker that runs
//! its queue:
//! [`SharedConnection::background_worker`](super::SharedConnection::background_worker)
//! builds one, and each of its methods forwards unchanged to the worker
//! method of the same name, which runs the queue's SQL. As this crate's
//! `AGENTS.md` says, a worker operation sbx-mgr does not call yet is a new
//! forwarding method here, and `BackgroundWorker` is never re-exported. A read
//! of job rows that must run under the caller's held connection or
//! transaction goes in [`read`](super::read) instead, as
//! [`read::cleanup_job`](super::read::cleanup_job) and
//! [`read::local_artifact_upload`](super::read::local_artifact_upload) do.
//! The store's job writes are its own functions, such as those in
//! [`cleanup_jobs`](super::cleanup_jobs), which also run in the caller's
//! connection or transaction. The `test-support` feature adds `submit` and
//! `wait_for_job` for sbx-mgr's tests.
//!
//! The re-exported items are data and traits that run no SQL:
//!
//! - [`Job`] is a persisted job. The worker's lookups and the store's job
//!   reads ([`read::cleanup_job`](super::read::cleanup_job),
//!   [`read::local_artifact_upload`](super::read::local_artifact_upload))
//!   return it. `Job::status` is a string, which callers parse into a
//!   [`JobStatus`] or compare with [`JobStatus::as_str`]; the only store API
//!   that returns a `JobStatus` is
//!   [`CleanupOutcome::job_status`](super::cleanup_jobs::CleanupOutcome::job_status).
//!   `Job::priority` is a [`JobPriority`], which is also an argument of
//!   [`cleanup_jobs::due_job_ids`](super::cleanup_jobs::due_job_ids), and
//!   `Job::failure` is a [`JobFailure`].
//! - [`JobExecutor`] is the trait sbx-mgr implements for the executors it
//!   hands to `background_worker`. Executors and [`Job::payload_as`] fail
//!   with [`JobError`]. The worker's fallible methods fail with
//!   `tonic::Status`, except [`JobWorker::try_prune_terminal_jobs_once`],
//!   which fails with [`PruneTerminalJobsError`]. A [`JobFailureKind`] is the
//!   kind of a `JobError` or a `JobFailure`.
//! - [`WorkerConfig`] configures the worker, and
//!   [`TERMINAL_JOB_PRUNE_INTERVAL`] paces calls to
//!   `try_prune_terminal_jobs_once`.

use std::time::Duration;

use pplx_space_accounts::JobId;
use pplx_space_common_jobs::BackgroundWorker;
pub use pplx_space_common_jobs::{
    Job, JobError, JobExecutor, JobFailure, JobFailureKind, JobPriority, JobStatus,
    PruneTerminalJobsError, TERMINAL_JOB_PRUNE_INTERVAL, WorkerConfig,
};
use tokio_util::sync::CancellationToken;
use tonic::Status;

/// A job worker over the store's connection. Each method is the common-jobs
/// worker method of the same name. The async ones return the worker's future
/// directly rather than awaiting it in another: at opt-level 0 each extra layer
/// grows the futures sbx-mgr's tests hold on their 2 MiB stacks. `Clone` is
/// cheap and shares the worker's state.
#[derive(Clone)]
pub struct JobWorker<E>(BackgroundWorker<E>);

impl<E: JobExecutor> JobWorker<E> {
    pub(in crate::db) fn new(worker: BackgroundWorker<E>) -> Self {
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

    /// Delete terminal jobs past their retention.
    pub fn try_prune_terminal_jobs_once(
        &self,
        worker_label: &str,
    ) -> Result<usize, PruneTerminalJobsError> {
        self.0.try_prune_terminal_jobs_once(worker_label)
    }

    /// Stop launching work and wait for in-flight executors to yield,
    /// returning false at the deadline.
    pub fn yield_for_restart(&self, timeout: Duration) -> impl Future<Output = bool> {
        self.0.yield_for_restart(timeout)
    }

    /// Emit the queue's metrics until `shutdown_token` fires.
    pub fn run_queue_metrics(self, shutdown_token: CancellationToken) -> impl Future<Output = ()> {
        self.0.run_queue_metrics(shutdown_token)
    }

    /// Submit a job with a JSON-encoded `payload`. Returns its id, or the
    /// existing job's when `dedup_key` deduplicates the submission.
    pub fn submit_json<T: serde::Serialize>(
        &self,
        kind: &str,
        payload: &T,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
    ) -> Result<JobId, Status> {
        self.0.submit_json(kind, payload, subject_id, dedup_key)
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

    /// Number of currently executing tasks.
    pub fn inflight(&self) -> usize {
        self.0.inflight()
    }

    /// Submit a job with a raw `payload`, for tests that skip JSON encoding.
    #[cfg(any(test, feature = "test-support"))]
    pub fn submit(
        &self,
        kind: &str,
        payload: &str,
        subject_id: Option<&str>,
        dedup_key: Option<&str>,
    ) -> Result<JobId, Status> {
        self.0.submit(kind, payload, subject_id, dedup_key)
    }

    /// Wait until the job with `job_id` is terminal or `timeout` elapses.
    #[cfg(any(test, feature = "test-support"))]
    pub fn wait_for_job(
        &self,
        job_id: &JobId,
        timeout: Duration,
    ) -> impl Future<Output = Result<Job, Status>> {
        self.0.wait_for_job(job_id, timeout)
    }
}
