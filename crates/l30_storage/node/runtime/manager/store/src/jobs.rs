//! runtime-manager's job queue, the shared `jobs` table in this store's
//! database.
//!
//! [`JobWorker`] is runtime-manager's only path to the common-jobs worker
//! that runs its queue:
//! [`SharedConnection::background_worker`](crate::database::SharedConnection::background_worker)
//! builds one, and each of its methods forwards unchanged to the worker
//! method of the same name, which runs the queue's SQL. As this crate's
//! `AGENTS.md` says, a worker operation runtime-manager does not call yet is
//! a new forwarding method here, and `BackgroundWorker` is never re-exported.
//!
//! The re-exported items are data and traits that run no SQL:
//!
//! - [`Job`] is a persisted job, which the worker's lookups return and
//!   executors receive. `Job::status` is a string, which callers parse into a
//!   [`JobStatus`], failing with [`ParseJobStatusError`].
//! - [`JobExecutor`] is the trait runtime-manager implements for the executor
//!   it hands to `background_worker`. Executors and [`Job::payload_as`] fail
//!   with [`JobError`]. The worker's fallible methods fail with
//!   `tonic::Status`.
//! - [`WorkerConfig`] configures the worker.

use std::time::Duration;

use pplx_space_common_jobs::BackgroundWorker;
pub use pplx_space_common_jobs::{
    Job, JobError, JobExecutor, JobStatus, ParseJobStatusError, WorkerConfig,
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

    /// Re-queue jobs a previous process left interrupted, logging under
    /// `worker_label` when any were.
    pub fn recover_and_log(&self, worker_label: &str) -> Vec<JobId> {
        self.0.recover_and_log(worker_label)
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

    /// The job with `job_id`.
    pub fn get_job(&self, job_id: &JobId) -> Result<Option<Job>, Status> {
        self.0.get_job(job_id)
    }

    /// Durably request cancellation of `job_id` and wait until its executor
    /// has stopped and cleaned up, or `timeout` elapses.
    pub fn cancel_job_and_wait(
        &self,
        job_id: &JobId,
        timeout: Duration,
    ) -> impl Future<Output = Result<Job, Status>> {
        self.0.cancel_job_and_wait(job_id, timeout)
    }

    /// Delete terminal jobs past their retention now and on the worker's
    /// schedule until `shutdown_token` fires.
    pub fn run_terminal_job_pruner(
        self,
        worker_label: &'static str,
        shutdown_token: CancellationToken,
    ) -> impl Future<Output = ()> {
        self.0.run_terminal_job_pruner(worker_label, shutdown_token)
    }

    /// Stop launching work and wait for in-flight executors to yield,
    /// returning false at the deadline.
    pub fn yield_for_restart(&self, timeout: Duration) -> impl Future<Output = bool> {
        self.0.yield_for_restart(timeout)
    }
}
