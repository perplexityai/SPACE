//! build-manager's job queue, the shared `jobs` table in this store's
//! database.
//!
//! [`JobWorker`] is build-manager's only path to the common-jobs worker that
//! runs its queue:
//! [`SharedConnection::background_worker`](crate::database::SharedConnection::background_worker)
//! builds one, and each of its methods forwards unchanged to the worker
//! method of the same name, which runs the queue's SQL. As this crate's
//! `AGENTS.md` says, a worker operation build-manager does not call yet is a
//! new forwarding method here, and `BackgroundWorker` is never re-exported.
//! build-manager's own reads of job rows are the crate root's functions,
//! which run under the caller's held connection. The `test-support` feature
//! adds `wait_for_job`, `list_jobs` and `inflight` for build-manager's tests.
//!
//! The re-exported items are data, traits and metric calls that run no SQL:
//!
//! - [`Job`] is a persisted job, which the worker's lookups return and
//!   executors receive. Executors and [`Job::payload_as`] fail with
//!   [`JobError`]. The worker's fallible methods fail with `tonic::Status`.
//! - [`JobExecutor`] is the trait build-manager implements for the executor
//!   it hands to `background_worker`; its `completed_result` returns a
//!   [`JobCompletedResult`].
//! - [`WorkerConfig`] configures the worker, and [`LeaseConfig`] the leases
//!   it takes on the jobs it runs.
//! - [`drain_outstanding`] reports how many jobs a draining process still
//!   waits for.

#[cfg(feature = "test-support")]
use std::time::Duration;

use pplx_space_common_jobs::BackgroundWorker;
pub use pplx_space_common_jobs::metrics::{JobCompletedResult, drain_outstanding};
pub use pplx_space_common_jobs::{Job, JobError, JobExecutor, LeaseConfig, WorkerConfig};
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

    /// Recover the queue at startup: start finishing the jobs a previous
    /// process left `cancelling`, then, for a worker with leases such as
    /// build-manager's, launch due pending jobs and return no ids. A worker
    /// without leases instead re-queues the jobs left `running`, logs their
    /// count under `worker_label`, and returns their ids.
    pub fn recover_and_log(&self, worker_label: &str) -> Vec<JobId> {
        self.0.recover_and_log(worker_label)
    }

    /// Launch due jobs under the worker's lease on its poll interval until
    /// `shutdown_token` fires; returns at once for a worker without leases.
    pub fn run_queue_driver(self, shutdown_token: CancellationToken) -> impl Future<Output = ()> {
        self.0.run_queue_driver(shutdown_token)
    }

    /// Emit the queue's metrics until `shutdown_token` fires.
    pub fn run_queue_metrics(self, shutdown_token: CancellationToken) -> impl Future<Output = ()> {
        self.0.run_queue_metrics(shutdown_token)
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

    /// The worker id its leases carry, when it is configured with leases.
    pub fn lease_worker_id(&self) -> Option<&str> {
        self.0.lease_worker_id()
    }

    /// Wait until the job with `job_id` is terminal or `timeout` elapses.
    #[cfg(feature = "test-support")]
    pub fn wait_for_job(
        &self,
        job_id: &JobId,
        timeout: Duration,
    ) -> impl Future<Output = Result<Job, Status>> {
        self.0.wait_for_job(job_id, timeout)
    }

    /// Up to `limit` jobs, optionally filtered by subject and status.
    #[cfg(feature = "test-support")]
    pub fn list_jobs(
        &self,
        subject_id: Option<&str>,
        status: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Job>, Status> {
        self.0.list_jobs(subject_id, status, limit)
    }

    /// Number of currently executing tasks.
    #[cfg(feature = "test-support")]
    pub fn inflight(&self) -> usize {
        self.0.inflight()
    }
}
