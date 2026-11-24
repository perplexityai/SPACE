//! template-manager's job queue in this store's SQLite database.
//!
//! [`JobWorker`] is template-manager's only path to the common-jobs worker
//! that runs its queue: [`background_worker`] builds one over the connection
//! template-manager shares with it, and each of its methods forwards
//! unchanged to the worker method of the same name, which runs the queue's
//! SQL. A worker operation template-manager does not call yet is a new
//! forwarding method here; `BackgroundWorker` is never re-exported.
//!
//! The re-exported items are data and traits that run no SQL:
//!
//! - [`JobExecutor`] is the trait template-manager implements for the
//!   executor it hands to [`background_worker`]. It receives each [`Job`],
//!   and it and [`Job::payload_as`] fail with [`JobError`]. The worker's
//!   fallible methods fail with `tonic::Status`.
//! - [`WorkerConfig`] configures the worker.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pplx_space_common_jobs::{BackgroundWorker, JobId};
pub use pplx_space_common_jobs::{Job, JobError, JobExecutor, WorkerConfig};
use tonic::Status;

/// A job worker over `db`, the connection template-manager shares with it.
pub fn background_worker<E: JobExecutor>(
    db: Arc<Mutex<rusqlite::Connection>>,
    executor: E,
    config: WorkerConfig,
) -> JobWorker<E> {
    JobWorker(BackgroundWorker::new(db, executor, config))
}

/// A job worker over template-manager's connection. Each method is the
/// common-jobs worker method of the same name. The async ones return the
/// worker's future directly rather than awaiting it in another. `Clone` is
/// cheap and shares the worker's state.
#[derive(Clone)]
pub struct JobWorker<E>(BackgroundWorker<E>);

impl<E: JobExecutor> JobWorker<E> {
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

    /// Drain the queue and its executor tasks, then log the jobs that
    /// failed permanently. Returns whether the worker proved quiescence
    /// within `drain_timeout`.
    pub fn shutdown(&self, drain_timeout: Duration) -> impl Future<Output = bool> {
        self.0.shutdown(drain_timeout)
    }
}
