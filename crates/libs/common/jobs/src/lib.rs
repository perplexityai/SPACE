//! Generic SQLite-backed background job queue with bounded concurrency,
//! exponential-backoff retry, and per-job completion notifications.
//!
//! Callers implement [`JobExecutor`] to define job execution logic. The
//! [`BackgroundWorker`] handles persistence, scheduling, retry, and
//! graceful drain.
//!
//! # Concurrency
//!
//! Per-kind concurrency limits are configured via [`WorkerConfig::kind_concurrency`].
//! Kinds not listed there fall back to [`WorkerConfig::default_concurrency`].
//! Per-kind active-job limits can reject new distinct work before insertion;
//! deduplication still reattaches callers to matching active jobs at capacity.
//! Jobs with higher [`JobPriority`] values run first. Per-kind [`PriorityBand`]s
//! protect execution and admission capacity above configurable thresholds
//! without letting lower-priority work consume it. Higher bands may borrow idle
//! capacity from lower bands, but lower bands cannot borrow protected capacity.
//!
//! # Persistence
//!
//! Every job is persisted to SQLite before any work starts. The `jobs` table
//! is installed from `db/common/` by `space-db-migrator` for each consumer
//! service. On restart, [`BackgroundWorker::recover`] resets interrupted
//! `running` jobs back to `pending` and re-queues them.

pub use pplx_space_common_core::types::JobId;

pub mod db;
mod scheduler;
mod worker;

pub use worker::{
    BackgroundWorker, COMPLETED_JOB_RETENTION, DuplicatePolicy, FAILED_JOB_RETENTION, LeaseConfig,
    PriorityBand, SubjectConcurrency, TERMINAL_JOB_PRUNE_INTERVAL, WorkerConfig,
};

/// Caller-selected scheduling priority. Higher values run first within a job kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobPriority(i32);

impl JobPriority {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

/// Lifecycle state persisted for a background job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid job status: {value}")]
pub struct ParseJobStatusError {
    value: String,
}

impl std::str::FromStr for JobStatus {
    type Err = ParseJobStatusError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "cancelling" => Ok(Self::Cancelling),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            value => Err(ParseJobStatusError {
                value: value.to_string(),
            }),
        }
    }
}

/// Retry disposition persisted for a failed background job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobFailureKind {
    Permanent,
    Retriable,
}

impl JobFailureKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Permanent => "permanent",
            Self::Retriable => "retriable",
        }
    }
}

impl std::fmt::Display for JobFailureKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Error returned when a persisted job failure kind is not recognized.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid job failure kind: {value}")]
pub struct ParseJobFailureKindError {
    value: String,
}

impl std::str::FromStr for JobFailureKind {
    type Err = ParseJobFailureKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "permanent" => Ok(Self::Permanent),
            "retriable" => Ok(Self::Retriable),
            value => Err(ParseJobFailureKindError {
                value: value.to_string(),
            }),
        }
    }
}

impl rusqlite::types::FromSql for JobFailureKind {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = <String as rusqlite::types::FromSql>::column_result(value)?;
        value
            .parse()
            .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

/// Domain reason persisted for a failed background job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobFailureReason {
    NotFound,
    PermissionDenied,
    InvalidInput,
    Internal,
}

impl JobFailureReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::PermissionDenied => "permission_denied",
            Self::InvalidInput => "invalid_input",
            Self::Internal => "internal",
        }
    }
}

impl std::fmt::Display for JobFailureReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Error returned when a persisted job failure reason is not recognized.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid job failure reason: {value}")]
pub struct ParseJobFailureReasonError {
    value: String,
}

impl std::str::FromStr for JobFailureReason {
    type Err = ParseJobFailureReasonError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "not_found" => Ok(Self::NotFound),
            "permission_denied" => Ok(Self::PermissionDenied),
            "invalid_input" => Ok(Self::InvalidInput),
            "internal" => Ok(Self::Internal),
            value => Err(ParseJobFailureReasonError {
                value: value.to_string(),
            }),
        }
    }
}

impl rusqlite::types::FromSql for JobFailureReason {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = <String as rusqlite::types::FromSql>::column_result(value)?;
        value
            .parse()
            .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

/// Persisted failure state for a background job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobFailure {
    Permanent { reason: Option<JobFailureReason> },
    Retriable,
}

impl JobFailure {
    pub const fn kind(self) -> JobFailureKind {
        match self {
            Self::Permanent { .. } => JobFailureKind::Permanent,
            Self::Retriable => JobFailureKind::Retriable,
        }
    }

    pub const fn reason(self) -> Option<JobFailureReason> {
        match self {
            Self::Permanent { reason } => reason,
            Self::Retriable => None,
        }
    }
}

/// A persisted background job.
#[derive(Debug, Clone)]
pub struct Job {
    pub job_id: JobId,
    /// Caller-defined job type (e.g. `"push"`, `"pull"`).
    pub kind: String,
    /// Higher values are scheduled before lower values within this job kind.
    pub priority: JobPriority,
    /// A persisted [`JobStatus`] string.
    pub status: String,
    /// Service-defined payload; typically JSON.
    pub payload: String,
    /// Optional entity identifier used for job listing/filtering.
    pub subject_id: Option<String>,
    /// Optional deduplication key; a non-failed job with the same key and
    /// kind is reused instead of inserting a duplicate.
    pub dedup_key: Option<String>,
    /// Set on terminal failure, with a typed domain reason when available.
    pub failure: Option<JobFailure>,
    pub attempts: i32,
    pub max_attempts: i32,
    /// Bytes processed by the executor (0 when not applicable).
    pub bytes_processed: i64,
    pub error: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    /// W3C `traceparent` captured from the submitting span. Restored on every
    /// execute attempt so retries and recovery after a process restart stay
    /// linked under the same Datadog trace as the originating RPC.
    pub traceparent: Option<String>,
    /// Worker process that currently owns this job lease, when the worker is
    /// running in opt-in leased mode.
    pub lease_owner: Option<String>,
}

impl Job {
    pub fn is_terminal(&self) -> bool {
        self.status
            .parse::<JobStatus>()
            .is_ok_and(JobStatus::is_terminal)
    }

    /// Deserialize the JSON `payload` into a typed value. A parse failure is
    /// surfaced as [`JobError::Permanent`] because a malformed payload is
    /// not recoverable by retry.
    pub fn payload_as<T: serde::de::DeserializeOwned>(&self) -> Result<T, JobError> {
        serde_json::from_str(&self.payload)
            .map_err(|e| JobError::Permanent(format!("malformed job payload: {e}")))
    }
}

/// Error returned by [`JobExecutor::execute`].
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// Non-recoverable failure (bad input, access denied, …). The job is
    /// marked `failed` with `error_kind = "permanent"` immediately.
    #[error("{0}")]
    Permanent(String),
    /// Non-recoverable failure with a typed reason that must survive the job
    /// persistence boundary for programmatic callers.
    #[error("{message}")]
    PermanentWithReason {
        reason: JobFailureReason,
        message: String,
    },
    /// Transient failure (network error, throttling, …). The job is retried
    /// with exponential backoff up to `max_attempts`.
    #[error("{0}")]
    Retriable(String),
    /// Execution yielded at a restart-safe boundary. The job returns to
    /// `pending` without consuming an attempt and is not restarted by the
    /// shutting-down worker.
    #[error("job execution yielded for restart")]
    YieldedForRestart,
}

impl JobError {
    pub fn message(&self) -> &str {
        match self {
            Self::Permanent(message)
            | Self::PermanentWithReason { message, .. }
            | Self::Retriable(message) => message,
            Self::YieldedForRestart => "job execution yielded for restart",
        }
    }

    pub const fn kind(&self) -> JobFailureKind {
        match self {
            Self::Permanent(_) | Self::PermanentWithReason { .. } => JobFailureKind::Permanent,
            Self::Retriable(_) | Self::YieldedForRestart => JobFailureKind::Retriable,
        }
    }

    pub const fn failure_reason(&self) -> Option<JobFailureReason> {
        match self {
            Self::PermanentWithReason { reason, .. } => Some(*reason),
            Self::Permanent(_) | Self::Retriable(_) | Self::YieldedForRestart => None,
        }
    }

    /// Shorthand for `JobError::Permanent(msg.to_string())` so executors
    /// can pass `format!(...)` results directly without an extra `.to_string()`.
    pub fn permanent(msg: impl std::fmt::Display) -> Self {
        Self::Permanent(msg.to_string())
    }

    pub fn permanent_with_reason(reason: JobFailureReason, msg: impl std::fmt::Display) -> Self {
        Self::PermanentWithReason {
            reason,
            message: msg.to_string(),
        }
    }

    pub fn with_default_failure_reason(self, reason: JobFailureReason) -> Self {
        match self {
            Self::Permanent(message) => Self::PermanentWithReason { reason, message },
            error => error,
        }
    }

    /// Shorthand for `JobError::Retriable(msg.to_string())`.
    pub fn retriable(msg: impl std::fmt::Display) -> Self {
        Self::Retriable(msg.to_string())
    }
}

/// A successful execution and an optional service-owned payload containing its
/// completion evidence. The worker publishes both in the same fenced SQLite
/// transition; partial or superseded executions never publish their evidence.
#[derive(Debug)]
pub struct JobCompletion {
    pub bytes_processed: i64,
    pub payload: Option<String>,
}

/// Defines the execution logic for a job kind.
///
/// Implementations must be `Clone` + `Send` + `Sync` + `'static` so they
/// can be shared across background tokio tasks.
#[async_trait::async_trait]
pub trait JobExecutor: Clone + Send + Sync + 'static {
    /// Execute the job. Returns the number of bytes processed (0 if N/A).
    async fn execute(
        &self,
        job: &Job,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<i64, JobError>;

    /// Executors that produce durable evidence may return an updated payload.
    /// Existing executors preserve their intent payload unchanged.
    async fn execute_with_completion(
        &self,
        job: &Job,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<JobCompletion, JobError> {
        self.execute(job, cancellation)
            .await
            .map(|bytes_processed| JobCompletion {
                bytes_processed,
                payload: None,
            })
    }

    /// Remove executor-owned partial state after cancellation. This runs only
    /// after the execute future has stopped, so cleanup cannot race a writer.
    async fn cleanup_cancelled(&self, _job: &Job) -> Result<(), JobError> {
        Ok(())
    }

    /// Observe a retriable error after the worker has persisted the job's
    /// terminal failed state. Executors use this to converge external state
    /// that must not remain pending after the local retry budget is exhausted.
    async fn retries_exhausted(&self, _job: &Job, _message: &str) {}

    /// Whether a job that [`Self::execute`] completed reached its goal, read
    /// after the worker has persisted the completion. An executor that
    /// completes a job with a domain failure recorded for its caller, rather
    /// than failing the job, reports it here, so the job is not counted as a
    /// success.
    fn completed_result(&self, _job: &Job) -> pplx_space_metrics::jobs::JobCompletedResult {
        pplx_space_metrics::jobs::JobCompletedResult::Success
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
fn fixture_job_id(label: &str) -> JobId {
    // Preserve label ordering in tests that exercise the queue's identity tie-breaker.
    let mut bytes = [0; 16];
    for (target, byte) in bytes.iter_mut().zip(label.bytes()) {
        *target = byte;
    }
    JobId::from_uuid(uuid::Uuid::from_bytes(bytes))
}
