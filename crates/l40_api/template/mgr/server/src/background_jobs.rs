//! Generic durable background worker for tmplmgr.
//!
//! tmplmgr's background work goes through one [`BackgroundWorker`]
//! instance whose [`TemplateBackgroundExecutor`] dispatches on
//! `job.kind`. Today the only job kind is [`STAMP_JOB_KIND`] — the
//! durable retry for the privileged `received_uuid` stamp on template
//! subvolumes. Future kinds (e.g. async base-snapshot reconcile,
//! lazy template prune) add a `match` arm here, not a new worker.
//!
//! ## Why a single worker
//!
//! The [`pplx_space_common_jobs::BackgroundWorker`] is generic over
//! one [`JobExecutor`] type, and that executor dispatches per-job by
//! inspecting [`Job::kind`]. Standing up one worker per job kind
//! would duplicate the SQLite recovery loop, the bounded-concurrency
//! scheduler, the shutdown drain — all for no isolation benefit.
//! Per-kind concurrency is configured on the shared worker via
//! [`WorkerConfig::with_kind_concurrency`] when a kind needs a
//! tighter limit than the default pool.
//!
//! ## Stamp job
//!
//! See [`stamp::execute`] for the per-attempt logic. The classifier
//! is:
//!
//! - `FailedPrecondition` from nodemgr → [`JobError::Permanent`].
//!   Digest dir isn't a btrfs subvolume (legacy plain-dir install,
//!   non-btrfs host) and never will be without operator
//!   intervention.
//! - Any other error → [`JobError::Retriable`]. Transient nodemgr
//!   unavailability will resolve; the worker's exponential backoff
//!   does the right thing.
//! - Success → `Ok(0)` (no bytes processed).
//!
//! ## Idempotency
//!
//! Submissions use `dedup_key = "{name}@{build_dir}"` (the content hash,
//! plus `@{environment}` for a build in an environment's cache
//! partition), so a
//! flapping nodemgr that produces multiple finalize failures for the
//! same build coalesces to a single in-flight job. The executor
//! itself is idempotent — re-stamping an already-stamped subvolume
//! is a no-op on the kernel side.

use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_jobs::{BackgroundWorker, Job, JobError, JobExecutor, WorkerConfig};
use serde::{Deserialize, Serialize};

use crate::btrfs_via_nodemgr::BtrfsViaNodemgrError;

pub const STAMP_JOB_KIND: &str = "template_stamp";

const DEFAULT_CONCURRENCY: usize = 2;
const JOB_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_ATTEMPTS: i32 = 10;

/// Payload for a [`STAMP_JOB_KIND`] job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StampPayload {
    pub name: String,
    pub content_hash: String,
    /// Environment of the build's cache partition. Absent for the legacy
    /// partition, including every job enqueued before partitions existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_environment: Option<pplx_space_common_core::types::EnvironmentReference>,
}

impl StampPayload {
    pub fn new(
        name: &str,
        content_hash: &str,
        partition: pplx_space_common_core::types::TemplateCachePartition,
    ) -> Self {
        Self {
            name: name.to_string(),
            content_hash: content_hash.to_string(),
            partition_environment: partition.environment(),
        }
    }

    pub fn partition(&self) -> pplx_space_common_core::types::TemplateCachePartition {
        use pplx_space_common_core::types::TemplateCachePartition;
        self.partition_environment.map_or(
            TemplateCachePartition::Legacy,
            TemplateCachePartition::Environment,
        )
    }

    /// Stable dedup key so concurrent re-enqueues of the same build
    /// coalesce to one in-flight job. Legacy builds keep the
    /// `{name}@{content_hash}` key.
    pub fn dedup_key(&self) -> String {
        format!(
            "{}@{}",
            self.name,
            self.partition().build_dir_name(&self.content_hash)
        )
    }
}

/// Build the shared `BackgroundWorker` for tmplmgr's durable jobs.
///
/// Caller owns the `rusqlite::Connection` (tmplmgr's local
/// `template-manager.db`). The `db/common/` migrations install the required
/// `jobs` table before this worker starts. Call
/// [`BackgroundWorker::recover_and_log`] at startup and
/// [`BackgroundWorker::shutdown`] at exit.
pub fn build_worker(
    db: Arc<std::sync::Mutex<rusqlite::Connection>>,
) -> BackgroundWorker<TemplateBackgroundExecutor> {
    let config = WorkerConfig::new(DEFAULT_CONCURRENCY, JOB_TIMEOUT, MAX_ATTEMPTS)
        .with_kind_concurrency(STAMP_JOB_KIND, DEFAULT_CONCURRENCY);
    BackgroundWorker::new(db, TemplateBackgroundExecutor, config)
}

/// Dispatches durable jobs by `job.kind`. Stateless — all per-job
/// state lives in the JSON payload — so it's freely cloned across
/// worker tasks.
#[derive(Clone, Default)]
pub struct TemplateBackgroundExecutor;

#[async_trait::async_trait]
impl JobExecutor for TemplateBackgroundExecutor {
    async fn execute(
        &self,
        job: &Job,
        _cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<i64, JobError> {
        match job.kind.as_str() {
            STAMP_JOB_KIND => stamp::execute(job).await,
            other => Err(JobError::permanent(format!(
                "tmplmgr background worker received unknown job kind: {other}"
            ))),
        }
    }
}

mod stamp {
    use super::*;
    use uuid::Uuid;

    pub(super) async fn execute(job: &Job) -> Result<i64, JobError> {
        let payload: StampPayload = job.payload_as()?;
        let result = crate::btrfs_via_nodemgr::finalize_template_subvolume(
            &payload.name,
            &payload.content_hash,
            payload.partition(),
        )
        .await;
        classify_finalize_result(result, &payload)
    }

    /// Classify the outcome of a `finalize_template_subvolume` call into
    /// a `JobError`. Extracted so the failure-mode contract (the rule
    /// from the module docs: `FailedPrecondition → Permanent`,
    /// everything else → `Retriable`) can be unit-tested without
    /// spinning up a real nodemgr — the durable retry is the *only*
    /// thing that converges the stamp after a transient nodemgr loss,
    /// so misclassifying connect failures as `Permanent` would silently
    /// strand the build.
    pub(super) fn classify_finalize_result(
        result: Result<Uuid, BtrfsViaNodemgrError>,
        payload: &StampPayload,
    ) -> Result<i64, JobError> {
        match result {
            Ok(received_uuid) => {
                tracing::info!(
                    template = %payload.name,
                    content_hash = %payload.content_hash,
                    cache_partition = %payload.partition(),
                    %received_uuid,
                    "template_stamp job: finalize succeeded (via nodemgr)"
                );
                Ok(0)
            }
            Err(BtrfsViaNodemgrError::Rpc { status, .. })
                if status.code() == tonic::Code::FailedPrecondition =>
            {
                // The digest dir isn't a subvolume; nodemgr returns
                // FailedPrecondition specifically for that. The state
                // can't change without operator action (re-install,
                // template re-bootstrap), so retrying achieves
                // nothing. Surface as permanent so the metric makes
                // it operationally visible.
                Err(JobError::permanent(format!(
                    "template {} digest dir is not a btrfs subvolume; \
                     btrfs-send codec permanently unavailable for this build",
                    payload.dedup_key()
                )))
            }
            Err(err) => Err(JobError::retriable(format!(
                "template_stamp {}: {err}",
                payload.dedup_key()
            ))),
        }
    }
}

#[cfg(test)]
mod tests;
