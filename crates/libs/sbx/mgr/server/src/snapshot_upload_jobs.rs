//! Durable worker for the published-artifact backup pipeline.
//!
//! The backup handler in `service::grpc_lifecycle` pins a locally published
//! artifact, allocates a UUIDv7 backup attempt id, prepares `snapshot.json`,
//! and persists the resulting [`SnapshotUploadJobs`] intent in sbx-mgr's local
//! SQLite (`jobs` table) before remote calls or runtime teardown. The suspend
//! path then requires ClusterApi to accept the exact attempt, stages directly
//! into vol-mgr, and persists those transfer handles before teardown. The worker
//! performs the same idempotent Begin and staging calls as a restart/failure
//! fallback; object upload and finalization remain asynchronous. A mid-flight
//! restart resumes via [`BackgroundWorker::recover`] instead of orphaning the
//! node's newest artifact.
//!
//! Each `execute` attempt:
//!
//! 1. For new prepared intents, idempotently registers the backup with
//!    ClusterApi, then submits or rejoins the exact artifact's vol-mgr jobs and
//!    persists those transfer handles.
//!    Payloads persisted by older binaries already contain transfer IDs and
//!    skip this step.
//! 2. Concurrently polls all vol-mgr jobs — every VM artifact and the
//!    `snapshot.json` metadata document — to a terminal
//!    state. Any permanent vol-mgr failure → [`JobError::Permanent`]; any
//!    transient transport/RPC failure → [`JobError::Retriable`].
//! 3. Calls `ClusterApi.FinalizeArtifactBackup` with `target_state=READY`
//!    only once *all* of those uploads succeed, so the row never becomes
//!    visible to restore while any file (including metadata) is missing.
//!    4xx-equivalent statuses → permanent; everything else → retriable. On
//!    permanent failure earlier in the pipeline, the worker best-effort
//!    flips the row to `target_state=FAILED` so retention can prune the
//!    orphan.
//!
//! Every step is idempotent so re-runs after a retry / restart are safe.

use pplx_space_common_core::types::JobId;

use std::time::{Duration, Instant};

use futures_util::future::join_all;
use pplx_space_common_jobs::{BackgroundWorker, Job, JobError, JobExecutor, WorkerConfig};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};
use pplx_space_volmgr_client::proto::{GetTransferJobRequest, TransferJobStatus};

use pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat;

use crate::snapshot_upload::{
    ROOTFS_BTRFS_SEND_FILENAME, SnapshotMeta, SnapshotUploadJobs, artifact_kind_from_meta,
    format_from_meta, upload_artifacts_for_meta,
};

pub use crate::db::snapshot_upload_jobs::{SNAPSHOT_UPLOAD_JOB_KIND, artifact_backup_dedup_key};

const DEFAULT_CONCURRENCY: usize = 4;
const JOB_TIMEOUT: Duration = Duration::from_secs(7200);
const MAX_ATTEMPTS: i32 = 5;
const TRANSFER_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Build a `BackgroundWorker` configured for the snapshot-upload pipeline.
/// Caller owns the `crate::db::Connection` (it lives alongside the rest of
/// sbx-mgr's SQLite state) and is responsible for calling
/// [`BackgroundWorker::recover_and_log`] at startup and
/// [`BackgroundWorker::yield_for_restart`] at exit.
///
/// This worker is intentionally scoped to [`SNAPSHOT_UPLOAD_JOB_KIND`]:
/// sandbox-manager gives local finalization a bounded deploy-shutdown drain,
/// while durable remote-upload work can resume after startup recovery.
pub(crate) fn build_worker(
    db: crate::db::SharedConnection,
    drain_state: std::sync::Arc<crate::service::drain::DrainState>,
    node_manager: crate::node_manager_transport::NodeManagerConnector,
    volume_manager_socket: std::path::PathBuf,
) -> BackgroundWorker<SnapshotUploadExecutor> {
    let config = WorkerConfig::new(DEFAULT_CONCURRENCY, JOB_TIMEOUT, MAX_ATTEMPTS)
        .with_kind_concurrency(SNAPSHOT_UPLOAD_JOB_KIND, DEFAULT_CONCURRENCY)
        .with_recovery_job_kind(SNAPSHOT_UPLOAD_JOB_KIND);
    let executor = SnapshotUploadExecutor {
        db: db.clone(),
        drain_state,
        retained_source_inspector: std::sync::Arc::new(
            crate::retained_source::NodeManagerSourceInspector(node_manager.clone()),
        ),
        node_manager,
        volume_manager_socket,
    };
    db.background_worker(executor, config)
}

pub fn refund_interrupted_running_attempts(
    conn: &crate::db::Connection,
) -> crate::db::Result<usize> {
    crate::db::snapshot_upload_jobs::refund_interrupted_running_attempts(
        conn,
        SNAPSHOT_UPLOAD_JOB_KIND,
    )
}

/// Executor for the durable snapshot-upload pipeline. Holds the
/// node-local connection handle for the single post-finalize
/// bookkeeping step ([`stamp_backup_completed_and_reclaim_stream`]):
/// stamping `backup_completed_at` — the on-node signal the snapshot
/// reaper consults when deciding whether an artifact's remote backup
/// is durable — and reclaiming the uploaded rootfs stream under the
/// same lock hold.
#[derive(Clone)]
pub struct SnapshotUploadExecutor {
    db: crate::db::SharedConnection,
    drain_state: std::sync::Arc<crate::service::drain::DrainState>,
    /// Sizes a retained source before its stream is reclaimed, so the local
    /// sidecar can switch to counting the image the artifact keeps.
    retained_source_inspector: std::sync::Arc<dyn crate::retained_source::RetainedSourceInspector>,
    node_manager: crate::node_manager_transport::NodeManagerConnector,
    /// The vol-mgr every transfer is staged with and observed through.
    volume_manager_socket: std::path::PathBuf,
}

#[async_trait::async_trait]
impl JobExecutor for SnapshotUploadExecutor {
    async fn execute(
        &self,
        job: &Job,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<i64, JobError> {
        if job.kind != SNAPSHOT_UPLOAD_JOB_KIND {
            return Err(JobError::permanent(format!(
                "unsupported snapshot-upload worker job kind: {}",
                job.kind
            )));
        }

        let mut payload: SnapshotUploadJobs = job.payload_as()?;
        if !payload.includes_metadata_job {
            let error = JobError::permanent("snapshot upload requires staged metadata");
            converge_failed_upload(&payload, "unstaged_metadata", error.message()).await;
            return Err(error);
        }
        let sandbox_id = payload.meta.sandbox_id.clone();
        let snapshot_id = payload.snapshot_id.clone();
        let artifacts = upload_artifacts_for_meta(&payload.meta);
        let origin = payload
            .meta
            .artifact_origin
            .map(|origin| origin.as_str())
            .unwrap_or("suspend");

        if job.attempts == 1
            && let Some(duration_ms) = job
                .started_at
                .as_deref()
                .and_then(|started_at| sqlite_duration_ms(&job.created_at, started_at))
        {
            pplx_space_metrics::snapshot::delivery_duration_ms(
                "snapshot_queue",
                origin,
                None,
                "success",
                duration_ms,
            );
        }

        // Prepared intents carry exact local-artifact and execution identity.
        // Legacy payloads were enqueued only after BeginArtifactBackup and may
        // not carry those fields, so they retain their polling-only behavior.
        // Begin is idempotent for the immutable snapshot/attempt id and is
        // required before volume staging so an authoritative rejection cannot
        // create transfer work. Teardown-gated callers perform the same Begin
        // synchronously and retain the runtime if the control plane rejects it.
        if prepared_upload_requires_begin(&payload)
            && let Err(refusal) = begin_prepared_upload_naming_supersession(&payload).await
        {
            // Best effort: an unrecorded scope reads as the attempt's, which
            // costs a later caller one doomed attempt at most.
            if refusal.refuses_artifact
                && let Err(error) = self
                    .db
                    .lock()
                    .map_err(|_| "database mutex poisoned".to_string())
                    .and_then(|conn| {
                        record_failure_scope(
                            &conn,
                            &job.job_id,
                            crate::snapshot_upload::UploadFailureScope::Artifact,
                        )
                        .map_err(|error| error.to_string())
                    })
            {
                tracing::warn!(job_id = %job.job_id, %error,
                    "could not record a refused snapshot upload's failure scope");
            }
            return Err(refusal.error);
        }

        // Runtime teardown may remove the source paths, while vol-mgr's staged
        // jobs survive it. Persist the durable transfer handles before allowing
        // a teardown-gated caller to release the runtime.
        if payload.job_ids.is_empty() {
            payload = crate::snapshot_upload::submit_prepared_snapshot(
                &payload,
                &self.node_manager,
                &self.volume_manager_socket,
            )
            .await
            .map_err(|error| {
                JobError::retriable(format!("vol-mgr submit prepared snapshot: {error}"))
            })?;
            persist_submitted_transfer_ids(&self.db, &job.job_id, &payload)?;
        }

        // Heartbeat the cluster row at the start of every attempt (including
        // retries and post-recovery requeues) so the stuck-snapshot reaper
        // ages from last-observed activity, not creation time. Best-effort:
        // a failure here is logged and never gates the upload.
        touch_with_cluster_api(&snapshot_id).await;

        let results = join_all(payload.job_ids.iter().map(|job_id| {
            wait_for_artifact_job(
                &self.volume_manager_socket,
                &sandbox_id,
                job_id,
                cancellation.clone(),
            )
        }))
        .await;

        // Per-job upload bytes in transfer-submission order: the
        // VM artifacts followed (for new payloads) by the metadata
        // document. Kept separate from the finalisation total because
        // `FinalizeArtifactBackup.bytes_uploaded` is contractually the VM
        // artifacts only (see `vm_artifact_total_bytes`).
        let mut artifact_results = Vec::with_capacity(results.len());
        for result in results {
            match result {
                Ok(observation) => {
                    artifact_results.push(observation);
                }
                Err(err) => {
                    // Best-effort mark the cluster row failed so retention
                    // can prune it. Failure to do so is logged but does
                    // not change the outcome — the worker still reports
                    // `err` so the harness retries / gives up per its own
                    // policy. The mark-failed call is only worthwhile on a
                    // permanent error (no further attempts will run); on
                    // retriable errors the next attempt may still
                    // succeed.
                    if matches!(err, JobError::Permanent(_)) {
                        converge_failed_upload(&payload, "vol_mgr_transfer", err.message()).await;
                    }
                    return Err(err);
                }
            }
        }
        let vm_artifact_count = artifacts.len().saturating_sub(1);
        if payload.deferred_metadata && payload.job_ids.len() == vm_artifact_count {
            let logical_bytes = artifact_results
                .iter()
                .map(|observation| observation.bytes)
                .sum();
            let rootfs_size_bytes = artifacts
                .iter()
                .zip(&artifact_results)
                .find_map(|(artifact, observation)| {
                    matches!(
                        artifact.kind,
                        pplx_space_common_core::types::SnapshotArtifact::RootfsBtrfsSend
                    )
                    .then_some(observation.bytes)
                })
                .ok_or_else(|| {
                    JobError::permanent("streamed snapshot is missing its rootfs transfer")
                })?;
            let metadata_job = crate::snapshot_upload::submit_streamed_metadata(
                &payload,
                logical_bytes,
                rootfs_size_bytes,
                &self.volume_manager_socket,
            )
            .await
            .map_err(JobError::retriable)?;
            payload.job_ids.push(metadata_job);
            persist_submitted_transfer_ids(&self.db, &job.job_id, &payload)?;
            match wait_for_artifact_job(
                &self.volume_manager_socket,
                &sandbox_id,
                &metadata_job,
                cancellation.clone(),
            )
            .await
            {
                Ok(observation) => artifact_results.push(observation),
                Err(error) => {
                    if matches!(error, JobError::Permanent(_)) {
                        converge_failed_upload(&payload, "vol_mgr_transfer", error.message()).await;
                    }
                    return Err(error);
                }
            }
        }
        if artifact_results.len() != artifacts.len() {
            let error = JobError::permanent("snapshot upload is missing artifact or metadata jobs");
            converge_failed_upload(&payload, "incomplete_transfer_set", error.message()).await;
            return Err(error);
        }
        crate::snapshot_upload::cleanup_metadata_staging_dir(&payload);

        // All VM files and staged metadata are uploaded before publishing Ready.
        //
        // `bytes_uploaded` is contractually the total of the VM artifact
        // bytes only (persisted as `snapshots.size_bytes`); the metadata
        // document is excluded here and tracked solely via its own
        // `snapshot.create.bytes{artifact:metadata}` metric.
        let vm_artifact_count = artifacts.len().saturating_sub(1);
        let artifact_bytes = artifact_results
            .iter()
            .map(|observation| observation.bytes)
            .collect::<Vec<_>>();
        let vm_total_bytes = vm_artifact_total_bytes(&artifact_bytes, vm_artifact_count);
        // Read just before finalizing: a suspend that failed its own handoff
        // rolled back to `paused` and kept the runtime while this job went on
        // uploading the capture.
        let suspend_uncommitted = suspend_uncommitted(&self.db, &payload.meta)?;
        let finalize_started_at = Instant::now();
        let finalize_result =
            finalize_with_cluster_api_ready(&snapshot_id, vm_total_bytes, suspend_uncommitted)
                .await;
        pplx_space_metrics::snapshot::delivery_duration_ms(
            "cluster_finalize",
            origin,
            None,
            if finalize_result.is_ok() {
                "success"
            } else {
                "error"
            },
            finalize_started_at.elapsed().as_secs_f64() * 1_000.0,
        );
        if let Err(error) = finalize_result {
            if matches!(error, JobError::Permanent(_)) {
                converge_failed_upload(&payload, "finalize", error.message()).await;
            }
            return Err(error);
        }

        emit_sandbox_event(
            &sandbox_id,
            SandboxEventType::ArtifactBackupCompleted,
            SandboxEventSource::SbxManager,
            artifact_backup_event_details(&payload.meta, &snapshot_id)
                .bytes_transferred(vm_total_bytes.to_string()),
        );

        // Emit byte counts only after the terminal finalize step so the
        // counter is exactly-once per successful upload (re-polled
        // vol-mgr jobs always return the same bytes_transferred, which
        // would otherwise double-count on retry). Tags come from the
        // per-codec artifact list — the rootfs slot's name varies
        // (`rootfs` vs `rootfs.btrfs-send`), and a positional const
        // would mislabel btrfs-send bytes as `artifact:rootfs`,
        // silently aggregating both codecs and zeroing out the
        // codec-specific tile on every dashboard that splits on
        // `(codec, artifact)`. The polled list ends with the `metadata` slot,
        // so the metadata job's bytes are attributed there.
        let codec = pplx_space_metrics::snapshot::codec_tag_from_diff_format(
            payload.meta.rootfs_diff_format.as_deref(),
        );
        // The sealed artifacts' own recorded fact; this job runs off the
        // published sidecar, with no sandbox row in scope.
        let encrypted = payload.meta.encrypted;
        let create_kind = payload
            .meta
            .artifact_origin
            .map(|origin| origin.as_str())
            .unwrap_or("suspend");
        for (i, observation) in artifact_results.iter().enumerate() {
            let artifact_tag = artifacts
                .get(i)
                .map(|a| a.metric_tag())
                .unwrap_or("unknown");
            if let Some(duration_ms) = observation.queue_duration_ms {
                pplx_space_metrics::snapshot::delivery_duration_ms(
                    "artifact_queue",
                    origin,
                    Some(artifact_tag),
                    "success",
                    duration_ms,
                );
            }
            if let Some(duration_ms) = observation.transfer_duration_ms {
                pplx_space_metrics::snapshot::delivery_duration_ms(
                    "artifact_transfer",
                    origin,
                    Some(artifact_tag),
                    "success",
                    duration_ms,
                );
            }
            pplx_space_metrics::snapshot::create_bytes(
                create_kind,
                codec,
                encrypted,
                artifact_tag,
                observation.bytes,
            );
        }
        if let Some(duration_ms) = elapsed_since_rfc3339_ms(&payload.meta.created_at) {
            pplx_space_metrics::snapshot::delivery_duration_ms(
                "remote_total",
                origin,
                None,
                "success",
                duration_ms,
            );
        }

        Ok(self.complete_ready_backup(&payload, vm_total_bytes))
    }

    async fn retries_exhausted(&self, job: &Job, message: &str) {
        let payload = match job.payload_as::<SnapshotUploadJobs>() {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!(job_id = %job.job_id, %error, "failed to decode exhausted snapshot upload");
                return;
            }
        };
        converge_failed_upload(&payload, "retry_budget_exhausted", message).await;
    }
}

impl SnapshotUploadExecutor {
    /// READY must return success without another await: optional local work,
    /// including the node-manager inspection a reclaim needs, cannot consume
    /// the execution deadline and turn the final attempt into a failed job
    /// that would lose its protected marker-repair evidence.
    fn complete_ready_backup(&self, payload: &SnapshotUploadJobs, bytes: u64) -> i64 {
        if payload.meta.local_artifact_id.is_some() {
            // The bookkeeping task owns its drain lease independently of this
            // handle. Failure or panic leaves completed-job repair evidence;
            // maintenance retries the marker, while stream reclaim is optional.
            drop(spawn_backup_bookkeeping(
                self.db.clone(),
                &self.drain_state,
                payload,
                std::sync::Arc::clone(&self.retained_source_inspector),
            ));
        }
        i64::try_from(bytes).unwrap_or(i64::MAX)
    }
}

/// Node-manager's current length for the retained source whose stream a
/// reclaim would remove. Asked by the bookkeeping task after the job's remote
/// completion, never under the database lock. `None` keeps the stream.
async fn retained_image_for_reclaim(
    inspector: &dyn crate::retained_source::RetainedSourceInspector,
    payload: &SnapshotUploadJobs,
) -> Option<u64> {
    let source_id = payload.meta.rootfs_send_source_id?;
    if !stream_reclaim_applies(&payload.meta) {
        return None;
    }
    let source = crate::retained_source::RetainedSource {
        sandbox_id: payload.meta.sandbox_id.clone(),
        artifact_dir: payload.snapshot_dir.clone(),
        source_id,
    };
    match inspector.inspect(&source).await {
        crate::retained_source::RetainedSourceCheck::Present { rootfs_bytes } => Some(rootfs_bytes),
        check => {
            tracing::info!(
                sandbox_id = %payload.meta.sandbox_id,
                source_id = %source_id,
                %check,
                "rootfs stream reclaim deferred: retained source not verified"
            );
            None
        }
    }
}

pub(crate) fn spawn_backup_bookkeeping(
    db: crate::db::SharedConnection,
    drain_state: &std::sync::Arc<crate::service::drain::DrainState>,
    payload: &SnapshotUploadJobs,
    inspector: std::sync::Arc<dyn crate::retained_source::RetainedSourceInspector>,
) -> Option<tokio::task::JoinHandle<crate::db::Result<()>>> {
    // Admission is capped before spawning, so returning the upload worker's
    // slot cannot accumulate unbounded queued writers. Refusal defers repair.
    let writer = match crate::service::drain::acquire_state_writer(
        drain_state,
        "SnapshotBackupBookkeeping",
    ) {
        Ok(writer) => writer,
        Err(error) => {
            if stream_reclaim_applies(&payload.meta) {
                pplx_space_metrics::snapshot::stream_reclaim_skipped("admission_refused");
            }
            tracing::info!(
                snapshot_id = %payload.snapshot_id,
                %error,
                "backup bookkeeping deferred to completed-job maintenance; rootfs stream may linger"
            );
            return None;
        }
    };
    let payload = payload.clone();
    Some(tokio::spawn(async move {
        // The executor returns after remote success without waiting for local
        // bookkeeping and can drop the handle immediately. Keep the lease
        // through the bounded inspection, queuing, the SQLite wait, and the
        // writer's last mutation.
        let _writer = writer;
        let join_failed = |join: tokio::task::JoinError| {
            crate::db::Error::to_sql_conversion(std::io::Error::other(format!(
                "backup bookkeeping writer panicked: {join}"
            )))
        };
        // The durability marker first, waiting on nothing but the database.
        // Whether a reclaim is due is read in the same hold, so node-manager
        // is asked only when the stream exists and the pin still stands.
        let reclaim_due = {
            let db = db.clone();
            let payload = payload.clone();
            tokio::task::spawn_blocking(move || {
                let conn = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                stamp_backup_completed(&conn, &payload)?;
                stream_reclaim_due(&conn, &payload)
            })
            .await
            .unwrap_or_else(|join| Err(join_failed(join)))
        };
        let result = match reclaim_due {
            Err(error) => Err(error),
            Ok(false) => Ok(()),
            Ok(true) => {
                let reclaim_image = retained_image_for_reclaim(inspector.as_ref(), &payload).await;
                let payload = payload.clone();
                tokio::task::spawn_blocking(move || {
                    let conn = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    reclaim_stream(&conn, &payload, &payload.snapshot_id, reclaim_image)
                })
                .await
                .unwrap_or_else(|join| Err(join_failed(join)))
            }
        };
        if let Err(error) = &result {
            if stream_reclaim_applies(&payload.meta) {
                pplx_space_metrics::snapshot::stream_reclaim_skipped("db_error");
            }
            tracing::warn!(
                sandbox_id = %payload.meta.sandbox_id,
                snapshot_id = %payload.snapshot_id,
                local_artifact_id = ?payload.meta.local_artifact_id,
                %error,
                "post-finalize backup bookkeeping failed; completed job retains marker repair evidence and the rootfs stream may linger",
            );
        }
        result
    }))
}

pub(crate) fn persist_submitted_transfer_ids(
    db: &crate::db::SharedConnection,
    job_id: &JobId,
    payload: &SnapshotUploadJobs,
) -> Result<(), JobError> {
    let payload = serde_json::to_string(payload).map_err(|error| {
        JobError::permanent(format!(
            "serialize submitted snapshot-upload payload for {job_id}: {error}"
        ))
    })?;
    let conn = db.lock().map_err(|_| {
        JobError::permanent(format!(
            "snapshot-upload database mutex poisoned while persisting transfer IDs for {job_id}"
        ))
    })?;
    let changed = crate::db::snapshot_upload_jobs::replace_active_job_payload(
        &conn,
        &payload,
        job_id,
        SNAPSHOT_UPLOAD_JOB_KIND,
    )
    .map_err(|error| {
        JobError::retriable(format!(
            "persist submitted transfer IDs for snapshot-upload job {job_id}: {error}"
        ))
    })?;
    if changed != 1 {
        return Err(JobError::permanent(format!(
            "snapshot-upload job {job_id} was not pending or running while persisting transfer IDs"
        )));
    }
    Ok(())
}

pub(crate) fn prepared_upload_requires_begin(payload: &SnapshotUploadJobs) -> bool {
    payload.meta.local_artifact_id.is_some()
        && payload.meta.artifact_origin.is_some()
        && payload.meta.execution.is_some()
}

/// A refused prepared `BeginArtifactBackup`: the job error, the generation
/// the control plane named when it refused the upload as superseded, and
/// whether the refusal is about the artifact itself.
#[derive(Debug)]
pub(crate) struct BeginRefusal {
    pub(crate) error: JobError,
    pub(crate) superseded_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    /// See [`registration_refusal_is_final`].
    pub(crate) refuses_artifact: bool,
}

/// Send a prepared upload's `BeginArtifactBackup`, keeping the refusal's
/// verdicts for a caller that acts on them.
pub(crate) async fn begin_prepared_upload_naming_supersession(
    payload: &SnapshotUploadJobs,
) -> Result<(), BeginRefusal> {
    let refused = match send_prepared_begin(payload, false).await {
        Ok(Ok(_)) => return Ok(()),
        Ok(Err(refused)) => refused,
        Err(error) => {
            return Err(BeginRefusal {
                error,
                superseded_generation: None,
                refuses_artifact: false,
            });
        }
    };
    let (superseded_generation, refuses_artifact) = match &refused {
        BeginArtifactBackupError::BeginArtifactBackup {
            superseded_generation,
            code,
            ..
        } => (
            *superseded_generation,
            registration_refusal_is_final(*code, *superseded_generation),
        ),
        BeginArtifactBackupError::Connect(_) => (None, false),
    };
    Err(BeginRefusal {
        error: classify_begin_error(refused),
        superseded_generation,
        refuses_artifact,
    })
}

/// Whether the control plane refused a registration because of the artifact
/// itself, so no attempt can register it: `InvalidArgument`, a request whose
/// artifact identity or metadata it rejects, or a refusal that names the
/// retired execution that captured the artifact. Every other refusal fails
/// only the attempt, since the response carries no typed reason and those
/// codes also name node- or attempt-level refusals: `NotFound` for a source
/// node that isn't registered yet, `FailedPrecondition` for a node with no
/// origin address or an attempt id reused with another identity, and
/// authentication or authorization refusals. A caller bounds the attempts it
/// starts after such a failure.
fn registration_refusal_is_final(
    code: tonic::Code,
    superseded_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
) -> bool {
    code == tonic::Code::InvalidArgument || superseded_generation.is_some()
}

/// Record on a failing upload's payload what its failure failed, for a later
/// caller that has to decide whether a new attempt can succeed.
pub(crate) fn record_failure_scope(
    conn: &crate::db::Connection,
    job_id: &JobId,
    scope: crate::snapshot_upload::UploadFailureScope,
) -> crate::db::Result<usize> {
    crate::db::snapshot_upload_jobs::set_upload_failure_scope(conn, job_id, scope.as_str())
}

/// What the re-sent registration says about the upload's execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationProbe {
    /// The control plane refused the registration because it superseded this
    /// generation.
    Superseded(pplx_space_common_core::types::ExecutionGeneration),
    /// The control plane ran the generation fence on the verdict-only
    /// registration and admitted it, so the fence would admit a fresh attempt
    /// too.
    Admitted,
    /// The control plane refused the registration without naming a
    /// superseded generation, admitted it without running the fence, or
    /// registered it as a server that predates verdict-only requests does; or
    /// the payload can't make one. The fence doesn't run on a replay of an
    /// attempt that has a row, even a failed one, or on a fresh attempt for
    /// bytes another attempt registered, so that admission says nothing about
    /// whether a new attempt would be fenced.
    NotSuperseded,
    /// No answer: cluster-api couldn't be reached, or failed in a way a retry
    /// may clear.
    Unanswered,
}

/// Re-send a prepared upload's `BeginArtifactBackup` as a verdict-only
/// request and report whether the control plane refuses it because the
/// upload's execution generation was retired. The teardown driver uses it to
/// recover the typed refusal that a permanent job failure no longer carries.
///
/// The server answers as the upload's own registration would and persists
/// nothing, so an admitted probe leaves no row, and in particular no failed
/// full row newer than the sandbox's ready snapshot, which would hold the
/// retired execution's release as not durably restorable. A server that
/// predates verdict-only requests registers the attempt instead; its reply
/// says so ([`verdict_only_reply_registered`]), and the probe closes that row
/// `failed`, best effort, so it doesn't gate this node's drain until the
/// stuck-attempt reaper finds it. A failed close is logged and leaves that
/// reaper as the backstop. Finalize never moves a `ready` row.
pub(crate) async fn prepared_begin_refused_as_superseded(
    payload: &SnapshotUploadJobs,
) -> RegistrationProbe {
    match send_prepared_begin(payload, true).await {
        Ok(Err(BeginArtifactBackupError::BeginArtifactBackup {
            superseded_generation: Some(generation),
            ..
        })) => RegistrationProbe::Superseded(generation),
        Ok(Ok(reply)) if verdict_only_reply_registered(&reply) => {
            finalize_with_cluster_api_failed(
                &payload.snapshot_id,
                "teardown backup probe re-registered an upload that had already failed",
            )
            .await;
            RegistrationProbe::NotSuperseded
        }
        Ok(Ok(reply)) if reply.generation_fence_admitted => RegistrationProbe::Admitted,
        Ok(Ok(_)) => RegistrationProbe::NotSuperseded,
        Ok(Err(refused)) => match classify_begin_error(refused).kind() {
            pplx_space_common_jobs::JobFailureKind::Permanent => RegistrationProbe::NotSuperseded,
            _ => RegistrationProbe::Unanswered,
        },
        Err(_) => RegistrationProbe::NotSuperseded,
    }
}

/// Whether the reply to a verdict-only registration shows the server
/// registered the attempt anyway: a server that predates `fence_verdict_only`
/// ignores it and doesn't echo it.
pub(crate) fn verdict_only_reply_registered(
    reply: &pplx_space_node_api_client::proto::BeginArtifactBackupResponse,
) -> bool {
    !reply.fence_verdict_only
}

/// The outer error is a malformed payload, which no retry can fix; the inner
/// result is the cluster-api call.
async fn send_prepared_begin(
    payload: &SnapshotUploadJobs,
    fence_verdict_only: bool,
) -> Result<
    Result<
        pplx_space_node_api_client::proto::BeginArtifactBackupResponse,
        BeginArtifactBackupError,
    >,
    JobError,
> {
    let local_artifact_id = payload.meta.local_artifact_id.ok_or_else(|| {
        JobError::permanent("prepared snapshot upload is missing local_artifact_id")
    })?;
    let artifact_origin = payload.meta.artifact_origin.ok_or_else(|| {
        JobError::permanent("prepared snapshot upload is missing artifact_origin")
    })?;
    let execution = payload.meta.execution.as_ref().ok_or_else(|| {
        JobError::permanent("prepared snapshot upload is missing execution metadata")
    })?;
    let snapshot_name = payload
        .meta
        .artifact_snapshot_name
        .as_ref()
        .map(|name| name.as_str());
    let local_artifact_id = local_artifact_id.to_string();

    Ok(begin_with_cluster_api(BeginArtifactBackup {
        sandbox_id: &payload.meta.sandbox_id,
        local_artifact_id: &local_artifact_id,
        artifact_kind: artifact_kind_from_meta(&payload.meta),
        artifact_origin,
        snapshot_name,
        backup_attempt_id: &payload.snapshot_id,
        storage_uri: &payload.storage_uri,
        execution,
        execution_generation: payload.execution_generation,
        fence_verdict_only,
    })
    .await)
}

fn classify_begin_error(error: BeginArtifactBackupError) -> JobError {
    use pplx_space_node_api_client::cluster::ConnectClusterApiError;
    match error {
        BeginArtifactBackupError::Connect(error) => {
            if matches!(
                error,
                ConnectClusterApiError::MissingEndpoint | ConnectClusterApiError::MissingToken(_)
            ) {
                JobError::permanent(format!("cluster-api not configured: {error}"))
            } else {
                JobError::retriable(format!("cluster-api connect: {error}"))
            }
        }
        BeginArtifactBackupError::BeginArtifactBackup { code, message, .. } => {
            classify_cluster_status("begin_artifact_backup", tonic::Status::new(code, message))
        }
    }
}

async fn converge_failed_upload(payload: &SnapshotUploadJobs, reason: &'static str, error: &str) {
    finalize_with_cluster_api_failed(&payload.snapshot_id, error).await;
    emit_sandbox_event(
        &payload.meta.sandbox_id,
        SandboxEventType::ArtifactBackupFailed,
        SandboxEventSource::SbxManager,
        artifact_backup_event_details(&payload.meta, &payload.snapshot_id)
            .reason(reason)
            .error(error),
    );
    crate::snapshot_upload::cleanup_metadata_staging_dir(payload);
}

/// Whether a finalized upload's payload is in scope for the post-backup
/// rootfs-stream reclaim: a suspend-origin full artifact whose rootfs
/// rode the btrfs-send codec, with the artifact identity needed to
/// prove the on-disk file is the uploaded bytes.
fn stream_reclaim_applies(meta: &SnapshotMeta) -> bool {
    format_from_meta(meta) == SuspendSnapshotFormat::BtrfsSend
        && artifact_kind_from_meta(meta) == pplx_space_common_core::types::ArtifactKind::Full
        && meta.artifact_origin == Some(pplx_space_common_core::types::ArtifactOrigin::Suspend)
        && meta.local_artifact_id.is_some()
}

/// Post-finalize node-local bookkeeping, in one hold of the connection
/// mutex: stamp the artifact's `backup_completed_at` marker (the
/// per-artifact durability signal the snapshot reaper gates on), then
/// reclaim the uploaded `rootfs.btrfs-send` stream from the published
/// snapshot directory. Same-node wake is preserved — the snapshot's
/// `memory`/`vmstate` files and the `instance/` rootfs subvolume are
/// never touched; restore paths pull the stream from durable storage.
///
/// Reclaim requires proof that the file on disk is exactly the bytes
/// the just-finalized backup uploaded: the sandbox row must still be
/// `suspended` with the payload's artifact pinned, and the unlink
/// happens under the same mutex hold every lifecycle state transition
/// serializes on. A suspend staging a newer stream holds the row in
/// `suspending` until it atomically publishes the new pin, so the gate
/// can never pass while the published path holds newer bytes. (Vol-mgr
/// additionally reflinks every push source into its own staging dir at
/// submit time, so no in-flight upload reads this path either.)
///
/// Scope (see [`stream_reclaim_applies`]): suspend-origin full
/// artifacts in the btrfs-send codec. Full-codec snapshots keep
/// `rootfs.ext4` (the reupload/promotion path can still need it), and
/// named snapshots are orthogonal to the suspend pin this gate proves
/// against. Skips and unlink failures are logged + counted here and
/// never propagated; only DB errors surface to the caller's
/// best-effort warn. Worst case of any skip: the stream lingers until
/// the next suspend rewrites the directory or eviction removes it.
/// `reclaim_image` is node-manager's length for the artifact's retained
/// source, when it has one. A retained artifact counts its stream in the
/// local sidecar while the stream exists; before the stream goes, the sidecar
/// is rewritten to count the image, which is what remains. Validation accepts
/// either accounting while both are present, so no ordering here can strand
/// the artifact.
/// The two bookkeeping steps in one hold, for tests of their combined effect.
#[cfg(test)]
fn stamp_backup_completed_and_reclaim_stream(
    conn: &crate::db::Connection,
    payload: &SnapshotUploadJobs,
    backup_attempt_id: &str,
    reclaim_image: Option<u64>,
) -> Result<(), crate::db::Error> {
    stamp_backup_completed(conn, payload)?;
    reclaim_stream(conn, payload, backup_attempt_id, reclaim_image)
}

/// Stamp the artifact's durability marker. Nothing else waits on this.
fn stamp_backup_completed(
    conn: &crate::db::Connection,
    payload: &SnapshotUploadJobs,
) -> Result<(), crate::db::Error> {
    let Some(local_artifact_id) = payload.meta.local_artifact_id else {
        return Ok(());
    };
    crate::db::mark_local_artifact_backup_completed(conn, &local_artifact_id.to_string())
}

/// Whether a reclaim is worth an inspection: a reclaimable artifact whose
/// stream still exists while the sandbox is suspended with it pinned. A
/// reclaimable artifact that fails either check is counted as skipped here,
/// because no later step sees it.
fn stream_reclaim_due(
    conn: &crate::db::Connection,
    payload: &SnapshotUploadJobs,
) -> Result<bool, crate::db::Error> {
    let Some(local_artifact_id) = payload.meta.local_artifact_id else {
        return Ok(false);
    };
    if !stream_reclaim_applies(&payload.meta) {
        return Ok(false);
    }
    let sandbox_id = &payload.meta.sandbox_id;
    // Not a fence here: the connection guard is released before node-manager
    // is asked, so `reclaim_stream` repeats this check under the hold that
    // unlinks.
    if !crate::db::read::sandbox_view(conn, sandbox_id)
        .suspended_with_pinned_artifact(&local_artifact_id.to_string())?
    {
        pplx_space_metrics::snapshot::stream_reclaim_skipped("not_pinned_suspended");
        tracing::info!(
            sandbox_id,
            backup_attempt_id = %payload.snapshot_id,
            local_artifact_id = %local_artifact_id,
            "rootfs stream reclaim skipped: sandbox no longer suspended with this artifact pinned",
        );
        return Ok(false);
    }
    if !payload
        .snapshot_dir
        .join(ROOTFS_BTRFS_SEND_FILENAME)
        .exists()
    {
        pplx_space_metrics::snapshot::stream_reclaim_skipped("missing");
        return Ok(false);
    }
    Ok(true)
}

/// Reclaim the uploaded stream under the connection hold that rechecks the
/// pin, so a wake that moved it cannot race the unlink.
fn reclaim_stream(
    conn: &crate::db::Connection,
    payload: &SnapshotUploadJobs,
    backup_attempt_id: &str,
    reclaim_image: Option<u64>,
) -> Result<(), crate::db::Error> {
    let Some(local_artifact_id) = payload.meta.local_artifact_id else {
        return Ok(());
    };
    if !stream_reclaim_applies(&payload.meta) {
        return Ok(());
    }
    let sandbox_id = &payload.meta.sandbox_id;
    // A fence only because `conn` is the connection guard every state
    // transition serializes on, held here across the unlink below.
    if !crate::db::read::sandbox_view(conn, sandbox_id)
        .suspended_with_pinned_artifact(&local_artifact_id.to_string())?
    {
        pplx_space_metrics::snapshot::stream_reclaim_skipped("not_pinned_suspended");
        tracing::info!(
            sandbox_id,
            backup_attempt_id,
            local_artifact_id = %local_artifact_id,
            "rootfs stream reclaim skipped: sandbox no longer suspended with this artifact pinned",
        );
        return Ok(());
    }

    let stream_path = payload.snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME);
    let bytes = std::fs::metadata(&stream_path).map(|m| m.len());
    if payload.meta.rootfs_send_source_id.is_some() {
        let Some(image) = reclaim_image else {
            pplx_space_metrics::snapshot::stream_reclaim_skipped("source_unverified");
            return Ok(());
        };
        if bytes.is_ok()
            && let Err(error) = rewrite_sidecar_for_reclaim(&payload.snapshot_dir, image)
        {
            pplx_space_metrics::snapshot::stream_reclaim_skipped("sidecar_rewrite_failed");
            tracing::warn!(
                sandbox_id,
                backup_attempt_id,
                local_artifact_id = %local_artifact_id,
                %error,
                "rootfs stream kept: local sidecar could not switch to the retained image",
            );
            return Ok(());
        }
    }
    match bytes.and_then(|bytes| std::fs::remove_file(&stream_path).map(|()| bytes)) {
        Ok(bytes) => {
            pplx_space_metrics::snapshot::stream_reclaimed(bytes);
            tracing::info!(
                sandbox_id,
                backup_attempt_id,
                local_artifact_id = %local_artifact_id,
                path = %stream_path.display(),
                bytes,
                "rootfs stream reclaimed after durable backup",
            );
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::RootfsStreamReclaimed,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .local_artifact_id(local_artifact_id.to_string())
                    .backup_attempt_id(backup_attempt_id)
                    .local_path(stream_path.display().to_string()),
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            pplx_space_metrics::snapshot::stream_reclaim_skipped("missing");
        }
        Err(error) => {
            pplx_space_metrics::snapshot::stream_reclaim_skipped("io_error");
            tracing::warn!(
                sandbox_id,
                backup_attempt_id,
                local_artifact_id = %local_artifact_id,
                path = %stream_path.display(),
                %error,
                "rootfs stream reclaim failed; stream lingers until the next suspend or eviction",
            );
        }
    }
    Ok(())
}

fn artifact_backup_event_details(
    meta: &SnapshotMeta,
    backup_attempt_id: &str,
) -> SandboxEventDetails {
    let mut details = SandboxEventDetails::default()
        .snapshot_id(backup_attempt_id)
        .backup_attempt_id(backup_attempt_id)
        .artifact_kind(artifact_kind_from_meta(meta).as_str());
    if let Some(local_artifact_id) = meta.local_artifact_id {
        details = details.local_artifact_id(local_artifact_id.to_string());
    }
    if let Some(origin) = meta.artifact_origin {
        details = details.artifact_origin(origin.as_str());
    }
    if let Some(execution) = &meta.execution {
        details = details.execution_metadata(execution.execution());
    }
    details
}

/// The `bytes_uploaded` value to finalise a snapshot with: the sum of the
/// VM artifact upload bytes only, excluding the trailing `snapshot.json`
/// metadata document.
///
/// `FinalizeArtifactBackup.bytes_uploaded` is documented as "total
/// compressed bytes of all VM artifacts" and persisted as
/// `snapshots.size_bytes`, so the metadata bytes must not leak into it
/// (they are tracked separately via `snapshot.create.bytes{artifact:metadata}`).
///
/// `polled_bytes` is the per-job byte count in `upload_artifacts_for_kind`
/// order (VM artifacts first); `vm_artifact_count` is how many leading
/// entries are VM artifacts. New payloads have one trailing metadata entry
/// (dropped here); legacy payloads have none (the slice is all VM
/// artifacts), so the same `take` is correct for both.
fn vm_artifact_total_bytes(polled_bytes: &[u64], vm_artifact_count: usize) -> u64 {
    polled_bytes
        .iter()
        .take(vm_artifact_count)
        .fold(0u64, |acc, b| acc.saturating_add(*b))
}

/// Completed transfer bytes and persisted queue/transfer timing.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ArtifactTransferObservation {
    bytes: u64,
    queue_duration_ms: Option<f64>,
    transfer_duration_ms: Option<f64>,
}

/// Observe a durable transfer until terminal, bounded by the worker's overall
/// attempt deadline. Connect and individual read-only probes each have a
/// ten-second budget; transport failures retry the upload job, while permanent
/// transfer failure remains terminal. Cancellation yields without cancelling
/// remote work, and re-polling terminal transfers is safe.
async fn wait_for_artifact_job(
    volume_manager_socket: &std::path::Path,
    sandbox_id: &str,
    job_id: &JobId,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<ArtifactTransferObservation, JobError> {
    // One reconnecting channel per artifact attempt, not a fresh connection
    // on every poll. These are observation-only requests: timing out does not
    // abandon the durable volume-manager transfer or cancel remote mutation.
    let volume_manager_socket = volume_manager_socket.to_string_lossy();
    let client = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(JobError::YieldedForRestart),
        result = tokio::time::timeout(
            TRANSFER_PROBE_TIMEOUT,
            pplx_space_volmgr_client::connect(&volume_manager_socket),
        ) => {
            result.map_err(|_| JobError::retriable(format!("vol-mgr connect timed out ({job_id})")))?
                .map_err(|e| JobError::retriable(format!("vol-mgr connect ({job_id}): {e}")))?
        }
    };
    poll_artifact_job(sandbox_id, *job_id, client, cancellation).await
}

/// Probe interval after the first pending answer. A teardown suspend holds its
/// caller (an E2B pause) until every artifact is durable, and a compressed
/// snapshot usually uploads in one to two seconds, so every probe of
/// node-local volume-manager comes soon. The interval doubles up to
/// [`ARTIFACT_POLL_MAX`], which bounds how late a completion is noticed.
const ARTIFACT_POLL_INITIAL: Duration = Duration::from_millis(100);
const ARTIFACT_POLL_MAX: Duration = Duration::from_millis(500);

fn next_artifact_poll_interval(interval: Duration) -> Duration {
    (interval * 2).min(ARTIFACT_POLL_MAX)
}

async fn poll_artifact_job(
    sandbox_id: &str,
    job_id: JobId,
    mut client: pplx_space_volmgr_client::ConnectedVolumeManagerClient,
    cancellation: tokio_util::sync::CancellationToken,
) -> Result<ArtifactTransferObservation, JobError> {
    let mut interval = ARTIFACT_POLL_INITIAL;
    loop {
        let resp = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(JobError::YieldedForRestart),
            response = tokio::time::timeout(TRANSFER_PROBE_TIMEOUT, client.get_transfer_job(GetTransferJobRequest {
                job_id: job_id.to_string(),
            })) => response
                .map_err(|_| JobError::retriable(format!("get_transfer_job timed out ({job_id})")))?
                .map_err(|e| JobError::retriable(format!("get_transfer_job ({job_id}): {e}")))?
                .into_inner(),
        };

        let job = resp.job.ok_or_else(|| {
            JobError::retriable(format!("get_transfer_job returned empty job ({job_id})"))
        })?;

        match TransferJobStatus::try_from(job.status).ok() {
            Some(TransferJobStatus::Completed) => {
                tracing::info!(
                    sandbox_id,
                    job_id = %job_id,
                    bytes = job.bytes_transferred,
                    "snapshot artifact upload complete"
                );
                emit_sandbox_event(
                    sandbox_id,
                    SandboxEventType::SnapshotArtifactUploadCompleted,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .transfer_job_id(job_id.to_string())
                        .bytes_transferred(job.bytes_transferred.to_string()),
                );
                let queue_duration_ms = job
                    .started_at
                    .as_deref()
                    .and_then(|started_at| sqlite_duration_ms(&job.created_at, started_at));
                let transfer_duration_ms = job
                    .started_at
                    .as_deref()
                    .zip(job.completed_at.as_deref())
                    .and_then(|(started_at, completed_at)| {
                        sqlite_duration_ms(started_at, completed_at)
                    });
                return Ok(ArtifactTransferObservation {
                    bytes: job.bytes_transferred,
                    queue_duration_ms,
                    transfer_duration_ms,
                });
            }
            Some(TransferJobStatus::Failed) => {
                let error = job.error.unwrap_or_default();
                emit_sandbox_event(
                    sandbox_id,
                    SandboxEventType::SnapshotArtifactUploadFailed,
                    SandboxEventSource::SbxManager,
                    SandboxEventDetails::default()
                        .transfer_job_id(job_id.to_string())
                        .error(error.clone()),
                );
                return Err(JobError::permanent(format!(
                    "vol-mgr job {job_id} failed permanently: {}",
                    error
                )));
            }
            _ => {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(JobError::YieldedForRestart),
                    _ = tokio::time::sleep(interval) => {}
                }
                interval = next_artifact_poll_interval(interval);
            }
        }
    }
}

fn sqlite_duration_ms(started_at: &str, completed_at: &str) -> Option<f64> {
    fn timestamp_ms(value: &str) -> Option<i64> {
        jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", value.trim())
            .ok()?
            .to_zoned(jiff::tz::TimeZone::UTC)
            .ok()
            .map(|value| value.timestamp().as_millisecond())
    }

    let elapsed = timestamp_ms(completed_at)?.checked_sub(timestamp_ms(started_at)?)?;
    Some(elapsed.max(0) as f64)
}

fn elapsed_since_rfc3339_ms(created_at: &str) -> Option<f64> {
    let created_at = created_at.parse::<jiff::Timestamp>().ok()?;
    let elapsed = jiff::Timestamp::now()
        .as_millisecond()
        .checked_sub(created_at.as_millisecond())?;
    Some(elapsed.max(0) as f64)
}

/// Connect to the cluster API, classifying connect errors as permanent vs
/// retriable. Missing config is a permanent operator-facing problem (the
/// node isn't wired to reach the cluster); anything else is a transient
/// transport blip.
async fn connect_cluster_api()
-> Result<pplx_space_node_api_client::cluster::AuthenticatedClusterApiClient, JobError> {
    use pplx_space_node_api_client::cluster::ConnectClusterApiError;
    pplx_space_node_api_client::cluster::connect_from_env()
        .await
        .map_err(|e| {
            if matches!(
                e,
                ConnectClusterApiError::MissingEndpoint | ConnectClusterApiError::MissingToken(_),
            ) {
                JobError::permanent(format!("cluster-api not configured: {e}"))
            } else {
                JobError::retriable(format!("cluster-api connect: {e}"))
            }
        })
}

/// Classify a `tonic::Status` from a suspend-snapshot RPC. 4xx-equivalent
/// codes mean "the cluster will reject this regardless of how many times we
/// ask" and are permanent. Everything else is retriable so the worker drives
/// the exp-backoff retry.
fn classify_cluster_status(rpc: &'static str, status: tonic::Status) -> JobError {
    if matches!(
        status.code(),
        tonic::Code::InvalidArgument
            | tonic::Code::NotFound
            | tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
            | tonic::Code::FailedPrecondition
    ) {
        JobError::permanent(format!(
            "cluster-api rejected {rpc} ({:?}): {}",
            status.code(),
            status.message()
        ))
    } else {
        JobError::retriable(format!(
            "cluster-api {rpc} ({:?}): {}",
            status.code(),
            status.message()
        ))
    }
}

/// Convert a `u64` byte count to the `i64` Postgres expects, clamping
/// at `i64::MAX` on overflow and emitting a warning when that happens.
/// `log_id` is whatever identifier the caller wants the warning tagged
/// with — currently always the per-attempt `snapshot_id` because both
/// finalize call sites operate at that scope (the original `sandbox_id`
/// naming was a hold-over from the previous register-snapshot flow
/// and would misdirect operators searching by sandbox ID).
fn bytes_uploaded_i64(log_id: &str, bytes_uploaded: u64) -> i64 {
    i64::try_from(bytes_uploaded).unwrap_or_else(|_| {
        tracing::warn!(
            log_id,
            bytes_uploaded,
            "snapshot upload size exceeds i64::MAX; clamping for API call"
        );
        i64::MAX
    })
}

/// Register an artifact-backup attempt before any vol-mgr submission. The
/// cluster row appears in `state='creating'` so retention can age out
/// attempts that never finalize.
pub(crate) struct BeginArtifactBackup<'a> {
    pub(crate) sandbox_id: &'a str,
    pub(crate) local_artifact_id: &'a str,
    pub(crate) artifact_kind: pplx_space_common_core::types::ArtifactKind,
    pub(crate) artifact_origin: pplx_space_common_core::types::ArtifactOrigin,
    pub(crate) snapshot_name: Option<&'a str>,
    pub(crate) backup_attempt_id: &'a str,
    pub(crate) storage_uri: &'a str,
    pub(crate) execution: &'a pplx_space_common_core::types::SnapshotExecutionMetadata,
    pub(crate) execution_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    /// Ask for the registration's verdict without persisting the attempt.
    pub(crate) fence_verdict_only: bool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BeginArtifactBackupError {
    #[error("cluster-api connect: {0}")]
    Connect(#[source] pplx_space_node_api_client::cluster::ConnectClusterApiError),
    #[error("begin_artifact_backup ({code:?}): {message}")]
    BeginArtifactBackup {
        code: tonic::Code,
        message: String,
        /// The generation a refusal says the control plane retired.
        superseded_generation: Option<pplx_space_common_core::types::ExecutionGeneration>,
    },
}

pub(crate) async fn begin_with_cluster_api(
    request: BeginArtifactBackup<'_>,
) -> Result<pplx_space_node_api_client::proto::BeginArtifactBackupResponse, BeginArtifactBackupError>
{
    let mut client = pplx_space_node_api_client::cluster::connect_from_env()
        .await
        .map_err(BeginArtifactBackupError::Connect)?;

    client
        .begin_artifact_backup(begin_artifact_backup_request(&request))
        .await
        .map(tonic::Response::into_inner)
        .map_err(|status| BeginArtifactBackupError::BeginArtifactBackup {
            code: status.code(),
            message: status.message().to_string(),
            superseded_generation:
                pplx_space_node_api_client::cluster::backup_execution_superseded_generation(
                    &status,
                ),
        })
}

fn begin_artifact_backup_request(
    request: &BeginArtifactBackup<'_>,
) -> pplx_space_node_api_client::proto::BeginArtifactBackupRequest {
    pplx_space_node_api_client::proto::BeginArtifactBackupRequest {
        sandbox_id: request.sandbox_id.to_string(),
        local_artifact_id: request.local_artifact_id.to_string(),
        artifact_kind: match request.artifact_kind {
            pplx_space_common_core::types::ArtifactKind::Disk => {
                pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32
            }
            pplx_space_common_core::types::ArtifactKind::Full => {
                pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32
            }
        },
        backup_attempt_id: request.backup_attempt_id.to_string(),
        storage_uri: request.storage_uri.to_string(),
        artifact_origin: match request.artifact_origin {
            pplx_space_common_core::types::ArtifactOrigin::Stop => {
                pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop as i32
            }
            pplx_space_common_core::types::ArtifactOrigin::Suspend => {
                pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend as i32
            }
            pplx_space_common_core::types::ArtifactOrigin::Snapshot => {
                pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Snapshot as i32
            }
            pplx_space_common_core::types::ArtifactOrigin::Fork => {
                pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Fork as i32
            }
        },
        snapshot_name: request.snapshot_name.map(str::to_owned),
        execution: Some(request.execution.execution().into()),
        capture_host_cpu: request
            .execution
            .host_cpu()
            .map(|host_cpu| host_cpu.as_str().to_owned()),
        execution_generation: request
            .execution_generation
            .map(pplx_space_common_core::types::ExecutionGeneration::get),
        fence_verdict_only: request.fence_verdict_only,
    }
}

/// Whether the node may still hold the runtime this upload's capture was
/// meant to release, so a ready backup of it must not make the control plane
/// retire the sandbox's execution. Only suspend captures are projected, so any
/// other origin answers `false`.
///
/// A row in a state that may hold a runtime outside a suspend
/// ([`SandboxState::holds_runtime_outside_suspend`]) answers `true`: a suspend
/// rolled back to `paused` lands there, as does a sandbox woken since, and so
/// does a quarantined one, whose VM nothing on the node settles. A
/// `suspending` or `suspended` row answers `true` unless it pins this capture.
/// A suspend begun after a rollback clears the pin, and a `suspended` row that
/// pins another capture committed that one instead. A node-local suspend pins
/// its capture only when its cleanup completes, so while it is still
/// `suspending` the node's own `suspended` report projects the row; a teardown
/// suspend pins its capture before it tears down. Every other state, like a
/// missing row, answers `false` and keeps the projection as before. That
/// includes the cleanup states, even after a node-local suspend's cleanup
/// fails without pinning its capture; a VM such a row keeps is the cleanup's
/// to settle. A state this binary can't parse answers `true`: withholding the
/// projection leaves it to the node's own `suspended` report, while a wrong
/// projection retires a live runtime.
///
/// The answer is read before the finalize call and not held across it, so a
/// wake that starts in between still meets a projection.
///
/// [`SandboxState::holds_runtime_outside_suspend`]: crate::state::SandboxState::holds_runtime_outside_suspend
fn suspend_uncommitted(
    db: &crate::db::SharedConnection,
    meta: &SnapshotMeta,
) -> Result<bool, JobError> {
    use crate::state::SandboxState;

    if meta
        .artifact_origin
        .is_some_and(|origin| origin != pplx_space_common_core::types::ArtifactOrigin::Suspend)
    {
        return Ok(false);
    }
    let sandbox_id = &meta.sandbox_id;
    let read_failed = |error: crate::db::Error| {
        JobError::retriable(format!(
            "read sandbox {sandbox_id} before finalize: {error}"
        ))
    };
    let conn = db
        .lock()
        .map_err(|_| JobError::retriable("snapshot-upload database mutex poisoned"))?;
    let view = crate::db::read::sandbox_view(&conn, sandbox_id);
    let Some(label) = view.current_state_label().map_err(read_failed)? else {
        return Ok(false);
    };
    Ok(match label.parse::<SandboxState>() {
        Err(_) => true,
        // A legacy payload names no artifact; it keeps the projection.
        Ok(SandboxState::Suspending | SandboxState::Suspended) => match meta.local_artifact_id {
            Some(artifact) => {
                let pin = view
                    .pinned_artifact()
                    .pin_and_updated_at()
                    .map_err(read_failed)?
                    .and_then(|(pin, _)| pin);
                pin.as_deref() != Some(artifact.to_string().as_str())
            }
            None => false,
        },
        Ok(state) => state.holds_runtime_outside_suspend(),
    })
}

/// Flip the backup row to `state='ready'`. This is the terminal step of the
/// durable worker's `execute` and makes the row visible to restore.
async fn finalize_with_cluster_api_ready(
    snapshot_id: &str,
    bytes_uploaded: u64,
    suspend_uncommitted: bool,
) -> Result<(), JobError> {
    let bytes = bytes_uploaded_i64(snapshot_id, bytes_uploaded);
    let mut client = connect_cluster_api().await?;
    let req = pplx_space_node_api_client::proto::FinalizeArtifactBackupRequest {
        backup_attempt_id: snapshot_id.to_string(),
        target_state: pplx_space_node_api_client::proto::SuspendSnapshotTerminalState::Ready as i32,
        bytes_uploaded: bytes,
        failure_reason: None,
        artifact_digests: Vec::new(),
        suspend_uncommitted,
    };

    match client.finalize_artifact_backup(req).await {
        Ok(_) => {
            tracing::info!(
                snapshot_id,
                bytes = bytes_uploaded,
                suspend_uncommitted,
                "snapshot finalized ready with cluster-api"
            );
            Ok(())
        }
        Err(status) => Err(classify_cluster_status("finalize_artifact_backup", status)),
    }
}

/// Best-effort heartbeat of an in-flight artifact backup. Called at the start of each `execute`
/// attempt so the cluster-controller's stuck-snapshot reaper ages the row
/// from last-observed worker activity (`last_attempt_at`) rather than
/// creation time. Logged on error but never propagated: a transient
/// cluster-api blip or missing config must not gate the upload, and the
/// heartbeat is advisory — the worst case of a missed beat is the reaper's
/// clock not pausing for this attempt.
async fn touch_with_cluster_api(snapshot_id: &str) {
    let mut client = match pplx_space_node_api_client::cluster::connect_from_env().await {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!(
                snapshot_id,
                error = %err,
                "best-effort heartbeat could not reach cluster-api"
            );
            return;
        }
    };
    let req = pplx_space_node_api_client::proto::TouchArtifactBackupRequest {
        backup_attempt_id: snapshot_id.to_string(),
    };
    if let Err(status) = client.touch_artifact_backup(req).await {
        tracing::warn!(
            snapshot_id,
            code = ?status.code(),
            message = status.message(),
            "best-effort heartbeat rejected by cluster-api"
        );
    }
}

/// Best-effort mark a snapshot row as failed when the upload pipeline gives
/// up permanently. Logged on error but does not propagate — the worker's
/// terminal outcome is already permanent, and a transient cluster-api blip
/// must not gate that.
///
/// `pub(crate)` so the backup handler in `service::grpc_lifecycle` can
/// reuse this best-effort transition when an error in
/// `submit_suspend_snapshot` / `submit_json` would otherwise leave the
/// `creating` row orphaned (no durable worker will ever pick it up,
/// because submission never happened or never got persisted).
pub(crate) async fn finalize_with_cluster_api_failed(snapshot_id: &str, reason: &str) {
    let client_result = pplx_space_node_api_client::cluster::connect_from_env().await;
    let mut client = match client_result {
        Ok(c) => c,
        Err(err) => {
            tracing::warn!(
                snapshot_id,
                error = %err,
                "best-effort mark-failed could not reach cluster-api"
            );
            return;
        }
    };
    let req = pplx_space_node_api_client::proto::FinalizeArtifactBackupRequest {
        backup_attempt_id: snapshot_id.to_string(),
        target_state: pplx_space_node_api_client::proto::SuspendSnapshotTerminalState::Failed
            as i32,
        bytes_uploaded: 0,
        failure_reason: Some(reason.to_string()),
        artifact_digests: Vec::new(),
        suspend_uncommitted: false,
    };
    if let Err(status) = client.finalize_artifact_backup(req).await {
        tracing::warn!(
            snapshot_id,
            code = ?status.code(),
            message = status.message(),
            "best-effort mark-failed rejected by cluster-api"
        );
    }
}

/// Progress of one startup or periodic repair batch. Missing identity and
/// malformed payloads require operator investigation; neither is permission
/// to discard evidence or infer remote durability for a local artifact.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BackupMarkerBackfill {
    /// Raw completed snapshot-upload rows examined, including malformed payloads.
    pub examined: usize,
    /// Rows newly stamped with `backup_completed_at`.
    pub stamped: usize,
    /// Rows already stamped (post-rollout steady state).
    pub already_stamped: usize,
    /// Completed jobs whose artifact row no longer exists.
    pub artifact_row_missing: usize,
    /// Completed jobs whose payload carries no
    /// `meta.local_artifact_id` (legacy payloads).
    pub payload_missing_artifact_id: usize,
    /// Completed jobs whose payload did not parse as JSON.
    pub malformed_payload: usize,
    /// Marker writes that failed; their retained evidence is retried next sweep.
    pub marker_write_failed: usize,
}

impl BackupMarkerBackfill {
    pub fn total_scanned(&self) -> usize {
        self.stamped
            + self.already_stamped
            + self.artifact_row_missing
            + self.payload_missing_artifact_id
            + self.malformed_payload
            + self.marker_write_failed
    }
}

/// Finite rowid sweep: retained malformed jobs cannot monopolize the first
/// page, and concurrent inserts wait for a later sweep without extending this
/// one's horizon. A row completed behind the cursor is revisited next sweep.
#[derive(Default)]
pub struct BackupMarkerScan {
    after: Option<i64>,
    through: Option<i64>,
}

const BACKUP_MARKER_BATCH: usize = 128;

/// Repair one bounded page from exact completed-upload evidence. Required
/// evidence is protected from pruning by the database; older jobs may already
/// have been pruned before that protection existed. Artifacts without evidence
/// remain unmarked and unreapable: operators must establish remote durability
/// or arrange a fresh backup, never infer success from age or sandbox identity.
pub fn backfill_backup_completed_markers(
    conn: &crate::db::Connection,
    scan: &mut BackupMarkerScan,
) -> crate::db::Result<BackupMarkerBackfill> {
    let mut report = BackupMarkerBackfill::default();
    let through = match scan.through {
        Some(through) => through,
        None => {
            let through = crate::db::snapshot_upload_jobs::max_job_rowid(conn)?;
            let Some(through) = through else {
                return Ok(report);
            };
            scan.through = Some(through);
            through
        }
    };
    crate::db::snapshot_upload_jobs::visit_completed_upload_payloads(
        conn,
        scan.after.unwrap_or(i64::MIN),
        through,
        BACKUP_MARKER_BATCH as i64,
        |now, rowid, payload| {
            scan.after = Some(rowid);
            report.examined += 1;
            let Ok(payload) = payload else {
                report.malformed_payload += 1;
                return Ok(());
            };
            // Extract via `serde_json::Value` rather than the full
            // `SnapshotUploadJobs` struct: historical payloads predate
            // several of its fields, and the backfill needs only the
            // artifact id.
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&payload) else {
                report.malformed_payload += 1;
                return Ok(());
            };
            let Some(artifact_id) = value
                .get("meta")
                .and_then(|m| m.get("local_artifact_id"))
                .and_then(|v| v.as_str())
            else {
                report.payload_missing_artifact_id += 1;
                return Ok(());
            };
            let changed = match crate::db::snapshot_upload_jobs::stamp_backup_completed_if_unset(
                conn,
                now,
                artifact_id,
            ) {
                Ok(changed) => changed,
                Err(error) => {
                    report.marker_write_failed += 1;
                    tracing::warn!(artifact_id, %error, "backup marker repair failed; evidence retained for the next sweep");
                    return Ok(());
                }
            };
            if changed == 1 {
                report.stamped += 1;
                return Ok(());
            }
            let exists = crate::db::snapshot_upload_jobs::local_artifact_exists(conn, artifact_id)?;
            if exists {
                report.already_stamped += 1;
            } else {
                report.artifact_row_missing += 1;
            }
            Ok(())
        },
    )?;
    if report.examined < BACKUP_MARKER_BATCH || scan.after == Some(through) {
        *scan = BackupMarkerScan::default();
    }
    for (reason, count) in [
        ("malformed_payload", report.malformed_payload),
        ("missing_artifact_id", report.payload_missing_artifact_id),
    ] {
        if count > 0 {
            pplx_space_metrics::snapshot::backup_marker_repair_unidentifiable(reason, count as u64);
        }
    }
    Ok(report)
}

/// Repairs markers independently of GC policy and upload retry exhaustion.
/// Each blocking pass is awaited through shutdown so no local writer escapes
/// the daemon's task drain. The pruning trigger protects unvisited obligations.
pub(crate) async fn run_maintenance(
    db: crate::db::SharedConnection,
    worker: BackgroundWorker<SnapshotUploadExecutor>,
    drain: std::sync::Arc<crate::service::drain::DrainState>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut scan = BackupMarkerScan::default();
    let mut history_scan = crate::db::quarantine_history::QuarantineHistoryPruner::default();
    let mut timestamp_scan = crate::db::timestamps::RestoreScopeTimestampScan::default();
    let mut last_prune = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {}
        }
        let Ok(writer) = crate::service::drain::acquire_state_writer(&drain, "BackupMarkerRepair")
        else {
            continue;
        };
        let db = db.clone();
        let worker = worker.clone();
        let prune = last_prune.is_none_or(|last: tokio::time::Instant| {
            last.elapsed() >= pplx_space_common_jobs::TERMINAL_JOB_PRUNE_INTERVAL
        });
        match tokio::task::spawn_blocking(move || {
            let _writer = writer;
            let (result, history_result, timestamp_result) = {
                let conn = crate::node_state_sync::connection_or_signal_failure(&db)?;
                let result = backfill_backup_completed_markers(&conn, &mut scan);
                let history_result = crate::db::now_epoch_i64().and_then(|now| {
                    let now = u64::try_from(now).map_err(crate::db::Error::to_sql_conversion)?;
                    history_scan.next_batch(&conn, now)
                });
                let timestamp_result = timestamp_scan.next_batch(&conn);
                (result, history_result, timestamp_result)
            };
            let pruned =
                prune.then(|| worker.try_prune_terminal_jobs_once("sandbox-mgr.snapshot_upload"));
            Ok::<_, crate::node_state_sync::DatabaseMutexPoisoned>((
                scan,
                history_scan,
                timestamp_scan,
                result,
                history_result,
                timestamp_result,
                pruned,
            ))
        })
        .await
        {
            Ok(Ok((
                next,
                next_history,
                next_timestamp,
                result,
                history_result,
                timestamp_result,
                pruned,
            ))) => {
                scan = next;
                history_scan = next_history;
                timestamp_scan = next_timestamp;
                match timestamp_result {
                    Ok(changed) if changed > 0 => {
                        tracing::info!(changed, "normalized historical restore-scope timestamps")
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "restore-scope timestamp normalization failed; retrying on the next tick")
                    }
                }
                match history_result {
                    Ok(report) if report.deleted > 0 => {
                        tracing::info!(
                            examined = report.examined,
                            deleted = report.deleted,
                            "expired inactive quarantine history pruned"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "quarantine history pruning failed; retrying on the next tick");
                    }
                }
                match pruned {
                    Some(Ok(_)) => last_prune = Some(tokio::time::Instant::now()),
                    Some(Err(error)) => {
                        tracing::warn!(%error, "terminal-job pruning failed; retrying on the next tick");
                    }
                    None => {}
                }
                match result {
                    Ok(report)
                        if report.malformed_payload > 0
                            || report.payload_missing_artifact_id > 0 =>
                    {
                        tracing::warn!(
                            ?report,
                            "backup-marker repair retained unidentifiable evidence; operator repair required"
                        )
                    }
                    Ok(report) if report.total_scanned() > 0 => {
                        tracing::info!(?report, "backup-marker repair batch complete")
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "backup-marker repair failed; retrying on the next tick")
                    }
                }
            }
            Ok(Err(_)) => return,
            Err(error) => {
                scan = BackupMarkerScan::default();
                history_scan = crate::db::quarantine_history::QuarantineHistoryPruner::default();
                timestamp_scan = crate::db::timestamps::RestoreScopeTimestampScan::default();
                tracing::warn!(%error, "backup-marker maintenance task failed; retrying on the next tick");
            }
        }
    }
}

/// Switch the local sidecar to count the retained image instead of the
/// stream, atomically, before the stream is unlinked. The total is recomputed
/// from the remaining VM files plus the verified image, never adjusted from the
/// previous value, so an interrupted or repeated reclaim converges on the same
/// accounting instead of counting the image twice.
pub(crate) fn rewrite_sidecar_for_reclaim(
    snapshot_dir: &std::path::Path,
    image_len: u64,
) -> std::io::Result<()> {
    let path = snapshot_dir.join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
    let mut meta: SnapshotMeta = serde_json::from_slice(&std::fs::read(&path)?)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut size_bytes = image_len;
    for artifact in
        crate::snapshot_upload::vm_artifacts_for_format(SuspendSnapshotFormat::BtrfsSend)
    {
        if artifact.kind == pplx_space_common_core::types::SnapshotArtifact::RootfsBtrfsSend {
            continue;
        }
        let len = std::fs::metadata(snapshot_dir.join(artifact.local_name))?.len();
        size_bytes = size_bytes.checked_add(len).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "artifact size overflow")
        })?;
    }
    if meta.size_bytes == size_bytes && meta.rootfs_size_bytes.is_none() {
        return Ok(());
    }
    meta.size_bytes = size_bytes;
    meta.rootfs_size_bytes = None;
    let bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    crate::snapshot_upload::replace_file_durably(
        snapshot_dir,
        ".snapshot.json.reclaim",
        crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME,
        &bytes,
    )
}

#[cfg(test)]
pub(crate) mod tests;
