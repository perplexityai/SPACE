//! Durable references to privileged send sources. Submission and orphan census
//! serialize so a source cannot be collected between staging and job commit.
use std::collections::HashSet;
use std::path::PathBuf;

use pplx_space_common_jobs::{JobError, JobStatus};
use pplx_space_node_mgr_client::proto as node;
use serde::{Deserialize, Serialize};
use tonic::Status;
use uuid::Uuid;

use crate::executor::TransferPayload;

pub(crate) const KIND: &str = "push_btrfs_send";

#[derive(Serialize, Deserialize)]
pub(crate) struct Payload {
    #[serde(flatten)]
    pub transfer: TransferPayload,
    pub send_lease: Uuid,
}

pub(crate) fn classify(status: Status) -> JobError {
    match status.code() {
        tonic::Code::InvalidArgument
        | tonic::Code::PermissionDenied
        | tonic::Code::NotFound
        | tonic::Code::FailedPrecondition => JobError::Permanent(format!("btrfs source: {status}")),
        _ => JobError::Retriable(format!("btrfs source: {status}")),
    }
}

pub(crate) async fn release(id: Uuid) -> Result<(), Status> {
    let mut client = pplx_space_node_mgr_client::connect_once()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    client
        .btrfs_delete_send_source(node::BtrfsSendLease {
            lease_id: id.to_string(),
        })
        .await?;
    Ok(())
}

impl crate::service::VolumeManagerService {
    pub(crate) async fn submit_btrfs_push(
        &self,
        req: crate::proto::PushBtrfsSendRequest,
    ) -> Result<crate::proto::SubmitTransferResponse, Status> {
        self.submit_btrfs_push_with_stager(req, |request| async move {
            let mut client = pplx_space_node_mgr_client::connect_once()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            client.btrfs_stage_send_source(request).await?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn submit_btrfs_push_with_stager<F>(
        &self,
        req: crate::proto::PushBtrfsSendRequest,
        stage: impl FnOnce(node::BtrfsStageSendSourceRequest) -> F,
    ) -> Result<crate::proto::SubmitTransferResponse, Status>
    where
        F: std::future::Future<Output = Result<(), Status>>,
    {
        use crate::executor::PullExistingPolicy;
        use pplx_space_common_core::types::MaterializationMode;
        use pplx_space_common_core::types::{SandboxId, SnapshotArtifact};
        let sandbox_id: SandboxId = req
            .sandbox_id
            .parse()
            .map_err(|error| Status::invalid_argument(format!("sandbox_id: {error}")))?;
        let source_id: Uuid = req
            .source_id
            .parse()
            .map_err(|_| Status::invalid_argument("invalid send source identity"))?;
        let subject_id = format!("sandbox:{sandbox_id}");
        let local_path = PathBuf::from(&req.artifact_dir)
            .join("rootfs.btrfs-send")
            .to_string_lossy()
            .into_owned();
        let dedup = format!("{sandbox_id}:{source_id}:{}", req.s3_uri);
        let _submission = self.btrfs_submission.lock().await;
        // Match ReuseExistingOrCompleted before staging: a failed/cancelled
        // job's lease may already be gone, so resubmission needs a fresh lease.
        if let Some(job) = self
            .worker
            .find_job_by_dedup_key(KIND, &dedup)?
            .filter(|job| {
                matches!(
                    job.status.parse::<JobStatus>(),
                    Ok(JobStatus::Pending
                        | JobStatus::Running
                        | JobStatus::Cancelling
                        | JobStatus::Completed)
                )
            })
        {
            return Ok(crate::proto::SubmitTransferResponse {
                job_id: job.job_id.to_string(),
            });
        }
        let id = Uuid::now_v7();
        stage(node::BtrfsStageSendSourceRequest {
            sandbox_id: sandbox_id.to_string(),
            artifact_dir: req.artifact_dir,
            source_id: source_id.to_string(),
            lease_id: id.to_string(),
        })
        .await?;
        let payload = Payload {
            transfer: TransferPayload {
                subject_id: subject_id.clone(),
                local_path,
                s3_uri: req.s3_uri,
                transfer_scope_id: None,
                snapshot_artifact: Some(SnapshotArtifact::RootfsBtrfsSend),
                staged_path: None,
                raw: false,
                file_mode: None,
                existing_file_policy: PullExistingPolicy::LegacySkipPresent,
                materialization_mode: MaterializationMode::Disabled,
                rootfs_materialization_mode: None,
                encryption: None,
                artifact_id: None,
                entry_role: None,
            },
            send_lease: id,
        };
        let serialized =
            serde_json::to_string(&payload).map_err(|error| Status::internal(error.to_string()))?;
        let submitted = self.worker.submit_with_duplicate_policy(
            KIND,
            &serialized,
            Some(&subject_id),
            Some(&dedup),
            pplx_space_common_jobs::DuplicatePolicy::ReuseExistingOrCompleted,
        );
        match submitted {
            Ok(job_id) => Ok(crate::proto::SubmitTransferResponse {
                job_id: job_id.to_string(),
            }),
            Err(error) => {
                if let Err(cleanup) = release(id).await {
                    tracing::warn!(%cleanup, lease_id = %id, "unsubmitted send source needs orphan sweep");
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn cleanup_btrfs_orphans(&self) -> Result<(), Status> {
        let _submission = self.btrfs_submission.lock().await;
        let keep = {
            let conn = self.lock_db()?;
            crate::db::active_btrfs_upload_payloads(&conn)
                .map_err(|error| Status::internal(error.to_string()))?
                .into_iter()
                .map(|json| {
                    serde_json::from_str::<Payload>(&json).map(|payload| payload.send_lease)
                })
                .collect::<Result<HashSet<_>, _>>()
                .map_err(|error| {
                    Status::failed_precondition(format!("send source census incomplete: {error}"))
                })?
        };
        // Old node-managers have no leases and no new RPC. A disabled service
        // with no durable readers can coexist with them during rolling upgrade.
        // Still attempt the census so disabling the budget cleans crash orphans.
        let census = async {
            let mut client = pplx_space_node_mgr_client::connect_once()
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
            let leases = client
                .btrfs_list_send_sources(node::BtrfsListSendSourcesRequest {})
                .await?
                .into_inner()
                .lease_ids;
            Ok::<_, Status>((client, leases))
        }
        .await;
        let (mut client, leases) = match census {
            Ok(result) => result,
            Err(_) if !self.btrfs_push_enabled && keep.is_empty() => return Ok(()),
            Err(error) => return Err(error),
        };
        // Parse the entire census before deleting anything. Malformed state is
        // not absence, and a cancelled job retains its source until cleanup ends.
        let leases = leases
            .into_iter()
            .map(|id| id.parse::<Uuid>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        for id in leases {
            if !keep.contains(&id) {
                client
                    .btrfs_delete_send_source(node::BtrfsSendLease {
                        lease_id: id.to_string(),
                    })
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn run_btrfs_source_cleanup(
        self,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { biased; _ = shutdown.cancelled() => return, _ = tick.tick() => {} }
            let result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                result = tokio::time::timeout(std::time::Duration::from_secs(30), self.cleanup_btrfs_orphans()) => result,
            };
            if !matches!(result, Ok(Ok(()))) {
                tracing::warn!(
                    ?result,
                    "send source orphan sweep incomplete; retrying next minute"
                );
            }
        }
    }
}
