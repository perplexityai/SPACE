//! Replay-safe admission of one immutable backup attempt.

use std::time::Duration;

use pplx_space_common_core::types::{JobId, SnapshotArtifact};
use pplx_space_common_grpc::{
    GrpcRetryContext, GrpcRetryPolicy, is_refused_before_start, retry_after,
    retry_grpc_status_with_context_if,
};
use pplx_space_volmgr_client::{ConnectedVolumeManagerClient, proto};
use tonic::{Code, Status};

use super::SnapshotMetadataWriteError;

pub(crate) const HANDOFF_RETRY_POLICY: GrpcRetryPolicy = GrpcRetryPolicy {
    max_retries: usize::MAX,
    initial_delay: Duration::from_millis(100),
    backoff_multiplier: 2.0,
    max_delay: Duration::from_secs(1),
    max_elapsed: Duration::from_secs(120),
    jitter_factor: 0.2,
    max_server_retry_after: Duration::from_secs(120),
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotSubmissionError {
    #[error("connect to volume-manager: {0}")]
    Connect(#[source] pplx_space_volmgr_client::ConnectError),
    #[error("stat prepared rootfs stream: {0}")]
    Stat(#[source] std::io::Error),
    #[error("materialize prepared rootfs stream: {0}")]
    Materialize(#[source] crate::btrfs_via_nodemgr::BtrfsViaNodemgrError),
    #[error("write prepared snapshot metadata: {0}")]
    Metadata(#[source] SnapshotMetadataWriteError),
    #[error("deferred rootfs stream has no source identity")]
    MissingSourceIdentity,
    #[error("admit {artifact} to volume-manager: {source}")]
    Admission {
        artifact: SnapshotArtifact,
        #[source]
        source: Status,
    },
}

impl SnapshotSubmissionError {
    pub(crate) fn status(&self) -> Option<&Status> {
        match self {
            Self::Admission { source, .. } => Some(source),
            Self::Connect(_)
            | Self::Stat(_)
            | Self::Materialize(_)
            | Self::Metadata(_)
            | Self::MissingSourceIdentity => None,
        }
    }

    pub(crate) fn is_transient(&self) -> bool {
        self.status().is_some_and(transient_admission_status)
    }
}

/// A wire refusal is not a transport diagnosis. In particular, never classify
/// Internal/Cancelled by their Display text or turn caller refusal into retry.
pub(crate) fn transient_admission_status(status: &Status) -> bool {
    if is_refused_before_start(status) {
        return false;
    }
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded => true,
        Code::ResourceExhausted => retry_after(status).is_some(),
        Code::Unknown | Code::Internal | Code::Cancelled => {
            let mut cause = std::error::Error::source(status);
            while let Some(error) = cause {
                if let Some(error) = error.downcast_ref::<std::io::Error>() {
                    return matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::NotFound
                            | std::io::ErrorKind::TimedOut
                    );
                }
                cause = error.source();
            }
            false
        }
        _ => false,
    }
}

#[derive(Clone)]
pub(super) enum PreparedTransfer {
    File(proto::PushFileRequest),
    Btrfs(proto::PushBtrfsSendRequest),
}

impl PreparedTransfer {
    /// Preserve the existing one-shot submission path for non-auto jobs.
    pub(super) async fn admit_once(
        &self,
        mut client: ConnectedVolumeManagerClient,
        artifact: SnapshotArtifact,
    ) -> Result<JobId, SnapshotSubmissionError> {
        let result = match self {
            Self::File(request) => client.submit_push_file(request.clone()).await,
            Self::Btrfs(request) => client.submit_push_btrfs_send(request.clone()).await,
        };
        let source = result.and_then(|response| {
            response.into_inner().job_id.parse().map_err(|error| {
                Status::data_loss(format!("invalid accepted transfer id: {error}"))
            })
        });
        source.map_err(|source| SnapshotSubmissionError::Admission { artifact, source })
    }

    fn identity(&self) -> (String, String, &str, &'static str) {
        match self {
            Self::File(request) => (
                request.subject_id.clone(),
                request.local_path.clone(),
                &request.s3_uri,
                "push",
            ),
            Self::Btrfs(request) => (
                format!("sandbox:{}", request.sandbox_id),
                std::path::Path::new(&request.artifact_dir)
                    .join(super::ROOTFS_BTRFS_SEND_FILENAME)
                    .to_string_lossy()
                    .into_owned(),
                &request.s3_uri,
                "push_btrfs_send",
            ),
        }
    }

    fn verify(&self, job: &proto::TransferJob) -> Result<JobId, Status> {
        let (subject, path, uri, kind) = self.identity();
        if job.subject_id != subject
            || job.local_path != path
            || job.s3_uri != uri
            || job.kind != kind
        {
            return Err(Status::failed_precondition(
                "transfer does not identify the intended immutable artifact",
            ));
        }
        if let Self::Btrfs(request) = self {
            let intended = format!(
                "{}:{}:{}",
                request.sandbox_id, request.source_id, request.s3_uri
            );
            if job.dedup_key.as_deref() != Some(intended.as_str()) {
                return Err(Status::failed_precondition(
                    "transfer retained-source identity does not match",
                ));
            }
        }
        if !matches!(
            proto::TransferJobStatus::try_from(job.status),
            Ok(proto::TransferJobStatus::Pending
                | proto::TransferJobStatus::Running
                | proto::TransferJobStatus::Completed)
        ) {
            return Err(Status::failed_precondition(format!(
                "intended artifact transfer {} is not usable: {:?}",
                job.job_id,
                proto::TransferJobStatus::try_from(job.status)
            )));
        }
        job.job_id
            .parse()
            .map_err(|error| Status::data_loss(format!("invalid transfer job identity: {error}")))
    }

    async fn rejoin(
        &self,
        client: &mut ConnectedVolumeManagerClient,
    ) -> Result<Option<JobId>, Status> {
        let (subject, _, uri, _) = self.identity();
        let response = client
            .list_transfer_jobs(proto::ListTransferJobsRequest {
                subject_id: Some(subject),
                status_filter: None,
                limit: 2,
                s3_uri: Some(uri.to_owned()),
            })
            .await?
            .into_inner();
        if !response.exact_destination_lookup {
            return Err(Status::unavailable(
                "volume-manager successor with immutable transfer lookup is not ready",
            ));
        }
        let jobs = response.jobs;
        let mut matching = jobs.iter().filter(|job| job.s3_uri == uri);
        let Some(job) = matching.next() else {
            return Ok(None);
        };
        if matching.next().is_some() {
            return Err(Status::failed_precondition(
                "multiple transfers claim the immutable artifact destination",
            ));
        }
        self.verify(job).map(Some)
    }

    async fn submit_or_rejoin(
        &self,
        mut client: ConnectedVolumeManagerClient,
        validate_owner: &impl Fn() -> Result<(), Status>,
        observe_refusal: &impl Fn(&Status),
    ) -> Result<JobId, Status> {
        // Inspect before replay, including after a lost/ambiguous response.
        // The immutable backup-attempt URI, source path, kind and subject must
        // agree. The server additionally deduplicates push and btrfs-send jobs,
        // including completed jobs, using their original immutable identities.
        validate_owner()?;
        if let Some(job_id) = self.rejoin(&mut client).await? {
            validate_owner()?;
            return Ok(job_id);
        }
        validate_owner()?;
        let result = match self {
            Self::File(request) => client.submit_push_file(request.clone()).await,
            Self::Btrfs(request) => client.submit_push_btrfs_send(request.clone()).await,
        };
        let id = match result {
            Ok(response) => response.into_inner().job_id,
            Err(status) if status.code() == Code::AlreadyExists => {
                let joined = self.rejoin(&mut client).await?.ok_or(status)?;
                validate_owner()?;
                return Ok(joined);
            }
            Err(status) => {
                observe_refusal(&status);
                return Err(status);
            }
        };
        let job = client
            .get_transfer_job(proto::GetTransferJobRequest { job_id: id })
            .await?
            .into_inner()
            .job
            .ok_or_else(|| Status::data_loss("accepted artifact transfer disappeared"))?;
        validate_owner()?;
        self.verify(&job)
    }

    pub(super) async fn admit(
        &self,
        client: &ConnectedVolumeManagerClient,
        context: &GrpcRetryContext,
        policy: GrpcRetryPolicy,
        artifact: SnapshotArtifact,
        validate_owner: &impl Fn() -> Result<(), Status>,
        observe_refusal: &impl Fn(&Status),
    ) -> Result<JobId, SnapshotSubmissionError> {
        retry_admission(context, policy, artifact, || {
            self.submit_or_rejoin(client.clone(), validate_owner, observe_refusal)
        })
        .await
    }
}

async fn retry_admission<F, Fut>(
    context: &GrpcRetryContext,
    policy: GrpcRetryPolicy,
    artifact: SnapshotArtifact,
    mut operation: F,
) -> Result<JobId, SnapshotSubmissionError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<JobId, Status>>,
{
    retry_grpc_status_with_context_if(
        "volume-manager",
        "SubmitSnapshotArtifact",
        policy,
        context.clone(),
        // Each replay first inspects the persisted work. Common refusal
        // guards still take precedence over this ambiguity-only predicate.
        |status| status.code() != Code::ResourceExhausted && transient_admission_status(status),
        || {
            let result = operation();
            async move {
                match result.await {
                    Ok(id) => Ok(Ok(id)),
                    // Keep terminal causes out of the common transport
                    // compatibility classifier, without weakening its guards.
                    Err(status) if !transient_admission_status(&status) => Ok(Err(status)),
                    Err(status) => Err(status),
                }
            }
        },
    )
    .await
    .and_then(std::convert::identity)
    .map_err(|source| SnapshotSubmissionError::Admission { artifact, source })
}

#[cfg(test)]
mod tests;
