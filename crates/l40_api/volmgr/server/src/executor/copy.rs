//! Durable byte-preserving copies. Source retention and publication belong to
//! the caller; this job never rewrites metadata or falls back to a download.

use super::*;
use pplx_space_artifact_model::{ExactStorageUri, StorageUriParseError};
use pplx_space_object_store::CopyStorageClass;
use pplx_space_subject_model::SubjectIdParseError;
use pplx_space_volmgr_client::proto::{
    CopyObjectRequest, CopyStorageClass as WireCopyStorageClass,
};

pub(crate) const KIND: &str = "copy_object";
// A worker that predates explicit classes must reject these recovered jobs
// rather than deserializing them as ordinary copies.
pub(crate) const CLASSED_KIND: &str = "copy_object_with_storage_class";

#[derive(Debug, thiserror::Error)]
pub(crate) enum CopyInputError {
    #[error("invalid copy subject: {0}")]
    Subject(#[from] SubjectIdParseError),
    #[error("invalid copy storage URI: {0}")]
    Uri(#[from] StorageUriParseError),
    #[error("object copies require a sandbox subject")]
    NotSandbox,
    #[error("copy source and destination must use the same object store")]
    DifferentStore,
    #[error("copy requires two different, nonempty object keys")]
    InvalidObjects,
    #[error("invalid destination storage class: {0}")]
    InvalidStorageClass(i32),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "CopyObjectRequest", into = "CopyObjectRequest")]
pub(crate) struct CopyPayload {
    subject: SubjectId,
    source: ExactStorageUri,
    destination: ExactStorageUri,
    storage_class: Option<CopyStorageClass>,
}

impl TryFrom<CopyObjectRequest> for CopyPayload {
    type Error = CopyInputError;

    fn try_from(request: CopyObjectRequest) -> Result<Self, Self::Error> {
        let subject = request.subject_id.parse()?;
        if !matches!(subject, SubjectId::Sandbox(_)) {
            return Err(CopyInputError::NotSandbox);
        }
        let source = ExactStorageUri::parse(&request.source_s3_uri)?;
        let destination = ExactStorageUri::parse(&request.destination_s3_uri)?;
        let a = source.parsed();
        let b = destination.parsed();
        if a.bucket != b.bucket
            || a.region != b.region
            || a.endpoint != b.endpoint
            || a.path_style != b.path_style
        {
            return Err(CopyInputError::DifferentStore);
        }
        if a.key_prefix.is_empty() || b.key_prefix.is_empty() || a.key_prefix == b.key_prefix {
            return Err(CopyInputError::InvalidObjects);
        }
        let storage_class = request
            .destination_storage_class
            .map(|value| match WireCopyStorageClass::try_from(value) {
                Ok(WireCopyStorageClass::Standard) => Ok(CopyStorageClass::Standard),
                Ok(WireCopyStorageClass::GlacierInstantRetrieval) => {
                    Ok(CopyStorageClass::GlacierInstantRetrieval)
                }
                Ok(WireCopyStorageClass::Unspecified) | Err(_) => {
                    Err(CopyInputError::InvalidStorageClass(value))
                }
            })
            .transpose()?;
        Ok(Self {
            subject,
            source,
            destination,
            storage_class,
        })
    }
}

impl From<CopyPayload> for CopyObjectRequest {
    fn from(payload: CopyPayload) -> Self {
        Self {
            subject_id: payload.subject.to_string(),
            source_s3_uri: payload.source.as_wire().to_string(),
            destination_s3_uri: payload.destination.as_wire().to_string(),
            destination_storage_class: payload.storage_class.map(|class| match class {
                CopyStorageClass::Standard => WireCopyStorageClass::Standard as i32,
                CopyStorageClass::GlacierInstantRetrieval => {
                    WireCopyStorageClass::GlacierInstantRetrieval as i32
                }
            }),
        }
    }
}

impl CopyPayload {
    pub(crate) fn subject(&self) -> &SubjectId {
        &self.subject
    }
    pub(crate) fn destination(&self) -> &ExactStorageUri {
        &self.destination
    }

    pub(crate) fn kind(&self) -> &'static str {
        match self.storage_class {
            None => KIND,
            Some(_) => CLASSED_KIND,
        }
    }

    pub(crate) fn dedup_key(&self) -> String {
        let mut hash = Sha256::new();
        for value in [
            self.subject.to_string(),
            self.source.parsed().to_string(),
            self.destination.parsed().to_string(),
        ] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        if let Some(class) = self.storage_class {
            let value = class.as_str();
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        format!("{}:{}", self.kind(), hex_encode(&hash.finalize()))
    }
}

impl TransferExecutor {
    pub(super) async fn execute_copy_object(
        &self,
        job: &Job,
        cancellation: CancellationToken,
    ) -> Result<i64, JobError> {
        let payload: CopyPayload = job.payload_as().map_err(|error| {
            JobError::permanent_with_reason(JobFailureReason::InvalidInput, error)
        })?;
        if job.kind != payload.kind() {
            return Err(JobError::permanent_with_reason(
                JobFailureReason::InvalidInput,
                "copy job kind does not match destination storage class",
            ));
        }
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(JobError::retriable("object copy cancelled")),
            result = self.copy_object_inner(&payload) => result,
        }
    }

    async fn copy_object_inner(&self, payload: &CopyPayload) -> Result<i64, JobError> {
        let source = payload.source.parsed();
        let destination = payload.destination.parsed();
        let creds = match &self.config.static_credentials {
            Some(creds) => creds.clone(),
            None => {
                crate::credentials::exchange_copy_credentials(source, destination, &payload.subject)
                    .await
                    .map_err(|error| {
                        classify_storage_credentials("copy credential exchange", error)
                    })?
            }
        };
        let source_target = target(source);
        let destination_target = target(destination);
        let source_meta = self
            .store
            .head(&creds, source_target)
            .await
            .map_err(|error| classify(&error))?
            .ok_or_else(|| {
                JobError::permanent_with_reason(
                    JobFailureReason::NotFound,
                    "copy source object is missing",
                )
            })?;
        let destination_meta = if let Some(class) = payload.storage_class {
            self.store
                .copy_object_with_storage_class(
                    &creds,
                    source_target,
                    destination_target,
                    source_meta.size,
                    class,
                )
                .await
                .map_err(|error| match error {
                    ObjectStoreError::CopyStorageClassUnsupported
                    | ObjectStoreError::CopyStorageClassMismatch { .. } => {
                        JobError::permanent_with_reason(JobFailureReason::InvalidInput, error)
                    }
                    _ => classify(&error),
                })?
        } else {
            self.store
                .copy_object(&creds, source_target, destination_target, source_meta.size)
                .await
                .map_err(|error| classify(&error))?;
            self.store
                .head(&creds, destination_target)
                .await
                .map_err(|error| classify(&error))?
                .ok_or_else(|| JobError::retriable("copied object is not visible"))?
        };
        if destination_meta.size != source_meta.size {
            return Err(JobError::retriable(format!(
                "copied object length {} differs from source length {}",
                destination_meta.size, source_meta.size
            )));
        }
        bytes_len_to_job_i64(source_meta.size, "copied object size")
    }
}

fn target(uri: &StorageUri) -> ObjectTarget<'_> {
    ObjectTarget {
        bucket: &uri.bucket,
        key: &uri.key_prefix,
        region: &uri.region,
        endpoint: uri.endpoint.as_deref(),
        path_style: uri.path_style,
    }
}

#[cfg(test)]
mod tests;
