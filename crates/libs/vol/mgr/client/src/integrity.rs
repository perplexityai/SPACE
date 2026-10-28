use pplx_space_common_core::types::Digest;

/// Identity of plaintext bytes, independent of transport compression.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Integrity {
    pub digest: Digest,
    pub size_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum IntegrityParseError {
    #[error("content integrity requires a canonical SHA-256 digest")]
    Digest,
}

impl Integrity {
    pub fn parse(digest: &str, size_bytes: u64) -> Result<Self, IntegrityParseError> {
        match digest.parse::<Digest>() {
            Ok(Digest::Sha256(sha256)) if Digest::sha256(sha256).to_string() == digest => {
                Ok(Self {
                    digest: Digest::sha256(sha256),
                    size_bytes,
                })
            }
            _ => Err(IntegrityParseError::Digest),
        }
    }

    pub fn digest(self) -> Digest {
        self.digest
    }
}

impl TryFrom<crate::proto::ContentIntegrity> for Integrity {
    type Error = IntegrityParseError;

    fn try_from(value: crate::proto::ContentIntegrity) -> Result<Self, Self::Error> {
        Self::parse(&value.digest, value.size_bytes)
    }
}

impl From<Integrity> for crate::proto::ContentIntegrity {
    fn from(value: Integrity) -> Self {
        Self {
            digest: value.digest().to_string(),
            size_bytes: value.size_bytes,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UploadReceiptError {
    #[error("upload receipt is not for the requested upload job {0}")]
    WrongJob(pplx_space_common_core::types::JobId),
    #[error("upload job {0} has not completed successfully")]
    Incomplete(pplx_space_common_core::types::JobId),
    #[error(
        "upload has no content receipt; retry backup from its retained source on upgraded workers"
    )]
    Missing,
    #[error("invalid upload integrity: {0}")]
    Invalid(#[from] IntegrityParseError),
    #[error("upload receipt length differs from completed transfer")]
    Size,
}

/// Read evidence from a completed upload, accepting streamed Btrfs uploads
/// only when that is one of the caller's expected transfer kinds.
pub fn completed_upload_integrity(
    job: &crate::proto::TransferJob,
    job_id: pplx_space_common_core::types::JobId,
    allow_btrfs_send: bool,
) -> Result<Integrity, UploadReceiptError> {
    if job.job_id != job_id.to_string()
        || !(job.kind == "push" || (allow_btrfs_send && job.kind == "push_btrfs_send"))
    {
        return Err(UploadReceiptError::WrongJob(job_id));
    }
    if job.status != crate::proto::TransferJobStatus::Completed as i32 {
        return Err(UploadReceiptError::Incomplete(job_id));
    }
    let receipt = job.integrity.as_ref().ok_or(UploadReceiptError::Missing)?;
    let integrity = Integrity::parse(&receipt.digest, receipt.size_bytes)?;
    if integrity.size_bytes != job.bytes_transferred {
        return Err(UploadReceiptError::Size);
    }
    Ok(integrity)
}

#[cfg(test)]
mod tests;
