//! Credentials and key-wrapping helpers used by vol-mgr's transfer
//! pipeline. Three concerns are kept deliberately distinct here so
//! they can evolve independently:
//!
//! - **Store-access credentials**: `exchange_credentials` mints
//!   short-lived AWS STS credentials scoped to one bucket, returned
//!   as [`ObjectCredentials`]. These authorize *who* can read/write
//!   bytes at a destination.
//! - **Artifact-encryption keys**: `issue_dek` / `unwrap_dek` mint and
//!   recover DEKs wrapped under a per-org KEK, used to encrypt the
//!   artifact bytes themselves (`TransferEncryption` carries the
//!   wrap; the plaintext DEK lives in [`Zeroizing<Vec<u8>>`]). These
//!   authorize *the bytes themselves*, independently of where
//!   they're stored.
//!
//! Store-side server protection (e.g. S3 SSE-KMS) and protected-mount
//! activation grants are separate concerns from both of the above and
//! are not modeled in this module; they would be added to the object
//! store and the filesystem layer respectively, not folded into a
//! single "encryption" type here.

use pplx_space_common_core::types::{KekProvider, MasterKeyIdentifier, OrgId, SubjectId};
use pplx_space_credmgr_client::{
    AWS_CREDENTIALS_TOKEN_TYPE, STORAGE_READ_SCOPE, STORAGE_SESSION_TTL_SECONDS,
    STORAGE_TOKEN_AUDIENCE, STORAGE_WRITE_SCOPE, SUBJECT_ID_TOKEN_TYPE, TOKEN_EXCHANGE_GRANT_TYPE,
    proto::{
        DekSpec, ExchangeTokenRequest, IssueDekRequest, UnwrapDekRequest, exchange_token_response,
    },
};
use pplx_space_object_store::ObjectCredentials;
use tonic::{Code, Status};
use zeroize::Zeroizing;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageAccess {
    Read,
    ReadWrite,
}

impl StorageAccess {
    fn scope(self) -> String {
        match self {
            Self::Read => STORAGE_READ_SCOPE.to_string(),
            Self::ReadWrite => format!("{STORAGE_READ_SCOPE} {STORAGE_WRITE_SCOPE}"),
        }
    }
}

/// Exchange a SPACE subject for short-lived AWS credentials scoped to one
/// exact object or canonical object prefix.
pub async fn exchange_credentials(
    bucket: &str,
    key: &str,
    is_prefix: bool,
    subject: &SubjectId,
    access: StorageAccess,
) -> Result<ObjectCredentials, Status> {
    let resource = if is_prefix {
        format!("arn:aws:s3:::{bucket}/{key}/*")
    } else {
        format!("arn:aws:s3:::{bucket}/{key}")
    };
    let mut client = pplx_space_credmgr_client::connect_from_env()
        .await
        .map_err(|e| Status::internal(format!("credmgr connect failed: {e}")))?;
    let mut resp = client
        .exchange_token(ExchangeTokenRequest {
            grant_type: TOKEN_EXCHANGE_GRANT_TYPE.to_string(),
            resource,
            audience: Some(STORAGE_TOKEN_AUDIENCE.to_string()),
            scope: Some(access.scope()),
            requested_token_type: AWS_CREDENTIALS_TOKEN_TYPE.to_string(),
            subject_token: Some(subject.to_string()),
            subject_token_type: Some(SUBJECT_ID_TOKEN_TYPE.to_string()),
            actor_token: None,
            actor_token_type: None,
            requested_expires_in: Some(STORAGE_SESSION_TTL_SECONDS),
        })
        .await
        .map_err(|e| Status::new(e.code(), format!("credmgr exchange_token failed: {e}")))?
        .into_inner();
    match resp.payload.as_mut() {
        Some(exchange_token_response::Payload::AwsCredentials(c)) => Ok(ObjectCredentials {
            access_key_id: std::mem::take(&mut c.access_key_id),
            secret_access_key: std::mem::take(&mut c.secret_access_key),
            session_token: std::mem::take(&mut c.session_token),
        }),
        _ => Err(Status::internal(
            "credmgr returned unexpected payload type; expected AwsCredentials",
        )),
    }
}

/// Errors surfaced by [`unwrap_dek`].
///
/// Split from [`tonic::Status`] so the executor can map permanent
/// credmgr-side rejections (bad provider, AAD mismatch, malformed
/// wrap) onto [`pplx_space_common_jobs::JobError::Permanent`] and
/// transport blips onto `Retriable`. The credmgr's own gRPC code
/// taxonomy informs this split: `InvalidArgument`, `FailedPrecondition`,
/// and `PermissionDenied` are caller-visible mistakes that won't resolve
/// by re-asking, while transport-class failures will.
#[derive(Debug, thiserror::Error)]
pub enum UnwrapDekError {
    /// Connection to the credmgr socket failed (UDS missing, peer
    /// not running, etc). Transient; the executor retries.
    #[error("credmgr connect: {0}")]
    Connect(String),
    /// `gRPC UnwrapDek` returned a permanent rejection — wrong AAD,
    /// unknown KEK, malformed wrap, unsupported provider. Re-asking
    /// will keep failing.
    #[error("credmgr unwrap rejected: {code:?}: {message}")]
    Permanent { code: Code, message: String },
    /// `gRPC UnwrapDek` returned a transient failure (unavailable,
    /// internal error, etc).
    #[error("credmgr unwrap transient: {code:?}: {message}")]
    Transient { code: Code, message: String },
}

/// Recover the plaintext DEK previously produced by credmgr `IssueDek`.
///
/// The returned bytes live inside [`Zeroizing`] so dropping the value
/// scrubs the memory — callers should keep the binding short-lived and
/// drop it immediately after the encrypt/decrypt call completes.
///
/// `organization_id`, `provider`, and `master_key_identifier` are
/// rendered to their canonical wire form (each newtype's `Display`)
/// and forwarded to credmgr; credmgr re-parses them at its own
/// boundary for defense in depth.
/// Errors surfaced by [`issue_dek`]. Same Permanent / Transient split
/// as [`UnwrapDekError`] so the caller can map onto the right
/// `JobError` (or in the session case, `Status`) variant.
#[derive(Debug, thiserror::Error)]
pub enum IssueDekError {
    #[error("credmgr connect: {0}")]
    Connect(String),
    #[error("credmgr issue rejected: {code:?}: {message}")]
    Permanent { code: Code, message: String },
    #[error("credmgr issue transient: {code:?}: {message}")]
    Transient { code: Code, message: String },
}

/// Output of [`issue_dek`]. The plaintext DEK is wrapped in
/// `Zeroizing<Vec<u8>>` so its bytes are scrubbed from heap memory when
/// the value (or the field) is dropped, not merely released back to the
/// allocator. Consumers that don't need the plaintext (e.g. batched
/// `PushFilesAsync` which re-unwraps at execute time) can drop the whole
/// `IssuedDek` and rely on the `Drop` impl rather than scrubbing
/// manually.
pub struct IssuedDek {
    pub plaintext_dek: Zeroizing<Vec<u8>>,
    pub wrapped_dek: Vec<u8>,
}

/// Mint a fresh DEK + wrap it under the named per-org KEK. Symmetric
/// to [`unwrap_dek`]; same UDS connection pattern, same error
/// taxonomy. The caller is responsible for zeroising the returned
/// `plaintext_dek` after use (typically by moving it into a
/// `Zeroizing<Vec<u8>>`).
pub async fn issue_dek(
    organization_id: OrgId,
    provider: KekProvider,
    master_key_identifier: &MasterKeyIdentifier,
    aad: &[u8],
) -> Result<IssuedDek, IssueDekError> {
    let mut client = pplx_space_credmgr_client::connect_from_env()
        .await
        .map_err(|e| IssueDekError::Connect(e.to_string()))?;
    let mut resp = client
        .issue_dek(IssueDekRequest {
            organization_id: organization_id.to_string(),
            provider: provider.to_string(),
            master_key_identifier: master_key_identifier.to_string(),
            spec: DekSpec::Aes256 as i32,
            aad: aad.to_vec(),
        })
        .await
        .map_err(|status| {
            let code = status.code();
            let message = status.message().to_string();
            if matches!(
                code,
                Code::InvalidArgument
                    | Code::NotFound
                    | Code::PermissionDenied
                    | Code::Unauthenticated
                    | Code::FailedPrecondition,
            ) {
                IssueDekError::Permanent { code, message }
            } else {
                IssueDekError::Transient { code, message }
            }
        })?
        .into_inner();
    Ok(IssuedDek {
        plaintext_dek: Zeroizing::new(std::mem::take(&mut resp.plaintext_dek)),
        wrapped_dek: std::mem::take(&mut resp.wrapped_dek),
    })
}

pub async fn unwrap_dek(
    organization_id: OrgId,
    provider: KekProvider,
    master_key_identifier: &MasterKeyIdentifier,
    wrapped_dek: &[u8],
    aad: &[u8],
) -> Result<Zeroizing<Vec<u8>>, UnwrapDekError> {
    let mut client = pplx_space_credmgr_client::connect_from_env()
        .await
        .map_err(|e| UnwrapDekError::Connect(e.to_string()))?;
    let mut resp = client
        .unwrap_dek(UnwrapDekRequest {
            organization_id: organization_id.to_string(),
            provider: provider.to_string(),
            master_key_identifier: master_key_identifier.to_string(),
            wrapped_dek: wrapped_dek.to_vec(),
            aad: aad.to_vec(),
        })
        .await
        .map_err(|status| {
            let code = status.code();
            let message = status.message().to_string();
            if matches!(
                code,
                Code::InvalidArgument
                    | Code::NotFound
                    | Code::PermissionDenied
                    | Code::Unauthenticated
                    | Code::FailedPrecondition,
            ) {
                UnwrapDekError::Permanent { code, message }
            } else {
                UnwrapDekError::Transient { code, message }
            }
        })?
        .into_inner();
    Ok(Zeroizing::new(std::mem::take(&mut resp.plaintext_dek)))
}

#[cfg(test)]
mod tests;
