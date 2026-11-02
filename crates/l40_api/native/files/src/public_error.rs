//! The Native public error each file request failure answers with.

use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_public_api_http::{PublicApiError, with_retry_after_hint};

use crate::{FileCallError, FileCallErrorKind, FileRequestError, FingerprintError};

impl From<FingerprintError> for PublicApiError {
    fn from(_: FingerprintError) -> Self {
        Self::internal()
    }
}

impl From<FileRequestError> for PublicApiError {
    fn from(error: FileRequestError) -> Self {
        Self::bad_request(error.to_string())
    }
}

impl From<FileCallError> for PublicApiError {
    fn from(error: FileCallError) -> Self {
        let public = match error.kind {
            FileCallErrorKind::InvalidRequest(message) => Self::bad_request(message),
            FileCallErrorKind::Forbidden(message) => Self::forbidden(message),
            FileCallErrorKind::PathNotFound(message) => Self::path_not_found(message),
            FileCallErrorKind::NotFound(message) => Self::not_found(message),
            FileCallErrorKind::Conflict(message) => Self::conflict(message),
            FileCallErrorKind::FailedPrecondition(message) => Self::failed_precondition(message),
            FileCallErrorKind::RateLimited {
                message,
                retry_after,
            } => with_retry_after_hint(
                Self::rate_limited(message)
                    .with_classification(ResponseOrigin::Node, ResponseKind::Admission),
                retry_after,
            ),
            FileCallErrorKind::Unavailable {
                message,
                retry_after,
            } => with_retry_after_hint(
                Self::service_unavailable(message)
                    .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus),
                retry_after,
            ),
            FileCallErrorKind::TimedOut { retry_after } => with_retry_after_hint(
                Self::service_unavailable("sandbox did not answer in time")
                    .with_classification(ResponseOrigin::Node, ResponseKind::Timeout),
                retry_after,
            ),
            FileCallErrorKind::BackendFailed => Self::internal()
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus),
            FileCallErrorKind::LockContention => Self::lock_contention(),
            FileCallErrorKind::Internal => Self::internal(),
        };
        if error.not_retryable {
            public.not_retryable()
        } else {
            public
        }
    }
}
