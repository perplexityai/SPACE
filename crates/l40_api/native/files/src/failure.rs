//! How a failed sandbox call for a Native file operation reaches the client.

use std::{fmt, time::Duration};

use http::StatusCode;

/// A failed call to the sandbox, as [`file_call_error`] reads it.
pub trait FileCallFailure: fmt::Display {
    /// The HTTP status the failure stands for.
    fn status(&self) -> StatusCode;
    /// The retry hint the failure carries, if any.
    fn retry_after(&self) -> Option<Duration>;
    /// The guest reported the requested path missing.
    fn is_guest_path_not_found(&self) -> bool;
    /// The backend refused the request before doing any of its work.
    fn is_refused_before_start(&self) -> bool;
    /// A statement in the answering server's own database lost a lock race.
    fn is_lock_contention(&self) -> bool;
}

/// A worker's gRPC status, read as an HTTP failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFailure {
    status: StatusCode,
    message: String,
    retry_after: Option<Duration>,
    refused_before_start: bool,
    guest_path_not_found: bool,
}

impl WorkerFailure {
    pub fn from_status(status: &tonic::Status) -> Self {
        if is_guest_storage_full(status) {
            // Retrying cannot succeed until something frees space in the
            // sandbox, so this must not surface as a retryable rate limit.
            return Self {
                status: StatusCode::PRECONDITION_FAILED,
                message: format!("sandbox storage is full: {}", status.message()),
                retry_after: None,
                refused_before_start: false,
                guest_path_not_found: false,
            };
        }
        // The backend's own message, not tonic's `code: …, message: …`
        // rendering, and its retry hint (a refusal made before anything
        // started carries one) as `Retry-After`.
        Self {
            status: http_status_for_grpc_code(status.code()),
            message: status.message().to_owned(),
            retry_after: pplx_space_common_grpc::retry_after(status)
                .map(|try_again| try_again.clamp(Duration::from_secs(1), Duration::from_secs(60))),
            refused_before_start: pplx_space_common_grpc::is_refused_before_start(status),
            guest_path_not_found: pplx_space_common_grpc::is_guest_path_not_found(status),
        }
    }

    pub fn into_message(self) -> String {
        self.message
    }
}

impl fmt::Display for WorkerFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl FileCallFailure for WorkerFailure {
    fn status(&self) -> StatusCode {
        self.status
    }

    fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    fn is_guest_path_not_found(&self) -> bool {
        self.guest_path_not_found
    }

    fn is_refused_before_start(&self) -> bool {
        self.refused_before_start
    }

    fn is_lock_contention(&self) -> bool {
        false
    }
}

/// Whether a guest filesystem call failed because the sandbox's disk or quota
/// is full, as the guest marks it.
fn is_guest_storage_full(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::ResourceExhausted
        && status
            .metadata()
            .get(pplx_space_common_core::spaced::ERROR_REASON_METADATA_KEY)
            .and_then(|reason| reason.to_str().ok())
            == Some(pplx_space_common_core::spaced::GUEST_STORAGE_FULL)
}

fn http_status_for_grpc_code(code: tonic::Code) -> StatusCode {
    match code {
        tonic::Code::Ok => StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::Cancelled => StatusCode::REQUEST_TIMEOUT,
        tonic::Code::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::AlreadyExists => StatusCode::CONFLICT,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        tonic::Code::Aborted => StatusCode::CONFLICT,
        tonic::Code::OutOfRange => StatusCode::BAD_REQUEST,
        tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        tonic::Code::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
    }
}

/// What a Native file operation answers for a failed sandbox call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCallError {
    pub kind: FileCallErrorKind,
    /// The operation may have taken effect, so the answer must not invite a
    /// retry; see [`unprotected_mutation_error`].
    pub not_retryable: bool,
}

/// The public error a failed file call becomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileCallErrorKind {
    InvalidRequest(String),
    Forbidden(String),
    /// The guest's own missing-path answer, inside a sandbox the caller owns.
    PathNotFound(String),
    NotFound(String),
    Conflict(String),
    FailedPrecondition(String),
    RateLimited {
        message: String,
        retry_after: Option<Duration>,
    },
    Unavailable {
        message: String,
        retry_after: Option<Duration>,
    },
    /// The sandbox did not answer within the budget; the operation may still
    /// complete, so mutations retry only with the same key.
    TimedOut {
        retry_after: Option<Duration>,
    },
    /// The sandbox answered with another server error.
    BackendFailed,
    /// A lock race in the answering server's own database, not the sandbox
    /// fleet.
    LockContention,
    Internal,
}

impl From<FileCallErrorKind> for FileCallError {
    fn from(kind: FileCallErrorKind) -> Self {
        Self {
            kind,
            not_retryable: false,
        }
    }
}

/// The public error for a failed sandbox call made after the caller's
/// ownership of the sandbox was checked.
pub fn file_call_error(failure: &impl FileCallFailure) -> FileCallError {
    // A lock race is this service's own database, not the sandbox fleet. The
    // status match below cannot tell it from an out-of-capacity 503, so it
    // would attribute it to the cluster or the node.
    if failure.is_lock_contention() {
        return FileCallErrorKind::LockContention.into();
    }
    let retry_after = failure.retry_after();
    let kind = match failure.status() {
        StatusCode::BAD_REQUEST => FileCallErrorKind::InvalidRequest(failure.to_string()),
        StatusCode::FORBIDDEN => FileCallErrorKind::Forbidden(failure.to_string()),
        // Every caller checked ownership before reaching the sandbox, so only
        // the guest's own missing-path answer is told apart from the rest.
        StatusCode::NOT_FOUND if failure.is_guest_path_not_found() => {
            FileCallErrorKind::PathNotFound(failure.to_string())
        }
        StatusCode::NOT_FOUND => FileCallErrorKind::NotFound(failure.to_string()),
        StatusCode::CONFLICT => FileCallErrorKind::Conflict(failure.to_string()),
        StatusCode::PRECONDITION_FAILED => {
            FileCallErrorKind::FailedPrecondition(failure.to_string())
        }
        StatusCode::TOO_MANY_REQUESTS => FileCallErrorKind::RateLimited {
            message: failure.to_string(),
            retry_after,
        },
        StatusCode::SERVICE_UNAVAILABLE => FileCallErrorKind::Unavailable {
            message: failure.to_string(),
            retry_after,
        },
        StatusCode::GATEWAY_TIMEOUT | StatusCode::REQUEST_TIMEOUT => {
            FileCallErrorKind::TimedOut { retry_after }
        }
        status if status.is_server_error() => {
            tracing::warn!(error = %failure, "native file operation failed");
            FileCallErrorKind::BackendFailed
        }
        _ => FileCallErrorKind::Internal,
    };
    kind.into()
}

/// For a mutation whose `Idempotency-Key` the sandbox does not see (mkdir,
/// remove, file-change tracking), a failure that may have left the change
/// applied must not invite a blind retry: a repeated remove can delete a file
/// recreated in between. A server error or timeout stays retryable only when
/// the backend said it refused the request before doing anything; a retry
/// hint is not that evidence. A 429, from any layer, is answered before work.
pub fn unprotected_mutation_error(failure: &impl FileCallFailure) -> FileCallError {
    let ambiguous = !failure.is_refused_before_start()
        && (failure.status().is_server_error() || failure.status() == StatusCode::REQUEST_TIMEOUT);
    let mut error = file_call_error(failure);
    error.not_retryable = ambiguous;
    error
}
