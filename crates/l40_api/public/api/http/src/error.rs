use axum::{
    Json,
    extract::Request,
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_public_api::{PublicErrorCode, PublicErrorDetail, PublicErrorEnvelope};
use std::time::Duration;

use crate::request_id;

#[derive(Clone, Copy, Debug)]
struct PublicErrorResponse;

#[derive(Debug)]
pub struct PublicApiError {
    status: StatusCode,
    code: PublicErrorCode,
    message: String,
    retryable: bool,
    retry_after: Option<Duration>,
    details: Vec<PublicErrorDetail>,
    origin: ResponseOrigin,
    kind: ResponseKind,
}

/// The public error contract: for every stable `code`, the HTTP status the
/// native API answers with and whether the envelope says `retryable`.
///
/// This table is the single source of truth. Every [`PublicApiError`]
/// constructor derives its status and `retryable` from it (a test pins that),
/// the OpenAPI description repeats it, and the public API LLD documents it.
///
/// Client rule: when a response carries a valid envelope, `retryable` is
/// authoritative. Only when it does not (an intermediary answered) does the
/// status decide: 429 and 5xx are retryable, other 4xx are not. `Internal` is
/// retryable by default because a request that failed before doing anything is
/// safe to repeat; handlers that fail *after* their work committed clear it
/// with [`PublicApiError::not_retryable`]. `OperationFailed` only ever appears
/// as an operation's `terminal_error_code`, never as an HTTP status; it is
/// listed so the table is total.
///
/// `RateLimited` covers *both* cluster-activation-limiter rejections: an
/// exhausted shared activation bucket and a cluster with no start-capable node
/// (`ClusterAdmissionError::EmptyCluster`). Both carry `Retry-After`. The
/// empty-cluster case is deliberately not `CapacityUnavailable`: at admission
/// the API cannot tell a node roll from an outage, so it asks for backoff
/// rather than declaring capacity gone. `CapacityUnavailable` is reserved for
/// placement failing after a request was admitted.
///
/// `QuotaExceeded` is a create refused by the organization or per-user
/// sandbox limit ([`PublicApiError::sandbox_quota_exceeded`]). A 429 like
/// `RateLimited`, but not retryable and without `Retry-After`: the limit lifts
/// once a delete completes or an in-flight create ends, not after a delay.
///
/// The statuses here are what this API's own handlers answer. A rejection the
/// framework produces before a handler runs (405 unsupported method, 413 body
/// too large, 415 wrong `Content-Type`, 422 body not matching the schema) is
/// normalized by [`normalize_public_error_response`] into an envelope that
/// *keeps that status* while carrying `InvalidRequest` and `retryable: false`;
/// likewise any other unhandled 5xx keeps its status under `Internal`. The
/// OpenAPI and LLD contracts say the same, so generated clients expect them.
pub fn error_contract(code: &PublicErrorCode) -> (StatusCode, bool) {
    match code {
        PublicErrorCode::InvalidRequest => (StatusCode::BAD_REQUEST, false),
        PublicErrorCode::Unauthorized => (StatusCode::UNAUTHORIZED, false),
        PublicErrorCode::Forbidden => (StatusCode::FORBIDDEN, false),
        PublicErrorCode::NotFound => (StatusCode::NOT_FOUND, false),
        PublicErrorCode::Conflict | PublicErrorCode::IdempotencyConflict => {
            (StatusCode::CONFLICT, false)
        }
        PublicErrorCode::FailedPrecondition => (StatusCode::PRECONDITION_FAILED, false),
        PublicErrorCode::RateLimited => (StatusCode::TOO_MANY_REQUESTS, true),
        PublicErrorCode::QuotaExceeded => (StatusCode::TOO_MANY_REQUESTS, false),
        PublicErrorCode::CapacityUnavailable | PublicErrorCode::ServiceUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, true)
        }
        PublicErrorCode::Internal => (StatusCode::INTERNAL_SERVER_ERROR, true),
        PublicErrorCode::OperationFailed => (StatusCode::INTERNAL_SERVER_ERROR, false),
        // Only reachable for a code stored by a newer server; this build never
        // answers an HTTP request with it. Fail closed on retries.
        PublicErrorCode::Other(_) => (StatusCode::INTERNAL_SERVER_ERROR, false),
    }
}

impl PublicApiError {
    /// Build an error whose status and `retryable` come from
    /// [`error_contract`]; the named constructors only add classification.
    fn from_contract(
        code: PublicErrorCode,
        message: impl Into<String>,
        origin: ResponseOrigin,
        kind: ResponseKind,
    ) -> Self {
        let (status, retryable) = error_contract(&code);
        Self {
            status,
            code,
            message: message.into(),
            retryable,
            retry_after: None,
            details: Vec::new(),
            origin,
            kind,
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::Unauthorized,
            message,
            ResponseOrigin::Client,
            ResponseKind::Auth,
        )
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::Forbidden,
            message,
            ResponseOrigin::Client,
            ResponseKind::Auth,
        )
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::InvalidRequest,
            message,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::NotFound,
            message,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }

    pub fn capacity_unavailable(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::CapacityUnavailable,
            message,
            ResponseOrigin::Cluster,
            ResponseKind::Admission,
        )
    }

    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::RateLimited,
            message,
            ResponseOrigin::Cluster,
            ResponseKind::Admission,
        )
    }

    /// A create refused because the organization or the caller already holds
    /// as many sandboxes as its limit allows.
    pub fn sandbox_quota_exceeded(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::QuotaExceeded,
            message,
            ResponseOrigin::Client,
            ResponseKind::Admission,
        )
    }

    /// Clear the `retryable` flag, and any `Retry-After` with it.
    ///
    /// For a failure that happens *after* the request's own work committed,
    /// where the status has to stay a 500 — the caller cannot be told the
    /// operation is safe to send again when it already landed. `internal()`
    /// marks every 500 retryable, which is right for a request that did
    /// nothing and wrong for one that did everything.
    pub fn not_retryable(mut self) -> Self {
        self.retryable = false;
        self.retry_after = None;
        self
    }

    /// A `not_found` for a path inside a sandbox the caller owns, as opposed
    /// to a sandbox that does not exist or belongs to someone else.
    ///
    /// Only call this after the owner check passed: the detail confirms the
    /// sandbox exists, so the not-found answers for missing and unowned
    /// sandboxes must never carry it.
    pub fn path_not_found(message: impl Into<String>) -> Self {
        let mut error = Self::not_found(message);
        error.details.push(PublicErrorDetail {
            key: pplx_space_public_api::NOT_FOUND_RESOURCE_DETAIL_KEY.to_string(),
            value: pplx_space_public_api::NOT_FOUND_RESOURCE_PATH.to_string(),
        });
        error
    }

    /// An `invalid_request` that names the `pty` request field.
    pub fn invalid_pty_field(message: impl Into<String>) -> Self {
        let mut error = Self::bad_request(message);
        error.details.push(PublicErrorDetail {
            key: pplx_space_public_api::INVALID_REQUEST_FIELD_DETAIL_KEY.to_string(),
            value: pplx_space_public_api::INVALID_REQUEST_FIELD_PTY.to_string(),
        });
        error
    }

    /// A `conflict` that names why it was refused in a `reason` detail.
    pub fn conflict_with_reason(message: impl Into<String>, reason: &str) -> Self {
        let mut error = Self::conflict(message);
        error.details.push(PublicErrorDetail {
            key: pplx_space_public_api::CONFLICT_REASON_DETAIL_KEY.to_string(),
            value: reason.to_string(),
        });
        error
    }

    /// An `invalid_request` for a create or update field outside its limit,
    /// naming the field in `details`.
    pub fn request_limit(error: pplx_space_public_api::limits::RequestLimitError) -> Self {
        let mut rejected = Self::bad_request(error.to_string());
        rejected.details.push(PublicErrorDetail {
            key: pplx_space_public_api::INVALID_REQUEST_FIELD_DETAIL_KEY.to_string(),
            value: error.field().as_str().to_string(),
        });
        rejected
    }

    pub fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = Some(retry_after);
        self
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::Conflict,
            message,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }

    /// A `conflict` for work that needs a running sandbox, with the
    /// [`pplx_space_public_api::sandbox_not_running_details`].
    pub fn sandbox_not_running(
        sandbox_id: pplx_space_public_api::PublicSandboxId,
        state: &pplx_space_public_api::SandboxLifecycleState,
    ) -> Self {
        let mut error = Self::conflict(format!("sandbox is {}, not running", state.as_str()));
        error.details = pplx_space_public_api::sandbox_not_running_details(sandbox_id, state);
        error
    }

    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::FailedPrecondition,
            message,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }

    /// A statement lost a race for a lock and the request can just be retried.
    ///
    /// The public surface's idempotency guards and the fork-source lifecycle
    /// guard are built on `pg_advisory_xact_lock`, whose whole job is to wait
    /// for a peer transaction. Under a role-level `lock_timeout` (see
    /// `db/README.md`) that wait aborts with `55P03` instead of blocking, and
    /// the serialization is still correct — the caller simply has to come back.
    /// Reporting it as a 500 says the opposite, and at the ECW boundary a 500
    /// and a 503 are not interchangeable.
    ///
    /// `ResponseOrigin::Api` rather than `Cluster`: the contention is in this
    /// service's own database, not in the sandbox fleet.
    pub fn lock_contention() -> Self {
        Self::from_contract(
            PublicErrorCode::ServiceUnavailable,
            "operation conflicted with a concurrent change; retry the request",
            ResponseOrigin::Api,
            ResponseKind::Internal,
        )
        .with_retry_after(LOCK_CONTENTION_RETRY_AFTER)
    }

    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::ServiceUnavailable,
            message,
            ResponseOrigin::Cluster,
            ResponseKind::RouteUnavailable,
        )
    }

    pub fn idempotency_conflict(message: impl Into<String>) -> Self {
        Self::from_contract(
            PublicErrorCode::IdempotencyConflict,
            message,
            ResponseOrigin::Client,
            ResponseKind::Validation,
        )
    }

    pub fn internal() -> Self {
        Self::from_contract(
            PublicErrorCode::Internal,
            "internal server error",
            ResponseOrigin::Api,
            ResponseKind::Internal,
        )
    }

    /// The status this error answers with and how its response is classified.
    pub fn classification(&self) -> (StatusCode, ResponseOrigin, ResponseKind) {
        (self.status, self.origin, self.kind)
    }

    pub fn with_classification(mut self, origin: ResponseOrigin, kind: ResponseKind) -> Self {
        self.origin = origin;
        self.kind = kind;
        self
    }

    fn into_envelope(self) -> PublicErrorEnvelope {
        PublicErrorEnvelope {
            code: self.code,
            message: self.message,
            request_id: request_id::current_request_id(),
            retryable: self.retryable,
            details: self.details,
        }
    }

    fn unhandled_response(status: StatusCode) -> Self {
        let mut error = match status {
            StatusCode::UNAUTHORIZED => Self::unauthorized("unauthorized"),
            StatusCode::FORBIDDEN => Self::forbidden("forbidden"),
            StatusCode::NOT_FOUND => Self::not_found("route not found"),
            StatusCode::CONFLICT => Self::conflict("request conflicts with current state"),
            StatusCode::PRECONDITION_FAILED => {
                Self::failed_precondition("request precondition failed")
            }
            StatusCode::TOO_MANY_REQUESTS => Self::rate_limited("request rate limit exceeded"),
            StatusCode::SERVICE_UNAVAILABLE => Self::service_unavailable("service unavailable"),
            status if status.is_client_error() => Self::bad_request("invalid request"),
            _ => Self::internal(),
        };
        error.status = status;
        error
    }
}

pub async fn normalize_public_error_response(request: Request, next: Next) -> Response {
    normalize_public_error(next.run(request).await)
}

/// What [`normalize_public_error_response`] does to one response, for a
/// server that cannot run it as a layer: a framework rejection or another
/// error status that is not already a Native envelope becomes one, keeping
/// its status and its `Allow`, `Retry-After` and `WWW-Authenticate` headers.
pub fn normalize_public_error(response: Response) -> Response {
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error())
        || response.extensions().get::<PublicErrorResponse>().is_some()
    {
        return response;
    }

    let allow = response.headers().get(header::ALLOW).cloned();
    let retry_after = response.headers().get(header::RETRY_AFTER).cloned();
    let www_authenticate = response.headers().get(header::WWW_AUTHENTICATE).cloned();
    let mut normalized = PublicApiError::unhandled_response(status).into_response();
    if let Some(allow) = allow {
        normalized.headers_mut().insert(header::ALLOW, allow);
    }
    if let Some(retry_after) = retry_after {
        normalized
            .headers_mut()
            .insert(header::RETRY_AFTER, retry_after);
    }
    if let Some(www_authenticate) = www_authenticate {
        normalized
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, www_authenticate);
    }
    normalized
}

pub async fn public_route_not_found() -> PublicApiError {
    PublicApiError::not_found("route not found")
}

pub fn with_retry_after_hint(
    error: PublicApiError,
    retry_after: Option<Duration>,
) -> PublicApiError {
    match retry_after {
        Some(retry_after) => error.with_retry_after(retry_after),
        None => error,
    }
}

impl IntoResponse for PublicApiError {
    fn into_response(self) -> Response {
        let status = self.status;
        let retry_after = self.retry_after;
        let origin = self.origin;
        let kind = self.kind;
        pplx_space_metrics::response::public_api(origin, kind, status.as_u16());
        tracing::Span::current().record("space.response_origin", origin.as_str());
        tracing::Span::current().record("space.response_kind", kind.as_str());
        let envelope = self.into_envelope();
        let mut response = (status, Json(envelope)).into_response();
        response.extensions_mut().insert(PublicErrorResponse);
        if let Some(retry_after) = retry_after
            && let Some(value) = retry_after_header_value(retry_after)
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }
}

/// The `Retry-After` of [`PublicApiError::lock_contention`]: one second, the
/// smallest delay the header can express.
pub const LOCK_CONTENTION_RETRY_AFTER: Duration = Duration::from_secs(1);

/// `Retry-After` for `retry_after`, in whole seconds rounded up and at least 1.
pub fn retry_after_header_value(retry_after: Duration) -> Option<header::HeaderValue> {
    header::HeaderValue::from_str(
        &retry_after
            .as_secs()
            .saturating_add(u64::from(retry_after.subsec_nanos() > 0))
            .max(1)
            .to_string(),
    )
    .ok()
}

#[cfg(test)]
mod tests;
