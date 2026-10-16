pub mod auto_lifecycle;
pub mod sandbox;
pub mod substrate;
pub mod template;

#[cfg(test)]
mod test_fixtures;

use std::{fmt, time::Duration};

use axum::{
    Json,
    http::{StatusCode, header},
    response::IntoResponse,
};

#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct ControllerError {
    status: StatusCode,
    message: String,
    kind: ControllerErrorKind,
    #[source]
    transport_source:
        Option<std::sync::Arc<pplx_space_common_grpc::internal_transport::TransportError>>,
    /// When set, the response carries a `Retry-After: <secs>` header AND marks
    /// this error as a "converging, retry the same resource" transient outcome.
    /// The ECW boundary keys the E2B SDK's recoverable `sandbox_timeout` (502)
    /// shape off this marker, so only set it for waits whose underlying work is
    /// still progressing in the background (create/restore/wake exhausting
    /// their ALB-aware budget) — not for genuine overload/quota 503s, where a
    /// resume+retry would be wrong.
    retry_after: Option<Duration>,
    /// Header-only retry hint for overload/quota or unpublished restore
    /// sources. This renders `Retry-After` but does not assert a converging
    /// sandbox operation for ECW translation.
    retry_after_header: Option<Duration>,
    retry_after_cause: Option<ControllerRetryAfterCause>,
    /// The backend refused this request before doing any of its work, as its
    /// status said (`pplx_space_common_grpc::is_refused_before_start`).
    refused_before_start: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControllerErrorKind {
    General,
    SandboxAlreadyExists,
    RestoreContextUnavailable,
    /// A database statement lost a race for a lock. Carried so the public and
    /// ECW translators can keep the 503 attributed to this service's own
    /// database: they otherwise re-label every controller 503 as a fleet
    /// condition — capacity-unavailable/`Cluster` or backend-status/`Node` —
    /// which blames the sandbox fleet for a Postgres lock race and puts the
    /// wrong `origin` on the response metrics.
    LockContention,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerRetryAfterCause {
    RestoreInFlight,
    /// Restore cannot start until a source is published or recovered. Unlike
    /// RestoreInFlight this does not assert an active restore owner.
    RestoreSourcePending,
    PodNeverReady,
    ProxyUnreachable,
    GuestNotReady,
    /// Cluster-wide sandbox-activation admission shed this request (429 + a
    /// header-only `Retry-After`). Distinct from a converging reconnect wait:
    /// the async dispatcher keys backpressure off this cause so it re-queues
    /// behind the limiter without spending its transient-error retry budget.
    ClusterActivationAdmission,
    /// A create admitted against a warm-pool slot is waiting for the pool to
    /// collect a slot that still holds its sandbox ID. Workflow progress, not
    /// a failure: the async dispatcher re-queues it without spending its
    /// transient-error retry budget.
    PoolSlotRelease,
}

impl ControllerError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            kind: ControllerErrorKind::General,
            transport_source: None,
            retry_after: None,
            retry_after_header: None,
            retry_after_cause: None,
            refused_before_start: false,
        }
    }

    /// Whether the backend said it refused this request before doing any of
    /// its work. Only a backend status sets it; a retry hint alone does not.
    pub fn is_refused_before_start(&self) -> bool {
        self.refused_before_start
    }

    fn node_api_transport(
        error: pplx_space_common_grpc::internal_transport::TransportError,
    ) -> Self {
        let mut result = Self::service_unavailable(error.to_string());
        result.transport_source = Some(std::sync::Arc::new(error));
        result
    }

    /// Attach a `Retry-After` hint, signalling a retryable transient outcome.
    pub fn with_retry_after(mut self, retry_after: Duration) -> Self {
        self.retry_after = Some(retry_after);
        self
    }

    /// Attach an HTTP `Retry-After` hint without signalling ECW convergence.
    pub fn with_retry_after_header(mut self, retry_after: Duration) -> Self {
        self.retry_after_header = Some(retry_after);
        self
    }

    pub fn with_retry_after_cause(mut self, cause: ControllerRetryAfterCause) -> Self {
        self.retry_after_cause = Some(cause);
        self
    }

    /// Strip the retryable disposition from an error raised *after*
    /// irreversible work landed.
    ///
    /// This is the path-level form of the exception documented on
    /// [`db_contention_error`], and it exists because the statement-level form
    /// does not hold. Once an external side effect has fired — a warm slot
    /// adopted on a worker, a pod created and readied — *every* later statement
    /// on that path shares the hazard, including the transaction's own
    /// `begin` and `commit`. Exempting the one statement that looked most
    /// obviously post-side-effect left its five siblings in the same
    /// transaction still telling the caller to retry, which is the same
    /// duplicate-provisioning bug reached one statement over.
    ///
    /// So callers apply this once, where they know the side effect landed,
    /// rather than reasoning per statement. [`Self::without_retry_after`] is
    /// not sufficient: it clears the hints but leaves
    /// [`ControllerErrorKind::LockContention`] set, and the public and ECW
    /// translators rebuild a retryable 503 from the kind alone.
    pub(crate) fn demoted_after_side_effect(mut self) -> Self {
        if matches!(self.kind, ControllerErrorKind::LockContention) {
            self.kind = ControllerErrorKind::General;
            self.status = StatusCode::INTERNAL_SERVER_ERROR;
        }
        self.retry_after = None;
        self.retry_after_header = None;
        self.retry_after_cause = None;
        self
    }

    /// Drop any `Retry-After` / "converging, retry the same resource" marker.
    /// Used where a timeout did NOT leave work converging — e.g. a create that
    /// rolled back and deleted the sandbox — so the client isn't told to
    /// retry/resume a sandbox that no longer exists.
    pub fn without_retry_after(mut self) -> Self {
        self.retry_after = None;
        self.retry_after_header = None;
        self.retry_after_cause = None;
        self
    }

    /// The `Retry-After` hint, if this error is a retryable transient outcome.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// The HTTP/client retry hint, including header-only prerequisite hints.
    pub fn retry_after_header(&self) -> Option<Duration> {
        self.retry_after_header.or(self.retry_after)
    }

    pub fn retry_after_cause(&self) -> Option<ControllerRetryAfterCause> {
        self.retry_after_cause
    }

    /// HTTP status this error maps to.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Mark this error as a lock race, for tests that exercise a surface
    /// translator without standing up Postgres to produce a real `55P03`.
    ///
    /// Replaces the whole disposition with [`Self::lock_contention`]'s,
    /// keeping only the caller's message, rather than re-listing the fields it
    /// sets. An earlier version assigned `kind` and `retry_after_header` by
    /// hand and had already drifted once — marking only the kind produced an
    /// error that claimed to be a lock race while lacking the header, which
    /// silently passed a test asserting the header's absence. Delegating means
    /// a field added to `lock_contention` cannot leave the fixtures asserting
    /// against a shape production never builds.
    #[cfg(test)]
    pub(crate) fn mark_lock_contention_for_test(&mut self) {
        let message = std::mem::take(&mut self.message);
        *self = Self::lock_contention();
        self.message = message;
    }

    /// Whether this is a database lock race rather than a fleet condition.
    ///
    /// The surface translators need it because they key off the status code,
    /// and a contention 503 is indistinguishable from an out-of-capacity 503
    /// without it.
    pub fn is_lock_contention(&self) -> bool {
        matches!(self.kind, ControllerErrorKind::LockContention)
    }

    pub(crate) fn is_restore_context_unavailable(&self) -> bool {
        matches!(self.kind, ControllerErrorKind::RestoreContextUnavailable)
    }

    pub(crate) fn restore_context_unavailable(message: impl Into<String>) -> Self {
        let mut error = Self::precondition_failed(message);
        error.kind = ControllerErrorKind::RestoreContextUnavailable;
        error
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
    }

    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, message)
    }

    /// A 503 for a statement that lost a race for a database lock, marked
    /// [`ControllerErrorKind::LockContention`] so the public and ECW
    /// translators keep the attribution on this service's database.
    ///
    /// The client-facing message carries no database detail on purpose: a
    /// caller cannot act on a SQLSTATE, and the `warn` at the classification
    /// site has it.
    pub(crate) fn lock_contention() -> Self {
        let mut error = Self::service_unavailable(
            "operation conflicted with a concurrent change; retry the request",
        )
        // Header-only, not [`Self::with_retry_after`], because that marker
        // means "work is still converging in the background, retry the same
        // resource" and a lost lock race left nothing converging. The pacing
        // is still wanted: an un-hinted retryable 503 replaces one sub-second
        // lock wait with a burst of round-trips against the row that is
        // already contended.
        //
        // Be precise about what this does *not* rest on. Neither production
        // reader of `retry_after()` can observe the difference for a
        // contention error today: `sandbox::reconnect_controller_error_outcome`
        // and `translate::controller_error` each return on
        // `is_lock_contention()` before reaching their `retry_after` branch.
        // So this is the semantically correct choice rather than a load-bearing
        // one — and it is one deletion away from being load-bearing, since
        // removing either guard would route every contention error into
        // `reason:wait_budget` or
        // into the SDK's `sandbox_timeout`. That is what the assertions in
        // `controllers::tests` and `sandbox::reconnect_metrics_tests` pin.
        .with_retry_after_header(substrate::LOCK_CONTENTION_RETRY_AFTER);
        error.kind = ControllerErrorKind::LockContention;
        error
    }

    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, message)
    }

    pub(crate) fn sandbox_already_exists() -> Self {
        let mut error = Self::conflict("sandbox already exists");
        error.kind = ControllerErrorKind::SandboxAlreadyExists;
        error
    }

    pub(crate) const fn is_sandbox_already_exists(&self) -> bool {
        matches!(self.kind, ControllerErrorKind::SandboxAlreadyExists)
    }

    pub fn precondition_failed(message: impl Into<String>) -> Self {
        Self::new(StatusCode::PRECONDITION_FAILED, message)
    }
}

pub(crate) fn internal_error(message: &'static str, error: impl fmt::Display) -> ControllerError {
    ControllerError::internal(format!("{message}: {error}"))
}

/// [`internal_error`]'s contention-aware sibling, for statements that acquire a
/// lock.
///
/// The application logins can carry a role-level `lock_timeout` (see
/// `db/README.md`), so a statement that waits for a lock — a
/// `SELECT ... FOR UPDATE` guard, an advisory lock, or a plain read that
/// collided with DDL on its table — aborts with `55P03` where it previously
/// blocked until the peer transaction committed. That is a lost race, not a
/// failure: the identical statement succeeds on retry. Reporting it as a 500
/// tells the caller the opposite, and at the ECW boundary a 500 and a 503 are
/// not interchangeable.
///
/// Use this wherever a statement can block on another transaction. That is
/// wider than `FOR UPDATE`: an `INSERT ... ON CONFLICT` waits on a concurrent
/// unfinished insert of the same key, and an ordinary `UPDATE`/`DELETE` takes a
/// row lock — all three raise `55P03` the same way, and only the first
/// announces itself in the SQL.
///
/// Strictly, then, *every* statement can block: a plain `SELECT` blocks when
/// DDL holds `ACCESS EXCLUSIVE` on its table. The call sites that produce a
/// [`ControllerError`] are converted accordingly, so the plain constructors
/// here are left for the errors that are not database errors at all — a `tonic::Status`
/// from a node RPC, a malformed stored id, a failed parse, an out-of-range
/// integer.
///
/// The one remaining class of `sqlx` error that still takes a plain
/// constructor is the post-side-effect exception described below, where
/// reporting retryable would tell the caller to provision a second sandbox or
/// replay a completed mutation. Those are deliberate, and each says so at its
/// call site.
///
/// That is a claim about *this* error type, not about the crate: several
/// `/admin` and `/debug` handlers carry their own error shapes and still answer
/// a contention SQLSTATE with a 500. Reaching for a plain constructor on an
/// unguarded `sqlx::Error` is a call site that will report a lost lock race as
/// permanent.
///
/// Two things make that easy to do by accident, both observed while converting:
/// a message with no `{error}` interpolation doesn't look like an error mapper
/// at a glance, and classifying a transaction's `begin`/`UPDATE`/`commit` while
/// missing one statement in the middle leaves the classification unreachable —
/// the missed one 500s first. Grep by *what the statement does*, not by the
/// mapper's shape.
///
/// Apply it to **every** blocking statement on the path, not just the guard,
/// and not just the ones inside a transaction — a read that runs before
/// `begin()` and decides whether the transaction happens at all is as able to
/// block as anything inside it. Classifying a guard while leaving an earlier blocking statement
/// on [`internal_error`] means the earlier one 500s and the guard is never
/// reached; leaving a *later* one means the guard's classification is undone by
/// the next statement that contends. Taking a row lock does not protect the
/// rest of the transaction either — a write to a different table is contended
/// by anyone who writes that table without holding this lock.
///
/// **The one exception, and it is not optional: a statement that runs after
/// irreversible work must NOT be classified.** "Retryable" is an instruction to
/// the caller, and where the caller's retry re-does a side effect that already
/// landed, that instruction is worse than a 500. The concrete cases, all of
/// which say so in their own message: `sandbox::queries`' "Sandbox started
/// but DB … failed" statements run once the sandbox is live, and `create`
/// mints a fresh `SandboxId` per request with no idempotency key, so a retry
/// provisions a second sandbox the caller never learns about;
/// `template::start_build`'s job-mapping write runs after build-manager
/// accepted the job, and a retry starts a second build; the `/admin` audit
/// insert runs after the mutation succeeded, so a retry replays it. Each keeps
/// a plain constructor on purpose, and reverting one to this helper
/// reintroduces a duplicate-provisioning bug, not a mis-labelled status.
///
/// Do not audit that set by grepping [`internal_error`]: the three use three
/// different constructors, because each is on a different surface —
/// [`internal_error`] in `sandbox::queries` / `sandbox::responses`,
/// `ControllerError::internal` in `template::start_build`, and
/// `PublicApiError::internal` at the `/admin` audit layer. Nor is every
/// [`internal_error`] call site an exception: `template`'s
/// `build_jobs_store_error` uses it for the malformed-stored-id arm, which is
/// a parse failure that cannot contend at all.
///
/// The test for whether a site qualifies is not "can this statement contend"
/// but "if the caller does what this response tells them, what happens twice".
///
/// That includes the terminal `commit()`. No schema here declares a
/// `DEFERRABLE` constraint, so at READ COMMITTED a COMMIT cannot currently
/// raise any of these codes — but that exemption would be silent and would rest
/// on a schema property no reader of this function can see, so the commit is
/// classified like every other statement.
///
/// The logging lives here so the level tracks the disposition: a bounded lock
/// wait is an expected outcome of enabling the bound, not something to page on.
/// The client-facing message deliberately carries no database detail — the log
/// has it.
pub(crate) fn db_contention_error(message: &str, error: &sqlx::Error) -> ControllerError {
    // SQLSTATEs only — deliberately *not* `Io`/`PoolTimedOut`, even though the
    // sibling predicates in `direct.rs` and `lifecycle.rs` accept them. Those
    // two gate bounded in-process retry loops, where a transport failure is
    // worth one more attempt on a fresh connection. This is an HTTP
    // disposition, and it carries a `Retry-After` (see
    // `ControllerError::lock_contention`): classifying a drained pool as
    // retryable would tell every stalled caller to come back on the same
    // unjittered one-second tick, re-arriving as a synchronised wave against a
    // pool that is still exhausted. That is the failure this bound exists to
    // prevent, made self-amplifying. A pool timeout stays a 500.
    if pplx_space_common_svc::sql::contention::is_lock_contention(error) {
        tracing::warn!(%error, message, "database statement lost a lock race; retryable");
        return ControllerError::lock_contention();
    }
    // Not [`internal_error`], whose `&'static str` this widens: two call sites
    // name the row they were reading, both `failed to load builder node
    // {node_id}` (`substrate::kubernetes::adapter` and `substrate::direct`).
    // The rendering is identical.
    ControllerError::internal(format!("{message}: {error}"))
}

/// [`db_contention_error`] for the warm-pool store's errors.
///
/// Used at 14 call sites — 13 in `substrate::kubernetes::pool` and one in
/// `sandbox::queries`. Twelve of them are `.map_err(…)?` and could equally
/// call [`substrate::DbContext::db_context`], since
/// `From<SubstrateError> for ControllerError` converts; this function exists
/// for the two that cannot (see [`substrate::pool_error`] for which and why).
///
/// The variant split lives once, on [`substrate::pool_error`]; this only
/// converts. The two are equivalent because
/// `From<SubstrateError> for ControllerError` maps `Failed { message }` to
/// `internal(message)` and `LockContention` to
/// [`ControllerError::lock_contention`] — the same two outcomes
/// [`db_contention_error`] produces.
pub(crate) fn pool_store_error(
    message: &str,
    error: &pplx_space_cluster_pool::PoolError,
) -> ControllerError {
    substrate::pool_error(message, error).into()
}

impl From<tonic::Status> for ControllerError {
    fn from(status: tonic::Status) -> Self {
        if is_guest_storage_full(&status) {
            // Retrying cannot succeed until something frees space in the
            // sandbox, so this must not surface as a retryable rate limit.
            return Self::new(
                StatusCode::PRECONDITION_FAILED,
                format!("sandbox storage is full: {}", status.message()),
            );
        }
        // The backend's own message, not tonic's `code: …, message: …`
        // rendering, and its retry hint (a refusal made before anything
        // started carries one) as `Retry-After`.
        let mut error = Self::new(
            http_status_for_grpc_code(status.code()),
            status.message().to_owned(),
        );
        error.refused_before_start = pplx_space_common_grpc::is_refused_before_start(&status);
        match pplx_space_common_grpc::retry_after(&status) {
            Some(try_again) => error.with_retry_after_header(
                try_again.clamp(Duration::from_secs(1), Duration::from_secs(60)),
            ),
            None => error,
        }
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

impl IntoResponse for ControllerError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status;
        let retry_after = self.retry_after_header();
        // The span keeps the real message even when the 500 body below is
        // redacted: the span is internal telemetry (the message is already
        // logged via `tracing::error!`), and without a status description
        // the Datadog exporter falls back to the bare HTTP status code as
        // `error.message`.
        tracing::Span::current().record("otel.status_description", self.message.as_str());
        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(error = %self.message, "controller returned internal error");
            "Internal server error".to_string()
        } else {
            self.message
        };

        let mut response = (status, Json(serde_json::json!({"error": message}))).into_response();
        if let Some(retry_after) = retry_after
            && let Some(value) = retry_after_header_value(retry_after)
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        response
    }
}

pub(crate) fn retry_after_header_value(retry_after: Duration) -> Option<header::HeaderValue> {
    header::HeaderValue::from_str(
        &retry_after
            .as_secs()
            .saturating_add(u64::from(retry_after.subsec_nanos() > 0))
            .max(1)
            .to_string(),
    )
    .ok()
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

impl From<crate::federated_identity::MaterializeOwnerError> for ControllerError {
    fn from(error: crate::federated_identity::MaterializeOwnerError) -> Self {
        db_contention_error(error.context(), error.database_error())
    }
}

#[cfg(test)]
mod tests;
