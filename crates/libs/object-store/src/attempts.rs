//! Visibility into the SDK's internal S3 retries.
//!
//! The SDK's standard retry strategy makes up to three attempts per
//! operation and retries timeouts, transport errors, throttling, and 5xx
//! responses on its own. Callers only see the final result, so an upload cut
//! twice by stalled-stream protection that succeeds on its third attempt
//! looks like one slow request. [`AttemptObserver`] is an SDK interceptor on
//! the shared base client that follows every operation's attempts and
//! reports:
//!
//! - each failed attempt that is part of a retry sequence — a retry follows
//!   it, or it is the last attempt of an operation that already retried — as
//!   a WARN log and an `object_store.s3.attempt_failed` count;
//! - an operation that succeeds after more than one attempt as an INFO log.
//!
//! A `GetObject` response body is read after `send()` returns, outside the
//! SDK's attempts, so the interceptor never sees it fail. The download paths
//! report those failures through the same observer
//! ([`AttemptObserver::body_read_failed`]), with the ranged download's own
//! chunk retry numbering the attempts.
//!
//! A first attempt that fails and ends the operation is not reported here.
//! That is the operation failing, which the caller's `observe_s3_error`
//! already logs and counts, and expected outcomes such as a `HeadObject` 404
//! must not become warnings. A clean first-attempt success reports nothing.
//!
//! Hook order, per aws-smithy-runtime's orchestrator: `read_before_execution`
//! once, then `read_before_attempt` … `read_after_attempt` per attempt, then
//! `read_after_execution`. The retry decision is made after
//! `read_after_attempt`, so a failed attempt is reported when the next hook
//! shows what followed it. The attempt timeout wraps the whole attempt,
//! including `read_after_attempt`, so a timed-out attempt is recognised by
//! the next hook arriving while it is still open. The operation timeout
//! drops the execution without running any final hook, so [`Execution`]'s
//! `Drop` reports that case.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use aws_sdk_s3::operation::abort_multipart_upload::{
    AbortMultipartUploadError, AbortMultipartUploadInput,
};
use aws_sdk_s3::operation::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadInput,
};
use aws_sdk_s3::operation::copy_object::{CopyObjectError, CopyObjectInput};
use aws_sdk_s3::operation::create_multipart_upload::{
    CreateMultipartUploadError, CreateMultipartUploadInput,
};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectInput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectInput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectInput};
use aws_sdk_s3::operation::upload_part::{UploadPartError, UploadPartInput};
use aws_sdk_s3::operation::upload_part_copy::{UploadPartCopyError, UploadPartCopyInput};
use aws_sdk_s3::primitives::ByteStreamError;
use aws_smithy_async::future::timeout::TimedOutError;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeSerializationInterceptorContextRef, BeforeTransmitInterceptorContextRef, Error,
    FinalizerInterceptorContextRef, Input,
};
use aws_smithy_runtime_api::client::orchestrator::OrchestratorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::{ConfigBag, Storable, StoreReplace};
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_smithy_types::timeout::TimeoutConfig;
use pplx_space_metrics::object_store::S3AttemptFailureKind;
use tracing::{debug, info, warn};

use crate::{S3Operation, is_s3_rate_limited};

/// What one S3 request addresses, as far as the request input names it.
/// Logged with attempt reports; never used as a metric tag.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AttemptTarget {
    pub(crate) bucket: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) upload_id: Option<String>,
    pub(crate) part_number: Option<i32>,
    pub(crate) range: Option<String>,
    /// Position of a ranged download chunk; set only by the chunk retry.
    pub(crate) chunk_index: Option<usize>,
}

impl AttemptTarget {
    fn object(bucket: Option<&str>, key: Option<&str>) -> Self {
        Self {
            bucket: bucket.map(str::to_string),
            key: key.map(str::to_string),
            ..Self::default()
        }
    }

    fn upload(bucket: Option<&str>, key: Option<&str>, upload_id: Option<&str>) -> Self {
        Self {
            upload_id: upload_id.map(str::to_string),
            ..Self::object(bucket, key)
        }
    }
}

/// One failed attempt and what followed it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FailedAttempt {
    pub(crate) operation: S3Operation,
    /// 1-based attempt number within the operation.
    pub(crate) attempt: u32,
    pub(crate) kind: S3AttemptFailureKind,
    pub(crate) will_retry: bool,
    pub(crate) elapsed: Duration,
    pub(crate) http_status: Option<u16>,
    pub(crate) error_code: Option<String>,
    pub(crate) error: String,
}

/// An operation that succeeded after at least one failed attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecoveredOperation {
    pub(crate) operation: S3Operation,
    pub(crate) attempts: u32,
    pub(crate) elapsed: Duration,
}

/// Where attempt reports go. Production logs and counts them
/// ([`LogAndCount`]); crate tests record them.
pub(crate) trait AttemptReporter: fmt::Debug + Send + Sync {
    fn attempt_failed(&self, target: &AttemptTarget, failure: &FailedAttempt);
    fn recovered(&self, target: &AttemptTarget, recovery: &RecoveredOperation);
}

#[derive(Debug)]
pub(crate) struct LogAndCount;

impl AttemptReporter for LogAndCount {
    fn attempt_failed(&self, target: &AttemptTarget, failure: &FailedAttempt) {
        pplx_space_metrics::object_store::s3_attempt_failed(
            failure.operation.as_str(),
            failure.kind,
            failure.will_retry,
        );
        warn!(
            operation = failure.operation.as_str(),
            attempt = failure.attempt,
            will_retry = failure.will_retry,
            failure_kind = failure.kind.as_str(),
            elapsed_ms = millis(failure.elapsed),
            http_status = failure.http_status,
            error_code = failure.error_code.as_deref(),
            bucket = target.bucket.as_deref(),
            key = target.key.as_deref(),
            upload_id = target.upload_id.as_deref(),
            part_number = target.part_number,
            range = target.range.as_deref(),
            chunk_index = target.chunk_index,
            error = failure.error,
            "S3 request attempt failed"
        );
    }

    fn recovered(&self, target: &AttemptTarget, recovery: &RecoveredOperation) {
        info!(
            operation = recovery.operation.as_str(),
            attempts = recovery.attempts,
            elapsed_ms = millis(recovery.elapsed),
            bucket = target.bucket.as_deref(),
            key = target.key.as_deref(),
            upload_id = target.upload_id.as_deref(),
            part_number = target.part_number,
            range = target.range.as_deref(),
            chunk_index = target.chunk_index,
            "S3 operation succeeded after retrying failed attempts"
        );
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// SDK interceptor that tracks each operation's attempts. Registered once on
/// the shared base client, so it sees every operation the store issues.
#[derive(Clone, Debug)]
pub(crate) struct AttemptObserver {
    reporter: Arc<dyn AttemptReporter>,
}

impl AttemptObserver {
    pub(crate) fn new(reporter: Arc<dyn AttemptReporter>) -> Self {
        Self { reporter }
    }

    pub(crate) fn logging() -> Self {
        Self::new(Arc::new(LogAndCount))
    }

    /// Reports one failed read of a `GetObject` response body. `attempt` and
    /// `will_retry` describe the caller's own retry of the body, since the
    /// SDK's ended when `send()` returned. Every failure is reported, the
    /// last one included: unlike a failed request, nothing else counts it.
    pub(crate) fn body_read_failed(
        &self,
        target: &AttemptTarget,
        attempt: u32,
        will_retry: bool,
        elapsed: Duration,
        error: &ByteStreamError,
    ) {
        self.reporter.attempt_failed(
            target,
            &FailedAttempt {
                operation: S3Operation::GetObject,
                attempt,
                kind: classify_body_read_failure(error),
                will_retry,
                elapsed,
                http_status: None,
                error_code: None,
                error: DisplayErrorContext(error).to_string(),
            },
        );
    }

    /// Reports a response body read in full after `attempts` tries.
    pub(crate) fn body_read_recovered(
        &self,
        target: &AttemptTarget,
        attempts: u32,
        elapsed: Duration,
    ) {
        self.reporter.recovered(
            target,
            &RecoveredOperation {
                operation: S3Operation::GetObject,
                attempts,
                elapsed,
            },
        );
    }
}

impl Intercept for AttemptObserver {
    fn name(&self) -> &'static str {
        "pplx-space-object-store-attempt-observer"
    }

    // Client interceptors run this before operation configuration is
    // applied, but the typed input is still present here and names both the
    // operation and its target.
    fn read_before_execution(
        &self,
        context: &BeforeSerializationInterceptorContextRef<'_>,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some((operation, target)) = describe_request(context.input()) {
            cfg.interceptor_state()
                .store_put(AttemptTracker(Arc::new(Mutex::new(Execution::new(
                    self.reporter.clone(),
                    operation,
                    target,
                )))));
        }
        Ok(())
    }

    fn read_before_attempt(
        &self,
        _context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(tracker) = cfg.load::<AttemptTracker>() {
            // Loaded here rather than at execution start: the orchestrator
            // stores the merged client/operation timeout config after the
            // client `read_before_execution` hooks have run.
            tracker.lock().begin_attempt(cfg.load::<TimeoutConfig>());
        }
        Ok(())
    }

    fn read_after_attempt(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(tracker) = cfg.load::<AttemptTracker>() {
            let http_status = context
                .response()
                .map(|response| response.status().as_u16());
            tracker
                .lock()
                .end_attempt(context.output_or_error().and_then(Result::err), http_status);
        }
        Ok(())
    }

    fn read_after_execution(
        &self,
        context: &FinalizerInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(tracker) = cfg.load::<AttemptTracker>() {
            tracker
                .lock()
                .finish(matches!(context.output_or_error(), Some(Ok(_))));
        }
        Ok(())
    }
}

/// Names the operation and target of a request from its typed input. Only
/// the operations this crate issues are tracked; anything else is ignored.
fn describe_request(input: &Input) -> Option<(S3Operation, AttemptTarget)> {
    if let Some(input) = input.downcast_ref::<PutObjectInput>() {
        return Some((
            S3Operation::PutObject,
            AttemptTarget::object(input.bucket(), input.key()),
        ));
    }
    if let Some(input) = input.downcast_ref::<UploadPartInput>() {
        return Some((
            S3Operation::UploadPart,
            AttemptTarget {
                part_number: input.part_number(),
                ..AttemptTarget::upload(input.bucket(), input.key(), input.upload_id())
            },
        ));
    }
    if let Some(input) = input.downcast_ref::<GetObjectInput>() {
        return Some((
            S3Operation::GetObject,
            AttemptTarget {
                range: input.range().map(str::to_string),
                ..AttemptTarget::object(input.bucket(), input.key())
            },
        ));
    }
    if let Some(input) = input.downcast_ref::<HeadObjectInput>() {
        return Some((
            S3Operation::HeadObject,
            AttemptTarget::object(input.bucket(), input.key()),
        ));
    }
    if let Some(input) = input.downcast_ref::<CreateMultipartUploadInput>() {
        return Some((
            S3Operation::CreateMultipartUpload,
            AttemptTarget::object(input.bucket(), input.key()),
        ));
    }
    if let Some(input) = input.downcast_ref::<CompleteMultipartUploadInput>() {
        return Some((
            S3Operation::CompleteMultipartUpload,
            AttemptTarget::upload(input.bucket(), input.key(), input.upload_id()),
        ));
    }
    if let Some(input) = input.downcast_ref::<AbortMultipartUploadInput>() {
        return Some((
            S3Operation::AbortMultipartUpload,
            AttemptTarget::upload(input.bucket(), input.key(), input.upload_id()),
        ));
    }
    if let Some(input) = input.downcast_ref::<CopyObjectInput>() {
        return Some((
            S3Operation::CopyObject,
            AttemptTarget::object(input.bucket(), input.key()),
        ));
    }
    if let Some(input) = input.downcast_ref::<UploadPartCopyInput>() {
        return Some((
            S3Operation::UploadPartCopy,
            AttemptTarget {
                part_number: input.part_number(),
                ..AttemptTarget::upload(input.bucket(), input.key(), input.upload_id())
            },
        ));
    }
    None
}

/// The S3 error code of a modeled service error, when the response carried
/// one.
fn service_error_code(error: &Error) -> Option<&str> {
    macro_rules! code_of {
        ($($error:ty),+ $(,)?) => {
            $(
                if let Some(error) = error.downcast_ref::<$error>() {
                    return error.code();
                }
            )+
        };
    }
    code_of!(
        AbortMultipartUploadError,
        CompleteMultipartUploadError,
        CopyObjectError,
        CreateMultipartUploadError,
        GetObjectError,
        HeadObjectError,
        PutObjectError,
        UploadPartError,
        UploadPartCopyError,
    );
    None
}

/// Classifies why one attempt failed.
///
/// Connector timeouts come from two places. The HTTP client's connect (and,
/// when configured, read) timeouts carry an `aws_smithy_async`
/// [`TimedOutError`] in their source chain, so they are transport failures.
/// The other producer is upload stalled-stream protection, whose error type
/// is private to aws-smithy-runtime; a connector timeout without that marker
/// is therefore a stall cut. Throttling is checked first and uses the same
/// rule as the operation-level `s3_error` classification.
pub(crate) fn classify_attempt_failure<E>(
    error: &OrchestratorError<E>,
    http_status: Option<u16>,
    error_code: Option<&str>,
) -> S3AttemptFailureKind {
    if is_s3_rate_limited(error_code, http_status) {
        return S3AttemptFailureKind::Throttled;
    }
    if let Some(connector) = error.as_connector_error() {
        return if connector.is_timeout() {
            if has_timer_source(connector) {
                S3AttemptFailureKind::Io
            } else {
                S3AttemptFailureKind::StalledStream
            }
        } else if connector.is_io() {
            S3AttemptFailureKind::Io
        } else {
            S3AttemptFailureKind::Other
        };
    }
    if error.is_timeout_error() {
        return S3AttemptFailureKind::AttemptTimeout;
    }
    match http_status {
        Some(500..=599) => S3AttemptFailureKind::Http5xx,
        Some(400..=499) => S3AttemptFailureKind::Http4xx,
        _ => S3AttemptFailureKind::Other,
    }
}

/// Classifies why reading a response body failed.
///
/// A transport failure carries an `io::Error` in its source chain: hyper
/// reports a reset, a closed connection, and a body that ends before its
/// `Content-Length` that way. The other producer on this path is download
/// stalled-stream protection, whose error type is private to
/// aws-smithy-runtime, so a body failure without an I/O source is a stall
/// cut.
pub(crate) fn classify_body_read_failure(error: &ByteStreamError) -> S3AttemptFailureKind {
    if has_source::<std::io::Error>(error) {
        S3AttemptFailureKind::Io
    } else {
        S3AttemptFailureKind::StalledStream
    }
}

fn has_timer_source(error: &(dyn std::error::Error + 'static)) -> bool {
    has_source::<TimedOutError>(error)
}

fn has_source<T: std::error::Error + 'static>(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(error);
    while let Some(error) = next {
        if error.is::<T>() {
            return true;
        }
        next = error.source();
    }
    false
}

/// Per-execution attempt state, stored in the operation's config bag. The
/// bag lives exactly as long as the execution, including one the operation
/// timeout drops.
#[derive(Clone, Debug)]
struct AttemptTracker(Arc<Mutex<Execution>>);

impl AttemptTracker {
    fn lock(&self) -> MutexGuard<'_, Execution> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Storable for AttemptTracker {
    type Storer = StoreReplace<Self>;
}

#[derive(Debug)]
struct Execution {
    reporter: Arc<dyn AttemptReporter>,
    operation: S3Operation,
    target: AttemptTarget,
    started_at: Instant,
    attempt_timeout: Option<Duration>,
    operation_timeout: Option<Duration>,
    attempts: u32,
    /// Start of the attempt that has not reached `read_after_attempt` yet.
    open_attempt: Option<Instant>,
    /// A failed attempt whose retry decision is not known yet.
    unresolved_failure: Option<FailedAttempt>,
    finished: bool,
}

impl Execution {
    fn new(
        reporter: Arc<dyn AttemptReporter>,
        operation: S3Operation,
        target: AttemptTarget,
    ) -> Self {
        Self {
            reporter,
            operation,
            target,
            started_at: Instant::now(),
            attempt_timeout: None,
            operation_timeout: None,
            attempts: 0,
            open_attempt: None,
            unresolved_failure: None,
            finished: false,
        }
    }

    fn begin_attempt(&mut self, timeouts: Option<&TimeoutConfig>) {
        if let Some(timeouts) = timeouts {
            self.attempt_timeout = timeouts.operation_attempt_timeout();
            self.operation_timeout = timeouts.operation_timeout();
        }
        // A new attempt means the SDK decided to retry whatever came before.
        if let Some(failure) = self.take_previous_failure() {
            self.report_failure(failure, true);
        }
        self.attempts += 1;
        self.open_attempt = Some(Instant::now());
    }

    fn end_attempt(&mut self, error: Option<&OrchestratorError<Error>>, http_status: Option<u16>) {
        let Some(started_at) = self.open_attempt.take() else {
            // Another interceptor failed this attempt before our
            // `read_before_attempt` ran; count it from here.
            self.begin_attempt(None);
            return self.end_attempt(error, http_status);
        };
        let Some(error) = error else {
            return;
        };
        let error_code = error.as_operation_error().and_then(service_error_code);
        self.unresolved_failure = Some(FailedAttempt {
            operation: self.operation,
            attempt: self.attempts,
            kind: classify_attempt_failure(error, http_status, error_code),
            will_retry: false,
            elapsed: started_at.elapsed(),
            http_status,
            error_code: error_code.map(str::to_string),
            error: DisplayErrorContext(error).to_string(),
        });
    }

    fn finish(&mut self, succeeded: bool) {
        self.finished = true;
        match self.take_previous_failure() {
            // A lone failed attempt is the operation's own failure; the
            // caller reports that.
            Some(failure) if self.attempts > 1 => self.report_failure(failure, false),
            Some(_) => {}
            None if succeeded && self.attempts > 1 => self.reporter.recovered(
                &self.target,
                &RecoveredOperation {
                    operation: self.operation,
                    attempts: self.attempts,
                    elapsed: self.started_at.elapsed(),
                },
            ),
            None => {}
        }
    }

    /// The previous attempt's failure, if it failed: either it reached
    /// `read_after_attempt` with an error, or it is still open, which means
    /// the attempt timeout cut it.
    fn take_previous_failure(&mut self) -> Option<FailedAttempt> {
        if let Some(started_at) = self.open_attempt.take() {
            return Some(self.timed_out_attempt(started_at, S3AttemptFailureKind::AttemptTimeout));
        }
        self.unresolved_failure.take()
    }

    fn timed_out_attempt(&self, started_at: Instant, kind: S3AttemptFailureKind) -> FailedAttempt {
        let limit = match kind {
            S3AttemptFailureKind::AttemptTimeout => self.attempt_timeout,
            _ => self.operation_timeout,
        };
        // Detected at the next hook, which may follow a retry backoff; an
        // attempt cut by a timeout lasted exactly that timeout.
        let elapsed = limit.map_or(started_at.elapsed(), |limit| {
            started_at.elapsed().min(limit)
        });
        let scope = match kind {
            S3AttemptFailureKind::AttemptTimeout => "attempt",
            _ => "operation",
        };
        let error = match limit {
            Some(limit) => format!("{scope} exceeded the {limit:?} {scope} timeout"),
            None => format!("{scope} timed out"),
        };
        FailedAttempt {
            operation: self.operation,
            attempt: self.attempts,
            kind,
            will_retry: false,
            elapsed,
            http_status: None,
            error_code: None,
            error,
        }
    }

    fn report_failure(&self, mut failure: FailedAttempt, will_retry: bool) {
        failure.will_retry = will_retry;
        self.reporter.attempt_failed(&self.target, &failure);
    }
}

impl Drop for Execution {
    /// The execution ended without `read_after_execution`: the operation
    /// timeout fired, or the caller dropped the request future. Either way
    /// no further attempt follows.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Some(failure) = self.unresolved_failure.take() {
            self.report_failure(failure, false);
        }
        let Some(started_at) = self.open_attempt.take() else {
            return;
        };
        let elapsed = started_at.elapsed();
        if self.attempt_timeout.is_some_and(|limit| elapsed >= limit) {
            // Cut by the attempt timeout; the operation deadline then
            // expired during the retry backoff.
            let failure = self.timed_out_attempt(started_at, S3AttemptFailureKind::AttemptTimeout);
            self.report_failure(failure, false);
        } else if self
            .operation_timeout
            .is_some_and(|limit| self.started_at.elapsed() >= limit)
        {
            let failure =
                self.timed_out_attempt(started_at, S3AttemptFailureKind::OperationTimeout);
            self.report_failure(failure, false);
        } else {
            debug!(
                operation = self.operation.as_str(),
                attempt = self.attempts,
                elapsed_ms = millis(elapsed),
                bucket = self.target.bucket.as_deref(),
                key = self.target.key.as_deref(),
                "S3 request attempt abandoned by its caller"
            );
        }
    }
}

#[cfg(test)]
mod tests;
