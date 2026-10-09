use crate::emit::{counter, tag};

/// One S3 SDK error observed by the shared object-store client.
///
/// Target bucket/key stays in structured logs, not metric tags, to keep
/// custom metric cardinality bounded.
pub fn s3_error(operation: &str, error_kind: &str, error_code: &str, http_status: &str) {
    counter(
        "object_store.s3.error",
        1,
        &[
            tag("operation", operation),
            tag("error_kind", error_kind),
            tag("error_code", error_code),
            tag("http_status", http_status),
        ],
    );
}

metric_vocabulary!(
    /// Why one S3 request attempt inside the SDK's retry loop failed.
    S3AttemptFailureKind {
        /// Upload stalled-stream protection cut the attempt: the request
        /// body made no progress for the grace period.
        StalledStream => "stalled_stream",
        /// The attempt exceeded the per-attempt timeout, typically while
        /// waiting for a response after the body was sent.
        AttemptTimeout => "attempt_timeout",
        /// The operation-wide deadline expired while this attempt was in
        /// flight.
        OperationTimeout => "operation_timeout",
        /// Transport failure: connect error or timeout, refused or reset
        /// connection, or a connection the peer closed before the response
        /// completed (such as a reused keep-alive connection the server had
        /// already closed).
        Io => "io",
        /// The endpoint answered with a non-throttling 5xx status.
        Http5xx => "http_5xx",
        /// The endpoint throttled the request (429, 503, or an S3
        /// throttling error code).
        Throttled => "throttled",
        /// The endpoint answered with a non-throttling 4xx status.
        Http4xx => "http_4xx",
        /// Anything else, such as an unreadable response body.
        Other => "other",
    }
);

/// One failed S3 request attempt that is part of a retry sequence: either
/// the SDK retries after it (`will_retry:true`), or it is the last attempt
/// of an operation that already retried (`will_retry:false`).
///
/// A first attempt that fails and ends the operation is not counted here;
/// [`s3_error`] already records that operation failure. Bucket, key, and
/// upload identity stay in the paired WARN log, not in tags.
pub fn s3_attempt_failed(operation: &str, failure_kind: S3AttemptFailureKind, will_retry: bool) {
    counter(
        "object_store.s3.attempt_failed",
        1,
        &[
            tag("operation", operation),
            tag("failure_kind", failure_kind),
            tag("will_retry", if will_retry { "true" } else { "false" }),
        ],
    );
}
