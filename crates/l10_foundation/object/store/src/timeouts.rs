//! Per-operation-class bounds on S3 requests.
//!
//! The SDK sets no attempt or operation timeout, so an endpoint that accepts
//! a request and never answers holds it forever. One bound for every call
//! would have to fit the slowest body upload and would leave the paths a
//! sandbox waits on (head, ranged reads) hanging for minutes, so each
//! operation belongs to a class with its own bound:
//!
//! | Class | Operations | Attempt | Operation |
//! |---|---|---|---|
//! | request | `GetObject`, `HeadObject`, `CreateMultipartUpload`, `AbortMultipartUpload` | 10 s | 35 s |
//! | body upload | `PutObject`, `UploadPart` | 60 s | 185 s |
//! | server-side | `CompleteMultipartUpload`, `CopyObject`, `UploadPartCopy` | 30 s | 95 s |
//!
//! An attempt covers sending the request body and waiting for the response
//! headers, plus the response body for operations the SDK deserializes in
//! full. A `GetObject` attempt ends at the response headers; its body is
//! read afterwards, bounded by stalled-stream protection and the ranged
//! download's own chunk retry.
//!
//! Each default is sized from how S3 serves its class:
//!
//! - Request: S3 normally answers a metadata request, and sends a
//!   `GetObject`'s response headers, within about a second, so 10 s leaves
//!   wide margin. Only the wait for the headers is under the attempt
//!   timeout; a ranged chunk's body is bounded as described above.
//! - Body upload: an attempt must fit sending one 256 MiB multipart part on
//!   a slow link. 60 s allows about 4.3 MiB/s per part, and the parts of
//!   one upload share the link, 16 at a time by default. Check
//!   `space.object_store.s3.attempt_failed` before tuning this bound.
//! - Server-side: S3 holds a `CopyObject` or `UploadPartCopy` request open
//!   while it copies the data, and answers `CompleteMultipartUpload` only
//!   once it has assembled the parts, so these wait on work proportional to
//!   the object's size rather than on a metadata lookup.
//!
//! The body-upload bound assumes the default 256 MiB multipart part: a
//! larger `SPACE_OBJECT_STORE_MULTIPART_PART_SIZE_BYTES` needs a
//! proportionally larger body-upload attempt timeout.

use std::ops::RangeInclusive;
use std::time::Duration;

use aws_smithy_types::timeout::TimeoutConfig;

use crate::S3Operation;

/// Attempts the SDK's standard retry strategy makes per operation
/// (aws-smithy-types `RetryConfig::standard`, the default for this behavior
/// version).
pub(crate) const SDK_MAX_ATTEMPTS: u32 = 3;

/// Room in a derived operation timeout for everything between attempts.
///
/// With no retry spec configured, as here, the standard strategy sleeps
/// `rand[0, 1) × min(1 s × 2^(n-1), 20 s)` before retry `n`: under 1 s
/// before the second attempt and under 2 s before the third, so under 3 s in
/// total. 5 s covers that plus request signing and endpoint resolution. A
/// server that sends `x-amz-retry-after` can ask for up to 20 s per retry; S3
/// does not, and the operation timeout then cuts the last attempt short
/// rather than letting a slow endpoint hold the caller.
pub(crate) const RETRY_BACKOFF_ALLOWANCE: Duration = Duration::from_secs(5);

const TIMEOUT_SECS_MIN: u64 = 5;
const ATTEMPT_TIMEOUT_SECS_MAX: u64 = 6 * 60 * 60;
const OPERATION_TIMEOUT_SECS_MAX: u64 = 24 * 60 * 60;

/// Groups S3 operations by what bounds their duration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum S3TimeoutClass {
    /// No request body; the server answers from metadata. `ListObjectsV2`
    /// and `DeleteObject(s)` belong here too, but this crate issues neither.
    Request,
    /// The request carries up to one multipart part of data.
    BodyUpload,
    /// The server does work proportional to the data before answering.
    ServerSide,
}

impl S3TimeoutClass {
    const fn default_attempt_timeout(self) -> Duration {
        match self {
            Self::Request => Duration::from_secs(10),
            Self::BodyUpload => Duration::from_secs(60),
            Self::ServerSide => Duration::from_secs(30),
        }
    }

    /// Env var that overrides this class's attempt timeout, in whole seconds,
    /// `5..=21600`.
    pub(crate) const fn attempt_timeout_env(self) -> &'static str {
        match self {
            Self::Request => "SPACE_OBJECT_STORE_REQUEST_ATTEMPT_TIMEOUT_SECS",
            Self::BodyUpload => "SPACE_OBJECT_STORE_BODY_UPLOAD_ATTEMPT_TIMEOUT_SECS",
            Self::ServerSide => "SPACE_OBJECT_STORE_SERVER_SIDE_ATTEMPT_TIMEOUT_SECS",
        }
    }

    /// Env var that overrides this class's operation timeout, in whole
    /// seconds, `5..=86400`. Unset, it is derived from the resolved attempt
    /// timeout, so raising only the attempt timeout keeps room for every
    /// retry.
    pub(crate) const fn operation_timeout_env(self) -> &'static str {
        match self {
            Self::Request => "SPACE_OBJECT_STORE_REQUEST_OPERATION_TIMEOUT_SECS",
            Self::BodyUpload => "SPACE_OBJECT_STORE_BODY_UPLOAD_OPERATION_TIMEOUT_SECS",
            Self::ServerSide => "SPACE_OBJECT_STORE_SERVER_SIDE_OPERATION_TIMEOUT_SECS",
        }
    }
}

impl S3Operation {
    pub(crate) const fn timeout_class(self) -> S3TimeoutClass {
        match self {
            Self::GetObject
            | Self::HeadObject
            | Self::CreateMultipartUpload
            | Self::AbortMultipartUpload => S3TimeoutClass::Request,
            Self::PutObject | Self::UploadPart => S3TimeoutClass::BodyUpload,
            Self::CompleteMultipartUpload | Self::CopyObject | Self::UploadPartCopy => {
                S3TimeoutClass::ServerSide
            }
        }
    }
}

/// Bounds on one S3 operation and on each of its attempts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClassTimeouts {
    pub(crate) attempt: Duration,
    pub(crate) operation: Duration,
}

impl ClassTimeouts {
    /// Every attempt timing out in full plus the backoff between them:
    /// `3 × attempt + 5 s`, so 35 s, 185 s, and 95 s at the defaults.
    pub(crate) fn derived(attempt: Duration) -> Self {
        Self {
            attempt,
            operation: attempt * SDK_MAX_ATTEMPTS + RETRY_BACKOFF_ALLOWANCE,
        }
    }

    /// Only the two bounds; unset fields such as the SDK's 3.1 s connect
    /// timeout keep their defaults because the orchestrator merges timeout
    /// layers.
    pub(crate) fn timeout_config(self) -> TimeoutConfig {
        TimeoutConfig::builder()
            .operation_attempt_timeout(self.attempt)
            .operation_timeout(self.operation)
            .build()
    }
}

/// Resolved bounds for every class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct S3Timeouts {
    pub(crate) request: ClassTimeouts,
    pub(crate) body_upload: ClassTimeouts,
    pub(crate) server_side: ClassTimeouts,
}

impl S3Timeouts {
    pub(crate) fn from_env() -> Self {
        Self::resolve(|name| std::env::var(name).ok())
    }

    /// Out-of-range or unparseable values fall back to the default, as the
    /// other transfer tuning does.
    pub(crate) fn resolve<F: Fn(&str) -> Option<String>>(read: F) -> Self {
        let class = |class: S3TimeoutClass| {
            let attempt = read_secs(
                &read,
                class.attempt_timeout_env(),
                TIMEOUT_SECS_MIN..=ATTEMPT_TIMEOUT_SECS_MAX,
            )
            .unwrap_or(class.default_attempt_timeout());
            let derived = ClassTimeouts::derived(attempt);
            ClassTimeouts {
                operation: read_secs(
                    &read,
                    class.operation_timeout_env(),
                    TIMEOUT_SECS_MIN..=OPERATION_TIMEOUT_SECS_MAX,
                )
                .unwrap_or(derived.operation),
                ..derived
            }
        };
        Self {
            request: class(S3TimeoutClass::Request),
            body_upload: class(S3TimeoutClass::BodyUpload),
            server_side: class(S3TimeoutClass::ServerSide),
        }
    }

    pub(crate) const fn class(&self, class: S3TimeoutClass) -> ClassTimeouts {
        match class {
            S3TimeoutClass::Request => self.request,
            S3TimeoutClass::BodyUpload => self.body_upload,
            S3TimeoutClass::ServerSide => self.server_side,
        }
    }

    pub(crate) fn for_operation(&self, operation: S3Operation) -> TimeoutConfig {
        self.class(operation.timeout_class()).timeout_config()
    }
}

/// Reads an object-store `*_SECS` knob: whole seconds within `range`.
/// Unset, unparseable, or out-of-range values read as `None`.
pub(crate) fn read_secs<F: FnOnce(&str) -> Option<String>>(
    read: F,
    env_name: &str,
    range: RangeInclusive<u64>,
) -> Option<Duration> {
    read(env_name)
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| range.contains(n))
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests;
