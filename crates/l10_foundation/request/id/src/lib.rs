//! Caller-supplied request id parsing.
//!
//! [`request_id_from_headers`] reads the id a caller sends with a request
//! (`x-request-id`, or `x-correlation-id`) and accepts it only in a bounded,
//! log-safe shape, so HTTP and gRPC servers can record it on request spans.
//!
//! It is its own crate so that code can read a request id without depending
//! on `pplx-space-common-svc`. `pplx-space-common-svc` re-exports it from
//! `bootstrap`.

use http::HeaderMap;

/// The longest caller-supplied request id a span or log line accepts.
const MAX_REQUEST_ID_BYTES: usize = 128;

/// A caller-supplied request id: `x-request-id`, or `x-correlation-id` when
/// that is absent or blank. The first non-blank header decides.
///
/// The id lands on the request's span and on every log line written inside
/// it, from callers that may be unauthenticated, so it is accepted only as at
/// most 128 bytes of `[A-Za-z0-9._:-]`; anything else yields `None`.
pub fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    for name in ["x-request-id", "x-correlation-id"] {
        let Some(value) = headers.get(name) else {
            continue;
        };
        let Ok(value) = value.to_str() else {
            return None;
        };
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let valid = trimmed.len() <= MAX_REQUEST_ID_BYTES
            && trimmed.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            });
        return valid.then(|| trimmed.to_owned());
    }
    None
}

#[cfg(test)]
mod tests;
