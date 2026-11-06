/// Record a typed guest outcome without changing the enclosing span's status.
/// Public HTTP spans still classify their legitimate 502 as an HTTP error.
pub fn record_guest_service_outcome(status: &tonic::Status, span: &tracing::Span) -> bool {
    if crate::GuestServiceUnavailable::from_status(status).is_none() {
        return false;
    }
    span.record("failure_origin", "guest");
    span.record("failure_reason", "connection_refused");
    true
}

/// Keep the gRPC status visible without classifying an expected guest refusal
/// as an internal APM error. The marker and code must both match.
pub fn record_guest_service_status(status: &tonic::Status, span: &tracing::Span) -> bool {
    if !record_guest_service_outcome(status, span) {
        return false;
    }
    span.record("otel.status_code", "OK");
    true
}
