//! The Native public API's HTTP error contract, for every server that answers
//! Native routes: the error envelope and the status each code answers with,
//! the request id every response carries, and the envelope a framework
//! rejection or an unhandled error status is normalized into.

mod error;
mod request_id;

pub use error::{
    LOCK_CONTENTION_RETRY_AFTER, PublicApiError, error_contract, normalize_public_error_response,
    public_route_not_found, retry_after_header_value, with_retry_after_hint,
};
pub use request_id::{current_request_id, scope_request_id, with_public_request_id};
