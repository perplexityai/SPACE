//! The Native error contract lives in `pplx-space-public-api-http`; this
//! module adds the conversions from this crate's own store and database
//! errors.

pub use pplx_space_public_api_http::{
    PublicApiError, error_contract, normalize_public_error_response, public_route_not_found,
    with_retry_after_hint,
};

impl From<crate::public_operations::PublicOperationStoreError> for PublicApiError {
    fn from(error: crate::public_operations::PublicOperationStoreError) -> Self {
        if let crate::public_operations::PublicOperationStoreError::Sql(error) = &error
            && let Some(contended) = lock_contention_error(error)
        {
            return contended;
        }
        tracing::error!(error = %error, "public operation store error");
        Self::internal()
    }
}

impl From<crate::public_operations::ResponseAdmissionError> for PublicApiError {
    fn from(error: crate::public_operations::ResponseAdmissionError) -> Self {
        match error {
            crate::public_operations::ResponseAdmissionError::Store(error) => Self::from(error),
            crate::public_operations::ResponseAdmissionError::Sql(error) => database_error(error),
            crate::public_operations::ResponseAdmissionError::Conflict => {
                Self::idempotency_conflict(error.to_string())
            }
        }
    }
}

impl From<crate::public_sessions::PublicSessionStoreError> for PublicApiError {
    fn from(error: crate::public_sessions::PublicSessionStoreError) -> Self {
        match error {
            crate::public_sessions::PublicSessionStoreError::IdempotencyConflict => {
                Self::idempotency_conflict(
                    "idempotency key was already used with a different request",
                )
            }
            crate::public_sessions::PublicSessionStoreError::SessionEnded => {
                Self::conflict_with_reason(
                    "session was revoked or has ended; create a new session with a new idempotency key",
                    pplx_space_public_api::CONFLICT_REASON_SESSION_ENDED,
                )
            }
            crate::public_sessions::PublicSessionStoreError::Sql(error) => database_error(error),
            other => {
                tracing::error!(error = %other, "public session store error");
                Self::internal()
            }
        }
    }
}

/// A retryable-contention response for `error`, or `None` if it is a real
/// failure. Logged here so a bounded lock wait lands at `warn` rather than the
/// `error` level a genuine fault deserves.
///
/// `pub(crate)` for the `PublicApiError`-shaped call sites outside this module
/// — the `/admin` session and authz middleware and the admin sandbox lookup —
/// which keep their own per-site `error!` (it carries the actor and target ids
/// this one does not) and so branch on this rather than going through
/// [`database_error`].
pub(crate) fn lock_contention_error(error: &sqlx::Error) -> Option<PublicApiError> {
    if !pplx_space_common_svc::sql::contention::is_lock_contention(error) {
        return None;
    }
    tracing::warn!(error = %error, "public api statement lost a lock race; retryable");
    Some(PublicApiError::lock_contention())
}

/// The public answer for a failed database statement: a retryable 503 for a
/// lost lock race, otherwise a logged 500.
pub(crate) fn database_error(error: sqlx::Error) -> PublicApiError {
    if let Some(contended) = lock_contention_error(&error) {
        return contended;
    }
    tracing::error!(error = %error, "public api database error");
    PublicApiError::internal()
}

#[cfg(test)]
mod tests;
