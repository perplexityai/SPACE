//! The pool's classification of errors from its connections: which errors a
//! statement on these pools returns mean it lost a race and the identical
//! statement can succeed on retry.
//!
//! Each predicate delegates to `pplx_space_postgres_contention`, which keeps
//! the SQLSTATE list in one place; its documentation explains each code and
//! why `57014` is classified separately.

/// True when `error` is `55P03` (`lock_not_available`): a lock wait
/// exceeded `lock_timeout`, or a `NOWAIT` acquisition found the lock held.
pub fn is_lock_not_available(error: &sqlx::Error) -> bool {
    pplx_space_postgres_contention::is_lock_not_available(error)
}

/// True when `error` is a contention abort: `40001` (`serialization_failure`),
/// `40P01` (`deadlock_detected`), or `55P03` (`lock_not_available`).
pub fn is_lock_contention(error: &sqlx::Error) -> bool {
    pplx_space_postgres_contention::is_lock_contention(error)
}

/// True when `error` is `57014` (`query_canceled`): a `statement_timeout`
/// cancellation, or a client-issued cancel. Not part of
/// [`is_lock_contention`]; the caller decides whether it is retryable.
pub fn is_query_canceled(error: &sqlx::Error) -> bool {
    pplx_space_postgres_contention::is_query_canceled(error)
}
