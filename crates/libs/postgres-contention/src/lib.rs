//! Classification of the PostgreSQL SQLSTATEs that mean "lost a race".
//!
//! The application logins carry an optional role-level `lock_timeout`,
//! enabled per environment (see `db/README.md`). Where it is on, a statement
//! that blocks on a lock aborts with `55P03` rather than holding its pooled
//! connection until `statement_timeout` expires. That is the point — a
//! blocked query occupies a pool connection for the whole wait, so DDL on a
//! hot table can park every connection on the same `ACCESS SHARE` wait — but
//! it converts a latency problem into an error-classification problem. Every
//! code below means the identical statement can succeed on retry, and a
//! caller that treats one as permanent turns a bounded wait into a failed
//! request.
//!
//! Keeping the list in one place is the load-bearing part. A caller that
//! open-codes `matches!(code, "40001" | "40P01")` silently excludes
//! `55P03`, and that omission is invisible until the role-level bound is
//! enabled — before then, a lock wait ends in `57014` (statement timeout)
//! or not at all.
//!
//! It is its own crate because it depends only on SQLx's error types, so a
//! crate that classifies these errors does not need `pplx-space-common-svc`,
//! which re-exports it as `sql::contention`. It is a classification helper,
//! not a storage layer: it owns no table and runs no statement, so the store
//! boundary rules do not apply to it.

/// True when `error` is `55P03` (`lock_not_available`): a lock wait
/// exceeded `lock_timeout`, or a `NOWAIT` acquisition found the lock held.
///
/// Callers that acquire with `NOWAIT` use this to distinguish "contended,
/// try again later" from a real failure. Under a role-level `lock_timeout`
/// every waiting acquisition can raise it too, including plain
/// `SELECT`/`UPDATE` statements that merely collided with DDL on the table.
pub fn is_lock_not_available(error: &sqlx::Error) -> bool {
    sqlstate(error).is_some_and(|code| code == "55P03")
}

/// True when `error` is a contention abort: `40001` (`serialization_failure`),
/// `40P01` (`deadlock_detected`), or `55P03` (`lock_not_available`).
///
/// These are the "retry the whole statement or transaction" conditions.
/// They are not interchangeable with `57014` (`query_canceled`, which a
/// `statement_timeout` raises): a statement that was too slow once is
/// likely to be too slow again, so callers that want it retryable opt in
/// explicitly rather than folding it in here.
pub fn is_lock_contention(error: &sqlx::Error) -> bool {
    sqlstate(error).is_some_and(|code| matches!(code.as_ref(), "40001" | "40P01" | "55P03"))
}

/// One statement's SQLSTATE, when it came from the server.
///
/// Borrowed rather than owned: every caller compares it against literals, so
/// there is nothing to allocate. Private — callers that need to classify codes
/// outside the contention set should get a predicate here rather than the raw
/// code, so the SQLSTATE literals stay in one file.
fn sqlstate(error: &sqlx::Error) -> Option<std::borrow::Cow<'_, str>> {
    error.as_database_error().and_then(|error| error.code())
}

/// True when `error` is `57014` (`query_canceled`): a `statement_timeout`
/// cancellation, or a client-issued cancel.
///
/// Deliberately separate from [`is_lock_contention`]. Whether it is retryable
/// depends on the statement: a short CAS that was cancelled was almost
/// certainly contended, while a genuinely slow query will be slow again. The
/// caller decides, but reads the code from here rather than typing it.
pub fn is_query_canceled(error: &sqlx::Error) -> bool {
    sqlstate(error).is_some_and(|code| code == "57014")
}

#[cfg(test)]
mod tests;
