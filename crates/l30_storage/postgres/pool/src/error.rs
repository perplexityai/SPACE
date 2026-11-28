//! [`DbError`]: a failed database call, classified by what callers branch on.

/// A database call failed: the driver's error, classified by [`DbError::kind`].
/// It displays, and chains its sources, exactly as the driver's error does.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DbError(sqlx::Error);

/// The kinds are the conditions converted callers branch on. `Other` holds
/// everything else, including conditions some unconverted callers still
/// classify on the driver's error (I/O errors, decode errors, connection- and
/// resource-class SQLSTATEs); they get kinds when those callers convert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbErrorKind {
    /// A statement that must return a row returned none.
    NotFound,
    /// `23505` (`unique_violation`).
    UniqueViolation,
    /// `23503` (`foreign_key_violation`).
    ForeignKeyViolation,
    /// `23514` (`check_violation`).
    CheckViolation,
    /// `40001` (`serialization_failure`).
    SerializationFailure,
    /// `40P01` (`deadlock_detected`).
    Deadlock,
    /// `55P03` (`lock_not_available`): a lock wait exceeded `lock_timeout`, or
    /// a `NOWAIT` acquisition found the lock held.
    LockNotAvailable,
    /// `57014` (`query_canceled`): a `statement_timeout` cancellation or a
    /// client-issued cancel.
    QueryCanceled,
    /// No pool connection became free within the pool's acquire timeout.
    PoolTimedOut,
    /// Any other failure.
    Other,
}

impl DbError {
    /// What kind of failure this is.
    pub fn kind(&self) -> DbErrorKind {
        use pplx_space_postgres_contention as contention;

        let error = &self.0;
        let database = error.as_database_error();
        if matches!(error, sqlx::Error::RowNotFound) {
            DbErrorKind::NotFound
        } else if matches!(error, sqlx::Error::PoolTimedOut) {
            DbErrorKind::PoolTimedOut
        } else if database.is_some_and(|database| database.is_unique_violation()) {
            DbErrorKind::UniqueViolation
        } else if database.is_some_and(|database| database.is_foreign_key_violation()) {
            DbErrorKind::ForeignKeyViolation
        } else if database.is_some_and(|database| database.is_check_violation()) {
            DbErrorKind::CheckViolation
        } else if contention::is_serialization_failure(error) {
            DbErrorKind::SerializationFailure
        } else if contention::is_deadlock_detected(error) {
            DbErrorKind::Deadlock
        } else if contention::is_lock_not_available(error) {
            DbErrorKind::LockNotAvailable
        } else if contention::is_query_canceled(error) {
            DbErrorKind::QueryCanceled
        } else {
            DbErrorKind::Other
        }
    }

    /// The constraint a violation names, when the server reported one.
    pub fn constraint(&self) -> Option<&str> {
        self.0
            .as_database_error()
            .and_then(|database| database.constraint())
    }

    /// True when the statement lost a race and the identical statement or
    /// transaction can succeed on retry: `40001`, `40P01` or `55P03`, per
    /// `pplx_space_postgres_contention::is_lock_contention`.
    pub fn is_lock_contention(&self) -> bool {
        pplx_space_postgres_contention::is_lock_contention(&self.0)
    }
}

impl From<sqlx::Error> for DbError {
    fn from(error: sqlx::Error) -> Self {
        Self(error)
    }
}

/// Transition scaffolding for callers whose own error type is still
/// `sqlx::Error`: lossless, since `DbError` is the driver's error. Delete it
/// when the last such caller is gone.
impl From<DbError> for sqlx::Error {
    fn from(error: DbError) -> Self {
        error.0
    }
}

#[cfg(test)]
mod tests;
