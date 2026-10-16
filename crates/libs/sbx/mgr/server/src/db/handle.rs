//! The store's handle types: the shared connection and its lock guard, the
//! connection view every `db` function takes, transactions, and the error
//! they return. Callers outside `db` name only these; the rusqlite types they
//! wrap stay inside the store.

use std::ops::Deref;
use std::sync::{Arc, LockResult, Mutex, MutexGuard, PoisonError};

use ref_cast::{RefCastCustom, ref_cast_custom};

/// A method that returns or takes rusqlite values: crate-visible in test
/// builds, for fixtures and tripwires outside `db`, and private to `db`
/// otherwise, so production code outside the store cannot reach a raw
/// connection, statement or error.
macro_rules! db_private_outside_tests {
    ($(#[$attr:meta])* fn $name:ident $($rest:tt)*) => {
        $(#[$attr])*
        #[cfg(test)]
        pub(crate) fn $name $($rest)*

        $(#[$attr])*
        #[cfg(not(test))]
        pub(in crate::db) fn $name $($rest)*
    };
}

/// A failed SQLite operation. `Display` and `source` are the underlying
/// rusqlite error's, unchanged.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] rusqlite::Error);

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// A guarded write's precondition failed: the store's "stale or
    /// conflicting" refusal.
    pub fn invalid_query() -> Self {
        Self(rusqlite::Error::InvalidQuery)
    }

    /// The value in `column` of SQL type `kind` did not decode.
    pub(in crate::db) fn decode(
        column: usize,
        kind: rusqlite::types::Type,
        error: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self(rusqlite::Error::FromSqlConversionFailure(
            column,
            kind,
            error.into(),
        ))
    }

    /// The statement matched no row.
    pub fn no_rows() -> Self {
        Self(rusqlite::Error::QueryReturnedNoRows)
    }

    /// A lock protecting store state was poisoned; `message` says which.
    pub fn poisoned(message: String) -> Self {
        Self(rusqlite::Error::InvalidParameterName(message))
    }

    /// A value could not be converted for storage.
    pub fn to_sql_conversion(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(rusqlite::Error::ToSqlConversionFailure(error.into()))
    }

    /// The statement matched no row.
    pub fn is_no_rows(&self) -> bool {
        matches!(self.0, rusqlite::Error::QueryReturnedNoRows)
    }

    /// SQLite reported the database busy.
    #[cfg(test)]
    pub(crate) fn is_database_busy(&self) -> bool {
        self.0.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy)
    }

    /// The column and SQL type of a value that failed to decode.
    #[cfg(test)]
    pub(crate) fn conversion_failure(&self) -> Option<(usize, rusqlite::types::Type)> {
        match &self.0 {
            rusqlite::Error::FromSqlConversionFailure(column, kind, _) => Some((*column, *kind)),
            _ => None,
        }
    }

    /// See [`Self::invalid_query`].
    pub fn is_invalid_query(&self) -> bool {
        matches!(self.0, rusqlite::Error::InvalidQuery)
    }

    db_private_outside_tests! {
        fn raw(&self) -> &rusqlite::Error {
            &self.0
        }
    }

    #[cfg(test)]
    pub(crate) fn into_raw(self) -> rusqlite::Error {
        self.0
    }
}

/// [`rusqlite::OptionalExtension`] for the store's [`Result`].
pub(crate) trait OptionalExtension<T> {
    /// `Ok(None)` when the statement matched no row.
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalExtension<T> for Result<T> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.is_no_rows() => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// A connection to sbx-mgr's database, as `db` functions take it: `&Connection`
/// views the connection held through a [`ConnectionGuard`] or a
/// [`Transaction`], and statements run through it join any transaction open on
/// that connection.
#[derive(RefCastCustom)]
#[repr(transparent)]
pub struct Connection(rusqlite::Connection);

impl Connection {
    #[ref_cast_custom]
    fn view(raw: &rusqlite::Connection) -> &Self;

    pub(in crate::db) fn from_raw(raw: rusqlite::Connection) -> Self {
        Self(raw)
    }

    /// An empty in-memory database, without the schema.
    #[cfg(test)]
    pub(crate) fn open_in_memory() -> Self {
        Self(rusqlite::Connection::open_in_memory().expect("open an in-memory SQLite database"))
    }

    /// A plain connection to the database file at `path`, without the
    /// production open's settings.
    #[cfg(test)]
    pub(crate) fn open_at(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self(rusqlite::Connection::open(path)?))
    }

    #[cfg(test)]
    pub(crate) fn pragma_update<V: rusqlite::ToSql>(
        &self,
        schema: Option<&str>,
        name: &str,
        value: V,
    ) -> Result<()> {
        Ok(self.0.pragma_update(schema, name, value)?)
    }

    #[cfg(test)]
    pub(crate) fn pragma_query_value<T, F>(
        &self,
        schema: Option<&str>,
        name: &str,
        f: F,
    ) -> Result<T>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        Ok(self.0.pragma_query_value(schema, name, f)?)
    }

    #[cfg(test)]
    pub(crate) fn busy_timeout(&self, timeout: std::time::Duration) -> Result<()> {
        Ok(self.0.busy_timeout(timeout)?)
    }

    /// [`rusqlite::Connection::trace_v2`], for tests that observe the
    /// statements a store operation runs.
    #[cfg(test)]
    pub(crate) fn trace_v2(
        &self,
        mask: rusqlite::trace::TraceEventCodes,
        trace: Option<fn(rusqlite::trace::TraceEvent<'_>)>,
    ) {
        self.0.trace_v2(mask, trace);
    }

    db_private_outside_tests! {
        fn raw(&self) -> &rusqlite::Connection {
            &self.0
        }
    }

    db_private_outside_tests! {
        fn execute<P: rusqlite::Params>(&self, sql: &str, params: P) -> Result<usize> {
            Ok(self.0.execute(sql, params)?)
        }
    }

    #[cfg(test)]
    pub(crate) fn execute_batch(&self, sql: &str) -> Result<()> {
        Ok(self.0.execute_batch(sql)?)
    }

    db_private_outside_tests! {
        fn prepare(&self, sql: &str) -> Result<rusqlite::Statement<'_>> {
            Ok(self.0.prepare(sql)?)
        }
    }

    db_private_outside_tests! {
        fn prepare_cached(&self, sql: &str) -> Result<rusqlite::CachedStatement<'_>> {
            Ok(self.0.prepare_cached(sql)?)
        }
    }

    db_private_outside_tests! {
        fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
        where
            P: rusqlite::Params,
            F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
        {
            Ok(self.0.query_row(sql, params, f)?)
        }
    }

    pub(crate) fn is_autocommit(&self) -> bool {
        self.0.is_autocommit()
    }
}

/// A transaction on a [`Connection`], rolled back when dropped uncommitted.
/// It derefs to the connection, so `db` functions run inside it.
pub struct Transaction<'c>(rusqlite::Transaction<'c>);

impl<'c> Transaction<'c> {
    /// `BEGIN IMMEDIATE`: takes the write lock up front.
    pub fn immediate(conn: &'c Connection) -> Result<Self> {
        Ok(Self(rusqlite::Transaction::new_unchecked(
            &conn.0,
            rusqlite::TransactionBehavior::Immediate,
        )?))
    }

    /// `BEGIN DEFERRED`: takes locks as its statements need them.
    pub fn deferred(conn: &'c Connection) -> Result<Self> {
        Ok(Self(conn.0.unchecked_transaction()?))
    }

    pub fn commit(self) -> Result<()> {
        Ok(self.0.commit()?)
    }

    pub fn rollback(self) -> Result<()> {
        Ok(self.0.rollback()?)
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        Connection::view(&self.0)
    }
}

/// The process's connection to sbx-mgr's database, shared behind one mutex
/// by every writer, including the jobs worker. A panic while it is locked
/// poisons it, as for any [`Mutex`].
#[derive(Clone)]
pub struct SharedConnection(Arc<Mutex<rusqlite::Connection>>);

impl SharedConnection {
    pub fn new(conn: Connection) -> Self {
        Self(Arc::new(Mutex::new(conn.0)))
    }

    /// [`Mutex::lock`]: a poisoned mutex still yields its guard through the
    /// [`PoisonError`].
    pub fn lock(&self) -> LockResult<ConnectionGuard<'_>> {
        match self.0.lock() {
            Ok(guard) => Ok(ConnectionGuard(guard)),
            Err(poisoned) => Err(PoisonError::new(ConnectionGuard(poisoned.into_inner()))),
        }
    }

    /// [`Mutex::is_poisoned`].
    pub fn is_poisoned(&self) -> bool {
        self.0.is_poisoned()
    }

    /// [`Mutex::try_lock`].
    #[cfg(test)]
    pub(crate) fn try_lock(&self) -> std::sync::TryLockResult<ConnectionGuard<'_>> {
        use std::sync::TryLockError;
        match self.0.try_lock() {
            Ok(guard) => Ok(ConnectionGuard(guard)),
            Err(TryLockError::Poisoned(poisoned)) => Err(TryLockError::Poisoned(PoisonError::new(
                ConnectionGuard(poisoned.into_inner()),
            ))),
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
        }
    }

    /// A jobs worker over this connection, sharing its mutex.
    pub fn background_worker<E: pplx_space_common_jobs::JobExecutor>(
        &self,
        executor: E,
        config: pplx_space_common_jobs::WorkerConfig,
    ) -> pplx_space_common_jobs::BackgroundWorker<E> {
        pplx_space_common_jobs::BackgroundWorker::new(Arc::clone(&self.0), executor, config)
    }
}

/// A held [`SharedConnection`] lock.
pub struct ConnectionGuard<'a>(MutexGuard<'a, rusqlite::Connection>);

impl Deref for ConnectionGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        Connection::view(&self.0)
    }
}
