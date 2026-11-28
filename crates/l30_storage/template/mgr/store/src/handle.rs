//! The store's handle types: the connection every store function takes, the
//! immediate transaction some take, the shared connection and its lock guard,
//! and the error they return. Callers name only these; the rusqlite types
//! they wrap stay inside the store.

use std::ops::Deref;
use std::sync::{Arc, LockResult, Mutex, MutexGuard, PoisonError};

use ref_cast::{RefCastCustom, ref_cast_custom};

use crate::jobs::{JobExecutor, JobWorker, WorkerConfig};

/// A failed SQLite operation. `Display` and `source` are the underlying
/// rusqlite error's, unchanged.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] rusqlite::Error);

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// A value could not be converted for storage: rusqlite's
    /// `ToSqlConversionFailure` over `error`, as template-manager reports a
    /// build it refuses to record, a poisoned connection mutex or a failed
    /// blocking task.
    pub fn to_sql_conversion(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(rusqlite::Error::ToSqlConversionFailure(error.into()))
    }

    /// An `SQLITE_IOERR` failure carrying `message`, as template-manager
    /// reports a disk failure or a poisoned connection mutex in place of a
    /// statement's error.
    pub fn io_failure(message: String) -> Self {
        Self(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
            Some(message),
        ))
    }

    /// The underlying error, for tests that check its kind.
    #[cfg(feature = "test-support")]
    pub fn raw(&self) -> &rusqlite::Error {
        &self.0
    }
}

/// A connection to template-manager's database, as store functions take it:
/// `&Connection` also views the connection held through a
/// [`ConnectionGuard`] or a [`Transaction`], and statements run through it
/// join any transaction open on that connection.
#[derive(RefCastCustom)]
#[repr(transparent)]
pub struct Connection(pub(crate) rusqlite::Connection);

impl Connection {
    #[ref_cast_custom]
    fn view(raw: &rusqlite::Connection) -> &Self;

    /// An empty in-memory database, without the schema.
    #[cfg(feature = "test-support")]
    #[allow(
        clippy::expect_used,
        reason = "test-support only: a test that cannot open an in-memory database has nothing to run against"
    )]
    pub fn open_in_memory() -> Self {
        Self(rusqlite::Connection::open_in_memory().expect("open an in-memory SQLite database"))
    }

    /// The underlying connection, for tests that seed or inspect the
    /// database directly.
    #[cfg(feature = "test-support")]
    pub fn raw(&self) -> &rusqlite::Connection {
        &self.0
    }
}

/// A transaction on a [`Connection`], rolled back when dropped uncommitted.
/// It derefs to the connection, so store functions run inside it.
pub struct Transaction<'c>(pub(crate) rusqlite::Transaction<'c>);

impl<'c> Transaction<'c> {
    /// `BEGIN IMMEDIATE`: takes the write lock up front.
    pub fn immediate(conn: &'c Connection) -> Result<Self> {
        Ok(Self(rusqlite::Transaction::new_unchecked(
            &conn.0,
            rusqlite::TransactionBehavior::Immediate,
        )?))
    }

    pub fn commit(self) -> Result<()> {
        Ok(self.0.commit()?)
    }
}

impl Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        Connection::view(&self.0)
    }
}

/// The process's connection to template-manager's database, shared behind
/// one mutex by every template-manager subsystem and its jobs worker. A panic
/// while it is locked poisons it, as for any [`Mutex`].
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

    /// A jobs worker over this connection, sharing its mutex.
    pub fn background_worker<E: JobExecutor>(
        &self,
        executor: E,
        config: WorkerConfig,
    ) -> JobWorker<E> {
        JobWorker::new(pplx_space_common_jobs::BackgroundWorker::new(
            Arc::clone(&self.0),
            executor,
            config,
        ))
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
