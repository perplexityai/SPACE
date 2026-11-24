//! Opening build-manager's database, which build-manager does once at
//! startup: [`open`] for the one connection it shares through a
//! [`SharedConnection`]. build-manager does not migrate it:
//! `pplx-space-db-migrate` applies the schema before build-manager starts.
//!
//! The handle types here are what callers outside the store name: the
//! [`Connection`] every store function takes, the [`SharedConnection`] the
//! service and its jobs worker share, its [`ConnectionGuard`], and the
//! [`Error`] a failed statement returns. The rusqlite types they wrap stay
//! inside the store, except through the `test-support` feature.

use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, LockResult, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ref_cast::{RefCastCustom, ref_cast_custom};

use crate::jobs::{JobExecutor, JobWorker, WorkerConfig};

/// Opens the database at `path` with `rusqlite::Connection::open`, a 30
/// second busy timeout, and then `journal_mode=WAL`, `synchronous=NORMAL` and
/// `foreign_keys=ON`.
pub fn open(path: &Path) -> Result<Connection, Error> {
    let conn = rusqlite::Connection::open(path)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA foreign_keys=ON;",
    )?;
    Ok(Connection(conn))
}

/// A failed SQLite operation. `Display` and `source` are the underlying
/// rusqlite error's, unchanged.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] rusqlite::Error);

impl Error {
    /// A value could not be converted for storage.
    pub fn to_sql_conversion(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self(rusqlite::Error::ToSqlConversionFailure(error.into()))
    }
}

/// A connection to build-manager's database, as store functions take it.
/// `&Connection` also views the connection a [`ConnectionGuard`] holds.
#[derive(Debug, RefCastCustom)]
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

    /// Wraps a connection a test opened itself.
    #[cfg(feature = "test-support")]
    pub fn from_raw(raw: rusqlite::Connection) -> Self {
        Self(raw)
    }

    /// The underlying connection, for tests that seed or inspect the
    /// database directly.
    #[cfg(feature = "test-support")]
    pub fn raw(&self) -> &rusqlite::Connection {
        &self.0
    }
}

/// The process's connection to build-manager's database, shared behind one
/// mutex by the service, its build executor and its jobs worker. A panic
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
