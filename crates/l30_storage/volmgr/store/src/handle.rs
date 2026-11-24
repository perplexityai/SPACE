//! The store's handle types: the connection every store function takes, the
//! shared connection and its lock guard, and the error they return. Callers
//! name only these; the rusqlite types they wrap stay inside the store.

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

/// A connection to volume-manager's database, as store functions take it:
/// `&Connection` also views the connection held through a
/// [`ConnectionGuard`].
#[derive(RefCastCustom)]
#[repr(transparent)]
pub struct Connection(pub(crate) rusqlite::Connection);

impl Connection {
    #[ref_cast_custom]
    fn view(raw: &rusqlite::Connection) -> &Self;

    /// Wraps a connection a test opened and seeded itself.
    #[cfg(feature = "test-support")]
    pub fn from_raw(raw: rusqlite::Connection) -> Self {
        Self(raw)
    }

    /// The raw connection, for tests that seed or inspect rows directly.
    #[cfg(feature = "test-support")]
    pub fn raw(&self) -> &rusqlite::Connection {
        &self.0
    }
}

/// The process's connection to volume-manager's database, shared behind one
/// mutex by the service and its jobs worker. A panic while it is locked
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
