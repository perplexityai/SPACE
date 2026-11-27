//! Migrating and opening node-ctl's database, which node-ctl does once at
//! startup after taking its service mutex: [`migrate`], then [`open`] for the
//! one connection it shares through a [`SharedConnection`].
//!
//! The handle types here are what callers outside the store name: the
//! [`Connection`] every store function takes, the [`Transaction`] node-ctl
//! opens on it, the [`SharedConnection`] and its [`ConnectionGuard`], and the
//! [`Error`] a failed statement returns. The rusqlite types they wrap stay
//! inside the store, except through the `test-support` feature.

use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, LockResult, Mutex, MutexGuard, PoisonError};

pub use pplx_space_sqlite::SqliteError;
use ref_cast::{RefCastCustom, ref_cast_custom};
use sqlx::migrate::Migrator;

static MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/node-ctl");

/// Applies node-ctl's embedded migrations to the database at `path`.
pub async fn migrate(path: &Path) -> Result<(), SqliteError> {
    pplx_space_sqlite::migrate("node-ctl", path, &MIGRATOR).await
}

/// Opens the database at `path` with the default busy timeout, in WAL mode
/// with `synchronous=NORMAL`.
pub fn open(path: &Path) -> Result<Connection, SqliteError> {
    pplx_space_sqlite::open_rusqlite(path, pplx_space_sqlite::RusqliteOpenOptions::default())
        .map(Connection)
}

/// A failed SQLite statement. `Display` and `source` are the underlying
/// rusqlite error's, unchanged.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] rusqlite::Error);

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// rusqlite's `InvalidQuery` error, which node-ctl returns when the
    /// [`SharedConnection`]'s mutex is poisoned.
    pub fn invalid_query() -> Self {
        Self(rusqlite::Error::InvalidQuery)
    }
}

/// A connection to node-ctl's database, as store functions take it.
/// `&Connection` also views the connection a [`ConnectionGuard`] holds or a
/// [`Transaction`] runs on, and statements run through it join any
/// transaction open on that connection.
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
        Self(rusqlite::Connection::open_in_memory().expect("open in-memory SQLite"))
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
pub struct Transaction<'c>(rusqlite::Transaction<'c>);

impl<'c> Transaction<'c> {
    /// `BEGIN DEFERRED`: takes locks as its statements need them.
    pub fn deferred(conn: &'c Connection) -> Result<Self> {
        Ok(Self(conn.0.unchecked_transaction()?))
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

/// The process's connection to node-ctl's database, shared behind one mutex
/// by the gRPC service and the reconcile worker. A panic while it is locked
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
}

/// A held [`SharedConnection`] lock.
pub struct ConnectionGuard<'a>(MutexGuard<'a, rusqlite::Connection>);

impl Deref for ConnectionGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        Connection::view(&self.0)
    }
}
