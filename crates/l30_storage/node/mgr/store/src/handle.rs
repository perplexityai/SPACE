//! The store's handle types: the connection store functions take, the
//! transaction node-manager opens on it, and the error they return. Callers
//! name only these; the rusqlite types they wrap stay inside the store.

use std::ops::Deref;

use ref_cast::{RefCastCustom, ref_cast_custom};

/// A failed SQLite operation. `Display` and `source` are the underlying
/// rusqlite error's, unchanged.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] rusqlite::Error);

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A connection to node-manager's database, as store functions take it:
/// `&Connection` also views the connection a [`Transaction`] is open on, and
/// statements run through that view join the transaction.
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

/// A transaction on a [`Connection`], rolled back when dropped uncommitted.
/// It derefs to the connection, so store functions run inside it.
pub struct Transaction<'c>(rusqlite::Transaction<'c>);

impl<'c> Transaction<'c> {
    /// `BEGIN DEFERRED`: takes locks as its statements need them. It borrows
    /// the connection exclusively until it ends, as
    /// `rusqlite::Connection::transaction` does.
    pub fn deferred(conn: &'c mut Connection) -> Result<Self> {
        Ok(Self(conn.0.transaction()?))
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
