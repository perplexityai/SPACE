//! Migrating and opening node-manager's database. node-manager runs
//! [`migrate`] once at startup after taking its service mutex, checks the
//! migrated rows through [`open_read_only`] before changing host limits, and
//! the slot pool then takes its one connection from [`open`].

use std::path::Path;
use std::time::Duration;

pub use pplx_space_sqlite::SqliteError;
use rusqlite::OpenFlags;
use sqlx::migrate::Migrator;

use crate::{Connection, Result};

/// node-manager's embedded migrations, `db/node-manager`.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/node-manager");

/// Applies node-manager's embedded migrations to the database at `path`.
pub async fn migrate(path: &Path) -> Result<(), SqliteError> {
    pplx_space_sqlite::migrate("node-manager", path, &MIGRATOR).await
}

/// Opens the database at `path` with the default busy timeout, in WAL mode
/// with `synchronous=NORMAL` and foreign keys enforced.
pub fn open(path: &Path) -> Result<Connection, SqliteError> {
    pplx_space_sqlite::open_rusqlite(
        path,
        pplx_space_sqlite::RusqliteOpenOptions {
            foreign_keys: true,
            ..Default::default()
        },
    )
    .map(Connection)
}

/// Opens the database at `path` with `SQLITE_OPEN_READ_ONLY` alone and a
/// five-second busy timeout: no PRAGMAs, and no write the open could make.
pub fn open_read_only(path: &Path) -> Result<Connection> {
    let conn = rusqlite::Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(Connection(conn))
}
