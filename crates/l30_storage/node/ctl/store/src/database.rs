//! Migrating and opening node-ctl's database, which node-ctl does once at
//! startup after taking its service mutex: [`migrate`], then [`open`] for the
//! one connection it shares.

use std::path::Path;

pub use pplx_space_sqlite::SqliteError;
use rusqlite::Connection;
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
}
