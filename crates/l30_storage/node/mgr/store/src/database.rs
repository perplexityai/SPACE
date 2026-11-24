//! Migrating and opening node-manager's database. node-manager runs
//! [`migrate`] once at startup after taking its service mutex, and the slot
//! pool then takes its one connection from [`open`].

use std::path::Path;

pub use pplx_space_sqlite::SqliteError;
use rusqlite::Connection;
use sqlx::migrate::Migrator;

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
}
