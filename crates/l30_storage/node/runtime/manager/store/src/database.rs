//! Migrating and opening runtime-manager's database, which runtime-manager
//! does when its operations start: [`migrate`], then [`open`] for the one
//! connection it shares.

use std::path::Path;

pub use pplx_space_sqlite::SqliteError;
use rusqlite::Connection;
use sqlx::migrate::Migrator;

const DATABASE_NAME: &str = "runtime-manager";

/// The shared schema the database also carries, `db/common`.
pub static COMMON_MIGRATOR: Migrator = sqlx::migrate!("../../../../../../db/common");

/// Applies the shared schema and runtime-manager's embedded migrations to the
/// database at `path`, selecting the catalog of its recorded baseline.
pub async fn migrate(path: &Path) -> Result<(), SqliteError> {
    pplx_space_sqlite::migrate_embedded(
        DATABASE_NAME,
        path,
        pplx_space_sqlite::EmbeddedMigrators {
            service: pplx_space_sqlite::runtime_manager::catalog(path)?,
            common: Some(&COMMON_MIGRATOR),
        },
    )
    .await
}

/// Opens the database at `path` with the default busy timeout, in WAL mode
/// with `synchronous=NORMAL`.
pub fn open(path: &Path) -> Result<Connection, SqliteError> {
    pplx_space_sqlite::open_rusqlite(path, pplx_space_sqlite::RusqliteOpenOptions::default())
}
