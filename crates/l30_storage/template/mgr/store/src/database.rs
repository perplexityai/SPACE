//! Migrating and opening template-manager's database, which template-manager
//! does once at startup after taking its service mutex: [`migrate`], then
//! [`open`] for the one connection it shares.

use std::path::Path;

pub use pplx_space_sqlite::SqliteError;
use rusqlite::Connection;
use sqlx::migrate::Migrator;

/// template-manager's embedded migrations, `db/template-manager`.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/template-manager");
/// The shared schema the database also carries, `db/common`.
pub static COMMON_MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/common");

/// Applies the shared schema and template-manager's embedded migrations to
/// the database at `path`.
pub async fn migrate(path: &Path) -> Result<(), SqliteError> {
    pplx_space_sqlite::migrate_embedded(
        "template-manager",
        path,
        pplx_space_sqlite::EmbeddedMigrators {
            service: &MIGRATOR,
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
