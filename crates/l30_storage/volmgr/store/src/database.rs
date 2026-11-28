//! Migrating and opening volume-manager's database. volume-manager runs
//! [`migrate`] once at startup after taking its service mutex, and its
//! service then takes its one connection from [`open`] and shares it through
//! a [`SharedConnection`](crate::SharedConnection).

use std::path::Path;

pub use pplx_space_sqlite::SqliteError;
use sqlx::migrate::Migrator;

use crate::{Connection, Result};

static MIGRATOR: Migrator = sqlx::migrate!("../../../../db/volume-manager");
/// The shared schema the database also carries, `db/common`.
pub static COMMON_MIGRATOR: Migrator = sqlx::migrate!("../../../../db/common");

/// Applies the shared schema and volume-manager's embedded migrations to the
/// database at `path`.
pub async fn migrate(path: &Path) -> Result<(), SqliteError> {
    pplx_space_sqlite::migrate_embedded(
        "volume-manager",
        path,
        pplx_space_sqlite::EmbeddedMigrators {
            service: &MIGRATOR,
            common: Some(&COMMON_MIGRATOR),
        },
    )
    .await
}

/// Opens the database at `path` with `rusqlite::Connection::open` alone:
/// rusqlite's default busy timeout and no PRAGMAs.
pub fn open(path: &Path) -> Result<Connection> {
    Ok(Connection(rusqlite::Connection::open(path)?))
}
