//! Migrating and opening runtime-manager's database, which runtime-manager
//! does when its operations start: [`migrate`], then [`open`] for the one
//! connection it shares. `pplx-space-db-migrate` applies the same catalog
//! through [`MIGRATOR`] and [`catalog`].
//!
//! Runtime-manager's two deployed baseline catalogs converge through a forward
//! migration. The prototype baseline changed executable SQL, so it must not use
//! the equivalent-checksum registry. Keep its actual SQL and history intact.

use std::borrow::Cow;
use std::path::Path;
use std::sync::LazyLock;

use pplx_space_sql_locator::sql;
pub use pplx_space_sqlite::SqliteError;
use rusqlite::{Connection, OpenFlags, OptionalExtension as _};
use sqlx::SqlSafeStr as _;
use sqlx::migrate::{Migration, MigrationType, Migrator};

const DATABASE_NAME: &str = "runtime-manager";

/// Runtime-manager's own migrations, `db/runtime-manager`.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../../../../../db/runtime-manager");
const BASELINE: i64 = pplx_space_sqlite::baselines::PUBLIC_SQLITE_BASELINE;
const PROTOTYPE_SQL: &str =
    include_str!("../../../../../../../db/runtime-manager/history/prototype-baseline.sql");

static PROTOTYPE_MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| {
    let mut migrations = MIGRATOR.migrations.to_vec();
    for migration in &mut migrations {
        if migration.version == BASELINE && migration.migration_type.is_up_migration() {
            *migration = Migration::new(
                BASELINE,
                "baseline".into(),
                MigrationType::ReversibleUp,
                PROTOTYPE_SQL.into_sql_str(),
                false,
            );
        }
    }
    Migrator {
        migrations: Cow::Owned(migrations),
        ..Migrator::DEFAULT
    }
});

/// The shared schema the database also carries, `db/common`.
pub static COMMON_MIGRATOR: Migrator = sqlx::migrate!("../../../../../../db/common");

/// Select only a known recorded baseline; full validation of both service and
/// common histories still happens before any migration writes. Fresh databases
/// always use the original baseline. Unknown/failed histories remain errors.
pub fn catalog(path: &Path) -> Result<&'static Migrator, SqliteError> {
    match std::fs::metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(&MIGRATOR),
        result => {
            result.map_err(|error| {
                SqliteError::context("cannot inspect runtime-manager database", error)
            })?;
        }
    }
    let conn =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|error| {
            SqliteError::context("cannot read runtime-manager migration history", error)
        })?;
    let exists: bool = conn
        .query_row(
            sql!("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '_sqlx_migrations')"),
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            SqliteError::context("cannot inspect runtime-manager history table", error)
        })?;
    if !exists {
        return Ok(&MIGRATOR);
    }
    let checksum: Option<Vec<u8>> = conn
        .query_row(
            sql!("SELECT checksum FROM _sqlx_migrations WHERE version = ?1 AND success = 1"),
            [BASELINE],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| SqliteError::context("cannot read runtime-manager baseline", error))?;
    let prototype = PROTOTYPE_MIGRATOR
        .iter()
        .find(|m| m.version == BASELINE && m.migration_type.is_up_migration())
        .expect("prototype catalog contains its baseline");
    Ok(
        if checksum.as_deref() == Some(prototype.checksum.as_ref()) {
            &PROTOTYPE_MIGRATOR
        } else {
            &MIGRATOR
        },
    )
}

/// Applies the shared schema and runtime-manager's embedded migrations to the
/// database at `path`, selecting the catalog of its recorded baseline.
pub async fn migrate(path: &Path) -> Result<(), SqliteError> {
    pplx_space_sqlite::migrate_embedded(
        DATABASE_NAME,
        path,
        pplx_space_sqlite::EmbeddedMigrators {
            service: catalog(path)?,
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
