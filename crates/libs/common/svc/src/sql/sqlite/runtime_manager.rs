//! Runtime-manager's two deployed baseline catalogs converge through a forward
//! migration. The prototype baseline changed executable SQL, so it must not use
//! the equivalent-checksum registry. Keep its actual SQL and history intact.

use std::borrow::Cow;
use std::sync::LazyLock;

use rusqlite::{OpenFlags, OptionalExtension as _};
use sqlx::SqlSafeStr as _;
use sqlx::migrate::{Migration, MigrationType};

use super::*;

pub static MIGRATOR: Migrator = sqlx::migrate!("../../../../db/runtime-manager");
const BASELINE: i64 = super::super::baselines::PUBLIC_SQLITE_BASELINE;
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

/// Select only a known recorded baseline; full validation of both service and
/// common histories still happens before any migration writes. Fresh databases
/// always use the original baseline. Unknown/failed histories remain errors.
pub fn catalog(path: &Path) -> SqliteResult<&'static Migrator> {
    match std::fs::metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(&MIGRATOR),
        result => {
            result.context("cannot inspect runtime-manager database")?;
        }
    }
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .context("cannot read runtime-manager migration history")?;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '_sqlx_migrations')",
            [],
            |row| row.get(0),
        )
        .context("cannot inspect runtime-manager history table")?;
    if !exists {
        return Ok(&MIGRATOR);
    }
    let checksum: Option<Vec<u8>> = conn
        .query_row(
            "SELECT checksum FROM _sqlx_migrations WHERE version = ?1 AND success = 1",
            [BASELINE],
            |row| row.get(0),
        )
        .optional()
        .context("cannot read runtime-manager baseline")?;
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
