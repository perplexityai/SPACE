//! Template-manager's cache-partition migration (20261026220935) was retired
//! after some databases had applied it. A rollout can migrate a node's
//! database from an init container while the previous template-manager still
//! serves, so a schema change must be readable by the release before it, and a
//! rollback must not need anything beyond the image.
//!
//! While the migration isn't bundled, a database that still records it is
//! returned to the bundled schema with the migration's own down SQL, but only
//! when that is lossless: no environment-partition build, tag pin or
//! unfinished stamp job exists. Otherwise migration stops before any write.
//! A catalog that bundles the version again applies it normally.

use rusqlite::{OptionalExtension as _, TransactionBehavior};
use sqlx::SqlSafeStr as _;
use sqlx::migrate::MigrationType;

use super::*;

pub(crate) const STORE: &str = "template-manager";
const VERSION: i64 = 20261026220935;
const DESCRIPTION: &str = "template_cache_partitions";
const UP_SQL: &str = include_str!(
    "../../../../db/template-manager/history/20261026220935_template_cache_partitions.up.sql"
);
const DOWN_SQL: &str = include_str!(
    "../../../../db/template-manager/history/20261026220935_template_cache_partitions.down.sql"
);

/// What reversing would lose: environment-partition builds, which also own
/// that partition's components and snapshots, environment tag pins, and stamp
/// jobs still to run for an environment. The down SQL also drops orphaned
/// children and finished environment stamp jobs; those carry nothing to keep.
const NON_LEGACY_ROWS: &str = "
    SELECT (SELECT COUNT(*) FROM template_builds WHERE cache_partition != '')
         + (SELECT COUNT(*) FROM template_pinned_tags WHERE cache_partition != '')
         + (SELECT COUNT(*) FROM jobs
            WHERE kind = 'template_stamp'
              AND status IN ('pending', 'running', 'cancelling')
              AND CASE
                    WHEN json_valid(payload) THEN json_extract(payload, '$.partition_environment')
                  END IS NOT NULL)";

/// The retired migration as SQLx recorded it, for its checksum.
fn retired_migration() -> Migration {
    Migration::new(
        VERSION,
        DESCRIPTION.into(),
        MigrationType::ReversibleUp,
        UP_SQL.into_sql_str(),
        false,
    )
}

/// The retired migration's history row, if `conn` records one.
fn recorded(conn: &Connection) -> SqliteResult<Option<(bool, Vec<u8>)>> {
    let history: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '_sqlx_migrations')",
            [],
            |row| row.get(0),
        )
        .context("failed to inspect template-manager history")?;
    if !history {
        return Ok(None);
    }
    conn.query_row(
        "SELECT success, checksum FROM _sqlx_migrations WHERE version = ?1",
        [VERSION],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .context("failed to read template-manager history")
}

/// Reverse the retired migration if `db_path` records it and `service`
/// doesn't bundle it. Runs before history validation, which would otherwise
/// reject the unbundled version.
pub(crate) fn reverse_retired_partitions(db_path: &Path, service: &Migrator) -> SqliteResult<()> {
    if service.iter().any(|migration| migration.version == VERSION) {
        return Ok(());
    }
    // Foreign keys match the SQLx connection the down SQL was written for.
    let mut conn = open_rusqlite(
        db_path,
        RusqliteOpenOptions {
            foreign_keys: true,
            ..RusqliteOpenOptions::default()
        },
    )?;
    // Probe without a lock: the serving template-manager keeps writing, and
    // most startups have nothing to reverse.
    if recorded(&conn)?.is_none() {
        return Ok(());
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to lock template-manager database")?;
    let Some((success, checksum)) = recorded(&tx)? else {
        return Ok(());
    };
    if !success || checksum != retired_migration().checksum.as_ref() {
        return Err(SqliteError::message(format!(
            "_sqlx_migrations has a failed or modified retired migration {VERSION}; see db/README.md"
        )));
    }
    let non_legacy: i64 = tx
        .query_row(NON_LEGACY_ROWS, [], |row| row.get(0))
        .context("failed to count template cache partition rows")?;
    if non_legacy != 0 {
        return Err(SqliteError::message(format!(
            "_sqlx_migrations records retired migration {VERSION} and {non_legacy} rows \
             outside the legacy template cache partition; reversing it would drop them. \
             Run a template-manager that bundles {VERSION}; see db/README.md"
        )));
    }
    tx.execute_batch(DOWN_SQL)
        .context("failed to reverse retired template cache partitions")?;
    tx.execute("DELETE FROM _sqlx_migrations WHERE version = ?1", [VERSION])
        .context("failed to remove retired template-manager history")?;
    tx.commit()
        .context("failed to commit retired template cache partition reversal")?;
    tracing::warn!(
        db.path = %db_path.display(),
        version = VERSION,
        "reversed retired template cache partition migration; every row was in the legacy partition"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
