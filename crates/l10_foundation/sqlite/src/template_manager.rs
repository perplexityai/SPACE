//! Template-manager's cache-partition migration (20261026220935) was retired
//! after some databases had applied it. A rollout can migrate a node's
//! database from an init container while the previous template-manager still
//! serves, so a schema change must be readable by the release before it, and a
//! rollback must not need anything beyond the image.
//!
//! So a release can find a migration recorded that it doesn't bundle: a
//! retired one, or the next release's after an image rollback. While a
//! migration in [`UNBUNDLED`] isn't bundled, a database that still records it
//! is returned to the bundled schema with the migration's own down SQL, but
//! only when the migration's check finds no data worth keeping that reversing
//! would drop; each entry says what reversing discards. Otherwise migration
//! stops before any write. A catalog that bundles the version applies it
//! normally. A migration ships only in a release after one that lists it, so
//! the image a rollback returns to can reverse it. 20261031050126 ships that
//! way, and its check never refuses, so rolling back from the release that
//! bundles it to the one that lists it is image-only.

use rusqlite::{OptionalExtension as _, TransactionBehavior};
use sqlx::SqlSafeStr as _;
use sqlx::migrate::MigrationType;

use super::*;

pub(crate) const STORE: &str = "template-manager";
/// The retired cache-partition migration.
const VERSION: i64 = 20261026220935;

/// A migration this release can find recorded without bundling it.
struct Unbundled {
    version: i64,
    description: &'static str,
    /// The up SQL the recording release applied, for its checksum.
    up_sql: &'static str,
    down_sql: &'static str,
    /// How messages name it.
    label: &'static str,
    /// Refuses a database whose reversal would drop data worth keeping. Runs
    /// for every recorded migration before any of them is reversed.
    lossless: fn(&Connection) -> SqliteResult<()>,
    /// Logged once reversed.
    reversed: &'static str,
}

const RETIRED_PARTITIONS: Unbundled = Unbundled {
    version: VERSION,
    description: "template_cache_partitions",
    up_sql: include_str!(
        "../../../../db/template-manager/history/20261026220935_template_cache_partitions.up.sql"
    ),
    down_sql: include_str!(
        "../../../../db/template-manager/history/20261026220935_template_cache_partitions.down.sql"
    ),
    label: "retired",
    lossless: legacy_partition_only,
    reversed: "reversed retired template cache partition migration; every row was in the legacy partition",
};

/// Bundled from this release on. The release before it lists the same up SQL
/// and reverses the migration when this image is rolled back, so the bundled
/// file must keep that checksum.
const DIRTY_REF_REVISIONS: Unbundled = Unbundled {
    version: 20261031050126,
    description: "node dirty ref revisions",
    up_sql: include_str!(
        "../../../../db/template-manager/history/20261031050126_node_dirty_ref_revisions.up.sql"
    ),
    down_sql: include_str!(
        "../../../../db/template-manager/history/20261031050126_node_dirty_ref_revisions.down.sql"
    ),
    label: "newer",
    lossless: dirty_ref_revisions_lossless,
    reversed: "reversed newer node dirty ref revision migration; every outbox row was kept",
};

/// Reversed newest first, whatever their order here.
const UNBUNDLED: &[Unbundled] = &[DIRTY_REF_REVISIONS, RETIRED_PARTITIONS];

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

fn legacy_partition_only(conn: &Connection) -> SqliteResult<()> {
    let non_legacy: i64 = conn
        .query_row(NON_LEGACY_ROWS, [], |row| row.get(0))
        .context("failed to count template cache partition rows")?;
    if non_legacy != 0 {
        return Err(SqliteError::message(format!(
            "_sqlx_migrations records retired migration {VERSION} and {non_legacy} rows \
             outside the legacy template cache partition; reversing it would drop them. \
             Run a template-manager that bundles {VERSION}; see db/README.md"
        )));
    }
    Ok(())
}

/// Never refuses: the down SQL drops only the revision column, its counter
/// and the triggers that stamp it, so every outbox row stays, and a release
/// without the migration acknowledges a row by its key. What it gives up is
/// the revision itself: that release can again lose a re-enqueue made while a
/// send is in flight, and re-applying the migration restarts revisions at 0.
fn dirty_ref_revisions_lossless(_: &Connection) -> SqliteResult<()> {
    Ok(())
}

impl Unbundled {
    /// The migration as SQLx recorded it, for its checksum.
    fn migration(&self) -> Migration {
        Migration::new(
            self.version,
            self.description.into(),
            MigrationType::ReversibleUp,
            self.up_sql.into_sql_str(),
            false,
        )
    }
}

/// The retired migration as SQLx recorded it, for its checksum.
#[cfg(test)]
fn retired_migration() -> Migration {
    RETIRED_PARTITIONS.migration()
}

fn has_history(conn: &Connection) -> SqliteResult<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = '_sqlx_migrations')",
        [],
        |row| row.get(0),
    )
    .context("failed to inspect template-manager history")
}

/// The history row of `version`, if `conn` records one.
fn recorded(conn: &Connection, version: i64) -> SqliteResult<Option<(bool, Vec<u8>)>> {
    conn.query_row(
        "SELECT success, checksum FROM _sqlx_migrations WHERE version = ?1",
        [version],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .context("failed to read template-manager history")
}

/// Reverse every migration in [`UNBUNDLED`] that `db_path` records and
/// `service` doesn't bundle, all in one transaction. Runs before history
/// validation, which would otherwise reject the unbundled versions.
pub(crate) fn reverse_unbundled(db_path: &Path, service: &Migrator) -> SqliteResult<()> {
    let mut candidates = UNBUNDLED
        .iter()
        .filter(|unbundled| {
            !service
                .iter()
                .any(|migration| migration.version == unbundled.version)
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|unbundled| std::cmp::Reverse(unbundled.version));
    if candidates.is_empty() {
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
    if !has_history(&conn)? {
        return Ok(());
    }
    let mut any_recorded = false;
    for unbundled in &candidates {
        any_recorded |= recorded(&conn, unbundled.version)?.is_some();
    }
    if !any_recorded {
        return Ok(());
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("failed to lock template-manager database")?;
    let mut reversing = Vec::new();
    for unbundled in candidates {
        let Some((success, checksum)) = recorded(&tx, unbundled.version)? else {
            continue;
        };
        if !success || checksum != unbundled.migration().checksum.as_ref() {
            return Err(SqliteError::message(format!(
                "_sqlx_migrations has a failed or modified {} migration {}; see db/README.md",
                unbundled.label, unbundled.version
            )));
        }
        (unbundled.lossless)(&tx)?;
        reversing.push(unbundled);
    }
    for unbundled in &reversing {
        tx.execute_batch(unbundled.down_sql).with_context(|| {
            format!(
                "failed to reverse {} template-manager migration {}",
                unbundled.label, unbundled.version
            )
        })?;
        tx.execute(
            "DELETE FROM _sqlx_migrations WHERE version = ?1",
            [unbundled.version],
        )
        .with_context(|| {
            format!(
                "failed to remove {} template-manager history {}",
                unbundled.label, unbundled.version
            )
        })?;
    }
    tx.commit()
        .context("failed to commit template-manager migration reversal")?;
    for unbundled in reversing {
        tracing::warn!(
            db.path = %db_path.display(),
            version = unbundled.version,
            "{}",
            unbundled.reversed
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
