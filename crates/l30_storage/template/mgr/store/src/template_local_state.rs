//! A template's node-local state across tables: its builds, their components
//! and snapshots, its pool and its settings.

use rusqlite::{Connection, Result, Transaction, params};

use pplx_space_sql_locator::sql;

/// Whether any build row of the template exists, in any partition or state.
pub fn template_has_builds(conn: &Connection, name: &str) -> Result<bool> {
    let has_builds = conn.query_row(
        sql!("SELECT EXISTS(SELECT 1 FROM template_builds WHERE name = ?1)"),
        params![name],
        |row| row.get::<_, i64>(0),
    )? != 0;
    Ok(has_builds)
}

/// Delete every row of the template's local state, child rows first since
/// nothing cascades. The deletes are only correct together, so it takes the
/// caller's transaction; the caller commits.
pub fn delete_template_rows(tx: &Transaction<'_>, name: &str) -> Result<()> {
    tx.execute(
        sql!("DELETE FROM template_snapshots WHERE name = ?1"),
        params![name],
    )?;
    tx.execute(
        sql!("DELETE FROM template_build_components WHERE name = ?1"),
        params![name],
    )?;
    tx.execute(
        sql!("DELETE FROM template_builds WHERE name = ?1"),
        params![name],
    )?;
    crate::pools::delete_pool(tx, name)?;
    tx.execute(
        sql!("DELETE FROM template_settings WHERE template = ?1"),
        params![name],
    )?;
    Ok(())
}

/// Whether the template has a snapshot, build, pool or settings row.
pub fn template_local_state_exists(conn: &Connection, name: &str) -> Result<bool> {
    let exists = conn.query_row(
        sql!(
            "SELECT EXISTS(
             SELECT 1 FROM template_snapshots WHERE name = ?1
             UNION ALL
             SELECT 1 FROM template_builds WHERE name = ?1
             UNION ALL
             SELECT 1 FROM pools WHERE template = ?1
             UNION ALL
             SELECT 1 FROM template_settings WHERE template = ?1
         )"
        ),
        params![name],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(exists != 0)
}
