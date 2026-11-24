//! `facet_sync_overrides`: an operator's pause of one facet's sync in one
//! direction, until a time or, with no time, until cleared.

use rusqlite::{Connection, OptionalExtension, Result, params};

use pplx_space_sql_locator::sql;

/// An override row. `disabled_until` is `None` for an override with no end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetSyncOverrideRow {
    pub facet: String,
    pub direction: String,
    pub disabled_until: Option<i64>,
    pub reason: String,
    pub updated_at: i64,
}

/// Set the override for the facet and direction, replacing any existing override for that pair.
pub fn upsert(
    conn: &Connection,
    facet: &str,
    direction: &str,
    disabled_until: Option<i64>,
    reason: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO facet_sync_overrides
                 (facet, direction, disabled_until, reason, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(facet, direction) DO UPDATE
             SET disabled_until = excluded.disabled_until,
                 reason = excluded.reason,
                 updated_at = excluded.updated_at"
        ),
        params![facet, direction, disabled_until, reason, now],
    )?;
    Ok(())
}

/// Delete the override for the facet and direction, if there is one.
pub fn delete(conn: &Connection, facet: &str, direction: &str) -> Result<()> {
    conn.execute(
        sql!("DELETE FROM facet_sync_overrides WHERE facet = ?1 AND direction = ?2"),
        params![facet, direction],
    )?;
    Ok(())
}

/// The override's `disabled_until`: `None` when there is no override for the
/// facet and direction, `Some(None)` when it has no end.
pub fn disabled_until(
    conn: &Connection,
    facet: &str,
    direction: &str,
) -> Result<Option<Option<i64>>> {
    conn.query_row(
        sql!(
            "SELECT disabled_until
             FROM facet_sync_overrides
             WHERE facet = ?1 AND direction = ?2"
        ),
        params![facet, direction],
        |row| row.get(0),
    )
    .optional()
}

/// The overrides with no end or that end after `now`, ordered by facet and
/// direction.
pub fn active(conn: &Connection, now: i64) -> Result<Vec<FacetSyncOverrideRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT facet, direction, disabled_until, reason, updated_at
         FROM facet_sync_overrides
         WHERE disabled_until IS NULL OR disabled_until > ?1
         ORDER BY facet ASC, direction ASC"
    ))?;
    let rows = stmt.query_map(params![now], |row| {
        let direction = row.get(1)?;
        let disabled_until = row.get(2)?;
        Ok(FacetSyncOverrideRow {
            facet: row.get(0)?,
            direction,
            disabled_until,
            reason: row.get(3)?,
            updated_at: row.get(4)?,
        })
    })?;
    rows.collect()
}
