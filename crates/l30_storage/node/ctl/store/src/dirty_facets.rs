//! The `dirty_facets` queue: one row per facet, or per element of a facet,
//! that node-ctl has to sync. The empty element stands for the whole facet.

use rusqlite::{Connection, OptionalExtension, Result, params};

use pplx_space_sql_locator::sql;

/// A claimed row: the facet, its element and the generation that was claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedDirtyFacetRow {
    pub facet: String,
    pub element: String,
    pub dirty_generation: i64,
}

/// A dirty row as the queue listing shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyFacetQueueRow {
    pub facet: String,
    pub element: String,
    pub attempts: i64,
    pub last_error: String,
    pub updated_at: i64,
    pub last_attempt_at: i64,
}

/// Delete the facet's per-element rows, leaving its whole-facet row. Run it in
/// the transaction that then marks the whole facet dirty.
pub fn delete_element_rows(conn: &Connection, facet: &str) -> Result<()> {
    conn.execute(
        sql!("DELETE FROM dirty_facets WHERE facet = ?1 AND element != ''"),
        params![facet],
    )?;
    Ok(())
}

/// The `dirty` flag of the facet's whole-facet row, or `None` when it has
/// none. Run it in the transaction whose write depends on it.
pub fn whole_facet_dirty(conn: &Connection, facet: &str) -> Result<Option<i64>> {
    conn.query_row(
        sql!("SELECT dirty FROM dirty_facets WHERE facet = ?1 AND element = ''"),
        params![facet],
        |row| row.get(0),
    )
    .optional()
}

/// Mark the element dirty at `now`: insert its row, or bump its generation
/// and reset its retry, dead-letter and claim state.
pub fn mark_dirty(conn: &Connection, facet: &str, element: &str, now: i64) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO dirty_facets
                 (facet, element, dirty, dirty_generation, attempts, last_error,
                  updated_at, last_attempt_at, next_attempt_at, claim_token, claimed_at)
             VALUES (?1, ?2, 1, 1, 0, '', ?3, 0, ?3, '', 0)
             ON CONFLICT(facet, element) DO UPDATE
             SET dirty = 1,
                 dirty_generation = dirty_generation + 1,
                 attempts = 0,
                 last_error = '',
                 dead_at = 0,
                 dead_reason = '',
                 updated_at = ?3,
                 next_attempt_at = ?3,
                 claim_token = '',
                 claimed_at = 0"
        ),
        params![facet, element, now],
    )?;
    Ok(())
}

/// The dirty rows that `claim_token` claimed, ordered by facet and element.
pub fn claimed(conn: &Connection, claim_token: &str) -> Result<Vec<ClaimedDirtyFacetRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT facet, element, dirty_generation
         FROM dirty_facets
         WHERE dirty != 0
           AND claim_token = ?1
         ORDER BY facet ASC, element ASC"
    ))?;
    let rows = stmt.query_map(params![claim_token], |row| {
        Ok(ClaimedDirtyFacetRow {
            facet: row.get(0)?,
            element: row.get(1)?,
            dirty_generation: row.get(2)?,
        })
    })?;
    rows.collect()
}

/// Every dirty row, ordered by facet and element.
pub fn queue(conn: &Connection) -> Result<Vec<DirtyFacetQueueRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT facet, element, attempts, last_error, updated_at, last_attempt_at
         FROM dirty_facets
         WHERE dirty != 0
         ORDER BY facet ASC, element ASC"
    ))?;
    let rows = stmt.query_map([], queue_row)?;
    rows.collect()
}

/// The facet's dirty rows, ordered by element.
pub fn facet_queue(conn: &Connection, facet: &str) -> Result<Vec<DirtyFacetQueueRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT facet, element, attempts, last_error, updated_at, last_attempt_at
         FROM dirty_facets
         WHERE dirty != 0 AND facet = ?1
         ORDER BY facet ASC, element ASC"
    ))?;
    let rows = stmt.query_map(params![facet], queue_row)?;
    rows.collect()
}

fn queue_row(row: &rusqlite::Row<'_>) -> Result<DirtyFacetQueueRow> {
    let element = row.get(1)?;
    Ok(DirtyFacetQueueRow {
        facet: row.get(0)?,
        element,
        attempts: row.get(2)?,
        last_error: row.get(3)?,
        updated_at: row.get(4)?,
        last_attempt_at: row.get(5)?,
    })
}

/// The element's failed-attempt count, or `None` when it has no row. Run it
/// in the transaction whose write depends on it.
pub fn attempts(conn: &Connection, facet: &str, element: &str) -> Result<Option<i64>> {
    conn.query_row(
        sql!("SELECT attempts FROM dirty_facets WHERE facet = ?1 AND element = ?2"),
        params![facet, element],
        |row| row.get(0),
    )
    .optional()
}
