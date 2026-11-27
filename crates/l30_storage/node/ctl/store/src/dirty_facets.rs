//! The `dirty_facets` queue: one row per facet, or per element of a facet,
//! that node-ctl has to sync. The empty element stands for the whole facet.

use rusqlite::{OptionalExtension, params};

use pplx_space_sql_locator::sql;

use crate::database::{Connection, Result};

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
    conn.0.execute(
        sql!("DELETE FROM dirty_facets WHERE facet = ?1 AND element != ''"),
        params![facet],
    )?;
    Ok(())
}

/// The `dirty` flag of the facet's whole-facet row, or `None` when it has
/// none. Run it in the transaction whose write depends on it.
pub fn whole_facet_dirty(conn: &Connection, facet: &str) -> Result<Option<i64>> {
    Ok(conn
        .0
        .query_row(
            sql!("SELECT dirty FROM dirty_facets WHERE facet = ?1 AND element = ''"),
            params![facet],
            |row| row.get(0),
        )
        .optional()?)
}

/// Mark the element dirty at `now`: insert its row, or bump its generation
/// and reset its retry, dead-letter and claim state.
pub fn mark_dirty(conn: &Connection, facet: &str, element: &str, now: i64) -> Result<()> {
    conn.0.execute(
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

/// Claim every due dirty row for `claim_token` at `now`: rows that are not
/// dead-lettered, whose retry is due, and that are unclaimed or whose claim
/// is at or before `stale_before`. Returns how many rows it claimed; another
/// claim holding every due row, or no row being due, claims none.
pub fn claim_due(
    conn: &Connection,
    claim_token: &str,
    now: i64,
    stale_before: i64,
) -> Result<usize> {
    Ok(conn.0.execute(
        sql!(
            "UPDATE dirty_facets
             SET claim_token = ?1,
                 claimed_at = ?2,
                 last_attempt_at = ?2
             WHERE dirty != 0
               AND next_attempt_at <= ?2
               AND dead_at = 0
               AND (
                   claim_token = ''
                   OR claimed_at <= ?3
               )"
        ),
        params![claim_token, now, stale_before],
    )?)
}

/// Delete the element's row if `claim_token` still holds it at
/// `dirty_generation`. Returns `false` when the row is gone, another claim
/// took it, or it was marked dirty again since the claim.
pub fn delete_synced(
    conn: &Connection,
    facet: &str,
    element: &str,
    dirty_generation: i64,
    claim_token: &str,
) -> Result<bool> {
    let changed = conn.0.execute(
        sql!(
            "DELETE FROM dirty_facets
             WHERE facet = ?1
               AND element = ?2
               AND dirty_generation = ?3
               AND claim_token = ?4"
        ),
        params![facet, element, dirty_generation, claim_token],
    )?;
    Ok(changed != 0)
}

/// Release `claim_token`'s claim on the element's row, leaving the row
/// otherwise unchanged. Returns `false` when the row is gone or the token no
/// longer holds it.
pub fn release_claim(
    conn: &Connection,
    facet: &str,
    element: &str,
    claim_token: &str,
) -> Result<bool> {
    let changed = conn.0.execute(
        sql!(
            "UPDATE dirty_facets
             SET claim_token = '',
                 claimed_at = 0
             WHERE facet = ?1
               AND element = ?2
               AND claim_token = ?3"
        ),
        params![facet, element, claim_token],
    )?;
    Ok(changed != 0)
}

/// A failed sync attempt on a claimed row, recorded by
/// [`dead_letter_failed_attempt`] or [`back_off_failed_attempt`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailedAttempt<'a> {
    pub facet: &'a str,
    pub element: &'a str,
    /// The generation the claim took.
    pub dirty_generation: i64,
    pub claim_token: &'a str,
    pub error: &'a str,
    pub failed_at: i64,
}

/// Count the failed attempt, dead-letter the row with the error as its
/// reason and release its claim, if `claim_token` still holds it at
/// `dirty_generation`. Returns `false` when the row is gone, another claim
/// took it, or it was marked dirty again since the claim.
pub fn dead_letter_failed_attempt(conn: &Connection, attempt: &FailedAttempt<'_>) -> Result<bool> {
    let changed = conn.0.execute(
        sql!(
            "UPDATE dirty_facets
             SET attempts = attempts + 1,
                 last_error = ?3,
                 last_attempt_at = ?4,
                 dead_at = ?4,
                 dead_reason = ?3,
                 claim_token = '',
                 claimed_at = 0,
                 updated_at = ?4
             WHERE facet = ?1
               AND element = ?2
               AND dirty_generation = ?5
               AND claim_token = ?6"
        ),
        params![
            attempt.facet,
            attempt.element,
            attempt.error,
            attempt.failed_at,
            attempt.dirty_generation,
            attempt.claim_token
        ],
    )?;
    Ok(changed != 0)
}

/// Count the failed attempt, schedule the row's retry at `next_attempt_at`
/// and release its claim, if `claim_token` still holds it at
/// `dirty_generation`. Returns `false` when the row is gone, another claim
/// took it, or it was marked dirty again since the claim.
pub fn back_off_failed_attempt(
    conn: &Connection,
    attempt: &FailedAttempt<'_>,
    next_attempt_at: i64,
) -> Result<bool> {
    let changed = conn.0.execute(
        sql!(
            "UPDATE dirty_facets
             SET attempts = attempts + 1,
                 last_error = ?3,
                 last_attempt_at = ?4,
                 next_attempt_at = ?5,
                 claim_token = '',
                 claimed_at = 0,
                 updated_at = ?4
             WHERE facet = ?1
               AND element = ?2
               AND dirty_generation = ?6
               AND claim_token = ?7"
        ),
        params![
            attempt.facet,
            attempt.element,
            attempt.error,
            attempt.failed_at,
            next_attempt_at,
            attempt.dirty_generation,
            attempt.claim_token
        ],
    )?;
    Ok(changed != 0)
}

/// Release `claim_token`'s claim on the element's row and make it due at
/// `now`, whatever its generation. Returns `false` when the row is gone or
/// the token no longer holds it.
pub fn release_claim_due_now(
    conn: &Connection,
    facet: &str,
    element: &str,
    claim_token: &str,
    now: i64,
) -> Result<bool> {
    let changed = conn.0.execute(
        sql!(
            "UPDATE dirty_facets
             SET claim_token = '',
                 claimed_at = 0,
                 next_attempt_at = ?4,
                 updated_at = ?4
             WHERE facet = ?1
               AND element = ?2
               AND claim_token = ?3"
        ),
        params![facet, element, claim_token, now],
    )?;
    Ok(changed != 0)
}

/// The dirty rows that `claim_token` claimed, ordered by facet and element.
pub fn claimed(conn: &Connection, claim_token: &str) -> Result<Vec<ClaimedDirtyFacetRow>> {
    let mut stmt = conn.0.prepare(sql!(
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
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Every dirty row, ordered by facet and element.
pub fn queue(conn: &Connection) -> Result<Vec<DirtyFacetQueueRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT facet, element, attempts, last_error, updated_at, last_attempt_at
         FROM dirty_facets
         WHERE dirty != 0
         ORDER BY facet ASC, element ASC"
    ))?;
    let rows = stmt.query_map([], queue_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// The facet's dirty rows, ordered by element.
pub fn facet_queue(conn: &Connection, facet: &str) -> Result<Vec<DirtyFacetQueueRow>> {
    let mut stmt = conn.0.prepare(sql!(
        "SELECT facet, element, attempts, last_error, updated_at, last_attempt_at
         FROM dirty_facets
         WHERE dirty != 0 AND facet = ?1
         ORDER BY facet ASC, element ASC"
    ))?;
    let rows = stmt.query_map(params![facet], queue_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn queue_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DirtyFacetQueueRow> {
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
    Ok(conn
        .0
        .query_row(
            sql!("SELECT attempts FROM dirty_facets WHERE facet = ?1 AND element = ?2"),
            params![facet, element],
            |row| row.get(0),
        )
        .optional()?)
}
