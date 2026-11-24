//! `reconciliation_status`: one row per node-ctl reconciler, with its
//! schedule and the outcome of its last run.

use rusqlite::{Connection, Result, params};

use pplx_space_sql_locator::sql;

/// A reconciler's row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationStatusRow {
    pub name: String,
    pub poll_interval_secs: i64,
    pub last_started_at: i64,
    pub last_completed_at: i64,
    pub last_success_at: i64,
    pub next_due_at: i64,
    pub running: bool,
    pub templates_seen: i64,
    pub sandboxes_seen: i64,
    pub templates_applied: i64,
    pub sandboxes_applied: i64,
    pub last_error: String,
}

/// The outcome of a finished run, recorded by [`mark_finished`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinishedRun<'a> {
    pub completed_at: i64,
    pub success: bool,
    pub templates_seen: i64,
    pub sandboxes_seen: i64,
    pub templates_applied: i64,
    pub sandboxes_applied: i64,
    /// Empty on success.
    pub last_error: &'a str,
}

/// Register the reconciler at startup: insert its row due now, or set its
/// poll interval, clear its running flag and, when a run was left running,
/// make it due now.
pub fn register(conn: &Connection, name: &str, poll_interval_secs: i64, now: i64) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO reconciliation_status
                 (name, poll_interval_secs, next_due_at, running, updated_at)
             VALUES (?1, ?2, ?3, 0, ?3)
             ON CONFLICT(name) DO UPDATE
             SET poll_interval_secs = excluded.poll_interval_secs,
                 next_due_at = CASE
                     WHEN reconciliation_status.running != 0
                     THEN ?3
                     ELSE reconciliation_status.next_due_at
                 END,
                 running = 0,
                 updated_at = ?3"
        ),
        params![name, poll_interval_secs, now],
    )?;
    Ok(())
}

/// Set the reconciler's next due time.
pub fn set_next_due(conn: &Connection, name: &str, next_due_at: i64, now: i64) -> Result<()> {
    conn.execute(
        sql!(
            "UPDATE reconciliation_status
             SET next_due_at = ?1,
                 updated_at = ?2
             WHERE name = ?3"
        ),
        params![next_due_at, now, name],
    )?;
    Ok(())
}

/// Record that the reconciler started a run at `now`.
pub fn mark_started(conn: &Connection, name: &str, now: i64) -> Result<()> {
    conn.execute(
        sql!(
            "UPDATE reconciliation_status
             SET last_started_at = ?1,
                 running = 1,
                 updated_at = ?1
             WHERE name = ?2"
        ),
        params![now, name],
    )?;
    Ok(())
}

/// Record the outcome of the reconciler's run and clear its running flag.
pub fn mark_finished(conn: &Connection, name: &str, run: &FinishedRun<'_>) -> Result<()> {
    conn.execute(
        sql!(
            "UPDATE reconciliation_status
             SET last_completed_at = ?1,
                 last_success_at = CASE WHEN ?2 != 0 THEN ?1 ELSE last_success_at END,
                 running = 0,
                 templates_seen = ?3,
                 sandboxes_seen = ?4,
                 templates_applied = ?5,
                 sandboxes_applied = ?6,
                 last_error = ?7,
                 updated_at = ?1
             WHERE name = ?8"
        ),
        params![
            run.completed_at,
            i64::from(run.success),
            run.templates_seen,
            run.sandboxes_seen,
            run.templates_applied,
            run.sandboxes_applied,
            run.last_error,
            name,
        ],
    )?;
    Ok(())
}

/// Every reconciler's row, ordered by name.
pub fn list(conn: &Connection) -> Result<Vec<ReconciliationStatusRow>> {
    let mut stmt = conn.prepare(sql!(
        "SELECT name,
                poll_interval_secs,
                last_started_at,
                last_completed_at,
                last_success_at,
                next_due_at,
                running,
                templates_seen,
                sandboxes_seen,
                templates_applied,
                sandboxes_applied,
                last_error
         FROM reconciliation_status
         ORDER BY name ASC"
    ))?;
    let rows = stmt.query_map([], |row| {
        Ok(ReconciliationStatusRow {
            name: row.get(0)?,
            poll_interval_secs: row.get(1)?,
            last_started_at: row.get(2)?,
            last_completed_at: row.get(3)?,
            last_success_at: row.get(4)?,
            next_due_at: row.get(5)?,
            running: row.get::<_, i64>(6)? != 0,
            templates_seen: row.get(7)?,
            sandboxes_seen: row.get(8)?,
            templates_applied: row.get(9)?,
            sandboxes_applied: row.get(10)?,
            last_error: row.get(11)?,
        })
    })?;
    rows.collect()
}
