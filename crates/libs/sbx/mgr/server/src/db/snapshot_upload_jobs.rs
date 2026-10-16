//! `jobs` rows of the snapshot-upload pipeline, and the backup markers its
//! completed jobs prove.

use pplx_space_common_core::types::JobId;
use rusqlite::{Connection, Result, params};

/// Give back the attempt a restart interrupted: every running job of `kind`
/// with attempts left loses one; the number refunded.
pub(crate) fn refund_interrupted_running_attempts(conn: &Connection, kind: &str) -> Result<usize> {
    conn.execute(
        "UPDATE jobs
         SET attempts = attempts - 1
         WHERE kind = ?1 AND status = 'running' AND attempts > 0",
        params![kind],
    )
}

/// Replace a pending or running job's payload; the number of rows changed.
pub(crate) fn replace_active_job_payload(
    conn: &Connection,
    payload: &str,
    job_id: &JobId,
    kind: &str,
) -> Result<usize> {
    conn.execute(
        "UPDATE jobs SET payload = ?1
         WHERE job_id = ?2 AND kind = ?3 AND status IN ('pending', 'running')",
        params![payload, job_id, kind],
    )
}

/// The highest job rowid, `None` for an empty table.
pub(crate) fn max_job_rowid(conn: &Connection) -> Result<Option<i64>> {
    conn.query_row("SELECT MAX(rowid) FROM jobs", [], |r| r.get(0))
}

/// Visit up to `limit` completed snapshot-upload jobs with rowids in
/// `(after, through]`, in rowid order, with the scan's timestamp, each rowid
/// and its payload or the error reading it. The timestamp is taken once the
/// statement is bound, before the first row is read. The statement stays open
/// while `visit` runs, so `visit` may write through the same connection; its
/// error stops the scan.
pub(crate) fn visit_completed_upload_payloads(
    conn: &Connection,
    after: i64,
    through: i64,
    limit: i64,
    mut visit: impl FnMut(&str, i64, Result<String>) -> Result<()>,
) -> Result<()> {
    let mut statement = conn.prepare(
        "SELECT rowid, payload FROM jobs
         WHERE kind = 'snapshot_upload' AND status = 'completed'
           AND rowid > ?1 AND rowid <= ?2 ORDER BY rowid LIMIT ?3",
    )?;
    let mut rows = statement.query(params![after, through, limit])?;
    let now = crate::db::now()?;
    while let Some(row) = rows.next()? {
        visit(&now, row.get(0)?, row.get::<_, String>(1))?;
    }
    Ok(())
}

/// Stamp `backup_completed_at` on an artifact that has none; the number of
/// rows changed.
pub(crate) fn stamp_backup_completed_if_unset(
    conn: &Connection,
    now: &str,
    local_artifact_id: &str,
) -> Result<usize> {
    conn.execute(
        "UPDATE local_artifacts SET backup_completed_at = ?1
         WHERE local_artifact_id = ?2 AND backup_completed_at IS NULL",
        params![now, local_artifact_id],
    )
}

/// Whether an artifact row exists.
pub(crate) fn local_artifact_exists(conn: &Connection, local_artifact_id: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM local_artifacts WHERE local_artifact_id = ?1)",
        params![local_artifact_id],
        |r| r.get(0),
    )
}
