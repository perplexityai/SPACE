//! Bounded retention of diagnostic history, never active recovery evidence.

use pplx_space_common_core::types::SandboxId;

use crate::db::{Connection, Result};
use rusqlite::{params, types::Value};

const ROWS_PER_BATCH: usize = 128;
const RETENTION_SECS: u64 = 30 * 24 * 60 * 60;

/// New history waits for the next finite sweep; retained rows cannot starve it.
#[derive(Debug)]
pub struct QuarantineHistoryPruner {
    next: i64,
    through: Option<i64>,
}

impl Default for QuarantineHistoryPruner {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct HistoryPruneBatch {
    pub examined: usize,
    pub deleted: usize,
}

impl QuarantineHistoryPruner {
    pub fn next_batch(&mut self, conn: &Connection, now: u64) -> Result<HistoryPruneBatch> {
        let result = self.prune_batch(conn, now);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }

    fn prune_batch(&mut self, conn: &Connection, now: u64) -> Result<HistoryPruneBatch> {
        let tx = super::immediate_transaction(conn)?;
        if self.through.is_none() {
            self.through = tx.query_row(
                "SELECT MAX(rowid) FROM sandbox_quarantine_history",
                [],
                |row| row.get(0),
            )?;
        }
        let Some(through) = self.through else {
            return Ok(HistoryPruneBatch::default());
        };
        let rows = {
            let mut statement = tx.prepare_cached(
                "SELECT rowid, occurred_at, sandbox_id FROM sandbox_quarantine_history
                 WHERE rowid >= ?1 AND rowid <= ?2 ORDER BY rowid LIMIT 128",
            )?;
            statement
                .query_map(params![self.next, through], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Value>(1)?,
                        row.get::<_, Value>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut report = HistoryPruneBatch {
            examined: rows.len(),
            deleted: 0,
        };
        for (rowid, timestamp, sandbox_id) in &rows {
            let Value::Text(sandbox_id) = sandbox_id else {
                continue;
            };
            let Ok(identity) = sandbox_id.parse::<SandboxId>() else {
                continue;
            };
            // Protection rows use canonical keys. A parseable alias must not
            // turn a case-sensitive lookup miss into proof of absent ownership.
            if identity.to_string() != *sandbox_id {
                continue;
            }
            // Never reinterpret malformed, negative, fractional or overflowing
            // timestamps as ancient history. Those rows require operator repair.
            let occurred_at = match timestamp {
                Value::Integer(value) => u64::try_from(*value).ok(),
                Value::Text(value)
                    if !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()) =>
                {
                    value.parse::<u64>().ok()
                }
                _ => None,
            };
            if occurred_at.is_some_and(|value| value < now.saturating_sub(RETENTION_SECS)) {
                report.deleted += tx.execute(
                    "DELETE FROM sandbox_quarantine_history
                     WHERE rowid = ?1
                       AND typeof(sandbox_id) = 'text' AND length(sandbox_id) > 0
                       AND NOT EXISTS (SELECT 1 FROM sandbox_quarantine_state q
                           WHERE q.sandbox_id = sandbox_quarantine_history.sandbox_id)
                       AND NOT EXISTS (SELECT 1 FROM sandbox_cleanup_episodes c
                           WHERE c.sandbox_id = sandbox_quarantine_history.sandbox_id)
                       AND NOT EXISTS (SELECT 1 FROM sandbox_restore_transfer_scopes t
                           WHERE t.sandbox_id = sandbox_quarantine_history.sandbox_id)
                       AND NOT EXISTS (SELECT 1 FROM auto_lifecycle_delete_outbox o
                           WHERE o.sandbox_id = sandbox_quarantine_history.sandbox_id)
                       AND NOT EXISTS (SELECT 1 FROM sandboxes s
                           WHERE s.id = sandbox_quarantine_history.sandbox_id
                             AND (typeof(s.state) != 'text' OR s.state NOT IN
                                 ('created', 'started', 'paused', 'stopped', 'suspended', 'failed')))",
                    [rowid],
                )?;
            }
        }
        tx.commit()?;
        if rows.len() < ROWS_PER_BATCH || rows.last().is_some_and(|(rowid, _, _)| *rowid == through)
        {
            *self = Self::default();
        } else if let Some((rowid, _, _)) = rows.last() {
            // The fixed upper bound proves this increment cannot overflow.
            self.next = rowid + 1;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests;
