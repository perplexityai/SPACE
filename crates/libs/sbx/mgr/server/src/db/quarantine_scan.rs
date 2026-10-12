//! Finite quarantine discovery; page counts are not node-wide backlog samples.

use std::time::{Duration, Instant};

use rusqlite::{Connection, Result, params};

use super::{
    QuarantineRecoveryCandidate, QuarantineRecoveryOwner, QuarantineRecoveryRow,
    QuarantineRecoveryScan, SandboxQuarantineSource, decode_quarantine_recovery_row, now_epoch_i64,
};

const RAW_ROWS_PER_BATCH: usize = 128;
const CANDIDATES_PER_BATCH: usize = 8;

#[derive(Debug)]
pub(crate) struct QuarantineRecoveryBatch {
    pub candidates: Vec<QuarantineRecoveryCandidate>,
    /// A completed sweep's observations, not a transactionally consistent snapshot.
    /// Absent on partial batches; consumers must not publish page counts as totals.
    pub completed_sweep: Option<QuarantineRecoveryScan>,
}

/// New rows wait for a later sweep. Busy ownership, slow I/O and skipped ticks
/// can postpone discovery, so this cursor does not promise a recovery deadline.
#[derive(Debug)]
pub(crate) struct PeriodicQuarantineRecoveryScan {
    next: i64,
    through: Option<i64>,
    summary: QuarantineRecoveryScan,
    oldest_entered: Option<u64>,
    last_completed: Option<Instant>,
}

impl Default for PeriodicQuarantineRecoveryScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
            summary: QuarantineRecoveryScan::default(),
            oldest_entered: None,
            last_completed: None,
        }
    }
}

impl PeriodicQuarantineRecoveryScan {
    /// Validity is refreshed by successful pages, but only while a complete
    /// census is recent. A stalled/oversized sweep must not look healthy forever.
    /// Every completion renews freshness, even after a slow sweep. Discovery
    /// stops after either 128 raw rows or eight due candidates per tick; tick
    /// work and admission delays prevent a fixed population-to-latency bound.
    pub(crate) fn has_recent_completed_sweep(&self) -> bool {
        self.has_recent_completed_sweep_at(Instant::now())
    }

    fn has_recent_completed_sweep_at(&self, now: Instant) -> bool {
        self.last_completed.is_some_and(|completed| {
            now.saturating_duration_since(completed) < Duration::from_secs(15 * 60)
        })
    }

    pub(crate) fn next_batch(&mut self, conn: &Connection) -> Result<QuarantineRecoveryBatch> {
        let batch = self.read_batch(conn);
        if batch.is_err() {
            // Never complete a backlog sample after an unread section of a sweep.
            *self = Self::default();
        }
        batch
    }

    fn read_batch(&mut self, conn: &Connection) -> Result<QuarantineRecoveryBatch> {
        let current_epoch = now_epoch_i64()?;
        if self.through.is_none() {
            self.through = conn.query_row(
                "SELECT MAX(rowid) FROM sandbox_quarantine_state",
                [],
                |row| row.get(0),
            )?;
        }
        let mut batch = QuarantineRecoveryBatch {
            candidates: Vec::new(),
            completed_sweep: None,
        };
        let Some(through) = self.through else {
            batch.completed_sweep = Some(self.complete(current_epoch)?);
            return Ok(batch);
        };
        let mut statement = conn.prepare_cached(
            "SELECT s.id, qs.quarantine_id, qs.entered_at, s.state, qs.reason_code,
                    s.current_local_artifact_id, s.current_local_artifact_kind,
                    s.current_local_artifact_origin, qs.attempt_count,
                    qs.next_action_after, qs.claim_token, qs.claim_expires_at,
                    qs.rowid,
                    (s.state IN ('quarantined', 'restoring')
                     AND qs.source = ?3 AND cleanup.sandbox_id IS NULL
                     AND (qs.next_action_owner IS NULL OR qs.next_action_owner = ?4)) IS TRUE,
                    qs.last_error
               FROM sandbox_quarantine_state qs
               LEFT JOIN sandboxes s ON s.id = qs.sandbox_id
               LEFT JOIN sandbox_cleanup_episodes cleanup ON cleanup.sandbox_id = s.id
              WHERE qs.rowid >= ?1 AND qs.rowid <= ?2
              ORDER BY qs.rowid LIMIT 128",
        )?;
        let mut rows = statement.query(params![
            self.next,
            through,
            SandboxQuarantineSource::SbxManager.as_str(),
            QuarantineRecoveryOwner::LegacyRecovery.as_str(),
        ])?;
        for _ in 0..RAW_ROWS_PER_BATCH {
            let Some(row) = rows.next()? else {
                batch.completed_sweep = Some(self.complete(current_epoch)?);
                break;
            };
            let rowid: i64 = row.get(12)?;
            if row.get::<_, bool>(13)? {
                match decode_quarantine_recovery_row(row, current_epoch)? {
                    QuarantineRecoveryRow::Malformed => {
                        self.summary.malformed_count =
                            self.summary.malformed_count.saturating_add(1);
                    }
                    QuarantineRecoveryRow::Ineligible => {}
                    QuarantineRecoveryRow::RecoveryStopped { entered_at } => {
                        self.count_pending(entered_at);
                    }
                    QuarantineRecoveryRow::Eligible { candidate, due } => {
                        self.count_pending(candidate.entered_at);
                        if due {
                            batch.candidates.push(candidate);
                        }
                    }
                }
            }
            if rowid == through {
                batch.completed_sweep = Some(self.complete(current_epoch)?);
                break;
            }
            self.next = rowid + 1;
            // Stop at the last candidate returned, not at the end of its SQL page:
            // permanently deferred candidates must not hide siblings behind them.
            if batch.candidates.len() == CANDIDATES_PER_BATCH {
                break;
            }
        }
        Ok(batch)
    }

    fn count_pending(&mut self, entered_at: u64) {
        self.summary.pending_count = self.summary.pending_count.saturating_add(1);
        self.oldest_entered = Some(
            self.oldest_entered
                .map_or(entered_at, |oldest| oldest.min(entered_at)),
        );
    }

    fn complete(&mut self, current_epoch: i64) -> Result<QuarantineRecoveryScan> {
        let current_epoch = u64::try_from(current_epoch)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let mut completed = std::mem::take(self);
        self.last_completed = Some(Instant::now());
        completed.summary.pending_oldest_age = Duration::from_secs(
            completed
                .oldest_entered
                .map_or(0, |entered| current_epoch.saturating_sub(entered)),
        );
        Ok(completed.summary)
    }
}

#[cfg(test)]
mod tests;
