//! Bounded, progressing artifact discovery; deletion authority stays with its lease.

use crate::db::{Connection, Result};
use pplx_space_common_core::inventory::{InventoryBudget, MAX_RAW_BYTES};
use pplx_space_common_core::sql;
use rusqlite::params;

use super::inventory_read::{InventoryReadError, check_row};
use super::timestamps::EpochSeconds;

pub(crate) const PAGE_SIZE: usize = 128;

#[derive(Debug)]
pub(crate) struct ArtifactObservation {
    pub(crate) local_artifact_id: String,
    pub(crate) sandbox_id: String,
    pub(crate) artifact_origin: String,
    pub(crate) artifact_kind: String,
    pub(crate) local_path: String,
    pub(crate) created_at_secs: u64,
    pub(crate) last_touched_at_secs: u64,
    pub(crate) backup_completed: bool,
    pub(crate) is_ready_state: bool,
    pub(crate) pinned_current: bool,
}

#[derive(Debug, Default)]
pub(crate) struct ArtifactPage {
    pub(crate) artifacts: Vec<ArtifactObservation>,
    pub(crate) examined: usize,
}

/// Fixed-ceiling sweeps visit malformed and protected rows without allowing
/// either those rows or concurrent insertion to starve later candidates.
/// This is page-local LRU discovery, not a globally ordered inventory.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ArtifactScan {
    next: i64,
    through: Option<i64>,
}

impl Default for ArtifactScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

const PAGE_SQL: &str = sql!(
    "SELECT a.rowid, a.local_artifact_id, a.sandbox_id,
            a.artifact_origin, a.artifact_kind, a.local_path,
            a.created_at, COALESCE(a.last_touched_at, a.created_at),
            a.backup_completed_at IS NOT NULL, a.state = 'ready',
            (EXISTS(SELECT 1 FROM sandboxes s WHERE s.current_local_artifact_id = a.local_artifact_id)
             OR EXISTS(SELECT 1 FROM sandboxes s WHERE s.suspend_predecessor_artifact_id = a.local_artifact_id))
       FROM local_artifacts a
      WHERE a.rowid >= ?1 AND a.rowid <= ?2
      ORDER BY a.rowid LIMIT ?3"
);

impl ArtifactScan {
    pub(crate) fn next_page(
        &mut self,
        conn: &Connection,
    ) -> std::result::Result<ArtifactPage, InventoryReadError> {
        let through = match self.through {
            Some(through) => through,
            None => {
                let maximum: Option<i64> =
                    conn.query_row(sql!("SELECT MAX(rowid) FROM local_artifacts"), [], |row| {
                        row.get(0)
                    })?;
                let Some(maximum) = maximum else {
                    return Ok(ArtifactPage::default());
                };
                maximum
            }
        };
        let mut statement = conn.prepare_cached(PAGE_SQL)?;
        let mut rows = statement.query(params![self.next, through, PAGE_SIZE as i64])?;
        let mut page = ArtifactPage::default();
        let mut last = None;
        let mut budget = InventoryBudget::new(PAGE_SIZE, MAX_RAW_BYTES);
        let mut page_full = false;
        while let Some(row) = rows.next()? {
            let rowid: i64 = row.get(0)?;
            // Oversize evidence is retained without allocation and does not
            // prevent later rows progressing. A row that fits alone but not
            // this page is retried with a fresh budget on the next tick.
            match check_row(row, &mut InventoryBudget::new(1, MAX_RAW_BYTES)) {
                Ok(()) => {}
                Err(InventoryReadError::Limit(_)) => {
                    page.examined += 1;
                    last = Some(rowid);
                    tracing::warn!(
                        rowid,
                        "snapshot reaper: retaining oversized artifact observation"
                    );
                    continue;
                }
                Err(error) => return Err(error),
            }
            match check_row(row, &mut budget) {
                Ok(()) => {}
                Err(InventoryReadError::Limit(_)) => {
                    page_full = true;
                    break;
                }
                Err(error) => return Err(error),
            }
            page.examined += 1;
            last = Some(rowid);
            let observation = (|| -> Result<ArtifactObservation> {
                Ok(ArtifactObservation {
                    local_artifact_id: row.get(1)?,
                    sandbox_id: row.get(2)?,
                    artifact_origin: row.get(3)?,
                    artifact_kind: row.get(4)?,
                    local_path: row.get(5)?,
                    created_at_secs: row.get::<_, EpochSeconds>(6)?.unsigned(),
                    last_touched_at_secs: row.get::<_, EpochSeconds>(7)?.unsigned(),
                    backup_completed: row.get(8)?,
                    is_ready_state: row.get(9)?,
                    pinned_current: row.get(10)?,
                })
            })();
            match observation {
                Ok(observation) => page.artifacts.push(observation),
                Err(error) => {
                    tracing::warn!(rowid, %error, "snapshot reaper: retaining malformed artifact observation")
                }
            }
        }
        *self = match last {
            Some(last) if (page.examined == PAGE_SIZE || page_full) && last < through => Self {
                next: last + 1,
                through: Some(through),
            },
            _ => Self::default(),
        };
        Ok(page)
    }
}

#[cfg(test)]
mod tests;
