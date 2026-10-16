//! Bounded local discovery. A batch is never a complete external inventory.

use crate::db::{Connection, Result};

use super::{SandboxRow, describe_error, describe_raw_error, get_sandbox, is_row_decode_error};

pub(crate) const INVENTORY_BATCH_SIZE: usize = 128;

/// Pins the sweep's upper rowid so new insertions cannot postpone older work.
/// Rowids are positions, not identities or mutation authority. Consumers must
/// recheck each observed row under its operation-specific ownership fence.
#[derive(Debug)]
pub(crate) struct SandboxInventoryScan {
    next: i64,
    through: Option<i64>,
}

impl Default for SandboxInventoryScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

impl SandboxInventoryScan {
    pub(crate) fn sweep_complete(&self) -> bool {
        self.through.is_none()
    }

    pub(crate) fn next_batch(&mut self, conn: &Connection) -> Result<Vec<SandboxRow>> {
        if self.through.is_none() {
            self.through =
                conn.query_row("SELECT MAX(rowid) FROM sandboxes", [], |row| row.get(0))?;
        }
        let Some(through) = self.through else {
            return Ok(Vec::new());
        };
        let mut statement = conn.prepare(
            "SELECT rowid, id FROM sandboxes
             WHERE rowid >= ?1 AND rowid <= ?2 ORDER BY rowid LIMIT 128",
        )?;
        let mut rows = statement.query([self.next, through])?;
        let mut batch = Vec::new();
        for _ in 0..INVENTORY_BATCH_SIZE {
            let Some(row) = rows.next()? else {
                *self = Self::default();
                break;
            };
            let rowid: i64 = row.get(0)?;
            if rowid == through {
                *self = Self::default();
            } else {
                self.next = rowid + 1;
            }
            let id = match row.get::<_, String>(1) {
                Ok(id) => id,
                Err(error) if is_row_decode_error(&error) => {
                    tracing::warn!(rowid, error = %describe_raw_error(&error), "preserving undecodable inventory identity");
                    continue;
                }
                Err(error) => return Err(crate::db::Error::from(error)),
            };
            match get_sandbox(conn, &id) {
                Ok(Some(row)) => batch.push(row),
                Ok(None) => {}
                Err(error) if is_row_decode_error(error.raw()) => {
                    tracing::warn!(sandbox_id = %id, error = %describe_error(&error), "preserving undecodable inventory row");
                }
                Err(error) => return Err(error),
            }
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod tests;

/// Periodic orphan-scope discovery, including live and malformed rows in its
/// scan budget so retained evidence cannot permanently occupy the first page.
#[derive(Debug)]
pub(crate) struct RestoreTransferScopeScan {
    next: i64,
    through: Option<i64>,
}

impl Default for RestoreTransferScopeScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

impl RestoreTransferScopeScan {
    pub(crate) fn next_batch(
        &mut self,
        conn: &Connection,
    ) -> Result<Vec<(String, super::RestoreTransferScopeRow)>> {
        if self.through.is_none() {
            self.through = conn.query_row(
                "SELECT MAX(rowid) FROM sandbox_restore_transfer_scopes",
                [],
                |row| row.get(0),
            )?;
        }
        let Some(through) = self.through else {
            return Ok(Vec::new());
        };
        let mut statement = conn.prepare(
            "SELECT scopes.rowid, scopes.sandbox_id, scopes.transfer_scope_id,
                    scopes.execution_generation, scopes.activation_attempt_id, scopes.staging_dir,
                    EXISTS (SELECT 1 FROM sandboxes WHERE id = scopes.sandbox_id)
             FROM sandbox_restore_transfer_scopes AS scopes
             WHERE scopes.rowid >= ?1 AND scopes.rowid <= ?2
             ORDER BY scopes.rowid LIMIT 128",
        )?;
        let mut rows = statement.query([self.next, through])?;
        let mut batch = Vec::new();
        let mut examined = 0;
        while let Some(row) = rows.next()? {
            examined += 1;
            let rowid: i64 = row.get(0)?;
            if rowid == through {
                *self = Self::default();
            } else {
                self.next = rowid + 1;
            }
            if row.get::<_, bool>(6)? {
                continue;
            }
            let decoded = (|| Ok((row.get(1)?, super::restore_transfer_scope_from_row(row, 2)?)))();
            match decoded {
                Ok(scope) => batch.push(scope),
                Err(error) if is_row_decode_error(&error) => {
                    tracing::warn!(rowid, error = %describe_raw_error(&error), "preserving malformed orphan transfer scope");
                }
                Err(error) => return Err(error.into()),
            }
            if batch.len() == 8 {
                return Ok(batch);
            }
        }
        // A short page means exhaustion; a full page may have more successors.
        if examined < INVENTORY_BATCH_SIZE {
            *self = Self::default();
        }
        Ok(batch)
    }
}
