//! Bounded identity discovery for operator selection, not mutation authority.

use super::inventory_read::{InventoryReadError, check_row};
use crate::db::Connection;
use pplx_space_common_core::inventory::{InventoryBudget, MAX_ENCODED_BYTES};
use pplx_space_common_core::types::{SandboxId, SandboxOwner, SandboxRuntimeLabel};
use rusqlite::types::Type;

use super::CleanupTarget;

pub const BATCH_SIZE: usize = 128;

#[derive(Debug, Default)]
pub struct CleanupTargetScan {
    pub next: Option<i64>,
    pub through: Option<i64>,
    pub complete: bool,
}

pub struct CleanupTargetObservation {
    pub raw_id: String,
    pub metadata: Option<CleanupTarget>,
}

impl CleanupTargetScan {
    pub fn complete(&self) -> bool {
        self.complete
    }

    pub fn next_batch(
        &mut self,
        conn: &Connection,
    ) -> Result<Vec<CleanupTargetObservation>, InventoryReadError> {
        if self.complete {
            return Ok(Vec::new());
        }
        if self.next.is_none() {
            self.through =
                conn.query_row("SELECT MAX(rowid) FROM sandboxes", [], |row| row.get(0))?;
            self.next = Some(i64::MIN);
        }
        let Some(through) = self.through else {
            self.complete = true;
            return Ok(Vec::new());
        };
        let mut statement = conn.prepare(
            "SELECT rowid, id, uuid8, template, user FROM sandboxes
             WHERE rowid >= ?1 AND rowid <= ?2 ORDER BY rowid LIMIT 128",
        )?;
        let mut rows = statement.query([self.next.unwrap_or(i64::MIN), through])?;
        let mut batch = Vec::new();
        let mut budget = InventoryBudget::new(BATCH_SIZE, MAX_ENCODED_BYTES);
        while let Some(row) = rows.next()? {
            check_row(row, &mut budget)?;
            let rowid: i64 = row.get(0)?;
            let raw_id: String = row.get(1)?;
            let metadata = (|| {
                Ok(CleanupTarget {
                    uuid8: SandboxRuntimeLabel::parse(row.get::<_, String>(2)?).map_err(
                        |error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                Type::Text,
                                Box::new(error),
                            )
                        },
                    )?,
                    template: row.get(3)?,
                    user: SandboxOwner::parse(row.get::<_, String>(4)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(error))
                    })?,
                })
            })();
            let metadata = match metadata {
                Ok(metadata)
                    if raw_id
                        .parse::<SandboxId>()
                        .is_ok_and(|id| id.to_string() == raw_id) =>
                {
                    Some(metadata)
                }
                Ok(_) => None,
                Err(error) if super::is_row_decode_error(&error) => None,
                Err(error) => return Err(error.into()),
            };
            batch.push(CleanupTargetObservation { raw_id, metadata });
            if rowid == through {
                self.complete = true;
            } else {
                self.next = Some(rowid + 1);
            }
        }
        if batch.len() < BATCH_SIZE {
            self.complete = true;
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod tests;
