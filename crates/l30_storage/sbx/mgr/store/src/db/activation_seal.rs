//! The node's persisted activation seal (`activation_seal`), which
//! sandbox-manager reads before it serves so a seal survives a restart.

use pplx_space_common_core::sql;
use pplx_space_sqlite::SqliteU64;
use rusqlite::params;

use crate::db::{Connection, OptionalExtension as _, Result};

/// The one `activation_seal` row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationSealRecord {
    /// The epoch the node is sealed under; `None` once it is unsealed.
    pub sealed_epoch: Option<u64>,
    /// Wall-clock Unix milliseconds at which the current seal, or the last
    /// one once unsealed, began.
    pub sealed_since_ms: u64,
}

/// The row, `None` on a node that was never sealed; a row that does not
/// decode fails the read.
pub(in crate::db) fn activation_seal(conn: &Connection) -> Result<Option<ActivationSealRecord>> {
    conn.query_row(
        sql!("SELECT sealed_epoch, sealed_since_ms FROM activation_seal WHERE singleton = 1"),
        [],
        |row| {
            Ok(ActivationSealRecord {
                sealed_epoch: row.get::<_, Option<SqliteU64>>(0)?.map(|epoch| epoch.0),
                sealed_since_ms: row.get::<_, SqliteU64>(1)?.0,
            })
        },
    )
    .optional()
}

/// Replace the row with `record`. A zero epoch, or an epoch or time above
/// `i64::MAX`, fails without writing.
pub fn write_activation_seal(conn: &Connection, record: ActivationSealRecord) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO activation_seal (singleton, sealed_epoch, sealed_since_ms)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET
                 sealed_epoch = excluded.sealed_epoch,
                 sealed_since_ms = excluded.sealed_since_ms"
        ),
        params![
            record.sealed_epoch.map(SqliteU64),
            SqliteU64(record.sealed_since_ms)
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
