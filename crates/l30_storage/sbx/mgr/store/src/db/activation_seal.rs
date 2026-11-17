//! The node's persisted activation seal (`activation_seal`), which
//! sandbox-manager reads before it serves so a seal survives a restart.

use pplx_space_common_core::sql;
use pplx_space_sqlite::SqliteU64;

use crate::db::{Connection, OptionalExtension as _, Result};

/// The one `activation_seal` row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationSealRecord {
    /// The epoch of the last seal or cancel; only moves forward.
    pub epoch: u64,
    /// Sealed under `epoch`. Once false, `epoch` was cancelled, and no seal
    /// at or below it is taken again.
    pub sealed: bool,
}

/// The row, `None` on a node that was never sealed; a row that does not
/// decode fails the read.
pub(in crate::db) fn activation_seal(conn: &Connection) -> Result<Option<ActivationSealRecord>> {
    conn.query_row(
        sql!("SELECT epoch, sealed FROM activation_seal WHERE singleton = 1"),
        [],
        |row| {
            Ok(ActivationSealRecord {
                epoch: row.get::<_, SqliteU64>(0)?.0,
                sealed: row.get(1)?,
            })
        },
    )
    .optional()
}

/// Replace the row with `record`. A zero epoch, or one above `i64::MAX`,
/// fails without writing. Only tests write the row until the node drain that
/// seals lands.
#[cfg(any(test, feature = "test-support"))]
pub fn write_activation_seal(conn: &Connection, record: ActivationSealRecord) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO activation_seal (singleton, epoch, sealed)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET
                 epoch = excluded.epoch,
                 sealed = excluded.sealed"
        ),
        rusqlite::params![SqliteU64(record.epoch), record.sealed],
    )?;
    Ok(())
}
