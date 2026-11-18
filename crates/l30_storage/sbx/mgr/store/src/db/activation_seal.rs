//! The node's persisted activation seal (`activation_seal`), which
//! sandbox-manager reads before it serves so a seal survives a restart.

use pplx_space_sql_locator::sql;
use pplx_space_sqlite::SqliteU64;
use rusqlite::params;

use crate::db::{Connection, OptionalExtension as _, Result};

/// The one `activation_seal` row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActivationSealRecord {
    /// The generation of the last seal or cancel; only moves forward.
    pub generation: u64,
    /// Sealed under `generation`. Once false, `generation` was cancelled,
    /// and no seal at or below it is taken again.
    pub sealed: bool,
}

/// The row, `None` on a node that was never sealed; a row that does not
/// decode fails the read.
pub(in crate::db) fn activation_seal(conn: &Connection) -> Result<Option<ActivationSealRecord>> {
    conn.query_row(
        sql!("SELECT generation, sealed FROM activation_seal WHERE singleton = 1"),
        [],
        |row| {
            Ok(ActivationSealRecord {
                generation: row.get::<_, SqliteU64>(0)?.0,
                sealed: row.get(1)?,
            })
        },
    )
    .optional()
}

/// Replace the row with `record`. A zero generation, or one above `i64::MAX`,
/// fails without writing.
pub fn write_activation_seal(conn: &Connection, record: ActivationSealRecord) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO activation_seal (singleton, generation, sealed)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET
                 generation = excluded.generation,
                 sealed = excluded.sealed"
        ),
        params![SqliteU64(record.generation), record.sealed],
    )?;
    Ok(())
}
