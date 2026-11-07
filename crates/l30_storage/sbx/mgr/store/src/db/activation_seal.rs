//! The node's persisted activation seal (`activation_seal`), which
//! sandbox-manager reads before it serves so a seal survives a restart, and
//! the lifecycle each resident a drain suspended had (`drain_evacuations`).

use pplx_space_common_core::sql;
use pplx_space_sqlite::SqliteU64;
use rusqlite::params;

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
/// fails without writing.
pub fn write_activation_seal(conn: &Connection, record: ActivationSealRecord) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO activation_seal (singleton, epoch, sealed)
             VALUES (1, ?1, ?2)
             ON CONFLICT(singleton) DO UPDATE SET
                 epoch = excluded.epoch,
                 sealed = excluded.sealed"
        ),
        params![SqliteU64(record.epoch), record.sealed],
    )?;
    Ok(())
}

/// The lifecycle a drained resident had, which the control plane restores
/// it to. A record holds a started or paused one; a stopped resident needs
/// none, since its row stays stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainPrior {
    Started,
    Paused,
    Stopped,
}

impl DrainPrior {
    pub const ALL: [Self; 3] = [Self::Started, Self::Paused, Self::Stopped];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|prior| prior.as_str() == value)
    }
}

/// One `drain_evacuations` row: a resident the drain suspended, and the
/// lifecycle it had.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainEvacuation {
    pub sandbox_id: String,
    pub prior: DrainPrior,
}

/// Record that a drain suspend of `sandbox_id` begins from `prior`, unless
/// an earlier attempt's record stands: a suspend that failed after its
/// pause leaves the row `paused`, and the lifecycle to restore is still the
/// one first recorded. Returns the prior the record holds.
pub fn record_drain_suspend(
    conn: &Connection,
    sandbox_id: &str,
    prior: DrainPrior,
) -> Result<DrainPrior> {
    let recorded: String = conn.query_row(
        sql!(
            "INSERT INTO drain_evacuations (sandbox_id, prior) VALUES (?1, ?2)
             ON CONFLICT(sandbox_id) DO UPDATE SET prior = drain_evacuations.prior
             RETURNING prior"
        ),
        params![sandbox_id, prior.as_str()],
        |row| row.get(0),
    )?;
    DrainPrior::parse(&recorded).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            format!("unknown drain prior {recorded:?}").into(),
        )
        .into()
    })
}

/// Forget `sandbox_id`'s record: its drain suspend did not happen.
pub fn forget_drain_suspend(conn: &Connection, sandbox_id: &str) -> Result<()> {
    conn.execute(
        sql!("DELETE FROM drain_evacuations WHERE sandbox_id = ?1"),
        params![sandbox_id],
    )?;
    Ok(())
}

/// Every evacuation, by sandbox id. A row that does not decode fails the
/// read.
pub(in crate::db) fn drain_evacuations(conn: &Connection) -> Result<Vec<DrainEvacuation>> {
    let mut statement = conn.prepare(sql!(
        "SELECT sandbox_id, prior FROM drain_evacuations ORDER BY sandbox_id"
    ))?;
    let rows = statement.query_map([], |row| {
        let prior: String = row.get(1)?;
        Ok(DrainEvacuation {
            sandbox_id: row.get(0)?,
            prior: DrainPrior::parse(&prior).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    format!("unknown drain prior {prior:?}").into(),
                )
            })?,
        })
    })?;
    rows.collect::<std::result::Result<_, _>>()
        .map_err(Into::into)
}

/// Write `record`, the seal row after a cancel, and forget every
/// evacuation, in one transaction: a node runs one drain at a time, and the
/// cancel ends it.
pub fn cancel_drain(conn: &Connection, record: ActivationSealRecord) -> Result<()> {
    let tx = crate::db::immediate_transaction(conn)?;
    write_activation_seal(&tx, record)?;
    tx.execute(sql!("DELETE FROM drain_evacuations"), [])?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests;
