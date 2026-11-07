//! The node's persisted activation seal (`activation_seal`), which
//! sandbox-manager reads before it serves so a seal survives a restart, and
//! the drain's evacuation record (`drain_evacuations`).

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
/// it to.
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

/// One `drain_evacuations` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainEvacuation {
    pub sandbox_id: String,
    /// The drain epoch the evacuation began under.
    pub epoch: u64,
    pub prior: DrainPrior,
    /// The durable artifact that holds the sandbox; `None` while the
    /// evacuation is still making it.
    pub artifact_id: Option<String>,
}

/// Record that the drain under `epoch` began evacuating `sandbox_id` from
/// `prior`. A row the sandbox already has keeps its prior and artifact; only
/// its epoch moves forward, so a later epoch reports it too.
pub fn begin_drain_evacuation(
    conn: &Connection,
    sandbox_id: &str,
    epoch: u64,
    prior: DrainPrior,
    now_ms: u64,
) -> Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO drain_evacuations (sandbox_id, epoch, prior, artifact_id, recorded_at_ms)
             VALUES (?1, ?2, ?3, NULL, ?4)
             ON CONFLICT(sandbox_id) DO UPDATE SET
                 epoch = MAX(drain_evacuations.epoch, excluded.epoch)"
        ),
        params![
            sandbox_id,
            SqliteU64(epoch),
            prior.as_str(),
            SqliteU64(now_ms)
        ],
    )?;
    Ok(())
}

/// Record the durable artifact of `sandbox_id`'s evacuation. Returns whether
/// a row took it.
pub fn complete_drain_evacuation(
    conn: &Connection,
    sandbox_id: &str,
    artifact_id: &str,
) -> Result<bool> {
    Ok(conn.execute(
        sql!("UPDATE drain_evacuations SET artifact_id = ?2 WHERE sandbox_id = ?1"),
        params![sandbox_id, artifact_id],
    )? == 1)
}

/// Every evacuation, by sandbox id. A row that does not decode fails the
/// read.
pub(in crate::db) fn drain_evacuations(conn: &Connection) -> Result<Vec<DrainEvacuation>> {
    let mut statement = conn.prepare(sql!(
        "SELECT sandbox_id, epoch, prior, artifact_id
           FROM drain_evacuations ORDER BY sandbox_id"
    ))?;
    let rows = statement.query_map([], |row| {
        let prior: String = row.get(2)?;
        Ok(DrainEvacuation {
            sandbox_id: row.get(0)?,
            epoch: row.get::<_, SqliteU64>(1)?.0,
            prior: DrainPrior::parse(&prior).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    format!("unknown drain prior {prior:?}").into(),
                )
            })?,
            artifact_id: row.get(3)?,
        })
    })?;
    rows.collect::<std::result::Result<_, _>>()
        .map_err(Into::into)
}

/// Write `record`, the seal row after a cancel of `cancelled`, and forget
/// the evacuations that cancel ends, in one transaction.
pub fn cancel_drain_epoch(
    conn: &Connection,
    record: ActivationSealRecord,
    cancelled: u64,
) -> Result<()> {
    let tx = crate::db::immediate_transaction(conn)?;
    write_activation_seal(&tx, record)?;
    forget_drain_evacuations_through(&tx, cancelled)?;
    tx.commit()?;
    Ok(())
}

/// Forget every evacuation begun under `epoch` or earlier: that drain was
/// cancelled.
pub fn forget_drain_evacuations_through(conn: &Connection, epoch: u64) -> Result<usize> {
    conn.execute(
        sql!("DELETE FROM drain_evacuations WHERE epoch <= ?1"),
        params![SqliteU64(epoch)],
    )
}

#[cfg(test)]
mod tests;
