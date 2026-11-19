//! The one firewall assignment this node last accepted, and its writer
//! connection. [`read`] and [`replace`] take a `rusqlite::Connection`, so the
//! caller runs them in its own transaction; they return rusqlite's error
//! unchanged.

use std::path::Path;

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use pplx_space_sqlite::{RusqliteOpenOptions, SqliteU64};
use rusqlite::{Connection, OptionalExtension as _, params};
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// The accepted assignment as stored: `document` is the canonical revision
/// document, which this store neither decodes nor checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssignmentRow {
    pub node_id: NodeId,
    pub generation: u64,
    pub revision_id: Uuid,
    pub document: String,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error(transparent)]
    Open(#[from] pplx_space_sqlite::SqliteError),
    #[error("failed to configure the firewall assignment writer: {0}")]
    Configure(#[from] rusqlite::Error),
    #[error("firewall assignment writer has {setting}={found}, not {required}")]
    NotDurable {
        setting: &'static str,
        required: &'static str,
        found: String,
    },
}

/// Opens a connection for accepting assignments, in WAL mode with
/// `synchronous=FULL`, so a returned commit survives power loss.
pub fn open_writer(db_path: &Path) -> Result<Connection, OpenError> {
    let conn = pplx_space_sqlite::open_rusqlite(
        db_path,
        RusqliteOpenOptions {
            synchronous_normal: false,
            ..Default::default()
        },
    )?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    require_durable(&conn)?;
    Ok(conn)
}

/// Refuses a connection whose effective settings are weaker than WAL and FULL,
/// since SQLite can leave a requested journal mode unchanged without an error.
pub fn require_durable(conn: &Connection) -> Result<(), OpenError> {
    let journal_mode: String = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(OpenError::NotDurable {
            setting: "journal_mode",
            required: "wal",
            found: journal_mode,
        });
    }
    // 2 is FULL.
    let synchronous: i64 = conn.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if synchronous != 2 {
        return Err(OpenError::NotDurable {
            setting: "synchronous",
            required: "2 (FULL)",
            found: synchronous.to_string(),
        });
    }
    Ok(())
}

/// The accepted assignment, or `None` before the first acceptance.
pub fn read(conn: &Connection) -> rusqlite::Result<Option<AssignmentRow>> {
    conn.query_row(
        sql!(
            "SELECT node_id, assignment_generation, revision_id, revision_document
             FROM firewall_rules_assignment
             WHERE id = 1"
        ),
        [],
        |row| {
            Ok(AssignmentRow {
                node_id: parse(row, 0)?,
                generation: row.get::<_, SqliteU64>(1)?.0,
                revision_id: parse(row, 2)?,
                document: row.get(3)?,
            })
        },
    )
    .optional()
}

/// Replaces the accepted assignment, or saves the first one. A generation
/// above `i64::MAX` is refused rather than stored negative.
pub fn replace(conn: &Connection, row: &AssignmentRow) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO firewall_rules_assignment
                 (id, node_id, assignment_generation, revision_id, revision_document)
             VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT (id) DO UPDATE SET
                 node_id = excluded.node_id,
                 assignment_generation = excluded.assignment_generation,
                 revision_id = excluded.revision_id,
                 revision_document = excluded.revision_document"
        ),
        params![
            row.node_id.to_string(),
            SqliteU64(row.generation),
            row.revision_id.to_string(),
            row.document,
        ],
    )?;
    Ok(())
}

fn parse<T>(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    row.get::<_, String>(index)?.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}
