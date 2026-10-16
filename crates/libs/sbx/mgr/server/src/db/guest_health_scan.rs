//! Sandbox rows the watchdog's guest-health and probe passes read.

use crate::db::{Connection, Error, Result};
use rusqlite::OptionalExtension as _;
use rusqlite::types::Type;

use pplx_space_common_core::sql;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SandboxGuestHealthRow {
    pub(crate) id: String,
    pub(crate) state: crate::state::SandboxState,
    pub(crate) template_content_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown sandbox state '{value}'")]
struct SandboxStateParseError {
    value: String,
}

pub(crate) fn list_guest_health_rows(conn: &Connection) -> Result<Vec<SandboxGuestHealthRow>> {
    let mut stmt = conn.prepare(sql!("SELECT id, state, template_hash FROM sandboxes"))?;
    let rows = stmt.query_map([], |row| {
        let state_value = row.get::<_, String>(1)?;
        let state = state_value
            .parse::<crate::state::SandboxState>()
            .map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    Type::Text,
                    Box::new(SandboxStateParseError { value: state_value }),
                )
            })?;
        Ok(SandboxGuestHealthRow {
            id: row.get(0)?,
            state,
            template_content_hash: row.get(2)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// One sandbox's `state` label, `None` without a row.
pub(crate) type StateLabelRead = Result<Option<String>>;

/// The `state` label of each of `sandbox_ids` through one prepared
/// statement, `None` for a sandbox without a row. Only preparing the
/// statement fails the whole lookup; each id's read fails on its own.
pub(crate) fn state_labels_for_ids<'a>(
    conn: &Connection,
    sandbox_ids: &'a [String],
) -> Result<Vec<(&'a String, StateLabelRead)>> {
    let mut statement = conn.prepare("SELECT state FROM sandboxes WHERE id = ?1")?;
    Ok(sandbox_ids
        .iter()
        .map(|sandbox_id| {
            let label = statement
                .query_row([sandbox_id], |row| row.get::<_, String>(0))
                .optional()
                .map_err(Error::from);
            (sandbox_id, label)
        })
        .collect())
}

#[cfg(test)]
mod tests;
