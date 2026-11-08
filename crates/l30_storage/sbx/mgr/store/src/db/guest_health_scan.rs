//! Sandbox rows the watchdog's guest-health and probe passes read.

use crate::db::db_private_outside_tests;
use crate::db::{Connection, Error, Result};
use rusqlite::OptionalExtension as _;
use rusqlite::types::Type;

use pplx_space_common_core::sql;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxGuestHealthRow {
    pub id: String,
    pub state: crate::state::SandboxStatus,
    pub template_content_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown sandbox state '{value}'")]
struct SandboxStateParseError {
    value: String,
}

db_private_outside_tests! {
    fn list_guest_health_rows(conn: &Connection) -> Result<Vec<SandboxGuestHealthRow>> {
        let mut stmt = conn.prepare(sql!(
            "SELECT id, state, template_hash, cleanup_state FROM sandboxes"
        ))?;
        let rows = stmt.query_map([], |row| {
            let state_value = row.get::<_, String>(1)?;
            let cleanup_value = row.get::<_, Option<String>>(3)?;
            let state = status_from_columns(1, state_value, cleanup_value)?;
            Ok(SandboxGuestHealthRow {
                id: row.get(0)?,
                state,
                template_content_hash: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }
}

/// The status the `state` and `cleanup_state` text names, or a conversion
/// error that quotes both.
fn status_from_columns(
    column: usize,
    state: String,
    cleanup: Option<String>,
) -> rusqlite::Result<crate::state::SandboxStatus> {
    crate::state::SandboxStatus::from_columns(&state, cleanup.as_deref()).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            Type::Text,
            Box::new(SandboxStateParseError {
                value: format!("{state}, cleanup {cleanup:?}"),
            }),
        )
    })
}

/// One sandbox's status, `None` without a row.
pub(crate) type StatusRead = Result<Option<crate::state::SandboxStatus>>;

db_private_outside_tests! {
    /// The status of each of `sandbox_ids` through one prepared statement,
    /// `None` for a sandbox without a row. Only preparing the statement fails
    /// the whole lookup; each id's read fails on its own, as does a row whose
    /// columns name no status.
    fn statuses_for_ids<'a>(
        conn: &Connection,
        sandbox_ids: &'a [String],
    ) -> Result<Vec<(&'a String, StatusRead)>> {
        let mut statement =
            conn.prepare("SELECT state, cleanup_state FROM sandboxes WHERE id = ?1")?;
        Ok(sandbox_ids
            .iter()
            .map(|sandbox_id| {
                let label = statement
                    .query_row([sandbox_id], |row| {
                        status_from_columns(0, row.get(0)?, row.get(1)?)
                    })
                    .optional()
                    .map_err(Error::from);
                (sandbox_id, label)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests;
