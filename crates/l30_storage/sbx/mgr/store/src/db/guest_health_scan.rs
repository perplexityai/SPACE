//! Sandbox rows the watchdog's guest-health and probe passes read.

use crate::db::db_private_outside_tests;
use crate::db::{Connection, Error, Result};
use rusqlite::OptionalExtension as _;
use rusqlite::types::Type;

use pplx_space_common_core::sql;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxGuestHealthRow {
    pub id: String,
    pub state: crate::state::SandboxState,
    pub template_content_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown sandbox state '{value}'")]
struct SandboxStateParseError {
    value: String,
}

db_private_outside_tests! {
    fn list_guest_health_rows(conn: &Connection) -> Result<Vec<SandboxGuestHealthRow>> {
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
}

/// Metadata captured together when the watchdog dispatches a health probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxHealthProbeContext {
    pub state: crate::state::SandboxState,
    /// An unreadable template omits telemetry without suppressing health probes.
    pub template: Option<String>,
}

pub(crate) type HealthProbeContextRead = Result<Option<SandboxHealthProbeContext>>;

db_private_outside_tests! {
    /// The probe context of each of `sandbox_ids` through one prepared
    /// statement, `None` for a sandbox without a row. Only preparing the
    /// statement fails the whole lookup; each id's read fails on its own.
    fn health_probe_contexts_for_ids<'a>(
        conn: &Connection,
        sandbox_ids: &'a [String],
    ) -> Result<Vec<(&'a String, HealthProbeContextRead)>> {
        let mut statement =
            conn.prepare(sql!("SELECT state, template FROM sandboxes WHERE id = ?1"))?;
        Ok(sandbox_ids
            .iter()
            .map(|sandbox_id| {
                let context = statement
                    .query_row([sandbox_id], |row| {
                        let state_value = row.get::<_, String>(0)?;
                        let state = state_value
                            .parse::<crate::state::SandboxState>()
                            .map_err(|_| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    0,
                                    Type::Text,
                                    Box::new(SandboxStateParseError { value: state_value }),
                                )
                            })?;
                        let template = row
                            .get::<_, String>(1)
                            .inspect_err(|error| {
                                tracing::debug!(sandbox_id, %error, "unreadable sandbox template; skipping guest memory");
                            })
                            .ok();
                        Ok(SandboxHealthProbeContext { state, template })
                    })
                    .optional()
                    .map_err(Error::from);
                (sandbox_id, context)
            })
            .collect())
    }
}

#[cfg(test)]
mod tests;
