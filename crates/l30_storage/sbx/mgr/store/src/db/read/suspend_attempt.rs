use crate::db::{Connection, OptionalExtension as _, Result};
use pplx_space_common_core::types::LocalArtifactId;

/// The attempt admitted by the lifecycle transition. Its clock is not a row
/// update time or a file mtime, and is never renewed by recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SuspendAttempt {
    pub artifact_id: LocalArtifactId,
    pub started_at: i64,
}

pub fn suspend_attempt(conn: &Connection, id: &str) -> Result<Option<SuspendAttempt>> {
    conn.query_row(
        "SELECT suspend_attempt_artifact_id, suspend_attempt_started_at FROM sandboxes
         WHERE id = ?1 AND suspend_attempt_artifact_id IS NOT NULL",
        [id],
        |row| {
            Ok(SuspendAttempt {
                artifact_id: row.get::<_, String>(0)?.parse().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?,
                started_at: row.get::<_, crate::db::timestamps::EpochSeconds>(1)?.get(),
            })
        },
    )
    .optional()
}

/// Pins that staging reclamation must preserve even after an attempt finishes.
pub fn suspend_staging_pins(conn: &Connection, id: &str) -> Result<Vec<LocalArtifactId>> {
    Ok(conn.query_row(
        "SELECT current_local_artifact_id, suspend_predecessor_artifact_id FROM sandboxes WHERE id = ?1",
        [id], |row| [row.get::<_, Option<String>>(0)?, row.get::<_, Option<String>>(1)?]
            .into_iter().enumerate().filter_map(|(index, value)| value.map(|value| (index, value)))
            .map(|(index, value)| value.parse().map_err(|error| rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(error))))
            .collect::<rusqlite::Result<Vec<_>>>(),
    ).optional()?.unwrap_or_default())
}
