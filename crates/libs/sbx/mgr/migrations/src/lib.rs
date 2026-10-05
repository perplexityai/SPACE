//! Sandbox-manager schema admission and migrations shared by the daemon and CLI.
use rusqlite::Connection;
use sqlx::migrate::Migrator;

pub static SANDBOX_MANAGER_MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/sandbox-manager");
pub static COMMON_SCHEMA_MIGRATOR: Migrator = sqlx::migrate!("../../../../../db/common");

// The final schema contraction raises SQLite's application-owned user_version.
// Admission happens before migrations or any service work so a
// binary from the preparation release cannot operate on the contracted store.
const SUPPORTED_SCHEMA_FLOOR: i64 = 3;

#[derive(Debug, thiserror::Error)]
enum PrepareMigrationsError {
    #[error("failed to read sandbox-manager schema floor: {0}")]
    ReadSchemaFloor(#[source] rusqlite::Error),
    #[error(
        "sandbox-manager schema floor {required} is not admitted for this operation (allowed 0..={supported}); use a forward upgrade or repair"
    )]
    UnsupportedSchemaFloor { required: i64, supported: i64 },
    #[error("failed to inspect active snapshot uploads: {0}")]
    InspectUploads(#[source] rusqlite::Error),
    #[error(
        "active snapshot upload {0} lacks staged metadata; finish or repair it with the preparation release before cutover"
    )]
    LegacyUpload(String),
}

/// Reject an incompatible schema floor or active upload before either the
/// daemon or CLI writes to the store.
pub fn prepare_migrations(
    conn: &Connection,
) -> Result<(), pplx_space_sqlite_migrations::MigrationCompatibilityError> {
    prepare_migrations_inner(conn)
        .map_err(pplx_space_sqlite_migrations::MigrationCompatibilityError::new)
}

/// No part of the contracted database, including its common job schema, may
/// be downgraded. Check before the CLI creates or edits migration history.
pub fn ensure_downgrade_allowed(
    conn: &Connection,
) -> Result<(), pplx_space_sqlite_migrations::MigrationCompatibilityError> {
    check_schema_floor(conn, 1)
        .map_err(pplx_space_sqlite_migrations::MigrationCompatibilityError::new)
}

fn prepare_migrations_inner(conn: &Connection) -> Result<(), PrepareMigrationsError> {
    check_schema_floor(conn, SUPPORTED_SCHEMA_FLOOR)?;
    check_active_upload_format(conn)
}

fn check_schema_floor(conn: &Connection, supported: i64) -> Result<(), PrepareMigrationsError> {
    let required = conn
        .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
        .map_err(PrepareMigrationsError::ReadSchemaFloor)?;
    if required < 0 || required > supported {
        return Err(PrepareMigrationsError::UnsupportedSchemaFloor {
            required,
            supported,
        });
    }
    Ok(())
}

fn check_active_upload_format(conn: &Connection) -> Result<(), PrepareMigrationsError> {
    use rusqlite::OptionalExtension;
    let has_jobs: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'jobs')",
            [],
            |row| row.get(0),
        )
        .map_err(PrepareMigrationsError::InspectUploads)?;
    if !has_jobs {
        return Ok(());
    }
    let incompatible: Option<String> = conn
        .query_row(
            "SELECT job_id FROM jobs WHERE kind = 'snapshot_upload'
         AND status IN ('pending', 'running')
         AND CASE WHEN json_valid(payload)
             THEN json_type(payload, '$.includes_metadata_job') IS NOT 'true'
             ELSE 1 END
         ORDER BY job_id LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(PrepareMigrationsError::InspectUploads)?;
    match incompatible {
        Some(job_id) => Err(PrepareMigrationsError::LegacyUpload(job_id)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;
