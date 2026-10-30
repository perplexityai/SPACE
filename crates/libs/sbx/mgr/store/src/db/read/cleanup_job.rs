//! One sandbox-cleanup job record.

use pplx_space_common_core::types::JobId;

use crate::db::{Connection, Result};

/// The job, whatever its kind and status, `None` without one.
pub fn cleanup_job(
    conn: &Connection,
    job_id: &JobId,
) -> Result<Option<pplx_space_common_jobs::Job>> {
    crate::db::cleanup_jobs::get_job(conn, job_id)
}

/// Persisted start of the cleanup episode, decoded through the store's timestamp
/// boundary. Reconciliation must not grant a fresh shutdown budget after restart.
pub fn cleanup_job_created_at(
    conn: &Connection,
    job_id: &JobId,
) -> Result<Option<crate::db::timestamps::EpochSeconds>> {
    cleanup_job(conn, job_id)?
        .map(|job| {
            crate::db::timestamps::EpochSeconds::parse(&job.created_at).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
                .into()
            })
        })
        .transpose()
}
