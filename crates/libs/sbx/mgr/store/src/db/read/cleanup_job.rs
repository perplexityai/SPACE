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
