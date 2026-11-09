//! What an eviction decides against: a row's activation generations, and
//! whether an upload still reads its artifacts.

use crate::db::{ActivationGenerations, Connection, Result};

/// `sandbox_id`'s activation generations, as a witness
/// [`crate::db::claim_stale_upgrade_copy_for_eviction`] re-asserts; `None`
/// without a row.
pub fn activation_generations(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<ActivationGenerations>> {
    crate::db::activation_generations(conn, sandbox_id)
}

/// Whether an upload of `sandbox_id`'s artifacts is pending, running or
/// still cancelling.
pub fn snapshot_upload_in_flight(conn: &Connection, sandbox_id: &str) -> Result<bool> {
    crate::db::snapshot_upload_in_flight(conn, sandbox_id)
}
