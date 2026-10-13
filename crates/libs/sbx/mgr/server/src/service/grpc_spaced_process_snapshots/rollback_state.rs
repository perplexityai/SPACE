//! The full process-snapshot rollback's write to the node-local SQLite
//! `sandboxes.state`.

use super::super::cleanup_support::{StateWriteRejected, state_write_result};
use crate::db::edges;
use rusqlite::Connection;
use tonic::Status;

/// Restage a full process-snapshot rollback's `stopped` row as `suspended`, so
/// `start_from_checkpoint` can restore the snapshot. A row that is already
/// `suspended` needs no write. Any other row was moved after the rollback
/// checked it, for example by a Stop admitted after the rollback's own stop,
/// and the rollback reports that it was superseded instead of overwriting it.
pub(super) fn restage_rolled_back_full_snapshot(
    db: &Connection,
    sandbox_id: &str,
) -> Result<(), Status> {
    const CONTEXT: &str = "complete process snapshot suspension";
    let edge = &edges::PROCESS_SNAPSHOT_ROLLBACK_RESTAGE;
    let outcome = crate::db::update_state_from(db, sandbox_id, edge)
        .map_err(|error| Status::internal(format!("{CONTEXT}: {error}")))?;
    match state_write_result(outcome) {
        Ok(_) => Ok(()),
        Err(StateWriteRejected::FromState { found }) if found == edge.to() => Ok(()),
        Err(rejected) => Err(rejected.into_status(CONTEXT)),
    }
}
