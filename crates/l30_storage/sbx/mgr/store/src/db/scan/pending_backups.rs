//! Lifecycle artifacts without a durable remote copy.

use crate::db::local_artifact_leases::PendingBackup;
use crate::db::{Connection, Result};

/// Every sandbox, in id order, whose pinned artifact has no backup-completed
/// stamp or names no artifact row the sandbox owns.
pub fn pending_backups(conn: &Connection) -> Result<Vec<PendingBackup>> {
    crate::db::local_artifact_leases::list_pending_backups(conn)
}
